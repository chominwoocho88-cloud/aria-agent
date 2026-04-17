"""
JACKAL backtest module.
Jackal research backtest runner (v2).

기존 문제:
  - load_tickers()가 포트폴리오 6개만 봄 (MY_PORTFOLIO와 동일)
  - Stage 파이프라인 없이 신호 규칙만 직접 적용

수정:
  Universe(~80) → Stage1→Top50 → Stage2→Top25 → Stage3→Top10 → Stage4→Top5
  Top5 결과는 SQLite research spine에 기록하고, 운영 weights 와 분리

비용: $0 (Stage3/4 Claude 없음, 수치 기반 대체)
소요: ~5분 (yfinance ~80종목 다운로드)

[Bug Fix] 경로 수정 (2024-04)
  - 실행 위치: repo root (`python -m jackal.backtest`)
  - _ROOT = Path(__file__).parent  → jackal/ 폴더
  - MEMORY_FILE = _ROOT / "data" / "memory.json"
      → 실제: jackal/data/memory.json (없음)
      → 정상: data/memory.json (repo root 기준)
  수정: _JACKAL_DIR / _REPO_ROOT 분리로 경로를 명확히 구분
"""

import sys
import json
import os
import time
import functools
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

os.environ["PYTHONIOENCODING"] = "utf-8"
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import yfinance as yf
import pandas as pd
from orca.state import (
    finish_backtest_session,
    get_latest_backtest_session,
    list_backtest_days,
    record_backtest_day,
    record_backtest_pick_results,
    start_backtest_session,
)

# ── 경로 (Bug Fix) ────────────────────────────────────────────────
_JACKAL_DIR = Path(__file__).parent          # jackal/
_REPO_ROOT  = _JACKAL_DIR.parent             # repo root (aria-agent/)

MEMORY_FILE = _REPO_ROOT / "data" / "memory.json"          # fallback source

_JACKAL_SESSION_ID: str | None = None

BACKTEST_DAYS = 60
TRACKING_DAYS = 10

# ── 실운용 상수 import (Universe 정의) ────────────────────────────
from .hunter import SECTOR_POOLS, MY_PORTFOLIO, SECTOR_ETF


# ══════════════════════════════════════════════════════════════════
# Universe 구성 (실운용 동일)
# ══════════════════════════════════════════════════════════════════

def _build_universe() -> list:
    """
    SECTOR_POOLS 전체에서 MY_PORTFOLIO 제외.
    실운용: SECTOR_POOLS 80개 + Claude 추천 20개 → 백테스트는 Claude 없이 80개만.
    """
    seen = set()
    universe = []
    for tickers in SECTOR_POOLS.values():
        for t in tickers:
            if t not in MY_PORTFOLIO and t not in seen:
                universe.append(t)
                seen.add(t)
    return universe


# ══════════════════════════════════════════════════════════════════
# 역사적 지표 계산 (look-ahead bias 없음)
# ══════════════════════════════════════════════════════════════════

def calc_indicators_hist(df: pd.DataFrame, as_of: str) -> dict | None:
    """
    as_of 날짜 이전 데이터만 사용 — 미래 데이터 참조 없음.
    jackal_hunter._calc_tech()와 동일한 지표 구조.
    """
    cutoff = pd.Timestamp(as_of)
    sub    = df[df.index <= cutoff].copy()
    if len(sub) < 22:
        return None

    close  = sub["Close"]
    volume = sub["Volume"] if "Volume" in sub.columns else pd.Series(dtype=float)
    price  = float(close.iloc[-1])
    if price <= 0:
        return None

    # RSI(14)
    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    rs    = gain / loss.replace(0, float("inf"))
    rsi   = float((100 - 100 / (1 + rs)).iloc[-1])

    # 볼린저 밴드
    ma20  = float(close.rolling(20).mean().iloc[-1])
    ma50  = float(close.rolling(50).mean().iloc[-1]) if len(close) >= 50 else None
    std20 = float(close.rolling(20).std().iloc[-1])
    bb_pos = (price - (ma20 - 2 * std20)) / (4 * std20) * 100 if std20 > 0 else 50.0

    # 거래량 비율
    avg_vol   = float(volume.iloc[-6:-1].mean()) if len(volume) >= 6 else float(volume.mean() or 1)
    vol_ratio = round(float(volume.iloc[-1]) / avg_vol, 2) if avg_vol > 0 else 1.0

    def chg(n: int) -> float:
        if len(close) > n:
            return round((price - float(close.iloc[-n - 1])) / float(close.iloc[-n - 1]) * 100, 2)
        return 0.0

    # RSI 강세 다이버전스 (가격 하락 + RSI 개선)
    bullish_div = False
    if len(close) >= 7 and chg(5) < -1.5:
        try:
            sub5     = close.iloc[:-5]
            d5       = sub5.diff()
            g5       = d5.clip(lower=0).rolling(14).mean()
            l5       = (-d5.clip(upper=0)).rolling(14).mean()
            rs5      = g5 / l5.replace(0, float("inf"))
            rsi_5d   = float((100 - 100 / (1 + rs5)).iloc[-1])
            p_5d     = float(close.iloc[-6])
            bullish_div = (price < p_5d) and (rsi > rsi_5d + 2)
        except Exception:
            pass

    # 양봉 여부
    bullish_candle = False
    if "Open" in sub.columns:
        try:
            bullish_candle = float(sub["Open"].iloc[-1]) < price
        except Exception:
            pass

    return {
        "price":          round(price, 2),
        "change_1d":      chg(1),
        "change_3d":      chg(3),
        "change_5d":      chg(5),
        "rsi":            round(rsi, 1),
        "ma20":           round(ma20, 2),
        "ma50":           round(ma50, 2) if ma50 else None,
        "bb_pos":         round(bb_pos, 1),
        "vol_ratio":      vol_ratio,
        "bullish_div":    bullish_div,
        "bullish_candle": bullish_candle,
    }


# ══════════════════════════════════════════════════════════════════
# Stage1 점수 (jackal_hunter._stage1_technical 이식)
# ══════════════════════════════════════════════════════════════════

def _s1_score(tech: dict, ticker: str, inflows: str = "") -> float:
    s   = 0.0
    rsi = tech.get("rsi", 50)
    bb  = tech.get("bb_pos", 50)
    ch5 = tech.get("change_5d", 0)
    vol = tech.get("vol_ratio", 1.0)

    # RSI 과매도
    if rsi <= 25:  s += 30
    elif rsi <= 30: s += 25
    elif rsi <= 35: s += 18
    elif rsi <= 40: s += 10
    elif rsi <= 45: s += 4

    # 볼린저 하단
    if bb <= 5:   s += 25
    elif bb <= 10: s += 20
    elif bb <= 20: s += 12
    elif bb <= 30: s += 5

    # 5일 낙폭
    if ch5 <= -10: s += 20
    elif ch5 <= -7: s += 15
    elif ch5 <= -5: s += 10
    elif ch5 <= -3: s += 5

    # 거래량 급등
    if vol >= 3.0:  s += 12
    elif vol >= 2.0: s += 8
    elif vol >= 1.5: s += 3

    # MA 지지 (보조)
    ma50 = tech.get("ma50")
    if ma50 and abs(tech["price"] - ma50) / ma50 < 0.03:
        if rsi <= 40 or bb <= 30:
            s += 5

    # 강세 다이버전스
    if tech.get("bullish_div"):
        s += 15

    # 양봉 반전
    if tech.get("bullish_candle") and ch5 < -3:
        s += 5

    # 섹터 유입 보정
    for sec, tks in SECTOR_POOLS.items():
        if ticker in tks:
            kws = sec.lower().replace("/", " ").split()
            if any(k in inflows for k in kws):
                s += 8
            break

    return round(s, 1)


# ══════════════════════════════════════════════════════════════════
# 결과 추적
# ══════════════════════════════════════════════════════════════════

def track_outcome(df: pd.DataFrame, signal_date: str,
                  tracking_days: int = 10) -> dict:
    cutoff = pd.Timestamp(signal_date)
    future = df[df.index > cutoff].iloc[:tracking_days].copy()

    if future.empty:
        return {"peak_day": None, "peak_pct": None, "final_pct": None,
                "d1_pct": None, "d1_hit": None, "swing_hit": None}

    entry   = float(df[df.index <= cutoff]["Close"].iloc[-1])
    returns = [(float(r) - entry) / entry * 100 for r in future["Close"]]

    d1_pct   = round(returns[0], 2) if returns else None
    d1_hit   = (d1_pct > 0.3) if d1_pct is not None else None

    sw_window = returns[:7]
    peak_pct  = round(max(sw_window), 2) if sw_window else 0.0
    peak_idx  = sw_window.index(max(sw_window)) if sw_window else 0
    swing_hit = peak_pct >= 1.0

    return {
        "peak_day":  peak_idx + 1,
        "peak_pct":  peak_pct,
        "final_pct": round(returns[-1], 2) if returns else None,
        "d1_pct":    d1_pct,
        "d1_hit":    d1_hit,
        "swing_hit": swing_hit,
    }


def parse_orca_context(report: dict) -> dict:
    return {
        "regime":       report.get("market_regime", ""),
        "key_inflows":  [i.get("zone", "") for i in report.get("inflows", [])[:3]],
        "key_outflows": [o.get("zone", "") for o in report.get("outflows", [])[:3]],
    }


# ══════════════════════════════════════════════════════════════════
# 데이터 로딩
# ══════════════════════════════════════════════════════════════════

def load_memory() -> tuple[list, dict]:
    source_info = {"source": "production_memory", "session_id": None, "phase_label": None}
    all_morning: list[dict] = []

    for label in ("walk_forward", "backtest"):
        orca_session = get_latest_backtest_session("orca", label=label)
        if not orca_session:
            continue
        for phase_label in ("Final", "main"):
            days = list_backtest_days(orca_session["session_id"], phase_label=phase_label)
            reports = [
                row.get("analysis", {})
                for row in days
                if isinstance(row.get("analysis", {}), dict)
                and row.get("analysis", {}).get("mode") == "MORNING"
            ]
            if reports:
                all_morning = sorted(reports, key=lambda r: r.get("analysis_date", ""))
                source_info = {
                    "source": "orca_research_session",
                    "orca_session_id": orca_session["session_id"],
                    "session_id": orca_session["session_id"],
                    "phase_label": phase_label,
                    "label": orca_session["label"],
                }
                print(
                    f"   ORCA research session used: {orca_session['session_id']} "
                    f"({orca_session['label']}/{phase_label})"
                )
                break
        if all_morning:
            break

    if not all_morning:
        if not MEMORY_FILE.exists():
            print(f"❌ memory.json 없음: {MEMORY_FILE}")
            sys.exit(1)

        mem = json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
        all_morning = sorted(
            [r for r in mem if r.get("mode") == "MORNING"],
            key=lambda r: r.get("analysis_date", ""),
        )
        print(f"   memory.json fallback 사용 | MORNING 전체: {len(all_morning)}개")

    cutoff  = (datetime.now() - timedelta(days=BACKTEST_DAYS + TRACKING_DAYS)).strftime("%Y-%m-%d")
    morning = [r for r in all_morning if r.get("analysis_date", "") >= cutoff]
    if not morning:
        print(f"⚠️  cutoff({cutoff}) 이후 없음 → 전체 사용")
        morning = all_morning

    if not morning:
        print("❌ Jackal backtest 입력용 MORNING 레코드 없음")
        sys.exit(1)

    print(f"✅ 백테스트 대상: {len(morning)}개 | "
          f"{morning[0]['analysis_date']} ~ {morning[-1]['analysis_date']}")
    return morning, source_info


@functools.lru_cache(maxsize=128)
def _fetch_yf_cached(ticker: str):
    """yfinance 1년 일봉 캐싱 — 중복 호출 방지."""
    for attempt in range(3):
        try:
            df = yf.Ticker(ticker).history(period="1y", interval="1d")
            if not df.empty:
                df.index = pd.to_datetime(df.index).tz_localize(None)
                return df
        except Exception:
            if attempt < 2:
                time.sleep(2)
    return None


# ══════════════════════════════════════════════════════════════════
# 메인 백테스트
# ══════════════════════════════════════════════════════════════════

def run_backtest():
    global _JACKAL_SESSION_ID

    print("\n" + "=" * 62)
    print("  🦊 Jackal Backtest v2 — research spine 연동")
    print("  파이프라인: Universe→Stage1(50)→Stage2(25)→Stage3(10)→Stage4(5)")
    print(f"  대상: 최근 {BACKTEST_DAYS}거래일 | Peak 추적: {TRACKING_DAYS}일")
    print("=" * 62)

    memory, source_info = load_memory()
    _JACKAL_SESSION_ID = start_backtest_session(
        "jackal",
        "backtest",
        config={
            "backtest_days": BACKTEST_DAYS,
            "tracking_days": TRACKING_DAYS,
            "source": source_info,
        },
    )
    print(f"\n🗃️ Jackal research session: {_JACKAL_SESSION_ID}")
    try:
        universe = _build_universe()
        print(f"\n🌐 Universe: {len(universe)}종목 (SECTOR_POOLS, MY_PORTFOLIO 제외)")
        print(f"   제외: {', '.join(sorted(MY_PORTFOLIO))}")

        print(f"\n📥 yfinance 다운로드 ({len(universe)}종목)...")
        hist: dict = {}
        batch = []
        for i, ticker in enumerate(universe):
            df = _fetch_yf_cached(ticker)
            if df is not None:
                hist[ticker] = df
                batch.append(f"{ticker}✅")
            else:
                batch.append(f"{ticker}❌")
            if len(batch) == 10 or i == len(universe) - 1:
                print("  " + "  ".join(batch))
                batch = []
            time.sleep(0.05)
        print(f"\n   완료: {len(hist)}/{len(universe)}종목\n")

        all_results: list = []
        funnel_totals = {
            "universe": 0,
            "s1_top50": 0,
            "s2_top25": 0,
            "s3_top10": 0,
            "s4_top5": 0,
            "tracked": 0,
        }

        print("=" * 62)
        print("  📅 날짜별 파이프라인 실행")
        print("=" * 62)

        for report in memory:
            date_str = report.get("analysis_date", "")
            orca     = parse_orca_context(report)
            inflows  = " ".join(orca["key_inflows"]).lower()
            regime   = orca["regime"]

            scored = []
            for ticker in universe:
                df = hist.get(ticker)
                if df is None:
                    continue
                tech = calc_indicators_hist(df, date_str)
                if tech is None:
                    continue
                s = _s1_score(tech, ticker, inflows)
                if s > 0:
                    scored.append({"ticker": ticker, "tech": tech, "s1_score": s})

            funnel_totals["universe"] += len(universe)
            scored.sort(key=lambda x: x["s1_score"], reverse=True)
            top50 = scored[:50]
            funnel_totals["s1_top50"] += len(top50)

            regime_boost = 8 if "선호" in regime else -5 if "회피" in regime else 2
            for item in top50:
                item["s2_score"] = item["s1_score"] + regime_boost
            top50.sort(key=lambda x: x["s2_score"], reverse=True)
            top25 = top50[:25]
            funnel_totals["s2_top25"] += len(top25)

            top10 = top25[:10]
            funnel_totals["s3_top10"] += len(top10)

            top5 = top10[:5]
            funnel_totals["s4_top5"] += len(top5)

            daily_picks = []
            daily_tracked = 0
            for rank, item in enumerate(top5, start=1):
                ticker = item["ticker"]
                df     = hist.get(ticker)
                outcome = track_outcome(df, date_str, TRACKING_DAYS) if df is not None else {
                    "peak_day": None,
                    "peak_pct": None,
                    "final_pct": None,
                    "d1_pct": None,
                    "d1_hit": False,
                    "swing_hit": False,
                }

                pick_entry = {
                    "rank_index": rank,
                    "ticker": ticker,
                    "regime": regime,
                    "scores": {
                        "s1_score": item.get("s1_score"),
                        "s2_score": item.get("s2_score"),
                    },
                    "indicators": {
                        "rsi": item["tech"].get("rsi"),
                        "bb_pos": item["tech"].get("bb_pos"),
                        "change_5d": item["tech"].get("change_5d"),
                        "vol_ratio": item["tech"].get("vol_ratio"),
                        "bullish_div": item["tech"].get("bullish_div"),
                    },
                    "outcome": outcome,
                }
                daily_picks.append(pick_entry)

                if outcome.get("peak_pct") is None:
                    continue

                all_results.append({
                    "date":       date_str,
                    "ticker":     ticker,
                    "regime":     regime,
                    "s1_score":   item["s1_score"],
                    "s2_score":   item["s2_score"],
                    "rsi":        item["tech"]["rsi"],
                    "bb_pos":     item["tech"]["bb_pos"],
                    "change_5d":  item["tech"]["change_5d"],
                    "vol_ratio":  item["tech"]["vol_ratio"],
                    "bullish_div": item["tech"]["bullish_div"],
                    **outcome,
                })
                daily_tracked += 1
                funnel_totals["tracked"] += 1

            record_backtest_pick_results(
                _JACKAL_SESSION_ID,
                "jackal",
                date_str,
                "main",
                daily_picks,
                source_session_id=source_info.get("orca_session_id") or source_info.get("session_id"),
            )
            record_backtest_day(
                _JACKAL_SESSION_ID,
                date_str,
                "main",
                market_note=report.get("one_line_summary", ""),
                analysis=report,
                results=[
                    {
                        "ticker": pick["ticker"],
                        "rank_index": pick["rank_index"],
                        "outcome": pick["outcome"],
                    }
                    for pick in daily_picks
                ],
                metrics={
                    "source": source_info,
                    "funnel": {
                        "s1_top50": len(top50),
                        "s2_top25": len(top25),
                        "s3_top10": len(top10),
                        "s4_top5": len(top5),
                        "tracked": daily_tracked,
                    },
                },
            )

            print(f"  {date_str} [{regime[:6]:6}] "
                  f"S1:{len(top50):2} S2:{len(top25):2} S3:{len(top10):2} "
                  f"S4:{len(top5)} 추적:{funnel_totals['tracked']}")

        print("\n" + "=" * 62)
        print("  📊 파이프라인 퍼널 요약")
        print("=" * 62)
        print(f"  Universe   : {funnel_totals['universe']:,}")
        print(f"  Stage1 Top50: {funnel_totals['s1_top50']:,}")
        print(f"  Stage2 Top25: {funnel_totals['s2_top25']:,}")
        print(f"  Stage3 Top10: {funnel_totals['s3_top10']:,}")
        print(f"  Stage4 Top5 : {funnel_totals['s4_top5']:,}")
        print(f"  추적 완료   : {funnel_totals['tracked']:,}")

        total = len(all_results)
        if total == 0:
            summary = {
                "mode": "jackal_backtest",
                "source": source_info,
                "total_tracked": 0,
                "funnel_totals": funnel_totals,
            }
            finish_backtest_session(_JACKAL_SESSION_ID, "completed", summary=summary)
            print("\n⚠️  추적 가능한 결과 없음 (데이터 부족 또는 최근 날짜 전용)")
            print(f"🗃️ Jackal research session saved: {_JACKAL_SESSION_ID}")
            return summary

        sw_hit = sum(1 for r in all_results if r.get("swing_hit"))
        d1_hit = sum(1 for r in all_results if r.get("d1_hit"))
        div_ok = sum(1 for r in all_results if r.get("bullish_div") and r.get("swing_hit"))
        div_n  = sum(1 for r in all_results if r.get("bullish_div"))

        sw_acc  = sw_hit / total * 100
        d1_acc  = d1_hit / total * 100
        div_acc = div_ok / div_n * 100 if div_n else 0.0

        print(f"\n  1일 정확도 : {d1_acc:.1f}% ({d1_hit}/{total})")
        print(f"  스윙 정확도: {sw_acc:.1f}% ({sw_hit}/{total})")
        print(f"  다이버전스  : {div_acc:.1f}% ({div_ok}/{div_n})")

        print("\n  📊 레짐별 스윙 정확도:")
        regime_acc: dict = defaultdict(lambda: {"total": 0, "swing_correct": 0})
        for r in all_results:
            rg = r["regime"]
            regime_acc[rg]["total"] += 1
            regime_acc[rg]["swing_correct"] += int(r.get("swing_hit", False))
        regime_stats = {}
        for rg, v in regime_acc.items():
            acc_pct = v["swing_correct"] / v["total"] * 100 if v["total"] else 0
            regime_stats[rg] = {
                "total": v["total"],
                "swing_correct": v["swing_correct"],
                "swing_accuracy": round(acc_pct, 1),
            }
            print(f"    {rg[:10]:10} {acc_pct:5.1f}% ({v['swing_correct']}/{v['total']})")

        print("\n  📊 티커별 스윙 정확도 (3건 이상):")
        ticker_acc: dict = defaultdict(list)
        for r in all_results:
            ticker_acc[r["ticker"]].append(r)
        ticker_stats = {}
        for tk, entries in sorted(ticker_acc.items(), key=lambda x: len(x[1]), reverse=True):
            if len(entries) < 3:
                continue
            ok = sum(1 for e in entries if e.get("swing_hit"))
            pk_days = [e["peak_day"] for e in entries if e.get("peak_day")]
            avg_d = round(sum(pk_days) / len(pk_days), 1) if pk_days else 5.0
            avg_pk = round(
                sum(e["peak_pct"] for e in entries if e.get("peak_pct") is not None) / len(entries),
                2,
            )
            swa = ok / len(entries) * 100
            ticker_stats[tk] = {
                "total": len(entries),
                "swing_correct": ok,
                "swing_accuracy": round(swa, 1),
                "avg_peak_day": avg_d,
                "avg_peak_pct": avg_pk,
            }
            print(f"    {tk:<14} {len(entries):3}건 | 스윙 {swa:5.1f}% | "
                  f"Peak D{avg_d:.1f} ({avg_pk:+.2f}%)")

        summary = {
            "mode": "jackal_backtest",
            "source": source_info,
            "backtest_version": "v2_pipeline",
            "pipeline": "Universe→Stage1(50)→Stage2(25)→Stage3(10)→Stage4(5)",
            "backtest_days": len(memory),
            "total_tracked": total,
            "d1_accuracy": round(d1_acc, 1),
            "swing_accuracy": round(sw_acc, 1),
            "bullish_div_accuracy": round(div_acc, 1),
            "regime_accuracy": regime_stats,
            "ticker_accuracy": ticker_stats,
            "funnel_totals": funnel_totals,
        }
        finish_backtest_session(_JACKAL_SESSION_ID, "completed", summary=summary)

        print("\n" + "=" * 62)
        print(f"  ✅ SQLite 저장 완료: {_JACKAL_SESSION_ID}")
        print(f"     스윙 {sw_acc:.1f}% | 1일 {d1_acc:.1f}% | 다이버전스 {div_acc:.1f}%")
        print("=" * 62 + "\n")

        return summary
    except Exception as e:
        if _JACKAL_SESSION_ID:
            try:
                finish_backtest_session(
                    _JACKAL_SESSION_ID,
                    "failed",
                    summary={"error": str(e), "source": source_info},
                )
            except Exception:
                pass
        raise

def main() -> None:
    run_backtest()


if __name__ == "__main__":
    main()




