"""
jackal_backtest.py — 실운용 파이프라인 반영 백테스트 (v2)

기존 문제:
  - load_tickers()가 포트폴리오 6개만 봄 (MY_PORTFOLIO와 동일)
  - Stage 파이프라인 없이 신호 규칙만 직접 적용

수정:
  Universe(~80) → Stage1→Top50 → Stage2→Top25 → Stage3→Top10 → Stage4→Top5
  Top5에 대해서만 1일/스윙 결과 추적 → jackal_weights.json

비용: $0 (Stage3/4 Claude 없음, 수치 기반 대체)
소요: ~5분 (yfinance ~80종목 다운로드)
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

# ── 경로 ──────────────────────────────────────────────────────────
_ROOT       = Path(__file__).parent
MEMORY_FILE = _ROOT / "data" / "memory.json"
OUTPUT_FILE = _ROOT / "jackal" / "jackal_weights.json"

BACKTEST_DAYS = 60
TRACKING_DAYS = 10

# ── 실운용 상수 import (Universe 정의) ────────────────────────────
# jackal_hunter.py의 SECTOR_POOLS, MY_PORTFOLIO를 그대로 사용
# → 백테스트와 실운용이 동일한 Universe에서 출발함을 보장
try:
    from jackal.jackal_hunter import SECTOR_POOLS, MY_PORTFOLIO, SECTOR_ETF
except ImportError:
    sys.path.insert(0, str(_ROOT / "jackal"))
    from jackal_hunter import SECTOR_POOLS, MY_PORTFOLIO, SECTOR_ETF


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
    rsi   = float((100 - 100 / (1 + gain / loss)).iloc[-1])

    # 볼린저 밴드
    ma20  = float(close.rolling(20).mean().iloc[-1])
    ma50  = float(close.rolling(50).mean().iloc[-1]) if len(close) >= 50 else None
    std20 = float(close.rolling(20).std().iloc[-1])
    bb_pos = (price - (ma20 - 2*std20)) / (4*std20) * 100 if std20 > 0 else 50

    # 거래량 비율
    avg_vol   = float(volume.iloc[-6:-1].mean()) if len(volume) >= 6 else float(volume.mean() or 1)
    vol_ratio = round(float(volume.iloc[-1]) / avg_vol, 2) if avg_vol > 0 else 1.0

    def chg(n):
        if len(close) > n:
            return round((price - float(close.iloc[-n-1])) / float(close.iloc[-n-1]) * 100, 2)
        return 0.0

    # RSI 강세 다이버전스 (가격 하락 + RSI 개선)
    bullish_div = False
    if len(close) >= 7 and chg(5) < -1.5:
        try:
            sub5 = close.iloc[:-5]
            d5   = sub5.diff()
            g5   = d5.clip(lower=0).rolling(14).mean()
            l5   = (-d5.clip(upper=0)).rolling(14).mean()
            rsi_5d_ago   = float((100 - 100 / (1 + g5 / l5)).iloc[-1])
            price_5d_ago = float(close.iloc[-6])
            bullish_div  = (price < price_5d_ago) and (rsi > rsi_5d_ago + 2)
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
# Stage1: 기술지표 점수 (jackal_hunter._stage1_technical 이식)
# ══════════════════════════════════════════════════════════════════

def _s1_score(tech: dict, ticker: str, inflows: str = "") -> float:
    """
    jackal_hunter._stage1_technical()의 점수 로직을 그대로 이식.
    ETF 상대강도는 생략 (백테스트 비용 절감).
    """
    s    = 0
    rsi  = tech["rsi"];    bb   = tech["bb_pos"]
    chg5 = tech["change_5d"]; chg1 = tech["change_1d"]
    vol  = tech["vol_ratio"]

    # RSI (최대 35점)
    if rsi <= 25:    s += 35
    elif rsi <= 30:  s += 28
    elif rsi <= 35:  s += 18
    elif rsi <= 40:  s +=  9
    elif rsi <= 50:  s +=  3
    elif rsi >= 75:  s -= 18
    elif rsi >= 65:  s -=  8

    # 볼린저 하단 (최대 30점)
    if bb <= 5:      s += 30
    elif bb <= 10:   s += 24
    elif bb <= 20:   s += 15
    elif bb <= 30:   s +=  7
    elif bb >= 90:   s -= 13
    elif bb >= 80:   s -=  6

    # 콤보 보너스: RSI+BB 동시 (최대 25점)
    if rsi <= 30 and bb <= 15:   s += 25
    elif rsi <= 35 and bb <= 25: s += 15
    elif rsi <= 40 and bb <= 35: s +=  8

    # 5일 낙폭 (최대 20점)
    if chg5 <= -10:  s += 20
    elif chg5 <= -7: s += 14
    elif chg5 <= -5: s +=  9
    elif chg5 <= -3: s +=  4
    elif chg5 >= 15: s -= 14
    elif chg5 >= 10: s -=  7

    # 거래량 투매 소진 (최대 15점)
    if vol >= 3.0 and chg1 < 0:   s += 15
    elif vol >= 2.0 and chg1 < 0: s += 10
    elif vol >= 3.0:               s +=  7
    elif vol >= 2.0:               s +=  5
    elif vol >= 1.5:               s +=  2

    # MA50 지지
    ma50 = tech.get("ma50")
    if ma50 and abs(tech["price"] - ma50) / ma50 < 0.03:
        has_oversold = (rsi <= 40 or bb <= 30 or chg5 <= -3)
        s += 5 if has_oversold else 1

    # 강세 다이버전스 (최대 15점)
    if tech.get("bullish_div"):
        s += 15

    # 양봉 (5점)
    if tech.get("bullish_candle") and chg5 < -3:
        s += 5

    # 섹터 유입 보너스 (ARIA key_inflows 기반, 약식)
    for sector, tks in SECTOR_POOLS.items():
        if ticker in tks:
            keywords = sector.lower().replace("/", " ").split()
            if any(k in inflows for k in keywords):
                s += 8   # ETF 상대강도 대신 단순 유입 보너스
            break

    return round(s, 1)


# ══════════════════════════════════════════════════════════════════
# Stage2: ARIA 레짐/섹터 보정 (jackal_hunter._stage2_aria_context 이식)
# ══════════════════════════════════════════════════════════════════

def _s2_boost(ticker: str, s1_score: float, aria: dict) -> float:
    """ARIA 레짐 + 섹터 유입/유출 보정."""
    regime   = aria.get("regime", "").lower()
    inflows  = " ".join(aria.get("key_inflows",  [])).lower()
    outflows = " ".join(aria.get("key_outflows", [])).lower()

    boost = 0
    if "선호" in regime:   boost += 8
    elif "회피" in regime: boost -= 5
    elif "혼조" in regime: boost += 2

    for sector, tickers in SECTOR_POOLS.items():
        if ticker in tickers:
            sl = sector.lower().replace("/", " ").split()
            if any(k in inflows  for k in sl): boost += 10
            if any(k in outflows for k in sl): boost -=  8
            break

    if ticker.endswith(".KS") and "회피" in regime:
        boost -= 5

    return round(s1_score + boost, 1)


# ══════════════════════════════════════════════════════════════════
# Stage4 근사 점수 (Analyst 없이 수치 기반)
# ══════════════════════════════════════════════════════════════════

def _s4_score(item: dict) -> float:
    """
    실운용 Stage4는 Claude Analyst+Devil.
    백테스트에서는 기술 지표 기반 근사 점수 사용.
    """
    t   = item["tech"]
    s   = item["s2_score"]
    rsi = t["rsi"]; bb = t["bb_pos"]
    vol = t["vol_ratio"]; chg1 = t["change_1d"]

    # 강한 콤보
    if rsi <= 30 and bb <= 10:   s += 20
    elif rsi <= 35 and bb <= 20: s += 12
    elif rsi <= 40 and bb <= 30: s +=  6

    # 강세 다이버전스
    if t.get("bullish_div"):     s += 15

    # 투매 소진 패턴
    if vol >= 2.0 and chg1 < -1: s +=  8

    return round(s, 1)


# ══════════════════════════════════════════════════════════════════
# 결과 추적
# ══════════════════════════════════════════════════════════════════

def track_peak(df: pd.DataFrame, signal_date: str,
               tracking_days: int = TRACKING_DAYS) -> dict:
    """신호 발생일 이후 TRACKING_DAYS일 결과 추적."""
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


def parse_aria_context(report: dict) -> dict:
    return {
        "regime":      report.get("market_regime", ""),
        "key_inflows": [i.get("zone", "") for i in report.get("inflows", [])[:3]],
        "key_outflows":[o.get("zone", "") for o in report.get("outflows", [])[:3]],
    }


# ══════════════════════════════════════════════════════════════════
# 데이터 로딩
# ══════════════════════════════════════════════════════════════════

def load_memory() -> list:
    if not MEMORY_FILE.exists():
        print(f"❌ memory.json 없음: {MEMORY_FILE}")
        sys.exit(1)

    mem = json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
    all_morning = sorted(
        [r for r in mem if r.get("mode") == "MORNING"],
        key=lambda r: r.get("analysis_date", "")
    )
    print(f"   MORNING 전체: {len(all_morning)}개")

    cutoff  = (datetime.now() - timedelta(days=BACKTEST_DAYS + TRACKING_DAYS)).strftime("%Y-%m-%d")
    morning = [r for r in all_morning if r.get("analysis_date", "") >= cutoff]
    if not morning:
        print(f"⚠️  cutoff({cutoff}) 이후 없음 → 전체 사용")
        morning = all_morning

    print(f"✅ 백테스트 대상: {len(morning)}개 | {morning[0]['analysis_date']} ~ {morning[-1]['analysis_date']}")
    return morning


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
    print("\n" + "=" * 62)
    print("  🦊 Jackal Backtest v2 — 실운용 파이프라인 반영")
    print(f"  파이프라인: Universe→Stage1(50)→Stage2(25)→Stage3(10)→Stage4(5)")
    print(f"  대상: 최근 {BACKTEST_DAYS}거래일 | Peak 추적: {TRACKING_DAYS}일")
    print("=" * 62)

    memory   = load_memory()
    universe = _build_universe()
    print(f"\n🌐 Universe: {len(universe)}종목 (SECTOR_POOLS, MY_PORTFOLIO 제외)")
    print(f"   제외: {', '.join(MY_PORTFOLIO)}")

    # ── yfinance 1회 전체 다운로드 ────────────────────────────────
    print(f"\n📥 yfinance 다운로드 ({len(universe)}종목)...")
    hist: dict = {}
    for i, ticker in enumerate(universe):
        df = _fetch_yf_cached(ticker)
        if df is not None:
            hist[ticker] = df
            print(f"  [{i+1:2}/{len(universe)}] {ticker}: {len(df)}일 ✅")
        else:
            print(f"  [{i+1:2}/{len(universe)}] {ticker}: 실패 ❌")
        time.sleep(0.05)
    print(f"\n   완료: {len(hist)}/{len(universe)}종목\n")

    # ── 날짜별 파이프라인 실행 ────────────────────────────────────
    all_results   = []
    funnel_totals = {"universe": 0, "s1_top50": 0, "s2_top25": 0,
                     "s3_top10": 0, "s4_top5":  0, "tracked": 0}

    print("=" * 62)
    print("  📅 날짜별 파이프라인 실행")
    print("=" * 62)

    for report in memory:
        date_str = report.get("analysis_date", "")
        aria     = parse_aria_context(report)
        inflows  = " ".join(aria["key_inflows"]).lower()

        # ── Stage1: Universe → Top50 (기술지표 점수) ─────────────
        scored = []
        for ticker in universe:
            df = hist.get(ticker)
            if df is None:
                continue
            tech = calc_indicators_hist(df, date_str)
            if not tech:
                continue
            s1 = _s1_score(tech, ticker, inflows)
            scored.append({
                "ticker":   ticker,
                "tech":     tech,
                "s1_score": s1,
                "market":   "KR" if ticker.endswith(".KS") else "US",
            })

        scored.sort(key=lambda x: x["s1_score"], reverse=True)
        top50 = scored[:50]
        funnel_totals["universe"]  += len(scored)
        funnel_totals["s1_top50"]  += len(top50)

        if not top50:
            continue

        # ── Stage2: Top50 → Top25 (ARIA 레짐/섹터 보정) ──────────
        for item in top50:
            item["s2_score"] = _s2_boost(item["ticker"], item["s1_score"], aria)
        top25 = sorted(top50, key=lambda x: x["s2_score"], reverse=True)[:25]
        funnel_totals["s2_top25"] += len(top25)

        # ── Stage3: Top25 → Top10 ─────────────────────────────────
        # 실운용: Claude Haiku (웹서치 없음)
        # 백테스트: s2_score 순위 그대로 유지 ($0)
        top10 = top25[:10]
        funnel_totals["s3_top10"] += len(top10)

        # ── Stage4: Top10 → Top5 ─────────────────────────────────
        # 실운용: Analyst+Devil (Claude)
        # 백테스트: 수치 기반 근사 점수 ($0)
        for item in top10:
            item["s4_score"] = _s4_score(item)
        top5 = sorted(top10, key=lambda x: x["s4_score"], reverse=True)[:5]
        funnel_totals["s4_top5"] += len(top5)

        # ── Top5 결과 추적 ────────────────────────────────────────
        for item in top5:
            ticker = item["ticker"]
            df = hist.get(ticker)
            if df is None:
                continue
            peak = track_peak(df, date_str)
            if peak["peak_day"] is None:
                continue

            funnel_totals["tracked"] += 1
            entry = {
                "date":        date_str,
                "ticker":      ticker,
                "regime":      aria["regime"],
                "s1_score":    item["s1_score"],
                "s2_score":    item["s2_score"],
                "s4_score":    item["s4_score"],
                "rsi":         item["tech"]["rsi"],
                "bb_pos":      item["tech"]["bb_pos"],
                "change_5d":   item["tech"]["change_5d"],
                "vol_ratio":   item["tech"]["vol_ratio"],
                "bullish_div": item["tech"].get("bullish_div", False),
                "d1_pct":      peak["d1_pct"],
                "d1_hit":      peak["d1_hit"],
                "peak_day":    peak["peak_day"],
                "peak_pct":    peak["peak_pct"],
                "swing_hit":   peak["swing_hit"],
            }
            all_results.append(entry)

            d1_str   = f"{peak['d1_pct']:+.1f}%" if peak["d1_pct"] is not None else "  N/A"
            div_mark = "★" if item["tech"].get("bullish_div") else " "
            print(
                f"  {date_str} | {ticker:<12}{div_mark}| "
                f"RSI:{item['tech']['rsi']:5.1f} BB:{item['tech']['bb_pos']:5.1f}% | "
                f"S4:{item['s4_score']:5.1f} | "
                f"1일:{d1_str}({'✅' if peak['d1_hit'] else '❌'}) "
                f"스윙 D{peak['peak_day']} {peak['peak_pct']:+.1f}%"
                f"({'✅' if peak['swing_hit'] else '❌'})"
            )

    # ══════════════════════════════════════════════════════════════
    # 집계 및 출력
    # ══════════════════════════════════════════════════════════════
    total = len(all_results)
    if total == 0:
        print("\n❌ 추적 가능한 결과 없음 (데이터 부족 가능)")
        return {}

    d1_judged  = [r for r in all_results if r.get("d1_hit") is not None]
    d1_ok      = sum(1 for r in d1_judged if r["d1_hit"])
    swing_ok   = sum(1 for r in all_results if r.get("swing_hit"))
    d1_acc     = d1_ok / len(d1_judged) * 100 if d1_judged else 0
    sw_acc     = swing_ok / total * 100 if total else 0

    div_results = [r for r in all_results if r.get("bullish_div")]
    div_sw_ok   = sum(1 for r in div_results if r.get("swing_hit"))
    div_acc     = div_sw_ok / len(div_results) * 100 if div_results else 0

    days = len(memory)
    print("\n" + "=" * 62)
    print("  📊 파이프라인 Funnel 요약")
    print("=" * 62)
    print(f"  분석 날짜       : {days}일")
    print(f"  Universe 평균   : {funnel_totals['universe']/days:.1f}종목/일")
    print(f"  Stage1 Top50    : {funnel_totals['s1_top50']/days:.1f}종목/일")
    print(f"  Stage2 Top25    : {funnel_totals['s2_top25']/days:.1f}종목/일")
    print(f"  Stage3 Top10    : {funnel_totals['s3_top10']/days:.1f}종목/일")
    print(f"  Stage4 Top5     : {funnel_totals['s4_top5']/days:.1f}종목/일")
    print(f"  추적 완료       : {funnel_totals['tracked']}건")

    print("\n" + "=" * 62)
    print("  📈 전체 적중률 (Top5 기준)")
    print("=" * 62)
    print(f"  총 추적         : {total}건")
    print(f"  1일 적중률      : {d1_acc:.1f}% ({d1_ok}/{len(d1_judged)})")
    print(f"  스윙 적중률     : {sw_acc:.1f}% ({swing_ok}/{total})")
    if div_results:
        print(f"  강세다이버전스  : {div_acc:.1f}% ({div_sw_ok}/{len(div_results)}) ★")

    # 레짐별
    print("\n" + "=" * 62)
    print("  🌐 레짐별 스윙 적중률")
    print("=" * 62)
    regime_map   = defaultdict(list)
    regime_stats = {}
    for r in all_results:
        regime_map[r["regime"][:35]].append(r)
    for reg, entries in sorted(regime_map.items(), key=lambda x: -len(x[1])):
        ok  = sum(1 for e in entries if e.get("swing_hit"))
        d1e = [e for e in entries if e.get("d1_hit") is not None]
        d1r = sum(1 for e in d1e if e["d1_hit"])
        d1a = d1r / len(d1e) * 100 if d1e else 0
        swa = ok / len(entries) * 100
        regime_stats[reg] = {
            "total":          len(entries),
            "swing_correct":  ok,
            "swing_accuracy": round(swa, 1),
            "d1_accuracy":    round(d1a, 1),
        }
        print(f"  {reg:<38} {len(entries):3}건 | 1일 {d1a:5.1f}% | 스윙 {swa:5.1f}%")

    # 종목별
    print("\n" + "=" * 62)
    print("  📊 종목별 (Top5 포함 빈도 + 적중률)")
    print("=" * 62)
    ticker_map   = defaultdict(list)
    ticker_stats = {}
    for r in all_results:
        ticker_map[r["ticker"]].append(r)
    for tk, entries in sorted(ticker_map.items(), key=lambda x: -len(x[1])):
        ok        = sum(1 for e in entries if e.get("swing_hit"))
        peak_days = [e["peak_day"] for e in entries if e.get("peak_day")]
        avg_d     = round(sum(peak_days) / len(peak_days), 1) if peak_days else 5.0
        avg_pk    = round(
            sum(e["peak_pct"] for e in entries if e.get("peak_pct") is not None) / len(entries), 2
        ) if entries else 0.0
        swa       = ok / len(entries) * 100
        ticker_stats[tk] = {
            "total":          len(entries),
            "swing_correct":  ok,
            "swing_accuracy": round(swa, 1),
            "avg_peak_day":   avg_d,
            "avg_peak_pct":   avg_pk,
        }
        print(f"  {tk:<14} {len(entries):3}건 | 스윙 {swa:5.1f}% | Peak D{avg_d:.1f} ({avg_pk:+.2f}%)")

    # ── jackal_weights.json 저장 ──────────────────────────────────
    existing = {}
    if OUTPUT_FILE.exists():
        try:
            existing = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass

    new_weights = {
        **existing,
        "last_backtest":        datetime.now().isoformat(),
        "backtest_version":     "v2_pipeline",
        "pipeline":             "Universe→Stage1(50)→Stage2(25)→Stage3(10)→Stage4(5)",
        "backtest_days":        len(memory),
        "total_tracked":        total,
        "d1_accuracy":          round(d1_acc, 1),
        "swing_accuracy":       round(sw_acc, 1),
        "bullish_div_accuracy": round(div_acc, 1),
        "regime_accuracy":      regime_stats,
        "ticker_accuracy":      ticker_stats,
        # 실운용 참조 필드 유지
        "alert_threshold":      55,
        "cooldown_hours":       6,
        "devil_weights":        {"동의": 1.1, "부분동의": 0.9, "반대": 0.6},
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(
        json.dumps(new_weights, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print("\n" + "=" * 62)
    print(f"  ✅ jackal_weights.json 저장 완료")
    print(f"     스윙 {sw_acc:.1f}% | 1일 {d1_acc:.1f}% | 다이버전스 {div_acc:.1f}%")
    print("=" * 62)

    return new_weights


if __name__ == "__main__":
    run_backtest()
