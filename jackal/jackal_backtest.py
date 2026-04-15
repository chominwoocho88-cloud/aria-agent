"""
jackal_backtest.py — Jackal 초기 학습 (Step B)

Look-ahead Bias 없는 방식:
  1. memory.json (63일 실제 ARIA 분석) 읽기
  2. yfinance 전체 히스토리 1회 다운로드 → 날짜별 슬라이싱
  3. 각 날짜에서 기술 신호 감지 (RSI/BB/거래량)
  4. 신호 발동일부터 10거래일 추적 → Peak Detection
  5. 신호별 정확도 + 최적 보유 기간 계산
  6. jackal_weights.json 초기화

비용: $0 (Claude 없음)
소요: ~3분 (yfinance 다운로드)
"""

import sys
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

os.environ["PYTHONIOENCODING"] = "utf-8"
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import yfinance as yf
import pandas as pd

_ROOT          = Path(__file__).parent.parent
MEMORY_FILE    = _ROOT / "data" / "memory.json"
PORTFOLIO_FILE = _ROOT / "data" / "portfolio.json"
OUTPUT_FILE    = _ROOT / "jackal" / "jackal_weights.json"

DEFAULT_TICKERS = {
    "NVDA":      {"name": "엔비디아",   "market": "US"},
    "AVGO":      {"name": "브로드컴",   "market": "US"},
    "SCHD":      {"name": "SCHD",       "market": "US"},
    "000660.KS": {"name": "SK하이닉스", "market": "KR"},
    "005930.KS": {"name": "삼성전자",   "market": "KR"},
    "035720.KS": {"name": "카카오",     "market": "KR"},
}

# 기본 신호 임계값
SIGNAL_RULES_STRICT = {
    "rsi_oversold":    lambda t: t["rsi"] < 32,
    "bb_touch":        lambda t: t["bb_pos"] < 15,
    "volume_climax":   lambda t: t["vol_ratio"] > 1.8 and t["change_1d"] < -1.0,
    "momentum_dip":    lambda t: t["change_5d"] < -4.0,
    "ma_support":      lambda t: (t["ma50"] is not None and abs(t["price"] - t["ma50"]) / t["ma50"] < 0.025),
    "sector_rebound":  lambda t: t["rsi"] < 40 and t.get("change_3d", t.get("change_5d", 0)) < -2.0,
    # 신규 신호 (calc_indicators에서 계산)
    "rsi_divergence":  lambda t: t.get("rsi_divergence", False),
    "52w_low_zone":    lambda t: t.get("52w_pos", 50) < 15,
    "vol_accumulation":lambda t: t.get("vol_accumulation", False),
}

# 완화 신호 임계값 (신호 0건 시 자동 전환)
SIGNAL_RULES_RELAXED = {
    "rsi_oversold":    lambda t: t["rsi"] < 40,
    "bb_touch":        lambda t: t["bb_pos"] < 25,
    "volume_climax":   lambda t: t["vol_ratio"] > 1.5 and t["change_1d"] < -0.5,
    "momentum_dip":    lambda t: t["change_5d"] < -2.0,
    "ma_support":      lambda t: (t["ma50"] is not None and abs(t["price"] - t["ma50"]) / t["ma50"] < 0.04),
    "sector_rebound":  lambda t: t["rsi"] < 45 and t.get("change_3d", t.get("change_5d", 0)) < -1.0,
    "rsi_divergence":  lambda t: t.get("rsi_divergence", False),
    "52w_low_zone":    lambda t: t.get("52w_pos", 50) < 20,
    "vol_accumulation":lambda t: t.get("vol_accumulation", False),
}

TRACKING_DAYS = 10
BACKTEST_DAYS = 60   # 수정: 30 → 60 (ARIA와 동일 기간)


def load_memory() -> list:
    if not MEMORY_FILE.exists():
        print(f"❌ memory.json 없음: {MEMORY_FILE}")
        sys.exit(1)
    mem = json.loads(MEMORY_FILE.read_text(encoding="utf-8"))

    all_morning = [r for r in mem if r.get("mode") == "MORNING"]
    all_morning.sort(key=lambda r: r.get("analysis_date", ""))
    print(f"   memory.json 전체 MORNING: {len(all_morning)}개")
    if all_morning:
        print(f"   전체 기간: {all_morning[0]['analysis_date']} ~ {all_morning[-1]['analysis_date']}")

    # cutoff: BACKTEST_DAYS + TRACKING_DAYS 전부터
    # end 필터 제거 → 최근 데이터도 포함 (Peak None이면 track_peak에서 자동 스킵)
    cutoff = (datetime.now() - timedelta(days=BACKTEST_DAYS + TRACKING_DAYS)).strftime("%Y-%m-%d")
    morning = [r for r in all_morning if r.get("analysis_date", "") >= cutoff]

    if not morning:
        print(f"⚠️  cutoff({cutoff}) 이후 데이터 없음 → 전체 사용")
        morning = all_morning

    print(f"✅ 백테스트 대상: {len(morning)}개 MORNING")
    print(f"   기간: {morning[0]['analysis_date']} ~ {morning[-1]['analysis_date']}")
    return morning


def load_tickers() -> dict:
    if PORTFOLIO_FILE.exists():
        try:
            data = json.loads(PORTFOLIO_FILE.read_text(encoding="utf-8"))
            result = {}
            for h in data.get("holdings", []):
                yf_t = h.get("ticker_yf")
                if yf_t:
                    result[yf_t] = {"name": h.get("name", yf_t), "market": h.get("market", "US")}
            if result:
                print(f"✅ portfolio.json: {len(result)}종목")
                return result
        except Exception as e:
            print(f"⚠️  portfolio.json 실패: {e}")
    return DEFAULT_TICKERS


def download_history(tickers: dict) -> dict:
    hist = {}
    print(f"\n📥 yfinance 다운로드 ({len(tickers)}종목)...")
    for ticker in tickers:
        for attempt in range(3):
            try:
                df = yf.Ticker(ticker).history(period="1y", interval="1d")
                if df.empty:
                    print(f"  {ticker}: 데이터 없음")
                    break
                df.index = pd.to_datetime(df.index).tz_localize(None)
                hist[ticker] = df
                print(f"  {ticker}: {len(df)}일 ✅")
                time.sleep(0.3)
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(2)
                else:
                    print(f"  {ticker}: 실패 ({e})")
    return hist


def calc_indicators(df: pd.DataFrame, as_of: str) -> dict | None:
    cutoff = pd.Timestamp(as_of)
    sub    = df[df.index <= cutoff].copy()
    if len(sub) < 22:
        return None

    close  = sub["Close"]
    volume = sub["Volume"]
    price  = float(close.iloc[-1])

    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    rs    = gain / loss.replace(0, float("nan"))
    rsi   = float((100 - 100 / (1 + rs)).iloc[-1])

    ma20 = float(close.rolling(20).mean().iloc[-1])
    ma50 = float(close.rolling(50).mean().iloc[-1]) if len(close) >= 50 else None
    std20 = float(close.rolling(20).std().iloc[-1])
    bb_pos = (price - (ma20 - 2*std20)) / (4*std20) * 100 if std20 > 0 else 50

    avg_vol   = float(volume.iloc[-6:-1].mean()) if len(volume) >= 6 else float(volume.mean())
    vol_ratio = round(float(volume.iloc[-1]) / avg_vol, 2) if avg_vol > 0 else 1.0

    def chg(n):
        if len(close) > n:
            return round((price - float(close.iloc[-n-1])) / float(close.iloc[-n-1]) * 100, 2)
        return 0.0

    # 신규 피처 계산
    # RSI 강세 다이버전스
    rsi_series = 100 - 100 / (1 + gain / loss)
    rsi_div = False
    if len(close) >= 7 and chg(5) < -1.5:
        rsi_5d = float(rsi_series.iloc[-6]) if len(rsi_series) >= 6 else float(rsi_series.iloc[0])
        p5d    = float(close.iloc[-6]) if len(close) >= 6 else float(close.iloc[0])
        if price < p5d and round(rsi, 1) > rsi_5d + 2:
            rsi_div = True

    # 52주 위치
    high52 = float(close.rolling(252).max().iloc[-1]) if len(close) >= 50 else float(close.max())
    low52  = float(close.rolling(252).min().iloc[-1]) if len(close) >= 50 else float(close.min())
    pos52  = round((price - low52) / (high52 - low52) * 100, 1) if high52 > low52 else 50.0

    # 거래량 추세 + 매집
    vol_trend = 0.0
    if len(volume) >= 10:
        vr = float(volume.iloc[-5:].mean())
        vp = float(volume.iloc[-10:-5].mean())
        vol_trend = round((vr - vp) / vp * 100, 1) if vp > 0 else 0.0
    vol_acc = chg(5) < -2.0 and vol_trend > 15

    return {
        "price":           round(price, 4),
        "change_1d":       chg(1),
        "change_3d":       chg(3),
        "change_5d":       chg(5),
        "rsi":             round(rsi, 1),
        "ma20":            round(ma20, 4),
        "ma50":            round(ma50, 4) if ma50 else None,
        "bb_pos":          round(bb_pos, 1),
        "vol_ratio":       vol_ratio,
        "rsi_divergence":  rsi_div,
        "52w_pos":         pos52,
        "vol_trend_5d":    vol_trend,
        "vol_accumulation": vol_acc,
    }


def track_peak(df: pd.DataFrame, signal_date: str, tracking_days: int = TRACKING_DAYS) -> dict:
    cutoff = pd.Timestamp(signal_date)
    future = df[df.index > cutoff].iloc[:tracking_days].copy()

    if future.empty:
        return {"peak_day": None, "peak_pct": None, "final_pct": None,
                "daily_returns": [], "d1_pct": None, "d1_hit": None, "swing_hit": None}

    entry   = float(df[df.index <= cutoff]["Close"].iloc[-1])
    returns = [(float(r) - entry) / entry * 100 for r in future["Close"]]

    d1_pct = round(returns[0], 2) if returns else None
    d1_hit = (d1_pct > 0.3) if d1_pct is not None else None

    swing_window = returns[:7]
    peak_pct     = round(max(swing_window), 2) if swing_window else 0
    peak_idx     = swing_window.index(max(swing_window)) if swing_window else 0
    swing_hit    = peak_pct >= 1.0
    final_pct    = round(returns[-1], 2) if returns else None

    return {
        "peak_day": peak_idx + 1, "peak_pct": peak_pct, "final_pct": final_pct,
        "daily_returns": [round(r, 2) for r in returns],
        "d1_pct": d1_pct, "d1_hit": d1_hit, "swing_hit": swing_hit,
    }


def parse_aria_context(report: dict) -> dict:
    return {
        "regime":     report.get("market_regime", ""),
        "trend":      report.get("trend_phase", ""),
        "inflows":    [i.get("zone", "") for i in report.get("inflows", [])[:3]],
        "outflows":   [o.get("zone", "") for o in report.get("outflows", [])[:3]],
        "confidence": report.get("confidence_overall", ""),
    }


def _run_signals(memory, tickers, hist, signal_rules, label=""):
    signal_results = defaultdict(list)
    regime_results = defaultdict(list)
    ticker_results = defaultdict(list)
    total_signals  = 0
    skipped        = 0

    for report in memory:
        date_str = report.get("analysis_date", "")
        aria     = parse_aria_context(report)

        for ticker, info in tickers.items():
            df = hist.get(ticker)
            if df is None:
                continue
            tech = calc_indicators(df, date_str)
            if not tech:
                continue
            fired = [sig for sig, rule in signal_rules.items() if rule(tech)]
            if not fired:
                continue
            peak = track_peak(df, date_str)
            if peak["peak_day"] is None:
                skipped += 1
                continue

            swing_hit = peak.get("swing_hit", False)
            d1_hit    = peak.get("d1_hit")
            entry = {
                "date": date_str, "ticker": ticker,
                "regime": aria["regime"], "trend": aria["trend"],
                "peak_day": peak["peak_day"], "peak_pct": peak["peak_pct"],
                "final_pct": peak["final_pct"],
                "d1_pct": peak.get("d1_pct"), "d1_hit": d1_hit,
                "swing_hit": swing_hit, "correct": swing_hit,
                "signals": fired, "rsi": tech["rsi"],
                "bb_pos": tech["bb_pos"], "vol_ratio": tech["vol_ratio"],
                "change_5d": tech["change_5d"],
            }
            for sig in fired:
                signal_results[sig].append(entry)
            regime_key = aria["regime"][:30] if aria["regime"] else "unknown"
            regime_results[regime_key].append(entry)
            ticker_results[ticker].append(entry)
            total_signals += 1

            d1_str = f"{peak.get('d1_pct', 0):+.1f}%" if peak.get('d1_pct') is not None else "N/A"
            print(
                f"  {date_str} | {ticker:12} | RSI:{tech['rsi']:5.1f} BB:{tech['bb_pos']:5.1f}% | "
                f"신호:{','.join(fired)} | "
                f"1일:{d1_str}({'✅' if d1_hit else '❌'}) "
                f"스윙 D{peak['peak_day']} {peak['peak_pct']:+.1f}%({'✅' if swing_hit else '❌'})"
            )

    return signal_results, regime_results, ticker_results, total_signals, skipped


def run_backtest():
    print("\n" + "=" * 60)
    print("  🦊 Jackal Backtest — Step B")
    print(f"  대상: 최근 {BACKTEST_DAYS}거래일 | Peak 추적: {TRACKING_DAYS}일")
    print("=" * 60)

    memory  = load_memory()
    tickers = load_tickers()
    hist    = download_history(tickers)

    if not hist:
        print("❌ 가격 데이터 없음")
        sys.exit(1)

    total_days = len(memory)
    print(f"\n📊 {total_days}개 거래일 분석 시작...\n")

    # 1차: 엄격한 임계값
    signal_results, regime_results, ticker_results, total_signals, skipped = \
        _run_signals(memory, tickers, hist, SIGNAL_RULES_STRICT, "엄격")

    print(f"\n총 신호 발동: {total_signals}건 / {total_days}거래일 (Peak 추적불가: {skipped}건)")

    # 신호 0건이면 완화 임계값으로 재시도
    if total_signals == 0:
        print("\n⚠️  신호 0건 → 완화 임계값으로 재시도\n")
        signal_results, regime_results, ticker_results, total_signals, skipped = \
            _run_signals(memory, tickers, hist, SIGNAL_RULES_RELAXED, "완화")
        print(f"\n완화 후 총 신호: {total_signals}건")

    # ── 집계 ──────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  📈 신호별 분석 결과")
    print("=" * 60)

    signal_stats = {}
    signal_opt_days = {}
    signal_weights = {}

    for sig, results in signal_results.items():
        if not results:
            continue
        total     = len(results)
        swing_ok  = sum(1 for r in results if r.get("swing_hit", r["correct"]))
        swing_acc = round(swing_ok / total * 100, 1)
        d1_judged = [r for r in results if r.get("d1_hit") is not None]
        d1_ok     = sum(1 for r in d1_judged if r.get("d1_hit"))
        d1_acc    = round(d1_ok / len(d1_judged) * 100, 1) if d1_judged else 0

        peak_days  = [r["peak_day"] for r in results if r["peak_day"] is not None]
        peak_gains = [r["peak_pct"] for r in results if r["peak_pct"] is not None]
        avg_peak_day  = round(sum(peak_days) / len(peak_days), 1) if peak_days else 5
        avg_peak_gain = round(sum(peak_gains) / len(peak_gains), 2) if peak_gains else 0

        weight = round(max(0.3, min(2.5, 0.5 + swing_acc / 100 * 2)), 3)

        signal_stats[sig]    = {"total": total, "swing_correct": swing_ok,
                                 "swing_accuracy": swing_acc, "d1_correct": d1_ok, "d1_accuracy": d1_acc}
        signal_opt_days[sig] = {"avg_peak_day": avg_peak_day, "avg_peak_gain": avg_peak_gain,
                                 "swing_accuracy": swing_acc, "day1_accuracy": d1_acc, "sample_count": total}
        signal_weights[sig]  = weight

        print(f"  {sig:<20} | {total:3}건 | 1일 {d1_acc:5.1f}% | 스윙 {swing_acc:5.1f}% | "
              f"Peak D{avg_peak_day:4.1f} {avg_peak_gain:+.2f}% | 가중치 {weight:.3f}")

    # ── 레짐별 ────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  🌐 레짐별 신호 적중률")
    print("=" * 60)

    regime_stats = {}
    for reg, results in regime_results.items():
        if not results:
            continue
        total    = len(results)
        correct  = sum(1 for r in results if r["correct"])
        accuracy = round(correct / total * 100, 1)
        avg_peak = round(sum(r["peak_pct"] for r in results if r["peak_pct"] is not None) / total, 2)
        regime_stats[reg] = {"total": total, "correct": correct, "accuracy": accuracy, "avg_peak": avg_peak}
        print(f"  {reg[:35]:<35} | {total:3}건 | {accuracy:5.1f}% | 평균수익 {avg_peak:+.2f}%")

    # ── 티커별 ────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  📊 종목별 신호 적중률")
    print("=" * 60)

    ticker_stats = {}
    for tk, results in ticker_results.items():
        if not results:
            continue
        total    = len(results)
        correct  = sum(1 for r in results if r["correct"])
        accuracy = round(correct / total * 100, 1)
        peak_days    = [r["peak_day"] for r in results if r["peak_day"] is not None]
        avg_peak_day = round(sum(peak_days) / len(peak_days), 1) if peak_days else 5
        ticker_stats[tk] = {"total": total, "correct": correct, "accuracy": accuracy, "avg_peak_day": avg_peak_day}
        name = tickers.get(tk, {}).get("name", tk)
        print(f"  {name:<12} ({tk:<12}) | {total:3}건 | {accuracy:5.1f}% | 평균 D{avg_peak_day:.1f}")

    # ── 저장 ──────────────────────────────────────────────────────
    existing = {}
    if OUTPUT_FILE.exists():
        try:
            existing = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass

    new_weights = {
        **existing,
        "last_backtest":       datetime.now().isoformat(),
        "backtest_days":       total_days,
        "backtest_signals":    total_signals,
        "signal_weights":      signal_weights,
        "signal_accuracy":     signal_stats,
        "signal_optimal_days": signal_opt_days,
        "regime_accuracy":     regime_stats,
        "ticker_accuracy":     ticker_stats,
        "alert_threshold":     65,
        "cooldown_hours":      4,
        "devil_weights":       {"동의": 1.1, "부분동의": 0.9, "반대": 0.6},
    }

    OUTPUT_FILE.parent.mkdir(exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(new_weights, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n" + "=" * 60)
    print(f"  ✅ jackal_weights.json 저장 완료")
    print(f"  신호 발동: {total_signals}건 | 가중치: {len(signal_weights)}개")
    if signal_opt_days:
        print("  Peak 최적 보유 기간:")
        for sig, opt in signal_opt_days.items():
            print(f"    {sig:<20}: D{opt['avg_peak_day']} ({opt['avg_peak_gain']:+.2f}%) 스윙{opt['swing_accuracy']:.0f}%")
    print("=" * 60)

    return new_weights


if __name__ == "__main__":
    run_backtest()
