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
from datetime import datetime, timedelta, date
from pathlib import Path
from collections import defaultdict

os.environ["PYTHONIOENCODING"] = "utf-8"
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import yfinance as yf
import pandas as pd

# ── 경로 설정 ──────────────────────────────────────────────────────
_ROOT         = Path(__file__).parent.parent   # repo 루트
MEMORY_FILE   = _ROOT / "data" / "memory.json"
PORTFOLIO_FILE = _ROOT / "data" / "portfolio.json"
OUTPUT_FILE   = _ROOT / "jackal" / "jackal_weights.json"

# ── 감시 종목 (portfolio.json 없으면 기본값) ─────────────────────
DEFAULT_TICKERS = {
    "NVDA":      {"name": "엔비디아",   "market": "US"},
    "AVGO":      {"name": "브로드컴",   "market": "US"},
    "SCHD":      {"name": "SCHD",       "market": "US"},
    "000660.KS": {"name": "SK하이닉스", "market": "KR"},
    "005930.KS": {"name": "삼성전자",   "market": "KR"},
    "035720.KS": {"name": "카카오",     "market": "KR"},
}

# ── 신호 감지 임계값 ──────────────────────────────────────────────
SIGNAL_RULES = {
    "rsi_oversold":   lambda t: t["rsi"] < 32,
    "bb_touch":       lambda t: t["bb_pos"] < 15,
    "volume_climax":  lambda t: t["vol_ratio"] > 1.8 and t["change_1d"] < -1.0,
    "momentum_dip":   lambda t: t["change_5d"] < -4.0,
    "ma_support":     lambda t: (t["ma50"] is not None and
                                 abs(t["price"] - t["ma50"]) / t["ma50"] < 0.025),
    "sector_rebound": lambda t: t["rsi"] < 40 and t["change_3d"] < -2.0,
}

TRACKING_DAYS = 10   # 신호 발동 후 추적 기간
SUCCESS_PCT   = 0.5  # 이 % 이상이면 성공


# ══════════════════════════════════════════════════════════════════
# 데이터 로딩
# ══════════════════════════════════════════════════════════════════

def load_memory() -> list:
    if not MEMORY_FILE.exists():
        print(f"❌ memory.json 없음: {MEMORY_FILE}")
        sys.exit(1)
    mem = json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
    # MORNING만, 날짜순 정렬
    morning = [r for r in mem if r.get("mode") == "MORNING"]
    morning.sort(key=lambda r: r.get("analysis_date", ""))
    print(f"✅ memory.json: {len(morning)}개 MORNING 리포트 로드")
    return morning


def load_tickers() -> dict:
    if PORTFOLIO_FILE.exists():
        try:
            data = json.loads(PORTFOLIO_FILE.read_text(encoding="utf-8"))
            result = {}
            for h in data.get("holdings", []):
                yf_t = h.get("ticker_yf")
                if yf_t:
                    result[yf_t] = {
                        "name":   h.get("name", yf_t),
                        "market": h.get("market", "US"),
                    }
            if result:
                print(f"✅ portfolio.json: {len(result)}종목 로드")
                return result
        except Exception as e:
            print(f"⚠️  portfolio.json 로드 실패: {e} → 기본값 사용")
    return DEFAULT_TICKERS


def download_history(tickers: dict) -> dict:
    """
    각 종목 1년치 일봉 한 번에 다운로드.
    날짜별 슬라이싱으로 Look-ahead Bias 방지.
    """
    hist = {}
    print(f"\n📥 yfinance 다운로드 ({len(tickers)}종목)...")
    for ticker in tickers:
        for attempt in range(3):
            try:
                df = yf.Ticker(ticker).history(period="1y", interval="1d")
                if df.empty:
                    print(f"  {ticker}: 데이터 없음")
                    break
                # timezone 제거
                df.index = pd.to_datetime(df.index).tz_localize(None)
                hist[ticker] = df
                print(f"  {ticker}: {len(df)}일 ✅")
                time.sleep(0.3)   # rate limit 방지
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(2)
                else:
                    print(f"  {ticker}: 실패 ({e})")
    return hist


# ══════════════════════════════════════════════════════════════════
# 기술 지표 계산 (특정 날짜 기준)
# ══════════════════════════════════════════════════════════════════

def calc_indicators(df: pd.DataFrame, as_of: str) -> dict | None:
    """
    as_of 날짜 기준으로 기술 지표 계산.
    as_of 이후 데이터는 사용 안 함 (Look-ahead Bias 방지).
    """
    cutoff = pd.Timestamp(as_of)
    sub    = df[df.index <= cutoff].copy()

    if len(sub) < 22:
        return None

    close  = sub["Close"]
    volume = sub["Volume"]
    price  = float(close.iloc[-1])

    # RSI 14
    delta  = close.diff()
    gain   = delta.clip(lower=0).rolling(14).mean()
    loss   = (-delta.clip(upper=0)).rolling(14).mean()
    rs     = gain / loss.replace(0, float("nan"))
    rsi    = float((100 - 100 / (1 + rs)).iloc[-1])

    # MA
    ma20  = float(close.rolling(20).mean().iloc[-1])
    ma50  = float(close.rolling(50).mean().iloc[-1]) if len(close) >= 50 else None

    # 볼린저
    std20  = float(close.rolling(20).std().iloc[-1])
    bb_pos = (price - (ma20 - 2*std20)) / (4*std20) * 100 if std20 > 0 else 50

    # 거래량
    avg_vol   = float(volume.iloc[-6:-1].mean()) if len(volume) >= 6 else float(volume.mean())
    vol_ratio = round(float(volume.iloc[-1]) / avg_vol, 2) if avg_vol > 0 else 1.0

    def chg(n):
        if len(close) > n:
            return round((price - float(close.iloc[-n-1])) / float(close.iloc[-n-1]) * 100, 2)
        return 0.0

    return {
        "price":     round(price, 4),
        "change_1d": chg(1),
        "change_3d": chg(3),
        "change_5d": chg(5),
        "rsi":       round(rsi, 1),
        "ma20":      round(ma20, 4),
        "ma50":      round(ma50, 4) if ma50 else None,
        "bb_pos":    round(bb_pos, 1),
        "vol_ratio": vol_ratio,
    }


# ══════════════════════════════════════════════════════════════════
# Peak Detection
# ══════════════════════════════════════════════════════════════════

def track_peak(df: pd.DataFrame, signal_date: str,
               tracking_days: int = TRACKING_DAYS) -> dict:
    """
    signal_date 다음 거래일부터 tracking_days일 추적.
    최대 수익률(Peak)과 그 날짜 반환.
    """
    cutoff    = pd.Timestamp(signal_date)
    future    = df[df.index > cutoff].copy()
    future    = future.iloc[:tracking_days]

    if future.empty:
        return {"peak_day": None, "peak_pct": None, "final_pct": None,
                "daily_returns": []}

    entry     = float(df[df.index <= cutoff]["Close"].iloc[-1])
    returns   = [(float(r) - entry) / entry * 100 for r in future["Close"]]
    peak_idx  = returns.index(max(returns))
    peak_pct  = round(max(returns), 2)
    final_pct = round(returns[-1], 2) if returns else None

    return {
        "peak_day":      peak_idx + 1,     # 1-indexed
        "peak_pct":      peak_pct,
        "final_pct":     final_pct,
        "daily_returns": [round(r, 2) for r in returns],
    }


# ══════════════════════════════════════════════════════════════════
# ARIA 컨텍스트 파싱
# ══════════════════════════════════════════════════════════════════

def parse_aria_context(report: dict) -> dict:
    return {
        "regime":    report.get("market_regime", ""),
        "trend":     report.get("trend_phase", ""),
        "inflows":   [i.get("zone", "") for i in report.get("inflows", [])[:3]],
        "outflows":  [o.get("zone", "") for o in report.get("outflows", [])[:3]],
        "confidence": report.get("confidence_overall", ""),
    }


# ══════════════════════════════════════════════════════════════════
# 메인 백테스트
# ══════════════════════════════════════════════════════════════════

def run_backtest():
    print("\n" + "=" * 58)
    print("  🦊 Jackal Backtest — Step B")
    print("  memory.json 기반 초기 weights 생성")
    print("=" * 58)

    memory  = load_memory()
    tickers = load_tickers()
    hist    = download_history(tickers)

    if not hist:
        print("❌ 가격 데이터 없음")
        sys.exit(1)

    # ── 결과 수집 구조 ────────────────────────────────────────────
    # signal_results[signal_name] = list of {peak_day, peak_pct, correct, regime, ticker}
    signal_results  = defaultdict(list)
    regime_results  = defaultdict(list)
    ticker_results  = defaultdict(list)

    total_signals = 0
    total_days    = 0

    print(f"\n📊 {len(memory)}개 거래일 분석 시작...\n")

    for report in memory:
        date_str = report.get("analysis_date", "")
        aria     = parse_aria_context(report)
        total_days += 1

        for ticker, info in tickers.items():
            df = hist.get(ticker)
            if df is None:
                continue

            # 해당 날짜 기술 지표 계산
            tech = calc_indicators(df, date_str)
            if not tech:
                continue

            # 신호 감지
            fired = [sig for sig, rule in SIGNAL_RULES.items() if rule(tech)]
            if not fired:
                continue

            # Peak Detection
            peak = track_peak(df, date_str)
            if peak["peak_day"] is None:
                continue

            correct = (peak["peak_pct"] or 0) >= SUCCESS_PCT
            total_signals += 1

            entry = {
                "date":       date_str,
                "ticker":     ticker,
                "regime":     aria["regime"],
                "trend":      aria["trend"],
                "peak_day":   peak["peak_day"],
                "peak_pct":   peak["peak_pct"],
                "final_pct":  peak["final_pct"],
                "correct":    correct,
                "signals":    fired,
                "rsi":        tech["rsi"],
                "bb_pos":     tech["bb_pos"],
                "vol_ratio":  tech["vol_ratio"],
                "change_5d":  tech["change_5d"],
            }

            # 신호별 기록
            for sig in fired:
                signal_results[sig].append(entry)

            # 레짐별 기록
            regime_key = aria["regime"][:30] if aria["regime"] else "unknown"
            regime_results[regime_key].append(entry)

            # 티커별 기록
            ticker_results[ticker].append(entry)

            print(
                f"  {date_str} | {ticker:12} | "
                f"RSI:{tech['rsi']:5.1f} BB:{tech['bb_pos']:5.1f}% | "
                f"신호:{','.join(fired)} | "
                f"Peak D{peak['peak_day']} {peak['peak_pct']:+.1f}% "
                f"{'✅' if correct else '❌'}"
            )

    print(f"\n총 신호 발동: {total_signals}건 / {total_days}거래일")

    # ── 집계 ──────────────────────────────────────────────────────
    print("\n" + "=" * 58)
    print("  📈 신호별 분석 결과")
    print("=" * 58)

    signal_stats     = {}
    signal_opt_days  = {}
    signal_weights   = {}

    for sig, results in signal_results.items():
        if not results:
            continue
        total    = len(results)
        correct  = sum(1 for r in results if r["correct"])
        accuracy = round(correct / total * 100, 1)

        peak_days  = [r["peak_day"] for r in results if r["peak_day"] is not None]
        peak_gains = [r["peak_pct"] for r in results if r["peak_pct"] is not None]

        avg_peak_day  = round(sum(peak_days) / len(peak_days), 1) if peak_days else 5
        avg_peak_gain = round(sum(peak_gains) / len(peak_gains), 2) if peak_gains else 0

        # 가중치 계산: 정확도 60% 이상이면 1.0 초과, 40% 미만이면 감소
        weight = round(max(0.3, min(2.5, 0.5 + accuracy / 100 * 2)), 3)

        signal_stats[sig]    = {
            "total":    total,
            "correct":  correct,
            "accuracy": accuracy,
        }
        signal_opt_days[sig] = {
            "avg_peak_day":  avg_peak_day,
            "avg_peak_gain": avg_peak_gain,
            "sample_count":  total,
        }
        signal_weights[sig]  = weight

        print(
            f"  {sig:<20} | {total:3}건 | 정확도 {accuracy:5.1f}% | "
            f"평균 Peak D{avg_peak_day:4.1f} | 평균수익 {avg_peak_gain:+.2f}% | "
            f"가중치 {weight:.3f}"
        )

    # ── 레짐별 ────────────────────────────────────────────────────
    print("\n" + "=" * 58)
    print("  🌐 레짐별 신호 적중률")
    print("=" * 58)

    regime_stats = {}
    for reg, results in regime_results.items():
        if not results:
            continue
        total    = len(results)
        correct  = sum(1 for r in results if r["correct"])
        accuracy = round(correct / total * 100, 1)
        avg_peak = round(sum(r["peak_pct"] for r in results
                             if r["peak_pct"] is not None) / total, 2)
        regime_stats[reg] = {
            "total":    total,
            "correct":  correct,
            "accuracy": accuracy,
            "avg_peak": avg_peak,
        }
        print(f"  {reg[:35]:<35} | {total:3}건 | {accuracy:5.1f}% | 평균수익 {avg_peak:+.2f}%")

    # ── 티커별 ────────────────────────────────────────────────────
    print("\n" + "=" * 58)
    print("  📊 종목별 신호 적중률")
    print("=" * 58)

    ticker_stats = {}
    for tk, results in ticker_results.items():
        if not results:
            continue
        total    = len(results)
        correct  = sum(1 for r in results if r["correct"])
        accuracy = round(correct / total * 100, 1)
        peak_days = [r["peak_day"] for r in results if r["peak_day"] is not None]
        avg_peak_day = round(sum(peak_days) / len(peak_days), 1) if peak_days else 5
        ticker_stats[tk] = {
            "total":         total,
            "correct":       correct,
            "accuracy":      accuracy,
            "avg_peak_day":  avg_peak_day,
        }
        name = tickers.get(tk, {}).get("name", tk)
        print(f"  {name:<12} ({tk:<12}) | {total:3}건 | {accuracy:5.1f}% | 평균 D{avg_peak_day:.1f}")

    # ══════════════════════════════════════════════════════════════
    # jackal_weights.json 저장
    # ══════════════════════════════════════════════════════════════

    # 기존 파일 로드 (있으면 병합)
    existing = {}
    if OUTPUT_FILE.exists():
        try:
            existing = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass

    new_weights = {
        **existing,
        "last_backtest":      datetime.now().isoformat(),
        "backtest_days":      total_days,
        "backtest_signals":   total_signals,
        "signal_weights":     signal_weights,
        "signal_accuracy":    signal_stats,
        "signal_optimal_days": signal_opt_days,
        "regime_accuracy":    regime_stats,
        "ticker_accuracy":    ticker_stats,
        "alert_threshold":    65,
        "cooldown_hours":     4,
        "devil_weights": {
            "동의":     1.1,
            "부분동의": 0.9,
            "반대":     0.6,
        },
    }

    OUTPUT_FILE.parent.mkdir(exist_ok=True)
    OUTPUT_FILE.write_text(
        json.dumps(new_weights, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print("\n" + "=" * 58)
    print("  ✅ jackal_weights.json 저장 완료")
    print(f"  신호별 가중치: {len(signal_weights)}개")
    print(f"  Peak 최적 보유 기간:")
    for sig, opt in signal_opt_days.items():
        print(f"    {sig:<20}: 평균 D{opt['avg_peak_day']} ({opt['avg_peak_gain']:+.2f}%)")
    print("=" * 58)

    return new_weights


if __name__ == "__main__":
    run_backtest()
