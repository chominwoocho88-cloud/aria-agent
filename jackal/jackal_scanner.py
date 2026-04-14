"""
jackal_scanner.py
Jackal Scanner — 매시간 종목 타점 계산 + 텔레그램 알림

동작:
  1. 시장 개장 여부 확인 (한국장 / 미국장)
  2. yfinance 실시간 데이터 수집
  3. 기술적 신호 계산 (RSI / MA크로스 / 볼린저 / 거래량 / MA지지)
  4. jackal_weights.json 의 signal_weights 로 신호별 가중치 적용
  5. data/sentiment.json 센티먼트 보정
  6. 최종 점수 ≥ 임계치 → 텔레그램 발송
  7. scan_log.json 에 결과 저장 (Evolution 학습용)
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx
import yfinance as yf

os.environ["PYTHONIOENCODING"] = "utf-8"
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

log = logging.getLogger("jackal_scanner")

KST = timezone(timedelta(hours=9))

# ─── 경로 ─────────────────────────────────────────────────────────
_BASE          = Path(__file__).parent
WEIGHTS_FILE   = _BASE / "jackal_weights.json"
SCAN_LOG_FILE  = _BASE / "scan_log.json"
COOLDOWN_FILE  = _BASE / "scan_cooldown.json"
SENTIMENT_FILE = Path("data") / "sentiment.json"

# ─── 텔레그램 ──────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ─── 감시 종목 ─────────────────────────────────────────────────────
WATCHLIST = {
    "NVDA":      {"name": "엔비디아",   "avg_cost": 182.99, "market": "US", "currency": "$"},
    "AVGO":      {"name": "브로드컴",   "avg_cost": None,   "market": "US", "currency": "$"},
    "SCHD":      {"name": "SCHD",       "avg_cost": None,   "market": "US", "currency": "$"},
    "000660.KS": {"name": "SK하이닉스", "avg_cost": None,   "market": "KR", "currency": "₩"},
    "005930.KS": {"name": "삼성전자",   "avg_cost": None,   "market": "KR", "currency": "₩"},
    "035720.KS": {"name": "카카오",     "avg_cost": None,   "market": "KR", "currency": "₩"},
}

# ─── 기본 임계치 ───────────────────────────────────────────────────
SIGNAL_THRESHOLD = 65
STRONG_THRESHOLD = 78
COOLDOWN_HOURS   = 4


# ══════════════════════════════════════════════════════════════════
# 시장 개장 여부
# ══════════════════════════════════════════════════════════════════

def is_us_open() -> bool:
    from datetime import time as _t
    now = datetime.now(timezone(timedelta(hours=-5)))
    return now.weekday() < 5 and _t(9, 30) <= now.time() <= _t(16, 0)


def is_kr_open() -> bool:
    from datetime import time as _t
    now = datetime.now(KST)
    return now.weekday() < 5 and _t(9, 0) <= now.time() <= _t(15, 30)


# ══════════════════════════════════════════════════════════════════
# 가중치 로드
# ══════════════════════════════════════════════════════════════════

def load_signal_weights() -> dict:
    """jackal_weights.json 에서 signal_weights 로드 (없으면 기본값 1.0)"""
    default = {
        "rsi_extreme":    1.0,   # RSI 극단 과매도
        "rsi_oversold":   1.0,   # RSI 과매도권
        "golden_cross":   1.0,   # MA20 골든크로스
        "dead_cross":     1.0,   # MA20 데드크로스 (감점)
        "bb_touch":       1.0,   # 볼린저 하단 터치
        "bb_near":        1.0,   # 볼린저 하단 근접
        "bb_upper":       1.0,   # 볼린저 상단 과확장 (감점)
        "volume_surge":   1.0,   # 거래량 급증
        "volume_rise":    1.0,   # 거래량 증가
        "ma20_support":   1.0,   # MA20 지지선 근접
        "cross_imminent": 1.0,   # 크로스 임박
    }
    try:
        if WEIGHTS_FILE.exists():
            data = json.loads(WEIGHTS_FILE.read_text(encoding="utf-8"))
            sw = data.get("signal_weights", {})
            default.update(sw)
    except Exception:
        pass
    return default


# ══════════════════════════════════════════════════════════════════
# 기술적 분석
# ══════════════════════════════════════════════════════════════════

def analyze(ticker: str, sig_w: dict) -> dict | None:
    """
    65일 일봉 기반 기술적 신호 분석.
    sig_w: signal_weights (Evolution 학습 결과)
    """
    try:
        hist = yf.Ticker(ticker).history(period="65d", interval="1d")
        if len(hist) < 22:
            return None

        close  = hist["Close"]
        volume = hist["Volume"]
        price  = float(close.iloc[-1])

        score: float = 50.0
        signals: list = []
        fired: list = []   # 어떤 signal key가 발동됐는지 (Evolution 학습용)

        # ── RSI 14일 ─────────────────────────────────────────────
        delta = close.diff()
        gain  = delta.clip(lower=0).rolling(14).mean()
        loss  = (-delta.clip(upper=0)).rolling(14).mean()
        rsi   = float((100 - 100 / (1 + gain / loss)).iloc[-1])

        if rsi <= 28:
            pts = 22 * sig_w.get("rsi_extreme", 1.0)
            score += pts; fired.append("rsi_extreme")
            signals.append(f"🔴 RSI {rsi:.1f} 극단 과매도")
        elif rsi <= 38:
            pts = 12 * sig_w.get("rsi_oversold", 1.0)
            score += pts; fired.append("rsi_oversold")
            signals.append(f"🟠 RSI {rsi:.1f} 과매도권")
        elif rsi >= 72:
            pts = 18 * sig_w.get("dead_cross", 1.0)
            score -= pts
            signals.append(f"⚠️ RSI {rsi:.1f} 과매수 — 진입 자제")
        elif rsi >= 62:
            score -= 8
            signals.append(f"RSI {rsi:.1f} 고점권")

        # ── MA20 × MA50 크로스 ───────────────────────────────────
        ma20     = close.rolling(20).mean()
        ma_cross = None

        if len(close) >= 52:
            ma50      = close.rolling(50).mean()
            was_above = bool(ma20.iloc[-2] > ma50.iloc[-2])
            now_above = bool(ma20.iloc[-1] > ma50.iloc[-1])

            if now_above and not was_above:
                pts = 16 * sig_w.get("golden_cross", 1.0)
                score += pts; ma_cross = "golden"; fired.append("golden_cross")
                signals.append("✅ MA20 골든크로스")
            elif not now_above and was_above:
                pts = 18 * sig_w.get("dead_cross", 1.0)
                score -= pts; ma_cross = "dead"
                signals.append("❌ MA20 데드크로스")
            else:
                gap = (float(ma20.iloc[-1]) - float(ma50.iloc[-1])) / float(ma50.iloc[-1]) * 100
                if abs(gap) < 0.5:
                    pts = 5 * sig_w.get("cross_imminent", 1.0)
                    score += pts; fired.append("cross_imminent")
                    signals.append(f"MA20/MA50 크로스 임박 ({gap:+.1f}%)")

        # ── 볼린저밴드 (20일, 2σ) ────────────────────────────────
        ma20_v = float(ma20.iloc[-1])
        std20  = float(close.rolling(20).std().iloc[-1])
        bb_up  = ma20_v + 2 * std20
        bb_dn  = ma20_v - 2 * std20
        rng    = bb_up - bb_dn
        bb_pos = (price - bb_dn) / rng if rng > 0 else 0.5

        if bb_pos <= 0.08:
            pts = 18 * sig_w.get("bb_touch", 1.0)
            score += pts; fired.append("bb_touch")
            signals.append(f"📉 볼린저 하단 터치 ({bb_pos:.0%})")
        elif bb_pos <= 0.20:
            pts = 9 * sig_w.get("bb_near", 1.0)
            score += pts; fired.append("bb_near")
            signals.append(f"볼린저 하단 근접 ({bb_pos:.0%})")
        elif bb_pos >= 0.92:
            pts = 12 * sig_w.get("bb_upper", 1.0)
            score -= pts
            signals.append(f"📈 볼린저 상단 과확장 ({bb_pos:.0%})")

        # ── 거래량 급증 ──────────────────────────────────────────
        vol_ratio = 1.0
        if len(volume) >= 6:
            avg_vol   = float(volume.iloc[-6:-1].mean())
            vol_ratio = float(volume.iloc[-1]) / avg_vol if avg_vol > 0 else 1.0

            if vol_ratio >= 2.5:
                pts = 12 * sig_w.get("volume_surge", 1.0)
                score += pts; fired.append("volume_surge")
                signals.append(f"🔥 거래량 급증 {vol_ratio:.1f}x")
            elif vol_ratio >= 1.8:
                pts = 6 * sig_w.get("volume_rise", 1.0)
                score += pts; fired.append("volume_rise")
                signals.append(f"거래량 증가 {vol_ratio:.1f}x")

        # ── MA20 지지선 근접 ─────────────────────────────────────
        if ma20_v > 0:
            prox = abs(price - ma20_v) / ma20_v
            if prox <= 0.012:
                pts = 6 * sig_w.get("ma20_support", 1.0)
                score += pts; fired.append("ma20_support")
                signals.append(f"MA20 지지선 근접 ({prox:.1%})")

        score = max(0.0, min(100.0, score))

        return {
            "price":     price,
            "rsi":       round(rsi, 1),
            "bb_pos":    round(bb_pos, 3),
            "ma_cross":  ma_cross,
            "vol_ratio": round(vol_ratio, 2),
            "score":     round(score, 1),
            "signals":   signals,
            "fired":     fired,   # Evolution 학습용
        }

    except Exception as e:
        log.error(f"  {ticker} 분석 실패: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
# 센티먼트 보정
# ══════════════════════════════════════════════════════════════════

def load_sentiment() -> float:
    try:
        if SENTIMENT_FILE.exists():
            data = json.loads(SENTIMENT_FILE.read_text(encoding="utf-8"))
            return float(data.get("current", {}).get("score", 50))
    except Exception:
        pass
    return 50.0


def apply_sentiment(base: float, sent: float) -> float:
    if sent < 30:
        return min(100.0, base * 1.15)   # 극단 공포 → 역발상 매수
    if sent > 65:
        return min(100.0, base * 1.10)   # 탐욕 → 추세 추종
    return base


# ══════════════════════════════════════════════════════════════════
# 쿨다운
# ══════════════════════════════════════════════════════════════════

def is_on_cooldown(ticker: str) -> bool:
    if not COOLDOWN_FILE.exists():
        return False
    try:
        cd = json.loads(COOLDOWN_FILE.read_text(encoding="utf-8"))
        last = cd.get(ticker)
        if not last:
            return False
        elapsed_h = (datetime.now() - datetime.fromisoformat(last)).total_seconds() / 3600
        return elapsed_h < COOLDOWN_HOURS
    except Exception:
        return False


def set_cooldown(ticker: str):
    cd: dict = {}
    if COOLDOWN_FILE.exists():
        try:
            cd = json.loads(COOLDOWN_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    cd[ticker] = datetime.now().isoformat()
    COOLDOWN_FILE.write_text(json.dumps(cd, ensure_ascii=False, indent=2), encoding="utf-8")


# ══════════════════════════════════════════════════════════════════
# 텔레그램
# ══════════════════════════════════════════════════════════════════

def send_telegram(text: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("  텔레그램 미설정 — 콘솔 출력")
        print(text)
        return False
    try:
        url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        resp = httpx.post(url, json={
            "chat_id":                  TELEGRAM_CHAT_ID,
            "text":                     text,
            "parse_mode":               "HTML",
            "disable_web_page_preview": True,
        }, timeout=10)
        ok = resp.json().get("ok", False)
        if not ok:
            log.error(f"  텔레그램 오류: {resp.text[:150]}")
        return ok
    except Exception as e:
        log.error(f"  텔레그램 예외: {e}")
        return False


def build_message(ticker: str, info: dict, sig: dict,
                  final: float, sent: float) -> str:
    now_str   = datetime.now(KST).strftime("%m/%d %H:%M")
    cur       = info["currency"]
    strong    = final >= STRONG_THRESHOLD
    header    = "🔥 <b>강한 매수 타점</b>" if strong else "🔵 <b>진입 검토 타점</b>"
    price_str = (f"{sig['price']:,.2f}" if info["market"] == "US"
                 else f"{sig['price']:,.0f}")

    pnl_line = ""
    if info.get("avg_cost") and info["market"] == "US":
        pnl      = (sig["price"] - info["avg_cost"]) / info["avg_cost"] * 100
        pnl_line = f"\n{'📈' if pnl >= 0 else '📉'} 내 수익률: {pnl:+.1f}% (평균단가 {cur}{info['avg_cost']})"

    sig_block = (
        "\n".join(f"  • {s}" for s in sig["signals"])
        if sig["signals"] else "  • 복합 기술적 조건 충족"
    )

    return (
        f"{header}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>{info['name']} ({ticker})</b>\n"
        f"💰 현재가: {cur}{price_str}{pnl_line}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 타점 점수: <b>{final:.0f}/100</b>\n"
        f"📊 RSI: {sig['rsi']} | 볼린저: {sig['bb_pos']:.0%}"
        f" | 거래량: {sig['vol_ratio']:.1f}x\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 감지 신호:\n{sig_block}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ {now_str} KST | Jackal Scanner"
    )


# ══════════════════════════════════════════════════════════════════
# 스캔 로그 (Evolution 학습용)
# ══════════════════════════════════════════════════════════════════

def save_scan_log(entry: dict):
    """스캔 결과를 scan_log.json 에 추가"""
    logs: list = []
    if SCAN_LOG_FILE.exists():
        try:
            logs = json.loads(SCAN_LOG_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    logs.append(entry)
    logs = logs[-500:]   # 최근 500건 유지
    SCAN_LOG_FILE.write_text(json.dumps(logs, ensure_ascii=False, indent=2), encoding="utf-8")


# ══════════════════════════════════════════════════════════════════
# 메인 스캔 실행
# ══════════════════════════════════════════════════════════════════

def run_scan(force: bool = False) -> dict:
    """
    스캔 1회 실행.
    Returns: {scanned: int, alerted: int, results: list}
    """
    now_kst  = datetime.now(KST)
    us_open  = is_us_open()
    kr_open  = is_kr_open()
    sent_val = load_sentiment()
    sig_w    = load_signal_weights()

    log.info(f"📡 Jackal Scanner | {now_kst.strftime('%Y-%m-%d %H:%M KST')}")
    log.info(
        f"   미국장 {'✅' if us_open else '❌'} | "
        f"한국장 {'✅' if kr_open else '❌'} | "
        f"센티먼트 {sent_val:.0f}"
    )

    results: list = []
    alerted = 0

    for ticker, info in WATCHLIST.items():
        market = info["market"]

        # 장 마감 확인 (force=True면 무시)
        if not force:
            if market == "US" and not us_open:
                continue
            if market == "KR" and not kr_open:
                continue

        # 쿨다운
        if is_on_cooldown(ticker):
            log.info(f"  {ticker}: 쿨다운 중 — 스킵")
            continue

        # 분석
        sig = analyze(ticker, sig_w)
        if not sig:
            continue

        final_score = round(apply_sentiment(sig["score"], sent_val), 1)

        log.info(
            f"  {ticker} ({info['name']}): "
            f"점수 {final_score:.0f} | RSI {sig['rsi']} | "
            f"BB {sig['bb_pos']:.0%} | 신호 {len(sig['signals'])}개"
        )
        for s in sig["signals"]:
            log.info(f"    → {s}")

        # 알림 발송
        alerted_flag = False
        if final_score >= SIGNAL_THRESHOLD:
            msg = build_message(ticker, info, sig, final_score, sent_val)
            ok  = send_telegram(msg)
            if ok:
                set_cooldown(ticker)
                alerted_flag = True
                alerted += 1
                log.info(f"  ✅ {ticker} 텔레그램 발송 (점수 {final_score:.0f})")
        else:
            log.info(f"  — {ticker}: 임계치 미달 ({final_score:.0f} < {SIGNAL_THRESHOLD})")

        # 로그 저장 (Evolution 학습용)
        entry = {
            "timestamp":        now_kst.isoformat(),
            "ticker":           ticker,
            "name":             info["name"],
            "market":           market,
            "price_at_scan":    sig["price"],
            "score":            sig["score"],
            "final_score":      final_score,
            "sent_score":       sent_val,
            "signals":          sig["signals"],
            "fired":            sig["fired"],   # 발동된 signal key 목록
            "alerted":          alerted_flag,
            # Evolution이 나중에 채움
            "outcome_checked":  False,
            "outcome_price":    None,
            "outcome_pct":      None,
            "outcome_correct":  None,
        }
        save_scan_log(entry)
        results.append(entry)

    log.info(f"📡 완료 | 분석 {len(results)}종목 | 알림 {alerted}건")
    return {"scanned": len(results), "alerted": alerted, "results": results}
