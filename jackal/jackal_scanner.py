"""
jackal_scanner.py
Jackal Scanner — Analyst → Devil → Final 3단계 타점 분석

흐름:
  1. 데이터 수집 (yfinance + FRED + KRX + FSC + ARIA 파일)
  2. Analyst (Haiku): 매수 근거 구성
  3. Devil   (Haiku): Analyst 반박 + ARIA Thesis Killer 체크
  4. Final   판단:
       둘 다 매수  → 강한 신호 (가중 합산)
       엇갈림      → 점수 낮춤
       둘 다 반대  → 알림 없음
  5. 결과 저장 (Evolution 학습용 — 신호·레짐·Devil 정확도 포함)
"""

import os
import sys
import json
import re
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx
from anthropic import Anthropic

from jackal_market_data import fetch_all, fetch_technicals

os.environ["PYTHONIOENCODING"] = "utf-8"
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

log = logging.getLogger("jackal_scanner")

KST   = timezone(timedelta(hours=9))
_BASE = Path(__file__).parent

SCAN_LOG_FILE = _BASE / "scan_log.json"
COOLDOWN_FILE = _BASE / "scan_cooldown.json"
WEIGHTS_FILE  = _BASE / "jackal_weights.json"

# ARIA 데이터 파일 (읽기만, 의존성 없음)
ARIA_BASELINE  = Path("data") / "morning_baseline.json"
ARIA_SENTIMENT = Path("data") / "sentiment.json"
ARIA_ROTATION  = Path("data") / "rotation.json"
PORTFOLIO_FILE = Path("data") / "portfolio.json"

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
MODEL_H          = os.environ.get("SUBAGENT_MODEL", "claude-haiku-4-5-20251001")


def _load_portfolio() -> dict:
    """data/portfolio.json 에서 포트폴리오 로드.
    없으면 기본값 반환."""
    default = {
        "NVDA":      {"name": "엔비디아",   "avg_cost": 182.99, "market": "US", "currency": "$", "portfolio": True},
        "AVGO":      {"name": "브로드컴",   "avg_cost": None,   "market": "US", "currency": "$", "portfolio": True},
        "SCHD":      {"name": "SCHD",       "avg_cost": None,   "market": "US", "currency": "$", "portfolio": True},
        "000660.KS": {"name": "SK하이닉스", "avg_cost": None,   "market": "KR", "currency": "₩", "portfolio": True},
        "005930.KS": {"name": "삼성전자",   "avg_cost": None,   "market": "KR", "currency": "₩", "portfolio": True},
        "035720.KS": {"name": "카카오",     "avg_cost": None,   "market": "KR", "currency": "₩", "portfolio": True},
    }
    if not PORTFOLIO_FILE.exists():
        return default
    try:
        data = json.loads(PORTFOLIO_FILE.read_text(encoding="utf-8"))
        result = {}
        for h in data.get("holdings", []):
            ticker = h.get("ticker", "")
            if not ticker:
                continue
            market = h.get("market", "US")
            result[ticker] = {
                "name":      h.get("name", ticker),
                "avg_cost":  h.get("avg_cost"),
                "market":    market,
                "currency":  h.get("currency", "$"),
                "portfolio": True,
            }
        return result if result else default
    except Exception:
        return default

ALERT_THRESHOLD = 65
STRONG_THRESHOLD = 78
COOLDOWN_HOURS  = 4


# ══════════════════════════════════════════════════════════════════
# 시장 개장 여부
# ══════════════════════════════════════════════════════════════════

def _is_us_open() -> bool:
    from datetime import time as t
    now = datetime.now(timezone(timedelta(hours=-5)))
    return now.weekday() < 5 and t(9, 30) <= now.time() <= t(16, 0)

def _is_kr_open() -> bool:
    from datetime import time as t
    now = datetime.now(KST)
    return now.weekday() < 5 and t(9, 0) <= now.time() <= t(15, 30)


# ══════════════════════════════════════════════════════════════════
# ARIA 컨텍스트 로딩 (파일 읽기만)
# ══════════════════════════════════════════════════════════════════

def _load_aria_context() -> dict:
    """
    ARIA가 생성한 파일들을 읽어 시장 맥락 구성.
    파일 없으면 빈 값 반환 — ARIA에 의존하지 않음.
    """
    ctx = {
        "regime":        "",
        "trend":         "",
        "confidence":    "",
        "one_line":      "",
        "thesis_killers": [],
        "key_inflows":   [],
        "key_outflows":  [],
        "sentiment_score": 50,
        "sentiment_level": "중립",
        "top_sector":    "",
        "bottom_sector": "",
    }

    # morning_baseline.json
    try:
        if ARIA_BASELINE.exists():
            b = json.loads(ARIA_BASELINE.read_text(encoding="utf-8"))
            ctx["regime"]     = b.get("market_regime", "")
            ctx["trend"]      = b.get("trend_phase", "")
            ctx["confidence"] = b.get("confidence", "")
            ctx["one_line"]   = b.get("one_line_summary", "")
            ctx["thesis_killers"] = b.get("thesis_killers", [])
            ctx["key_inflows"]    = [i.get("zone","") for i in b.get("key_inflows", [])[:3]]
            ctx["key_outflows"]   = [o.get("zone","") for o in b.get("key_outflows", [])[:3]]
    except Exception:
        pass

    # sentiment.json
    try:
        if ARIA_SENTIMENT.exists():
            s = json.loads(ARIA_SENTIMENT.read_text(encoding="utf-8"))
            cur = s.get("current", {})
            ctx["sentiment_score"] = cur.get("score", 50)
            ctx["sentiment_level"] = cur.get("level", "중립")
    except Exception:
        pass

    # rotation.json
    try:
        if ARIA_ROTATION.exists():
            r = json.loads(ARIA_ROTATION.read_text(encoding="utf-8"))
            ranking = r.get("ranking", [])
            if ranking:
                ctx["top_sector"]    = ranking[0][0] if ranking else ""
                ctx["bottom_sector"] = ranking[-1][0] if ranking else ""
            sig = r.get("rotation_signal", {})
            ctx["rotation_from"] = sig.get("from", "")
            ctx["rotation_to"]   = sig.get("to", "")
    except Exception:
        pass

    return ctx


def _load_weights() -> dict:
    try:
        if WEIGHTS_FILE.exists():
            return json.loads(WEIGHTS_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


# ══════════════════════════════════════════════════════════════════
# Agent 1: Analyst — 매수 근거 구성
# ══════════════════════════════════════════════════════════════════

def agent_analyst(ticker: str, info: dict, tech: dict,
                  macro: dict, aria: dict) -> dict:
    """
    Haiku로 매수 근거를 구성.
    Returns: {score, signals_fired, reasoning, confidence}
    """
    cur     = info["currency"]
    fred    = macro.get("fred", {})
    weights = _load_weights()
    stw     = weights.get("signal_weights", {})

    price_str = f"{tech['price']:,.2f}" if info["market"] == "US" else f"{tech['price']:,.0f}"
    pnl_str = ""
    if info.get("avg_cost") and info["market"] == "US":
        pnl     = (tech["price"] - info["avg_cost"]) / info["avg_cost"] * 100
        pnl_str = f"\n내 평균단가: {cur}{info['avg_cost']} (현재 {pnl:+.1f}%)"

    # 학습된 신호별 정확도 힌트
    acc_hint = ""
    sig_acc = weights.get("signal_accuracy", {})
    if sig_acc:
        top = sorted(sig_acc.items(), key=lambda x: x[1].get("accuracy", 0), reverse=True)[:3]
        acc_hint = "\n[학습 정확도 높은 신호] " + " | ".join(
            f"{k}:{v['accuracy']:.0f}%" for k, v in top if v.get("total", 0) >= 3
        )

    prompt = f"""당신은 주식 매수 타점 분석가(Analyst)입니다.
아래 데이터로 {info['name']} ({ticker})의 매수 근거를 분석하세요.
반드시 JSON만 반환하세요.

[종목]
현재가: {cur}{price_str}
전일比: {tech['change_1d']:+.1f}% | 5일比: {tech['change_5d']:+.1f}%{pnl_str}

[기술 지표]
RSI(14): {tech['rsi']} | MA20: {cur}{tech['ma20']} | MA50: {cur}{tech.get('ma50','N/A')}
볼린저: {tech['bb_pos']}% (0%=하단, 100%=상단) | 거래량: 평균 대비 {tech['vol_ratio']:.1f}x

[매크로 (FRED)]
VIX: {fred.get('vix','N/A')} | HY스프레드: {fred.get('hy_spread','N/A')}%
장단기금리차: {fred.get('yield_curve','N/A')}% | 달러지수: {fred.get('dxy','N/A')}
소비자심리: {fred.get('consumer_sent','N/A')}

[ARIA 시장 맥락]
레짐: {aria['regime'] or '정보없음'} | 추세: {aria['trend'] or '정보없음'}
센티먼트: {aria['sentiment_score']}점 ({aria['sentiment_level']})
섹터유입: {', '.join(aria['key_inflows']) or '없음'}
섹터유출: {', '.join(aria['key_outflows']) or '없음'}
강세섹터: {aria.get('top_sector','N/A')} | 약세섹터: {aria.get('bottom_sector','N/A')}
{acc_hint}

매수 근거가 있다면 높은 점수, 없다면 낮은 점수를 주세요.

{{
  "analyst_score": 0~100,
  "confidence": "낮음" 또는 "보통" 또는 "높음",
  "signals_fired": ["rsi_oversold", "bb_touch", "volume_surge", "ma_support", "golden_cross", "fear_regime", "sector_inflow"],
  "bull_case": "매수 근거 2~3줄",
  "entry_price": 숫자 또는 null,
  "stop_loss": 숫자 또는 null
}}"""

    try:
        resp = Anthropic().messages.create(
            model=MODEL_H, max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        raw  = re.sub(r"```(?:json)?|```", "", resp.content[0].text).strip()
        m    = re.search(r"\{[\s\S]*\}", raw)
        if not m:
            return {"analyst_score": 50, "confidence": "낮음",
                    "signals_fired": [], "bull_case": "분석 실패"}
        result = json.loads(m.group())
        result["analyst_score"] = int(result.get("analyst_score", 50))
        return result
    except Exception as e:
        log.error(f"  Analyst 실패: {e}")
        return {"analyst_score": 50, "confidence": "낮음",
                "signals_fired": [], "bull_case": "분석 실패"}


# ══════════════════════════════════════════════════════════════════
# Agent 2: Devil — 반박 + Thesis Killer 체크
# ══════════════════════════════════════════════════════════════════

def agent_devil(ticker: str, info: dict, tech: dict,
                macro: dict, aria: dict, analyst: dict) -> dict:
    """
    Haiku로 Analyst 결론을 반박.
    ARIA의 Thesis Killer를 직접 체크.
    Returns: {devil_score, verdict, objections, thesis_killer_hit}
    """
    cur  = info["currency"]
    fred = macro.get("fred", {})

    price_str = f"{tech['price']:,.2f}" if info["market"] == "US" else f"{tech['price']:,.0f}"

    # Thesis Killer 텍스트 구성
    tk_text = ""
    tks = aria.get("thesis_killers", [])
    if tks:
        tk_lines = []
        for tk in tks[:3]:
            event = tk.get("event", "")
            inv   = tk.get("invalidates_if", "")
            if event and inv:
                tk_lines.append(f"  • {event}: {inv}")
        if tk_lines:
            tk_text = "\n[ARIA Thesis Killers — 이 조건이면 매수 무효]\n" + "\n".join(tk_lines)

    prompt = f"""당신은 투자 리스크 분석가(Devil)입니다.
Analyst가 {info['name']} ({ticker}) 매수를 주장합니다.
당신은 반드시 반박해야 합니다. JSON만 반환하세요.

[Analyst 결론]
점수: {analyst['analyst_score']} | 신뢰도: {analyst['confidence']}
근거: {analyst.get('bull_case','')}
발동 신호: {', '.join(analyst.get('signals_fired', []))}

[현재 상황]
현재가: {cur}{price_str}
RSI: {tech['rsi']} | 볼린저: {tech['bb_pos']}% | 거래량: {tech['vol_ratio']:.1f}x
VIX: {fred.get('vix','N/A')} | HY스프레드: {fred.get('hy_spread','N/A')}%
장단기금리차: {fred.get('yield_curve','N/A')}%

[ARIA 시장 맥락]
레짐: {aria['regime'] or '정보없음'} | 센티먼트: {aria['sentiment_score']}점
유출섹터: {', '.join(aria['key_outflows']) or '없음'}
{tk_text}

반박 기준:
- VIX > 25이면 변동성 과대
- HY스프레드 > 4%이면 위험회피 강화
- 레짐이 위험회피이면 매수 부적절
- Thesis Killer 조건 해당이면 즉시 무효
- 연속 3일+ 상승이면 과열 경고

{{
  "devil_score": 0~100 (높을수록 반박 강함, 매수 부적절),
  "verdict": "동의" 또는 "부분동의" 또는 "반대",
  "objections": ["반박 이유 1", "반박 이유 2"],
  "thesis_killer_hit": true 또는 false,
  "killer_detail": "해당 Thesis Killer 내용 (없으면 빈 문자열)",
  "bear_case": "매수 반대 근거 1~2줄"
}}"""

    try:
        resp = Anthropic().messages.create(
            model=MODEL_H, max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        raw  = re.sub(r"```(?:json)?|```", "", resp.content[0].text).strip()
        m    = re.search(r"\{[\s\S]*\}", raw)
        if not m:
            return {"devil_score": 30, "verdict": "부분동의",
                    "objections": [], "thesis_killer_hit": False}
        result = json.loads(m.group())
        result["devil_score"] = int(result.get("devil_score", 30))
        return result
    except Exception as e:
        log.error(f"  Devil 실패: {e}")
        return {"devil_score": 30, "verdict": "부분동의",
                "objections": [], "thesis_killer_hit": False}


# ══════════════════════════════════════════════════════════════════
# Final 판단
# ══════════════════════════════════════════════════════════════════

def _final_judgment(analyst: dict, devil: dict) -> dict:
    """
    Analyst와 Devil 결과를 합산해 최종 판단.

    계산:
      - Thesis Killer 발동 → 즉시 is_entry=False
      - Devil 반대         → analyst_score * 0.5
      - Devil 부분동의     → analyst_score * 0.75
      - Devil 동의         → analyst_score * 1.0
      - final_score        → 위 조정값 (devil_score는 페널티로 반영)
    """
    a_score   = analyst.get("analyst_score", 50)
    d_score   = devil.get("devil_score", 30)
    verdict   = devil.get("verdict", "부분동의")
    tk_hit    = devil.get("thesis_killer_hit", False)

    # Thesis Killer 발동 → 즉시 매수 불가
    if tk_hit:
        return {
            "final_score": 20,
            "is_entry":    False,
            "signal_type": "매도주의",
            "reason":      f"⛔ Thesis Killer: {devil.get('killer_detail','무효화 조건 충족')}",
        }

    # Devil 판정에 따른 가중치
    weight_map = {"동의": 1.0, "부분동의": 0.75, "반대": 0.5}
    weight     = weight_map.get(verdict, 0.75)

    # Devil 반박 강도 페널티 (devil_score가 높을수록 페널티)
    devil_penalty = (d_score - 30) * 0.2   # devil_score 30 기준, 초과분의 20%
    final = max(0, min(100, round(a_score * weight - devil_penalty, 1)))

    # 신호 타입 결정
    if final >= STRONG_THRESHOLD and verdict != "반대":
        sig_type = "강한매수"
    elif final >= ALERT_THRESHOLD and verdict != "반대":
        sig_type = "매수검토"
    elif verdict == "반대" or final < 40:
        sig_type = "매도주의" if final < 30 else "관망"
    else:
        sig_type = "관망"

    is_entry = final >= ALERT_THRESHOLD and verdict != "반대" and not tk_hit

    # 판단 이유 구성
    reason_parts = []
    if analyst.get("bull_case"):
        reason_parts.append(analyst["bull_case"][:40])
    if devil.get("objections"):
        reason_parts.append("⚠️ " + devil["objections"][0][:30])

    return {
        "final_score": final,
        "is_entry":    is_entry,
        "signal_type": sig_type,
        "reason":      " | ".join(reason_parts)[:80],
        "entry_price": analyst.get("entry_price"),
        "stop_loss":   analyst.get("stop_loss"),
    }


# ══════════════════════════════════════════════════════════════════
# 쿨다운
# ══════════════════════════════════════════════════════════════════

def _is_on_cooldown(ticker: str) -> bool:
    if not COOLDOWN_FILE.exists():
        return False
    try:
        cd   = json.loads(COOLDOWN_FILE.read_text(encoding="utf-8"))
        last = cd.get(ticker)
        if not last:
            return False
        return (datetime.now() - datetime.fromisoformat(last)).total_seconds() / 3600 < COOLDOWN_HOURS
    except Exception:
        return False

def _set_cooldown(ticker: str):
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

def _send_telegram(text: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(text); return False
    try:
        resp = httpx.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10,
        )
        return resp.json().get("ok", False)
    except Exception as e:
        log.error(f"  텔레그램 예외: {e}")
        return False


def _build_alert_message(ticker: str, info: dict, tech: dict,
                          analyst: dict, devil: dict, final: dict,
                          aria: dict) -> str:
    now_str  = datetime.now(KST).strftime("%m/%d %H:%M")
    cur      = info["currency"]
    strong   = final["signal_type"] == "강한매수"
    header   = "🔥 <b>강한 매수 타점</b>" if strong else "🔵 <b>매수 검토 타점</b>"
    price_str = f"{tech['price']:,.2f}" if info["market"] == "US" else f"{tech['price']:,.0f}"

    pnl_line = ""
    if info.get("avg_cost") and info["market"] == "US":
        pnl      = (tech["price"] - info["avg_cost"]) / info["avg_cost"] * 100
        pnl_line = f"\n{'📈' if pnl >= 0 else '📉'} 내 수익률: {pnl:+.1f}%"

    entry = final.get("entry_price")
    stop  = final.get("stop_loss")

    # Devil 판정
    verdict_icon = {"동의": "✅", "부분동의": "⚠️", "반대": "❌"}.get(devil.get("verdict",""), "")
    devil_line = f"\n🔴 Devil {verdict_icon}: {devil.get('objections',[None])[0] or ''}"[:60] if devil.get("objections") else ""

    fred = aria  # macro already in aria context for message

    return (
        f"{header}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>{info['name']} ({ticker})</b>\n"
        f"💰 {cur}{price_str} ({tech['change_1d']:+.1f}%){pnl_line}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 최종 점수: <b>{final['final_score']:.0f}/100</b>\n"
        f"📊 Analyst {analyst['analyst_score']} → Devil {devil['devil_score']} → Final {final['final_score']:.0f}\n"
        f"📉 RSI {tech['rsi']} | BB {tech['bb_pos']}% | 거래량 {tech['vol_ratio']:.1f}x\n"
        f"💡 {final.get('reason','')}{devil_line}\n"
        f"{'🎯 진입가: ' + cur + str(entry) if entry else ''}"
        f"{'  🛑 손절: ' + cur + str(stop) if stop else ''}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"😐 센티먼트: {aria['sentiment_score']}점 | 레짐: {aria['regime'][:20] if aria['regime'] else 'N/A'}\n"
        f"⏰ {now_str} KST | Jackal"
    )


def _build_summary_message(results: list, macro: dict, aria: dict) -> str:
    """타점 없을 때 스캔 결과 요약"""
    now_str = datetime.now(KST).strftime("%m/%d %H:%M")
    fred    = macro.get("fred", {})
    lines   = ["📊 <b>Jackal 스캔 완료 — 타점 없음</b>",
               "━━━━━━━━━━━━━━━━━━━━"]

    # 포트폴리오 / 추천 종목 구분
    portfolio_results = [r for r in results if r.get("is_portfolio", True)]
    extra_results     = [r for r in results if not r.get("is_portfolio", True)]

    if portfolio_results:
        lines.append("<b>📋 보유 포트폴리오</b>")
    for r in portfolio_results:
        sig  = r.get("signal_type", "관망")
        icon = {"강한매수": "🔴", "매수검토": "🟡", "관망": "⚪", "매도주의": "🔵"}.get(sig, "⚪")
        v    = r.get("devil_verdict", "")
        dv   = f" | Devil:{v}" if v else ""
        lines.append(f"{icon} {r['name']}: {r['final_score']:.0f}점 | RSI {r['rsi']} | {sig}{dv}")

    if extra_results:
        lines.append("")
        lines.append("<b>💡 ARIA 추천 종목 (포트폴리오 외)</b>")
    for r in extra_results:
        sig  = r.get("signal_type", "관망")
        icon = {"강한매수": "🔴", "매수검토": "🟡", "관망": "⚪", "매도주의": "🔵"}.get(sig, "⚪")
        reason = r.get("aria_reason", "")
        lines.append(f"{icon} {r['name']} ({r['ticker']}): {r['final_score']:.0f}점 | {sig}")
        if reason:
            lines.append(f"   └ {reason}")

    lines.append("━━━━━━━━━━━━━━━━━━━━")
    fred_parts = []
    if fred.get("vix"):         fred_parts.append(f"VIX {fred['vix']}")
    if fred.get("hy_spread"):   fred_parts.append(f"HY {fred['hy_spread']}%")
    if fred.get("yield_curve") is not None:
        fred_parts.append(f"금리차 {fred['yield_curve']:+.2f}%")
    if fred_parts:
        lines.append("📈 " + " | ".join(fred_parts))

    lines.append(f"😐 센티먼트: {aria['sentiment_score']}점 ({aria['sentiment_level']})")
    if aria.get("regime"):
        lines.append(f"🌐 레짐: {aria['regime'][:30]}")
    lines.append(f"⏰ {now_str} KST | Jackal")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════
# 스캔 로그 (Evolution 학습용 — 풍부한 메타데이터 포함)
# ══════════════════════════════════════════════════════════════════

def _save_log(entry: dict):
    logs: list = []
    if SCAN_LOG_FILE.exists():
        try:
            logs = json.loads(SCAN_LOG_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    logs.append(entry)
    logs = logs[-500:]
    SCAN_LOG_FILE.write_text(json.dumps(logs, ensure_ascii=False, indent=2), encoding="utf-8")


# ══════════════════════════════════════════════════════════════════
# 메인 스캔
# ══════════════════════════════════════════════════════════════════

def _suggest_extra_tickers(aria: dict, portfolio: dict) -> dict:
    """
    ARIA 분석에서 타점 가능성 높은 추가 5종목 추천.
    Claude Haiku가 섹터 유입/헤드라인 기반으로 추천.
    포트폴리오에 이미 있는 종목은 제외.
    """
    existing = set(portfolio.keys())
    inflows  = aria.get("key_inflows", [])
    outflows = aria.get("key_outflows", [])
    regime   = aria.get("regime", "")
    one_line = aria.get("one_line", "")
    top_sec  = aria.get("top_sector", "")

    if not inflows and not regime:
        return {}

    prompt = f"""당신은 주식 종목 추천 전문가입니다.
ARIA 시장 분석 결과를 보고 타점이 생길 가능성이 높은 종목 5개를 추천하세요.
이미 보유 중인 종목({', '.join(existing)})은 제외하세요.

[ARIA 분석]
레짐: {regime}
요약: {one_line[:80]}
주요 유입 섹터: {', '.join(inflows)}
주요 유출 섹터: {', '.join(outflows)}
강세 섹터: {top_sec}

조건:
- yfinance로 조회 가능한 실제 티커 심볼 사용
- 미국 주식: TICKER 형식 (예: TSM, AMD, QCOM)
- 한국 주식: 6자리+.KS 형식 (예: 012450.KS)
- 유입 섹터와 연관된 종목 우선
- 현재 레짐에서 수혜 가능한 종목

JSON만 반환하세요:
{{
  "recommendations": [
    {{"ticker": "TSM", "name": "TSMC", "market": "US", "currency": "$", "reason": "AI 반도체 수혜"}},
    {{"ticker": "AMD", "name": "AMD", "market": "US", "currency": "$", "reason": "GPU 경쟁"}},
    {{"ticker": "012450.KS", "name": "한화에어로스페이스", "market": "KR", "currency": "₩", "reason": "방산 섹터 유입"}}
  ]
}}"""

    try:
        resp = Anthropic().messages.create(
            model=MODEL_H, max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        raw  = re.sub(r"", "", resp.content[0].text).strip()
        m    = re.search(r"\{{[\s\S]*\}}", raw)
        if not m:
            return {}
        data  = json.loads(m.group())
        extra = {}
        for r in data.get("recommendations", [])[:5]:
            t = r.get("ticker", "")
            if t and t not in existing:
                extra[t] = {
                    "name":      r.get("name", t),
                    "avg_cost":  None,
                    "market":    r.get("market", "US"),
                    "currency":  r.get("currency", "$"),
                    "portfolio": False,
                    "reason":    r.get("reason", ""),
                }
        log.info(f"   ARIA 추가 추천: {list(extra.keys())}")
        return extra
    except Exception as e:
        log.error(f"추가 종목 추천 실패: {e}")
        return {}


def run_scan(force: bool = False) -> dict:
    now_kst = datetime.now(KST)
    us_open = _is_us_open()
    kr_open = _is_kr_open()

    log.info(f"📡 Jackal Scanner | {now_kst.strftime('%Y-%m-%d %H:%M KST')}")
    log.info(f"   미국장 {'✅' if us_open else '❌'} | 한국장 {'✅' if kr_open else '❌'}")

    # 공통 데이터 수집 (1회)
    macro = fetch_all()
    aria  = _load_aria_context()

    log.info(f"   ARIA 레짐: {aria['regime'][:20] if aria['regime'] else '정보없음'} | "
             f"센티먼트: {aria['sentiment_score']}점")

    # 포트폴리오 로드
    portfolio = _load_portfolio()
    log.info(f"   포트폴리오: {len(portfolio)}종목 | ")

    # ARIA 기반 추가 5종목 추천
    extra = _suggest_extra_tickers(aria, portfolio)

    # 전체 스캔 대상 = 포트폴리오 + 추가 추천
    watchlist = {**portfolio, **extra}

    scanned = 0
    alerted = 0
    results: list = []

    for ticker, info in watchlist.items():
        market = info["market"]

        if not force:
            if market == "US" and not us_open:
                continue
            if market == "KR" and not kr_open:
                continue

        if _is_on_cooldown(ticker):
            log.info(f"  {ticker}: 쿨다운 — 스킵")
            continue

        tech = fetch_technicals(ticker)
        if not tech:
            continue

        log.info(f"  {ticker} ({info['name']}): RSI={tech['rsi']} BB={tech['bb_pos']}% vol={tech['vol_ratio']:.1f}x")

        # ── Agent 1: Analyst ─────────────────────────────────────
        analyst = agent_analyst(ticker, info, tech, macro, aria)
        log.info(f"    Analyst: {analyst['analyst_score']}점 | {analyst['confidence']} | {analyst.get('signals_fired', [])}")

        # ── Agent 2: Devil ───────────────────────────────────────
        devil = agent_devil(ticker, info, tech, macro, aria, analyst)
        log.info(f"    Devil: {devil['verdict']} | {devil['devil_score']}점 | TK:{devil['thesis_killer_hit']}")

        # ── Final 판단 ───────────────────────────────────────────
        final = _final_judgment(analyst, devil)
        scanned += 1

        log.info(f"    Final: {final['final_score']:.0f}점 | {final['signal_type']} | is_entry={final['is_entry']}")

        results.append({
            "ticker":        ticker,
            "name":          info["name"],
            "final_score":   final["final_score"],
            "signal_type":   final["signal_type"],
            "devil_verdict":  devil.get("verdict", ""),
            "rsi":           tech["rsi"],
            "is_portfolio":  info.get("portfolio", True),
            "aria_reason":   info.get("reason", ""),
        })

        # ── 알림 발송 ─────────────────────────────────────────────
        if final["is_entry"] and final["final_score"] >= ALERT_THRESHOLD:
            msg = _build_alert_message(ticker, info, tech, analyst, devil, final, aria)
            ok  = _send_telegram(msg)
            if ok:
                _set_cooldown(ticker)
                alerted += 1
                log.info(f"    ✅ 텔레그램 발송 완료")

        # ── 로그 저장 (Evolution 학습용) ──────────────────────────
        _save_log({
            "timestamp":        now_kst.isoformat(),
            "ticker":           ticker,
            "name":             info["name"],
            "market":           market,
            # 기술 지표
            "price_at_scan":    tech["price"],
            "rsi":              tech["rsi"],
            "bb_pos":           tech["bb_pos"],
            "vol_ratio":        tech["vol_ratio"],
            # 매크로
            "vix":              macro["fred"].get("vix"),
            "hy_spread":        macro["fred"].get("hy_spread"),
            "yield_curve":      macro["fred"].get("yield_curve"),
            # ARIA 컨텍스트
            "aria_regime":      aria["regime"],
            "aria_sentiment":   aria["sentiment_score"],
            "aria_trend":       aria["trend"],
            # Analyst
            "analyst_score":    analyst["analyst_score"],
            "analyst_confidence": analyst.get("confidence",""),
            "signals_fired":    analyst.get("signals_fired", []),
            "bull_case":        analyst.get("bull_case",""),
            # Devil
            "devil_score":      devil["devil_score"],
            "devil_verdict":    devil.get("verdict",""),
            "devil_objections": devil.get("objections", []),
            "thesis_killer_hit": devil.get("thesis_killer_hit", False),
            "killer_detail":    devil.get("killer_detail",""),
            # Final
            "final_score":      final["final_score"],
            "signal_type":      final["signal_type"],
            "is_entry":         final["is_entry"],
            "reason":           final.get("reason",""),
            # 결과 (Evolution이 4시간 후 채움)
            "alerted":          final["is_entry"] and final["final_score"] >= ALERT_THRESHOLD,
            "outcome_checked":  False,
            "outcome_price":    None,
            "outcome_pct":      None,
            "outcome_correct":  None,
        })

    log.info(f"📡 완료 | 분석 {scanned}종목 | 알림 {alerted}건")

    # 장이 열려있는데 타점 없으면 요약 발송
    any_open = (us_open or kr_open) or force
    if any_open and alerted == 0 and scanned > 0:
        _send_telegram(_build_summary_message(results, macro, aria))

    return {"scanned": scanned, "alerted": alerted}
