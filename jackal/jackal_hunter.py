"""
jackal_hunter.py
Jackal Hunter — ARIA 뉴스 기반 단기 스윙 타점 탐색

Jackal Scanner (포트폴리오 모니터링)와 완전 독립.
오늘의 ARIA 시장 분석에서 스윙 기회를 찾는다.

흐름:
  1. ARIA 최신 분석 읽기 (morning_baseline.json)
  2. Haiku + 웹서치: 뉴스에서 스윙 가능 종목 추출
  3. yfinance: 기술지표 계산
  4. Analyst (Haiku): 1~5일 반등 근거
  5. Devil   (Haiku): 반박
  6. Final: 68점 이상이면 텔레그램
  7. hunt_log.json 저장 → Evolution 학습

Scanner와의 차이:
  - Scanner: 내 포트폴리오 종목 모니터링
  - Hunter: ARIA 뉴스에서 새 스윙 기회 발굴
"""

import os
import sys
import json
import re
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx
import yfinance as yf
from anthropic import Anthropic

os.environ["PYTHONIOENCODING"] = "utf-8"
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

log = logging.getLogger("jackal_hunter")

KST   = timezone(timedelta(hours=9))
_BASE = Path(__file__).parent

HUNT_LOG_FILE  = _BASE / "hunt_log.json"
HUNT_COOL_FILE = _BASE / "hunt_cooldown.json"
ARIA_BASELINE  = Path("data") / "morning_baseline.json"
ARIA_MEMORY    = Path("data") / "memory.json"

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
MODEL_H          = os.environ.get("SUBAGENT_MODEL", "claude-haiku-4-5-20251001")

MAX_TICKERS     = 5
ALERT_THRESHOLD = 68    # Scanner(65)보다 높게 — 신규 종목이라 더 엄격
HUNT_COOLDOWN_H = 6     # 동일 종목 재알림 방지

# 내 포트폴리오 (중복 제외용)
MY_PORTFOLIO = {"NVDA", "AVGO", "SCHD", "000660.KS", "005930.KS", "035720.KS", "466920.KS"}


# ══════════════════════════════════════════════════════════════════
# ARIA 컨텍스트
# ══════════════════════════════════════════════════════════════════

def _load_aria_context() -> dict:
    ctx = {
        "one_line": "", "regime": "",
        "top_headlines": [], "key_inflows": [],
        "key_outflows": [], "thesis_killers": [],
        "actionable": [],
    }
    try:
        if ARIA_BASELINE.exists():
            b = json.loads(ARIA_BASELINE.read_text(encoding="utf-8"))
            ctx["one_line"]       = b.get("one_line_summary", "")
            ctx["regime"]         = b.get("market_regime", "")
            ctx["top_headlines"]  = [h.get("headline","") for h in b.get("top_headlines",[])[:5]]
            ctx["key_inflows"]    = [i.get("zone","") for i in b.get("inflows",[])[:3]]
            ctx["key_outflows"]   = [o.get("zone","") for o in b.get("outflows",[])[:3]]
            ctx["thesis_killers"] = b.get("thesis_killers", [])
            ctx["actionable"]     = b.get("actionable_watch", [])[:5]
    except Exception as e:
        log.warning(f"  ARIA baseline 로드 실패: {e}")
    return ctx


# ══════════════════════════════════════════════════════════════════
# Step 1: Haiku + 웹서치 → 스윙 후보 추출
# ══════════════════════════════════════════════════════════════════

def _find_swing_candidates(aria: dict) -> list:
    headlines = "\n".join(f"  - {h}" for h in aria["top_headlines"] if h)
    inflows   = ", ".join(aria["key_inflows"]) or "없음"
    watchlist = "\n".join(f"  - {a}" for a in aria["actionable"] if a)
    portfolio = ", ".join(MY_PORTFOLIO)

    prompt = f"""당신은 단기 스윙 트레이더입니다.
오늘 ARIA 시장 분석을 보고 1~5일 기술적 반등 가능성이 높은 종목을 찾으세요.

[오늘 ARIA 분석]
레짐: {aria['regime']}
요약: {aria['one_line'][:80]}

주요 헤드라인:
{headlines}

유입 섹터: {inflows}

ARIA 관심 종목:
{watchlist}

웹서치로 확인하세요:
1. 헤드라인에서 언급된 구체적 기업 중 최근 급락 종목
2. 섹터 수혜 종목 중 기술적 과매도 상태인 것
3. 뉴스 악재가 과반영되어 반등 가능한 종목

조건:
- yfinance 조회 가능한 실제 티커만
- 미국: TICKER (예: TSM, AAPL, MSFT)
- 한국: 000000.KS (예: 012450.KS)
- 이미 내가 보유한 종목 제외: {portfolio}
- 최대 {MAX_TICKERS}개

JSON만 반환:
{{
  "candidates": [
    {{"ticker": "TSM", "name": "TSMC", "market": "US", "currency": "$",
      "hunt_reason": "TSMC 어닝 서프라이즈 후 눌림목 — AI 수요 지속 확인"}}
  ]
}}"""

    try:
        client = Anthropic()
        resp = client.messages.create(
            model=MODEL_H, max_tokens=800,
            tools=[{"type": "web_search_20250305", "name": "web_search"}],
            messages=[{"role": "user", "content": prompt}],
        )
        full = "".join(
            getattr(b, "text", "") for b in resp.content
        )
        m = re.search(r"\{[\s\S]*\}", re.sub(r"```(?:json)?|```", "", full).strip())
        if not m:
            return []
        data = json.loads(m.group())
        cands = [c for c in data.get("candidates", []) if c.get("ticker") not in MY_PORTFOLIO]
        log.info(f"  Hunter 후보: {[c['ticker'] for c in cands]}")
        return cands[:MAX_TICKERS]
    except Exception as e:
        log.error(f"  후보 탐색 실패: {e}")
        return []


# ══════════════════════════════════════════════════════════════════
# Step 2: 기술지표
# ══════════════════════════════════════════════════════════════════

def _get_technicals(ticker: str) -> dict | None:
    try:
        hist = yf.Ticker(ticker).history(period="65d", interval="1d")
        if len(hist) < 22:
            return None
        close  = hist["Close"]
        volume = hist["Volume"]
        price  = float(close.iloc[-1])

        delta = close.diff()
        gain  = delta.clip(lower=0).rolling(14).mean()
        loss  = (-delta.clip(upper=0)).rolling(14).mean()
        rsi   = float((100 - 100 / (1 + gain / loss)).iloc[-1])

        ma20   = float(close.rolling(20).mean().iloc[-1])
        ma50   = float(close.rolling(50).mean().iloc[-1]) if len(close) >= 50 else None
        std20  = float(close.rolling(20).std().iloc[-1])
        bb_pos = (price - (ma20 - 2*std20)) / (4*std20) * 100 if std20 > 0 else 50

        avg_vol   = float(volume.iloc[-6:-1].mean()) if len(volume) >= 6 else float(volume.mean())
        vol_ratio = round(float(volume.iloc[-1]) / avg_vol, 2) if avg_vol > 0 else 1.0

        def chg(n):
            return round((price - float(close.iloc[-n-1])) / float(close.iloc[-n-1]) * 100, 2) \
                   if len(close) > n else 0

        return {
            "price": round(price, 2), "change_1d": chg(1),
            "change_3d": chg(3), "change_5d": chg(5),
            "rsi": round(rsi, 1), "ma20": round(ma20, 2),
            "ma50": round(ma50, 2) if ma50 else None,
            "bb_pos": round(bb_pos, 1), "vol_ratio": vol_ratio,
        }
    except Exception as e:
        log.error(f"  {ticker} 기술지표 실패: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
# Step 3: Analyst (스윙 전용)
# ══════════════════════════════════════════════════════════════════

def _analyst_swing(ticker: str, name: str, tech: dict,
                   hunt_reason: str, aria: dict, cur: str) -> dict:
    price_str = f"{tech['price']:,.2f}" if cur == "$" else f"{tech['price']:,.0f}"
    tk_events = [tk.get("event","") for tk in aria.get("thesis_killers",[])[:3]]

    prompt = f"""당신은 단기 스윙 트레이더 분석가입니다.
{name} ({ticker})의 1~5일 기술적 반등 가능성을 판단하세요. JSON만 반환하세요.

[발굴 이유] {hunt_reason}

[기술 지표]
현재가: {cur}{price_str}
변화: 1일{tech['change_1d']:+.1f}% / 3일{tech['change_3d']:+.1f}% / 5일{tech['change_5d']:+.1f}%
RSI: {tech['rsi']} | 볼린저: {tech['bb_pos']:.0f}% | 거래량: {tech['vol_ratio']:.1f}x
MA20: {cur}{tech['ma20']} | MA50: {cur}{tech.get('ma50','N/A')}

[시장] 레짐: {aria['regime']} | TK 주의: {', '.join(tk_events) or '없음'}

판단 기준:
- RSI≤35 + BB≤20%: 강한 반등 신호
- 3~5일 -5%이상 급락 + 거래량 1.5x: 투매 소진
- 섹터 유입 + 개별 악재 과반영: 역발상 매수

{{"analyst_score": 0~100, "swing_setup": "강한반등/반등가능/중립/추가하락",
  "signals_fired": ["rsi_oversold","bb_touch","volume_climax","sector_rebound","momentum_dip"],
  "entry_zone": "가격범위", "target_1d": "숫자", "target_5d": "숫자",
  "stop_loss": "숫자", "bull_case": "반등 근거 2줄"}}"""

    try:
        resp = Anthropic().messages.create(
            model=MODEL_H, max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = re.sub(r"```(?:json)?|```", "", resp.content[0].text).strip()
        m   = re.search(r"\{[\s\S]*\}", raw)
        r   = json.loads(m.group()) if m else {}
        r["analyst_score"] = int(r.get("analyst_score", 50))
        r.setdefault("swing_setup",  "중립")
        r.setdefault("signals_fired", [])
        r.setdefault("bull_case",    "")
        r.setdefault("entry_zone",   "")
        r.setdefault("target_1d",    "")
        r.setdefault("target_5d",    "")
        r.setdefault("stop_loss",    "")
        return r
    except Exception as e:
        log.error(f"  Analyst 실패: {e}")
        return {"analyst_score": 50, "swing_setup": "중립", "signals_fired": [],
                "bull_case": "", "entry_zone": "", "target_1d": "", "target_5d": "", "stop_loss": ""}


# ══════════════════════════════════════════════════════════════════
# Step 4: Devil (스윙 전용)
# ══════════════════════════════════════════════════════════════════

def _devil_swing(ticker: str, tech: dict, analyst: dict, aria: dict, cur: str) -> dict:
    price_str = f"{tech['price']:,.2f}" if cur == "$" else f"{tech['price']:,.0f}"
    tk_text   = "".join(
        f"\n  • {tk.get('event','')}: {tk.get('invalidates_if','')}"
        for tk in aria.get("thesis_killers",[])[:2] if tk.get("invalidates_if")
    )

    prompt = f"""반드시 {ticker} ({cur}{price_str}) 단기 반등을 반박하세요. JSON만 반환하세요.

[Analyst 주장] 점수:{analyst['analyst_score']} | {analyst.get('swing_setup','')}
근거: {analyst.get('bull_case','')[:60]}
신호: {', '.join(analyst.get('signals_fired',[]))}

RSI:{tech['rsi']} | BB:{tech['bb_pos']:.0f}% | 5일:{tech['change_5d']:+.1f}%
Thesis Killer:{tk_text or ' 없음'} | 레짐:{aria['regime']}

실패 패턴: 구조적 하락/레짐 위험회피/거래량 없는 과매도/데드캣

{{"devil_score": 0~100, "verdict": "동의/부분동의/반대",
  "main_risk": "반등 실패 이유 1줄",
  "is_dead_cat": true/false, "thesis_killer_hit": true/false}}"""

    try:
        resp = Anthropic().messages.create(
            model=MODEL_H, max_tokens=250,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = re.sub(r"```(?:json)?|```", "", resp.content[0].text).strip()
        m   = re.search(r"\{[\s\S]*\}", raw)
        r   = json.loads(m.group()) if m else {}
        r["devil_score"] = int(r.get("devil_score", 30))
        r.setdefault("verdict",           "부분동의")
        r.setdefault("main_risk",         "")
        r.setdefault("thesis_killer_hit", False)
        r.setdefault("is_dead_cat",       False)
        return r
    except Exception as e:
        log.error(f"  Devil 실패: {e}")
        return {"devil_score": 30, "verdict": "부분동의", "main_risk": "",
                "thesis_killer_hit": False, "is_dead_cat": False}


# ══════════════════════════════════════════════════════════════════
# Final 판단
# ══════════════════════════════════════════════════════════════════

def _final(analyst: dict, devil: dict) -> dict:
    if devil.get("thesis_killer_hit") or devil.get("is_dead_cat"):
        return {"final_score": 20, "is_entry": False, "label": "🚫 데드캣/TK 발동"}

    w       = {"동의": 1.0, "부분동의": 0.72, "반대": 0.45}.get(devil.get("verdict","부분동의"), 0.72)
    penalty = max(0, (devil.get("devil_score", 30) - 30) * 0.18)
    score   = round(max(0, min(100, analyst.get("analyst_score", 50) * w - penalty)), 1)
    setup   = analyst.get("swing_setup", "중립")

    is_entry = score >= ALERT_THRESHOLD and devil.get("verdict") != "반대" and setup != "추가하락"
    label    = {"강한반등":"🔥 강한 반등 타점","반등가능":"🟢 반등 가능",
                "중립":"⚪ 중립","추가하락":"🔴 추가 하락 경고"}.get(setup, "⚪ 중립")

    return {"final_score": score, "is_entry": is_entry, "label": label}


# ══════════════════════════════════════════════════════════════════
# 쿨다운 / 텔레그램 / 로그
# ══════════════════════════════════════════════════════════════════

def _is_on_cooldown(ticker: str) -> bool:
    if not HUNT_COOL_FILE.exists():
        return False
    try:
        cd = json.loads(HUNT_COOL_FILE.read_text(encoding="utf-8"))
        last = cd.get(ticker)
        return bool(last) and \
               (datetime.now() - datetime.fromisoformat(last)).total_seconds() / 3600 < HUNT_COOLDOWN_H
    except Exception:
        return False

def _set_cooldown(ticker: str):
    cd: dict = {}
    if HUNT_COOL_FILE.exists():
        try:
            cd = json.loads(HUNT_COOL_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    cd[ticker] = datetime.now().isoformat()
    HUNT_COOL_FILE.write_text(json.dumps(cd, ensure_ascii=False), encoding="utf-8")

def _send_telegram(text: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(text); return False
    try:
        r = httpx.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10,
        )
        return r.json().get("ok", False)
    except Exception as e:
        log.error(f"  텔레그램 오류: {e}"); return False

def _build_alert(ticker, name, tech, analyst, devil, final, hunt_reason, cur):
    now_str   = datetime.now(KST).strftime("%m/%d %H:%M")
    price_str = f"{tech['price']:,.2f}" if cur == "$" else f"{tech['price']:,.0f}"
    dv_icon   = {"동의":"✅","부분동의":"⚠️","반대":"❌"}.get(devil.get("verdict",""), "")
    return (
        f"🎯 <b>Jackal Hunter — 스윙 타점</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>{name} ({ticker})</b>\n"
        f"💰 {cur}{price_str}  1일:{tech['change_1d']:+.1f}%  5일:{tech['change_5d']:+.1f}%\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 Hunter 점수: <b>{final['final_score']:.0f}/100</b>  {final['label']}\n"
        f"📊 RSI {tech['rsi']} | BB {tech['bb_pos']:.0f}% | 거래량 {tech['vol_ratio']:.1f}x\n"
        f"💡 {hunt_reason[:55]}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🐂 {analyst.get('bull_case','')[:55]}\n"
        f"🔴 Devil {dv_icon}: {devil.get('main_risk','')[:45]}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 진입: {analyst.get('entry_zone','')}  "
        f"📈 5일목표: {analyst.get('target_5d','')}  "
        f"🛑 손절: {analyst.get('stop_loss','')}\n"
        f"⏰ {now_str} KST | Jackal Hunter × ARIA"
    )

def _save_log(entry: dict):
    logs: list = []
    if HUNT_LOG_FILE.exists():
        try:
            logs = json.loads(HUNT_LOG_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    logs.append(entry)
    HUNT_LOG_FILE.write_text(json.dumps(logs[-300:], ensure_ascii=False, indent=2), encoding="utf-8")


# ══════════════════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════════════════

def run_hunt(force: bool = False) -> dict:
    now_kst = datetime.now(KST)
    log.info(f"🎯 Jackal Hunter | {now_kst.strftime('%Y-%m-%d %H:%M KST')}")

    if not ARIA_BASELINE.exists():
        log.info("  ARIA baseline 없음 — 스킵 (MORNING 실행 후 동작)")
        return {"hunted": 0, "alerted": 0}

    aria = _load_aria_context()
    if not aria["regime"]:
        log.info("  ARIA 레짐 없음 — 스킵")
        return {"hunted": 0, "alerted": 0}

    log.info(f"  ARIA 레짐: {aria['regime']} | 헤드라인 {len(aria['top_headlines'])}건")

    candidates = _find_swing_candidates(aria)
    if not candidates:
        log.info("  스윙 후보 없음")
        return {"hunted": 0, "alerted": 0}

    hunted = alerted = 0

    for cand in candidates:
        ticker      = cand.get("ticker", "")
        name        = cand.get("name", ticker)
        market      = cand.get("market", "US")
        cur         = cand.get("currency", "$")
        hunt_reason = cand.get("hunt_reason", "")

        if not ticker or _is_on_cooldown(ticker):
            continue

        tech = _get_technicals(ticker)
        if not tech:
            continue

        log.info(f"  {ticker}: RSI={tech['rsi']} BB={tech['bb_pos']:.0f}% "
                 f"5d={tech['change_5d']:+.1f}% vol={tech['vol_ratio']:.1f}x")

        analyst = _analyst_swing(ticker, name, tech, hunt_reason, aria, cur)
        devil   = _devil_swing(ticker, tech, analyst, aria, cur)
        final   = _final(analyst, devil)

        log.info(f"    A:{analyst.get('analyst_score',50)} D:{devil.get('devil_score',30)} "
                 f"({devil.get('verdict','?')}) → F:{final['final_score']:.0f} {final['label']}")

        hunted += 1

        if final["is_entry"]:
            ok = _send_telegram(_build_alert(ticker, name, tech, analyst, devil, final, hunt_reason, cur))
            if ok:
                _set_cooldown(ticker)
                alerted += 1
                log.info("    ✅ Hunter 알림 발송")

        _save_log({
            "timestamp":         now_kst.isoformat(),
            "ticker":            ticker, "name": name, "market": market,
            "hunt_reason":       hunt_reason,
            "price_at_hunt":     tech["price"],
            "rsi":               tech["rsi"], "bb_pos": tech["bb_pos"],
            "change_5d":         tech["change_5d"], "vol_ratio": tech["vol_ratio"],
            "aria_regime":       aria["regime"], "aria_inflows": aria["key_inflows"],
            "analyst_score":     analyst["analyst_score"],
            "swing_setup":       analyst.get("swing_setup",""),
            "signals_fired":     analyst.get("signals_fired",[]),
            "devil_verdict":     devil.get("verdict",""),
            "devil_score":       devil["devil_score"],
            "is_dead_cat":       devil.get("is_dead_cat", False),
            "thesis_killer_hit": devil.get("thesis_killer_hit", False),
            "final_score":       final["final_score"],
            "is_entry":          final["is_entry"],
            "alerted":           final["is_entry"],
            # 5일 후 Evolution이 채움
            "outcome_checked":   False,
            "price_5d_later":    None,
            "outcome_pct":       None,
            "outcome_correct":   None,
        })

    log.info(f"🎯 Hunter 완료 | 분석 {hunted}종목 | 알림 {alerted}건")
    return {"hunted": hunted, "alerted": alerted}
