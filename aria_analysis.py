"""
aria_analysis.py — ARIA 분석 모듈 통합
포함: sentiment · portfolio · rotation · baseline · verifier · weights · lessons

[수정]
- MODEL: 환경변수 ARIA_MODEL 지원
- run_verification: [-1] 인덱스 제거, ARIA_FORCE_VERIFY 환경변수 추가
"""
import os
import sys
import json
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

import anthropic

KST = timezone(timedelta(hours=9))

from aria_paths import (
    SENTIMENT_FILE, ROTATION_FILE, BASELINE_FILE,
    ACCURACY_FILE, MEMORY_FILE, WEIGHTS_FILE,
    LESSONS_FILE, PATTERN_DB_FILE,
)

API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL   = os.environ.get("ARIA_MODEL", os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6"))
client  = anthropic.Anthropic(api_key=API_KEY)


def _now() -> datetime:
    return datetime.now(KST)

def _today() -> str:
    return _now().strftime("%Y-%m-%d")

def _load(path: Path, default=None):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return default if default is not None else {}

def _save(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ══════════════════════════════════════════════════════════════════════════════
# WEIGHTS
# ══════════════════════════════════════════════════════════════════════════════
_DEFAULT_WEIGHTS = {
    "version": 1, "last_updated": "", "total_learning_cycles": 0,
    "sentiment": {
        "시장레짐": 1.0, "추세방향": 1.0, "변동성지수": 1.2,
        "자금흐름": 1.0, "반론강도": 0.8, "한국시장": 0.8, "숨은시그널": 0.7,
    },
    "prediction_confidence": {
        "금리": 1.0, "환율": 1.0, "주식": 1.0,
        "지정학": 0.7, "원자재": 1.0, "기업": 1.0, "기타": 0.8,
    },
    "learning_log": [],
    "component_accuracy": {
        "시장레짐":   {"correct": 0, "total": 0},
        "추세방향":   {"correct": 0, "total": 0},
        "변동성지수": {"correct": 0, "total": 0},
        "자금흐름":   {"correct": 0, "total": 0},
    },
}


def load_weights() -> dict:
    saved = _load(WEIGHTS_FILE)
    if not saved:
        return _DEFAULT_WEIGHTS.copy()
    for key, val in _DEFAULT_WEIGHTS.items():
        if key not in saved:
            saved[key] = val
        elif isinstance(val, dict):
            for k2, v2 in val.items():
                if k2 not in saved[key]:
                    saved[key][k2] = v2
    return saved


def get_sentiment_weights() -> dict:
    return load_weights().get("sentiment", _DEFAULT_WEIGHTS["sentiment"])


def update_weights_from_accuracy(accuracy_data: dict) -> list:
    """history_by_category(날짜별 스냅샷)에서 최근 30일 데이터만 집계해 가중치 업데이트."""
    weights = load_weights()
    conf    = weights.get("prediction_confidence", {})

    cutoff = (_now() - timedelta(days=30)).strftime("%Y-%m-%d")
    hist   = [
        h for h in accuracy_data.get("history_by_category", [])
        if h.get("date", "") >= cutoff
    ]
    if not hist:
        return []

    recent: dict = {}
    for snap in hist:
        for cat, v in snap.get("by_category", {}).items():
            if cat not in recent:
                recent[cat] = {"correct": 0, "total": 0}
            recent[cat]["correct"] += v.get("correct", 0)
            recent[cat]["total"]   += v.get("total", 0)

    changes = []
    for cat, v in recent.items():
        if v["total"] < 3:
            continue
        acc     = v["correct"] / v["total"]
        old_w   = conf.get(cat, 1.0)
        adj     = 0.05 if acc >= 0.7 else -0.05 if acc <= 0.4 else 0.0
        new_w   = round(max(0.3, min(2.0, old_w + adj)), 3)
        if abs(new_w - old_w) >= 0.001:
            conf[cat] = new_w
            changes.append(f"{cat}: {old_w:.3f}→{new_w:.3f} (acc={acc:.1%})")

    if changes:
        weights["prediction_confidence"] = conf
        weights["last_updated"] = _today()
        weights["total_learning_cycles"] = weights.get("total_learning_cycles", 0) + 1
        _save(WEIGHTS_FILE, weights)

    return changes


# ══════════════════════════════════════════════════════════════════════════════
# SENTIMENT
# ══════════════════════════════════════════════════════════════════════════════

def calculate_sentiment(report: dict, market_data: dict = None) -> dict:
    weights = get_sentiment_weights()
    regime  = report.get("market_regime", "")
    trend   = report.get("trend_phase", "")
    devil   = report.get("counterarguments", [])
    hidden  = report.get("hidden_signals", [])
    korea   = report.get("korea_focus", {})
    vi      = report.get("volatility_index", {})

    vix_val    = None
    vkospi_val = None
    if market_data:
        try: vix_val    = float(str(market_data.get("vix", "")).replace(",", ""))
        except Exception: pass
        try: vkospi_val = float(str(market_data.get("vkospi", "")).replace(",", ""))
        except Exception: pass
    if vix_val is None:
        try: vix_val = float(str(vi.get("vix", "20")).replace(",", ""))
        except Exception: vix_val = 20.0
    if vkospi_val is None:
        try: vkospi_val = float(str(vi.get("vkospi", "15")).replace(",", ""))
        except Exception: vkospi_val = 15.0

    comps = {}

    # 레짐
    reg_s = 70 if "선호" in regime else 30 if "회피" in regime else 50 if "전환" in regime else 50
    comps["시장레짐"] = round(reg_s * weights.get("시장레짐", 1.0))

    # 추세
    tr_s = 70 if "상승" in trend else 30 if "하락" in trend else 50
    comps["추세방향"] = round(tr_s * weights.get("추세방향", 1.0))

    # VIX
    if vix_val < 15: vi_s = 70
    elif vix_val < 20: vi_s = 60
    elif vix_val < 25: vi_s = 45
    elif vix_val < 30: vi_s = 35
    else: vi_s = 20
    comps["변동성지수"] = round(vi_s * weights.get("변동성지수", 1.2))

    # 자금흐름
    inflows  = len(report.get("inflows", []))
    outflows = len(report.get("outflows", []))
    fl_s = 60 if inflows > outflows else 40 if outflows > inflows else 50
    comps["자금흐름"] = round(fl_s * weights.get("자금흐름", 1.0))

    # 반론강도
    high_risk = sum(1 for d in devil if d.get("risk_level") == "높음")
    ca_s = 40 if high_risk >= 2 else 55 if high_risk == 1 else 65
    comps["반론강도"] = round(ca_s * weights.get("반론강도", 0.8))

    # 한국시장
    kor_assess = korea.get("assessment", "")
    ko_s = 60 if "긍정" in kor_assess or "강세" in kor_assess else 40 if "부정" in kor_assess or "약세" in kor_assess else 50
    comps["한국시장"] = round(ko_s * weights.get("한국시장", 0.8))

    # 숨은 시그널
    hi_conf = sum(1 for h in hidden if h.get("confidence") == "높음")
    hs_s = 65 if hi_conf >= 2 else 58 if hi_conf == 1 else 50
    comps["숨은시그널"] = round(hs_s * weights.get("숨은시그널", 0.7))

    # FRED 점수
    fred_score = 50
    fred_indicators = {}
    if market_data:
        hy = market_data.get("hy_spread")
        yc = market_data.get("yield_curve")
        cs = market_data.get("consumer_sent")
        if hy is not None:
            try:
                hy_f = float(hy)
                fred_indicators["hy_spread"] = hy_f
                if hy_f < 3: fred_score += 5
                elif hy_f > 5: fred_score -= 10
            except Exception: pass
        if yc is not None:
            try:
                yc_f = float(yc)
                fred_indicators["yield_curve"] = yc_f
                if yc_f < 0: fred_score -= 8
                elif yc_f > 1: fred_score += 5
            except Exception: pass
        if cs is not None:
            try:
                cs_f = float(cs)
                fred_indicators["consumer_sent"] = cs_f
                if cs_f > 80: fred_score += 5
                elif cs_f < 60: fred_score -= 5
            except Exception: pass
        fred_score = max(20, min(80, fred_score))

    total_w = sum(weights.get(k, 1.0) for k in comps)
    raw     = sum(comps.values()) / max(total_w, 1)
    raw     = max(0, min(100, raw))

    # Fear&Greed 블렌딩
    fg_raw = None
    if market_data:
        try: fg_raw = float(str(market_data.get("fear_greed_value", "")).replace(",", ""))
        except Exception: pass
    if fg_raw is None:
        try: fg_raw = float(str(vi.get("fear_greed", "50")).replace(",", ""))
        except Exception: fg_raw = 50.0

    internal_raw = raw
    score = round(raw * 0.7 + fg_raw * 0.3 + (fred_score - 50) * 0.1)
    score = max(0, min(100, score))

    divergence     = abs(internal_raw - (fg_raw or 50))
    divergence_flag = divergence >= 25

    if score <= 20:   level, emoji = "극단공포", "😱"
    elif score <= 40: level, emoji = "공포",     "😰"
    elif score <= 60: level, emoji = "중립",     "😐"
    elif score <= 80: level, emoji = "탐욕",     "😏"
    else:             level, emoji = "극단탐욕", "🤑"

    return {
        "date": _today(), "score": score, "level": level, "emoji": emoji,
        "components": comps, "regime": regime, "trend": trend,
        "vix_level": vi.get("level", ""), "vix_val": vix_val,
        "vkospi_val": vkospi_val, "fear_greed": fg_raw,
        "internal_raw": internal_raw, "divergence": divergence_flag,
        "fred_score": fred_score, "fred_indicators": fred_indicators,
    }


def _analyze_trend(history: list) -> dict:
    if len(history) < 2:
        return {"direction": "neutral", "change": 0, "avg_7d": 50,
                "min_30d": 50, "max_30d": 50, "avg_30d": 50}
    sc7  = [h["score"] for h in history[-7:]]
    sc30 = [h["score"] for h in history[-30:]]
    half = len(sc7) // 2
    chg  = round(sum(sc7[half:]) / max(len(sc7) - half, 1)
                 - sum(sc7[:half]) / max(half, 1), 1)
    return {
        "direction": "improving" if chg > 5 else "deteriorating" if chg < -5 else "stable",
        "change": chg,
        "avg_7d":  round(sum(sc7) / len(sc7), 1),
        "min_30d": min(sc30), "max_30d": max(sc30),
        "avg_30d": round(sum(sc30) / len(sc30), 1),
    }


def run_sentiment(report: dict, market_data: dict = None) -> dict:
    data    = _load(SENTIMENT_FILE, {"history": [], "current": None})
    new     = calculate_sentiment(report, market_data)
    history = data.get("history", [])

    _BLEND_WEIGHT = {"MORNING": 1.0, "AFTERNOON": 0.7, "EVENING": 0.8, "DAWN": 0.5}
    mode       = report.get("mode", "MORNING")
    new_weight = _BLEND_WEIGHT.get(mode, 0.6)

    try:
        from aria_data import load_market_data
        md    = load_market_data()
        sp_chg = float(str(md.get("sp500_change", "0")).replace("%", "").replace("+", ""))
        if sp_chg <= -3:
            new["score"] = min(new["score"] + 8, 100)
            new["rebound_bias"] = True
    except Exception:
        pass

    if history:
        prev      = history[-1]
        prev_score = prev.get("score", new["score"])
        blended   = round(prev_score * (1 - new_weight) + new["score"] * new_weight)
        new["score"] = max(0, min(100, blended))

    history = [h for h in history if h.get("date") != _today()]
    history.append(new)
    history = history[-90:]

    trend = _analyze_trend(history)
    data  = {"history": history, "current": new, "trend": trend,
             "last_updated": _now().isoformat()}
    _save(SENTIMENT_FILE, data)
    return new


# ══════════════════════════════════════════════════════════════════════════════
# PORTFOLIO
# ══════════════════════════════════════════════════════════════════════════════

def run_portfolio(report: dict, market_data: dict = None) -> dict:
    from aria_paths import PORTFOLIO_FILE
    portfolio = _load(PORTFOLIO_FILE, {"holdings": []})
    if not portfolio.get("holdings"):
        print("  포트폴리오 없음 — 스킵")
        return {}

    regime  = report.get("market_regime", "")
    inflows = [i.get("zone", "") for i in report.get("inflows", [])[:3]]
    outflows= [o.get("zone", "") for o in report.get("outflows", [])[:3]]

    assessments = []
    for h in portfolio["holdings"]:
        ticker = h.get("ticker_yf") or h.get("ticker", "")
        name   = h.get("name", ticker)
        sector = h.get("sector", "")
        signal = "neutral"
        if any(sector.lower() in i.lower() for i in inflows if sector):
            signal = "bullish"
        elif any(sector.lower() in o.lower() for o in outflows if sector):
            signal = "bearish"
        assessments.append({"ticker": ticker, "name": name, "signal": signal, "regime": regime})

    print(f"  포트폴리오 {len(assessments)}종목 평가 완료")
    return {"assessments": assessments}


# ══════════════════════════════════════════════════════════════════════════════
# ROTATION
# ══════════════════════════════════════════════════════════════════════════════

def run_rotation(report: dict) -> dict:
    data     = _load(ROTATION_FILE, {"ranking": [], "history": []})
    inflows  = report.get("inflows", [])
    outflows = report.get("outflows", [])

    scores: dict = {}
    for item in inflows:
        zone = item.get("zone", "")
        mom  = item.get("momentum", "")
        if zone:
            s = 3 if mom == "강함" else 2 if mom == "형성중" else 1
            scores[zone] = scores.get(zone, 0) + s
    for item in outflows:
        zone = item.get("zone", "")
        sev  = item.get("severity", "")
        if zone:
            s = -3 if sev == "높음" else -2 if sev == "보통" else -1
            scores[zone] = scores.get(zone, 0) + s

    ranking = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    rotation_signal = {}
    if len(ranking) >= 2:
        top    = ranking[0]
        bottom = ranking[-1]
        if top[1] > 0 and bottom[1] < 0:
            rotation_signal = {"from": bottom[0], "to": top[0],
                               "strength": "강함" if top[1] >= 3 else "보통"}

    history = data.get("history", [])
    history.append({"date": _today(), "ranking": ranking[:8],
                    "rotation_signal": rotation_signal})
    history = history[-30:]

    result = {"ranking": ranking, "rotation_signal": rotation_signal,
              "history": history, "last_updated": _today()}
    _save(ROTATION_FILE, result)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# BASELINE
# ══════════════════════════════════════════════════════════════════════════════

def save_baseline(report: dict, market_data: dict = None) -> None:
    baseline = {
        "date":             _today(),
        "one_line_summary": report.get("one_line_summary", ""),
        "market_regime":    report.get("market_regime", ""),
        "trend_phase":      report.get("trend_phase", ""),
        "confidence":       report.get("confidence_overall", ""),
        "top_headlines":    report.get("top_headlines", [])[:5],
        "inflows":          report.get("inflows", [])[:4],
        "outflows":         report.get("outflows", [])[:3],
        "thesis_killers":   report.get("thesis_killers", [])[:3],
        "actionable_watch": report.get("actionable_watch", [])[:5],
        "korea_focus":      report.get("korea_focus", {}),
        "hidden_signals":   report.get("hidden_signals", [])[:3],
    }
    if market_data:
        baseline["market_snapshot"] = {
            k: market_data.get(k)
            for k in ["sp500", "nasdaq", "vix", "kospi", "krw_usd",
                      "fear_greed_value", "fear_greed_rating"]
        }
    _save(BASELINE_FILE, baseline)


def build_baseline_context(memory: list) -> str:
    if not memory or len(memory) < 2:
        return ""
    prev = memory[-1]
    return (
        f"\n[어제 분석] {prev.get('analysis_date','')} "
        f"레짐={prev.get('market_regime','')} "
        f"요약={prev.get('one_line_summary','')[:50]}"
    )


def get_regime_drift(current_regime: str) -> str:
    data = _load(SENTIMENT_FILE, {})
    history = data.get("history", [])
    if len(history) < 3:
        return "STABLE"
    recent_regimes = [h.get("regime", "") for h in history[-3:]]
    if all(r == current_regime for r in recent_regimes):
        return "STABLE"
    if recent_regimes.count(current_regime) == 0:
        return "SHIFT"
    return "DRIFT"


# ══════════════════════════════════════════════════════════════════════════════
# VERIFICATION
# ══════════════════════════════════════════════════════════════════════════════

_VERIFIER_SYSTEM = """You are a financial prediction verifier.
Search for actual market outcomes and verify if predictions came true.
Return ONLY valid JSON. No markdown.
{"results":[{"event":"","verdict":"confirmed/invalidated/unclear","evidence":"","category":"금리/지정학/기업/기타"}]}"""


def _verify_price(thesis_killers: list, market_data: dict) -> list:
    """
    [Bug Fix] 백테스트 check_direction() 로직 적용.
    기존: 임계값 초과만 confirmed → dead zone 속출 → 방향 맞아도 오답 처리
    수정: 방향 일치 + ±0.3% 이상이면 partial confirmed 인정
          VIX / 환율 자동 unclear (정확도 22.8% / 0%)
    """
    import re as _re

    def _pct(v):
        try: return float(str(v or "0").replace("%", "").replace("+", ""))
        except: return 0.0

    nq  = _pct(market_data.get("nasdaq_change"))
    sp  = _pct(market_data.get("sp500_change"))
    ks  = _pct(market_data.get("kospi_change"))
    sk  = _pct(market_data.get("sk_hynix_change"))
    sam = _pct(market_data.get("samsung_change"))
    nv  = _pct(market_data.get("nvda_change"))

    def _extract_thr(text):
        nums = _re.findall(r"[+-]?\d+\.?\d*", str(text))
        return float(nums[0]) if nums else None

    def check_direction(chg: float, conf: str, inv: str) -> tuple:
        """
        방향 + 임계값으로 verdict 결정.
        임계값 완전 달성 → confirmed(100%)
        방향 일치 + ±0.3% 이상 → confirmed(partial)
        반대 방향 + ±0.3% 이상 → invalidated
        그 외 → unclear
        """
        thr = abs(_extract_thr(conf) or 1.0)
        c_l, i_l = conf.lower(), inv.lower()
        up   = any(w in c_l for w in ["상승","반등","올라","증가","+"])
        down = any(w in c_l for w in ["하락","급락","내려","감소","-"])

        if up:
            if chg >= thr:         return "confirmed",   f"실제 {chg:+.2f}% (예측 +{thr:.1f}% 이상)"
            if 0.3 <= chg < thr:   return "confirmed",   f"실제 {chg:+.2f}% (방향 일치, 수치 부분달성)"
            if chg <= -0.3:        return "invalidated", f"실제 {chg:+.2f}% (예측 반대)"
        if down:
            if chg <= -thr:        return "confirmed",   f"실제 {chg:+.2f}% (예측 -{thr:.1f}% 이하)"
            if -thr < chg <= -0.3: return "confirmed",   f"실제 {chg:+.2f}% (방향 일치, 수치 부분달성)"
            if chg >= 0.3:         return "invalidated", f"실제 {chg:+.2f}% (예측 반대)"

        return "unclear", f"변동 미미 ({chg:+.2f}%)"

    results = []
    for tk in thesis_killers:
        event    = tk.get("event", "")
        confirms = tk.get("confirms_if", "")
        invalids = tk.get("invalidates_if", "")
        ev_l     = event.lower()
        verdict, evidence, category = "unclear", "", tk.get("category", "기타")

        # 환율: 정확도 0% → 자동 제외
        if any(k in ev_l for k in ["환율","원달러","krw","원화"]):
            category = "환율"
            verdict, evidence = "unclear", "환율 예측 제외 (정확도 0%)"

        # VIX: 정확도 22.8% → 자동 제외
        elif any(k in ev_l for k in ["vix","변동성","공포지수"]):
            category = "VIX"
            verdict, evidence = "unclear", "VIX 예측 제외 (정확도 22.8%)"

        # 나스닥 / 기술주 / S&P
        elif any(k in ev_l for k in ["나스닥","nasdaq","기술주"]):
            category = "주식"
            verdict, evidence = check_direction(nq, confirms, invalids)
        elif any(k in ev_l for k in ["s&p","sp500","s&p500"]):
            category = "주식"
            verdict, evidence = check_direction(sp, confirms, invalids)

        # 코스피
        elif any(k in ev_l for k in ["코스피","kospi","한국 주식"]):
            category = "주식"
            verdict, evidence = check_direction(ks, confirms, invalids)

        # 개별 종목
        elif any(k in ev_l for k in ["sk하이닉스","하이닉스","sk hynix"]):
            category = "주식"
            verdict, evidence = check_direction(sk, confirms, invalids)
        elif any(k in ev_l for k in ["삼성전자","삼성"]):
            category = "주식"
            verdict, evidence = check_direction(sam, confirms, invalids)
        elif any(k in ev_l for k in ["엔비디아","nvidia","nvda"]):
            category = "주식"
            verdict, evidence = check_direction(nv, confirms, invalids)
        elif any(k in ev_l for k in ["반도체","semiconductor","hbm"]):
            category = "주식"
            chg = max([sk, nv], key=abs)
            verdict, evidence = check_direction(chg, confirms, invalids)

        results.append({
            "event":    event,
            "verdict":  verdict,
            "evidence": evidence,
            "category": category,
        })
    return results


def _ai_verify(unclear: list) -> list:
    if not unclear:
        return []
    full = ""
    with client.messages.stream(
        model=MODEL, max_tokens=1000, system=_VERIFIER_SYSTEM,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        messages=[{"role": "user",
                   "content": "Search and verify:\n" + json.dumps(unclear, ensure_ascii=False) + "\nReturn JSON."}]
    ) as s:
        for ev in s:
            t = getattr(ev, "type", "")
            if t == "content_block_start":
                blk = getattr(ev, "content_block", None)
                if blk and getattr(blk, "type", "") == "tool_use":
                    print("  Search: " + getattr(blk, "input", {}).get("query", ""))
            elif t == "content_block_delta":
                d = getattr(ev, "delta", None)
                if d and getattr(d, "type", "") == "text_delta":
                    full += d.text
    raw = re.sub(r"```json|```", "", full).strip()
    m   = re.search(r"\{[\s\S]*\}", raw)
    try:
        return json.loads(m.group()).get("results", []) if m else []
    except Exception:
        return []


def run_verification() -> dict:
    try:
        memory = _load(MEMORY_FILE, [])
        if not isinstance(memory, list):
            print("⚠️ memory.json 형식 오류 — 빈 메모리로 재시작")
            memory = []
    except Exception:
        print("⚠️ memory.json 손상 감지 — 빈 메모리로 재시작")
        memory = []

    accuracy = _load(ACCURACY_FILE, {
        "total": 0, "correct": 0, "by_category": {},
        "history": [], "weak_areas": [], "strong_areas": [],
    })

    if not memory:
        print("No previous analysis")
        return accuracy

    yesterday = memory[-1]
    today     = _today()

    # [Bug Fix 11] any()로 날짜 체크 + ARIA_FORCE_VERIFY 환경변수
    force_verify = os.environ.get("ARIA_FORCE_VERIFY", "").lower() == "true"
    already_done = any(h.get("date") == today for h in accuracy.get("history", []))
    if already_done and not force_verify:
        print(f"Already verified today ({today}) — set ARIA_FORCE_VERIFY=true to rerun")
        return accuracy

    tks = yesterday.get("thesis_killers", [])
    if not tks:
        print("No thesis killers to verify")
        return accuracy

    try:
        from aria_data import load_market_data
        md = load_market_data()
    except ImportError:
        md = {}

    print("Verifying " + str(len(tks)) + " predictions...")
    results = _verify_price(tks, md)

    unclear = [r for r in results if r["verdict"] == "unclear"]
    if unclear:
        print("[2단계] AI 보완 채점 (" + str(len(unclear)) + "개)")
        ai     = _ai_verify(unclear)
        ai_map = {r["event"]: r for r in ai}
        for r in results:
            if r["verdict"] == "unclear" and r["event"] in ai_map:
                r.update({k: ai_map[r["event"]].get(k, r[k])
                           for k in ["verdict", "evidence", "category"]})
    else:
        print("[2단계] unclear 없음 — AI 호출 스킵")

    changes = update_weights_from_accuracy(accuracy)
    if changes:
        print("Weight updates: " + str(len(changes)))

    judged  = [r for r in results if r["verdict"] != "unclear"]
    correct = [r for r in judged  if r["verdict"] == "confirmed"]

    def _is_direction_correct(r):
        return r["verdict"] == "confirmed"

    def _is_full_correct(r):
        if r["verdict"] != "confirmed":
            return False
        ev = r.get("evidence", "")
        return "임계 미달" not in ev and "방향 일치" not in ev

    dir_correct  = sum(1 for r in judged if _is_direction_correct(r))
    full_correct = sum(1 for r in judged if _is_full_correct(r))

    accuracy["total"]   += len(judged)
    accuracy["correct"] += len(correct)
    accuracy.setdefault("dir_total",   0)
    accuracy.setdefault("dir_correct", 0)
    accuracy["dir_total"]   += len(judged)
    accuracy["dir_correct"] += dir_correct

    def _strength(r):
        if r["verdict"] != "confirmed":
            return 0.0
        ev = r.get("evidence", "")
        return 0.5 if "임계 미달" in ev or "방향 일치" in ev else 1.0

    score_earned = sum(_strength(r) for r in judged)
    accuracy.setdefault("score_total",  0.0)
    accuracy.setdefault("score_earned", 0.0)
    accuracy["score_total"]  += len(judged)
    accuracy["score_earned"] += score_earned
    accuracy["score_accuracy"] = round(
        accuracy["score_earned"] / accuracy["score_total"] * 100, 1
    ) if accuracy["score_total"] > 0 else 0.0

    today_cat: dict = {}
    for r in judged:
        cat = r.get("category", "기타")
        if cat not in accuracy["by_category"]:
            accuracy["by_category"][cat] = {"total": 0, "correct": 0}
        accuracy["by_category"][cat]["total"] += 1
        if r["verdict"] == "confirmed":
            accuracy["by_category"][cat]["correct"] += 1
        if cat not in today_cat:
            today_cat[cat] = {"total": 0, "correct": 0}
        today_cat[cat]["total"] += 1
        if r["verdict"] == "confirmed":
            today_cat[cat]["correct"] += 1

    today_acc = round(len(correct) / len(judged) * 100, 1) if judged else 0
    dir_acc   = round(dir_correct / len(judged) * 100, 1) if judged else 0

    # 중복 날짜 방어 후 append
    accuracy["history"] = [h for h in accuracy["history"] if h.get("date") != today]
    accuracy["history"].append({
        "date": today, "total": len(judged),
        "correct": len(correct), "accuracy": today_acc,
        "dir_correct": dir_correct, "dir_accuracy": dir_acc,
        "full_correct": full_correct,
    })
    accuracy["history"] = sorted(accuracy["history"], key=lambda x: x.get("date", ""))[-90:]

    if "history_by_category" not in accuracy:
        accuracy["history_by_category"] = []
    accuracy["history_by_category"] = [
        h for h in accuracy["history_by_category"] if h.get("date") != today
    ]
    accuracy["history_by_category"].append({"date": today, "by_category": today_cat})
    accuracy["history_by_category"] = accuracy["history_by_category"][-90:]

    strong, weak = [], []
    for cat, s in accuracy["by_category"].items():
        if s["total"] >= 3:
            a = s["correct"] / s["total"] * 100
            if a >= 70: strong.append(cat + " (" + str(round(a)) + "%)")
            elif a <= 40: weak.append(cat + " (" + str(round(a)) + "%)")
    accuracy["strong_areas"] = strong
    accuracy["weak_areas"]   = weak

    d_total   = accuracy.get("dir_total", 0)
    d_correct = accuracy.get("dir_correct", 0)
    accuracy["dir_accuracy_pct"] = round(d_correct / d_total * 100, 1) if d_total > 0 else 0

    _save(ACCURACY_FILE, accuracy)
    _send_verification_report(results, accuracy, today_acc, dir_acc)
    print("Done. Today accuracy: " + str(today_acc) + "%")
    return accuracy


def _send_verification_report(results, accuracy, today_acc, dir_acc=0):
    try:
        from aria_notify import send_message
    except ImportError:
        return

    judged    = [r for r in results if r["verdict"] != "unclear"]
    total_acc = round(accuracy["correct"] / accuracy["total"] * 100, 1) if accuracy["total"] > 0 else 0
    d_pct     = accuracy.get("dir_accuracy_pct", 0)

    lines = ["<b>📋 어제 예측 채점</b>", "<code>" + _today() + "</code>", ""]
    for r in results:
        em = "✅" if r["verdict"] == "confirmed" else "❌" if r["verdict"] == "invalidated" else "❓"
        lines.append(em + " <b>" + r.get("event", "")[:40] + "</b>")
        if r.get("evidence"): lines.append("  <i>" + r["evidence"] + "</i>")
    lines += ["",
              "오늘: <b>" + str(today_acc) + "%</b> (" + str(len([r for r in results if r["verdict"] == "confirmed"])) + "/" + str(len(judged)) + ")",
              "  방향정확도: <b>" + str(dir_acc) + "%</b>",
              "누적 방향: <b>" + str(d_pct) + "%</b> | 종합: <b>" + str(total_acc) + "%</b> (" + str(accuracy["correct"]) + "/" + str(accuracy["total"]) + ")"]
    if accuracy.get("strong_areas"): lines.append("💪 강점: " + ", ".join(accuracy["strong_areas"][:3]))
    if accuracy.get("weak_areas"):   lines.append("⚠️ 약점: " + ", ".join(accuracy["weak_areas"][:3]))
    send_message("\n".join(lines))


# ══════════════════════════════════════════════════════════════════════════════
# LESSONS
# ══════════════════════════════════════════════════════════════════════════════

def load_lessons() -> dict:
    return _load(LESSONS_FILE, {"lessons": [], "total_lessons": 0, "last_updated": ""})


def add_lesson(source: str, category: str, lesson_text: str, severity: str = "medium"):
    data     = load_lessons()
    today    = _today()
    existing = next((l for l in data["lessons"] if l["date"] == today and l["category"] == category), None)
    if existing:
        existing["lesson"] += " / " + lesson_text
        existing["reinforced"] = existing.get("reinforced", 0) + 1
    else:
        data["lessons"].append({
            "date": today, "source": source, "category": category,
            "lesson": lesson_text, "severity": severity, "applied": 0, "reinforced": 0,
        })
        data["total_lessons"] += 1

    data["lessons"]      = sorted(data["lessons"], key=lambda x: x["date"], reverse=True)[:60]
    data["last_updated"] = today
    _save(LESSONS_FILE, data)


def get_active_lessons(max_lessons: int = 8) -> list:
    data    = load_lessons()
    lessons = data.get("lessons", [])
    today   = _today()
    expiry  = {"high": 30, "medium": 14, "low": 7}
    active  = []
    for l in lessons:
        days = expiry.get(l.get("severity", "medium"), 14)
        try:
            if (datetime.strptime(today, "%Y-%m-%d") - datetime.strptime(l["date"], "%Y-%m-%d")).days <= days:
                active.append(l)
        except Exception:
            active.append(l)

    def pri(l):
        return (3 if l["severity"] == "high" else 2 if l["severity"] == "medium" else 1) * 2 + l.get("reinforced", 0)
    sorted_l = sorted(active, key=pri, reverse=True)
    for l in sorted_l[:max_lessons]:
        l["applied"] = l.get("applied", 0) + 1
    _save(LESSONS_FILE, data)
    return sorted_l[:max_lessons]


def build_lessons_prompt(max_lessons: int = 6) -> str:
    lessons = get_active_lessons(max_lessons)
    if not lessons:
        return ""
    lines = ["[과거 교훈 — 반드시 반영]"]
    for l in lessons:
        sev = "🔴" if l["severity"] == "high" else "🟡" if l["severity"] == "medium" else "🟢"
        lines.append(f"{sev} [{l['category']}] {l['lesson'][:80]}")
    return "\n".join(lines) + "\n\n"


def extract_dawn_lessons(today_analyses: list, actual_news: str):
    if not today_analyses:
        print("No analyses to review")
        return

    try:
        from aria_data import load_market_data
        market_data = load_market_data()
    except ImportError:
        market_data = {}

    local_lessons = _local_lesson_check(today_analyses, market_data)
    for l in local_lessons:
        add_lesson("dawn", l["category"], l["lesson"], l["severity"])
        print("Local lesson: [" + l["category"] + "] " + l["lesson"][:50])

    summary = [{"time": a.get("analysis_time", ""), "regime": a.get("market_regime", ""),
                "trend": a.get("trend_phase", ""), "one_line": a.get("one_line_summary", ""),
                "thesis_killers": a.get("thesis_killers", [])[:2]} for a in today_analyses]

    market_snapshot = {
        k: market_data.get(k, "N/A")
        for k in ["vix", "kospi", "kospi_change", "krw_usd",
                  "fear_greed_value", "fear_greed_rating", "nvda_change"]
    }

    _DAWN_LESSON_SYS = """You are ARIA's self-reflection engine.
Compare today's predictions against actual market data.
Return JSON: {"has_lessons": true/false, "lessons": [{"category":"","lesson":"","severity":"high/medium/low"}]}"""

    full = ""
    with client.messages.stream(
        model=MODEL, max_tokens=800, system=_DAWN_LESSON_SYS,
        messages=[{"role": "user", "content":
                   "오늘 실제 시장 데이터:\n" + json.dumps(market_snapshot, ensure_ascii=False)
                   + "\n\nARIA 예측:\n" + json.dumps(summary, ensure_ascii=False)
                   + "\n\n로컬에서 이미 감지한 오판: " + str(len(local_lessons)) + "개"
                   + "\n\n추가로 놓친 오판이 있으면 JSON으로 반환. 없으면 has_lessons:false."}]
    ) as s:
        for ev in s:
            t = getattr(ev, "type", "")
            if t == "content_block_delta":
                d = getattr(ev, "delta", None)
                if d and getattr(d, "type", "") == "text_delta":
                    full += d.text

    raw = re.sub(r"```json|```", "", full).strip()
    m   = re.search(r"\{[\s\S]*\}", raw)
    if not m:
        return
    try:
        data = json.loads(m.group())
        if data.get("has_lessons"):
            for l in data.get("lessons", []):
                add_lesson("dawn_ai", l.get("category", "기타"),
                           l.get("lesson", ""), l.get("severity", "medium"))
                print("AI lesson: [" + l.get("category", "") + "] " + l.get("lesson", "")[:50])
    except Exception as e:
        print("Dawn lesson 파싱 오류: " + str(e))


def _local_lesson_check(analyses: list, market_data: dict) -> list:
    lessons = []
    vix = market_data.get("vix")
    sp_chg = market_data.get("sp500_change", "0")
    try:
        sp_chg_f = float(str(sp_chg).replace("%", "").replace("+", ""))
    except Exception:
        sp_chg_f = 0.0

    for a in analyses:
        regime = a.get("market_regime", "")
        conf   = a.get("confidence_overall", "")

        if "선호" in regime and sp_chg_f < -2:
            lessons.append({
                "category": "시장레짐",
                "lesson": f"위험선호 예측 중 S&P {sp_chg_f:+.1f}% 급락 — 레짐 판단 재검토",
                "severity": "high",
            })
        if vix and conf == "높음":
            try:
                vix_f = float(str(vix).replace(",", ""))
                if vix_f > 30:
                    lessons.append({
                        "category": "변동성지수",
                        "lesson": f"VIX {vix_f:.0f} 고공포 구간에서 높음 신뢰도 — 과신 주의",
                        "severity": "medium",
                    })
            except Exception:
                pass
    return lessons


# ══════════════════════════════════════════════════════════════════════════════
# PATTERN DB
# ══════════════════════════════════════════════════════════════════════════════

def update_pattern_db(memory: list) -> None:
    if len(memory) < 5:
        return
    db   = _load(PATTERN_DB_FILE, {"patterns": {}, "last_updated": ""})
    pats = db.get("patterns", {})

    for i in range(len(memory) - 1):
        curr = memory[i]
        nxt  = memory[i + 1]
        if curr.get("mode") != "MORNING" or nxt.get("mode") != "MORNING":
            continue
        key = curr.get("market_regime", "") + "|" + curr.get("trend_phase", "")
        if not key or key == "|":
            continue
        outcome = nxt.get("market_regime", "")
        if key not in pats:
            pats[key] = {}
        pats[key][outcome] = pats[key].get(outcome, 0) + 1

    db["patterns"]     = pats
    db["last_updated"] = _today()
    _save(PATTERN_DB_FILE, db)


def get_pattern_context(memory: list, current_regime: str, current_trend: str) -> str:
    db   = _load(PATTERN_DB_FILE, {"patterns": {}})
    key  = current_regime + "|" + current_trend
    pats = db.get("patterns", {}).get(key, {})
    if not pats:
        return ""
    total = sum(pats.values())
    if total < 3:
        return ""
    top = sorted(pats.items(), key=lambda x: x[1], reverse=True)[:2]
    lines = [f"[패턴DB] {key} 이후 ({total}회):"]
    for regime, cnt in top:
        lines.append(f"  → {regime}: {cnt}회 ({cnt/total:.0%})")
    return "\n".join(lines)


def build_compact_history(memory: list, n: int = 7) -> str:
    recent = [m for m in memory if m.get("mode") == "MORNING"][-n:]
    if not recent:
        return ""
    lines = ["[최근 분석 요약]"]
    for m in recent:
        lines.append(
            f"  {m.get('analysis_date','')} {m.get('market_regime','')} "
            f"| {m.get('one_line_summary','')[:40]}"
        )
    return "\n".join(lines)
