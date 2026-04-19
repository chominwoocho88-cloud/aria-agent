"""
orca_analysis.py — ARIA 분석 모듈 통합
포함: sentiment · portfolio · rotation · baseline · verifier · weights · lessons

[수정]
- MODEL: 환경변수 ORCA_MODEL 지원
- run_verification: [-1] 인덱스 제거, ORCA_FORCE_VERIFY 환경변수 추가
"""
import os
import sys
import json
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

from .compat import get_orca_env, get_orca_flag
from .learning_policy import MIN_SAMPLES, suggest_weight_delta

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

import anthropic

KST = timezone(timedelta(hours=9))

from .paths import (
    SENTIMENT_FILE, ROTATION_FILE, BASELINE_FILE,
    ACCURACY_FILE, MEMORY_FILE, WEIGHTS_FILE,
    LESSONS_FILE, PATTERN_DB_FILE, atomic_write_json,
)
from .state import (
    list_candidates,
    record_candidate_review,
    resolve_verification_outcomes,
)

API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL   = get_orca_env("ORCA_MODEL", os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6"))
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
    atomic_write_json(path, data)


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
        if v["total"] < MIN_SAMPLES:
            continue
        acc     = v["correct"] / v["total"]
        old_w   = conf.get(cat, 1.0)
        adj     = suggest_weight_delta(v["correct"], v["total"])
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
        from .data import load_market_data
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
    from .paths import PORTFOLIO_FILE
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


def _parse_iso(ts: str) -> datetime | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        return dt.astimezone(KST)
    except Exception:
        return None


def _report_market_bias(report: dict) -> dict:
    regime = str(report.get("market_regime", ""))
    trend = str(report.get("trend_phase", ""))
    summary = str(report.get("one_line_summary", ""))
    confidence = str(report.get("confidence_overall", ""))
    text = " ".join([regime, trend, summary])

    bearish = ("위험회피" in regime) or ("하락" in trend)
    bullish = ("위험선호" in regime) and ("하락" not in trend)
    mixed = any(token in text for token in ["혼조", "전환", "반론", "불확실", "관망"])

    if bearish and not bullish:
        bias = "risk_off"
        label = "위험회피"
        reason = "현재 ORCA 레짐이 방어적이어서 장기 추세보다 리스크 관리가 우선입니다."
    elif bullish and not mixed and confidence != "낮음":
        bias = "risk_on"
        label = "위험선호"
        reason = "현재 ORCA 레짐이 우호적이라 JACKAL의 롱 후보를 함께 검토하기 좋은 구간입니다."
    else:
        bias = "mixed"
        label = "혼조/관망"
        reason = "현재 ORCA 레짐이 혼조라서 후보를 바로 추종하기보다 관찰 대상으로 보는 편이 안전합니다."

    return {"bias": bias, "label": label, "reason": reason}


def _match_candidate_themes(candidate_terms: list[str], flows: list[dict]) -> list[str]:
    matches: list[str] = []
    blobs = [
        " ".join(
            [
                str(flow.get("zone", "")),
                str(flow.get("reason", "")),
                str(flow.get("data_point", "")),
            ]
        ).lower()
        for flow in flows
    ]
    for term in candidate_terms:
        lowered = str(term or "").strip().lower()
        if len(lowered) < 2:
            continue
        tokens = [tok for tok in re.split(r"[/,·()\s]+", lowered) if len(tok) >= 2]
        for blob in blobs:
            if lowered in blob or any(tok in blob for tok in tokens):
                if term not in matches:
                    matches.append(term)
                break
    return matches


def review_recent_candidates(
    report: dict,
    *,
    run_id: str | None = None,
    analysis_date: str | None = None,
    limit: int = 12,
    max_age_days: int = 7,
) -> dict:
    analysis_date = analysis_date or _today()
    bias = _report_market_bias(report)
    inflows = report.get("inflows", [])
    outflows = report.get("outflows", [])

    recent = list_candidates(source_system="jackal", unresolved_only=True, limit=max(limit * 4, 40))
    cutoff = _now() - timedelta(days=max_age_days)
    selected = []
    for candidate in recent:
        detected_at = _parse_iso(candidate.get("detected_at", ""))
        if detected_at and detected_at < cutoff:
            continue
        selected.append(candidate)
        if len(selected) >= limit:
            break

    summary = {
        "analysis_date": analysis_date,
        "market_bias": bias["bias"],
        "market_bias_label": bias["label"],
        "reviewed_count": 0,
        "aligned_count": 0,
        "neutral_count": 0,
        "opposed_count": 0,
        "follow_count": 0,
        "watch_count": 0,
        "avoid_count": 0,
        "highlights": [],
    }
    if not selected:
        return summary

    for candidate in selected:
        payload = candidate.get("payload", {})
        signal_quality = payload.get("quality_score", candidate.get("quality_score"))
        devil_verdict = str(payload.get("devil_verdict", ""))
        theme_terms = payload.get("orca_inflows", []) if isinstance(payload.get("orca_inflows"), list) else []
        inflow_matches = _match_candidate_themes(theme_terms, inflows)
        outflow_matches = _match_candidate_themes(theme_terms, outflows)

        if bias["bias"] == "risk_on":
            alignment = "aligned"
        elif bias["bias"] == "risk_off":
            alignment = "opposed"
        else:
            alignment = "neutral"

        if outflow_matches and alignment == "aligned":
            alignment = "neutral"

        thesis_killer = ""
        if payload.get("thesis_killer_hit"):
            thesis_killer = str(payload.get("killer_detail") or "JACKAL Devil thesis killer hit")
        elif outflow_matches:
            thesis_killer = "현재 ORCA 역풍 테마와 겹침: " + ", ".join(outflow_matches[:2])
        elif report.get("thesis_killers"):
            thesis_killer = str(report["thesis_killers"][0].get("event", ""))

        if alignment == "aligned" and (signal_quality or 0) >= 75 and devil_verdict != "반대":
            review_verdict = "follow"
        elif alignment == "opposed" or devil_verdict == "반대" or payload.get("thesis_killer_hit"):
            review_verdict = "avoid"
        else:
            review_verdict = "watch"

        rationale_parts = [bias["reason"]]
        if inflow_matches:
            rationale_parts.append("현재 ORCA 유입 테마와 겹침: " + ", ".join(inflow_matches[:2]))
        if outflow_matches:
            rationale_parts.append("현재 ORCA 역풍 테마와 겹침: " + ", ".join(outflow_matches[:2]))
        if devil_verdict:
            rationale_parts.append("Devil verdict: " + devil_verdict)

        review = {
            "alignment": alignment,
            "review_verdict": review_verdict,
            "orca_regime": report.get("market_regime", ""),
            "orca_trend": report.get("trend_phase", ""),
            "candidate_signal_family": candidate.get("signal_family", ""),
            "quality_score": signal_quality,
            "inflow_matches": inflow_matches,
            "outflow_matches": outflow_matches,
            "rationale": rationale_parts,
        }
        record_candidate_review(
            candidate["candidate_id"],
            analysis_date=analysis_date,
            run_id=run_id,
            alignment=alignment,
            review_verdict=review_verdict,
            orca_regime=report.get("market_regime", ""),
            orca_trend=report.get("trend_phase", ""),
            confidence=report.get("confidence_overall", ""),
            thesis_killer=thesis_killer or None,
            review=review,
        )

        summary["reviewed_count"] += 1
        summary[f"{alignment}_count"] += 1
        summary[f"{review_verdict}_count"] += 1
        if len(summary["highlights"]) < 5:
            summary["highlights"].append(
                {
                    "ticker": candidate.get("ticker", ""),
                    "name": candidate.get("name", ""),
                    "source_event_type": candidate.get("source_event_type", ""),
                    "alignment": alignment,
                    "review_verdict": review_verdict,
                    "quality_score": signal_quality,
                    "signal_family": candidate.get("signal_family", ""),
                    "why": " | ".join(rationale_parts[:2]),
                }
            )

    return summary


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
    if not isinstance(memory, list) or not memory:
        return ""
    prev = memory[-1]
    if not isinstance(prev, dict):
        return ""
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
    results = []
    for tk in thesis_killers:
        event     = tk.get("event", "")
        confirms  = tk.get("confirms_if", "")
        invalids  = tk.get("invalidates_if", "")
        verdict   = "unclear"
        evidence  = ""
        category  = tk.get("category", "기타")

        # 간단한 가격 기반 검증
        import re as _re
        nums_c = _re.findall(r"[-+]?\d+\.?\d*", confirms)
        nums_i = _re.findall(r"[-+]?\d+\.?\d*", invalids)

        # 나스닥/S&P 검증
        if "나스닥" in event or "nasdaq" in event.lower():
            chg = market_data.get("nasdaq_change", "")
            try:
                chg_f = float(str(chg).replace("%", "").replace("+", ""))
                if nums_c:
                    thr = float(nums_c[0])
                    if "+" in confirms and chg_f >= thr:
                        verdict = "confirmed"; evidence = f"나스닥 {chg_f:+.1f}%"
                    elif "-" in invalids and chg_f <= -abs(float(nums_i[0])) if nums_i else False:
                        verdict = "invalidated"; evidence = f"나스닥 {chg_f:+.1f}%"
            except Exception:
                pass

        # 코스피 검증
        elif "코스피" in event or "kospi" in event.lower():
            chg = market_data.get("kospi_change", "")
            try:
                chg_f = float(str(chg).replace("%", "").replace("+", ""))
                if nums_c and chg_f >= float(nums_c[0]):
                    verdict = "confirmed"; evidence = f"코스피 {chg_f:+.1f}%"
                elif nums_i and chg_f <= -abs(float(nums_i[0])):
                    verdict = "invalidated"; evidence = f"코스피 {chg_f:+.1f}%"
            except Exception:
                pass

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

    # [Bug Fix 11] any()로 날짜 체크 + ORCA_FORCE_VERIFY 환경변수
    force_verify = get_orca_flag("ORCA_FORCE_VERIFY")
    already_done = any(h.get("date") == today for h in accuracy.get("history", []))
    if already_done and not force_verify:
        print(f"Already verified today ({today}) — set ORCA_FORCE_VERIFY=true to rerun")
        return accuracy

    tks = yesterday.get("thesis_killers", [])
    if not tks:
        print("No thesis killers to verify")
        return accuracy

    try:
        from .data import load_market_data
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

    try:
        resolution = resolve_verification_outcomes(
            str(yesterday.get("analysis_date", "")),
            results,
            resolved_analysis_date=today,
            metadata={
                "verification_date": today,
                "judged_count": len(judged),
                "confirmed_count": len(correct),
            },
        )
        if resolution.get("matched") or resolution.get("updated"):
            print(
                "State DB outcomes: "
                + str(resolution.get("matched", 0))
                + " inserted, "
                + str(resolution.get("updated", 0))
                + " updated"
            )
        if resolution.get("unmatched"):
            print("State DB unmatched predictions: " + str(len(resolution["unmatched"])))
    except Exception as e:
        print("State DB outcome sync skipped: " + str(e))

    _save(ACCURACY_FILE, accuracy)
    _send_verification_report(results, accuracy, today_acc, dir_acc)
    print("Done. Today accuracy: " + str(today_acc) + "%")
    return accuracy


def _send_verification_report(results, accuracy, today_acc, dir_acc=0):
    try:
        from .notify import send_message
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


def get_active_lessons(max_lessons: int = 8,
                       current_regime: str = "") -> list:
    """
    [Updated] 3개 파일에서 교훈을 읽고 regime + severity 기반으로 우선순위 결정.
    라이브 시스템용: current_date 필터 없음 (모든 교훈이 과거).
    """
    from .paths import LESSONS_FILE as _LF
    _data_dir = _LF.parent

    REGIME_SIM = {
        "위험선호": {"위험선호", "전환중", "혼조"},
        "위험회피": {"위험회피", "전환중", "혼조"},
        "전환중":   {"전환중", "위험선호", "위험회피", "혼조"},
        "혼조":     {"혼조", "전환중", "위험선호", "위험회피"},
    }
    similar = REGIME_SIM.get(current_regime, set())

    today  = _today()
    expiry = {"high": 45, "medium": 21, "low": 10}

    def _is_active(l: dict) -> bool:
        days = expiry.get(l.get("severity", "medium"), 21)
        try:
            return (datetime.strptime(today, "%Y-%m-%d") -
                    datetime.strptime(l["date"], "%Y-%m-%d")).days <= days
        except Exception:
            return True

    def _priority(l: dict) -> float:
        sev_w = {"high": 3.0, "medium": 2.0, "low": 0.5}.get(l.get("severity","medium"), 1.0)
        # 동일/유사 레짐 보너스 +40%
        if similar and l.get("regime","") in similar:
            sev_w *= 1.4
        return sev_w + l.get("reinforced", 0) * 0.3

    all_lessons: list = []

    # 3개 파일에서 통합 수집
    for fname in ("lessons_failure.json", "lessons_strength.json", "lessons_regime.json"):
        try:
            path = _data_dir / fname
            data = _load(path, {"lessons": []})
            for l in data.get("lessons", []):
                if _is_active(l):
                    all_lessons.append(l)
        except Exception:
            pass

    # 3파일 없으면 레거시 fallback
    if not all_lessons:
        data = load_lessons()
        all_lessons = [l for l in data.get("lessons", []) if _is_active(l)]

    ranked = sorted(all_lessons, key=_priority, reverse=True)

    # apply 카운트 업데이트 (레거시 파일 기준 — 대시보드용)
    top = ranked[:max_lessons]
    try:
        legacy = load_lessons()
        for l in legacy.get("lessons", []):
            if any(l.get("lesson") == t.get("lesson") for t in top):
                l["applied"] = l.get("applied", 0) + 1
        _save(LESSONS_FILE, legacy)
    except Exception:
        pass

    return top


def build_lessons_prompt(max_lessons: int = 6,
                          current_regime: str = "") -> str:
    """
    라이브 시스템용 교훈 프롬프트 빌더.
    regime 기반으로 우선 교훈을 선택하고, VIX/환율 TK 금지 지시를 포함.
    """
    lessons = get_active_lessons(max_lessons, current_regime=current_regime)
    lines   = []

    if lessons:
        lines.append("[과거 교훈 — 반드시 반영]")
        for l in lessons:
            sev        = "🔴" if l.get("severity") == "high" else "🟡" if l.get("severity") == "medium" else "🟢"
            regime_tag = f"[{l.get('regime','')}] " if l.get("regime") else ""
            lines.append(f"{sev} [{l.get('category','')}] {regime_tag}{l.get('lesson','')[:80]}")
        lines.append("")

    # [Phase 3] Analyst/Devil 단계 VIX·환율 TK 차단 — orca_agents.py 없이도 여기서 적용
    lines.append(
        "🚫 [thesis_killer 필수 규칙] "
        "VIX와 원달러 환율은 thesis_killer 주제로 절대 사용 금지. "
        "나스닥·코스피·반도체(SK하이닉스·삼성전자·엔비디아) 주가 수치만 사용할 것."
    )

    return "\n".join(lines) + "\n\n"


def extract_dawn_lessons(today_analyses: list, actual_news: str):
    if not today_analyses:
        print("No analyses to review")
        return

    try:
        from .data import load_market_data
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


def extract_monthly_lessons(memory: list, accuracy: dict) -> list:
    """Persist a small set of monthly review lessons."""
    now = _now()
    last_month = (now.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")

    monthly_memory = [
        m for m in (memory if isinstance(memory, list) else [])
        if str(m.get("analysis_date", "")).startswith(last_month)
    ]
    monthly_hist = [
        h for h in (accuracy.get("history", []) if isinstance(accuracy, dict) else [])
        if str(h.get("date", "")).startswith(last_month)
    ]
    monthly_by_cat = [
        h for h in (accuracy.get("history_by_category", []) if isinstance(accuracy, dict) else [])
        if str(h.get("date", "")).startswith(last_month)
    ]

    if not monthly_memory and not monthly_hist:
        print("No monthly data to review")
        return []

    lessons = []

    total = sum(int(h.get("total", 0)) for h in monthly_hist)
    correct = sum(int(h.get("correct", 0)) for h in monthly_hist)
    if total >= 5:
        acc_pct = round(correct / total * 100, 1) if total else 0.0
        if acc_pct < 55:
            lessons.append({
                "category": "monthly_accuracy",
                "lesson": (
                    f"Monthly review {last_month}: accuracy fell to {acc_pct}%. "
                    "Reduce strong directional conviction until thesis killers and data quality agree."
                ),
                "severity": "high",
            })
        elif acc_pct >= 70:
            lessons.append({
                "category": "monthly_strength",
                "lesson": (
                    f"Monthly review {last_month}: accuracy reached {acc_pct}%. "
                    "Keep the regime filters that worked, but do not treat one good month as a permanent edge."
                ),
                "severity": "low",
            })

    by_cat = {}
    for snap in monthly_by_cat:
        for cat, stats in snap.get("by_category", {}).items():
            bucket = by_cat.setdefault(cat, {"correct": 0, "total": 0})
            bucket["correct"] += int(stats.get("correct", 0))
            bucket["total"] += int(stats.get("total", 0))

    weak_ranked = []
    for cat, stats in by_cat.items():
        if stats["total"] < 2:
            continue
        cat_acc = stats["correct"] / stats["total"]
        weak_ranked.append((cat_acc, cat, stats))
    weak_ranked.sort(key=lambda x: x[0])
    if weak_ranked and weak_ranked[0][0] <= 0.45:
        cat_acc, cat, stats = weak_ranked[0]
        lessons.append({
            "category": "monthly_weakness",
            "lesson": (
                f"Monthly review {last_month}: category '{cat}' produced only "
                f"{cat_acc:.0%} accuracy across {stats['total']} checks. "
                "Treat it as weak evidence until new validation improves it."
            ),
            "severity": "high" if cat_acc < 0.35 else "medium",
        })

    high_conf_calls = sum(
        1
        for item in monthly_memory
        if str(item.get("confidence_overall", "")).strip().lower() in {"high", "높음"}
    )
    if total >= 5 and high_conf_calls >= max(3, len(monthly_memory) // 2) and correct / max(total, 1) < 0.6:
        lessons.append({
            "category": "monthly_risk",
            "lesson": (
                f"Monthly review {last_month}: high-confidence calls were too frequent "
                "relative to realized accuracy. Tighten confidence calibration before issuing strong conviction."
            ),
            "severity": "medium",
        })

    persisted = []
    for lesson in lessons[:3]:
        add_lesson("monthly", lesson["category"], lesson["lesson"], lesson["severity"])
        persisted.append(lesson)
        print("Monthly lesson: [" + lesson["category"] + "] " + lesson["lesson"][:80])

    return persisted


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



