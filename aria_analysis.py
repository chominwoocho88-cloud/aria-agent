"""
aria_analysis.py — ARIA 분석 모듈 통합
포함: sentiment · portfolio · rotation · baseline · verifier · weights · lessons
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

SENTIMENT_FILE = Path("sentiment.json")
ROTATION_FILE  = Path("rotation.json")
BASELINE_FILE  = Path("morning_baseline.json")
ACCURACY_FILE  = Path("accuracy.json")
MEMORY_FILE    = Path("memory.json")
WEIGHTS_FILE   = Path("aria_weights.json")
LESSONS_FILE   = Path("aria_lessons.json")

API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL   = "claude-sonnet-4-6"
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
        "시장레짐": 1.0, "추세방향": 1.0, "변동성지수": 1.5,
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
    weights = load_weights()
    changes = []

    for cat, stats in accuracy_data.get("by_category", {}).items():
        total   = stats.get("total", 0)
        correct = stats.get("correct", 0)
        if total < 3:
            continue
        acc          = correct / total
        current_conf = weights["prediction_confidence"].get(cat, 1.0)
        new_conf     = current_conf
        if acc >= 0.75:
            new_conf = min(1.5, current_conf + 0.05)
        elif acc <= 0.40:
            new_conf = max(0.4, current_conf - 0.1)
        if new_conf != current_conf:
            changes.append(cat + " 신뢰도 " + ("증가" if new_conf > current_conf else "감소")
                           + ": " + str(round(current_conf, 2)) + " -> " + str(round(new_conf, 2)))
        weights["prediction_confidence"][cat] = round(new_conf, 2)

    geo_conf = weights["prediction_confidence"].get("지정학", 1.0)
    if geo_conf < 0.6:
        old = weights["sentiment"]["시장레짐"]
        weights["sentiment"]["시장레짐"] = max(0.7, old - 0.05)
        if weights["sentiment"]["시장레짐"] != old:
            changes.append("시장레짐 가중치 조정: " + str(round(old, 2))
                           + " -> " + str(round(weights["sentiment"]["시장레짐"], 2)))

    if changes:
        weights["learning_log"].append(
            {"date": _today(), "changes": changes, "trigger": "accuracy_update"}
        )
        weights["learning_log"] = weights["learning_log"][-30:]
        weights["total_learning_cycles"] += 1

    weights["last_updated"] = _today()
    _save(WEIGHTS_FILE, weights)
    return changes


# ══════════════════════════════════════════════════════════════════════════════
# SENTIMENT
# ══════════════════════════════════════════════════════════════════════════════
def _parse_num(s) -> float | None:
    m = re.search(r"(\d+\.?\d*)", str(s or ""))
    return float(m.group(1)) if m else None


def calculate_sentiment(report: dict, market_data: dict = None) -> dict:
    sw = get_sentiment_weights()

    regime   = report.get("market_regime", "")
    trend    = report.get("trend_phase", "")
    vi       = report.get("volatility_index", {})
    outflows = report.get("outflows", [])
    inflows  = report.get("inflows", [])
    counters = report.get("counterarguments", [])
    korea    = report.get("korea_focus", {})
    hidden   = report.get("hidden_signals", [])

    vix_val    = _parse_num((market_data or {}).get("vix")) or _parse_num(vi.get("vix"))
    vkospi_val = _parse_num(vi.get("vkospi"))
    fg_raw     = _parse_num((market_data or {}).get("fear_greed_value")) or _parse_num(vi.get("fear_greed"))

    comps = {}

    # 1. 시장레짐
    raw = 20 if "선호" in regime else -20 if "회피" in regime else 5 if "전환" in regime else 0
    if fg_raw is not None and fg_raw <= 25 and raw > 0:
        raw //= 2
    comps["시장레짐"] = {"score": round(raw * sw.get("시장레짐", 1.0)), "reason": regime[:25] or "데이터없음"}

    # 2. 추세방향
    raw = 15 if "상승" in trend else -15 if "하락" in trend else 0
    comps["추세방향"] = {"score": round(raw * sw.get("추세방향", 1.0)), "reason": trend or "데이터없음"}

    # 3. 변동성
    raw, reason = 0, ""
    if fg_raw is not None:
        if fg_raw <= 20:   raw, reason = -25, "Fear&Greed " + str(fg_raw) + " (극단공포)"
        elif fg_raw <= 35: raw, reason = -15, "Fear&Greed " + str(fg_raw) + " (공포)"
        elif fg_raw <= 55: raw, reason =   0, "Fear&Greed " + str(fg_raw) + " (중립)"
        elif fg_raw <= 75: raw, reason =  12, "Fear&Greed " + str(fg_raw) + " (탐욕)"
        else:              raw, reason =  20, "Fear&Greed " + str(fg_raw) + " (극단탐욕)"
        if vix_val:
            if vix_val >= 30:   raw = min(raw, -10)
            elif vix_val <= 15: raw = max(raw, 5)
    elif vix_val:
        if vix_val >= 35:   raw, reason = -20, "VIX " + str(vix_val) + " 극단공포"
        elif vix_val >= 25: raw, reason = -10, "VIX " + str(vix_val) + " 공포"
        elif vix_val >= 20: raw, reason =  -5, "VIX " + str(vix_val) + " 경계"
        elif vix_val <= 15: raw, reason =  10, "VIX " + str(vix_val) + " 안정"
    else:
        lvl = vi.get("level", "")
        if "극단공포" in lvl:   raw, reason = -20, lvl
        elif "공포" in lvl:     raw, reason = -10, lvl
        elif "극단탐욕" in lvl: raw, reason =  20, lvl
        elif "탐욕" in lvl:     raw, reason =  10, lvl
    comps["변동성지수"] = {"score": round(raw * sw.get("변동성지수", 1.5)), "reason": reason[:30] or "데이터없음"}

    # 4. 자금흐름
    raw = ((sum(1 for i in inflows if i.get("momentum") == "강함") * 5)
           - (sum(1 for o in outflows if o.get("severity") == "높음") * 5)
           + (len(inflows) - len(outflows)) * 2)
    raw = max(-15, min(15, raw))
    comps["자금흐름"] = {"score": round(raw * sw.get("자금흐름", 1.0)),
                        "reason": "유입" + str(len(inflows)) + " / 유출" + str(len(outflows))}

    # 5. 반론강도
    h_risk = sum(1 for c in counters if c.get("risk_level") == "높음")
    m_risk = sum(1 for c in counters if c.get("risk_level") == "보통")
    raw    = max(-10, -(h_risk * 4 + m_risk * 2))
    comps["반론강도"] = {"score": round(raw * sw.get("반론강도", 0.8)),
                        "reason": "고위험" + str(h_risk) + " / 중위험" + str(m_risk)}

    # 6. 한국시장
    raw, notes = 0, []
    krw, kopi  = korea.get("krw_usd", ""), korea.get("kospi_flow", "")
    if "약세" in krw or "하락" in krw: raw -= 3; notes.append("원화약세")
    elif "강세" in krw or "상승" in krw: raw += 3; notes.append("원화강세")
    if "하락" in kopi or "-" in kopi: raw -= 4; notes.append("코스피하락")
    elif "상승" in kopi or "+" in kopi: raw += 4; notes.append("코스피상승")
    if market_data:
        try:
            k = float(str(market_data.get("kospi_change", "0%")).replace("%", "").replace("+", ""))
            if k >= 2: raw += 3
            elif k <= -2: raw -= 3
        except Exception: pass
    raw = max(-10, min(10, raw))
    comps["한국시장"] = {"score": round(raw * sw.get("한국시장", 0.8)),
                        "reason": ", ".join(notes) if notes else "중립"}

    # 7. 숨은시그널
    h_conf = sum(1 for h in hidden if h.get("confidence") == "높음")
    l_conf = sum(1 for h in hidden if h.get("confidence") == "낮음")
    raw    = max(-10, min(10, h_conf * 3 - l_conf * 2))
    comps["숨은시그널"] = {"score": round(raw * sw.get("숨은시그널", 0.7)),
                          "reason": "고신뢰" + str(h_conf) + " / 저신뢰" + str(l_conf)}

    score = 50 + sum(c["score"] for c in comps.values())
    if fg_raw is not None and fg_raw <= 25: score = min(score, 45)
    if fg_raw is not None and fg_raw >= 80: score = max(score, 70)
    if vix_val and vix_val >= 25:           score = min(score, 60)
    if vkospi_val and vkospi_val >= 40:     score = min(score, 55)
    score = max(0, min(100, score))

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

    existing = next((h for h in history if h["date"] == new["date"]), None)
    if existing:
        blended = round(existing["score"] * 0.4 + new["score"] * 0.6)
        new["score"] = blended
        lvl = ("극단공포" if blended <= 20 else "공포" if blended <= 40
               else "중립" if blended <= 60 else "탐욕" if blended <= 80 else "극단탐욕")
        emo = ("😱" if blended <= 20 else "😰" if blended <= 40
               else "😐" if blended <= 60 else "😏" if blended <= 80 else "🤑")
        new["level"], new["emoji"] = lvl, emo

    history = [h for h in history if h["date"] != new["date"]]
    history.append(new)
    history = history[-90:]
    trend   = _analyze_trend(history)
    data    = {"history": history, "current": new, "trend": trend}
    _save(SENTIMENT_FILE, data)

    _send_sentiment_report(data)
    return data


def _send_sentiment_report(data: dict):
    try:
        from aria_notify import send_message
    except ImportError:
        return

    cur   = data.get("current", {})
    trend = data.get("trend", {})
    hist  = data.get("history", [])
    comps = cur.get("components", {})
    score = cur.get("score", 50)
    level = cur.get("level", "")

    arrow = ("↑ 개선중" if trend.get("direction") == "improving"
             else "↓ 악화중" if trend.get("direction") == "deteriorating" else "→ 안정")

    pct = round(sum(1 for h in hist[-30:] if h["score"] <= score) / max(len(hist[-30:]), 1) * 100)

    comp_lines = []
    for name, info in comps.items():
        abs_s = abs(info["score"])
        bar   = "[" + "█" * min(5, round(abs_s / 20 * 5)) + "░" * (5 - min(5, round(abs_s / 20 * 5))) + "] "
        comp_lines.append(name[:5] + " " + bar + ("+" if info["score"] > 0 else "") + str(info["score"]))
        comp_lines.append("  " + info["reason"][:18])

    chart_lines = []
    for h in hist[-10:]:
        s   = h["score"]
        bar = "█" * (s // 10) + "░" * (10 - s // 10)
        chart_lines.append(h["date"][5:] + " " + bar + " " + str(s) + h.get("emoji", ""))

    fg_note = ("\n<i>Fear&Greed " + str(cur.get("fear_greed")) + " 실수치 반영</i>"
               if cur.get("fear_greed") is not None else "")

    insight = ("극단공포 - 분할매수 최적 타이밍" if score <= 20
               else "공포 - 분할매수 적극 검토" if score <= 35
               else "공포우위 - 신중한 분할매수" if score <= 50
               else "중립 - 추세 확인 후 대응" if score <= 65
               else "탐욕 - 리스크 관리 강화" if score <= 80
               else "극단탐욕 - 비중 축소 고려")

    send_message("\n".join([
        cur.get("emoji", "") + " <b>ARIA 시장 감정지수</b>",
        "<code>" + cur.get("date", "") + "</code>",
        "",
        "오늘: <b>" + str(score) + "/100</b> (" + level + ")",
        "추세: " + arrow + " | 7일평균: " + str(trend.get("avg_7d", "-")),
        fg_note, "",
        "━━ 구성요소 ━━",
        "<pre>" + "\n".join(comp_lines) + "</pre>", "",
        "━━ 10일 추이 ━━",
        "<pre>" + "\n".join(chart_lines) + "</pre>", "",
        "최저:" + str(trend.get("min_30d")) + " 최고:" + str(trend.get("max_30d")) + " 평균:" + str(trend.get("avg_30d")),
        "현재: 하위 " + str(pct) + "% 구간", "",
        "💡 " + insight,
    ]))
    print("Sentiment report sent. Score: " + str(score) + " / " + level)


# ══════════════════════════════════════════════════════════════════════════════
# PORTFOLIO
# ══════════════════════════════════════════════════════════════════════════════
_HOLDINGS = [
    {"name": "엔비디아",      "ticker": "nvda",     "weight": 35.0, "sector": "반도체/ai"},
    {"name": "SK하이닉스",    "ticker": "sk_hynix", "weight": 15.0, "sector": "반도체"},
    {"name": "삼성전자",      "ticker": "samsung",  "weight": 10.0, "sector": "반도체/전자"},
    {"name": "브로드컴",      "ticker": "avgo",     "weight": 10.0, "sector": "반도체/ai"},
    {"name": "카카오",        "ticker": "kakao",    "weight":  5.0, "sector": "플랫폼/it"},
    {"name": "한국고배당ETF", "ticker": "kodex",    "weight": 10.0, "sector": "배당"},
    {"name": "SCHD",         "ticker": "schd",     "weight": 10.0, "sector": "배당"},
    {"name": "현금",          "ticker": "cash",     "weight":  5.0, "sector": "현금"},
]
_RISK_THR = -2.0
_OPPO_THR = +1.5


def _pct(s) -> float:
    try:
        return float(str(s or "0").replace("%", "").replace("+", "").strip())
    except Exception:
        return 0.0


def run_portfolio(report: dict, market_data: dict = None) -> dict:
    if not market_data:
        try:
            from aria_data import load_market_data
            market_data = load_market_data()
        except ImportError:
            market_data = {}

    regime    = report.get("market_regime", "")
    trend     = report.get("trend_phase", "")
    outflows  = report.get("outflows", [])
    inflows   = report.get("inflows", [])
    korea     = report.get("korea_focus", {})
    vix_level = report.get("volatility_index", {}).get("level", "")
    out_txt   = " ".join(o.get("zone", "") for o in outflows).lower()
    in_txt    = " ".join(i.get("zone", "") for i in inflows).lower()

    results, total_risk, total_oppo, pnl = [], 0.0, 0.0, 0.0

    for h in _HOLDINGS:
        name, ticker, weight, sector = h["name"], h["ticker"], h["weight"], h["sector"]
        chg = _pct(market_data.get(ticker + "_change", "0"))
        if ticker != "cash":
            pnl += chg * (weight / 100)

        status, reason = "neutral", ""
        if ticker == "cash":
            reason = "현금 보유"
        elif chg <= _RISK_THR:
            status, reason = "risk", "실제 하락 " + str(chg) + "% 감지"
        elif chg >= _OPPO_THR:
            status, reason = "opportunity", "실제 상승 " + str(chg) + "% 확인"
        else:
            if "반도체" in sector or "ai" in sector:
                if "반도체" in out_txt or "nvidia" in out_txt or "엔비디아" in out_txt:
                    status, reason = "risk", "반도체 섹터 자금 유출"
                elif "반도체" in in_txt or "ai" in in_txt:
                    status, reason = "opportunity", "반도체/AI 자금 유입"
            elif "플랫폼" in sector or "it" in sector:
                if "회피" in regime:   status, reason = "risk", "위험회피 환경"
                elif "선호" in regime: status, reason = "opportunity", "위험선호 환경 수혜"
            elif "배당" in sector:
                if "하락" in trend or "회피" in regime:
                    status, reason = "opportunity", "하락장 방어주 수혜"

        if ticker == "sk_hynix" and not chg and korea.get("sk_hynix"):
            reason = "SK하이닉스: " + korea["sk_hynix"]
        if ticker == "samsung" and not chg and korea.get("samsung"):
            reason = "삼성전자: " + korea["samsung"]

        if status == "risk":        total_risk += weight
        elif status == "opportunity": total_oppo += weight
        results.append({"name": name, "ticker": ticker, "weight": weight,
                        "actual_change": chg, "status": status, "reason": reason})

    port_risk = "높음" if total_risk >= 40 else "보통" if total_risk >= 20 else "낮음"
    pnl_str   = ("+" if pnl >= 0 else "") + str(round(pnl, 2)) + "%"
    actions   = []
    if total_risk > 40: actions.append("위험 노출 " + str(round(total_risk)) + "% — 현금 비중 확대 검토")
    if "극단공포" in vix_level: actions.append("VIX 극단공포 — 분할매수 적극 검토")
    elif "공포" in vix_level:   actions.append("VIX 공포 구간 — 분할매수 유지")
    if "회피" in regime and "하락" in trend:
        actions.append("위험회피 + 하락추세 — 배당/방어주 비중 유지")
    if pnl <= -2.0: actions.append("오늘 포트 -2% 이하 — 손절 기준 재점검")

    analysis = {
        "date": _today(), "holdings": results,
        "total_risk": round(total_risk, 1), "total_opportunity": round(total_oppo, 1),
        "portfolio_risk": port_risk, "portfolio_pnl": pnl_str,
        "actions": actions, "regime": regime, "trend": trend,
        "data_source": "Yahoo Finance 실시간" if market_data else "자금흐름 기반",
    }
    _send_portfolio_report(analysis)
    return analysis


def _send_portfolio_report(analysis: dict):
    try:
        from aria_notify import send_message
    except ImportError:
        return

    risk  = analysis["portfolio_risk"]
    re_em = "🔴" if risk == "높음" else "🟡" if risk == "보통" else "🟢"
    pnl   = analysis.get("portfolio_pnl", "0%")

    lines = [
        "<b>💼 포트폴리오 분석</b>",
        "<code>" + analysis["date"] + " (" + analysis.get("data_source", "") + ")</code>", "",
        re_em + " 전체 위험도: <b>" + risk + "</b>",
        ("📈" if "+" in str(pnl) else "📉") + " 오늘 포트 손익: <b>" + pnl + "</b>",
        "위험 " + str(analysis["total_risk"]) + "% | 기회 " + str(analysis["total_opportunity"]) + "%", "",
    ]
    for h in analysis["holdings"]:
        if h["ticker"] == "cash": continue
        em = "🔴" if h["status"] == "risk" else "🟢" if h["status"] == "opportunity" else "⚪"
        chg_s = ("+" if h["actual_change"] >= 0 else "") + str(h["actual_change"]) + "%" if h["actual_change"] != 0 else ""
        lines.append(em + " <b>" + h["name"] + "</b> (" + str(h["weight"]) + "%)"
                     + (" <code>" + chg_s + "</code>" if chg_s else ""))
        if h["reason"]: lines.append("   <i>" + h["reason"] + "</i>")
    if analysis["actions"]:
        lines += ["", "📌 <b>권장 액션</b>"]
        for a in analysis["actions"]: lines.append("  • " + a)

    send_message("\n".join(lines))
    print("Portfolio report sent")


# ══════════════════════════════════════════════════════════════════════════════
# ROTATION
# ══════════════════════════════════════════════════════════════════════════════
SECTORS = ["반도체/AI","빅테크","에너지/원유","방산","헬스케어",
           "금융","소비재","배당/가치주","원자재","부동산(리츠)"]


def run_rotation(report: dict) -> dict:
    today   = _today()
    data    = _load(ROTATION_FILE, {})
    history = data.get("history", [])

    flows = {s: 0 for s in SECTORS}
    for o in report.get("outflows", []):
        zone = o.get("zone", "").lower()
        sc   = -3 if o.get("severity") == "높음" else -2 if o.get("severity") == "보통" else -1
        for s in SECTORS:
            if any(k in zone for k in s.lower().split("/")):
                flows[s] += sc
    for i in report.get("inflows", []):
        zone = i.get("zone", "").lower()
        sc   = 3 if i.get("momentum") == "강함" else 2 if i.get("momentum") == "형성중" else 1
        for s in SECTORS:
            if any(k in zone for k in s.lower().split("/")):
                flows[s] += sc
    for s in flows:
        flows[s] = max(-3, min(3, flows[s]))

    history = [h for h in history if h.get("date") != today]
    history.append({"date": today, "flows": flows})
    history = history[-30:]

    cumulative = {s: sum(h["flows"].get(s, 0) for h in history) for s in SECTORS}
    ranking    = sorted(cumulative.items(), key=lambda x: x[1], reverse=True)

    # 로테이션 감지
    rotation = {"from": "", "to": "", "confidence": "낮음"}
    if len(history) >= 2:
        recent = history[-7:]
        prev   = history[-14:-7] if len(history) >= 14 else []
        r_sum  = {s: sum(h["flows"].get(s, 0) for h in recent) for s in SECTORS}
        p_sum  = {s: sum(h["flows"].get(s, 0) for h in prev) for s in SECTORS}
        changes = [(s, r_sum[s] - p_sum.get(s, 0)) for s in SECTORS]
        mag    = max(c[1] for c in changes) - min(c[1] for c in changes)
        rotation = {
            "from": min(changes, key=lambda x: x[1])[0],
            "to":   max(changes, key=lambda x: x[1])[0],
            "confidence": "높음" if mag > 5 else "보통" if mag > 2 else "낮음",
        }

    result = {
        "last_updated": today, "today_flows": flows,
        "cumulative_30d": cumulative, "ranking": ranking,
        "rotation_signal": rotation, "history": history,
    }
    _save(ROTATION_FILE, result)
    _send_rotation_report(result)
    return result


def _send_rotation_report(data: dict):
    try:
        from aria_notify import send_message
    except ImportError:
        return

    tf   = data.get("today_flows", {})
    rot  = data.get("rotation_signal", {})
    hlen = len(data.get("history", []))

    si = sorted([(s, v) for s, v in tf.items() if v >= 2],  key=lambda x: x[1], reverse=True)
    wi = [(s, v) for s, v in tf.items() if v == 1]
    so = sorted([(s, v) for s, v in tf.items() if v <= -2], key=lambda x: x[1])
    wo = [(s, v) for s, v in tf.items() if v == -1]
    nt = [(s, v) for s, v in tf.items() if v == 0]

    def bar(n): return "█" * (n * 2) + "░" * ((3 - n) * 2)

    lines = ["<b>🔄 섹터 자금 흐름</b>",
             "<code>" + data.get("last_updated", "") + " (" + str(hlen) + "일 누적)</code>", ""]
    if si or wi:
        lines.append("━━ 자금 유입 ━━")
        for s, v in si: lines.append("🔥 " + s + "  <code>" + bar(v) + "</code> 강한유입")
        for s, v in wi: lines.append("📈 " + s + "  <code>" + bar(v) + "</code> 유입")
        lines.append("")
    if so or wo:
        lines.append("━━ 자금 유출 ━━")
        for s, v in so: lines.append("📉 " + s + "  <code>" + bar(abs(v)) + "</code> 강한유출")
        for s, v in wo: lines.append("📉 " + s + "  <code>" + bar(abs(v)) + "</code> 소폭유출")
        lines.append("")
    if nt:
        lines += ["━━ 중립 관망 ━━", "➡️ " + ", ".join(s for s, _ in nt), ""]
    if rot.get("from") and rot.get("to") and hlen >= 3:
        lines += ["━━ 로테이션 감지 ━━",
                  rot["from"] + " → " + rot["to"], "신뢰도: " + rot["confidence"], ""]
    if si: lines.append("💡 " + si[0][0] + "로 자금 집중 중")
    if so: lines.append("   " + so[0][0] + " 자금 이탈 중")
    if hlen < 7:
        lines += ["", "<i>데이터 " + str(hlen) + "일째 누적 중 (7일 이상부터 정확도 향상)</i>"]

    send_message("\n".join(lines))
    print("Rotation report sent")


# ══════════════════════════════════════════════════════════════════════════════
# BASELINE
# ══════════════════════════════════════════════════════════════════════════════
def save_baseline(report: dict, market_data: dict = None):
    today = _today()
    md    = market_data or {}
    baseline = {
        "date": today, "saved_at": _now().strftime("%H:%M KST"),
        "market_regime": report.get("market_regime", ""),
        "trend_phase":   report.get("trend_phase", ""),
        "confidence":    report.get("confidence_overall", ""),
        "one_line_summary": report.get("one_line_summary", ""),
        "thesis_killers": report.get("thesis_killers", []),
        "key_outflows": [{"zone": o["zone"], "severity": o.get("severity", "")}
                         for o in report.get("outflows", [])[:3]],
        "key_inflows":  [{"zone": i["zone"], "momentum": i.get("momentum", "")}
                         for i in report.get("inflows", [])[:3]],
        "korea_focus": report.get("korea_focus", {}),
        "volatility": {k: report.get("volatility_index", {}).get(k, "")
                       for k in ["vix", "vkospi", "fear_greed", "level"]},
        "market_snapshot": {k: md.get(k, "N/A")
                            for k in ["sp500", "nasdaq", "vix", "kospi", "krw_usd", "nvda"]},
        "actionable_watch": report.get("actionable_watch", []),
    }
    _save(BASELINE_FILE, baseline)
    print("Baseline saved: " + today + " " + baseline["saved_at"])
    return baseline


def load_baseline() -> dict:
    data = _load(BASELINE_FILE)
    if not data or data.get("date") != _today():
        return {}
    return data


def build_baseline_context(mode: str) -> str:
    b = load_baseline()
    if not b or mode == "MORNING":
        return ""

    snap, vol = b.get("market_snapshot", {}), b.get("volatility", {})
    lines = [
        "\n\n## 오늘 아침 기준점 (07:30 MORNING 분석 결과)",
        "이 기준점과 비교해서 무엇이 변했는지 분석하세요.", "",
        "아침 레짐: " + b.get("market_regime", ""),
        "아침 추세: " + b.get("trend_phase", ""),
        "아침 신뢰도: " + b.get("confidence", ""),
        "아침 요약: " + b.get("one_line_summary", ""), "",
        "아침 시장 수치:",
        "- S&P500: " + snap.get("sp500", "N/A"),
        "- 나스닥:  " + snap.get("nasdaq", "N/A"),
        "- VIX:    " + snap.get("vix", "N/A") + " (" + vol.get("level", "") + ")",
        "- 코스피:  " + snap.get("kospi", "N/A"),
        "- 원달러:  " + snap.get("krw_usd", "N/A"),
        "- 엔비디아: " + snap.get("nvda", "N/A"), "",
        "아침 핵심 유출: " + ", ".join(o["zone"] for o in b.get("key_outflows", [])),
        "아침 핵심 유입: " + ", ".join(i["zone"] for i in b.get("key_inflows", [])),
    ]
    for tk in b.get("thesis_killers", [])[:3]:
        lines += ["", "아침 테제 킬러 (오늘 확인 필요):"]
        lines.append("- " + tk.get("event", "") + " [" + tk.get("timeframe", "") + "]")
        lines.append("  확인: " + tk.get("confirms_if", "")[:50])

    mode_notes = {
        "AFTERNOON": ["## 오후 분석 지시",
                      "- 아침 대비 달라진 것만 집중 분석하세요",
                      "- 레짐이 바뀌었다면 반드시 명확한 근거를 제시하세요",
                      "- 아침 테제 킬러 중 확인된 것이 있으면 보고하세요"],
        "EVENING":   ["## 저녁 분석 지시",
                      "- 오늘 하루를 총정리하세요",
                      "- 아침 예측이 실제로 맞았는지 평가하세요",
                      "- 내일 아침 준비 포인트를 명확히 제시하세요"],
        "DAWN":      ["## 새벽 분석 지시",
                      "- 어제 아침 예측 대비 미국 마감 결과를 평가하세요",
                      "- 오늘 아침 분석을 위한 글로벌 세팅을 제공하세요"],
    }
    if mode in mode_notes:
        lines += [""] + mode_notes[mode]

    return "\n".join(lines)


def get_regime_drift(current_regime: str) -> str:
    b = load_baseline()
    if not b: return ""
    morning = b.get("market_regime", "")
    if not morning or not current_regime: return ""
    return "STABLE" if morning == current_regime else "DRIFT: " + morning + " → " + current_regime


# ══════════════════════════════════════════════════════════════════════════════
# VERIFIER
# ══════════════════════════════════════════════════════════════════════════════
def _parse_chg(s) -> float | None:
    try:
        return float(str(s or "").replace("%", "").replace("+", "").strip())
    except Exception:
        return None


def _verify_price(thesis_killers: list, market_data: dict) -> list:
    ps = {k: _parse_chg(market_data.get(k + "_change"))
          for k in ["sp500", "nasdaq", "vix", "kospi", "sk_hynix", "samsung", "nvda"]}

    results = []
    for tk in thesis_killers:
        event = tk.get("event", "").lower()
        conf  = tk.get("confirms_if", "").lower()
        verdict, evidence, category = "unclear", "", "기타"

        if any(k in event for k in ["나스닥","nasdaq","s&p","미국증시","기술주"]):
            category = "주식"
            chg = ps.get("nasdaq") or ps.get("sp500")
            if chg is not None:
                up = chg > 0; dn = chg < 0
                if up and any(w in conf for w in ["상승","반등","올라"]):
                    verdict, evidence = "confirmed", "나스닥 실제 +" + str(chg) + "%"
                elif dn and any(w in conf for w in ["하락","급락","내려"]):
                    verdict, evidence = "confirmed", "나스닥 실제 " + str(chg) + "%"
                elif up and any(w in conf for w in ["하락","급락"]):
                    verdict, evidence = "invalidated", "나스닥 실제 +" + str(chg) + "% (반등)"
                elif dn and any(w in conf for w in ["상승","반등"]):
                    verdict, evidence = "invalidated", "나스닥 실제 " + str(chg) + "% (하락)"

        elif any(k in event for k in ["반도체","sk하이닉스","엔비디아","nvidia","hbm"]):
            category = "주식"
            chg = ps.get("sk_hynix") or ps.get("nvda")
            if chg is not None:
                if chg >= 2 and any(w in conf for w in ["상승","강세","유입"]):
                    verdict, evidence = "confirmed", "반도체 실제 +" + str(chg) + "%"
                elif chg <= -2 and any(w in conf for w in ["하락","약세","유출"]):
                    verdict, evidence = "confirmed", "반도체 실제 " + str(chg) + "%"
                elif chg >= 2 and any(w in conf for w in ["하락","유출"]):
                    verdict, evidence = "invalidated", "반도체 실제 +" + str(chg) + "%"
                elif chg <= -2 and any(w in conf for w in ["상승","유입"]):
                    verdict, evidence = "invalidated", "반도체 실제 " + str(chg) + "%"

        elif any(k in event for k in ["코스피","kospi","한국증시"]):
            category = "주식"
            chg = ps.get("kospi")
            if chg is not None:
                if chg > 0 and any(w in conf for w in ["상승","반등"]):
                    verdict, evidence = "confirmed", "코스피 실제 +" + str(chg) + "%"
                elif chg < 0 and any(w in conf for w in ["하락","급락"]):
                    verdict, evidence = "confirmed", "코스피 실제 " + str(chg) + "%"
                elif chg > 0 and "하락" in conf:
                    verdict, evidence = "invalidated", "코스피 실제 +" + str(chg) + "%"
                elif chg < 0 and "상승" in conf:
                    verdict, evidence = "invalidated", "코스피 실제 " + str(chg) + "%"

        elif any(k in event for k in ["환율","원달러","krw"]):
            category = "환율"
            try:
                krw = float(re.search(r"[\d.]+", str(market_data.get("krw_usd", ""))).group())
                if krw >= 1500 and any(w in conf for w in ["약세","상승","1500"]):
                    verdict, evidence = "confirmed", "원달러 실제 " + str(krw)
                elif krw < 1450 and any(w in conf for w in ["강세","하락"]):
                    verdict, evidence = "confirmed", "원달러 실제 " + str(krw)
            except Exception: pass

        elif any(k in event for k in ["금리","국채","연준","fomc"]):
            category = "금리"
            verdict, evidence = "unclear", "실시간 금리 데이터 미제공 — 뉴스 확인 필요"

        results.append({
            "event": tk.get("event", ""), "verdict": verdict,
            "evidence": evidence, "category": category,
            "confirms_if": tk.get("confirms_if", ""),
            "invalidates_if": tk.get("invalidates_if", ""),
        })
    return results


_VERIFIER_SYSTEM = """You are ARIA-Verifier.
Only check the UNCLEAR items that could not be verified with price data.
Search for specific news about these events.
Return ONLY valid JSON. No markdown.
{"results":[{"event":"","verdict":"confirmed/invalidated/unclear","evidence":"","category":"금리/지정학/기업/기타"}]}"""


def _ai_verify(unclear: list) -> list:
    if not unclear: return []
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
    memory   = _load(MEMORY_FILE, [])
    accuracy = _load(ACCURACY_FILE, {"total":0,"correct":0,"by_category":{},"history":[],"weak_areas":[],"strong_areas":[]})

    if not memory:
        print("No previous analysis"); return accuracy
    yesterday = memory[-1]
    today     = _today()
    if accuracy.get("history") and accuracy["history"][-1].get("date") == today:
        print("Already verified today"); return accuracy

    tks = yesterday.get("thesis_killers", [])
    if not tks:
        print("No thesis killers to verify"); return accuracy

    try:
        from aria_data import load_market_data
        md = load_market_data()
    except ImportError:
        md = {}

    print("Verifying " + str(len(tks)) + " predictions...")
    results = _verify_price(tks, md)

    unclear = [r for r in results if r["verdict"] == "unclear"]
    if unclear:
        ai = _ai_verify(unclear)
        ai_map = {r["event"]: r for r in ai}
        for r in results:
            if r["verdict"] == "unclear" and r["event"] in ai_map:
                r.update({k: ai_map[r["event"]].get(k, r[k])
                           for k in ["verdict", "evidence", "category"]})

    changes = update_weights_from_accuracy(accuracy)
    if changes: print("Weight updates: " + str(len(changes)))

    judged  = [r for r in results if r["verdict"] != "unclear"]
    correct = [r for r in judged if r["verdict"] == "confirmed"]

    accuracy["total"]   += len(judged)
    accuracy["correct"] += len(correct)
    for r in judged:
        cat = r.get("category", "기타")
        if cat not in accuracy["by_category"]:
            accuracy["by_category"][cat] = {"total": 0, "correct": 0}
        accuracy["by_category"][cat]["total"] += 1
        if r["verdict"] == "confirmed":
            accuracy["by_category"][cat]["correct"] += 1

    today_acc = round(len(correct) / len(judged) * 100, 1) if judged else 0
    accuracy["history"].append({"date": today, "total": len(judged),
                                 "correct": len(correct), "accuracy": today_acc})
    accuracy["history"] = accuracy["history"][-90:]

    strong, weak = [], []
    for cat, s in accuracy["by_category"].items():
        if s["total"] >= 3:
            a = s["correct"] / s["total"] * 100
            if a >= 70: strong.append(cat + " (" + str(round(a)) + "%)")
            elif a <= 40: weak.append(cat + " (" + str(round(a)) + "%)")
    accuracy["strong_areas"] = strong
    accuracy["weak_areas"]   = weak

    _save(ACCURACY_FILE, accuracy)
    _send_verification_report(results, accuracy, today_acc)
    print("Done. Today accuracy: " + str(today_acc) + "%")
    return accuracy


def _send_verification_report(results, accuracy, today_acc):
    try:
        from aria_notify import send_message
    except ImportError:
        return

    judged    = [r for r in results if r["verdict"] != "unclear"]
    total_acc = round(accuracy["correct"] / accuracy["total"] * 100, 1) if accuracy["total"] > 0 else 0
    lines     = ["<b>📋 어제 예측 채점</b>", "<code>" + _today() + "</code>", ""]
    for r in results:
        em = "✅" if r["verdict"] == "confirmed" else "❌" if r["verdict"] == "invalidated" else "❓"
        lines.append(em + " <b>" + r.get("event", "")[:40] + "</b>")
        if r.get("evidence"): lines.append("  <i>" + r["evidence"] + "</i>")
    lines += ["",
              "오늘: <b>" + str(today_acc) + "%</b> (" + str(len([r for r in results if r["verdict"]=="confirmed"])) + "/" + str(len(judged)) + ")",
              "누적: <b>" + str(total_acc) + "%</b> (" + str(accuracy["correct"]) + "/" + str(accuracy["total"]) + ")"]
    if accuracy.get("strong_areas"): lines.append("💪 강점: " + ", ".join(accuracy["strong_areas"][:3]))
    if accuracy.get("weak_areas"):   lines.append("⚠️ 약점: " + ", ".join(accuracy["weak_areas"][:3]))
    send_message("\n".join(lines))


# ══════════════════════════════════════════════════════════════════════════════
# LESSONS
# ══════════════════════════════════════════════════════════════════════════════
def load_lessons() -> dict:
    return _load(LESSONS_FILE, {"lessons": [], "total_lessons": 0, "last_updated": ""})


def add_lesson(source: str, category: str, lesson_text: str, severity: str = "medium"):
    data    = load_lessons()
    today   = _today()
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

    data["lessons"] = sorted(data["lessons"], key=lambda x: x["date"], reverse=True)[:60]
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

    def pri(l): return (3 if l["severity"]=="high" else 2 if l["severity"]=="medium" else 1) * 2 + l.get("reinforced", 0)
    sorted_l = sorted(active, key=pri, reverse=True)
    for l in sorted_l[:max_lessons]:
        l["applied"] = l.get("applied", 0) + 1
    _save(LESSONS_FILE, data)
    return sorted_l[:max_lessons]


def build_lessons_prompt() -> str:
    lessons = get_active_lessons()
    if not lessons: return ""
    lines = ["\n\n## ARIA 과거 실수 교훈 (반드시 반영)",
             "아래는 과거 분석에서 틀렸던 것들입니다. 이번 분석에서 같은 실수를 반복하지 마세요:\n"]
    for i, l in enumerate(lessons, 1):
        mark = "!!!" if l["severity"]=="high" else "!!" if l["severity"]=="medium" else "!"
        lines.append(str(i) + ". [" + mark + "] [" + l["category"] + "] " + l["lesson"]
                     + " (출처: " + l["source"] + " " + l["date"] + ")")
    return "\n".join(lines)


_DAWN_LESSON_SYS = """You are ARIA-LessonExtractor.
Compare today's analysis results with what actually happened.
Return ONLY valid JSON. No markdown.
{"has_lessons":true,"lessons":[{"category":"레짐판단/VIX/섹터/지정학/한국시장","lesson":"","severity":"high/medium/low","what_happened":"","what_was_predicted":""}],"overall_assessment":""}"""


def extract_dawn_lessons(today_analyses: list, actual_news: str):
    if not today_analyses: return
    summary = [{"time": a.get("analysis_time",""), "regime": a.get("market_regime",""),
                "trend": a.get("trend_phase",""), "one_line": a.get("one_line_summary",""),
                "thesis_killers": a.get("thesis_killers",[])[:2]} for a in today_analyses]
    full = ""
    with client.messages.stream(
        model=MODEL, max_tokens=1500, system=_DAWN_LESSON_SYS,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        messages=[{"role": "user", "content": "Search today's market outcomes and compare:\n"
                   + json.dumps({"today_analyses": summary, "actual_news": actual_news, "analysis_date": _today()},
                                ensure_ascii=False) + "\n\nReturn JSON."}]
    ) as s:
        for ev in s:
            if getattr(ev, "type", "") == "content_block_delta":
                d = getattr(ev, "delta", None)
                if d and getattr(d, "type", "") == "text_delta":
                    full += d.text

    m = re.search(r"\{[\s\S]*\}", re.sub(r"```json|```", "", full).strip())
    if not m: return
    try:
        result = json.loads(m.group())
    except Exception: return
    if not result.get("has_lessons"): return
    for l in result.get("lessons", []):
        add_lesson("dawn", l.get("category","기타"), l.get("lesson",""), l.get("severity","medium"))
        print("Lesson: [" + l.get("category","") + "] " + l.get("lesson","")[:50])


def extract_weekly_lessons(memory_data: list, accuracy_data: dict):
    week_ago = (_now() - timedelta(days=7)).strftime("%Y-%m-%d")
    analyses = [m for m in memory_data if isinstance(m, dict) and m.get("analysis_date","") >= week_ago]
    if not analyses: return

    regimes = [a.get("market_regime","") for a in analyses]
    if len(set(regimes)) >= 3:
        add_lesson("weekly", "레짐판단",
                   "이번 주 레짐 판단이 " + str(len(set(regimes))) + "번 바뀜. 지정학 이슈 시 더 보수적 접근 필요.", "medium")

    for cat, s in accuracy_data.get("by_category", {}).items():
        if s.get("total", 0) >= 3 and s["correct"] / s["total"] < 0.4:
            add_lesson("weekly", cat,
                       cat + " 예측 정확도 " + str(round(s["correct"]/s["total"]*100)) + "% - 반론 더 강하게 적용.", "high")
    print("Weekly lessons extracted")


def extract_monthly_lessons(memory_data: list, accuracy_data: dict):
    month = (_now().replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    ma    = [m for m in memory_data if isinstance(m, dict) and m.get("analysis_date","").startswith(month)]
    if not ma: return
    total = len(ma)
    risk_on  = sum(1 for a in ma if "선호" in a.get("market_regime",""))
    risk_off = sum(1 for a in ma if "회피" in a.get("market_regime",""))
    if risk_on / total > 0.7:
        add_lesson("monthly","레짐판단","지난달 위험선호 " + str(round(risk_on/total*100)) + "% — 낙관 편향 주의.","high")
    elif risk_off / total > 0.7:
        add_lesson("monthly","레짐판단","지난달 위험회피 " + str(round(risk_off/total*100)) + "% — 비관 편향 주의.","medium")
    t, c = accuracy_data.get("total",0), accuracy_data.get("correct",0)
    if t >= 10 and c / t < 0.5:
        add_lesson("monthly","전반","지난달 정확도 " + str(round(c/t*100)) + "% — Devil 반론 더 강하게 반영.","high")
    print("Monthly lessons extracted")
