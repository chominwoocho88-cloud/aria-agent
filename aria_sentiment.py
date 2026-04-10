import os
import sys
import json
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

KST            = timezone(timedelta(hours=9))
SENTIMENT_FILE = Path("sentiment.json")


def now_kst():
    return datetime.now(KST)


def parse_vix_number(vix_str):
    if not vix_str:
        return None
    m = re.search(r"(\d+\.?\d*)", str(vix_str))
    return float(m.group(1)) if m else None


def calculate_sentiment(report, market_data=None):
    try:
        from aria_weights import get_sentiment_weights
        sw = get_sentiment_weights()
    except ImportError:
        sw = {k: 1.0 for k in ["시장레짐","추세방향","변동성지수","자금흐름","반론강도","한국시장","숨은시그널"]}

    regime   = report.get("market_regime", "")
    trend    = report.get("trend_phase", "")
    vi       = report.get("volatility_index", {})
    outflows = report.get("outflows", [])
    inflows  = report.get("inflows", [])
    counters = report.get("counterarguments", [])
    korea    = report.get("korea_focus", {})
    hidden   = report.get("hidden_signals", [])

    # 실제 수치 우선 추출
    vix_val    = parse_vix_number(market_data.get("vix")) if market_data else None
    vix_val    = vix_val or parse_vix_number(vi.get("vix"))
    vkospi_val = parse_vix_number(vi.get("vkospi"))

    fg_raw = None
    if market_data:
        fg_raw = parse_vix_number(market_data.get("fear_greed_value"))
    if fg_raw is None:
        fg_raw = parse_vix_number(vi.get("fear_greed"))

    components = {}

    # 1. 시장 레짐
    if "선호" in regime:   raw = 20
    elif "회피" in regime: raw = -20
    elif "전환" in regime: raw = 5
    else:                  raw = 0
    if fg_raw is not None and fg_raw <= 25 and raw > 0:
        raw = raw // 2
    components["시장레짐"] = {
        "score":  round(raw * sw.get("시장레짐", 1.0)),
        "reason": regime[:25] if regime else "데이터없음"
    }

    # 2. 추세 방향
    if "상승" in trend:   raw = 15
    elif "하락" in trend: raw = -15
    else:                 raw = 0
    components["추세방향"] = {
        "score":  round(raw * sw.get("추세방향", 1.0)),
        "reason": trend if trend else "데이터없음"
    }

    # 3. 변동성 지수 (Fear&Greed 실수치 우선)
    raw    = 0
    reason = ""
    if fg_raw is not None:
        if fg_raw <= 20:
            raw    = -25
            reason = "Fear&Greed " + str(fg_raw) + " (극단공포)"
        elif fg_raw <= 35:
            raw    = -15
            reason = "Fear&Greed " + str(fg_raw) + " (공포)"
        elif fg_raw <= 55:
            raw    = 0
            reason = "Fear&Greed " + str(fg_raw) + " (중립)"
        elif fg_raw <= 75:
            raw    = 12
            reason = "Fear&Greed " + str(fg_raw) + " (탐욕)"
        else:
            raw    = 20
            reason = "Fear&Greed " + str(fg_raw) + " (극단탐욕)"
        if vix_val:
            if vix_val >= 30:   raw = min(raw, -10)
            elif vix_val <= 15: raw = max(raw, 5)
    else:
        vix_level = vi.get("level", "")
        if vix_val:
            if vix_val >= 35:   raw, reason = -20, "VIX " + str(vix_val) + " 극단공포"
            elif vix_val >= 25: raw, reason = -10, "VIX " + str(vix_val) + " 공포"
            elif vix_val >= 20: raw, reason = -5,  "VIX " + str(vix_val) + " 경계"
            elif vix_val <= 15: raw, reason = 10,  "VIX " + str(vix_val) + " 안정"
        elif "극단공포" in vix_level: raw, reason = -20, vix_level
        elif "공포" in vix_level:     raw, reason = -10, vix_level
        elif "극단탐욕" in vix_level: raw, reason = 20,  vix_level
        elif "탐욕" in vix_level:     raw, reason = 10,  vix_level

    components["변동성지수"] = {
        "score":  round(raw * sw.get("변동성지수", 1.5)),
        "reason": reason[:30] if reason else "데이터없음"
    }

    # 4. 자금 흐름
    out_count = len(outflows)
    in_count  = len(inflows)
    high_out  = sum(1 for o in outflows if o.get("severity") == "높음")
    strong_in = sum(1 for i in inflows  if i.get("momentum") == "강함")
    raw       = (strong_in * 5) - (high_out * 5) + (in_count - out_count) * 2
    raw       = max(-15, min(15, raw))
    components["자금흐름"] = {
        "score":  round(raw * sw.get("자금흐름", 1.0)),
        "reason": "유입" + str(in_count) + " / 유출" + str(out_count)
    }

    # 5. 반론 강도
    high_risk = sum(1 for c in counters if c.get("risk_level") == "높음")
    mid_risk  = sum(1 for c in counters if c.get("risk_level") == "보통")
    raw       = -(high_risk * 4 + mid_risk * 2)
    raw       = max(-10, raw)
    components["반론강도"] = {
        "score":  round(raw * sw.get("반론강도", 0.8)),
        "reason": "고위험" + str(high_risk) + " / 중위험" + str(mid_risk)
    }

    # 6. 한국시장
    raw      = 0
    kr_notes = []
    krw      = korea.get("krw_usd", "")
    kospi    = korea.get("kospi_flow", "")
    if "약세" in krw or "하락" in krw:
        raw -= 3; kr_notes.append("원화약세")
    elif "강세" in krw or "상승" in krw:
        raw += 3; kr_notes.append("원화강세")
    if "하락" in kospi or "-" in kospi:
        raw -= 4; kr_notes.append("코스피하락")
    elif "상승" in kospi or "+" in kospi:
        raw += 4; kr_notes.append("코스피상승")
    if market_data:
        try:
            k_chg = float(str(market_data.get("kospi_change","0%")).replace("%","").replace("+",""))
            if k_chg >= 2:    raw += 3
            elif k_chg <= -2: raw -= 3
        except:
            pass
    raw = max(-10, min(10, raw))
    components["한국시장"] = {
        "score":  round(raw * sw.get("한국시장", 0.8)),
        "reason": ", ".join(kr_notes) if kr_notes else "중립"
    }

    # 7. 숨겨진 시그널
    high_conf = sum(1 for h in hidden if h.get("confidence") == "높음")
    low_conf  = sum(1 for h in hidden if h.get("confidence") == "낮음")
    raw       = (high_conf * 3) - (low_conf * 2)
    raw       = max(-10, min(10, raw))
    components["숨은시그널"] = {
        "score":  round(raw * sw.get("숨은시그널", 0.7)),
        "reason": "고신뢰" + str(high_conf) + " / 저신뢰" + str(low_conf)
    }

    # 총점
    total_delta = sum(c["score"] for c in components.values())
    score       = 50 + total_delta

    # 캡 적용
    if fg_raw is not None and fg_raw <= 25:
        score = min(score, 45)
    if fg_raw is not None and fg_raw >= 80:
        score = max(score, 70)
    if vix_val and vix_val >= 25:
        score = min(score, 60)
    if vkospi_val and vkospi_val >= 40:
        score = min(score, 55)

    score = max(0, min(100, score))

    if score <= 20:   level, emoji = "극단공포", "😱"
    elif score <= 40: level, emoji = "공포",     "😰"
    elif score <= 60: level, emoji = "중립",     "😐"
    elif score <= 80: level, emoji = "탐욕",     "😏"
    else:             level, emoji = "극단탐욕", "🤑"

    return {
        "date":        now_kst().strftime("%Y-%m-%d"),
        "score":       score,
        "level":       level,
        "emoji":       emoji,
        "components":  components,
        "regime":      regime,
        "trend":       trend,
        "vix_level":   vi.get("level", ""),
        "vix_val":     vix_val,
        "vkospi_val":  vkospi_val,
        "fear_greed":  fg_raw,
    }


def analyze_trend(history):
    if len(history) < 2:
        return {"direction": "neutral", "change": 0, "avg_7d": 50, "min_30d": 50, "max_30d": 50, "avg_30d": 50}
    scores_7d  = [h["score"] for h in history[-7:]]
    scores_30d = [h["score"] for h in history[-30:]]
    half       = len(scores_7d) // 2
    first_avg  = sum(scores_7d[:half]) / max(half, 1)
    second_avg = sum(scores_7d[half:]) / max(len(scores_7d) - half, 1)
    change     = round(second_avg - first_avg, 1)
    if change > 5:    direction = "improving"
    elif change < -5: direction = "deteriorating"
    else:             direction = "stable"
    return {
        "direction": direction,
        "change":    change,
        "avg_7d":    round(sum(scores_7d) / len(scores_7d), 1),
        "min_30d":   min(scores_30d),
        "max_30d":   max(scores_30d),
        "avg_30d":   round(sum(scores_30d) / len(scores_30d), 1),
    }


def get_percentile(score, history):
    scores = [h["score"] for h in history[-30:]]
    if not scores:
        return 50
    below = sum(1 for s in scores if s <= score)
    return round(below / len(scores) * 100)


def make_component_bar(score, max_val=20):
    abs_s = abs(score)
    bar   = "█" * min(5, round(abs_s / max_val * 5))
    empty = "░" * (5 - len(bar))
    sign  = "+" if score > 0 else ""
    return "[" + bar + empty + "] " + sign + str(score)


def make_history_chart(history, days=10):
    recent = history[-days:]
    if not recent:
        return "No data"
    lines = []
    for h in recent:
        score      = h["score"]
        bar_len    = int(score / 10)
        bar        = "█" * bar_len
        empty      = "░" * (10 - bar_len)
        date_short = h["date"][5:]
        lines.append(date_short + " " + bar + empty + " " + str(score) + h.get("emoji", ""))
    return "\n".join(lines)


def load_sentiment():
    if SENTIMENT_FILE.exists():
        return json.loads(SENTIMENT_FILE.read_text(encoding="utf-8"))
    return {"history": [], "current": None}


def save_sentiment(data):
    SENTIMENT_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def update_sentiment(report, market_data=None):
    data    = load_sentiment()
    new     = calculate_sentiment(report, market_data)
    history = data.get("history", [])

    existing = next((h for h in history if h["date"] == new["date"]), None)
    if existing:
        blended        = round(existing["score"] * 0.4 + new["score"] * 0.6)
        new["score"]   = blended
        if blended <= 20:   new["level"], new["emoji"] = "극단공포", "😱"
        elif blended <= 40: new["level"], new["emoji"] = "공포",     "😰"
        elif blended <= 60: new["level"], new["emoji"] = "중립",     "😐"
        elif blended <= 80: new["level"], new["emoji"] = "탐욕",     "😏"
        else:               new["level"], new["emoji"] = "극단탐욕", "🤑"

    history = [h for h in history if h["date"] != new["date"]]
    history.append(new)
    history = history[-90:]

    trend = analyze_trend(history)
    data  = {"history": history, "current": new, "trend": trend}
    save_sentiment(data)
    return data


def send_sentiment_report(data):
    try:
        from aria_telegram import send_message
    except ImportError:
        print("aria_telegram not found")
        return

    current    = data.get("current", {})
    trend      = data.get("trend", {})
    history    = data.get("history", [])
    components = current.get("components", {})

    score = current.get("score", 50)
    level = current.get("level", "")
    emoji = current.get("emoji", "")

    if trend.get("direction") == "improving":     arrow = "↑ 개선중"
    elif trend.get("direction") == "deteriorating": arrow = "↓ 악화중"
    else:                                           arrow = "→ 안정"

    percentile = get_percentile(score, history)

    comp_lines = []
    for name, info in components.items():
        bar    = make_component_bar(info["score"])
        reason = info["reason"][:18]
        comp_lines.append(name[:5] + " " + bar)
        comp_lines.append("  " + reason)

    chart  = make_history_chart(history)
    min_30 = trend.get("min_30d", score)
    max_30 = trend.get("max_30d", score)
    avg_30 = trend.get("avg_30d", score)

    if score <= 20:   insight = "극단공포 - 분할매수 최적 타이밍"
    elif score <= 35: insight = "공포 - 분할매수 적극 검토"
    elif score <= 50: insight = "공포우위 - 신중한 분할매수"
    elif score <= 65: insight = "중립 - 추세 확인 후 대응"
    elif score <= 80: insight = "탐욕 - 리스크 관리 강화"
    else:             insight = "극단탐욕 - 비중 축소 고려"

    fg_val = current.get("fear_greed")
    vix_cap_note = ""
    if fg_val is not None:
        vix_cap_note = "\n<i>Fear&Greed " + str(fg_val) + " 실수치 반영</i>"

    lines = [
        emoji + " <b>ARIA 시장 감정지수</b>",
        "<code>" + current.get("date", "") + "</code>",
        "",
        "오늘: <b>" + str(score) + "/100</b> (" + level + ")",
        "추세: " + arrow + " | 7일평균: " + str(trend.get("avg_7d", "-")),
        vix_cap_note,
        "",
        "━━ 구성요소 ━━",
        "<pre>" + "\n".join(comp_lines) + "</pre>",
        "",
        "━━ " + str(len(history[-10:])) + "일 추이 ━━",
        "<pre>" + chart + "</pre>",
        "",
        "최저:" + str(min_30) + " 최고:" + str(max_30) + " 평균:" + str(avg_30),
        "현재: 하위 " + str(percentile) + "% 구간",
        "",
        "💡 " + insight,
    ]

    send_message("\n".join(lines))
    print("Sentiment report sent. Score: " + str(score) + " / " + level)


def run_sentiment(report, market_data=None):
    data = update_sentiment(report, market_data)
    send_sentiment_report(data)
    return data


if __name__ == "__main__":
    test = {
        "market_regime": "위험선호",
        "trend_phase":   "상승추세",
        "volatility_index": {"level": "공포", "vix": "19.49"},
        "outflows": [{}, {}],
        "inflows":  [{}, {}, {}],
        "counterarguments": [{"risk_level": "높음"}, {"risk_level": "높음"}],
        "korea_focus": {"krw_usd": "1483", "kospi_flow": "+1.4%"},
        "hidden_signals": [{"confidence": "높음"}],
    }
    test_market = {
        "vix":              "19.49",
        "fear_greed_value": "16",
        "fear_greed_rating": "Extreme Fear",
        "kospi_change":     "+1.4%",
    }
    result = run_sentiment(test, test_market)
    print("Score: " + str(result["current"]["score"]) + " / " + result["current"]["level"])
    print("(Fear&Greed 16 반영 → 45점 이하 예상)")
