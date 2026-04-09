import os
import sys
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

KST            = timezone(timedelta(hours=9))
SENTIMENT_FILE = Path("sentiment.json")


def now_kst():
    return datetime.now(KST)


def calculate_sentiment(report):
    """7개 요소로 세분화된 감정지수 계산"""

    regime   = report.get("market_regime", "")
    trend    = report.get("trend_phase", "")
    vi       = report.get("volatility_index", {})
    outflows = report.get("outflows", [])
    inflows  = report.get("inflows", [])
    counters = report.get("counterarguments", [])
    korea    = report.get("korea_focus", {})
    hidden   = report.get("hidden_signals", [])

    components = {}

    # 1. 시장 레짐 (최대 ±20점)
    if "선호" in regime:
        components["시장레짐"] = {"score": 20, "reason": "위험선호 (" + regime + ")"}
    elif "회피" in regime:
        components["시장레짐"] = {"score": -20, "reason": "위험회피 (" + regime + ")"}
    elif "전환" in regime:
        components["시장레짐"] = {"score": 5, "reason": "전환 중 (" + regime + ")"}
    else:
        components["시장레짐"] = {"score": 0, "reason": "혼조 (" + regime + ")"}

    # 2. 추세 방향 (최대 ±15점)
    if "상승" in trend:
        components["추세방향"] = {"score": 15, "reason": "상승추세"}
    elif "하락" in trend:
        components["추세방향"] = {"score": -15, "reason": "하락추세"}
    else:
        components["추세방향"] = {"score": 0, "reason": "횡보추세"}

    # 3. VIX/VKOSPI (최대 ±20점)
    vix_level = vi.get("level", "")
    vix_str   = "VIX " + vi.get("vix", "?") + " / VKOSPI " + vi.get("vkospi", "?")
    if "극단공포" in vix_level:
        components["변동성지수"] = {"score": -20, "reason": "극단공포 (" + vix_str + ")"}
    elif "공포" in vix_level:
        components["변동성지수"] = {"score": -10, "reason": "공포 (" + vix_str + ")"}
    elif "극단탐욕" in vix_level:
        components["변동성지수"] = {"score": 20, "reason": "극단탐욕 (" + vix_str + ")"}
    elif "탐욕" in vix_level:
        components["변동성지수"] = {"score": 10, "reason": "탐욕 (" + vix_str + ")"}
    else:
        components["변동성지수"] = {"score": 0, "reason": "중립 (" + vix_str + ")"}

    # 4. 자금 흐름 (최대 ±15점)
    out_count = len(outflows)
    in_count  = len(inflows)
    high_out  = sum(1 for o in outflows if o.get("severity") == "높음")
    strong_in = sum(1 for i in inflows if i.get("momentum") == "강함")
    flow_score = (strong_in * 5) - (high_out * 5) + (in_count - out_count) * 2
    flow_score = max(-15, min(15, flow_score))
    components["자금흐름"] = {
        "score": flow_score,
        "reason": "유입 " + str(in_count) + "개 / 유출 " + str(out_count) + "개"
    }

    # 5. 반론 강도 (최대 -10점)
    high_risk = sum(1 for c in counters if c.get("risk_level") == "높음")
    mid_risk  = sum(1 for c in counters if c.get("risk_level") == "보통")
    counter_score = -(high_risk * 4 + mid_risk * 2)
    counter_score = max(-10, counter_score)
    components["반론강도"] = {
        "score": counter_score,
        "reason": "고위험 " + str(high_risk) + "개 / 중위험 " + str(mid_risk) + "개"
    }

    # 6. 한국시장 특수 (최대 ±10점)
    kr_score  = 0
    kr_reason = []
    krw = korea.get("krw_usd", "")
    kospi = korea.get("kospi_flow", "")
    if "강세" in krw or "하락" in krw:
        kr_score -= 3
        kr_reason.append("원화약세")
    elif "약세" in krw or "상승" in krw:
        kr_score += 3
        kr_reason.append("원화강세")
    if "하락" in kospi or "-" in kospi:
        kr_score -= 4
        kr_reason.append("코스피하락")
    elif "상승" in kospi or "+" in kospi:
        kr_score += 4
        kr_reason.append("코스피상승")
    kr_score = max(-10, min(10, kr_score))
    components["한국시장"] = {
        "score": kr_score,
        "reason": ", ".join(kr_reason) if kr_reason else "중립"
    }

    # 7. 숨겨진 시그널 (최대 ±10점)
    high_conf = sum(1 for h in hidden if h.get("confidence") == "높음")
    low_conf  = sum(1 for h in hidden if h.get("confidence") == "낮음")
    hidden_score = (high_conf * 3) - (low_conf * 2)
    hidden_score = max(-10, min(10, hidden_score))
    components["숨은시그널"] = {
        "score": hidden_score,
        "reason": "고신뢰 " + str(high_conf) + "개 / 저신뢰 " + str(low_conf) + "개"
    }

    # 총점 계산 (50 기준점 + 각 요소 합산)
    total_delta = sum(c["score"] for c in components.values())
    score = 50 + total_delta
    score = max(0, min(100, score))

    # 레벨 판정
    if score <= 20:
        level = "극단공포"
        emoji = "😱"
    elif score <= 40:
        level = "공포"
        emoji = "😰"
    elif score <= 60:
        level = "중립"
        emoji = "😐"
    elif score <= 80:
        level = "탐욕"
        emoji = "😏"
    else:
        level = "극단탐욕"
        emoji = "🤑"

    return {
        "date":       now_kst().strftime("%Y-%m-%d"),
        "score":      score,
        "level":      level,
        "emoji":      emoji,
        "components": components,
        "regime":     regime,
        "trend":      trend,
        "vix_level":  vix_level,
    }


def analyze_trend(history):
    if len(history) < 2:
        return {"direction": "neutral", "change": 0, "avg_7d": 50, "min_30d": 50, "max_30d": 50}

    scores_7d  = [h["score"] for h in history[-7:]]
    scores_30d = [h["score"] for h in history[-30:]]

    half       = len(scores_7d) // 2
    first_avg  = sum(scores_7d[:half]) / max(half, 1)
    second_avg = sum(scores_7d[half:]) / max(len(scores_7d) - half, 1)
    change     = round(second_avg - first_avg, 1)

    if change > 5:
        direction = "improving"
    elif change < -5:
        direction = "deteriorating"
    else:
        direction = "stable"

    return {
        "direction": direction,
        "change":    change,
        "avg_7d":    round(sum(scores_7d) / len(scores_7d), 1),
        "min_30d":   min(scores_30d),
        "max_30d":   max(scores_30d),
        "avg_30d":   round(sum(scores_30d) / len(scores_30d), 1),
    }


def get_percentile(score, history):
    """현재 점수가 30일 중 몇 % 위치인지"""
    scores = [h["score"] for h in history[-30:]]
    if not scores:
        return 50
    below = sum(1 for s in scores if s <= score)
    return round(below / len(scores) * 100)


def make_component_bar(score, max_val=20):
    """점수를 시각적 바로 변환"""
    abs_score = abs(score)
    bar_len   = round(abs_score / max_val * 6)
    bar       = "#" * bar_len
    empty     = "." * (6 - bar_len)
    if score > 0:
        return "[" + bar + empty + "] +" + str(score) + "점"
    elif score < 0:
        return "[" + bar + empty + "] " + str(score) + "점"
    else:
        return "[......] 0점"


def make_history_chart(history, days=14):
    recent = history[-days:]
    if not recent:
        return "No data"
    lines = []
    for h in recent:
        score      = h["score"]
        bar_len    = int(score / 5)
        bar        = "#" * bar_len
        empty      = " " * (20 - bar_len)
        date_short = h["date"][5:]
        lines.append(date_short + " [" + bar + empty + "] " + str(score) + " " + h.get("emoji", ""))
    return "\n".join(lines)


def load_sentiment():
    if SENTIMENT_FILE.exists():
        return json.loads(SENTIMENT_FILE.read_text(encoding="utf-8"))
    return {"history": [], "current": None}


def save_sentiment(data):
    SENTIMENT_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def update_sentiment(report):
    data    = load_sentiment()
    today   = calculate_sentiment(report)
    history = data.get("history", [])

    history = [h for h in history if h["date"] != today["date"]]
    history.append(today)
    history = history[-90:]

    trend = analyze_trend(history)
    data  = {"history": history, "current": today, "trend": trend}
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

    if trend.get("direction") == "improving":
        arrow = "↑ 개선중"
    elif trend.get("direction") == "deteriorating":
        arrow = "↓ 악화중"
    else:
        arrow = "→ 안정적"

    percentile = get_percentile(score, history)

    # 구성 요소 바
    comp_lines = []
    for name, info in components.items():
        bar = make_component_bar(info["score"])
        comp_lines.append("  " + name.ljust(6) + " " + bar)
        comp_lines.append("           " + info["reason"])

    # 히스토리 차트
    chart = make_history_chart(history)

    # 30일 통계
    min_30 = trend.get("min_30d", score)
    max_30 = trend.get("max_30d", score)
    avg_30 = trend.get("avg_30d", score)

    # 투자 시사점
    if score <= 20:
        insight = "극단 공포 - 역사적 분할매수 최적 타이밍"
    elif score <= 35:
        insight = "공포 구간 - 분할매수 적극 검토 시점"
    elif score <= 50:
        insight = "공포 우위 - 신중한 접근, 분할매수 유지"
    elif score <= 65:
        insight = "중립 구간 - 추세 확인 후 대응"
    elif score <= 80:
        insight = "탐욕 구간 - 리스크 관리 강화"
    else:
        insight = "극단 탐욕 - 비중 축소 및 현금 확보 고려"

    lines = [
        emoji + " <b>ARIA 시장 감정지수</b>",
        "<code>" + current.get("date", "") + "</code>",
        "",
        "오늘: <b>" + str(score) + "/100</b> (" + level + ")",
        "추세: " + arrow + " | 7일 평균: " + str(trend.get("avg_7d", "-")),
        "",
        "━━ 구성 요소별 분석 ━━",
        "<pre>" + "\n".join(comp_lines) + "</pre>",
        "",
        "━━ " + str(len(history[-14:])) + "일 추이 ━━",
        "<pre>" + chart + "</pre>",
        "",
        "━━ 30일 통계 ━━",
        "최저: " + str(min_30) + " | 최고: " + str(max_30) + " | 평균: " + str(avg_30),
        "현재 위치: 하위 " + str(percentile) + "% 구간",
        "",
        "💡 " + insight,
    ]

    send_message("\n".join(lines))
    print("Sentiment report sent. Score: " + str(score) + " / " + level)


def run_sentiment(report):
    data = update_sentiment(report)
    send_sentiment_report(data)
    return data


if __name__ == "__main__":
    test = {
        "market_regime": "위험회피",
        "trend_phase": "하락추세",
        "volatility_index": {"level": "공포", "vix": "28.5", "vkospi": "35.2"},
        "outflows": [
            {"zone": "반도체", "severity": "높음"},
            {"zone": "성장주", "severity": "보통"},
            {"zone": "암호화폐", "severity": "높음"},
        ],
        "inflows": [{"zone": "금/안전자산", "momentum": "강함"}],
        "counterarguments": [
            {"risk_level": "높음"},
            {"risk_level": "높음"},
        ],
        "korea_focus": {"krw_usd": "1487 (약세)", "kospi_flow": "-1.8% 하락"},
        "hidden_signals": [
            {"confidence": "낮음"},
            {"confidence": "낮음"},
        ],
    }
    result = run_sentiment(test)
    print("Score: " + str(result["current"]["score"]) + " / " + result["current"]["level"])
