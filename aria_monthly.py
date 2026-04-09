import os
import sys
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

KST           = timezone(timedelta(hours=9))
MEMORY_FILE   = Path("memory.json")
ACCURACY_FILE = Path("accuracy.json")
SENTIMENT_FILE = Path("sentiment.json")
ROTATION_FILE = Path("rotation.json")


def load_json(path):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def get_last_month_str():
    now = datetime.now(KST)
    first_this_month = now.replace(day=1)
    last_month_end   = first_this_month - timedelta(days=1)
    return last_month_end.strftime("%Y-%m")


def get_month_data():
    last_month = get_last_month_str()

    memory    = load_json(MEMORY_FILE)
    accuracy  = load_json(ACCURACY_FILE)
    sentiment = load_json(SENTIMENT_FILE)
    rotation  = load_json(ROTATION_FILE)

    if not isinstance(memory, list):
        memory = []

    month_memory    = [m for m in memory if m.get("analysis_date", "").startswith(last_month)]
    month_accuracy  = [h for h in accuracy.get("history", []) if h.get("date", "").startswith(last_month)]
    month_sentiment = [h for h in sentiment.get("history", []) if h.get("date", "").startswith(last_month)]

    return last_month, month_memory, month_accuracy, month_sentiment, rotation


def analyze_month(last_month, month_memory, month_accuracy, month_sentiment, rotation):
    result = {"month": last_month}

    # 분석 일수
    result["total_days"] = len(month_memory)

    # 예측 정확도
    if month_accuracy:
        total   = sum(h.get("total", 0) for h in month_accuracy)
        correct = sum(h.get("correct", 0) for h in month_accuracy)
        result["accuracy"] = round(correct / total * 100, 1) if total > 0 else 0
        result["accuracy_total"]   = total
        result["accuracy_correct"] = correct
    else:
        result["accuracy"] = 0
        result["accuracy_total"]   = 0
        result["accuracy_correct"] = 0

    # 레짐 분포
    regimes = [m.get("market_regime", "") for m in month_memory]
    regime_count = {}
    for r in regimes:
        regime_count[r] = regime_count.get(r, 0) + 1
    result["regime_distribution"] = regime_count
    result["dominant_regime"] = max(regime_count, key=regime_count.get) if regime_count else ""

    # 추세 분포
    trends = [m.get("trend_phase", "") for m in month_memory]
    trend_count = {}
    for t in trends:
        trend_count[t] = trend_count.get(t, 0) + 1
    result["trend_distribution"] = trend_count

    # 감정지수
    if month_sentiment:
        scores = [h.get("score", 50) for h in month_sentiment]
        result["sentiment_avg"] = round(sum(scores) / len(scores), 1)
        result["sentiment_min"] = min(scores)
        result["sentiment_max"] = max(scores)
        min_day = month_sentiment[scores.index(min(scores))].get("date", "")
        max_day = month_sentiment[scores.index(max(scores))].get("date", "")
        result["sentiment_min_day"] = min_day
        result["sentiment_max_day"] = max_day
    else:
        result["sentiment_avg"] = 50
        result["sentiment_min"] = 50
        result["sentiment_max"] = 50

    # 섹터 로테이션 (이번달 상위/하위)
    ranking = rotation.get("ranking", [])
    result["top_sectors"]    = [r[0] for r in ranking[:3]] if ranking else []
    result["bottom_sectors"] = [r[0] for r in ranking[-3:]] if ranking else []

    # ARIA 성장 지표
    all_memory = load_json(MEMORY_FILE)
    result["total_analyses"] = len(all_memory) if isinstance(all_memory, list) else 0

    all_accuracy = load_json(ACCURACY_FILE)
    total_all   = all_accuracy.get("total", 0)
    correct_all = all_accuracy.get("correct", 0)
    result["overall_accuracy"] = round(correct_all / total_all * 100, 1) if total_all > 0 else 0
    result["strong_areas"] = all_accuracy.get("strong_areas", [])
    result["weak_areas"]   = all_accuracy.get("weak_areas", [])

    return result


def send_monthly_report(analysis):
    try:
        from aria_telegram import send_message
    except ImportError:
        print("aria_telegram not found")
        return

    month        = analysis.get("month", "")
    total_days   = analysis.get("total_days", 0)
    accuracy     = analysis.get("accuracy", 0)
    overall_acc  = analysis.get("overall_accuracy", 0)
    sent_avg     = analysis.get("sentiment_avg", 50)
    sent_min     = analysis.get("sentiment_min", 50)
    sent_max     = analysis.get("sentiment_max", 50)
    dom_regime   = analysis.get("dominant_regime", "")
    top_sectors  = analysis.get("top_sectors", [])
    bot_sectors  = analysis.get("bottom_sectors", [])
    strong_areas = analysis.get("strong_areas", [])
    weak_areas   = analysis.get("weak_areas", [])
    total_ana    = analysis.get("total_analyses", 0)

    regime_dist = analysis.get("regime_distribution", {})
    trend_dist  = analysis.get("trend_distribution", {})

    acc_emoji = "📈" if accuracy >= 65 else "📉" if accuracy < 50 else "➡️"

    lines = [
        "<b>📊 ARIA " + month + " 월간 리포트</b>",
        "",
        "━━ 이달의 분석 성과 ━━",
        "분석 일수: <b>" + str(total_days) + "일</b>",
        acc_emoji + " 예측 정확도: <b>" + str(accuracy) + "%</b>",
        "   (" + str(analysis.get("accuracy_correct", 0)) + "/" + str(analysis.get("accuracy_total", 0)) + "개 적중)",
        "누적 정확도: " + str(overall_acc) + "%",
        "",
        "━━ 이달의 시장 특성 ━━",
        "지배 레짐: <b>" + dom_regime + "</b>",
    ]

    if regime_dist:
        dist_str = " | ".join([k + " " + str(v) + "일" for k, v in regime_dist.items()])
        lines.append("분포: " + dist_str)

    if trend_dist:
        t_str = " | ".join([k + " " + str(v) + "일" for k, v in trend_dist.items()])
        lines.append("추세: " + t_str)

    lines += [
        "",
        "━━ 감정지수 ━━",
        "평균: <b>" + str(sent_avg) + "</b>",
        "최저: " + str(sent_min) + " (" + analysis.get("sentiment_min_day", "") + ")",
        "최고: " + str(sent_max) + " (" + analysis.get("sentiment_max_day", "") + ")",
        "",
        "━━ 섹터 로테이션 ━━",
    ]

    if top_sectors:
        lines.append("🔥 강세: " + " > ".join(top_sectors))
    if bot_sectors:
        lines.append("❄️ 약세: " + " > ".join(bot_sectors))

    lines.append("")
    lines.append("━━ ARIA 성장 현황 ━━")

    if strong_areas:
        lines.append("💪 강점: " + ", ".join(strong_areas[:3]))
    if weak_areas:
        lines.append("📚 개선중: " + ", ".join(weak_areas[:3]))

    lines += [
        "",
        "<code>ARIA 누적 분석 " + str(total_ana) + "일 | 계속 성장 중</code>",
    ]

    send_message("\n".join(lines))
    print("Monthly report sent for " + month)


def run_monthly():
    last_month, month_memory, month_accuracy, month_sentiment, rotation = get_month_data()
    analysis = analyze_month(last_month, month_memory, month_accuracy, month_sentiment, rotation)
    send_monthly_report(analysis)
    return analysis


if __name__ == "__main__":
    run_monthly()
