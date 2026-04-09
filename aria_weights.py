import os
import sys
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

KST          = timezone(timedelta(hours=9))
WEIGHTS_FILE = Path("aria_weights.json")

# 기본 가중치 (처음 실행 시 이 값으로 시작)
DEFAULT_WEIGHTS = {
    "version": 1,
    "last_updated": "",
    "total_learning_cycles": 0,

    # 감정지수 구성요소 가중치 (기본값 1.0, 범위 0.5~2.0)
    "sentiment": {
        "시장레짐":    1.0,   # 위험선호/회피 레짐 반영 비중
        "추세방향":    1.0,   # 상승/하락 추세 반영 비중
        "변동성지수":  1.5,   # VIX/VKOSPI 반영 비중 (기본 높게 시작)
        "자금흐름":    1.0,   # 유입/유출 반영 비중
        "반론강도":    0.8,   # Devil 반론 반영 비중
        "한국시장":    0.8,   # 한국 시장 특수 반영 비중
        "숨은시그널":  0.7,   # 숨겨진 시그널 반영 비중
    },

    # 예측 카테고리별 신뢰도 (Verifier가 매일 업데이트)
    "prediction_confidence": {
        "금리":    1.0,
        "환율":    1.0,
        "주식":    1.0,
        "지정학":  0.7,   # 기본적으로 지정학은 불확실성 높음
        "원자재":  1.0,
        "기업":    1.0,
        "기타":    0.8,
    },

    # 학습 기록
    "learning_log": [],

    # 각 구성요소 정확도 추적
    "component_accuracy": {
        "시장레짐":   {"correct": 0, "total": 0},
        "추세방향":   {"correct": 0, "total": 0},
        "변동성지수": {"correct": 0, "total": 0},
        "자금흐름":   {"correct": 0, "total": 0},
    },
}


def load_weights():
    if WEIGHTS_FILE.exists():
        saved = json.loads(WEIGHTS_FILE.read_text(encoding="utf-8"))
        # 새로운 키가 추가됐을 때 기본값으로 채우기
        for key, val in DEFAULT_WEIGHTS.items():
            if key not in saved:
                saved[key] = val
            elif isinstance(val, dict):
                for k2, v2 in val.items():
                    if k2 not in saved[key]:
                        saved[key][k2] = v2
        return saved
    return DEFAULT_WEIGHTS.copy()


def save_weights(weights):
    WEIGHTS_FILE.write_text(
        json.dumps(weights, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def update_weights_from_accuracy(accuracy_data):
    """Verifier 정확도 데이터로 가중치 자동 조정"""
    weights = load_weights()
    today   = datetime.now(KST).strftime("%Y-%m-%d")

    by_cat = accuracy_data.get("by_category", {})
    changes = []

    for cat, stats in by_cat.items():
        total   = stats.get("total", 0)
        correct = stats.get("correct", 0)
        if total < 3:
            continue  # 데이터 부족 시 스킵

        acc = correct / total
        current_conf = weights["prediction_confidence"].get(cat, 1.0)
        new_conf     = current_conf

        if acc >= 0.75:
            # 잘 맞추면 신뢰도 소폭 증가
            new_conf = min(1.5, current_conf + 0.05)
            if new_conf != current_conf:
                changes.append(cat + " 신뢰도 증가: " + str(round(current_conf, 2)) + " -> " + str(round(new_conf, 2)))
        elif acc <= 0.40:
            # 못 맞추면 신뢰도 소폭 감소
            new_conf = max(0.4, current_conf - 0.1)
            if new_conf != current_conf:
                changes.append(cat + " 신뢰도 감소: " + str(round(current_conf, 2)) + " -> " + str(round(new_conf, 2)))

        weights["prediction_confidence"][cat] = round(new_conf, 2)

    # 지정학 반복 실패 시 감정지수 가중치도 조정
    geo_conf = weights["prediction_confidence"].get("지정학", 1.0)
    if geo_conf < 0.6:
        # 지정학 신뢰도 낮으면 레짐 가중치도 줄임 (지정학 이슈가 많은 레짐 오판 방지)
        old_regime = weights["sentiment"]["시장레짐"]
        weights["sentiment"]["시장레짐"] = max(0.7, old_regime - 0.05)
        if weights["sentiment"]["시장레짐"] != old_regime:
            changes.append("시장레짐 가중치 조정: " + str(round(old_regime, 2)) + " -> " + str(round(weights["sentiment"]["시장레짐"], 2)))

    # 학습 기록 추가
    if changes:
        weights["learning_log"].append({
            "date":    today,
            "changes": changes,
            "trigger": "accuracy_update",
        })
        weights["learning_log"] = weights["learning_log"][-30:]
        weights["total_learning_cycles"] += 1

    weights["last_updated"] = today
    save_weights(weights)

    return changes


def get_sentiment_weights():
    return load_weights().get("sentiment", DEFAULT_WEIGHTS["sentiment"])


def get_prediction_confidence(category):
    weights = load_weights()
    return weights["prediction_confidence"].get(category, 1.0)


def get_learning_summary():
    weights = load_weights()
    logs    = weights.get("learning_log", [])
    cycles  = weights.get("total_learning_cycles", 0)
    conf    = weights.get("prediction_confidence", {})

    strong = [k for k, v in conf.items() if v >= 1.2]
    weak   = [k for k, v in conf.items() if v <= 0.6]

    return {
        "total_cycles": cycles,
        "last_updated": weights.get("last_updated", ""),
        "strong_categories": strong,
        "weak_categories":   weak,
        "recent_changes":    logs[-3:] if logs else [],
        "sentiment_weights": weights.get("sentiment", {}),
    }


def send_learning_report():
    """텔레그램으로 학습 현황 리포트"""
    try:
        from aria_telegram import send_message
    except ImportError:
        return

    summary = get_learning_summary()
    weights = load_weights()
    conf    = weights.get("prediction_confidence", {})

    lines = [
        "<b>🧬 ARIA 자기학습 현황</b>",
        "총 학습 사이클: <b>" + str(summary["total_cycles"]) + "회</b>",
        "마지막 업데이트: " + summary["last_updated"],
        "",
        "━━ 예측 신뢰도 ━━",
    ]

    for cat, val in conf.items():
        bar_len = round(val / 1.5 * 5)
        bar     = "█" * bar_len + "░" * (5 - bar_len)
        status  = "↑" if val >= 1.1 else "↓" if val <= 0.7 else "→"
        lines.append(cat + " [" + bar + "] " + str(round(val, 2)) + " " + status)

    if summary["strong_categories"]:
        lines += ["", "💪 강한 분야: " + ", ".join(summary["strong_categories"])]
    if summary["weak_categories"]:
        lines += ["⚠️ 약한 분야: " + ", ".join(summary["weak_categories"])]

    if summary["recent_changes"]:
        lines.append("")
        lines.append("━━ 최근 가중치 변화 ━━")
        for log in summary["recent_changes"]:
            lines.append("<i>" + log["date"] + "</i>")
            for c in log.get("changes", []):
                lines.append("  • " + c)

    send_message("\n".join(lines))


if __name__ == "__main__":
    weights = load_weights()
    print("Weights loaded. Learning cycles: " + str(weights["total_learning_cycles"]))
    summary = get_learning_summary()
    print("Strong: " + str(summary["strong_categories"]))
    print("Weak: "   + str(summary["weak_categories"]))
