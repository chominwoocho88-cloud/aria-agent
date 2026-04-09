import os
import sys
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

KST           = timezone(timedelta(hours=9))
ROTATION_FILE = Path("rotation.json")
MEMORY_FILE   = Path("memory.json")

SECTORS = [
    "반도체/AI",
    "빅테크",
    "에너지/원유",
    "방산",
    "헬스케어",
    "금융",
    "소비재",
    "배당/가치주",
    "원자재",
    "부동산(리츠)",
]


def load_json(path):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def save_rotation(data):
    ROTATION_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def extract_sector_flows(report):
    """ARIA 리포트에서 섹터별 자금 흐름 추출"""
    flows = {}
    for s in SECTORS:
        flows[s] = 0  # -3 ~ +3

    outflows = report.get("outflows", [])
    inflows  = report.get("inflows", [])

    for o in outflows:
        zone = o.get("zone", "").lower()
        severity = o.get("severity", "보통")
        score = -3 if severity == "높음" else -2 if severity == "보통" else -1

        for s in SECTORS:
            keywords = s.lower().split("/")
            if any(k in zone for k in keywords):
                flows[s] += score

    for i in inflows:
        zone = i.get("zone", "").lower()
        momentum = i.get("momentum", "약함")
        score = 3 if momentum == "강함" else 2 if momentum == "형성중" else 1

        for s in SECTORS:
            keywords = s.lower().split("/")
            if any(k in zone for k in keywords):
                flows[s] += score

    # -3 ~ +3 범위로 클리핑
    for s in flows:
        flows[s] = max(-3, min(3, flows[s]))

    return flows


def update_rotation(report):
    today = datetime.now(KST).strftime("%Y-%m-%d")
    data  = load_json(ROTATION_FILE)
    history = data.get("history", [])

    # 오늘 섹터 흐름 추출
    today_flows = extract_sector_flows(report)

    # 히스토리 추가
    history = [h for h in history if h.get("date") != today]
    history.append({"date": today, "flows": today_flows})
    history = history[-30:]  # 30일 보존

    # 30일 누적 흐름 계산
    cumulative = {s: 0 for s in SECTORS}
    for h in history:
        for s, v in h.get("flows", {}).items():
            if s in cumulative:
                cumulative[s] += v

    # 섹터 랭킹
    ranking = sorted(cumulative.items(), key=lambda x: x[1], reverse=True)

    # 로테이션 방향 감지 (최근 7일 vs 이전 7일)
    rotation_signal = detect_rotation(history)

    result = {
        "last_updated": today,
        "today_flows": today_flows,
        "cumulative_30d": cumulative,
        "ranking": ranking,
        "rotation_signal": rotation_signal,
        "history": history,
    }

    save_rotation(result)
    return result


def detect_rotation(history):
    """최근 7일 vs 이전 7일 비교로 로테이션 방향 감지"""
    if len(history) < 8:
        return {"from": "", "to": "", "confidence": "낮음"}

    recent  = history[-7:]
    prev    = history[-14:-7] if len(history) >= 14 else []

    recent_sum = {s: 0 for s in SECTORS}
    prev_sum   = {s: 0 for s in SECTORS}

    for h in recent:
        for s, v in h.get("flows", {}).items():
            if s in recent_sum:
                recent_sum[s] += v

    for h in prev:
        for s, v in h.get("flows", {}).items():
            if s in prev_sum:
                prev_sum[s] += v

    # 가장 많이 줄어든 섹터 (돈이 빠지는 곳)
    outflow_sector = min(
        [(s, recent_sum[s] - prev_sum.get(s, 0)) for s in SECTORS],
        key=lambda x: x[1]
    )

    # 가장 많이 늘어난 섹터 (돈이 들어오는 곳)
    inflow_sector = max(
        [(s, recent_sum[s] - prev_sum.get(s, 0)) for s in SECTORS],
        key=lambda x: x[1]
    )

    change_magnitude = inflow_sector[1] - outflow_sector[1]
    confidence = "높음" if change_magnitude > 5 else "보통" if change_magnitude > 2 else "낮음"

    return {
        "from": outflow_sector[0],
        "to": inflow_sector[0],
        "confidence": confidence,
    }


def make_flow_bar(score):
    """점수를 시각적 바로 변환 -3~+3"""
    if score >= 2:   return "++++"
    elif score == 1: return "++"
    elif score == 0: return "  "
    elif score == -1: return "--"
    else:             return "----"


def send_rotation_report(data):
    try:
        from aria_telegram import send_message
    except ImportError:
        print("aria_telegram not found")
        return

    ranking  = data.get("ranking", [])
    rotation = data.get("rotation_signal", {})
    today    = data.get("last_updated", "")

    lines = [
        "<b>🔄 섹터 로테이션 추적</b>",
        "<code>" + today + " (30일 누적)</code>",
        "",
        "━━ 섹터 랭킹 ━━",
    ]

    for i, (sector, score) in enumerate(ranking[:8], 1):
        bar   = make_flow_bar(score)
        emoji = "🔥" if score >= 3 else "📈" if score > 0 else "📉" if score < 0 else "➡️"
        lines.append(
            str(i) + ". " + emoji + " <b>" + sector + "</b> <code>[" + bar + "]</code>"
        )

    if rotation.get("from") and rotation.get("to"):
        lines += [
            "",
            "━━ 로테이션 감지 ━━",
            "신뢰도: " + rotation.get("confidence", ""),
            rotation.get("from", "") + " → " + rotation.get("to", ""),
        ]

    send_message("\n".join(lines))
    print("Rotation report sent")


def run_rotation(report):
    data = update_rotation(report)
    send_rotation_report(data)
    return data


if __name__ == "__main__":
    test_report = {
        "outflows": [{"zone": "반도체/AI 섹터", "severity": "높음"}],
        "inflows":  [{"zone": "방산/에너지", "momentum": "강함"}],
    }
    result = run_rotation(test_report)
    print("Top sector: " + result["ranking"][0][0])
