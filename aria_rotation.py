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
    flows = {s: 0 for s in SECTORS}

    for o in report.get("outflows", []):
        zone     = o.get("zone", "").lower()
        severity = o.get("severity", "보통")
        score    = -3 if severity == "높음" else -2 if severity == "보통" else -1
        for s in SECTORS:
            for k in s.lower().split("/"):
                if k in zone:
                    flows[s] += score

    for i in report.get("inflows", []):
        zone     = i.get("zone", "").lower()
        momentum = i.get("momentum", "약함")
        score    = 3 if momentum == "강함" else 2 if momentum == "형성중" else 1
        for s in SECTORS:
            for k in s.lower().split("/"):
                if k in zone:
                    flows[s] += score

    for s in flows:
        flows[s] = max(-3, min(3, flows[s]))

    return flows


def update_rotation(report):
    today   = datetime.now(KST).strftime("%Y-%m-%d")
    data    = load_json(ROTATION_FILE)
    history = data.get("history", [])

    today_flows = extract_sector_flows(report)

    history = [h for h in history if h.get("date") != today]
    history.append({"date": today, "flows": today_flows})
    history = history[-30:]

    # 누적 흐름
    cumulative = {s: 0 for s in SECTORS}
    for h in history:
        for s, v in h.get("flows", {}).items():
            if s in cumulative:
                cumulative[s] += v

    ranking = sorted(cumulative.items(), key=lambda x: x[1], reverse=True)
    rotation_signal = detect_rotation(history)

    result = {
        "last_updated":   today,
        "today_flows":    today_flows,
        "cumulative_30d": cumulative,
        "ranking":        ranking,
        "rotation_signal": rotation_signal,
        "history":        history,
    }

    save_rotation(result)
    return result


def detect_rotation(history):
    if len(history) < 2:
        return {"from": "", "to": "", "confidence": "낮음"}

    recent = history[-7:]
    prev   = history[-14:-7] if len(history) >= 14 else []

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

    changes = [(s, recent_sum[s] - prev_sum.get(s, 0)) for s in SECTORS]
    outflow = min(changes, key=lambda x: x[1])
    inflow  = max(changes, key=lambda x: x[1])

    magnitude  = inflow[1] - outflow[1]
    confidence = "높음" if magnitude > 5 else "보통" if magnitude > 2 else "낮음"

    return {"from": outflow[0], "to": inflow[0], "confidence": confidence}


def make_flow_bar(score):
    """점수를 블럭 바로 변환 -3 ~ +3"""
    abs_s = abs(score)
    bar   = "█" * (abs_s * 2)
    empty = "░" * ((3 - abs_s) * 2)
    return bar + empty


def send_rotation_report(data):
    try:
        from aria_telegram import send_message
    except ImportError:
        print("aria_telegram not found")
        return

    today_flows = data.get("today_flows", {})
    rotation    = data.get("rotation_signal", {})
    today       = data.get("last_updated", "")
    history_len = len(data.get("history", []))

    # 오늘 흐름 기준으로 분류
    strong_in  = [(s, v) for s, v in today_flows.items() if v >= 2]
    weak_in    = [(s, v) for s, v in today_flows.items() if v == 1]
    neutral    = [(s, v) for s, v in today_flows.items() if v == 0]
    weak_out   = [(s, v) for s, v in today_flows.items() if v == -1]
    strong_out = [(s, v) for s, v in today_flows.items() if v <= -2]

    # 정렬
    strong_in.sort(key=lambda x: x[1], reverse=True)
    strong_out.sort(key=lambda x: x[1])

    lines = [
        "<b>🔄 섹터 자금 흐름</b>",
        "<code>" + today + " (" + str(history_len) + "일 누적)</code>",
        "",
    ]

    # 유입
    if strong_in or weak_in:
        lines.append("━━ 자금 유입 ━━")
        for s, v in strong_in:
            bar = make_flow_bar(v)
            lines.append("🔥 " + s + "  <code>" + bar + "</code> 강한유입")
        for s, v in weak_in:
            bar = make_flow_bar(v)
            lines.append("📈 " + s + "  <code>" + bar + "</code> 유입")
        lines.append("")

    # 유출
    if strong_out or weak_out:
        lines.append("━━ 자금 유출 ━━")
        for s, v in strong_out:
            bar = make_flow_bar(abs(v))
            lines.append("📉 " + s + "  <code>" + bar + "</code> 강한유출")
        for s, v in weak_out:
            bar = make_flow_bar(abs(v))
            lines.append("📉 " + s + "  <code>" + bar + "</code> 소폭유출")
        lines.append("")

    # 중립
    if neutral:
        neutral_names = ", ".join([s for s, v in neutral])
        lines.append("━━ 중립 관망 ━━")
        lines.append("➡️ " + neutral_names)
        lines.append("")

    # 로테이션 감지
    if rotation.get("from") and rotation.get("to") and history_len >= 3:
        lines.append("━━ 로테이션 감지 ━━")
        lines.append(rotation["from"] + " → " + rotation["to"])
        lines.append("신뢰도: " + rotation["confidence"])
        lines.append("")

    # 오늘 핵심 요약
    if strong_in:
        top = strong_in[0][0]
        lines.append("💡 " + top + "로 자금 집중 중")
    if strong_out:
        bot = strong_out[0][0]
        lines.append("   " + bot + " 자금 이탈 중")

    # 데이터 부족 안내
    if history_len < 7:
        lines.append("")
        lines.append("<i>데이터 " + str(history_len) + "일째 누적 중</i>")
        lines.append("<i>(7일 이상부터 로테이션 패턴 정확도 향상)</i>")

    send_message("\n".join(lines))
    print("Rotation report sent")


def run_rotation(report):
    data = update_rotation(report)
    send_rotation_report(data)
    return data


if __name__ == "__main__":
    test = {
        "outflows": [
            {"zone": "에너지/원유 섹터", "severity": "높음"},
            {"zone": "방산 관련주", "severity": "보통"},
        ],
        "inflows": [
            {"zone": "반도체/AI 메모리", "momentum": "강함"},
            {"zone": "빅테크 나스닥", "momentum": "형성중"},
        ],
    }
    result = run_rotation(test)
    print("Top inflow: " + result["ranking"][0][0])
