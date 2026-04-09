import os
os.environ[“PYTHONIOENCODING”] = “utf-8”
import sys
sys.stdout.reconfigure(encoding=“utf-8”)
sys.stderr.reconfigure(encoding=“utf-8”)

import anthropic
import json
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

KST          = timezone(timedelta(hours=9))
MEMORY_FILE  = Path(“memory.json”)
ACCURACY_FILE = Path(“accuracy.json”)
API_KEY      = os.environ.get(“ANTHROPIC_API_KEY”, “”)
MODEL        = “claude-sonnet-4-6”

client = anthropic.Anthropic(api_key=API_KEY)

# ── 유틸 ──────────────────────────────────────────────────────────────────────

def now_kst():
return datetime.now(KST)

def parse_json(text: str) -> dict:
raw = re.sub(r”`json|`”, “”, text).strip()
m = re.search(r”{[\s\S]*}”, raw)
if not m:
raise ValueError(“JSON not found”)
s = m.group()
s = re.sub(r”,\s*([}]])”, r”\1”, s)
s += “]” * (s.count(”[”) - s.count(”]”))
s += “}” * (s.count(”{”) - s.count(”}”))
return json.loads(s)

def load_memory() -> list:
if MEMORY_FILE.exists():
return json.loads(MEMORY_FILE.read_text(encoding=“utf-8”))
return []

def load_accuracy() -> dict:
if ACCURACY_FILE.exists():
return json.loads(ACCURACY_FILE.read_text(encoding=“utf-8”))
return {
“total”: 0,
“correct”: 0,
“by_category”: {},
“history”: [],
“weak_areas”: [],
“strong_areas”: [],
}

def save_accuracy(data: dict):
ACCURACY_FILE.write_text(
json.dumps(data, ensure_ascii=False, indent=2), encoding=“utf-8”
)

# ── 채점 에이전트 ──────────────────────────────────────────────────────────────

VERIFIER_SYSTEM = “”“You are ARIA-Verifier. Your job is to check if yesterday’s predictions came true.

You will receive:

1. Yesterday’s thesis_killers (predictions)
1. Today’s news search results

For each thesis_killer, determine:

- “confirmed”: the confirms_if scenario happened
- “invalidated”: the invalidates_if scenario happened
- “unclear”: not enough information to judge

Be strict. Only mark “confirmed” or “invalidated” if there is clear evidence.

Return ONLY valid JSON. No markdown.
{
“verification_date”: “YYYY-MM-DD”,
“results”: [
{
“event”: “”,
“predicted_confirms”: “”,
“predicted_invalidates”: “”,
“actual_outcome”: “”,
“verdict”: “confirmed/invalidated/unclear”,
“evidence”: “”,
“category”: “금리/환율/주식/지정학/원자재/기업”
}
],
“summary”: {
“total”: 0,
“confirmed”: 0,
“invalidated”: 0,
“unclear”: 0,
“accuracy_today”: 0.0
},
“pattern_insight”: “오늘 채점에서 발견한 패턴이나 ARIA가 개선해야 할 점”
}”””

def verify_predictions(yesterday_analysis: dict) -> dict:
thesis_killers = yesterday_analysis.get(“thesis_killers”, [])
if not thesis_killers:
print(“No thesis killers to verify”)
return {}

```
print(f"\nVerifying {len(thesis_killers)} predictions from {yesterday_analysis.get('analysis_date')}...")

# 각 테제 킬러에 대해 뉴스 검색
search_results = []
search_count = 0
for tk in thesis_killers:
    event = tk.get("event", "")
    query = f"{event} 결과 오늘"

    with client.messages.stream(
        model=MODEL,
        max_tokens=500,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        messages=[{"role": "user", "content": f"Search for: {query}. Return a brief summary of what happened."}]
    ) as s:
        text = ""
        for ev in s:
            t = getattr(ev, "type", "")
            if t == "content_block_start":
                blk = getattr(ev, "content_block", None)
                if blk and getattr(blk, "type", "") == "tool_use":
                    search_count += 1
                    print(f"  Search [{search_count}]: {query}")
            elif t == "content_block_delta":
                d = getattr(ev, "delta", None)
                if d and getattr(d, "type", "") == "text_delta":
                    text += d.text
        search_results.append({"event": event, "news": text})

# 채점 요청
payload = {
    "thesis_killers": thesis_killers,
    "news_results": search_results,
    "analysis_date": yesterday_analysis.get("analysis_date"),
}

with client.messages.stream(
    model=MODEL,
    max_tokens=2000,
    system=VERIFIER_SYSTEM,
    messages=[{"role": "user", "content": f"Verify these predictions:\n{json.dumps(payload, ensure_ascii=False)}\n\nReturn JSON only."}]
) as s:
    full = ""
    for ev in s:
        t = getattr(ev, "type", "")
        if t == "content_block_delta":
            d = getattr(ev, "delta", None)
            if d and getattr(d, "type", "") == "text_delta":
                full += d.text

return parse_json(full)
```

# ── 정확도 업데이트 ────────────────────────────────────────────────────────────

def update_accuracy(verification: dict, accuracy: dict) -> dict:
summary = verification.get(“summary”, {})
results = verification.get(“results”, [])

```
# 전체 통계 업데이트
total_judged = summary.get("confirmed", 0) + summary.get("invalidated", 0)
correct = summary.get("confirmed", 0)

accuracy["total"] += total_judged
accuracy["correct"] += correct

# 카테고리별 통계
for r in results:
    if r.get("verdict") == "unclear":
        continue
    cat = r.get("category", "기타")
    if cat not in accuracy["by_category"]:
        accuracy["by_category"][cat] = {"total": 0, "correct": 0}
    accuracy["by_category"][cat]["total"] += 1
    if r.get("verdict") == "confirmed":
        accuracy["by_category"][cat]["correct"] += 1

# 히스토리 추가
accuracy["history"].append({
    "date": verification.get("verification_date"),
    "total": total_judged,
    "correct": correct,
    "accuracy": round(correct / total_judged * 100, 1) if total_judged > 0 else 0,
    "pattern_insight": verification.get("pattern_insight", ""),
})
accuracy["history"] = accuracy["history"][-90:]  # 90일 보존

# 강점/약점 분석
strong, weak = [], []
for cat, stats in accuracy["by_category"].items():
    if stats["total"] >= 3:
        acc = stats["correct"] / stats["total"] * 100
        if acc >= 70:
            strong.append(f"{cat} ({acc:.0f}%)")
        elif acc <= 40:
            weak.append(f"{cat} ({acc:.0f}%)")
accuracy["strong_areas"] = strong
accuracy["weak_areas"] = weak

return accuracy
```

# ── 텔레그램 채점 리포트 ──────────────────────────────────────────────────────

def send_verification_report(verification: dict, accuracy: dict):
try:
from aria_telegram import send_message
except ImportError:
print(“aria_telegram not found, skipping telegram”)
return

```
summary = verification.get("summary", {})
results = verification.get("results", [])
total_acc = round(accuracy["correct"] / accuracy["total"] * 100, 1) if accuracy["total"] > 0 else 0
today_acc = summary.get("accuracy_today", 0)

lines = [
    f"<b>📋 ARIA 어제 예측 채점 결과</b>",
    f"<code>{verification.get('verification_date','')}</code>",
    "",
]

for r in results:
    verdict = r.get("verdict", "")
    emoji = "✅" if verdict == "confirmed" else "❌" if verdict == "invalidated" else "❓"
    lines.append(f"{emoji} <b>{r.get('event','')}</b>")
    lines.append(f"  <i>{r.get('actual_outcome','')[:60]}</i>")

lines += [
    "",
    f"오늘 적중률: <b>{today_acc:.0f}%</b> ({summary.get('confirmed',0)}/{summary.get('confirmed',0)+summary.get('invalidated',0)})",
    f"누적 정확도: <b>{total_acc:.0f}%</b> ({accuracy['correct']}/{accuracy['total']})",
    "",
]

if accuracy.get("strong_areas"):
    lines.append(f"💪 강점: {', '.join(accuracy['strong_areas'][:3])}")
if accuracy.get("weak_areas"):
    lines.append(f"⚠️ 약점: {', '.join(accuracy['weak_areas'][:3])}")

if verification.get("pattern_insight"):
    lines += ["", f"🧠 <i>{verification.get('pattern_insight','')}</i>"]

send_message("\n".join(lines))
print("Verification report sent to Telegram")
```

# ── 메인 ──────────────────────────────────────────────────────────────────────

def run_verification():
memory = load_memory()
accuracy = load_accuracy()

```
if len(memory) < 1:
    print("No previous analysis to verify")
    return accuracy

# 어제 분석 가져오기
yesterday = memory[-1]
yesterday_date = yesterday.get("analysis_date", "")
today = now_kst().strftime("%Y-%m-%d")

# 이미 오늘 채점했으면 스킵
if accuracy.get("history") and accuracy["history"][-1].get("date") == today:
    print(f"Already verified today ({today}), skipping")
    return accuracy

print(f"Verifying predictions from {yesterday_date}...")

# 채점 실행
verification = verify_predictions(yesterday)
if not verification:
    return accuracy

# 정확도 업데이트
accuracy = update_accuracy(verification, accuracy)
save_accuracy(accuracy)

# 텔레그램 전송
send_verification_report(verification, accuracy)

print(f"Verification complete. Today accuracy: {verification.get('summary', {}).get('accuracy_today', 0):.0f}%")
return accuracy
```

if **name** == “**main**”:
run_verification()