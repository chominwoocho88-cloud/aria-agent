"""
aria_notify.py — ARIA 알림 모듈 통합
포함: telegram · weekly · monthly · breaking · calendar
"""
import os
import sys
import json
import re
import anthropic
import httpx
from datetime import datetime, timezone, timedelta
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

KST = timezone(timedelta(hours=9))

from aria_paths import (
    MEMORY_FILE, ACCURACY_FILE, SENTIMENT_FILE,
    ROTATION_FILE, BREAKING_FILE,
)

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
BASE_URL         = "https://api.telegram.org/bot" + TELEGRAM_TOKEN

API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL_S = "claude-haiku-4-5-20251001"   # 캘린더/속보용 가벼운 모델
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
# TELEGRAM
# ══════════════════════════════════════════════════════════════════════════════
def _send_single(text: str, reply_markup=None, parse_mode: str = "HTML") -> bool:
    """단일 메시지 전송 (4096자 이하 보장된 텍스트)"""
    try:
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        r = httpx.post(BASE_URL + "/sendMessage", json=payload, timeout=10)
        return r.json().get("ok", False)
    except Exception as e:
        print("Telegram send error: " + str(e))
        return False


def send_message(text: str, reply_markup=None, parse_mode: str = "HTML") -> bool:
    """4096자 초과 시 자동 분할 전송"""
    LIMIT = 4000
    if len(text) <= LIMIT:
        return _send_single(text, reply_markup, parse_mode)

    # 줄 단위로 분할
    lines  = text.split("\n")
    chunks = []
    current = []
    length  = 0

    for line in lines:
        if length + len(line) + 1 > LIMIT:
            chunks.append("\n".join(current))
            current = [line]
            length  = len(line)
        else:
            current.append(line)
            length += len(line) + 1

    if current:
        chunks.append("\n".join(current))

    ok = True
    for i, chunk in enumerate(chunks):
        suffix = ("\n<i>(" + str(i+1) + "/" + str(len(chunks)) + ")</i>") if len(chunks) > 1 else ""
        # 마지막 청크에만 버튼 첨부
        markup = reply_markup if i == len(chunks) - 1 else None
        ok = ok and _send_single(chunk + suffix, markup, parse_mode)

    return ok


def make_buttons() -> dict:
    return {
        "inline_keyboard": [[
            {"text": "🔄 지금 분석",  "callback_data": "run_now"},
            {"text": "📋 히스토리",   "callback_data": "history"},
            {"text": "🧠 성장리뷰",   "callback_data": "review"},
            {"text": "📚 학습현황",   "callback_data": "lessons"},
        ]]
    }


def send_error(message: str) -> bool:
    return send_message(
        "⚠️ <b>ARIA 오류</b>\n\n<code>" + message + "</code>\n\n"
        + "<i>" + _now().strftime("%Y-%m-%d %H:%M KST") + "</i>"
    )


def send_start_notification() -> bool:
    mode = os.environ.get("ARIA_MODE", "MORNING")
    labels = {"MORNING":"🌅 아침 풀분석","AFTERNOON":"☀️ 오후 업데이트",
              "EVENING":"🌆 저녁 마감","DAWN":"🌙 새벽 글로벌"}
    return send_message(
        "⚙️ <b>ARIA 분석 시작</b> " + labels.get(mode, mode) + "\n"
        + "<i>" + _now().strftime("%Y-%m-%d %H:%M KST") + "</i>\n"
        + "Hunter → Analyst → Devil → Reporter..."
    )


def send_report(report: dict, run_number: int) -> bool:
    mode       = report.get("mode", "MORNING")
    regime     = report.get("market_regime", "?")
    confidence = report.get("confidence_overall", "?")
    date       = report.get("analysis_date", "")
    time_      = report.get("analysis_time", "")
    summary    = report.get("one_line_summary", "")
    mode_label = report.get("mode_label", "")

    regime_emoji = "🟢" if "선호" in regime else "🔴" if "회피" in regime else "🟡"
    mode_icon    = {"MORNING":"🌅","AFTERNOON":"☀️","EVENING":"🌆","DAWN":"🌙"}.get(mode, "📊")

    header = [
        mode_icon + " <b>ARIA " + (mode_label or mode) + " #" + str(run_number) + "</b>",
        "<code>" + date + " " + time_ + "</code>", "",
        regime_emoji + " <b>" + regime + "</b>  신뢰도: " + confidence, "",
        "💡 <i>" + summary + "</i>", "",
    ]

    builders = {
        "MORNING":   _build_morning,
        "AFTERNOON": _build_afternoon,
        "EVENING":   _build_evening,
        "DAWN":      _build_dawn,
    }
    lines = header + builders.get(mode, _build_morning)(report)
    lines += [
        "",
        "📊 <a href=\"https://chominwoocho88-cloud.github.io/aria-agent/dashboard.html\">대시보드 보기</a>",
        "<code>ARIA Multi-Agent | Anthropic</code>",
    ]
    return send_message("\n".join(lines), reply_markup=make_buttons())


def _build_morning(report: dict) -> list:
    lines = []
    kr = report.get("korea_focus", {})
    if kr:
        lines += [
            "🇰🇷 <b>한국 시장</b>",
            "  원/달러: <code>" + kr.get("krw_usd", "-") + "</code>",
            "  코스피:  <code>" + kr.get("kospi_flow", "-") + "</code>",
            "  SK하이닉스: <code>" + kr.get("sk_hynix", "-") + "</code>",
            "  삼성전자:   <code>" + kr.get("samsung", "-") + "</code>",
            "  <i>" + kr.get("assessment", "") + "</i>", "",
        ]
    for o in report.get("outflows", [])[:3]:
        lines += ["▼ <b>" + o.get("zone","") + "</b> [" + o.get("severity","") + "]",
                  "  <i>" + o.get("reason","")[:70] + "</i>"]
    if report.get("outflows"): lines.append("")
    for i in report.get("inflows", [])[:3]:
        lines += ["▲ <b>" + i.get("zone","") + "</b> [" + i.get("momentum","") + "]",
                  "  <i>" + i.get("reason","")[:70] + "</i>"]
    if report.get("inflows"): lines.append("")
    for tk in report.get("thesis_killers", [])[:3]:
        lines += ["🎯 [" + tk.get("timeframe","") + "] <b>" + tk.get("event","") + "</b>",
                  "  ✓ " + tk.get("confirms_if","")[:50],
                  "  ✗ " + tk.get("invalidates_if","")[:50]]
    if report.get("thesis_killers"): lines.append("")
    for idx, a in enumerate(report.get("actionable_watch", [])[:3], 1):
        lines.append("📌 " + str(idx) + ". " + a)

    try:
        from aria_analysis import get_active_lessons
        lessons = get_active_lessons(max_lessons=3)
        if lessons:
            lines += ["", "━━ 🧠 오늘 반영된 교훈 ━━"]
            for l in lessons:
                lines.append(("🔴" if l["severity"]=="high" else "🟡")
                              + " [" + l["category"] + "] " + l["lesson"][:50])
    except ImportError:
        pass
    return lines


def _build_afternoon(report: dict) -> list:
    lines = ["━━ 오후 업데이트 ━━", ""]
    outflows = report.get("outflows", [])
    inflows  = report.get("inflows", [])
    if outflows: lines.append("▼ " + outflows[0].get("zone","") + " — " + outflows[0].get("reason","")[:50])
    if inflows:  lines.append("▲ " + inflows[0].get("zone","") + " — " + inflows[0].get("reason","")[:50])
    if report.get("actionable_watch"): lines.append("📌 " + report["actionable_watch"][0])
    kr = report.get("korea_focus", {})
    if kr.get("krw_usd"):
        lines += ["", "원/달러: <code>" + kr["krw_usd"] + "</code>  코스피: <code>" + kr.get("kospi_flow","-") + "</code>"]
    tks = report.get("thesis_killers", [])
    if tks:
        lines += ["", "🎯 <b>" + tks[0].get("event","") + "</b>",
                  "  ✓ " + tks[0].get("confirms_if","")[:50]]
    return lines


def _build_evening(report: dict) -> list:
    lines = ["━━ 오늘 총정리 ━━", ""]
    tomorrow = report.get("tomorrow_setup", "")
    if tomorrow: lines += ["🌙 <b>내일 준비</b>", "<i>" + tomorrow[:100] + "</i>", ""]
    counters = report.get("counterarguments", [])
    if counters:
        lines.append("⚔️ <b>주요 리스크</b>")
        for c in counters[:2]: lines.append("• " + c.get("against","")[:50])
        lines.append("")
    tails = report.get("tail_risks", [])
    if tails: lines.append("☠️ " + str(tails[0])[:60])
    return lines


def _build_dawn(report: dict) -> list:
    lines = ["━━ 새벽 글로벌 브리핑 ━━", ""]
    inflows  = report.get("inflows", [])
    outflows = report.get("outflows", [])
    if inflows:  lines.append("▲ " + inflows[0].get("zone","") + " — " + inflows[0].get("reason","")[:60])
    if outflows: lines.append("▼ " + outflows[0].get("zone","") + " — " + outflows[0].get("reason","")[:60])
    lines.append("")
    tomorrow = report.get("tomorrow_setup", "")
    if tomorrow: lines += ["📋 <b>오늘 아침 준비</b>", "<i>" + tomorrow[:100] + "</i>"]
    return lines


def send_lessons_status() -> bool:
    try:
        from aria_analysis import load_lessons
        data    = load_lessons()
        lessons = data.get("lessons", [])
        total   = data.get("total_lessons", 0)
        updated = data.get("last_updated", "없음")
        if not lessons:
            return send_message("📚 <b>학습 현황</b>\n\n아직 누적된 교훈이 없습니다.")
        lines = ["📚 <b>ARIA 학습 현황</b>",
                 "누적 교훈: <b>" + str(total) + "개</b>",
                 "마지막 업데이트: " + updated, "", "━━ 현재 적용 중인 교훈 ━━"]
        for l in sorted(lessons, key=lambda x: (3 if x["severity"]=="high" else 2 if x["severity"]=="medium" else 1), reverse=True)[:8]:
            em = "🔴" if l["severity"]=="high" else "🟡" if l["severity"]=="medium" else "🟢"
            st = "적용 " + str(l.get("applied",0)) + "회"
            if l.get("reinforced",0) > 0: st += " | 반복 " + str(l["reinforced"]) + "회"
            lines += [em + " <b>[" + l["category"] + "]</b> " + l["source"] + " " + l["date"],
                      "  " + l["lesson"][:70], "  <i>" + st + "</i>", ""]
        cats = {}
        for l in lessons: cats[l["category"]] = cats.get(l["category"], 0) + 1
        if cats:
            lines.append("━━ 카테고리별 교훈 수 ━━")
            for cat, cnt in sorted(cats.items(), key=lambda x: x[1], reverse=True):
                bar = "█" * min(cnt,5) + "░" * (5-min(cnt,5))
                lines.append("<code>" + cat.ljust(8) + " [" + bar + "] " + str(cnt) + "개</code>")
        lines += ["", "<i>교훈은 매일 새벽 자동 추출되어 아침 분석에 반영됩니다</i>"]
        return send_message("\n".join(lines))
    except Exception as e:
        return send_message("오류: " + str(e))


# ══════════════════════════════════════════════════════════════════════════════
# WEEKLY
# ══════════════════════════════════════════════════════════════════════════════
def send_weekly_report():
    now      = _now()
    week_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d")

    memory    = _load(MEMORY_FILE, [])
    accuracy  = _load(ACCURACY_FILE, {})
    sentiment = _load(SENTIMENT_FILE, {})

    wm  = [m for m in (memory if isinstance(memory,list) else []) if m.get("analysis_date","") >= week_ago]
    wa  = [h for h in accuracy.get("history",[]) if h.get("date","") >= week_ago]
    ws  = [h for h in sentiment.get("history",[]) if h.get("date","") >= week_ago]
    all_hist = accuracy.get("history",[])
    s_hist   = sentiment.get("history",[])

    total   = sum(h.get("total",0) for h in wa)
    correct = sum(h.get("correct",0) for h in wa)
    wacc    = round(correct / total * 100, 1) if total > 0 else 0
    pacc    = 0
    if len(all_hist) >= 14:
        pw = all_hist[-14:-7]
        pt, pc = sum(h.get("total",0) for h in pw), sum(h.get("correct",0) for h in pw)
        pacc   = round(pc / pt * 100, 1) if pt > 0 else 0

    sc = [h.get("score",50) for h in ws]
    sent_avg  = round(sum(sc)/len(sc),1) if sc else 50
    prev_sent = 50
    if len(s_hist) >= 14:
        ps = [h.get("score",50) for h in s_hist[-14:-7]]
        prev_sent = round(sum(ps)/len(ps),1) if ps else 50

    day_names = ["월","화","수","목","금","토","일"]
    bar_parts = []
    for h in ws[-7:]:
        try:
            d   = datetime.strptime(h.get("date",""), "%Y-%m-%d")
            day = day_names[d.weekday()]
        except Exception:
            day = h.get("date","")[-2:]
        bar_parts.append(day + " " + str(h.get("score",50)) + h.get("emoji","😐"))
    sent_bar = "  ".join(bar_parts) if bar_parts else "데이터 없음"

    regimes = [m.get("market_regime","") for m in wm]
    dom_reg = max(set(regimes), key=regimes.count) if regimes else "데이터 없음"

    risk_counts = {}
    for m in wm:
        lv = ("높음" if "회피" in m.get("market_regime","") or "하락" in m.get("trend_phase","")
              else "낮음" if "선호" in m.get("market_regime","") or "상승" in m.get("trend_phase","")
              else "보통")
        risk_counts[lv] = risk_counts.get(lv, 0) + 1

    acc_chg  = round(wacc - pacc, 1)
    sent_chg = round(sent_avg - prev_sent, 1)
    lines = [
        "<b>📊 ARIA 주간 성장 리포트</b>",
        "<code>" + now.strftime("%Y-%m-%d") + " (주간)</code>", "",
        "━━ 이번 주 예측 성과 ━━",
        ("📈" if acc_chg > 0 else "📉" if acc_chg < 0 else "➡️")
        + " 정확도: <b>" + str(wacc) + "%</b>",
        "   지난주 " + str(pacc) + "% → " + ("+" if acc_chg >= 0 else "") + str(acc_chg) + "%p",
        "   적중: " + str(correct) + "/" + str(total) + "개", "",
    ]
    strong = accuracy.get("strong_areas",[])
    weak   = accuracy.get("weak_areas",[])
    if strong:
        lines.append("━━ 잘 맞추는 분야 ━━")
        for s in strong[:3]: lines.append("✅ " + s)
        lines.append("")
    if weak:
        lines.append("━━ 아직 약한 분야 ━━")
        for w in weak[:3]: lines.append("❌ " + w)
        lines.append("")
    lines += [
        "━━ 감정지수 추이 ━━",
        "<code>" + sent_bar + "</code>",
        "평균: " + str(sent_avg) + " (지난주 대비 " + ("+" if sent_chg >= 0 else "") + str(sent_chg) + ")", "",
        "━━ 포트폴리오 위험 흐름 ━━",
        "지배 레짐: " + dom_reg,
    ]
    for lv, cnt in risk_counts.items():
        em = "🔴" if lv == "높음" else "🟢" if lv == "낮음" else "🟡"
        lines.append(em + " " + lv + ": " + str(cnt) + "일")
    lines += ["", "<code>ARIA — 누적 " + str(len(memory if isinstance(memory,list) else [])) + "일째 성장 중</code>"]
    send_message("\n".join(lines))
    print("Weekly report sent")


# ══════════════════════════════════════════════════════════════════════════════
# MONTHLY
# ══════════════════════════════════════════════════════════════════════════════
def send_monthly_report():
    now         = _now()
    last_month  = (now.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")

    memory    = _load(MEMORY_FILE, [])
    accuracy  = _load(ACCURACY_FILE, {})
    sentiment = _load(SENTIMENT_FILE, {})
    rotation  = _load(ROTATION_FILE, {})

    mm = [m for m in (memory if isinstance(memory,list) else []) if m.get("analysis_date","").startswith(last_month)]
    ma = [h for h in accuracy.get("history",[]) if h.get("date","").startswith(last_month)]
    ms = [h for h in sentiment.get("history",[]) if h.get("date","").startswith(last_month)]

    total   = sum(h.get("total",0) for h in ma)
    correct = sum(h.get("correct",0) for h in ma)
    acc     = round(correct / total * 100, 1) if total > 0 else 0

    regimes = [m.get("market_regime","") for m in mm]
    reg_cnt = {}
    for r in regimes: reg_cnt[r] = reg_cnt.get(r, 0) + 1
    trends  = [m.get("trend_phase","") for m in mm]
    trn_cnt = {}
    for t in trends: trn_cnt[t] = trn_cnt.get(t, 0) + 1

    sc       = [h.get("score",50) for h in ms]
    sent_avg = round(sum(sc)/len(sc),1) if sc else 50
    sent_min = min(sc) if sc else 50
    sent_max = max(sc) if sc else 50
    min_day  = ms[sc.index(sent_min)].get("date","") if sc else ""
    max_day  = ms[sc.index(sent_max)].get("date","") if sc else ""

    ranking = rotation.get("ranking",[])
    t_all   = _load(ACCURACY_FILE,{}).get("total",0)
    c_all   = _load(ACCURACY_FILE,{}).get("correct",0)

    lines = [
        "<b>📊 ARIA " + last_month + " 월간 리포트</b>", "",
        "━━ 이달의 분석 성과 ━━",
        "분석 일수: <b>" + str(len(mm)) + "일</b>",
        ("📈" if acc >= 65 else "📉" if acc < 50 else "➡️") + " 예측 정확도: <b>" + str(acc) + "%</b>",
        "   (" + str(correct) + "/" + str(total) + "개 적중)",
        "누적 정확도: " + str(round(c_all/t_all*100,1) if t_all else 0) + "%", "",
        "━━ 이달의 시장 특성 ━━",
        "지배 레짐: <b>" + (max(reg_cnt, key=reg_cnt.get) if reg_cnt else "") + "</b>",
        "분포: " + " | ".join(k + " " + str(v) + "일" for k, v in reg_cnt.items()),
        "추세: " + " | ".join(k + " " + str(v) + "일" for k, v in trn_cnt.items()), "",
        "━━ 감정지수 ━━",
        "평균: <b>" + str(sent_avg) + "</b>",
        "최저: " + str(sent_min) + " (" + min_day + ")",
        "최고: " + str(sent_max) + " (" + max_day + ")", "",
        "━━ 섹터 로테이션 ━━",
    ]
    if ranking:
        lines.append("🔥 강세: " + " > ".join(r[0] for r in ranking[:3]))
        lines.append("❄️ 약세: " + " > ".join(r[0] for r in ranking[-3:]))
    strong = _load(ACCURACY_FILE,{}).get("strong_areas",[])
    weak   = _load(ACCURACY_FILE,{}).get("weak_areas",[])
    lines += ["", "━━ ARIA 성장 현황 ━━"]
    if strong: lines.append("💪 강점: " + ", ".join(strong[:3]))
    if weak:   lines.append("📚 개선중: " + ", ".join(weak[:3]))
    lines += ["", "<code>ARIA 누적 분석 " + str(len(memory if isinstance(memory,list) else [])) + "일 | 계속 성장 중</code>"]
    send_message("\n".join(lines))
    print("Monthly report sent for " + last_month)


# ══════════════════════════════════════════════════════════════════════════════
# BREAKING NEWS
# ══════════════════════════════════════════════════════════════════════════════
_BREAKING_SYS = """You are a financial breaking news detector.
Search for urgent financial news from the last 2 hours.
Return ONLY valid JSON. No markdown.
{"has_breaking":true,"breaking_news":[{"headline":"","severity":"critical/high/medium","impact":"","affected_assets":[],"source_hint":""}],"summary":""}"""


def check_breaking_news():
    now_str   = _now().strftime("%Y-%m-%d %H:%M")
    today_str = _today()
    sent      = _load(BREAKING_FILE, {"sent_today":[], "last_check":""})
    today_sent = [s for s in sent.get("sent_today",[]) if s.startswith(today_str)]
    if len(today_sent) >= 5:
        print("Daily breaking news limit reached (5)"); return

    print("Checking breaking news at " + now_str)
    full = ""
    with client.messages.stream(
        model=MODEL_S, max_tokens=1000, system=_BREAKING_SYS,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        messages=[{"role": "user",
                   "content": "Search major financial breaking news in last 2 hours: " + now_str + ". Return JSON."}]
    ) as s:
        for ev in s:
            if getattr(ev, "type", "") == "content_block_delta":
                d = getattr(ev, "delta", None)
                if d and getattr(d, "type", "") == "text_delta":
                    full += d.text

    if not full.strip(): return
    m = re.search(r"\{[\s\S]*\}", re.sub(r"```json|```","",full).strip())
    if not m: return
    try:
        data = json.loads(m.group())
    except Exception: return
    if not data.get("has_breaking"): print("No breaking news"); return

    new_alerts = []
    for news in data.get("breaking_news", []):
        headline = news.get("headline","")
        if news.get("severity") not in ["critical","high"]: continue
        if any(headline[:30] in s for s in sent.get("sent_today",[])):  continue
        new_alerts.append(news)

    if not new_alerts: print("No new high/critical news"); return

    for news in new_alerts:
        em  = "🚨" if news.get("severity") == "critical" else "⚠️"
        aff = ", ".join(news.get("affected_assets",[]))
        lines = [em + " <b>ARIA 긴급 속보</b>", "<code>" + now_str + "</code>", "",
                 "<b>" + news.get("headline","") + "</b>", "",
                 "영향: " + news.get("impact","")]
        if aff: lines.append("관련 자산: " + aff)
        send_message("\n".join(lines))
        print("Breaking alert: " + news.get("headline","")[:50])
        sent.setdefault("sent_today",[]).append(today_str + " " + news.get("headline","")[:30])

    sent["last_check"] = now_str
    _save(BREAKING_FILE, sent)


# ══════════════════════════════════════════════════════════════════════════════
# CALENDAR
# ══════════════════════════════════════════════════════════════════════════════
_CALENDAR_SYS = """You are a financial calendar agent.
Search for this week's major economic events and data releases.
Focus on: US FOMC/CPI/NFP, Korea BOK, major semiconductor earnings, geopolitical events.
Return ONLY valid JSON. No markdown.
{"week_start":"YYYY-MM-DD","week_end":"YYYY-MM-DD","events":[{"date":"","day":"월/화/수/목/금","time":"","event":"","importance":"high/medium/low","expected":"","previous":"","market_impact":"","affected_assets":[]}],"week_summary":"","key_watch":""}"""


def send_calendar_report():
    week_str = _now().strftime("%Y-%m-%d")
    print("Fetching economic calendar for week of " + week_str)

    full = ""
    with client.messages.stream(
        model=MODEL_S, max_tokens=2000, system=_CALENDAR_SYS,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        messages=[{"role": "user",
                   "content": "Search economic calendar events for week of " + week_str + ". Include US and Korea. Return JSON."}]
    ) as s:
        for ev in s:
            t = getattr(ev, "type", "")
            if t == "content_block_start":
                blk = getattr(ev, "content_block", None)
                if blk and getattr(blk, "type","") == "tool_use":
                    print("  Search: " + getattr(blk, "input", {}).get("query",""))
            elif t == "content_block_delta":
                d = getattr(ev, "delta", None)
                if d and getattr(d, "type","") == "text_delta":
                    full += d.text

    raw = re.sub(r"```json|```","",full).strip()
    m   = re.search(r"\{[\s\S]*\}", raw)
    if not m:
        print("No calendar JSON"); return {}
    try:
        cal = re.sub(r",\s*([}\]])", r"\1", m.group())
        cal += "]" * (cal.count("[") - cal.count("]"))
        cal += "}" * (cal.count("{") - cal.count("}"))
        calendar = json.loads(cal)
    except Exception as e:
        print("Calendar parse error: " + str(e)); return {}

    events      = calendar.get("events", [])
    high_events = [e for e in events if e.get("importance") == "high"]
    other       = [e for e in events if e.get("importance") != "high"]

    lines = [
        "<b>📅 이번 주 경제 캘린더</b>",
        "<code>" + calendar.get("week_start","") + " ~ " + calendar.get("week_end","") + "</code>", "",
        "<b>" + calendar.get("week_summary","") + "</b>", "",
        "━━ 핵심 이벤트 ━━",
    ]
    for e in high_events[:6]:
        lines += ["🔴 <b>[" + e.get("day","") + "] " + e.get("event","") + "</b>",
                  "   " + e.get("time","") + " | " + e.get("market_impact","")[:50]]
        if e.get("expected"): lines.append("   예상: " + e["expected"])
    if other:
        lines += ["", "━━ 기타 일정 ━━"]
        for e in other[:4]: lines.append("⚪ [" + e.get("day","") + "] " + e.get("event",""))
    if calendar.get("key_watch"):
        lines += ["", "👀 <b>이번 주 핵심 관전 포인트</b>", "<i>" + calendar["key_watch"] + "</i>"]

    send_message("\n".join(lines))
    print("Calendar report sent")
    return calendar


# ══════════════════════════════════════════════════════════════════════════════
# 직접 실행
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "test"
    if cmd == "weekly":    send_weekly_report()
    elif cmd == "monthly": send_monthly_report()
    elif cmd == "breaking": check_breaking_news()
    elif cmd == "calendar": send_calendar_report()
    else:
        ok = send_message("✅ ARIA 텔레그램 연결 성공!", reply_markup=make_buttons())
        print("OK" if ok else "FAIL")
