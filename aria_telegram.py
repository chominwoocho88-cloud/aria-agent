import httpx
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

KST = timezone(timedelta(hours=9))

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
BASE_URL         = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"


def send_message(text, reply_markup=None, parse_mode="HTML"):
    try:
        payload = {
            "chat_id":               TELEGRAM_CHAT_ID,
            "text":                  text,
            "parse_mode":            parse_mode,
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        r = httpx.post(f"{BASE_URL}/sendMessage", json=payload, timeout=10)
        return r.json().get("ok", False)
    except Exception as e:
        print("Telegram send error: " + str(e))
        return False


def make_buttons():
    return {
        "inline_keyboard": [[
            {"text": "🔄 지금 분석",  "callback_data": "run_now"},
            {"text": "📋 히스토리",   "callback_data": "history"},
            {"text": "🧠 성장리뷰",   "callback_data": "review"},
            {"text": "📚 학습현황",   "callback_data": "lessons"},
        ]]
    }


def send_report(report, run_number):
    mode       = report.get("mode", "MORNING")
    regime     = report.get("market_regime", "?")
    confidence = report.get("confidence_overall", "?")
    date       = report.get("analysis_date", "")
    time_      = report.get("analysis_time", "")
    summary    = report.get("one_line_summary", "")
    mode_label = report.get("mode_label", "")

    regime_emoji = "🟢" if "선호" in regime else "🔴" if "회피" in regime else "🟡"
    mode_icons   = {"MORNING": "🌅", "AFTERNOON": "☀️", "EVENING": "🌆", "DAWN": "🌙"}
    mode_icon    = mode_icons.get(mode, "📊")

    # ── 공통 헤더 ──
    header = [
        mode_icon + " <b>ARIA " + (mode_label or mode) + " #" + str(run_number) + "</b>",
        "<code>" + date + " " + time_ + "</code>",
        "",
        regime_emoji + " <b>" + regime + "</b>  신뢰도: " + confidence,
        "",
        "💡 <i>" + summary + "</i>",
        "",
    ]

    # ── 모드별 분기 ────────────────────────────────────────────────────────────

    # MORNING: 풀 리포트
    if mode == "MORNING":
        lines = header + _build_full_report(report)

    # AFTERNOON: 핵심 3줄 카드
    elif mode == "AFTERNOON":
        lines = header + _build_afternoon_card(report)

    # EVENING: 오늘 총정리
    elif mode == "EVENING":
        lines = header + _build_evening_summary(report)

    # DAWN: 글로벌 브리핑
    else:
        lines = header + _build_dawn_brief(report)

    lines += ["", "<code>ARIA Multi-Agent | Anthropic</code>"]
    return send_message("\n".join(lines), reply_markup=make_buttons())


def _build_full_report(report):
    """MORNING 풀 리포트"""
    lines = []

    kr = report.get("korea_focus", {})
    if kr:
        lines += [
            "🇰🇷 <b>한국 시장</b>",
            "  원/달러: <code>" + kr.get("krw_usd", "-") + "</code>",
            "  코스피:  <code>" + kr.get("kospi_flow", "-") + "</code>",
            "  SK하이닉스: <code>" + kr.get("sk_hynix", "-") + "</code>",
            "  삼성전자:   <code>" + kr.get("samsung", "-") + "</code>",
            "  <i>" + kr.get("assessment", "") + "</i>",
            "",
        ]

    for o in report.get("outflows", [])[:3]:
        lines.append("▼ <b>" + o.get("zone", "") + "</b> [" + o.get("severity", "") + "]")
        lines.append("  <i>" + o.get("reason", "")[:70] + "</i>")
    if report.get("outflows"):
        lines.append("")

    for i in report.get("inflows", [])[:3]:
        lines.append("▲ <b>" + i.get("zone", "") + "</b> [" + i.get("momentum", "") + "]")
        lines.append("  <i>" + i.get("reason", "")[:70] + "</i>")
    if report.get("inflows"):
        lines.append("")

    for tk in report.get("thesis_killers", [])[:3]:
        lines.append("🎯 [" + tk.get("timeframe", "") + "] <b>" + tk.get("event", "") + "</b>")
        lines.append("  ✓ " + tk.get("confirms_if", "")[:50])
        lines.append("  ✗ " + tk.get("invalidates_if", "")[:50])
    if report.get("thesis_killers"):
        lines.append("")

    for idx, a in enumerate(report.get("actionable_watch", [])[:3], 1):
        lines.append("📌 " + str(idx) + ". " + a)

    # 교훈 반영 표시
    try:
        from aria_lessons import get_active_lessons
        lessons = get_active_lessons(max_lessons=3)
        if lessons:
            lines += ["", "━━ 🧠 오늘 반영된 교훈 ━━"]
            for l in lessons:
                sev = "🔴" if l["severity"] == "high" else "🟡"
                lines.append(sev + " [" + l["category"] + "] " + l["lesson"][:50])
    except ImportError:
        pass

    return lines


def _build_afternoon_card(report):
    """AFTERNOON 핵심 3줄 카드 (C1+C2)"""
    lines = ["━━ 오후 업데이트 ━━", ""]

    # 아침 대비 변화 포인트
    actionable = report.get("actionable_watch", [])
    outflows   = report.get("outflows", [])
    inflows    = report.get("inflows", [])

    if outflows:
        lines.append("▼ " + outflows[0].get("zone", "") + " — " + outflows[0].get("reason", "")[:50])
    if inflows:
        lines.append("▲ " + inflows[0].get("zone", "") + " — " + inflows[0].get("reason", "")[:50])
    if actionable:
        lines.append("📌 " + actionable[0])

    kr = report.get("korea_focus", {})
    if kr.get("krw_usd"):
        lines += ["", "원/달러: <code>" + kr["krw_usd"] + "</code>  코스피: <code>" + kr.get("kospi_flow", "-") + "</code>"]

    # 테제 킬러 1개
    tks = report.get("thesis_killers", [])
    if tks:
        lines += ["", "🎯 <b>" + tks[0].get("event", "") + "</b>"]
        lines.append("  ✓ " + tks[0].get("confirms_if", "")[:50])

    return lines


def _build_evening_summary(report):
    """EVENING 오늘 총정리 (C1+C4)"""
    lines = ["━━ 오늘 총정리 ━━", ""]

    # 내일 준비
    tomorrow = report.get("tomorrow_setup", "")
    if tomorrow:
        lines += ["🌙 <b>내일 준비</b>", "<i>" + tomorrow[:100] + "</i>", ""]

    # 핵심 반론
    counters = report.get("counterarguments", [])
    if counters:
        lines.append("⚔️ <b>주요 리스크</b>")
        for c in counters[:2]:
            lines.append("• " + c.get("against", "")[:50])
        lines.append("")

    # 꼬리 리스크
    tails = report.get("tail_risks", [])
    if tails:
        lines.append("☠️ " + tails[0][:60])

    return lines


def _build_dawn_brief(report):
    """DAWN 글로벌 브리핑 (C1)"""
    lines = ["━━ 새벽 글로벌 브리핑 ━━", ""]

    # 미국 마감 요약
    outflows = report.get("outflows", [])
    inflows  = report.get("inflows", [])

    if inflows:
        lines.append("▲ " + inflows[0].get("zone", "") + " — " + inflows[0].get("reason", "")[:60])
    if outflows:
        lines.append("▼ " + outflows[0].get("zone", "") + " — " + outflows[0].get("reason", "")[:60])

    lines.append("")

    # 내일 오전 준비
    tomorrow = report.get("tomorrow_setup", "")
    if tomorrow:
        lines += ["📋 <b>오늘 아침 준비</b>", "<i>" + tomorrow[:100] + "</i>"]

    return lines


def send_lessons_status():
    """학습현황 버튼 눌렸을 때"""
    try:
        from aria_lessons import load_lessons
        data    = load_lessons()
        lessons = data.get("lessons", [])
        total   = data.get("total_lessons", 0)
        updated = data.get("last_updated", "없음")

        if not lessons:
            return send_message(
                "📚 <b>학습 현황</b>\n\n아직 누적된 교훈이 없습니다.\n내일부터 새벽 리포트가 실수를 감지하기 시작합니다."
            )

        lines = [
            "📚 <b>ARIA 학습 현황</b>",
            "누적 교훈: <b>" + str(total) + "개</b>",
            "마지막 업데이트: " + updated,
            "",
            "━━ 현재 적용 중인 교훈 ━━",
        ]

        # 심각도 순으로 정렬
        sorted_lessons = sorted(
            lessons,
            key=lambda x: (
                3 if x["severity"] == "high" else
                2 if x["severity"] == "medium" else 1
            ),
            reverse=True
        )

        for l in sorted_lessons[:8]:
            sev_emoji = "🔴" if l["severity"] == "high" else "🟡" if l["severity"] == "medium" else "🟢"
            applied   = l.get("applied", 0)
            reinforced = l.get("reinforced", 0)
            stats     = "적용 " + str(applied) + "회"
            if reinforced > 0:
                stats += " | 반복 " + str(reinforced) + "회"

            lines.append(
                sev_emoji + " <b>[" + l["category"] + "]</b> " + l["source"] + " " + l["date"]
            )
            lines.append("  " + l["lesson"][:70])
            lines.append("  <i>" + stats + "</i>")
            lines.append("")

        # 카테고리별 요약
        categories = {}
        for l in lessons:
            cat = l["category"]
            categories[cat] = categories.get(cat, 0) + 1

        if categories:
            lines.append("━━ 카테고리별 교훈 수 ━━")
            for cat, cnt in sorted(categories.items(), key=lambda x: x[1], reverse=True):
                bar = "█" * min(cnt, 5) + "░" * (5 - min(cnt, 5))
                lines.append("<code>" + cat.ljust(8) + " [" + bar + "] " + str(cnt) + "개</code>")

        lines += [
            "",
            "<i>교훈은 매일 새벽 자동 추출되어 아침 분석에 반영됩니다</i>",
        ]

        return send_message("\n".join(lines))

    except ImportError:
        return send_message("aria_lessons.py 파일을 찾을 수 없습니다.")
    except Exception as e:
        return send_message("오류: " + str(e))


def send_error(message):
    return send_message(
        "⚠️ <b>ARIA 오류</b>\n\n<code>" + message + "</code>\n\n"
        + "<i>" + datetime.now(KST).strftime("%Y-%m-%d %H:%M KST") + "</i>"
    )


def send_start_notification():
    mode = os.environ.get("ARIA_MODE", "MORNING")
    mode_labels = {
        "MORNING":   "🌅 아침 풀분석",
        "AFTERNOON": "☀️ 오후 업데이트",
        "EVENING":   "🌆 저녁 마감",
        "DAWN":      "🌙 새벽 글로벌",
    }
    label = mode_labels.get(mode, mode)
    return send_message(
        "⚙️ <b>ARIA 분석 시작</b> " + label + "\n"
        + "<i>" + datetime.now(KST).strftime("%Y-%m-%d %H:%M KST") + "</i>\n"
        + "Hunter → Analyst → Devil → Reporter..."
    )


if __name__ == "__main__":
    print("Telegram test...")
    ok = send_message("✅ ARIA 텔레그램 연결 성공!", reply_markup=make_buttons())
    print("OK" if ok else "FAIL")
