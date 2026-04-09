import httpx
import os
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
GITHUB_TOKEN      = os.environ.get("GH_TOKEN", "")        # GitHub Personal Access Token
GITHUB_REPO       = os.environ.get("GH_REPO", "")         # 예: minwoo/aria-agent
BASE_URL          = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"


# ── 기본 전송 ──────────────────────────────────────────────────────────────────
def send_message(text: str, reply_markup=None, parse_mode: str = "HTML") -> bool:
    try:
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        r = httpx.post(f"{BASE_URL}/sendMessage", json=payload, timeout=10)
        return r.json().get("ok", False)
    except Exception as e:
        print(f"[Telegram] 전송 실패: {e}")
        return False


# ── 버튼 만들기 ────────────────────────────────────────────────────────────────
def make_buttons():
    """리포트 하단에 붙는 버튼 3개"""
    return {
        "inline_keyboard": [[
            {"text": "🔄 지금 분석", "callback_data": "run_now"},
            {"text": "📋 히스토리",  "callback_data": "history"},
            {"text": "🧠 성장리뷰",  "callback_data": "review"},
        ]]
    }


# ── GitHub Actions 즉시 트리거 ────────────────────────────────────────────────
def trigger_github_action(workflow: str = "aria.yml") -> bool:
    """버튼 눌렸을 때 GitHub Actions 즉시 실행"""
    if not GITHUB_TOKEN or not GITHUB_REPO:
        print("[GitHub] GH_TOKEN 또는 GH_REPO 미설정")
        return False
    try:
        r = httpx.post(
            f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{workflow}/dispatches",
            headers={
                "Authorization": f"token {GITHUB_TOKEN}",
                "Accept": "application/vnd.github.v3+json",
            },
            json={"ref": "main"},
            timeout=10,
        )
        return r.status_code == 204
    except Exception as e:
        print(f"[GitHub] 트리거 실패: {e}")
        return False


# ── 버튼 콜백 처리 ────────────────────────────────────────────────────────────
def handle_callback(callback_data: str):
    """텔레그램 버튼 눌렸을 때 처리"""
    if callback_data == "run_now":
        send_message("⚙️ <b>ARIA 분석 시작!</b>\n약 3~5분 후 리포트가 도착합니다.")
        trigger_github_action()

    elif callback_data == "history":
        import json
        from pathlib import Path
        memory_file = Path("memory.json")
        if memory_file.exists():
            memory = json.loads(memory_file.read_text(encoding="utf-8"))
            if memory:
                lines = ["📋 <b>ARIA 분석 기록</b>\n"]
                for m in reversed(memory[-10:]):
                    regime = m.get("market_regime", "?")
                    emoji = "🟢" if "선호" in regime else "🔴" if "회피" in regime else "🟡"
                    lines.append(f"{emoji} {m.get('analysis_date','')} — {m.get('one_line_summary','')[:40]}")
                send_message("\n".join(lines))
            else:
                send_message("아직 분석 기록이 없어요.")
        else:
            send_message("memory.json 파일을 찾을 수 없어요.")

    elif callback_data == "review":
        import json
        from pathlib import Path
        memory_file = Path("memory.json")
        if memory_file.exists():
            memory = json.loads(memory_file.read_text(encoding="utf-8"))
            recent = memory[-7:]
            if recent:
                lines = ["🧠 <b>최근 7일 성장 리뷰</b>\n"]
                for m in recent:
                    meta = m.get("meta_improvement", {})
                    lines.append(
                        f"<b>{m.get('analysis_date','')}</b>\n"
                        f"  조정: {meta.get('reweighting','기록없음')}\n"
                    )
                send_message("\n".join(lines))
            else:
                send_message("아직 성장 기록이 없어요.")
        else:
            send_message("memory.json 파일을 찾을 수 없어요.")


# ── 리포트 전송 ────────────────────────────────────────────────────────────────
def send_report(report: dict, run_number: int) -> bool:
    regime = report.get("market_regime", "?")
    regime_emoji = "🟢" if "선호" in regime else "🔴" if "회피" in regime else "🟡"

    lines = [
        f"<b>📊 ARIA 리포트 #{run_number}</b>",
        f"<code>{report.get('analysis_date','')} {report.get('analysis_time','')}</code>",
        "",
        f"{regime_emoji} <b>{regime}</b>  |  신뢰도: {report.get('confidence_overall','?')}",
        "",
        f"💡 <i>{report.get('one_line_summary','')}</i>",
        "",
    ]

    kr = report.get("korea_focus", {})
    if kr:
        lines += [
            "🇰🇷 <b>한국 시장</b>",
            f"  원/달러: <code>{kr.get('krw_usd','-')}</code>",
            f"  코스피:  <code>{kr.get('kospi_flow','-')}</code>",
            f"  SK하이닉스: <code>{kr.get('sk_hynix','-')}</code>",
            f"  삼성전자:   <code>{kr.get('samsung','-')}</code>",
            f"  <i>{kr.get('assessment','')}</i>",
            "",
        ]

    for o in report.get("outflows", [])[:3]:
        lines.append(f"▼ <b>{o.get('zone','')}</b> [{o.get('severity','')}]")
        lines.append(f"  <i>{o.get('reason','')[:60]}</i>")
    if report.get("outflows"): lines.append("")

    for i in report.get("inflows", [])[:3]:
        lines.append(f"▲ <b>{i.get('zone','')}</b> [{i.get('momentum','')}]")
        lines.append(f"  <i>{i.get('reason','')[:60]}</i>")
    if report.get("inflows"): lines.append("")

    for tk in report.get("thesis_killers", [])[:3]:
        lines.append(f"🎯 [{tk.get('timeframe','')}] <b>{tk.get('event','')}</b>")
        lines.append(f"  ✓ {tk.get('confirms_if','')[:50]}")
        lines.append(f"  ✗ {tk.get('invalidates_if','')[:50]}")
    if report.get("thesis_killers"): lines.append("")

    for idx, a in enumerate(report.get("actionable_watch", [])[:3], 1):
        lines.append(f"📌 {idx}. {a}")

    lines.append("\n<code>ARIA Multi-Agent | Anthropic</code>")

    # 버튼 포함해서 전송
    return send_message("\n".join(lines), reply_markup=make_buttons())


def send_error(message: str) -> bool:
    return send_message(
        f"⚠️ <b>ARIA 오류</b>\n\n<code>{message}</code>\n\n"
        f"<i>{datetime.now(KST).strftime('%Y-%m-%d %H:%M KST')}</i>"
    )


def send_start_notification() -> bool:
    return send_message(
        f"⚙️ <b>ARIA 분석 시작</b>\n"
        f"<i>{datetime.now(KST).strftime('%Y-%m-%d %H:%M KST')}</i>\n"
        f"Hunter → Analyst → Devil → Reporter 순서로 실행 중..."
    )


if __name__ == "__main__":
    print("텔레그램 연결 테스트...")
    ok = send_message("✅ ARIA 텔레그램 연결 성공!", reply_markup=make_buttons())
    print("성공!" if ok else "실패")