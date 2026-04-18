"""
ORCA main orchestrator.
Hunter -> Analyst -> Devil -> Reporter
"""
import os
import sys
import json
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

from rich.console import Console
from rich.panel   import Panel
from rich.table   import Table
from rich         import box

from .agents import agent_hunter, agent_analyst, agent_devil, agent_reporter
from .analysis import (
    run_sentiment, run_portfolio, run_rotation,
    save_baseline, build_baseline_context, get_regime_drift,
    run_verification, build_lessons_prompt, extract_dawn_lessons,
    review_recent_candidates,
)
from .brand import JACKAL_NAME, ORCA_FULL_NAME, ORCA_NAME
from .compat import get_orca_env
from .data import fetch_all_market_data, update_cost, get_monthly_cost_summary
from .notify import send_message, send_start_notification, send_report, send_error
from .state import (
    start_run as state_start_run,
    finish_run as state_finish_run,
    summarize_candidate_probabilities,
    record_report_predictions,
)

KST     = timezone(timedelta(hours=9))
from .paths import MEMORY_FILE, REPORTS_DIR, atomic_write_json
MODE    = get_orca_env("ORCA_MODE", "MORNING")
console = Console()


def sanitize_korea_claims(report: dict, market_data: dict) -> dict:
    """KIS 미연결 시 한국 수급 단정 표현 완화"""
    import re
    kis_connected = os.environ.get("KIS_CONNECTED", "").lower() == "true"
    if kis_connected:
        return report

    SOFTEN_MAP = {
        r"외국인\s*\d+[개월주일]+\s*연속\s*순매도": "외국인 순매도 흐름 지속 추정(수급 미확인)",
        r"외국인\s*\d+[개월주일]+\s*연속\s*순매수": "외국인 순매수 흐름 추정(수급 미확인)",
        r"외국인\s*누적\s*[+-]?\d+": "외국인 누적 흐름 추정(직접 데이터 미확인)",
        r"기관\s*\d+[조억만]+\s*원\s*순[매도수]": "기관 수급 추정(직접 데이터 미확인)",
        r"수급\s*(악화|개선)\s*확정": "수급 추정",
        r"외국인\s*이탈\s*가속": "외국인 이탈 압력 추정",
        r"(확정|확인됨)(?=.*수급)": "가능성",
    }

    def soften_text(text: str) -> str:
        if not isinstance(text, str):
            return text
        for pattern, replacement in SOFTEN_MAP.items():
            text = re.sub(pattern, replacement, text)
        return text

    def soften_recursive(obj):
        if isinstance(obj, str):  return soften_text(obj)
        if isinstance(obj, list): return [soften_recursive(i) for i in obj]
        if isinstance(obj, dict): return {k: soften_recursive(v) for k, v in obj.items()}
        return obj

    return soften_recursive(report)


def _now() -> datetime:
    return datetime.now(KST)


def load_memory() -> list:
    if not MEMORY_FILE.exists():
        return []
    try:
        return json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        print("⚠️ memory.json 손상 감지 (" + str(e) + ") — 빈 메모리로 재시작")
        backup = MEMORY_FILE.with_suffix(".json.bak")
        MEMORY_FILE.rename(backup)
        print("백업 저장: " + str(backup))
        return []


def save_memory(memory: list, analysis: dict):
    memory = [m for m in memory if m.get("analysis_date") != analysis.get("analysis_date")]
    new_memory = memory + [analysis]

    # 90일 초과분 → memory_archive.json에 누적 보존 (영구 소실 방지)
    if len(new_memory) > 90:
        overflow = new_memory[:-90]
        archive_file = MEMORY_FILE.with_name("memory_archive.json")
        archived: list = []
        if archive_file.exists():
            try:
                archived = json.loads(archive_file.read_text(encoding="utf-8"))
            except Exception:
                pass
        archived.extend(overflow)
        atomic_write_json(archive_file, archived[-365:])

    atomic_write_json(MEMORY_FILE, new_memory[-90:])


def save_report(analysis: dict) -> Path:
    REPORTS_DIR.mkdir(exist_ok=True)
    date = analysis.get("analysis_date", _now().strftime("%Y-%m-%d"))
    mode = analysis.get("mode", "MORNING").lower()
    path = REPORTS_DIR / (date + "_" + mode + ".json")
    atomic_write_json(path, analysis)
    return path


def get_todays_analyses() -> list:
    today   = _now().strftime("%Y-%m-%d")
    reports = []
    if REPORTS_DIR.exists():
        for f in REPORTS_DIR.glob(today + "_*.json"):
            try:
                reports.append(json.loads(f.read_text(encoding="utf-8")))
            except Exception:
                pass
    return reports


def print_report(report: dict, run_n: int):
    regime     = report.get("market_regime", "?")
    mode       = report.get("mode", "MORNING")
    mode_label = report.get("mode_label", mode)
    rc = "green" if "선호" in regime else "red" if "회피" in regime else "yellow"

    console.rule("[bold purple]" + ORCA_NAME + " [" + mode_label + "] #" + str(run_n) + "[/bold purple]")
    console.print(Panel(
        "[bold]" + report.get("one_line_summary", "") + "[/bold]",
        title="[" + rc + "]" + regime + "[/" + rc + "]  " + report.get("confidence_overall", "")
              + "  " + report.get("analysis_date", ""),
        border_style="purple",
    ))

    tp = report.get("trend_phase", "")
    ts = report.get("trend_strategy", {})
    if tp:
        tc = "green" if "상승" in tp else "red" if "하락" in tp else "yellow"
        console.print(Panel(
            "[bold]" + tp + "[/bold]\n\nStrategy: " + ts.get("recommended", "")
            + "\nCaution: " + ts.get("caution", ""),
            title="Trend", border_style=tc,
        ))

    vi = report.get("volatility_index", {})
    if vi:
        vt = Table(box=box.SIMPLE, show_header=False)
        vt.add_column("", style="dim", width=12)
        vt.add_column("")
        for label, key in [("VIX","vix"),("VKOSPI","vkospi"),("공포탐욕","fear_greed"),("레벨","level")]:
            vt.add_row(label, vi.get(key, "-"))
        console.print(Panel(vt, title="Volatility", border_style="yellow"))

    kr = report.get("korea_focus", {})
    if kr:
        kt = Table(box=box.SIMPLE, show_header=False)
        kt.add_column("", style="dim", width=12)
        kt.add_column("", style="cyan")
        for label, key in [("KRW/USD","krw_usd"),("KOSPI","kospi_flow"),("SK Hynix","sk_hynix"),("Samsung","samsung")]:
            kt.add_row(label, kr.get(key) or "")
        console.print(Panel(kt, title="Korea Market", border_style="cyan"))

    ft = Table(box=box.SIMPLE, show_header=True, header_style="bold", expand=True)
    ft.add_column("Outflow", style="red")
    ft.add_column("Inflow",  style="green")
    out = report.get("outflows", [])
    inp = report.get("inflows", [])
    for i in range(max(len(out), len(inp))):
        oc = ("[bold]" + out[i]["zone"] + "[/bold]\n[dim]" + out[i].get("reason","")[:80] + "[/dim]") if i < len(out) else ""
        ic = ("[bold]" + inp[i]["zone"] + "[/bold]\n[dim]" + inp[i].get("reason","")[:80] + "[/dim]") if i < len(inp) else ""
        ft.add_row(oc, ic)
    console.print(Panel(ft, title="Capital Flow", border_style="blue"))

    candidate_review = report.get("jackal_candidate_review", {})
    if candidate_review.get("reviewed_count"):
        lines = [
            "시장 바이어스: " + candidate_review.get("market_bias_label", ""),
            "분류: aligned {aligned_count} / neutral {neutral_count} / opposed {opposed_count}".format(**candidate_review),
        ]
        for item in candidate_review.get("highlights", [])[:3]:
            lines.append(
                "- {ticker} {alignment}/{review_verdict} ({quality})".format(
                    ticker=item.get("ticker", ""),
                    alignment=item.get("alignment", ""),
                    review_verdict=item.get("review_verdict", ""),
                    quality=item.get("quality_score", "-"),
                )
            )
        console.print(Panel("\n".join(lines), title=JACKAL_NAME + " Candidate Review", border_style="magenta"))

    probability_summary = report.get("jackal_probability_summary", {})
    if probability_summary.get("overall", {}).get("total", 0) > 0:
        overall = probability_summary.get("overall", {})
        lines = [
            "최근 {window}일 overall {win_rate}% | 보수적 {effective}% (n={total})".format(
                window=probability_summary.get("window_days", 90),
                win_rate=overall.get("win_rate", 0.0),
                effective=overall.get("effective_win_rate", overall.get("win_rate", 0.0)),
                total=overall.get("total", 0),
            )
        ]
        skipped = int(probability_summary.get("duplicates_skipped", 0) or 0)
        deduped_rows = int(probability_summary.get("deduped_rows", 0) or 0)
        raw_rows = int(probability_summary.get("raw_rows", deduped_rows) or deduped_rows)
        if raw_rows > 0:
            lines.append(f"표본 정리: raw {raw_rows} → unique {deduped_rows}" + (f" (중복 {skipped} 제거)" if skipped else ""))
        trusted = probability_summary.get("trusted_families", [])
        cautious = probability_summary.get("cautious_families", [])
        if trusted:
            lines.append("신뢰: " + ", ".join(
                f"{item.get('signal_family_label', item.get('signal_family',''))} {item.get('effective_win_rate', item.get('win_rate',0)):.1f}%/{item.get('total',0)}"
                for item in trusted[:3]
            ))
        if cautious:
            lines.append("경계: " + ", ".join(
                f"{item.get('signal_family_label', item.get('signal_family',''))} {item.get('effective_win_rate', item.get('win_rate',0)):.1f}%/{item.get('total',0)}"
                for item in cautious[:3]
            ))
        aligned_best = probability_summary.get("best_aligned_families", [])
        opposed_best = probability_summary.get("best_opposed_families", [])
        if aligned_best:
            lines.append("정합 강점: " + ", ".join(
                f"{item.get('signal_family_label', item.get('signal_family',''))} {item.get('effective_win_rate', item.get('win_rate',0)):.1f}%/{item.get('total',0)}"
                for item in aligned_best[:2]
            ))
        if opposed_best:
            lines.append("역행 강점: " + ", ".join(
                f"{item.get('signal_family_label', item.get('signal_family',''))} {item.get('effective_win_rate', item.get('win_rate',0)):.1f}%/{item.get('total',0)}"
                for item in opposed_best[:2]
            ))
        console.print(Panel("\n".join(lines), title=JACKAL_NAME + " Probability View", border_style="bright_blue"))

    if report.get("tomorrow_setup") and mode in ["EVENING", "DAWN"]:
        console.print(Panel(report["tomorrow_setup"], title="Tomorrow Setup", border_style="yellow"))

    console.rule()

def _compact_probability_summary(*, days: int = 90, min_samples: int = 5) -> dict:
    summary = summarize_candidate_probabilities(days=days, min_samples=min_samples)
    trusted = [
        item for item in summary.get("best_signal_families", [])
        if item.get("qualified")
    ][:5]
    cautious = [
        item for item in summary.get("weak_signal_families", [])
        if item.get("qualified")
    ][:5]
    return {
        "window_days": days,
        "min_samples": min_samples,
        "raw_rows": summary.get("raw_rows", 0),
        "deduped_rows": summary.get("deduped_rows", 0),
        "duplicates_skipped": summary.get("duplicates_skipped", 0),
        "overall": summary.get("overall", {}),
        "trusted_families": trusted,
        "cautious_families": cautious,
        "alignment_summary": summary.get("by_alignment", {}),
        "best_aligned_families": summary.get("best_aligned_families", [])[:3],
        "best_opposed_families": summary.get("best_opposed_families", [])[:3],
    }


def _collect_jackal_news(hunter_data: dict):
    """
    data/jackal_watchlist.json 읽기 → 해당 종목 뉴스 수집 → data/jackal_news.json 저장.
    ORCA Hunter의 웹서치 결과에서 JACKAL 추천 종목 관련 헤드라인 추출.
    비용: Claude Haiku 1회 (약 $0.002)
    """
    from .paths import DATA_DIR
    watchlist_file = DATA_DIR / "jackal_watchlist.json"
    news_file      = DATA_DIR / "jackal_news.json"

    if not watchlist_file.exists():
        return

    try:
        wl = json.loads(watchlist_file.read_text(encoding="utf-8"))
    except Exception:
        return

    tickers = wl.get("tickers", [])
    details = wl.get("details", {})
    if not tickers:
        return

    console.print(f"[dim]{JACKAL_NAME} watchlist 뉴스 수집: {tickers}[/dim]")

    # Hunter가 수집한 신호에서 관련 헤드라인 추출
    signals = hunter_data.get("raw_signals", [])
    ticker_names = [details.get(t, {}).get("name", t) for t in tickers]
    search_terms = tickers + ticker_names

    relevant = []
    for sig in signals:
        headline = sig.get("headline", "")
        if any(term.lower() in headline.lower() for term in search_terms):
            relevant.append({
                "ticker":   next((t for t in tickers if t.lower() in headline.lower() or
                                  details.get(t,{}).get("name","").lower() in headline.lower()), tickers[0]),
                "headline": headline,
                "source":   sig.get("source_hint", ""),
                "data_pt":  sig.get("data_point", ""),
            })

    # Hunter 결과가 부족하면 Haiku로 보완 검색
    if len(relevant) < 3:
        try:
            from .agents import call_api, MODEL_HUNTER
            ticker_str = ", ".join([
                f"{t}({details.get(t, {}).get('name', t)})" for t in tickers
            ])
            regime_str = wl.get("regime", "")
            news_prompt = (
                f"Search recent news for these stocks: {ticker_str}. "
                f"Market regime: {regime_str}. "
                "Return ONLY valid JSON: "
                '{"news_items": [{"ticker": "X", "headline": "...", "impact": "bullish/bearish/neutral"}]}'
            )
            raw = call_api(
                "You are a financial news collector. Return ONLY valid JSON, no markdown.",
                news_prompt,
                use_search=True,
                model=MODEL_HUNTER,
                max_tokens=800,
            )
            import re as _re, json as _json
            cleaned = _re.sub(r"```(?:json)?|```", "", raw).strip()
            m = _re.search(r"\{[\s\S]*\}", cleaned)
            if m:
                data = _json.loads(m.group())
                for item in data.get("news_items", []):
                    relevant.append({
                        "ticker":   item.get("ticker", ""),
                        "headline": item.get("headline", ""),
                        "impact":   item.get("impact", "neutral"),
                        "source":   "web_search",
                    })
        except Exception as e:
            console.print(f"[yellow]{JACKAL_NAME} 뉴스 보완 검색 실패: {e}[/yellow]")

    # 저장
    try:
        result = {
            "collected_at": _now().strftime("%Y-%m-%d %H:%M KST"),
            "tickers":      tickers,
            "regime":       wl.get("regime", ""),
            "news_items":   relevant[:10],
            "total":        len(relevant),
        }
        atomic_write_json(news_file, result)
        console.print(f"[dim]{JACKAL_NAME} 뉴스 {len(relevant)}건 저장 → jackal_news.json[/dim]")
    except Exception as e:
        console.print(f"[yellow]jackal_news.json 저장 실패: {e}[/yellow]")


def main():
    parser = argparse.ArgumentParser(description=ORCA_NAME + " — " + ORCA_FULL_NAME)
    parser.add_argument("--history", action="store_true")
    args = parser.parse_args()

    memory = load_memory()

    if args.history:
        if not memory:
            console.print("[dim]No saved analyses[/dim]"); return
        t = Table(title=ORCA_NAME + " History", box=box.ROUNDED)
        t.add_column("Date"); t.add_column("Mode")
        t.add_column("Regime"); t.add_column("Summary")
        for m in reversed(memory[-20:]):
            reg = m.get("market_regime", "")
            col = "green" if "선호" in reg else "red" if "회피" in reg else "yellow"
            t.add_row(m.get("analysis_date",""), m.get("mode",""),
                      "[" + col + "]" + reg + "[/" + col + "]",
                      m.get("one_line_summary","")[:40])
        console.print(t); return

    today = _now().strftime("%Y-%m-%d")
    console.print(Panel(
        "[bold]" + ORCA_NAME + " [" + MODE + "] Analysis Start[/bold]\nHunter → Analyst → Devil → Reporter",
        border_style="purple",
    ))

    run_id = None

    def _finish_state(status: str, **kwargs):
        if not run_id:
            return
        try:
            state_finish_run(run_id, status, **kwargs)
        except Exception as state_err:
            console.print("[yellow]State DB finish skipped: " + str(state_err) + "[/yellow]")

    try:
        try:
            run_id = state_start_run(
                "orca",
                MODE,
                today,
                metadata={
                    "history_size": len(memory),
                    "github_event": os.environ.get("GITHUB_EVENT_NAME", ""),
                },
            )
        except Exception as state_err:
            console.print("[yellow]State DB start skipped: " + str(state_err) + "[/yellow]")
            run_id = None

        # ── 중복 실행 방어 ─────────────────────────────────────────
        if MODE in ["MORNING", "EVENING"]:
            existing = list(REPORTS_DIR.glob(today + "_" + MODE.lower() + ".json")) if REPORTS_DIR.exists() else []
            if existing:
                event = os.environ.get("GITHUB_EVENT_NAME", "")
                if event == "schedule":
                    _finish_state("aborted", metadata={"reason": "duplicate_scheduled_report"})
                    console.print("[red]⛔ 스케줄 중복 감지 — 종료[/red]")
                    sys.exit(0)
                else:
                    console.print("[yellow]⚠️ 오늘 " + MODE + " 이미 존재 — 수동 실행으로 덮어쓰기[/yellow]")

        # ── 1. 데이터 수집 ─────────────────────────────────────────
        print("\n=== 실시간 시장 데이터 수집 ===")
        market_data = fetch_all_market_data()
        update_cost(MODE)
        print(get_monthly_cost_summary())

        try:
            from .data import load_cost
            _cost = load_cost()
            _mk   = _now().strftime("%Y-%m")
            _monthly_usd = _cost.get("monthly_runs", {}).get(_mk, {}).get("estimated_usd", 0)
            if _monthly_usd >= 20.0:
                send_message(
                    "⚠️ <b>" + ORCA_NAME + " 월 비용 경고</b>\n\n"
                    "이번 달 추정 비용: <b>$" + str(round(_monthly_usd, 2))
                    + " (약 " + f"{round(_monthly_usd*1480):,}" + "원)</b>\n"
                    "임계값 $20 초과"
                )
        except Exception:
            pass

        if market_data.get("data_quality") == "poor":
            msg = "⚠️ 핵심 시장 데이터 수집 실패 — 분석 중단"
            console.print("[bold red]" + msg + "[/bold red]")
            send_message("⚠️ <b>" + ORCA_NAME + " 데이터 오류</b>\n\n" + msg)
            _finish_state(
                "aborted",
                data_quality=market_data.get("data_quality", "poor"),
                metadata={"reason": "poor_market_data"},
            )
            return

        # ── 2. 교훈 로드 ───────────────────────────────────────────
        lessons_prompt = ""
        if MODE == "MORNING":
            lessons_prompt = build_lessons_prompt()
            if lessons_prompt:
                console.print("[dim]Lessons injected[/dim]")

        # ── 3. Baseline 컨텍스트 ───────────────────────────────────
        baseline_context = ""
        if MODE != "MORNING":
            baseline_context = build_baseline_context(memory)
            console.print("[dim]Morning baseline loaded[/dim]" if baseline_context
                          else "[yellow]No baseline — full analysis[/yellow]")

        # ── 4. DAWN: 교훈 추출 ─────────────────────────────────────
        if MODE == "DAWN":
            todays = get_todays_analyses()
            if todays:
                extract_dawn_lessons(todays, "market outcomes today")

        # ── 5. MORNING: 어제 예측 채점 ────────────────────────────
        accuracy = {}
        if MODE == "MORNING":
            print("\n=== Verifying yesterday predictions ===")
            accuracy = run_verification()
            try:
                from .analysis import update_weights_from_accuracy
                changes = update_weights_from_accuracy(accuracy)
                if changes:
                    print("  📊 가중치 업데이트:", " | ".join(changes[:3]))
            except Exception as e:
                print(f"  가중치 업데이트 스킵: {e}")

        # ── 6. 4-Agent 파이프라인 ──────────────────────────────────
        send_start_notification()
        hunter  = agent_hunter(today, MODE, market_data)
        analyst = agent_analyst(hunter, MODE, lessons_prompt + baseline_context, memory=memory)
        devil   = agent_devil(analyst, memory, MODE)
        report  = agent_reporter(hunter, analyst, devil, memory, accuracy, MODE)

        report["analysis_date"] = today
        report["analysis_time"] = _now().strftime("%H:%M KST")
        report["mode"]          = MODE
        report["data_quality"]  = market_data.get("data_quality", "ok")

        # ── 7. 레짐 드리프트 ───────────────────────────────────────
        drift = get_regime_drift(report.get("market_regime", ""))
        if drift and drift != "STABLE":
            console.print("[yellow]Regime drift: " + drift + "[/yellow]")

        report = sanitize_korea_claims(report, market_data)

        print("\n=== JACKAL Candidate Review ===")
        candidate_review = review_recent_candidates(
            report,
            run_id=run_id,
            analysis_date=today,
        )
        if candidate_review.get("reviewed_count", 0) > 0:
            report["jackal_candidate_review"] = candidate_review
            console.print(
                "[dim]{count} candidates reviewed | aligned {aligned} / neutral {neutral} / opposed {opposed}[/dim]".format(
                    count=candidate_review.get("reviewed_count", 0),
                    aligned=candidate_review.get("aligned_count", 0),
                    neutral=candidate_review.get("neutral_count", 0),
                    opposed=candidate_review.get("opposed_count", 0),
                )
            )
        else:
            report["jackal_candidate_review"] = candidate_review
            console.print("[dim]No recent unresolved JACKAL candidates to review[/dim]")

        print("\n=== JACKAL Probability Summary ===")
        try:
            probability_summary = _compact_probability_summary(days=90, min_samples=5)
            report["jackal_probability_summary"] = probability_summary
            overall = probability_summary.get("overall", {})
            console.print(
                "[dim]overall {win_rate}% | effective {effective}% | trusted {trusted} | cautious {cautious}[/dim]".format(
                    win_rate=overall.get("win_rate", 0.0),
                    effective=overall.get("effective_win_rate", overall.get("win_rate", 0.0)),
                    trusted=len(probability_summary.get("trusted_families", [])),
                    cautious=len(probability_summary.get("cautious_families", [])),
                )
            )
        except Exception as prob_err:
            report["jackal_probability_summary"] = {"error": str(prob_err)}
            console.print("[yellow]Probability summary skipped: " + str(prob_err) + "[/yellow]")

        # ── 8. 출력 + 텔레그램 ────────────────────────────────────
        print_report(report, len(memory) + 1)
        send_report(report, len(memory) + 1)

        # ── 9. MORNING: Baseline 저장 ──────────────────────────────
        if MODE == "MORNING":
            save_baseline(report, market_data)
            console.print("[dim]Morning baseline saved[/dim]")

        # ── 10. 서브 분석 ──────────────────────────────────────────
        print("\n=== Sentiment Tracking ===")
        run_sentiment(report, market_data)

        print("\n=== Sector Rotation ===")
        run_rotation(report)

        print("\n=== Portfolio Analysis ===")
        run_portfolio(report, market_data)

        # ── 11. 저장 ───────────────────────────────────────────────
        save_memory(memory, report)
        path = save_report(report)
        console.print("[dim]Saved: " + str(path) + "[/dim]")
        prediction_stats = {"count": 0}
        if run_id:
            try:
                prediction_stats = record_report_predictions(run_id, report)
                console.print("[dim]State DB predictions: " + str(prediction_stats.get("count", 0)) + "[/dim]")
            except Exception as state_err:
                console.print("[yellow]State DB prediction save skipped: " + str(state_err) + "[/yellow]")

        try:
            from .analysis import update_pattern_db
            update_pattern_db(load_memory())
            console.print("[dim]Pattern DB updated[/dim]")
        except Exception as e:
            console.print("[yellow]Pattern DB 스킵: " + str(e) + "[/yellow]")

        # ── 12. Jackal watchlist 뉴스 수집 (MORNING만) ──────────
        if MODE == "MORNING":
            _collect_jackal_news(hunter)

        # ── 13. Dashboard HTML (MORNING만) ────────────────────────
        if MODE == "MORNING":
            try:
                from .dashboard import build_dashboard
                build_dashboard()
                console.print("[dim]Dashboard updated[/dim]")
            except Exception as e:
                console.print("[yellow]Dashboard 실패: " + str(e) + "[/yellow]")

        _finish_state(
            "completed",
            data_quality=report.get("data_quality", ""),
            report_path=str(path),
            report_summary=report.get("one_line_summary", ""),
            metadata={
                "market_regime": report.get("market_regime", ""),
                "trend_phase": report.get("trend_phase", ""),
                "consensus_level": report.get("consensus_level", ""),
                "prediction_count": prediction_stats.get("count", 0),
            },
        )

    except Exception as e:
        _finish_state("failed", metadata={"error": str(e)})
        console.print("[bold red]Error: " + str(e) + "[/bold red]")
        try:
            send_error(str(e))
        except Exception:
            pass
        import traceback; traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()


