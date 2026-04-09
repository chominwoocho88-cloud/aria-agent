import os
import sys
import json
import re
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

import anthropic
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

# ── 설정 ──────────────────────────────────────────────────────────────────────
API_KEY     = os.environ.get("ANTHROPIC_API_KEY", "")
MEMORY_FILE = Path("memory.json")
REPORTS_DIR = Path("reports")
KST         = timezone(timedelta(hours=9))

MODEL_HUNTER   = "claude-haiku-4-5-20251001"
MODEL_ANALYST  = "claude-sonnet-4-6"
MODEL_DEVIL    = "claude-sonnet-4-6"
MODEL_REPORTER = "claude-opus-4-6"

# 실행 모드 (환경변수로 주입)
MODE = os.environ.get("ARIA_MODE", "MORNING")
# MORNING  : 07:30 풀 분석 + 교훈 반영
# AFTERNOON: 14:30 오후 업데이트
# EVENING  : 20:30 저녁 마감
# DAWN     : 04:30 글로벌 + 실수 추출

console = Console()
client  = anthropic.Anthropic(api_key=API_KEY)


# ── 유틸 ──────────────────────────────────────────────────────────────────────
def now_kst():
    return datetime.now(KST)

def parse_json(text):
    raw = re.sub(r"```json|```", "", text).strip()
    m   = re.search(r"\{[\s\S]*\}", raw)
    if not m:
        raise ValueError("JSON not found:\n" + text[:300])
    s = m.group()
    s = re.sub(r",\s*([}\]])", r"\1", s)
    s += "]" * (s.count("[") - s.count("]"))
    s += "}" * (s.count("{") - s.count("}"))
    return json.loads(s)

def call_api(system, user, use_search=False, model=MODEL_ANALYST, max_tokens=2000):
    kwargs = dict(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    if use_search:
        kwargs["tools"] = [{"type": "web_search_20250305", "name": "web_search"}]

    full = ""
    sc   = 0
    with client.messages.stream(**kwargs) as s:
        for ev in s:
            t = getattr(ev, "type", "")
            if t == "content_block_start":
                blk = getattr(ev, "content_block", None)
                if blk and getattr(blk, "type", "") == "tool_use":
                    sc += 1
                    q = getattr(blk, "input", {}).get("query", "")
                    console.print("    [dim]Search [" + str(sc) + "]: " + q + "[/dim]")
            elif t == "content_block_delta":
                d = getattr(ev, "delta", None)
                if d and getattr(d, "type", "") == "text_delta":
                    full += d.text
    return full

def load_memory():
    if MEMORY_FILE.exists():
        return json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
    return []

def save_memory(memory, analysis):
    memory = [m for m in memory if m.get("analysis_date") != analysis.get("analysis_date")]
    memory = (memory + [analysis])[-90:]
    MEMORY_FILE.write_text(json.dumps(memory, ensure_ascii=False, indent=2), encoding="utf-8")

def save_report(analysis):
    REPORTS_DIR.mkdir(exist_ok=True)
    date  = analysis.get("analysis_date", now_kst().strftime("%Y-%m-%d"))
    mode  = analysis.get("mode", "MORNING").lower()
    path  = REPORTS_DIR / (date + "_" + mode + ".json")
    path.write_text(json.dumps(analysis, ensure_ascii=False, indent=2), encoding="utf-8")
    return path

def get_todays_analyses():
    """오늘 저장된 모든 분석 불러오기"""
    today   = now_kst().strftime("%Y-%m-%d")
    reports = []
    if REPORTS_DIR.exists():
        for f in REPORTS_DIR.glob(today + "_*.json"):
            try:
                reports.append(json.loads(f.read_text(encoding="utf-8")))
            except:
                pass
    return reports


# ── 모드별 시스템 프롬프트 ─────────────────────────────────────────────────────
def get_mode_context(mode, lessons_prompt=""):
    base = lessons_prompt  # 교훈 주입

    if mode == "MORNING":
        return """You are ARIA in MORNING mode (07:30 KST).
This is the PRIMARY daily analysis. Set the baseline for today.
Do a FULL comprehensive analysis.
Pay special attention to overnight US market moves and pre-market signals.
""" + base

    elif mode == "AFTERNOON":
        return """You are ARIA in AFTERNOON mode (14:30 KST).
This is an UPDATE analysis. Focus on what CHANGED since this morning.
Compare with morning analysis if available.
Focus on: intraday reversals, new news, volume anomalies, momentum shifts.
Do NOT repeat the morning analysis. Only highlight CHANGES and new developments.
""" + base

    elif mode == "EVENING":
        return """You are ARIA in EVENING mode (20:30 KST).
This is the DAY SUMMARY. Summarize what actually happened today.
- What did the morning analysis get right?
- What did it miss?
- What is the setup for tomorrow?
- Key levels to watch overnight.
Be honest about prediction quality.
""" + base

    elif mode == "DAWN":
        return """You are ARIA in DAWN mode (04:30 KST).
Focus on GLOBAL markets overnight:
- US market close results
- After-hours / futures
- Asian market open
- Overnight geopolitical developments
Also review today's earlier analyses and note what was right/wrong.
Set the stage for tomorrow morning.
""" + base

    return base


# ── Agent 1: Hunter ───────────────────────────────────────────────────────────
def get_hunter_queries(mode):
    base = [
        "global financial markets today",
        "US stock market S&P Nasdaq today",
        "Korean won USD exchange rate today",
        "semiconductor Nvidia SK Hynix Samsung today",
        "VIX index value today AND CNN fear greed index exact number",
        "geopolitical risk today",
    ]
    if mode == "DAWN":
        base[0] = "US market close results today AND Asia market open"
        base.append("overnight futures S&P Nasdaq")
    elif mode == "AFTERNOON":
        base.append("market reversal midday today")
    elif mode == "EVENING":
        base.append("market close summary today")
    return base

HUNTER_SYSTEM = """You are a financial news collection agent. Only collect facts.
Search the provided areas and return structured data.
Find EXACT numbers for VIX and fear/greed index. Do not estimate.
Return ONLY valid JSON. No markdown.
{
  "collected_at": "YYYY-MM-DD HH:MM KST",
  "mode": "",
  "raw_signals": [{"category":"","headline":"","data_point":""}],
  "market_snapshot": {
    "sp500":"","nasdaq":"","kospi":"",
    "krw_usd":"","us_10y":"","vix":"","vkospi":"","fear_greed":""
  },
  "total_signals": 0
}"""

def agent_hunter(date_str, mode):
    console.print("\n[bold cyan]Agent 1 - HUNTER [" + mode + "][/bold cyan]")
    queries = get_hunter_queries(mode)
    search_str = " AND ".join(queries[:3])
    raw = call_api(
        HUNTER_SYSTEM,
        "Today: " + date_str + " Mode: " + mode + ". Search: " + search_str + ". Return JSON.",
        use_search=True, model=MODEL_HUNTER, max_tokens=2000
    )
    result = parse_json(raw)
    result["mode"] = mode
    console.print("  [green]Done: " + str(result.get("total_signals", 0)) + " signals[/green]")
    return result


# ── Agent 2: Analyst ──────────────────────────────────────────────────────────
ANALYST_SYSTEM_BASE = """You are a capital flow analysis agent.
Analyze Hunter data and map capital flows.
Return ONLY valid JSON in Korean. No markdown.
{
  "market_regime": "위험선호/위험회피/전환중/혼조",
  "trend_phase": "상승추세/횡보추세/하락추세",
  "trend_strategy": {"recommended":"","caution":"","difficulty":"쉬움/보통/어려움"},
  "regime_reason": "",
  "volatility_index": {
    "vkospi":"","vix":"","fear_greed":"",
    "level":"극단공포/공포/중립/탐욕/극단탐욕",
    "interpretation":""
  },
  "retail_reversal_signal": {"retail_behavior":"","contrarian_implication":"","reliability":"낮음/보통/높음"},
  "outflows": [{"zone":"","reason":"","severity":"높음/보통/낮음","data_point":""}],
  "inflows":  [{"zone":"","reason":"","momentum":"강함/형성중/약함","data_point":""}],
  "neutral_waiting": [{"zone":"","catalyst_needed":""}],
  "hidden_signals": [{"signal":"","implication":"","confidence":"낮음/보통/높음"}],
  "korea_focus": {"krw_usd":"","kospi_flow":"","sk_hynix":"","samsung":"","assessment":""},
  "analyst_confidence": "낮음/보통/높음"
}"""

def agent_analyst(hunter_data, mode, lessons_prompt=""):
    console.print("\n[bold yellow]Agent 2 - ANALYST [" + mode + "][/bold yellow]")
    mode_ctx = get_mode_context(mode, lessons_prompt)
    slim = {
        "market_snapshot": hunter_data.get("market_snapshot", {}),
        "raw_signals":     hunter_data.get("raw_signals", [])[:15],
        "mode":            mode,
    }
    raw = call_api(
        mode_ctx + "\n\n" + ANALYST_SYSTEM_BASE,
        "Hunter data:\n" + json.dumps(slim, ensure_ascii=False) + "\n\nReturn JSON.",
        model=MODEL_ANALYST, max_tokens=2500
    )
    result = parse_json(raw)
    console.print("  [green]Done: " + str(result.get("market_regime","")) + " / " + str(result.get("trend_phase","")) + "[/green]")
    return result


# ── Agent 3: Devil ────────────────────────────────────────────────────────────
DEVIL_SYSTEM = """You are a counter-argument agent. Challenge the Analyst sharply.
Return ONLY valid JSON in Korean. No markdown.
{
  "verdict": "동의/부분동의/반대",
  "counterarguments": [{"against":"","because":"","risk_level":"낮음/보통/높음"}],
  "alternative_scenario": {"regime":"","narrative":"","probability":"낮음/보통/높음"},
  "thesis_killers": [{"event":"","timeframe":"","confirms_if":"","invalidates_if":""}],
  "tail_risks": []
}"""

def agent_devil(analyst_data, memory, mode):
    console.print("\n[bold red]Agent 3 - DEVIL [" + mode + "][/bold red]")
    past = ""
    if memory:
        last = memory[-1]
        past = "\n\nPrior: regime=" + str(last.get("market_regime","")) + " summary=" + str(last.get("one_line_summary",""))
    slim = {
        "market_regime":  analyst_data.get("market_regime",""),
        "trend_phase":    analyst_data.get("trend_phase",""),
        "outflows":       analyst_data.get("outflows",[])[:3],
        "inflows":        analyst_data.get("inflows",[])[:3],
        "hidden_signals": analyst_data.get("hidden_signals",[])[:3],
        "analyst_confidence": analyst_data.get("analyst_confidence",""),
    }
    raw = call_api(
        DEVIL_SYSTEM,
        "Analyst:\n" + json.dumps(slim, ensure_ascii=False) + past + "\n\nReturn JSON.",
        model=MODEL_DEVIL, max_tokens=2000
    )
    result = parse_json(raw)
    console.print("  [green]Done: " + str(result.get("verdict","")) + " / " + str(len(result.get("counterarguments",[]))) + " counters[/green]")
    return result


# ── Agent 4: Reporter ─────────────────────────────────────────────────────────
REPORTER_SYSTEM = """You are the final report agent. Synthesize all agent results.
Return ONLY valid JSON in Korean. No markdown.
{
  "analysis_date": "YYYY-MM-DD",
  "analysis_time": "HH:MM KST",
  "mode": "",
  "mode_label": "아침 풀분석/오후 업데이트/저녁 마감/새벽 글로벌",
  "market_regime": "",
  "trend_phase": "상승추세/횡보추세/하락추세",
  "trend_strategy": {"recommended":"","caution":"","difficulty":""},
  "confidence_overall": "낮음/보통/높음",
  "consensus_level": "높음/보통/낮음",
  "top_headlines": [{"headline":"","signal_tag":"","impact":"높음/보통/낮음"}],
  "volatility_index": {"vkospi":"","vix":"","fear_greed":"","level":"","interpretation":""},
  "retail_reversal_signal": {"retail_behavior":"","contrarian_implication":"","reliability":""},
  "outflows": [{"zone":"","reason":"","severity":"","data_point":""}],
  "inflows":  [{"zone":"","reason":"","momentum":"","data_point":""}],
  "neutral_waiting": [{"zone":"","catalyst_needed":""}],
  "hidden_signals": [{"signal":"","implication":"","confidence":""}],
  "korea_focus": {"krw_usd":"","kospi_flow":"","sk_hynix":"","samsung":"","assessment":""},
  "counterarguments": [{"against":"","because":"","risk_level":""}],
  "thesis_killers": [{"event":"","timeframe":"","confirms_if":"","invalidates_if":""}],
  "tail_risks": [],
  "agent_consensus": {"agreed":[],"disputed":[]},
  "meta_improvement": {"missed_last_time":"","accuracy_review":"","reweighting":"","aria_version":""},
  "tomorrow_setup": "",
  "one_line_summary": "",
  "actionable_watch": []
}"""

def agent_reporter(hunter, analyst, devil, memory, accuracy={}, mode="MORNING"):
    console.print("\n[bold green]Agent 4 - REPORTER [" + mode + "][/bold green]")
    past_ctx = ""
    if memory:
        past_ctx = "\n\nPast:\n" + json.dumps(memory[-2:], ensure_ascii=False)
    acc_ctx = ""
    if accuracy.get("total", 0) > 0:
        total_acc = round(accuracy["correct"] / accuracy["total"] * 100, 1)
        acc_ctx   = "\n\nAccuracy: " + str(total_acc) + "%"
        if accuracy.get("weak_areas"):
            acc_ctx += " Weak: " + ", ".join(accuracy["weak_areas"])

    payload = {
        "mode":    mode,
        "hunter":  hunter.get("market_snapshot", {}),
        "analyst": analyst,
        "devil":   devil,
    }
    raw = call_api(
        REPORTER_SYSTEM,
        "Mode: " + mode + "\nData:\n" + json.dumps(payload, ensure_ascii=False) + past_ctx + acc_ctx + "\n\nReturn JSON.",
        model=MODEL_REPORTER, max_tokens=4000
    )
    result = parse_json(raw)
    result["mode"] = mode
    console.print("  [green]Done: " + str(result.get("market_regime","")) + " / consensus: " + str(result.get("consensus_level","")) + "[/green]")
    return result


# ── 터미널 출력 ────────────────────────────────────────────────────────────────
def print_report(report, run_n):
    regime     = report.get("market_regime", "?")
    mode       = report.get("mode", "MORNING")
    mode_label = report.get("mode_label", mode)
    rc = "green" if "선호" in regime else "red" if "회피" in regime else "yellow"

    console.rule("[bold purple]ARIA [" + mode_label + "] #" + str(run_n) + "[/bold purple]")
    console.print(Panel(
        "[bold]" + report.get("one_line_summary","") + "[/bold]",
        title="[" + rc + "]" + regime + "[/" + rc + "]  " + report.get("confidence_overall","") + "  " + report.get("analysis_date",""),
        border_style="purple"
    ))

    tp = report.get("trend_phase","")
    ts = report.get("trend_strategy",{})
    if tp:
        tc = "green" if "상승" in tp else "red" if "하락" in tp else "yellow"
        console.print(Panel(
            "[bold]" + tp + "[/bold]\n\nStrategy: " + ts.get("recommended","") + "\nCaution: " + ts.get("caution",""),
            title="Trend", border_style=tc
        ))

    kr = report.get("korea_focus",{})
    if kr:
        kt = Table(box=box.SIMPLE, show_header=False)
        kt.add_column("", style="dim", width=12)
        kt.add_column("", style="cyan")
        for k, v in [("KRW/USD", kr.get("krw_usd")), ("KOSPI", kr.get("kospi_flow")),
                     ("SK Hynix", kr.get("sk_hynix")), ("Samsung", kr.get("samsung"))]:
            kt.add_row(k, v or "")
        console.print(Panel(kt, title="Korea Market", border_style="cyan"))

    ft = Table(box=box.SIMPLE, show_header=True, header_style="bold", expand=True)
    ft.add_column("Outflow", style="red")
    ft.add_column("Inflow",  style="green")
    out = report.get("outflows",[])
    inp = report.get("inflows",[])
    for i in range(max(len(out), len(inp))):
        oc = "[bold]" + out[i]["zone"] + "[/bold]\n[dim]" + out[i].get("reason","")[:80] + "[/dim]" if i < len(out) else ""
        ic = "[bold]" + inp[i]["zone"] + "[/bold]\n[dim]" + inp[i].get("reason","")[:80] + "[/dim]" if i < len(inp) else ""
        ft.add_row(oc, ic)
    console.print(Panel(ft, title="Capital Flow", border_style="blue"))

    if report.get("tomorrow_setup") and mode in ["EVENING", "DAWN"]:
        console.print(Panel(report["tomorrow_setup"], title="Tomorrow Setup", border_style="yellow"))

    console.rule()


# ── 메인 ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--history", action="store_true")
    parser.add_argument("--review",  type=int, metavar="N")
    args = parser.parse_args()

    memory = load_memory()

    if args.history:
        if not memory:
            console.print("[dim]No saved analyses[/dim]")
            return
        t = Table(title="ARIA History", box=box.ROUNDED)
        t.add_column("Date"); t.add_column("Mode"); t.add_column("Regime"); t.add_column("Summary")
        for m in reversed(memory[-20:]):
            reg = m.get("market_regime","")
            col = "green" if "선호" in reg else "red" if "회피" in reg else "yellow"
            t.add_row(m.get("analysis_date",""), m.get("mode",""), "[" + col + "]" + reg + "[/" + col + "]", m.get("one_line_summary","")[:40])
        console.print(t)
        return

    today = now_kst().strftime("%Y-%m-%d")
    console.print(Panel(
        "[bold]ARIA [" + MODE + "] Analysis Start[/bold]\nHunter -> Analyst -> Devil -> Reporter",
        border_style="purple"
    ))

    try:
        from aria_telegram import send_start_notification, send_report, send_error
        from aria_verifier import run_verification
        from aria_sentiment import run_sentiment
        from aria_portfolio import run_portfolio
        from aria_rotation import run_rotation

        # 교훈 로드 (아침 분석에만 주입)
        lessons_prompt = ""
        if MODE == "MORNING":
            try:
                from aria_lessons import build_lessons_prompt
                lessons_prompt = build_lessons_prompt()
                if lessons_prompt:
                    console.print("[dim]Lessons injected: " + str(lessons_prompt.count("\n")) + " lines[/dim]")
            except ImportError:
                pass

        # 새벽 모드: 어제 분석 돌아보고 교훈 추출
        if MODE == "DAWN":
            try:
                from aria_lessons import extract_dawn_lessons
                todays = get_todays_analyses()
                if todays:
                    extract_dawn_lessons(todays, "market outcomes today")
            except ImportError:
                pass

        # 아침 모드: 어제 예측 채점
        if MODE == "MORNING":
            print("\n=== Verifying yesterday predictions ===")
            accuracy = run_verification()
        else:
            accuracy = {}

        send_start_notification()

        hunter  = agent_hunter(today, MODE)
        analyst = agent_analyst(hunter, MODE, lessons_prompt)
        devil   = agent_devil(analyst, memory, MODE)
        report  = agent_reporter(hunter, analyst, devil, memory, accuracy, MODE)

        print_report(report, len(memory) + 1)
        send_report(report, len(memory) + 1)

        run_sentiment(report)
        run_rotation(report)
        run_portfolio(report)

        save_memory(memory, report)
        path = save_report(report)
        console.print("[dim]Saved: " + str(path) + "[/dim]")

    except Exception as e:
        console.print("[bold red]Error: " + str(e) + "[/bold red]")
        try:
            send_error(str(e))
        except:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
