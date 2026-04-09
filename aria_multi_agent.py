import os
os.environ["PYTHONIOENCODING"] = "utf-8"
import sys
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

import anthropic
import json
import re
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

# ── 설정 ──────────────────────────────────────────────────────────────────────
API_KEY        = os.environ.get("ANTHROPIC_API_KEY", "")
MEMORY_FILE    = Path("memory.json")
REPORTS_DIR    = Path("reports")
KST            = timezone(timedelta(hours=9))

MODEL_HUNTER   = "claude-haiku-4-5-20251001"
MODEL_ANALYST  = "claude-sonnet-4-6"
MODEL_DEVIL    = "claude-sonnet-4-6"
MODEL_REPORTER = "claude-opus-4-6"

console = Console()
client  = anthropic.Anthropic(api_key=API_KEY)


# ── 유틸 ──────────────────────────────────────────────────────────────────────
def now_kst():
    return datetime.now(KST)

def parse_json(text: str) -> dict:
    raw = re.sub(r"```json|```", "", text).strip()
    m = re.search(r"\{[\s\S]*\}", raw)
    if not m:
        raise ValueError("JSON not found:\n" + text[:400])
    s = m.group()
    s = re.sub(r",\s*([}\]])", r"\1", s)
    opens     = s.count("{") - s.count("}")
    opens_arr = s.count("[") - s.count("]")
    s += "]" * opens_arr + "}" * opens
    return json.loads(s)

def call_api(system: str, user: str, use_search: bool = False, model: str = MODEL_ANALYST, max_tokens: int = 2000) -> str:
    kwargs = dict(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    if use_search:
        kwargs["tools"] = [{"type": "web_search_20250305", "name": "web_search"}]

    full = ""
    search_count = 0
    with client.messages.stream(**kwargs) as s:
        for ev in s:
            t = getattr(ev, "type", "")
            if t == "content_block_start":
                blk = getattr(ev, "content_block", None)
                if blk and getattr(blk, "type", "") == "tool_use":
                    search_count += 1
                    q = getattr(blk, "input", {}).get("query", "")
                    console.print(f"    [dim]Search [{search_count}]: {q}[/dim]")
            elif t == "content_block_delta":
                d = getattr(ev, "delta", None)
                if d and getattr(d, "type", "") == "text_delta":
                    full += d.text
    return full


# ── Agent 1: Hunter ───────────────────────────────────────────────────────────
HUNTER_SYSTEM = """You are a financial news collection agent. Only collect facts, do not analyze.

Search these 6 areas using web_search:
1. Global financial markets today
2. US stock market S&P Nasdaq today
3. Korean won USD exchange rate today
4. Semiconductor Nvidia SK Hynix Samsung today
5. US treasury bond yield today
6. Geopolitical risk today

Return ONLY valid JSON. No markdown.
{
  "collected_at": "YYYY-MM-DD HH:MM KST",
  "raw_signals": [
    {"category": "US_market|KR_market|FX|rates|commodity|geopolitical|corporate",
     "headline": "",
     "data_point": ""}
  ],
  "market_snapshot": {
    "sp500": "", "nasdaq": "", "kospi": "",
    "krw_usd": "", "us_10y": "", "vix": ""
  },
  "total_signals": 0
}"""

def agent_hunter(date_str: str) -> dict:
    console.print("\n[bold cyan]Agent 1 - HUNTER - News Collection[/bold cyan]")
    raw = call_api(HUNTER_SYSTEM,
        f"Today is {date_str}. Search all 6 areas and return JSON.",
        use_search=True, model=MODEL_HUNTER, max_tokens=2000)
    result = parse_json(raw)
    console.print(f"  [green]Done: {result.get('total_signals', 0)} signals collected[/green]")
    return result


# ── Agent 2: Analyst ──────────────────────────────────────────────────────────
ANALYST_SYSTEM = """You are a capital flow analysis agent.
Analyze the data from Hunter and map capital flows.
Return ONLY valid JSON in Korean. No markdown.
{
  "market_regime": "위험선호/위험회피/전환중/혼조",
  "trend_phase": "상승추세/횡보추세/하락추세",
  "trend_strategy": {"recommended": "", "caution": "", "difficulty": "쉬움/보통/어려움"},
  "regime_reason": "",
  "volatility_index": {
    "vkospi": "", "vix": "", "fear_greed": "",
    "level": "극단공포/공포/중립/탐욕/극단탐욕",
    "interpretation": ""
  },
  "retail_reversal_signal": {
    "retail_behavior": "",
    "contrarian_implication": "",
    "reliability": "낮음/보통/높음"
  },
  "outflows": [{"zone":"","reason":"","severity":"높음/보통/낮음","data_point":""}],
  "inflows":  [{"zone":"","reason":"","momentum":"강함/형성중/약함","data_point":""}],
  "neutral_waiting": [{"zone":"","catalyst_needed":""}],
  "hidden_signals": [{"signal":"","implication":"","confidence":"낮음/보통/높음"}],
  "korea_focus": {
    "krw_usd":"","kospi_flow":"","sk_hynix":"","samsung":"","assessment":""
  },
  "analyst_confidence": "낮음/보통/높음"
}"""

def agent_analyst(hunter_data: dict) -> dict:
    console.print("\n[bold yellow]Agent 2 - ANALYST - Flow Analysis[/bold yellow]")
    slim_hunter = {
        "market_snapshot": hunter_data.get("market_snapshot", {}),
        "raw_signals": hunter_data.get("raw_signals", [])[:15],
        "total_signals": hunter_data.get("total_signals", 0),
    }
    raw = call_api(ANALYST_SYSTEM,
        f"Hunter data:\n{json.dumps(slim_hunter, ensure_ascii=False)}\n\nReturn JSON.",
        model=MODEL_ANALYST, max_tokens=2500)
    result = parse_json(raw)
    console.print(f"  [green]Done: {result.get('market_regime')} / {result.get('trend_phase')}[/green]")
    return result


# ── Agent 3: Devil ────────────────────────────────────────────────────────────
DEVIL_SYSTEM = """You are a counter-argument agent. Challenge the Analyst's conclusions sharply.
Return ONLY valid JSON in Korean. No markdown.
{
  "verdict": "동의/부분동의/반대",
  "counterarguments": [
    {"against":"","because":"","risk_level":"낮음/보통/높음"}
  ],
  "alternative_scenario": {
    "regime": "",
    "narrative": "",
    "probability": "낮음/보통/높음"
  },
  "thesis_killers": [
    {"event":"","timeframe":"","confirms_if":"","invalidates_if":""}
  ],
  "tail_risks": []
}"""

def agent_devil(analyst_data: dict, memory: list) -> dict:
    console.print("\n[bold red]Agent 3 - DEVIL - Counter Arguments[/bold red]")
    past = ""
    if memory:
        last = memory[-1]
        past = f"\n\nPrior analysis:\nRegime: {last.get('market_regime')}\nSummary: {last.get('one_line_summary')}"
    slim_analyst = {
        "market_regime": analyst_data.get("market_regime"),
        "trend_phase": analyst_data.get("trend_phase"),
        "outflows": analyst_data.get("outflows", [])[:3],
        "inflows": analyst_data.get("inflows", [])[:3],
        "hidden_signals": analyst_data.get("hidden_signals", [])[:3],
        "analyst_confidence": analyst_data.get("analyst_confidence"),
    }
    raw = call_api(DEVIL_SYSTEM,
        f"Analyst result:\n{json.dumps(slim_analyst, ensure_ascii=False)}{past}\n\nReturn JSON.",
        model=MODEL_DEVIL, max_tokens=2000)
    result = parse_json(raw)
    console.print(f"  [green]Done: {result.get('verdict')} / {len(result.get('counterarguments',[]))} counter args[/green]")
    return result


# ── Agent 4: Reporter ─────────────────────────────────────────────────────────
REPORTER_SYSTEM = """You are the final report agent. Synthesize Hunter, Analyst, Devil results.
Return ONLY valid JSON in Korean. No markdown.
{
  "analysis_date": "YYYY-MM-DD",
  "analysis_time": "HH:MM KST",
  "market_regime": "",
  "trend_phase": "상승추세/횡보추세/하락추세",
  "trend_strategy": {"recommended": "", "caution": "", "difficulty": ""},
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
  "agent_consensus": {"agreed": [], "disputed": []},
  "meta_improvement": {
    "missed_last_time": "",
    "accuracy_review": "",
    "reweighting": "",
    "aria_version": ""
  },
  "one_line_summary": "",
  "actionable_watch": []
}"""

def agent_reporter(hunter: dict, analyst: dict, devil: dict, memory: list, accuracy: dict = {}) -> dict:
    console.print("\n[bold green]Agent 4 - REPORTER - Final Report[/bold green]")
    past_ctx = ""
    if memory:
        past_ctx = f"\n\nPast analyses:\n{json.dumps(memory[-3:], ensure_ascii=False)}"

    # 정확도 정보 추가 (자기보정)
    accuracy_ctx = ""
    if accuracy.get("total", 0) > 0:
        total_acc = round(accuracy["correct"] / accuracy["total"] * 100, 1)
        accuracy_ctx = f"\n\nARIA accuracy stats: {total_acc}% overall"
        if accuracy.get("weak_areas"):
            accuracy_ctx += f"\nWeak areas (be more cautious): {', '.join(accuracy['weak_areas'])}"
        if accuracy.get("strong_areas"):
            accuracy_ctx += f"\nStrong areas: {', '.join(accuracy['strong_areas'])}"
    payload = {
        "hunter": hunter.get("market_snapshot", {}),
        "analyst": analyst,
        "devil": devil,
    }
    raw = call_api(REPORTER_SYSTEM,
        f"Agent results:\n{json.dumps(payload, ensure_ascii=False)}{past_ctx}{accuracy_ctx}\n\nReturn JSON.",
        model=MODEL_REPORTER, max_tokens=4000)
    result = parse_json(raw)
    console.print(f"  [green]Done: {result.get('market_regime')} / consensus: {result.get('consensus_level')}[/green]")
    return result


# ── 메모리 관리 ────────────────────────────────────────────────────────────────
def load_memory() -> list:
    if MEMORY_FILE.exists():
        return json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
    return []

def save_memory(memory: list, analysis: dict):
    memory = [m for m in memory if m.get("analysis_date") != analysis.get("analysis_date")]
    memory = (memory + [analysis])[-90:]
    MEMORY_FILE.write_text(json.dumps(memory, ensure_ascii=False, indent=2), encoding="utf-8")

def save_report(analysis: dict) -> Path:
    REPORTS_DIR.mkdir(exist_ok=True)
    date = analysis.get("analysis_date", now_kst().strftime("%Y-%m-%d"))
    path = REPORTS_DIR / f"{date}.json"
    path.write_text(json.dumps(analysis, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


# ── 터미널 출력 ────────────────────────────────────────────────────────────────
def print_report(report: dict, run_n: int):
    regime = report.get("market_regime", "?")
    rc = "green" if "선호" in regime else "red" if "회피" in regime else "yellow"

    console.print(Panel(
        f"[bold]{report.get('one_line_summary', '')}[/bold]",
        title=f"[{rc}]{regime}[/{rc}]  {report.get('confidence_overall')}  {report.get('analysis_date')}",
        border_style="purple"
    ))

    tp = report.get("trend_phase", "")
    ts = report.get("trend_strategy", {})
    if tp:
        tp_color = "green" if "상승" in tp else "red" if "하락" in tp else "yellow"
        console.print(Panel(
            f"[bold]{tp}[/bold]\n\n"
            f"Strategy: {ts.get('recommended','')}\n"
            f"Caution: {ts.get('caution','')}\n"
            f"Difficulty: {ts.get('difficulty','')}",
            title="Trend Analysis", border_style=tp_color
        ))

    vi = report.get("volatility_index", {})
    if vi:
        fg_level = vi.get("level", "")
        fg_color = "red" if "극단공포" in fg_level else "orange1" if "공포" in fg_level else \
                   "green" if "탐욕" in fg_level else "yellow"
        vt = Table(box=box.SIMPLE, show_header=False)
        vt.add_column("", style="dim", width=14)
        vt.add_column("")
        vt.add_row("VKOSPI", vi.get("vkospi", "-"))
        vt.add_row("VIX", vi.get("vix", "-"))
        vt.add_row("Fear/Greed", vi.get("fear_greed", "-"))
        vt.add_row("Level", f"[{fg_color}]{fg_level}[/{fg_color}]")
        console.print(Panel(vt, title="Volatility Index", border_style=fg_color))

    kr = report.get("korea_focus", {})
    if kr:
        kt = Table(box=box.SIMPLE, show_header=False)
        kt.add_column("", style="dim", width=12)
        kt.add_column("", style="cyan")
        for k, v in [("KRW/USD", kr.get("krw_usd")), ("KOSPI", kr.get("kospi_flow")),
                     ("SK Hynix", kr.get("sk_hynix")), ("Samsung", kr.get("samsung")),
                     ("Assessment", kr.get("assessment"))]:
            kt.add_row(k, v or "")
        console.print(Panel(kt, title="Korea Market", border_style="cyan"))

    ft = Table(box=box.SIMPLE, show_header=True, header_style="bold", expand=True)
    ft.add_column("Outflow", style="red")
    ft.add_column("Inflow", style="green")
    out, inp = report.get("outflows", []), report.get("inflows", [])
    for i in range(max(len(out), len(inp))):
        oc = f"[bold]{out[i]['zone']}[/bold]\n[dim]{out[i].get('reason','')[:55]}[/dim]" if i < len(out) else ""
        ic = f"[bold]{inp[i]['zone']}[/bold]\n[dim]{inp[i].get('reason','')[:55]}[/dim]" if i < len(inp) else ""
        ft.add_row(oc, ic)
    console.print(Panel(ft, title="Capital Flow Map", border_style="blue"))

    if report.get("actionable_watch"):
        console.print("\n[bold yellow]Action Points[/bold yellow]")
        for i, a in enumerate(report["actionable_watch"], 1):
            console.print(f"  {i}. {a}")

    console.rule()


# ── 메인 ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--history", action="store_true")
    parser.add_argument("--review", type=int, metavar="N")
    args = parser.parse_args()

    memory = load_memory()

    if args.history:
        if not memory:
            console.print("[dim]No saved analyses[/dim]")
            return
        t = Table(title="ARIA Analysis History", box=box.ROUNDED)
        t.add_column("Date"); t.add_column("Regime"); t.add_column("Summary")
        for m in reversed(memory[-20:]):
            reg = m.get("market_regime", "")
            col = "green" if "선호" in reg else "red" if "회피" in reg else "yellow"
            t.add_row(m.get("analysis_date",""), f"[{col}]{reg}[/{col}]",
                      m.get("one_line_summary","")[:50])
        console.print(t)
        return

    today = now_kst().strftime("%Y-%m-%d")
    existing = next((m for m in memory if m.get("analysis_date") == today), None)
    if existing:
        console.print(f"[yellow]Analysis already exists for today. Re-run? (y/n)[/yellow] ", end="")
        if input().strip().lower() != "y":
            print_report(existing, len(memory))
            return

    console.print(Panel(
        "[bold]ARIA Analysis Start[/bold]\nHunter -> Analyst -> Devil -> Reporter",
        border_style="purple"
    ))

    try:
        from aria_telegram import send_start_notification, send_report, send_error
        from aria_verifier import run_verification

        # 1. 어제 예측 먼저 채점
        print("\n=== Verifying yesterday predictions ===")
        accuracy = run_verification()

        send_start_notification()

        hunter  = agent_hunter(today)
        analyst = agent_analyst(hunter)
        devil   = agent_devil(analyst, memory)
        report  = agent_reporter(hunter, analyst, devil, memory, accuracy)

        print_report(report, len(memory) + 1)
        send_report(report, len(memory) + 1)

        save_memory(memory, report)
        path = save_report(report)
        console.print(f"[dim]Saved: {path}[/dim]")

    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        sys.exit(1)

if __name__ == "__main__":
    main()
