# -*- coding: utf-8 -*-
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import anthropic
import json
import re
import argparse
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

# ── 설정 ──────────────────────────────────────────────────────────────────────
API_KEY     = "여기에_ANTHROPIC_API_키_입력"
MEMORY_FILE = Path("memory.json")
REPORTS_DIR = Path("reports")
KST         = timezone(timedelta(hours=9))
# 에이전트별 모델 분리 (비용 최적화)
MODEL_HUNTER   = "claude-haiku-4-5-20251001"   # 검색 전담 → 가장 저렴
MODEL_ANALYST  = "claude-sonnet-4-6"            # 분석 → 중간급
MODEL_DEVIL    = "claude-sonnet-4-6"            # 반론 → 중간급
MODEL_REPORTER = "claude-opus-4-6"              # 최종 종합 → 고급

console = Console()
client  = anthropic.Anthropic(api_key=API_KEY)


# ── 유틸 ──────────────────────────────────────────────────────────────────────
def now_kst():
    return datetime.now(KST)

def parse_json(text: str) -> dict:
    raw = re.sub(r"```json|```", "", text).strip()
    m = re.search(r"\{[\s\S]*\}", raw)
    if not m:
        raise ValueError("JSON 없음:\n" + text[:400])
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
                    console.print(f"    [dim]검색 [{search_count}]: {q}[/dim]")
            elif t == "content_block_delta":
                d = getattr(ev, "delta", None)
                if d and getattr(d, "type", "") == "text_delta":
                    full += d.text
    return full


# ── Agent 1: Hunter ───────────────────────────────────────────────────────────
HUNTER_SYSTEM = """당신은 금융 뉴스 수집 전담 에이전트입니다.
분석하지 마세요. 사실만 수집하세요.

아래 6개 영역을 web_search로 검색하세요:
1. 글로벌 금융시장 오늘
2. 미국 증시 S&P 나스닥 오늘
3. 원달러 환율 오늘
4. 반도체 엔비디아 SK하이닉스 삼성전자 오늘
5. 미국 국채 금리 오늘
6. 지정학 리스크 오늘

반드시 JSON만 반환. 마크다운 금지.
{
  "collected_at": "YYYY-MM-DD HH:MM KST",
  "raw_signals": [
    {"category": "미국증시|한국증시|환율|금리|원자재|지정학|기업",
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
    console.print(f"\n[bold cyan]Agent 1 · HUNTER — News Collection[/bold cyan]")
    raw = call_api(HUNTER_SYSTEM,
        f"오늘 날짜 {date_str}. 6개 영역 검색 후 JSON 반환.",
        use_search=True, model=MODEL_HUNTER, max_tokens=2000)
    result = parse_json(raw)
    console.print(f"  [green]Done: {result.get('total_signals', 0)} signals[/green]")
    return result


# ── Agent 2: Analyst ──────────────────────────────────────────────────────────
ANALYST_SYSTEM = """당신은 자본 흐름 분석 전담 에이전트입니다.
Hunter가 수집한 데이터를 받아 분석하세요.

반드시 JSON만 반환. 마크다운 금지. 모든 텍스트 한국어.
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
    console.print("\n[bold yellow]Agent 2 · ANALYST — Flow Analysis[/bold yellow]")
    # Hunter: 핵심 시그널만 추려서 넘기기 (컨텍스트 로트 방지)
    slim_hunter = {
        "market_snapshot": hunter_data.get("market_snapshot", {}),
        "raw_signals": hunter_data.get("raw_signals", [])[:15],  # 상위 15개만
        "total_signals": hunter_data.get("total_signals", 0),
    }
    raw = call_api(ANALYST_SYSTEM,
        f"Hunter 데이터:\n{json.dumps(slim_hunter, ensure_ascii=False)}\n\nJSON 반환.",
        model=MODEL_ANALYST, max_tokens=2500)
    result = parse_json(raw)
    console.print(f"  [green]Done: {result.get('market_regime')} / {result.get('trend_phase')}[/green]")
    return result


# ── Agent 3: Devil ────────────────────────────────────────────────────────────
DEVIL_SYSTEM = """당신은 반론 전담 에이전트입니다.
Analyst 결론에 날카롭게 반론하세요.

반드시 JSON만 반환. 마크다운 금지. 모든 텍스트 한국어.
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
    console.print("\n[bold red]Agent 3 · DEVIL — Counter Arguments[/bold red]")
    past = ""
    if memory:
        last = memory[-1]
        past = f"\n\n과거 분석:\n레짐: {last.get('market_regime')}\n요약: {last.get('one_line_summary')}"
    # Devil: Analyst 핵심만 넘기기 (컨텍스트 로트 방지)
    slim_analyst = {
        "market_regime": analyst_data.get("market_regime"),
        "trend_phase": analyst_data.get("trend_phase"),
        "outflows": analyst_data.get("outflows", [])[:3],
        "inflows": analyst_data.get("inflows", [])[:3],
        "hidden_signals": analyst_data.get("hidden_signals", [])[:3],
        "analyst_confidence": analyst_data.get("analyst_confidence"),
    }
    raw = call_api(DEVIL_SYSTEM,
        f"Analyst 분석:\n{json.dumps(slim_analyst, ensure_ascii=False)}{past}\n\nJSON 반환.",
        model=MODEL_DEVIL, max_tokens=2000)
    result = parse_json(raw)
    console.print(f"  [green]Done: {result.get('verdict')} / {len(result.get('counterarguments',[]))} counter args[/green]")
    return result


# ── Agent 4: Reporter ─────────────────────────────────────────────────────────
REPORTER_SYSTEM = """당신은 최종 리포트 작성 전담 에이전트입니다.
Hunter, Analyst, Devil 결과를 종합하세요.

반드시 JSON만 반환. 마크다운 금지. 모든 텍스트 한국어.
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
  "agent_consensus": {
    "agreed": [],
    "disputed": []
  },
  "meta_improvement": {
    "missed_last_time": "",
    "accuracy_review": "",
    "reweighting": "",
    "aria_version": ""
  },
  "one_line_summary": "",
  "actionable_watch": []
}"""

def agent_reporter(hunter: dict, analyst: dict, devil: dict, memory: list) -> dict:
    console.print("\n[bold green]Agent 4 · REPORTER — Final Report[/bold green]")
    past_ctx = ""
    if memory:
        past_ctx = f"\n\n과거 분석:\n{json.dumps(memory[-3:], ensure_ascii=False)}"
    payload = {
        "hunter": hunter.get("market_snapshot", {}),
        "analyst": analyst,
        "devil": devil,
    }
    raw = call_api(REPORTER_SYSTEM,
        f"에이전트 결과:\n{json.dumps(payload, ensure_ascii=False)}{past_ctx}\n\nJSON 반환.",
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

    console.rule(f"[bold purple]ARIA 리포트 #{run_n}[/bold purple]")
    console.print(Panel(
        f"[bold]{report.get('one_line_summary', '')}[/bold]",
        title=f"[{rc}]{regime}[/{rc}]  신뢰도:{report.get('confidence_overall')}  {report.get('analysis_date')}",
        border_style="purple"
    ))

    # 추세 판단
    tp = report.get("trend_phase", "")
    ts = report.get("trend_strategy", {})
    if tp:
        tp_color = "green" if "상승" in tp else "red" if "하락" in tp else "yellow"
        tp_emoji = "📈" if "상승" in tp else "📉" if "하락" in tp else "↔"
        console.print(Panel(
            f"[{tp_color}]{tp_emoji} {tp}[/{tp_color}]\n\n"
            f"[bold]권장 전략:[/bold] {ts.get('recommended','')}\n"
            f"[yellow]주의:[/yellow] {ts.get('caution','')}\n"
            f"[dim]난이도: {ts.get('difficulty','')}[/dim]",
            title="추세 판단 & 전략 권고", border_style=tp_color
        ))

    # 변동성 지수
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
        vt.add_row("공포탐욕", vi.get("fear_greed", "-"))
        vt.add_row("수준", f"[{fg_color}]{fg_level}[/{fg_color}]")
        vt.add_row("해석", vi.get("interpretation", ""))
        console.print(Panel(vt, title="변동성 & 공포탐욕 지수", border_style=fg_color))

    # 개미 역추세
    rrs = report.get("retail_reversal_signal", {})
    if rrs:
        console.print(Panel(
            f"[bold]개인 동향:[/bold] {rrs.get('retail_behavior','')}\n"
            f"[yellow]역추세 해석:[/yellow] {rrs.get('contrarian_implication','')}\n"
            f"[dim]신뢰도: {rrs.get('reliability','')}[/dim]",
            title="개미 역추세 감지", border_style="orange1"
        ))

    # 한국 시장
    kr = report.get("korea_focus", {})
    if kr:
        kt = Table(box=box.SIMPLE, show_header=False)
        kt.add_column("", style="dim", width=12)
        kt.add_column("", style="cyan")
        for k, v in [("원/달러", kr.get("krw_usd")), ("코스피", kr.get("kospi_flow")),
                     ("SK하이닉스", kr.get("sk_hynix")), ("삼성전자", kr.get("samsung")),
                     ("종합 평가", kr.get("assessment"))]:
            kt.add_row(k, v or "")
        console.print(Panel(kt, title="한국 시장", border_style="cyan"))

    # 자금 흐름
    ft = Table(box=box.SIMPLE, show_header=True, header_style="bold", expand=True)
    ft.add_column("▼ 유출", style="red")
    ft.add_column("▲ 유입", style="green")
    out, inp = report.get("outflows", []), report.get("inflows", [])
    for i in range(max(len(out), len(inp))):
        oc = f"[bold]{out[i]['zone']}[/bold]\n[dim]{out[i].get('reason','')[:55]}[/dim]" if i < len(out) else ""
        ic = f"[bold]{inp[i]['zone']}[/bold]\n[dim]{inp[i].get('reason','')[:55]}[/dim]" if i < len(inp) else ""
        ft.add_row(oc, ic)
    console.print(Panel(ft, title="자본 흐름 지도", border_style="blue"))

    # 자기반론
    console.print("\n[bold orange1]자기반론[/bold orange1]")
    for c in report.get("counterarguments", []):
        console.print(f"  • {c.get('against','')}")
        console.print(f"    [dim]{c.get('because','')}[/dim]")

    # 테제 킬러
    console.print("\n[bold bright_blue]48~72h 테제 킬러[/bold bright_blue]")
    for tk in report.get("thesis_killers", []):
        console.print(f"  [{tk.get('timeframe','')}] [bold]{tk.get('event','')}[/bold]")
        console.print(f"    ✓ [green]{tk.get('confirms_if','')}[/green]")
        console.print(f"    ✗ [red]{tk.get('invalidates_if','')}[/red]")

    # 액션 포인트
    if report.get("actionable_watch"):
        console.print("\n[bold yellow]주목 포인트[/bold yellow]")
        for i, a in enumerate(report["actionable_watch"], 1):
            console.print(f"  {i}. {a}")

    # 메타
    meta = report.get("meta_improvement", {})
    if meta:
        console.print(Panel(
            f"[dim]놓친 것: {meta.get('missed_last_time','')}\n"
            f"정확도: {meta.get('accuracy_review','')}\n"
            f"조정: {meta.get('reweighting','')}[/dim]",
            title=f"메타 성장 로그  ARIA {meta.get('aria_version','')}",
            border_style="dim"
        ))
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
            console.print("[dim]저장된 분석 없음[/dim]")
            return
        t = Table(title="ARIA 분석 기록", box=box.ROUNDED)
        t.add_column("날짜"); t.add_column("레짐"); t.add_column("한줄 요약")
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
        console.print(f"[yellow]오늘 분석 이미 있음. 재실행? (y/n)[/yellow] ", end="")
        if input().strip().lower() != "y":
            print_report(existing, len(memory))
            return

    console.print(Panel(
        "[bold]ARIA Analysis Start[/bold]\nHunter -> Analyst -> Devil -> Reporter",
        border_style="purple"
    ))

    try:
        from aria_telegram import send_start_notification, send_report, send_error
        send_start_notification()

        hunter  = agent_hunter(today)
        analyst = agent_analyst(hunter)
        devil   = agent_devil(analyst, memory)
        report  = agent_reporter(hunter, analyst, devil, memory)

        print_report(report, len(memory) + 1)
        send_report(report, len(memory) + 1)

        save_memory(memory, report)
        path = save_report(report)
        console.print(f"[dim]Saved: {path}[/dim]")

    except Exception as e:
        console.print(f"[bold red]오류: {e}[/bold red]")
        sys.exit(1)

if __name__ == "__main__":
    main()
