"""
aria_agents.py — ARIA 4-Agent Pipeline
Hunter → Analyst → Devil → Reporter
"""
import os
import sys
import json
import re
from datetime import datetime, timezone, timedelta

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

import anthropic
from rich.console import Console

KST     = timezone(timedelta(hours=9))
API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

MODEL_HUNTER        = "claude-haiku-4-5-20251001"
MODEL_ANALYST       = "claude-sonnet-4-6"
MODEL_DEVIL         = "claude-sonnet-4-6"
MODEL_REPORTER_FULL = "claude-opus-4-6"    # MORNING 풀 리포트 전용
MODEL_REPORTER_LITE = "claude-sonnet-4-6"  # AFTERNOON/EVENING/DAWN — 비용 40% 절감

console = Console()
client  = anthropic.Anthropic(api_key=API_KEY)


# ── 공통 유틸 ──────────────────────────────────────────────────────────────────
def parse_json(text: str) -> dict:
    raw = re.sub(r"```json|```", "", text).strip()
    m   = re.search(r"\{[\s\S]*\}", raw)
    if not m:
        raise ValueError("JSON not found:\n" + text[:300])
    s = m.group()
    s = re.sub(r",\s*([}\]])", r"\1", s)
    s += "]" * (s.count("[") - s.count("]"))
    s += "}" * (s.count("{") - s.count("}"))
    return json.loads(s)


def call_api(system: str, user: str, use_search=False,
             model=MODEL_ANALYST, max_tokens=2000) -> str:
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


def get_mode_context(mode: str, lessons_prompt: str = "") -> str:
    base = lessons_prompt
    contexts = {
        "MORNING": (
            "You are ARIA in MORNING mode (07:30 KST). "
            "This is the PRIMARY daily analysis. Set the baseline for today. "
            "Do a FULL comprehensive analysis. "
            "Pay special attention to overnight US market moves.\n"
        ),
        "AFTERNOON": (
            "You are ARIA in AFTERNOON mode (14:30 KST). "
            "Focus ONLY on what CHANGED since this morning. "
            "Do NOT repeat the morning analysis. Highlight intraday reversals and new news.\n"
        ),
        "EVENING": (
            "You are ARIA in EVENING mode (20:30 KST). "
            "Summarize what actually happened today. "
            "What did morning analysis get right/wrong? "
            "What is the setup for tomorrow?\n"
        ),
        "DAWN": (
            "You are ARIA in DAWN mode (04:30 KST). "
            "Focus on global markets overnight: US close, after-hours, Asian open. "
            "Set the stage for tomorrow morning.\n"
        ),
    }
    return contexts.get(mode, "") + base


# ── Agent 1: Hunter ───────────────────────────────────────────────────────────
HUNTER_SYSTEM = """You are a financial news collection agent. Only collect facts.
Real-time market data is already provided above — use those exact numbers.
Search for additional news context only.
Return ONLY valid JSON. No markdown.
{
  "collected_at": "YYYY-MM-DD HH:MM KST",
  "mode": "",
  "raw_signals": [{"category":"","headline":"","data_point":""}],
  "market_snapshot": {
    "sp500":"","nasdaq":"","kospi":"",
    "krw_usd":"","us_10y":"","vix":""
  },
  "total_signals": 0
}"""

_HUNTER_QUERIES = {
    "MORNING":   [
        "global financial markets major news today",
        "Korea market KOSPI news today",
        "semiconductor AI Nvidia SK Hynix news today",
        "geopolitical risk market impact today",
    ],
    "DAWN":      [
        "US market close results AND Asia market open today",
        "Korea market KOSPI news today",
        "semiconductor AI Nvidia SK Hynix news today",
        "geopolitical risk market impact today",
    ],
    "AFTERNOON": [
        "market intraday reversal midday news today",
        "Korea market KOSPI news today",
        "semiconductor AI Nvidia SK Hynix news today",
        "geopolitical risk market impact today",
    ],
    "EVENING":   [
        "market close summary today what happened",
        "Korea market KOSPI news today",
        "semiconductor AI Nvidia SK Hynix news today",
        "geopolitical risk market impact today",
    ],
}


def agent_hunter(date_str: str, mode: str, market_data: dict = None) -> dict:
    console.print("\n[bold cyan]Agent 1 - HUNTER [" + mode + "][/bold cyan]")

    market_ctx = ""
    if market_data:
        try:
            from aria_data import format_for_hunter
            market_ctx = format_for_hunter(market_data)
        except ImportError:
            pass

    queries    = _HUNTER_QUERIES.get(mode, _HUNTER_QUERIES["MORNING"])
    search_str = " AND ".join(queries[:3])

    raw = call_api(
        HUNTER_SYSTEM,
        "Today: " + date_str + " Mode: " + mode + "."
        + market_ctx
        + "\nSearch for additional context: " + search_str + ". Return JSON.",
        use_search=True, model=MODEL_HUNTER, max_tokens=2000,
    )
    result = parse_json(raw)
    result["mode"] = mode

    if market_data:
        snap = result.get("market_snapshot", {})
        for key in ["sp500", "nasdaq", "vix", "kospi", "krw_usd", "us_10y"]:
            if market_data.get(key) and market_data[key] != "N/A":
                snap[key] = market_data[key]
        result["market_snapshot"] = snap

    console.print("  [green]Done: " + str(result.get("total_signals", 0)) + " signals[/green]")
    return result


# ── Agent 2: Analyst ──────────────────────────────────────────────────────────
ANALYST_SYSTEM_BASE = """You are a capital flow analysis agent.
Analyze Hunter data and map capital flows.
Use the real-time market data numbers provided — do not override them with estimates.
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


def agent_analyst(hunter_data: dict, mode: str, lessons_prompt: str = "") -> dict:
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
        model=MODEL_ANALYST, max_tokens=2500,
    )
    result = parse_json(raw)
    console.print("  [green]Done: " + str(result.get("market_regime", ""))
                  + " / " + str(result.get("trend_phase", "")) + "[/green]")
    return result


# ── Agent 3: Devil ────────────────────────────────────────────────────────────
DEVIL_SYSTEM = """You are a counter-argument agent. Challenge the Analyst sharply.
Return ONLY valid JSON in Korean. No markdown.

thesis_killers 작성 필수 규칙:
- event: 반드시 주가/지수로 검증 가능한 구체적 이벤트 (코스피, 나스닥, SK하이닉스, VIX, 원달러 등)
- confirms_if / invalidates_if: 반드시 숫자 기준 포함 (예: "코스피 +1% 이상 유지", "VIX 25 이하", "원달러 1480원 이하")
- "외국인 심리", "시장 분위기", "모멘텀 유지" 같은 추상적 표현 절대 금지
- 하나의 thesis_killer는 하나의 검증 가능한 조건만

{
  "verdict": "동의/부분동의/반대",
  "counterarguments": [{"against":"","because":"","risk_level":"낮음/보통/높음"}],
  "alternative_scenario": {"regime":"","narrative":"","probability":"낮음/보통/높음"},
  "thesis_killers": [{"event":"","timeframe":"","confirms_if":"","invalidates_if":""}],
  "tail_risks": []
}"""


def agent_devil(analyst_data: dict, memory: list, mode: str) -> dict:
    console.print("\n[bold red]Agent 3 - DEVIL [" + mode + "][/bold red]")
    past = ""
    if memory:
        last = memory[-1]
        past = ("\n\nPrior: regime=" + str(last.get("market_regime", ""))
                + " summary=" + str(last.get("one_line_summary", "")))
    slim = {
        "market_regime":      analyst_data.get("market_regime", ""),
        "trend_phase":        analyst_data.get("trend_phase", ""),
        "outflows":           analyst_data.get("outflows", [])[:3],
        "inflows":            analyst_data.get("inflows", [])[:3],
        "hidden_signals":     analyst_data.get("hidden_signals", [])[:3],
        "analyst_confidence": analyst_data.get("analyst_confidence", ""),
    }
    raw = call_api(
        DEVIL_SYSTEM,
        "Analyst:\n" + json.dumps(slim, ensure_ascii=False) + past + "\n\nReturn JSON.",
        model=MODEL_DEVIL, max_tokens=2000,
    )
    result = parse_json(raw)
    console.print("  [green]Done: " + str(result.get("verdict", ""))
                  + " / " + str(len(result.get("counterarguments", []))) + " counters[/green]")
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


def agent_reporter(hunter: dict, analyst: dict, devil: dict,
                   memory: list, accuracy: dict = None, mode: str = "MORNING") -> dict:
    console.print("\n[bold green]Agent 4 - REPORTER [" + mode + "][/bold green]")
    accuracy = accuracy or {}

    past_ctx = ""
    if memory:
        past_ctx = "\n\nPast:\n" + json.dumps(memory[-2:], ensure_ascii=False)

    acc_ctx = ""
    if accuracy.get("total", 0) > 0:
        total_acc = round(accuracy["correct"] / accuracy["total"] * 100, 1)
        acc_ctx   = "\n\nAccuracy: " + str(total_acc) + "%"
        if accuracy.get("weak_areas"):
            acc_ctx += " Weak: " + ", ".join(accuracy["weak_areas"])

    real_data_ctx = ""
    try:
        from aria_data import load_market_data
        md = load_market_data()
        real_data_ctx = (
            "\n\n## CRITICAL: Use these EXACT numbers in volatility_index field"
            "\nDo NOT estimate or say 데이터 미제공:"
            "\n- vix: "        + str(md.get("vix", "N/A"))
            + "\n- fear_greed: " + str(md.get("fear_greed_value", "N/A"))
            + " (" + str(md.get("fear_greed_rating", "")) + ")"
            + "\n- kospi: "    + str(md.get("kospi", "N/A"))
            + "\n- krw_usd: "  + str(md.get("krw_usd", "N/A"))
        )
    except Exception:
        pass

    payload = {
        "mode":    mode,
        "hunter":  hunter.get("market_snapshot", {}),
        "analyst": analyst,
        "devil":   devil,
    }

    # Devil "반대" 판정 시 Reporter에 강화 지시
    devil_override = ""
    if devil.get("verdict") == "반대":
        devil_override = (
            "\n\n## CRITICAL: Devil 에이전트가 \'반대\' 판정을 내렸습니다."
            "\n- confidence_overall은 반드시 \'낮음\'으로 설정하세요."
            "\n- counterarguments를 one_line_summary에 반드시 포함하세요."
            "\n- Analyst 결론을 그대로 따르지 말고 Devil 반론을 우선 반영하세요."
        )

    # MORNING만 Opus, 나머지는 Sonnet (비용 최적화)
    reporter_model = MODEL_REPORTER_FULL if mode == "MORNING" else MODEL_REPORTER_LITE
    max_tok        = 4000 if mode == "MORNING" else 2500

    raw = call_api(
        REPORTER_SYSTEM,
        "Mode: " + mode + "\nData:\n" + json.dumps(payload, ensure_ascii=False)
        + past_ctx + acc_ctx + real_data_ctx + devil_override + "\n\nReturn JSON.",
        model=reporter_model, max_tokens=max_tok,
    )
    result = parse_json(raw)
    result["mode"] = mode
    console.print("  [green]Done: " + str(result.get("market_regime", ""))
                  + " / consensus: " + str(result.get("consensus_level", "")) + "[/green]")
    return result
