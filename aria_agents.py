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
MODEL_REPORTER_FULL = "claude-sonnet-4-6"    # MORNING 리포트 (Opus→Sonnet, 비용 35% 절감)
MODEL_REPORTER_LITE = "claude-sonnet-4-6"  # AFTERNOON/EVENING/DAWN — 비용 40% 절감

console = Console()
client  = anthropic.Anthropic(api_key=API_KEY)


# ── 공통 유틸 ──────────────────────────────────────────────────────────────────
def parse_json(text: str) -> dict:
    """3단계 폴백 JSON 파싱 — Claude 응답이 어떤 형태여도 최대한 복구"""
    # 1. 코드블록 제거
    text = re.sub(r"```json|```", "", text).strip()

    # 2. JSON 객체 추출
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        raise ValueError("JSON not found:\n" + text[:300])
    s = m.group()

    # 3-1. 바로 파싱 시도 (가장 안전)
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass

    # 3-2. trailing comma 제거 후 재시도
    s2 = re.sub(r",\s*([}\]])", r"\1", s)
    try:
        return json.loads(s2)
    except json.JSONDecodeError:
        pass

    # 3-3. 괄호 불균형 보정 후 최종 시도
    s3 = s2
    s3 += "]" * (s3.count("[") - s3.count("]"))
    s3 += "}" * (s3.count("{") - s3.count("}"))
    try:
        return json.loads(s3)
    except json.JSONDecodeError as e:
        print("❌ JSON 파싱 3단계 모두 실패: " + str(e)[:200])
        raise


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
    search_str = " AND ".join(queries[:2])  # 4회 → 2회로 축소 (비용 절감)

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

[백테스트 60거래일 실증 분석 지침 — 2026-01-13~04-11]
- 주식 예측 정확도 51% / VIX 32% / 환율 0%
  → thesis_killers는 주식/반도체만. VIX 단독 검증 금지. 환율 절대 금지.
- 블랙스완 패턴 (DeepSeek·관세·FOMC 쇼크): 3일+ 연속 상승 직후 갑작스런 급락
  → 상승 모멘텀 3일 이상 지속 중이면 반드시 "블랙스완 리스크" 언급
- 급락(-3%+) 다음날 반등 확률 83%: confidence_overall="낮음" + 반등 바이어스 포함
- 급등 후 반전 경고 (60일 신규 발견): FG>=40 구간에서 S&P 이틀 합산 +3% 이상이면 다음날 반전 확률 높음
  대표: 01-24(FG70상승)→DeepSeek-17%, 02-07(FG52상승)→관세충격, 03-21→엔비디아-13%, 04-09(+7.5%)→04-10 -4.3%
  → 이 조건 해당 시 반드시 "급등 후 되돌림 리스크" 반론 필수 포함
- FG<20 정확도 61%: 방향 판단 가능. FG>=20 정확도 17%: "방향 불명확" 인정
- SK하이닉스 나스닥 대비 1.36배 변동폭 → 반도체 예측에 베타 반영
- 원달러 thesis_killers 생성 절대 금지 (60일 정확도 0%)
- VIX thesis_killers 최소화 (60일 정확도 32%)

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
        model=MODEL_ANALYST, max_tokens=1800,  # 2500 → 1800 절감
    )
    result = parse_json(raw)
    console.print("  [green]Done: " + str(result.get("market_regime", ""))
                  + " / " + str(result.get("trend_phase", "")) + "[/green]")
    return result


# ── Agent 3: Devil ────────────────────────────────────────────────────────────
DEVIL_SYSTEM = """You are a counter-argument agent. Challenge the Analyst sharply.
Return ONLY valid JSON in Korean. No markdown.

[60거래일 백테스트 실증 패턴 — 반드시 반영]
- 주식 51% / VIX 32% / 환율 0%: VIX·환율 thesis_killer 절대 금지
- 블랙스완 6회 (DeepSeek·관세·FOMC): 상승 3일+ 후 충격 → 연속 상승 시 가장 강한 반론
- 급락(-3%+) 다음날 반등 83%: 하락 예측에 반드시 "기술적 반등 리스크" 반론
- FG<20 정확도 61%: 방향 확신 가능. FG>=20 정확도 17%: "방향 불명확, 관망" 권고
- SK하이닉스 베타 1.36x: 나스닥 예측치 × 1.4 = 반도체 예상 낙폭
- [핵심] 전날 예측 100% 적중 + 위험선호 → 다음날 급반전 7/60일 발생 (FOMC·DeepSeek·CPI):
  Analyst가 상승 지속 예측하면 반드시 "전날 강세 이후 반전 리스크" 반론 필수
  이런 날은 confidence_overall="낮음" 권고, thesis_killers에 하락 시나리오 반드시 포함


thesis_killers 작성 필수 규칙 (60일 백테스트 기반):
- event: 나스닥/코스피/SK하이닉스/삼성전자/엔비디아 중 하나만 (주가·지수)
- confirms_if / invalidates_if: 반드시 숫자 포함 (예: "나스닥 +1% 이상", "코스피 -1% 이하")
- VIX, 원달러/환율 절대 금지 (VIX 32%, 환율 0% — 노이즈만 추가)
- "외국인 심리", "협상 분위기", "모멘텀 유지" 등 검증 불가 표현 절대 금지
- 급락 후(전일 -3% 이상): 반등 시나리오 thesis_killer 반드시 포함 (83% 적중)
- 블랙스완 경보 (연속 상승 3일+): "갑작스런 충격" 가능성 thesis_killer 추가

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
    data_quality_score = 100
    data_missing = []
    try:
        from aria_data import load_market_data
        md = load_market_data()

        # 데이터 결측 점수 계산
        if md.get("vix", "N/A") == "N/A":          data_missing.append("VIX"); data_quality_score -= 10
        if md.get("kospi", "N/A") == "N/A":         data_missing.append("KOSPI"); data_quality_score -= 15
        if md.get("fear_greed_value","N/A") == "N/A": data_missing.append("Fear&Greed"); data_quality_score -= 10
        if "(alt)" in str(md.get("fear_greed_rating","")): data_missing.append("F&G폴백(암호화폐기반)"); data_quality_score -= 5
        if md.get("krw_usd","N/A") == "N/A":        data_missing.append("환율"); data_quality_score -= 10
        if md.get("nvda","N/A") == "N/A":           data_missing.append("NVDA"); data_quality_score -= 5
        # KIS 항상 미연결
        data_missing.append("KIS미연결(수급데이터없음)"); data_quality_score -= 10
        data_quality_score = max(0, data_quality_score)

        quality_label = "높음" if data_quality_score >= 80 else "보통" if data_quality_score >= 60 else "낮음"

        real_data_ctx = (
            "\n\n## CRITICAL: Use these EXACT numbers in volatility_index field"
            "\nDo NOT estimate or say 데이터 미제공:"
            "\n- vix: "        + str(md.get("vix", "N/A"))
            + "\n- fear_greed: " + str(md.get("fear_greed_value", "N/A"))
            + " (" + str(md.get("fear_greed_rating", "")) + ")"
            + "\n- kospi: "    + str(md.get("kospi", "N/A"))
            + "\n- krw_usd: "  + str(md.get("krw_usd", "N/A"))
            + "\n\n## 데이터 품질: " + str(data_quality_score) + "/100 (" + quality_label + ")"
            + "\n결측: " + (", ".join(data_missing) if data_missing else "없음")
            + "\n→ confidence_overall 결정 시 반드시 반영. 데이터 낮음이면 '낮음'으로."
        )
    except Exception:
        data_quality_score = 50
        data_missing = ["데이터로드실패"]
        quality_label = "낮음"

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

    # 실제 오늘 날짜 주입 (모델이 날짜를 임의로 추측하지 않도록)
    from datetime import datetime as _dt
    today_str = _dt.now(KST).strftime("%Y-%m-%d")
    now_str   = _dt.now(KST).strftime("%H:%M KST")

    raw = call_api(
        REPORTER_SYSTEM,
        "Mode: " + mode + "\nToday: " + today_str + " " + now_str
        + "\n반드시 analysis_date=" + today_str + " analysis_time=" + now_str + " 로 설정"
        + "\nData:\n" + json.dumps(payload, ensure_ascii=False)
        + past_ctx + acc_ctx + real_data_ctx + devil_override + "\n\nReturn JSON.",
        model=reporter_model, max_tokens=max_tok,
    )
    result = parse_json(raw)
    result["mode"] = mode
    console.print("  [green]Done: " + str(result.get("market_regime", ""))
                  + " / consensus: " + str(result.get("consensus_level", "")) + "[/green]")
    return result
