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

_DEFAULT_HAIKU  = "claude-haiku-4-5-20251001"
_DEFAULT_SONNET = "claude-sonnet-4-6"
MODEL_HUNTER        = os.environ.get("ARIA_MODEL_HUNTER", _DEFAULT_HAIKU)
MODEL_ANALYST       = os.environ.get("ARIA_MODEL",        _DEFAULT_SONNET)
MODEL_DEVIL         = os.environ.get("ARIA_MODEL",        _DEFAULT_SONNET)
MODEL_REPORTER_FULL = os.environ.get("ARIA_MODEL",        _DEFAULT_SONNET)
MODEL_REPORTER_LITE = os.environ.get("ARIA_MODEL_LITE",   _DEFAULT_SONNET)

# ── 토큰 예산 (비용 최적화: 실측 기반 상한 설정)
_TOK = {
    "HUNTER":         2500,   # Hunter는 웹서치 포함 — 2500 실사용에 맞춤
    "ANALYST":        1400,   # 1800 → 1400  (-22%)
    "DEVIL":          1500,   # 2000 → 1500  (-25%)
    "REPORTER_FULL":  3000,   # 4000 → 3000  (-25%) MORNING
    "REPORTER_LITE":  2000,   # 2500 → 2000  (-20%) EVENING/DAWN
}

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





def call_api(system: str, user: str, use_search: bool = False,
             model: str = MODEL_ANALYST, max_tokens: int = 2000,
             max_retries: int = 2) -> str:
    """Anthropic API 호출 — 루프 방식 재시도"""
    import anthropic as _ac
    import time as _t
    kwargs = dict(
        model=model, max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    if use_search:
        kwargs["tools"] = [{"type": "web_search_20250305", "name": "web_search"}]
    _DELAYS = {_ac.InternalServerError: 20, _ac.RateLimitError: 60}
    last_exc = None
    for attempt in range(max_retries):
        full = ""; sc = 0
        try:
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
        except tuple(_DELAYS.keys()) as e:
            last_exc = e
            delay = _DELAYS.get(type(e), 30)
            if attempt < max_retries - 1:
                console.print(f"  [yellow]⚠️ {type(e).__name__} — {delay}s 후 재시도[/yellow]")
                _t.sleep(delay)
    raise last_exc


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
HUNTER_SYSTEM = """You are a financial news collection agent. Your job is to gather as many
DIVERSE, FACTUAL signals as possible — earnings, policy, flows, macro, geopolitics, supply chain.
Do NOT pre-filter by market direction. Collect ALL facts regardless of bullish or bearish implications.
Real-time market data is already provided above — use those exact numbers, do not re-state them as signals.
Search for NEWS and EVENTS that are NOT already in the market data provided.

Return ONLY valid JSON. No markdown.
{
  "collected_at": "YYYY-MM-DD HH:MM KST",
  "mode": "",
  "raw_signals": [
    {
      "category": "earnings|policy|flows|macro|geopolitics|supply_chain|sector|central_bank|korea",
      "headline": "구체적 사실 (숫자 포함 우선)",
      "data_point": "수치·날짜·출처 등 구체 데이터",
      "source_hint": "Reuters|Bloomberg|WSJ|Yonhap 등"
    }
  ],
  "market_snapshot": {"sp500":"","nasdaq":"","kospi":"","krw_usd":"","us_10y":"","vix":""},
  "total_signals": 0
}"""

# ── 모드별 검색 쿼리 — 방향성 없이 사실 수집에 집중
_HUNTER_QUERIES = {
    "MORNING": [
        "US stock market earnings news today",                       # 실적
        "Korea KOSPI foreign investor buying selling today",         # 한국 수급 (핵심)
        "Federal Reserve interest rate inflation data today",        # 연준·매크로
        "semiconductor chip AI supply demand news today",            # 반도체 공급망
        "oil energy geopolitical Middle East news today",            # 에너지·지정학
        "Korea economy trade export data today",                     # 한국 경제 지표
    ],
    "EVENING": [
        "US stock market close results today",
        "Korea KOSPI foreign investor net buy sell today",
        "Federal Reserve Fed officials speech today",
        "semiconductor earnings guidance outlook today",
        "crude oil OPEC supply news today",
        "US economic data jobs inflation today",
    ],
    "DAWN": [
        "US market close Asia market open overnight",
        "Korea KOSPI premarket futures foreign flow",
        "Fed officials statements overnight",
        "semiconductor AI hardware announcements overnight",
        "global macro risk event overnight news",
        "currency FX dollar yen yuan movement today",
    ],
    "AFTERNOON": [
        "US market midday intraday news today",
        "Korea KOSPI afternoon session news",
        "economic data release today results",
        "sector rotation midday capital flow today",
        "geopolitical risk update today",
        "earnings announcement guidance today",
    ],
}


def agent_hunter(date_str: str, mode: str, market_data: dict = None, memory: list = None) -> dict:
    console.print("\n[bold cyan]Agent 1 - HUNTER [" + mode + "][/bold cyan]")

    market_ctx = ""
    if market_data:
        try:
            from aria_data import format_for_hunter
            market_ctx = format_for_hunter(market_data)
        except ImportError:
            pass

    # 모든 쿼리를 개별 검색으로 전달 (2개 제한 제거 — 정보 최대 수집)
    queries = _HUNTER_QUERIES.get(mode, _HUNTER_QUERIES["MORNING"])
    search_instruction = (
        "\n\nSearch these topics SEPARATELY to collect diverse facts:\n"
        + "\n".join(str(i+1) + ". " + q for i, q in enumerate(queries))
        + "\n\nFor each search, extract concrete facts with numbers/dates. "
        "Aim for 15+ diverse signals across different categories."
    )

    raw = call_api(
        HUNTER_SYSTEM,
        "Today: " + date_str + " Mode: " + mode + "."
        + market_ctx
        + search_instruction
        + "\nReturn JSON.",
        use_search=True, model=MODEL_HUNTER, max_tokens=_TOK["HUNTER"],  # _TOK 통일
    )
    result = parse_json(raw)
    result["mode"] = mode

    if market_data:
        snap = result.get("market_snapshot", {})
        for key in ["sp500", "nasdaq", "vix", "kospi", "krw_usd", "us_10y"]:
            if market_data.get(key) and market_data[key] != "N/A":
                snap[key] = market_data[key]
        result["market_snapshot"] = snap

    sig_count = result.get("total_signals", len(result.get("raw_signals", [])))
    console.print("  [green]Done: " + str(sig_count) + " signals[/green]")
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


def agent_analyst(hunter_data: dict, mode: str, lessons_prompt: str = "", memory: list = None) -> dict:
    console.print("\n[bold yellow]Agent 2 - ANALYST [" + mode + "][/bold yellow]")

    # 패턴 힌트 (메모리에서 로컬 계산, 비용 0)
    pattern_hint = ""
    if memory and len(memory) >= 3:
        try:
            from aria_analysis import get_pattern_context
            snap = hunter_data.get("market_snapshot", {})
            # Hunter가 수집한 레짐 추정값이 없으므로 최근 메모리 레짐 기반
            last_regime = memory[-1].get("market_regime", "")
            last_trend  = memory[-1].get("trend_phase", "")
            pattern_hint = get_pattern_context(memory, last_regime, last_trend)
            if pattern_hint:
                console.print("  [dim]" + pattern_hint + "[/dim]")
        except Exception:
            pass

    mode_ctx = get_mode_context(mode, lessons_prompt + ("\n" + pattern_hint if pattern_hint else ""))
    slim = {
        "market_snapshot": hunter_data.get("market_snapshot", {}),
        "raw_signals":     hunter_data.get("raw_signals", [])[:15],  # 8 → 15 (Hunter 확장에 맞춤)
        "mode":            mode,
    }
    raw = call_api(
        mode_ctx + "\n\n" + ANALYST_SYSTEM_BASE,
        "Hunter data:\n" + json.dumps(slim, ensure_ascii=False) + "\n\nReturn JSON.",
        model=MODEL_ANALYST, max_tokens=_TOK["ANALYST"],
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
- [핵심] 전날 예측 100% 적중 + 위험선호 → 다음날 급반전 7/60일: 상승 지속 예측 시 반드시 "반전 리스크" 반론

thesis_killers 작성 필수 규칙:
- event: 나스닥/코스피/SK하이닉스/삼성전자/엔비디아 중 하나만
- confirms_if / invalidates_if: 반드시 구체적 숫자 포함

❌ 절대 금지 (검증 불가 — 이런 표현이 있으면 무조건 거부):
  "모멘텀 유지", "심리 개선", "협상 분위기", "외국인 복귀 기대",
  "추세 지속", "안정화 확인", "투자심리 안정"

✅ 반드시 이 형식으로 (숫자 필수):
  confirms_if: "나스닥 +1% 이상 종가"
  confirms_if: "코스피 5,800pt 이상 유지"
  confirms_if: "SK하이닉스 +2% 이상"
  invalidates_if: "나스닥 -1% 이하"
  invalidates_if: "코스피 5,500pt 이하 이탈"

- VIX·원달러·환율 event 절대 금지
- 급락 후(전일 -3%): 반등 시나리오 thesis_killer 필수
- 연속 상승 3일+: "갑작스런 충격" thesis_killer 필수

{
  "verdict": "동의/부분동의/반대",
  "counterarguments": [{"against":"","because":"","risk_level":"낮음/보통/높음"}],
  "alternative_scenario": {"regime":"","narrative":"","probability":"낮음/보통/높음"},
  "thesis_killers": [{"event":"","timeframe":"","confirms_if":"(숫자포함)","invalidates_if":"(숫자포함)"}],
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
        model=MODEL_DEVIL, max_tokens=_TOK["DEVIL"],
    )
    result = parse_json(raw)

    # ── thesis_killers 후처리: VIX·환율 event 자동 제거 (백테스트 정확도 0~32%)
    _BLOCKED_EVENTS = {"vix", "환율", "원달러", "달러", "달러/원", "금리", "국채"}
    _ALLOWED_EVENTS = {"나스닥", "nasdaq", "코스피", "kospi", "sk하이닉스", "삼성전자", "엔비디아", "s&p", "반도체"}
    filtered_tks = []
    for tk in result.get("thesis_killers", []):
        event_lower = tk.get("event", "").lower()
        # 차단 키워드 포함 또는 허용 키워드 없으면 제거
        if any(b in event_lower for b in _BLOCKED_EVENTS):
            console.print("  [dim]thesis_killer 제거 (VIX/환율): " + tk.get("event", "") + "[/dim]")
            continue
        if not any(a in event_lower for a in _ALLOWED_EVENTS):
            console.print("  [dim]thesis_killer 제거 (검증불가): " + tk.get("event", "") + "[/dim]")
            continue
        filtered_tks.append(tk)
    result["thesis_killers"] = filtered_tks

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
  "one_line_summary": "오늘 시장 핵심 한 문장 (예: '나스닥 +1.2% 반등에도 코스피 외국인 매도 지속, VIX 21로 관망세')",
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
  "actionable_watch": []
}"""


def agent_reporter(hunter: dict, analyst: dict, devil: dict,
                   memory: list, accuracy: dict = None, mode: str = "MORNING") -> dict:
    console.print("\n[bold green]Agent 4 - REPORTER [" + mode + "][/bold green]")
    accuracy = accuracy or {}

    # ── 컴팩트 메모리 컨텍스트 (full JSON 대비 ~90% 토큰 절감)
    past_ctx = ""
    if memory:
        try:
            from aria_analysis import build_compact_history
            past_ctx = "\n\n" + build_compact_history(memory, n=7)
        except ImportError:
            # 폴백: 마지막 1개만 요약
            last = memory[-1]
            past_ctx = "\n\nPrev: " + last.get("analysis_date","") + " " + last.get("market_regime","") + " " + last.get("one_line_summary","")[:40]

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
        if md.get("vix", "N/A") == "N/A":            data_missing.append("VIX"); data_quality_score -= 10
        if md.get("kospi", "N/A") == "N/A":           data_missing.append("KOSPI"); data_quality_score -= 15
        if md.get("fear_greed_value","N/A") == "N/A": data_missing.append("Fear&Greed"); data_quality_score -= 10
        if "(alt)" in str(md.get("fear_greed_rating","")): data_missing.append("F&G폴백"); data_quality_score -= 5
        if md.get("krw_usd","N/A") == "N/A":          data_missing.append("환율"); data_quality_score -= 10
        if md.get("nvda","N/A") == "N/A":             data_missing.append("NVDA"); data_quality_score -= 5
        # 외국인 수급: KRX 실데이터 > EWY 프록시 > 없음
        if md.get("krx_flow_source") == "krx_api":
            pass  # KRX 실데이터 연결됨 — 감점 없음
        elif md.get("ewy","N/A") != "N/A":
            data_missing.append("KRX수급(EWY프록시대체)"); data_quality_score -= 3
        else:
            data_missing.append("외국인수급없음"); data_quality_score -= 10
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

    # MORNING만 full 토큰, 나머지는 lite
    reporter_model = MODEL_REPORTER_FULL if mode == "MORNING" else MODEL_REPORTER_LITE
    max_tok        = _TOK["REPORTER_FULL"] if mode == "MORNING" else _TOK["REPORTER_LITE"]

    # 실제 오늘 날짜 주입 (모델이 날짜를 임의로 추측하지 않도록)
    from datetime import datetime as _dt
    today_str = _dt.now(KST).strftime("%Y-%m-%d")
    now_str   = _dt.now(KST).strftime("%H:%M KST")

    raw = call_api(
        REPORTER_SYSTEM,
        "Mode: " + mode + "\nToday: " + today_str + " " + now_str
        + "\n반드시 analysis_date=" + today_str + " analysis_time=" + now_str + " 로 설정"
        + "\n⚠️ one_line_summary 필수: 오늘 시장 핵심을 30자 이상 한국어 문장으로. 예) '나스닥 +1.2% 반등에도 코스피 외국인 매도 지속, VIX 21로 관망세 유지'"
        + "\nData:\n" + json.dumps(payload, ensure_ascii=False)
        + past_ctx + acc_ctx + real_data_ctx + devil_override + "\n\nReturn JSON.",
        model=reporter_model, max_tokens=max_tok,
    )
    result = parse_json(raw)
    result["mode"] = mode

    # ── one_line_summary 비어있으면 자동 생성 ─────────────────────────────
    if not result.get("one_line_summary", "").strip():
        regime  = result.get("market_regime", "")
        trend   = result.get("trend_phase", "")
        conf    = result.get("confidence_overall", "")
        result["one_line_summary"] = (
            today_str + " " + regime + " / " + trend
            + (" | 신뢰도:" + conf if conf else "")
        )
        console.print("  [yellow]⚠️ one_line_summary 자동 생성됨[/yellow]")

    # ── thesis_killers 품질 후처리 — 숫자 없는 항목 자동 플래그 ──────────
    _VAGUE_KW = {"모멘텀 유지","심리 개선","협상 분위기","외국인 복귀",
                 "추세 지속","안정화 확인","투자심리","분위기 호전"}
    _has_num  = re.compile(r'\d')
    for tk in result.get("thesis_killers", []):
        cf  = tk.get("confirms_if", "")
        inv = tk.get("invalidates_if", "")
        vague = (not _has_num.search(cf) or not _has_num.search(inv) or
                 any(kw in cf for kw in _VAGUE_KW) or any(kw in inv for kw in _VAGUE_KW))
        tk["quality"] = "vague" if vague else "ok"

    console.print("  [green]Done: " + str(result.get("market_regime", ""))
                  + " / consensus: " + str(result.get("consensus_level", "")) + "[/green]")

    # thesis_killers quality 요약 출력
    tks    = result.get("thesis_killers", [])
    ok_n   = sum(1 for t in tks if t.get("quality") == "ok")
    vague_n = sum(1 for t in tks if t.get("quality") == "vague")
    if tks:
        console.print("  [dim]thesis_killers: ok=" + str(ok_n) + " vague=" + str(vague_n) + "[/dim]")

    return result
