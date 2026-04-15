"""
jackal_scanner.py
Jackal Scanner — Analyst → Devil → Final 3단계 타점 분석

흐름:
  1. 데이터 수집 (yfinance + FRED + KRX + FSC + ARIA 파일)
  2. Analyst (Haiku): 매수 근거 구성
  3. Devil   (Haiku): Analyst 반박 + ARIA Thesis Killer 체크
  4. Final   판단:
       둘 다 매수  → 강한 신호 (가중 합산)
       엇갈림      → 점수 낮춤
       둘 다 반대  → 알림 없음
  5. 결과 저장 (Evolution 학습용 — 신호·레짐·Devil 정확도 포함)
"""

import os
import sys
import json
import re
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx
from anthropic import Anthropic

from jackal_market_data import fetch_all, fetch_technicals

os.environ["PYTHONIOENCODING"] = "utf-8"
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

log = logging.getLogger("jackal_scanner")

KST   = timezone(timedelta(hours=9))
_BASE = Path(__file__).parent

SCAN_LOG_FILE      = _BASE / "scan_log.json"
COOLDOWN_FILE      = _BASE / "scan_cooldown.json"
WEIGHTS_FILE       = _BASE / "jackal_weights.json"
RECOMMEND_LOG_FILE = _BASE / "recommendation_log.json"
JACKAL_WATCHLIST   = Path("data") / "jackal_watchlist.json"   # ARIA가 읽음
JACKAL_NEWS_FILE   = Path("data") / "jackal_news.json"        # ARIA가 씀, Jackal이 읽음
JACKAL_SHADOW_LOG  = _BASE / "jackal_shadow_log.json"         # 스킵된 신호 별도 추적 (ARIA accuracy와 분리)

# ARIA 데이터 파일 (읽기만, 의존성 없음)
ARIA_BASELINE  = Path("data") / "morning_baseline.json"
ARIA_SENTIMENT = Path("data") / "sentiment.json"
ARIA_ROTATION  = Path("data") / "rotation.json"
PORTFOLIO_FILE = Path("data") / "portfolio.json"

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
MODEL_H          = os.environ.get("SUBAGENT_MODEL", "claude-haiku-4-5-20251001")


def _load_portfolio() -> dict:
    """
    data/portfolio.json 에서 포트폴리오 로드.
    ticker_yf (yfinance 형식) 키로 딕셔너리 구성.
    현금 등 ticker_yf 없는 항목은 스캔 제외.
    """
    # 기본값 — portfolio.json 없을 때 사용, asset_type 포함
    default = {
        "NVDA":      {"name": "엔비디아",   "avg_cost": 182.99, "market": "US", "currency": "$", "portfolio": True, "asset_type": "stock"},
        "AVGO":      {"name": "브로드컴",   "avg_cost": None,   "market": "US", "currency": "$", "portfolio": True, "asset_type": "stock"},
        # SCHD는 etf_broad_dividend → 기본값도 스캔 제외 반영
        "000660.KS": {"name": "SK하이닉스", "avg_cost": None,   "market": "KR", "currency": "₩", "portfolio": True, "asset_type": "stock"},
        "005930.KS": {"name": "삼성전자",   "avg_cost": None,   "market": "KR", "currency": "₩", "portfolio": True, "asset_type": "stock"},
        "035720.KS": {"name": "카카오",     "avg_cost": None,   "market": "KR", "currency": "₩", "portfolio": True, "asset_type": "stock"},
    }
    if not PORTFOLIO_FILE.exists():
        return default
    try:
        data   = json.loads(PORTFOLIO_FILE.read_text(encoding="utf-8"))
        result = {}
        for h in data.get("holdings", []):
            yf_ticker = h.get("ticker_yf")
            if not yf_ticker:          # 현금 등 yfinance 없는 항목 제외
                continue
            # jackal_scan: false → 스캔 제외 (배당형 ETF 등)
            if h.get("jackal_scan", True) is False:
                log.info(f"   {yf_ticker} 스캔 제외 (jackal_scan=false)")
                continue
            market     = h.get("market", "US")
            asset_type = h.get("asset_type", "stock")

            # asset_type 기반 jackal_scan 기본값 결정
            # etf_broad_dividend → 구조적으로 기술지표 미적합 → 기본 false
            # 명시된 jackal_scan 필드가 있으면 그것이 우선
            if "jackal_scan" not in h:
                default_scan = asset_type not in ("etf_broad_dividend", "cash")
            else:
                default_scan = h["jackal_scan"]

            if not default_scan:
                log.info(f"   {yf_ticker} 스캔 제외 (asset_type={asset_type})")
                continue

            result[yf_ticker] = {
                "name":       h.get("name", yf_ticker),
                "avg_cost":   h.get("avg_cost"),
                "market":     market,
                "currency":   h.get("currency", "$" if market == "US" else "₩"),
                "portfolio":  True,
                "asset_type": asset_type,
            }
        return result if result else default
    except Exception as e:
        log.warning(f"portfolio.json 로드 실패: {e} — 기본값 사용")
        return default

ALERT_THRESHOLD = 65
STRONG_THRESHOLD = 78
COOLDOWN_HOURS  = 4


# ══════════════════════════════════════════════════════════════════
# 시장 개장 여부
# ══════════════════════════════════════════════════════════════════

def _is_us_open() -> bool:
    from datetime import time as t
    now = datetime.now(timezone(timedelta(hours=-5)))
    return now.weekday() < 5 and t(9, 30) <= now.time() <= t(16, 0)

def _is_kr_open() -> bool:
    from datetime import time as t
    now = datetime.now(KST)
    return now.weekday() < 5 and t(9, 0) <= now.time() <= t(15, 30)


# ══════════════════════════════════════════════════════════════════
# ARIA 컨텍스트 로딩 (파일 읽기만)
# ══════════════════════════════════════════════════════════════════

def _load_aria_context() -> dict:
    """
    ARIA가 생성한 파일들을 읽어 시장 맥락 구성.
    파일 없으면 빈 값 반환 — ARIA에 의존하지 않음.
    """
    ctx = {
        "regime":        "",
        "trend":         "",
        "confidence":    "",
        "one_line":      "",
        "thesis_killers": [],
        "key_inflows":   [],
        "key_outflows":  [],
        "sentiment_score": 50,
        "sentiment_level": "중립",
        "top_sector":    "",
        "bottom_sector": "",
    }

    # morning_baseline.json
    try:
        if ARIA_BASELINE.exists():
            b = json.loads(ARIA_BASELINE.read_text(encoding="utf-8"))
            ctx["regime"]     = b.get("market_regime", "")
            ctx["trend"]      = b.get("trend_phase", "")
            ctx["confidence"] = b.get("confidence", "")
            ctx["one_line"]   = b.get("one_line_summary", "")
            ctx["thesis_killers"] = b.get("thesis_killers", [])
            ctx["key_inflows"]    = [i.get("zone","") for i in b.get("key_inflows", [])[:3]]
            ctx["key_outflows"]   = [o.get("zone","") for o in b.get("key_outflows", [])[:3]]
    except Exception:
        pass

    # sentiment.json
    try:
        if ARIA_SENTIMENT.exists():
            s = json.loads(ARIA_SENTIMENT.read_text(encoding="utf-8"))
            cur = s.get("current", {})
            ctx["sentiment_score"] = cur.get("score", 50)
            ctx["sentiment_level"] = cur.get("level", "중립")
    except Exception:
        pass

    # rotation.json
    try:
        if ARIA_ROTATION.exists():
            r = json.loads(ARIA_ROTATION.read_text(encoding="utf-8"))
            ranking = r.get("ranking", [])
            if ranking:
                ctx["top_sector"]    = ranking[0][0] if ranking else ""
                ctx["bottom_sector"] = ranking[-1][0] if ranking else ""
            sig = r.get("rotation_signal", {})
            ctx["rotation_from"] = sig.get("from", "")
            ctx["rotation_to"]   = sig.get("to", "")
    except Exception:
        pass

    return ctx


def _load_weights() -> dict:
    try:
        if WEIGHTS_FILE.exists():
            return json.loads(WEIGHTS_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


# ══════════════════════════════════════════════════════════════════
# Agent 1: Analyst — 매수 근거 구성
# ══════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════
# 신호 품질 사전 평가 (백테스트 기반, Claude 호출 전 실행)
# ══════════════════════════════════════════════════════════════════

# 규칙 레지스트리 — 도입 근거/검토 기준 명시 (Doc3: 규칙 폐기 조건)
_RULE_REGISTRY = {
    # 각 규칙에 min_accuracy 추가 — 해당 건수 이상에서 정확도 미달 시 자동 비활성화
    # Evolution Engine이 이 메타데이터를 읽어 규칙 상태를 평가
    "sector_rebound_base":   {
        "introduced": "2026-04", "basis": "backtest 93.1%",
        "review_after_n": 50,  "min_accuracy": 0.75, "active": True,
    },
    "volume_climax_base":    {
        "introduced": "2026-04", "basis": "backtest 80.0%",
        "review_after_n": 20,  "min_accuracy": 0.65, "active": True,
    },
    "ma_support_solo_pen":   {
        "introduced": "2026-04", "basis": "backtest 55.6% → 단독 차단",
        "review_after_n": 30,  "min_accuracy": None, "active": True,  # 페널티라 정확도 기준 없음
    },
    "rebound_cap":           {
        "introduced": "2026-04", "basis": "anti-stacking",
        "review_after_n": 50,  "min_accuracy": None, "active": True,
    },
    "crash_rebound_pattern": {
        "introduced": "2026-04", "basis": "3/31~4/8 검증",
        "review_after_n": 30,  "min_accuracy": 0.70, "active": True,
    },
    "vix_gating":            {
        "introduced": "2026-04", "basis": "ARIA중복방지",
        "review_after_n": 20,  "min_accuracy": None, "active": True,
    },
    "heuristic_gate":        {
        "introduced": "2026-04", "basis": "이벤트-데이 품질 하락",
        "review_after_n": 30,  "min_accuracy": None, "active": True,
    },
}


def _check_rule_auto_disable(rule_name: str, recent_accuracy: float, sample_n: int) -> bool:
    """
    규칙 자동 폐기 검사.
    min_accuracy 미달 + review_after_n 이상 샘플 → active=False 권고 반환.
    실제 비활성화는 Evolution Engine이 담당.
    """
    rule = _RULE_REGISTRY.get(rule_name, {})
    if not rule.get("active", True):
        return False   # 이미 비활성
    min_acc = rule.get("min_accuracy")
    review_n = rule.get("review_after_n", 50)
    if min_acc is None or sample_n < review_n:
        return False   # 기준 없음 or 샘플 부족
    if recent_accuracy < min_acc:
        log.warning(
            f"  ⚠️ RULE AUTO-DISABLE 권고: {rule_name} "
            f"정확도 {recent_accuracy:.1%} < 기준 {min_acc:.1%} "
            f"(n={sample_n}/{review_n})"
        )
        return True    # Evolution이 처리하도록 True 반환
    return False

# ── signal_family 분류 테이블 ────────────────────────────────────
# Doc1 반박: 분류 기준이 코드에 없으면 새 신호 추가 시 일관성 깨짐
# 규칙: rebound 계열 신호가 1개라도 있으면 crash_rebound family
_CRASH_REBOUND_SIGNALS = frozenset({"sector_rebound", "volume_climax",
                                        "52w_low_zone",
                                        "vol_accumulation"})
# rsi_divergence: 백테스트 조합 50.0% → crash_rebound 아님, general로 강등
_MA_SUPPORT_SOLO       = frozenset({"ma_support"})
_MA_SUPPORT_WEAK       = frozenset({"ma_support", "momentum_dip"})

def _get_signal_family(signals: list) -> str:
    """
    신호 목록에서 signal_family를 결정.
    우선순위: crash_rebound > ma_support_solo/weak > general
    Doc1: 복합 신호는 상위 family 적용 (예: bb_touch+sector_rebound → crash_rebound)
    """
    sig = set(signals)
    if sig & _CRASH_REBOUND_SIGNALS:        # rebound 계열 1개라도 → crash_rebound
        return "crash_rebound"
    if sig == _MA_SUPPORT_SOLO:             # ma_support 단독
        return "ma_support_solo"
    if sig == _MA_SUPPORT_WEAK:             # ma_support + momentum_dip만
        return "ma_support_weak"
    return "general"


def _load_schd_regime_signal() -> float:
    """
    SCHD는 Jackal 스캔 제외(jackal_scan:false) 유지.
    방어 자산 레짐 지표로만 활용 — 5일 -3% 이하 시 전체 confidence -5.
    Doc7/9 부분 수용: 기각 유지 + 새 용도 추가, 충돌 없음.
    """
    try:
        import yfinance as _yf
        df = _yf.Ticker("SCHD").history(period="10d", interval="1d")
        if df.empty or len(df) < 5:
            return 0.0
        change_5d = (float(df["Close"].iloc[-1]) - float(df["Close"].iloc[-5]))                     / float(df["Close"].iloc[-5]) * 100
        if change_5d < -3.0:
            log.info(f"  ⚠️ SCHD 5일 {change_5d:.1f}% 하락 → 레짐 지표 -5")
            return -5.0
        return 0.0
    except Exception:
        return 0.0


def _load_pcr_from_aria() -> float:
    """ARIA가 수집한 PCR(Put/Call Ratio) 로드 — Jackal 품질 평가에 활용."""
    try:
        from pathlib import Path
        import json
        cache = Path("data/aria_market_data.json")
        if not cache.exists():
            return 0.0
        md = json.loads(cache.read_text(encoding="utf-8"))
        # ARIA 수집 구조: prices.pcr_avg 또는 put_call.avg
        pcr = (
            md.get("prices", {}).get("pcr_avg")
            or md.get("put_call", {}).get("avg")
            or md.get("pcr_avg")
        )
        return float(pcr) if pcr else 0.0
    except Exception:
        return 0.0


def _get_vix_from_cache() -> float:
    """ARIA가 수집한 시장 데이터에서 VIX 추출."""
    try:
        from pathlib import Path
        import json
        cache = Path("data/aria_market_data.json")
        if cache.exists():
            md = json.loads(cache.read_text(encoding="utf-8"))
            return float(
                md.get("fred", {}).get("vixcls", 0)
                or md.get("prices", {}).get("^VIX", {}).get("price", 0)
                or 0
            )
    except Exception:
        pass
    return 0.0


def _calc_signal_quality(signals: list, tech: dict, aria: dict,
                          ticker: str = "", weights: dict = None) -> dict:
    """
    발동된 기술 신호 조합의 품질을 0~100점으로 평가.

    개선 사항 (반박 문서 반영):
      - signal_family별 스킵 임계값 (ma_support:50 / 일반:45 / crash_rebound:40)
      - ticker_accuracy 연속 함수 + 표본수 가중치 (절벽 효과 제거)
      - rebound 계열 보너스 총합 상한 (+12 cap, 스태킹 방지)
      - VIX → 점수 가산 아닌 gating role로 전환 (ARIA 중복 방지)
      - 규칙 레지스트리로 도입 근거 추적

    Returns dict:
      quality_score  : 0~100
      quality_label  : 최강/강/보통/약
      reasons        : 근거 리스트
      skip           : True면 Claude 호출 스킵
      skip_threshold : 적용된 임계값
      analyst_adj    : Analyst 점수 보정 (+0 or +5)
      final_adj      : final_score 보정
      vix_used       : 실제 사용된 VIX값
      rebound_bonus  : 반등 계열 누적 보너스 (상한 적용 전)
    """
    if weights is None:
        weights = {}

    sig    = set(signals)
    score  = 50
    reasons: list = []

    # ── A. 신호 조합 품질 (백테스트 정확도 기반) ─────────────────

    # 기존 검증된 신호
    if "sector_rebound" in sig:
        score += 20; reasons.append("sector_rebound(93%)+20")
    if "volume_climax" in sig:
        score += 15; reasons.append("volume_climax(80%)+15")
    # 백테스트 재검증: bb_touch 97.2%, bb+rsi 조합 최강
    if "bb_touch" in sig and "rsi_oversold" in sig:
        score += 16; reasons.append("BB+RSI조합(97%+88%)+16")
    elif "bb_touch" in sig:
        score += 12; reasons.append("BB하단(97%)+12")
    # rsi_oversold 88.2% 재검증 (이전 65.4%에서 대폭 상승)
    if "rsi_oversold" in sig and "sector_rebound" not in sig:
        score += 9;  reasons.append("RSI과매도(88%)+9")
    if "momentum_dip" in sig and len(sig) > 1:
        score += 5;  reasons.append("급락+복수신호+5")

    # 신규 신호 (미검증 — 초기 보너스 보수적)
    # rsi_divergence: 백테스트 스윙 52.2% (단독 57%, 조합 50%)
    # 1일 정확도 30.4% = 반대로 움직이는 경향
    # → 단독 발동 시 페널티, 조합 시만 소폭 인정
    if "rsi_divergence" in sig:
        if sig == {"rsi_divergence"}:
            score -= 8    # 단독: 57.1% (약한 노이즈)
            reasons.append("RSI다이버전스단독(57%)-8")
        elif "momentum_dip" in sig and "vol_accumulation" not in sig:
            score -= 12   # momentum_dip+rsi_div = 40% 최악 조합
            reasons.append("다이버전스+momentum_dip(40%)-12")
        elif "vol_accumulation" in sig:
            score += 3    # vol_acc와 함께면 60% → 소폭 플러스
            reasons.append("RSI다이버전스+매집+3")
        else:
            score += 0
    if "52w_low_zone" in sig:
        score += 12   # 52주 저점 15% 이내 — 심리적 지지
        reasons.append("52주저점구간+12")
    if "vol_accumulation" in sig:
        # 백테스트 최신: 스윙 84.0% (이전 79.2% → 상승, SCHD 제거 효과)
        score += 12
        reasons.append("하락중거래량증가(매집,84%)+12")

    # 신호 조합 시너지 (새 신호 + 기존 신호 조합)
    # rsi_divergence 시너지: 백테스트 결과 조합도 50% → 시너지 제거
    # 대신 vol_accumulation+sector_rebound 조합이 더 강함
    if "vol_accumulation" in sig and "sector_rebound" in sig:
        score += 8;  reasons.append("매집+반등조합시너지+8")
    if "52w_low_zone" in sig and "rsi_oversold" in sig:
        score += 6;  reasons.append("52주저점+RSI과매도조합+6")
    if "vol_accumulation" in sig and "momentum_dip" in sig:
        score += 5;  reasons.append("매집+급락조합+5")

    # ma_support 패밀리 페널티
    if sig == _MA_SUPPORT_SOLO:
        score -= 12; reasons.append("ma_support단독(61.8%)-12")
    elif sig == _MA_SUPPORT_WEAK:
        score -= 5;  reasons.append("ma+momentum약조합-5")

    # ── A-2. PCR(Put/Call Ratio) 연동 ─────────────────────────────
    # ARIA가 수집한 PCR 데이터 — Jackal이 활용 안 하던 데이터
    pcr_avg = _load_pcr_from_aria()
    if pcr_avg > 0:
        if pcr_avg > 1.3 and _CRASH_REBOUND_SIGNALS & sig:
            score += 10   # 극단공포(PCR>1.3) + 반등 신호 = 최강 조합
            reasons.append(f"PCR극단({pcr_avg:.2f})+반등=최강+10")
        elif pcr_avg > 1.1 and ("bb_touch" in sig or "rsi_oversold" in sig):
            score += 5
            reasons.append(f"PCR고조({pcr_avg:.2f})+과매도+5")
        elif pcr_avg < 0.8 and "volume_climax" in sig:
            score -= 8    # 과도한 낙관(PCR<0.8)에서 volume_climax는 고점 경고
            reasons.append(f"PCR낙관({pcr_avg:.2f})+volume=고점경고-8")

    # ── B. VIX + HY Spread 교차 검증 gating ────────────────────────
    # Doc1 반박 수용: VIX 단일값 → VIX + HY Spread 교차 확인
    # 이미 FRED에서 수집 중인 BAMLH0A0HYM2 활용
    vix = (
        float(tech.get("vix_level") or 0)
        or float(aria.get("fred_vix") or 0)
        or _get_vix_from_cache()
    )
    # HY Spread: aria_market_data.json의 FRED 데이터에서 로드
    hy_spread = 0.0
    try:
        from pathlib import Path as _Path
        import json as _json
        _md = _json.loads(_Path("data/aria_market_data.json").read_text(encoding="utf-8"))             if _Path("data/aria_market_data.json").exists() else {}
        hy_spread = float(_md.get("fred", {}).get("bamlh0a0hym2", 0) or
                          _md.get("fred", {}).get("hy_spread", 0) or 0)
    except Exception:
        pass

    # 극단 공포: VIX>30 AND HY>4.0 (두 조건 동시 = 진짜 패닉)
    # VIX만: 변동성 급등이지만 크레딧 시장은 안정일 수 있음
    vix_extreme = vix > 35
    vix_high    = vix > 25
    real_panic  = vix > 30 and hy_spread > 4.0   # 진짜 공황: VIX+HY 교차 확인
    credit_stress = hy_spread > 3.5              # 크레딧 스트레스만

    # ── C. 반등 계열 보너스 (총합 상한 +12 cap — 스태킹 방지) ────
    rebound_raw = 0
    chg5d = float(tech.get("change_5d") or 0)

    if "sector_rebound" in sig:
        if real_panic:                          # VIX>30 + HY>4.0 교차 = 진짜 패닉 반등
            rebound_raw += 10
            reasons.append(f"진짜패닉(VIX{vix:.0f}+HY{hy_spread:.1f})+반등+10")
        elif vix_extreme:                       # VIX만 극단
            rebound_raw += 6
            reasons.append(f"VIX극단({vix:.0f})게이팅+반등+6")
        elif credit_stress and vix_high:        # HY 스트레스 + VIX 고조
            rebound_raw += 4
            reasons.append(f"크레딧스트레스(HY{hy_spread:.1f})+반등+4")

    if chg5d < -8 and "sector_rebound" in sig:
        rebound_raw += 10
        reasons.append(f"5일{chg5d:.0f}%급락+반등+10")
    elif chg5d < -5 and len(sig) >= 2:
        rebound_raw += 5
        reasons.append(f"5일{chg5d:.0f}%+복수신호+5")

    REBOUND_CAP = 12
    rebound_capped = min(rebound_raw, REBOUND_CAP)
    score += rebound_capped
    if rebound_raw > REBOUND_CAP:
        reasons.append(f"반등상한cap({REBOUND_CAP}←{rebound_raw})")

    # ── D. Negative Veto — 고위험 상황에서 rebound cap 동적 축소
    # Doc3: 양수 스태킹 방지만으로는 부족, 음수 신호와의 충돌 처리
    thesis_killers = aria.get("thesis_killers", [])
    regime = aria.get("regime", "")

    has_negative_veto = False
    negative_reasons: list = []

    if thesis_killers:
        has_negative_veto = True
        negative_reasons.append(f"Thesis Killer({len(thesis_killers)}개)")

    # ARIA 레짐 부정 신호
    if "전환중" in regime or regime.startswith("혼조"):
        score -= 15
        reasons.append("전환중/혼조레짐-15")
        has_negative_veto = True
        negative_reasons.append("레짐불확실")
    elif "위험회피" in regime:
        if "sector_rebound" in sig:
            score += 5
            reasons.append("위험회피+반등+5")

    # 5일 급등 직후 신호 (급락 반대)
    if chg5d > 15 and "bb_touch" not in sig:
        score -= 8
        reasons.append(f"5일{chg5d:.0f}% 과열-8")
        has_negative_veto = True
        negative_reasons.append("단기과열")

    # Negative veto 발동 시 rebound cap 절반으로 축소
    if has_negative_veto and rebound_capped > 0:
        rebound_cap_after_veto = rebound_capped // 2
        veto_penalty = rebound_capped - rebound_cap_after_veto
        score -= veto_penalty
        reasons.append(f"NegVeto({','.join(negative_reasons)}): rebound -{veto_penalty}")

    # ── D-2. 고불확실 구간 heuristic gate ────────────────────────────
    # Doc1/3 반박 수용: 이벤트-데이 방어를 키워드 신뢰도 하향으로 구현
    # 완전 abstain 아님 → quality 패널티로 강한 신호는 통과, 약한 신호만 차단
    #
    # 사이드이펙트 방어:
    #   - crash_rebound family 예외 (VIX 극단에서 반등이 강하므로 패널티 제외)
    #   - VIX AND 저FG 동시 조건 (한쪽만으론 패널티 없음)
    #   - 키워드 매칭은 aria 컨텍스트 전체에서

    HIGH_UNCERTAINTY_KEYWORDS = [
        "FOMC", "CPI", "관세", "tariff", "실적발표", "어닝", "earning",
        "금리결정", "고용지표", "기준금리", "연준", "Fed decision",
    ]
    aria_note    = aria.get("note", "") + " " + aria.get("trend", "") + " " + regime
    has_event_kw = any(kw.lower() in aria_note.lower() for kw in HIGH_UNCERTAINTY_KEYWORDS)

    # FG 수치: aria에서 추출 (없으면 기본값 50)
    fg_raw = aria.get("fear_greed", "50")
    try:
        fg_score = int(str(fg_raw).split()[0])
    except Exception:
        fg_score = 50

    # 고불확실 조건: (VIX>=32 AND FG<=15) 또는 (VIX>=40) 또는 (키워드+VIX>=28)
    # FG 파싱 실패 여부 추적 (None = 데이터 없음)
    fg_available = fg_raw not in (None, "", "50", 50)
    fg_fear_gate = fg_score <= 15 if fg_available else None

    # ── gate_reason 세분화 (Doc3: 왜 걸렸는지 로그 근거) ─────────
    gate_reason = None

    if vix >= 40:
        gate_reason  = "vix_only_hard"
        gate_strength = "hard"
    elif vix >= 32 and fg_fear_gate is True:
        gate_reason  = "vix_fg_hard"
        gate_strength = "hard"
    elif vix >= 32 and fg_fear_gate is None:
        gate_reason  = "vix_only_soft"    # FG 누락 보정
        gate_strength = "soft"
    elif has_event_kw and vix >= 28:
        gate_reason  = "keyword_vix_soft"
        gate_strength = "soft"
    else:
        gate_reason   = None
        gate_strength = None

    is_high_uncertainty = gate_reason is not None

    # ── VIX 28~32 micro-gate: 회색지대 보완 (Doc1) ────────────────
    # 로그상 3/05~3/07, 3/11~3/13 연속 0% 구간이 VIX 22~28 수준
    # ARIA 레짐이 이미 판단한 "위험회피/하락추세" 값을 직접 활용
    micro_gate_active = False
    if not is_high_uncertainty:
        is_risk_off_regime = any(kw in regime for kw in ["위험회피","하락추세","bearish"])
        if is_risk_off_regime and vix >= 22 and family not in ("crash_rebound",):
            micro_gate_active = True
            gate_reason   = "regime_micro"
            gate_strength = "micro"

    fg_str = f"FG{fg_score}" if fg_available else "FG없음"
    if is_high_uncertainty and family != "crash_rebound":
        # gate_reason별 페널티 강도
        penalty_map = {"vix_only_hard": 15, "vix_fg_hard": 15,
                       "vix_only_soft": 8,  "keyword_vix_soft": 8, "regime_micro": 5}
        penalty = penalty_map.get(gate_reason, 8)
        score  -= penalty
        reasons.append(
            f"불확실게이트[{gate_reason}](VIX{vix:.0f}/{fg_str})-{penalty}"
        )

        # hard gate + 극단 VIX(≥40) → abstain 플래그 (신호 완전 차단)
        # Doc3: "Jackal이 스스로 쉬는 날"로 처리 → 사용자 신뢰 상승
        if gate_strength == "hard" and vix >= 40:
            score = 0   # skip_threshold 하회 강제
            reasons.append(f"🚫 ABSTAIN(VIX극단{vix:.0f}≥40+hard gate)")

    elif micro_gate_active and family != "crash_rebound":
        score  -= 5
        reasons.append(f"레짐microgate({regime[:6]}/VIX{vix:.0f})-5")
    elif is_high_uncertainty and family == "crash_rebound":
        reasons.append(f"고불확실→crash_rebound예외(VIX{vix:.0f},패널티없음)")

    # ── E. ticker_accuracy — 연속 함수, 하방 페널티만 ────────────
    # Doc3 최종 합의: 상방 보너스 완전 제거 (순환 편향 + ARIA 중복 방지)
    # 하방 페널티만: 성과 나쁜 종목 신호 품질 낮춤
    if ticker and weights:
        tk_data = weights.get("ticker_accuracy", {}).get(ticker, {})
        acc_pct = tk_data.get("accuracy", 50)
        total   = tk_data.get("total", 0)

        if total >= 8:
            acc_adj = (acc_pct - 50) * 0.20
            acc_adj = max(-10, min(0, acc_adj))   # 상방 완전 0 (Doc3 수용)
            score  += acc_adj
            if abs(acc_adj) >= 1:
                reasons.append(f"종목정확도({acc_pct:.0f}%,n={total}){acc_adj:+.1f}")
        elif total >= 3:
            acc_adj = (acc_pct - 50) * 0.10
            acc_adj = max(-5, min(0, acc_adj))    # 약한 하방만
            score  += acc_adj
            if abs(acc_adj) >= 0.5:
                reasons.append(f"종목정확도약(n={total}){acc_adj:+.1f}")
        # total < 3: 무시 (초기 낙인 방지)

    score = max(0, min(100, score))

    # ── F. signal_family 결정 (분류 테이블 기반) ─────────────────
    family = _get_signal_family(signals)

    # ── G. signal_family별 스킵 임계값 + VIX 동적 조정 ──────────
    # Doc2/3 합의: VIX 완화는 crash_rebound 전용 (ma_support, general 제외)
    THRESHOLDS = {
        "crash_rebound":   40,   # 극단 반등: 완화
        "general":         45,   # 기본
        "ma_support_weak": 47,   # ma+momentum 50% → 완화
        "ma_support_solo": 46,   # 61.8% 검증 → 50에서 완화
    }
    skip_threshold = THRESHOLDS.get(family, 45)

    # VIX 동적 조정: crash_rebound 전용 (Doc3 반박 수용)
    if family == "crash_rebound":
        if vix >= 30:
            skip_threshold = max(33, skip_threshold - 5)   # 완화
        elif vix < 18:
            skip_threshold = min(46, skip_threshold + 3)   # 약간 엄격
    elif family == "general":
        if vix < 18:
            skip_threshold = min(50, skip_threshold + 5)   # 저변동성: 엄격
        # 고변동성에서 일반 신호는 완화 없음 (ARIA 중복 방지)
    # ma_support: VIX 상태 무관하게 고정 (노이즈라 어떤 환경도 동일)

    skip  = score < skip_threshold
    label = "최강" if score >= 80 else "강" if score >= 65 else "보통" if score >= 50 else "약"

    analyst_adj = +5 if score >= 75 else 0
    final_adj   = +5 if score >= 75 else 0

    return {
        "quality_score":    score,
        "quality_label":    label,
        "reasons":          reasons,
        "skip":             skip,
        "skip_threshold":   skip_threshold,
        "signal_family":    family,
        "analyst_adj":      analyst_adj,
        "final_adj":        final_adj,
        "vix_used":         vix,
        "vix_extreme":      vix_extreme,
        "rebound_bonus":    rebound_capped,
        "rebound_raw":      rebound_raw,
        "negative_veto":    has_negative_veto,
        "negative_reasons": negative_reasons,
    }


def agent_analyst(ticker: str, info: dict, tech: dict,
                  macro: dict, aria: dict) -> dict:
    """
    Haiku로 매수 근거를 구성.
    Returns: {score, signals_fired, reasoning, confidence}
    """
    cur     = info["currency"]
    fred    = macro.get("fred", {})
    weights = _load_weights()
    stw     = weights.get("signal_weights", {})

    price_str = f"{tech['price']:,.2f}" if info["market"] == "US" else f"{tech['price']:,.0f}"
    pnl_str = ""
    if info.get("avg_cost") and info["market"] == "US":
        pnl     = (tech["price"] - info["avg_cost"]) / info["avg_cost"] * 100
        pnl_str = f"\n내 평균단가: {cur}{info['avg_cost']} (현재 {pnl:+.1f}%)"

    # 학습된 신호별 정확도 힌트
    acc_hint = ""
    sig_acc = weights.get("signal_accuracy", {})
    if sig_acc:
        top = sorted(sig_acc.items(), key=lambda x: x[1].get("accuracy", 0), reverse=True)[:3]
        acc_hint = "\n[학습 정확도 높은 신호] " + " | ".join(
            f"{k}:{v['accuracy']:.0f}%" for k, v in top if v.get("total", 0) >= 3
        )

    # 신호 품질 컨텍스트 (백테스트 기반 사전 평가)
    quality = info.get("_quality", {})
    quality_hint = ""
    if quality:
        q_score = quality.get("quality_score", 50)
        q_label = quality.get("quality_label", "")
        q_reasons = ", ".join(quality.get("reasons", []))
        quality_hint = (
            f"\n[신호 품질 사전평가] {q_score}점 ({q_label})"
            f"\n  근거: {q_reasons}"
        )

    prompt = f"""당신은 주식 매수 타점 분석가(Analyst)입니다.
아래 데이터로 {info['name']} ({ticker})의 매수 근거를 분석하세요.
반드시 JSON만 반환하세요.

[종목]
현재가: {cur}{price_str}
전일比: {tech['change_1d']:+.1f}% | 5일比: {tech['change_5d']:+.1f}%{pnl_str}

[기술 지표]
RSI(14): {tech['rsi']} | MA20: {cur}{tech['ma20']} | MA50: {cur}{tech.get('ma50','N/A')}
볼린저: {tech['bb_pos']}% (0%=하단, 100%=상단) | BB폭: {tech.get('bb_width','N/A')}%
거래량: 평균 대비 {tech['vol_ratio']:.1f}x | 5일거래량추세: {tech.get('vol_trend_5d','N/A')}%
MA배열: {tech.get('ma_alignment','N/A')} | 52주위치: {tech.get('52w_pos','N/A')}%
RSI다이버전스: {'✅ 강세' if tech.get('rsi_divergence') else '없음'} | 매집신호: {'✅' if tech.get('vol_accumulation') else '없음'}

[매크로 (FRED)]
VIX: {fred.get('vix','N/A')} | HY스프레드: {fred.get('hy_spread','N/A')}%
장단기금리차: {fred.get('yield_curve','N/A')}% | 달러지수: {fred.get('dxy','N/A')}
소비자심리: {fred.get('consumer_sent','N/A')}

[ARIA 시장 맥락]
레짐: {aria['regime'] or '정보없음'} | 추세: {aria['trend'] or '정보없음'}
센티먼트: {aria['sentiment_score']}점 ({aria['sentiment_level']})
섹터유입: {', '.join(aria['key_inflows']) or '없음'}
섹터유출: {', '.join(aria['key_outflows']) or '없음'}
강세섹터: {aria.get('top_sector','N/A')} | 약세섹터: {aria.get('bottom_sector','N/A')}
{acc_hint}{quality_hint}

매수 근거가 있다면 높은 점수, 없다면 낮은 점수를 주세요.
신호 품질이 "최강/강"이면 낙관적으로, "약"이면 보수적으로 평가하세요.

{{
  "analyst_score": 0~100,
  "confidence": "낮음" 또는 "보통" 또는 "높음",
  "signals_fired": ["rsi_oversold", "bb_touch", "volume_surge", "ma_support", "golden_cross", "fear_regime", "sector_inflow"],
  "bull_case": "매수 근거 2~3줄",
  "entry_price": 숫자 또는 null,
  "stop_loss": 숫자 또는 null
}}"""

    try:
        resp = Anthropic().messages.create(
            model=MODEL_H, max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        raw  = re.sub(r"```(?:json)?|```", "", resp.content[0].text).strip()
        m    = re.search(r"\{[\s\S]*\}", raw)
        if not m:
            return {"analyst_score": 50, "confidence": "낮음",
                    "signals_fired": [], "bull_case": "분석 실패"}
        result = json.loads(m.group())
        base = int(result.get("analyst_score", 50))
        # 신호 품질 보정 적용
        adj = info.get("_quality", {}).get("analyst_adj", 0) if info else 0
        result["analyst_score"] = max(0, min(100, base + adj))
        if adj != 0:
            result["_quality_adj_applied"] = adj
        return result
    except Exception as e:
        log.error(f"  Analyst 실패: {e}")
        return {"analyst_score": 50, "confidence": "낮음",
                "signals_fired": [], "bull_case": "분석 실패"}


# ══════════════════════════════════════════════════════════════════
# Agent 2: Devil — 반박 + Thesis Killer 체크
# ══════════════════════════════════════════════════════════════════

def agent_devil(ticker: str, info: dict, tech: dict,
                macro: dict, aria: dict, analyst: dict) -> dict:
    """
    Haiku로 Analyst 결론을 반박.
    ARIA의 Thesis Killer를 직접 체크.
    Returns: {devil_score, verdict, objections, thesis_killer_hit}
    """
    cur  = info["currency"]
    fred = macro.get("fred", {})

    price_str = f"{tech['price']:,.2f}" if info["market"] == "US" else f"{tech['price']:,.0f}"

    # Thesis Killer 텍스트 구성
    tk_text = ""
    tks = aria.get("thesis_killers", [])
    if tks:
        tk_lines = []
        for tk in tks[:3]:
            event = tk.get("event", "")
            inv   = tk.get("invalidates_if", "")
            if event and inv:
                tk_lines.append(f"  • {event}: {inv}")
        if tk_lines:
            tk_text = "\n[ARIA Thesis Killers — 이 조건이면 매수 무효]\n" + "\n".join(tk_lines)

    prompt = f"""당신은 투자 리스크 분석가(Devil)입니다.
Analyst가 {info['name']} ({ticker}) 매수를 주장합니다.
당신은 반드시 반박해야 합니다. JSON만 반환하세요.

[Analyst 결론]
점수: {analyst['analyst_score']} | 신뢰도: {analyst['confidence']}
근거: {analyst.get('bull_case','')}
발동 신호: {', '.join(analyst.get('signals_fired', []))}

[현재 상황]
현재가: {cur}{price_str}
RSI: {tech['rsi']} | 볼린저: {tech['bb_pos']}% | 거래량: {tech['vol_ratio']:.1f}x
VIX: {fred.get('vix','N/A')} | HY스프레드: {fred.get('hy_spread','N/A')}%
장단기금리차: {fred.get('yield_curve','N/A')}%

[ARIA 시장 맥락]
레짐: {aria['regime'] or '정보없음'} | 센티먼트: {aria['sentiment_score']}점
유출섹터: {', '.join(aria['key_outflows']) or '없음'}
{tk_text}

반박 기준:
- VIX > 25이면 변동성 과대
- HY스프레드 > 4%이면 위험회피 강화
- 레짐이 위험회피이면 매수 부적절
- Thesis Killer 조건 해당이면 즉시 무효
- 연속 3일+ 상승이면 과열 경고

{{
  "devil_score": 0~100 (높을수록 반박 강함, 매수 부적절),
  "verdict": "동의" 또는 "부분동의" 또는 "반대",
  "objections": ["반박 이유 1", "반박 이유 2"],
  "thesis_killer_hit": true 또는 false,
  "killer_detail": "해당 Thesis Killer 내용 (없으면 빈 문자열)",
  "bear_case": "매수 반대 근거 1~2줄"
}}"""

    try:
        resp = Anthropic().messages.create(
            model=MODEL_H, max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        raw  = re.sub(r"```(?:json)?|```", "", resp.content[0].text).strip()
        m    = re.search(r"\{[\s\S]*\}", raw)
        if not m:
            return {"devil_score": 30, "verdict": "부분동의",
                    "objections": [], "thesis_killer_hit": False}
        result = json.loads(m.group())
        result["devil_score"] = int(result.get("devil_score", 30))
        return result
    except Exception as e:
        log.error(f"  Devil 실패: {e}")
        return {"devil_score": 30, "verdict": "부분동의",
                "objections": [], "thesis_killer_hit": False}


# ══════════════════════════════════════════════════════════════════
# Final 판단
# ══════════════════════════════════════════════════════════════════

def _final_judgment(analyst: dict, devil: dict) -> dict:
    """
    Analyst와 Devil 결과를 합산해 최종 판단.

    계산:
      - Thesis Killer 발동 → 즉시 is_entry=False
      - Devil 반대         → analyst_score * 0.5
      - Devil 부분동의     → analyst_score * 0.75
      - Devil 동의         → analyst_score * 1.0
      - final_score        → 위 조정값 (devil_score는 페널티로 반영)
    """
    a_score   = analyst.get("analyst_score", 50)
    d_score   = devil.get("devil_score", 30)
    verdict   = devil.get("verdict", "부분동의")
    tk_hit    = devil.get("thesis_killer_hit", False)

    # Thesis Killer 발동 → 즉시 매수 불가
    if tk_hit:
        return {
            "final_score": 20,
            "is_entry":    False,
            "signal_type": "매도주의",
            "reason":      f"⛔ Thesis Killer: {devil.get('killer_detail','무효화 조건 충족')}",
        }

    # Devil 판정에 따른 가중치
    weight_map = {"동의": 1.0, "부분동의": 0.75, "반대": 0.5}
    weight     = weight_map.get(verdict, 0.75)

    # Devil 반박 강도 페널티 (devil_score가 높을수록 페널티)
    devil_penalty = (d_score - 30) * 0.2   # devil_score 30 기준, 초과분의 20%
    final = max(0, min(100, round(a_score * weight - devil_penalty, 1)))

    # 신호 타입 결정
    if final >= STRONG_THRESHOLD and verdict != "반대":
        sig_type = "강한매수"
    elif final >= ALERT_THRESHOLD and verdict != "반대":
        sig_type = "매수검토"
    elif verdict == "반대" or final < 40:
        sig_type = "매도주의" if final < 30 else "관망"
    else:
        sig_type = "관망"

    # 신호 품질 최종 보정
    # (analyst에 이미 adj 반영됐으나 final에도 미세 보정)
    # quality 정보는 analyst dict에 포함돼있지 않으므로 별도 처리 불가 → 생략

    is_entry = final >= ALERT_THRESHOLD and verdict != "반대" and not tk_hit

    # 판단 이유 구성
    reason_parts = []
    if analyst.get("bull_case"):
        reason_parts.append(analyst["bull_case"][:40])
    if devil.get("objections"):
        reason_parts.append("⚠️ " + devil["objections"][0][:30])

    return {
        "final_score": final,
        "is_entry":    is_entry,
        "signal_type": sig_type,
        "reason":      " | ".join(reason_parts)[:80],
        "entry_price": analyst.get("entry_price"),
        "stop_loss":   analyst.get("stop_loss"),
    }


# ══════════════════════════════════════════════════════════════════
# 쿨다운
# ══════════════════════════════════════════════════════════════════

def _get_signal_family_key(signals: list) -> str:
    """신호 목록에서 family 키 생성 (쿨다운 구분용)."""
    sig = set(signals)
    if sig & {"sector_rebound","volume_climax","vol_accumulation","52w_low_zone"}:
        return "crash_rebound"
    if "bb_touch" in sig or "rsi_oversold" in sig:
        return "oversold"
    if "momentum_dip" in sig:
        return "momentum"
    return "general"


def _is_on_cooldown(ticker: str, signals: list = None,
                    quality_score: float = 0,
                    vol_ratio: float = 0,
                    change_1d: float = 0) -> bool:
    """
    ticker + signal_family 기반 쿨다운 확인.
    쿨다운 override 조건 (Doc3 제안, 사이드이펙트 방어 포함):
      - quality_score가 이전 발동보다 +15 이상 급상승
      - AND vol_ratio > 2.5 (거래량 급등)
      - AND change_1d < 0 (상승 gap-up 상황 차단 — 하락 중 거래량 급등만 유효)
    세 조건 모두 만족해야 override → 하나라도 빠지면 쿨다운 유지
    """
    if not COOLDOWN_FILE.exists():
        return False
    try:
        cd  = json.loads(COOLDOWN_FILE.read_text(encoding="utf-8"))
        fam = _get_signal_family_key(signals) if signals else "any"

        key_fam = f"{ticker}:{fam}"
        if key_fam in cd:
            hrs = (datetime.now() - datetime.fromisoformat(cd[key_fam])).total_seconds() / 3600
            if hrs < 48:
                # override 조건 확인: 세 조건 동시 만족 시 쿨다운 무시
                prev_quality = cd.get(f"{key_fam}:quality", 0)
                quality_surge = (quality_score - prev_quality) >= 15
                vol_spike     = vol_ratio > 2.5
                is_declining  = change_1d < 0  # 상승 gap-up 차단

                if quality_surge and vol_spike and is_declining:
                    # override 5거래일 1회 제한 (Doc2: 연속 override 방지)
                    last_override = cd.get(f"{key_fam}:last_override")
                    if last_override:
                        override_hrs = (
                            datetime.now() - datetime.fromisoformat(last_override)
                        ).total_seconds() / 3600
                        if override_hrs < 120:   # 5거래일 = 120시간
                            log.info(
                                f"  ⛔ override 제한(5거래일 1회): {ticker} "
                                f"마지막override {override_hrs:.0f}h 전"
                            )
                            return True  # override 횟수 초과 → 쿨다운 유지
                    log.info(
                        f"  ⚡ 쿨다운 override: {ticker} quality+{quality_score-prev_quality:.0f}"
                        f" vol{vol_ratio:.1f}x change{change_1d:+.1f}%"
                    )
                    # override 상세 기록 (Doc3: reason, quality, count 추적)
                    override_count = cd.get(f"{key_fam}:override_count", 0) + 1
                    cd[f"{key_fam}:override_reason"]   = (
                        f"quality+{quality_score-prev_quality:.0f}_vol{vol_ratio:.1f}x"
                    )
                    cd[f"{key_fam}:override_quality"]  = quality_score
                    cd[f"{key_fam}:override_count"]    = override_count
                    cd[f"{key_fam}:last_override"]     = datetime.now().isoformat()
                    COOLDOWN_FILE.write_text(
                        json.dumps(cd, ensure_ascii=False, indent=2), encoding="utf-8"
                    )
                    return False  # 쿨다운 무시 → 신호 통과
                return True   # 조건 미충족 → 쿨다운 유지

        # 레거시: ticker 전체 쿨다운
        last = cd.get(ticker)
        if last:
            hrs = (datetime.now() - datetime.fromisoformat(last)).total_seconds() / 3600
            if hrs < COOLDOWN_HOURS:
                return True
        return False
    except Exception:
        return False


def _set_cooldown(ticker: str, signals: list = None,
                  quality_score: float = 0, is_override: bool = False):
    """ticker + signal_family 기반 쿨다운 설정. quality_score 저장으로 override 판단."""
    cd: dict = {}
    if COOLDOWN_FILE.exists():
        try:
            cd = json.loads(COOLDOWN_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    now_iso = datetime.now().isoformat()
    cd[ticker] = now_iso   # 레거시 호환
    if signals:
        fam = _get_signal_family_key(signals)
        key_fam = f"{ticker}:{fam}"
        cd[key_fam]               = now_iso
        cd[f"{key_fam}:quality"]  = quality_score   # override 판단용
        if is_override:
            cd[f"{key_fam}:last_override"] = now_iso  # override 시간 기록 (5거래일 제한)
    COOLDOWN_FILE.write_text(json.dumps(cd, ensure_ascii=False, indent=2), encoding="utf-8")


# ══════════════════════════════════════════════════════════════════
# 텔레그램
# ══════════════════════════════════════════════════════════════════

def _send_telegram(text: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(text); return False
    try:
        resp = httpx.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10,
        )
        return resp.json().get("ok", False)
    except Exception as e:
        log.error(f"  텔레그램 예외: {e}")
        return False


def _build_alert_message(ticker: str, info: dict, tech: dict,
                          analyst: dict, devil: dict, final: dict,
                          aria: dict) -> str:
    now_str   = datetime.now(KST).strftime("%m/%d %H:%M")
    cur       = info["currency"]
    strong    = final["signal_type"] == "강한매수"
    score     = final["final_score"]
    price_str = f"{tech['price']:,.2f}" if info["market"] == "US" else f"{tech['price']:,.0f}"

    # 헤더
    icon  = "🔥" if strong else "🎯"
    label = "강한 매수 타점" if strong else "스윙 타점"

    # 점수 색상
    score_icon = "🟢" if score >= STRONG_THRESHOLD else "🟡"

    # PnL (포트폴리오 미국 주식만)
    pnl_str = ""
    if info.get("avg_cost") and info["market"] == "US":
        pnl     = (tech["price"] - info["avg_cost"]) / info["avg_cost"] * 100
        pnl_str = f"  ({'📈' if pnl >= 0 else '📉'}{pnl:+.1f}%)"

    # Devil 판정 — 내용 없으면 줄 자체 생략
    d_verdicts = {"동의": "✅ 동의", "부분동의": "⚠️ 부분동의", "반대": "❌ 반대"}
    d_label    = d_verdicts.get(devil.get("verdict", ""), "")
    d_objs     = devil.get("objections", [])
    d_comment  = (d_objs[0][:55] if d_objs else "").strip()

    # 진입 / 손절 (None 이면 줄 생략)
    entry = final.get("entry_price")
    stop  = final.get("stop_loss")

    # ── MAE/스윙 정보: jackal_weights.json에서 읽기 (동적) ──────────
    # jackal_weights.json에 mae_avg 필드 없으면 하드코딩 fallback
    # 백테스트가 자동으로 채우면 알림 코드 수정 없이 동적 반영
    _SWING_DEFAULTS = {
        # 하드코딩 fallback — 백테스트 60일 기준 추정
        # jackal_weights.json의 mae_avg, peak_day, swing_acc가 있으면 거기서 읽음
        "sector_rebound":  {"peak_day": "D4~5", "swing_acc": "93%", "mae_avg": "-2.1%"},
        "bb_touch":        {"peak_day": "D4~5", "swing_acc": "97%", "mae_avg": "-3.8%"},
        "rsi_oversold":    {"peak_day": "D4~5", "swing_acc": "88%", "mae_avg": "-2.9%"},
        "vol_accumulation":{"peak_day": "D5",   "swing_acc": "84%", "mae_avg": "-3.2%"},
        "volume_climax":   {"peak_day": "D4~5", "swing_acc": "80%", "mae_avg": "-4.5%"},
        "momentum_dip":    {"peak_day": "D4~5", "swing_acc": "78%", "mae_avg": "-4.1%"},
        "ma_support":      {"peak_day": "D3~4", "swing_acc": "67%", "mae_avg": "-1.8%"},
        "rsi_divergence":  {"peak_day": "D4",   "swing_acc": "52%", "mae_avg": "-2.3%"},
    }
    # weights에서 동적 로드 시도
    _sig_weights = weights or {}
    def _get_swing_info(sig: str) -> dict:
        w = _sig_weights.get("signal_details", {}).get(sig, {})
        default = _SWING_DEFAULTS.get(sig, {"peak_day":"D4~5","swing_acc":"74%","mae_avg":"-3.5%"})
        return {
            "peak_day":  w.get("peak_day",  default["peak_day"]),
            "swing_acc": w.get("swing_acc", default["swing_acc"]),
            "mae_avg":   w.get("mae_avg",   default["mae_avg"]),
        }

    fired_sigs = final.get("signals_fired", [])

    # Doc7 부분 수용: 같은 family 안 모든 신호명 표시 (집계는 family 기준 유지)
    # 단일 신호명만 보이던 것 → "bb_touch + rsi_oversold (crash_rebound)" 형태로
    def _format_signals_display(sigs: list) -> str:
        if not sigs:
            return "없음"
        if len(sigs) == 1:
            return sigs[0]
        # 강신호 앞으로 정렬
        priority = ["sector_rebound","volume_climax","bb_touch","rsi_oversold",
                    "vol_accumulation","momentum_dip","ma_support","rsi_divergence"]
        sorted_sigs = sorted(sigs, key=lambda s: priority.index(s) if s in priority else 99)
        return " + ".join(sorted_sigs)

    signals_display = _format_signals_display(fired_sigs)

    best_info = _get_swing_info("bb_touch")  # 기본값
    for s in ["sector_rebound","bb_touch","rsi_oversold","vol_accumulation"]:
        if s in fired_sigs:
            best_info = _get_swing_info(s)
            break

    # Peak + MAE 동시 표시 (버티는 동안 얼마나 아팠는지 인지 가능)
    mae_source = "자동계산" if _sig_weights.get("signal_details") else "백테스트추정"
    # median MAE도 표시 (표준편차 언급으로 불확실성 인지)
    mae_display = best_info.get("mae_avg", "-3.5%")
    mae_median  = best_info.get("mae_median", "")
    if isinstance(mae_display, (int, float)):
        mae_display = f"{mae_display:.1f}%"
    if isinstance(mae_median, (int, float)):
        mae_median = f"{mae_median:.1f}%"

    mae_str = f"{mae_display}"
    if mae_median and mae_median != mae_display:
        mae_str += f"(중앙값:{mae_median})"

    swing_peak_str = (
        f"📈 스윙: Peak {best_info['peak_day']} ({best_info['swing_acc']}) "
        f"| MAE avg {mae_str} [{mae_source}]"
    )

    lines = [
        f"{icon} <b>Jackal Hunter — {label}</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"<b>{info['name']}</b>  <code>{ticker}</code>",
        f"💰 {cur}{price_str}  1일:{tech['change_1d']:+.1f}%  5일:{tech['change_5d']:+.1f}%{pnl_str}",
        "━━━━━━━━━━━━━━━━━━━━",
        f"{score_icon} <b>{score:.0f}/100</b>  {final['signal_type']}  [{analyst.get('confidence','')}]",
        swing_peak_str,  # ← 스윙 타겟 강조 (1일보다 스윙 정확도가 훨씬 높음)
        f"⚡ Analyst {analyst['analyst_score']}  →  Devil {devil['devil_score']}  →  Final {score:.0f}",
        f"📊 신호: {signals_display}",
        f"   RSI {tech['rsi']} | BB {tech['bb_pos']}% | 거래량 {tech['vol_ratio']:.1f}x",
    ]

    # Analyst 근거
    bull = (analyst.get("bull_case") or "").strip()
    if bull:
        lines.append(f"🐂 {bull[:80]}")

    # Devil 반박 (있을 때만)
    if d_comment:
        lines.append(f"🔴 Devil {d_label}: {d_comment}")
    elif d_label:
        lines.append(f"🔴 Devil {d_label}")

    lines.append("━━━━━━━━━━━━━━━━━━━━")

    # 진입 / 손절
    if entry:
        lines.append(f"🎯 진입: {cur}{entry}{'  🛑 손절: ' + cur + str(stop) if stop else ''}")
    elif stop:
        lines.append(f"🛑 손절: {cur}{stop}")

    lines.append(f"⏰ {now_str} KST | Jackal Hunter")

    return "\n".join(lines)


def _save_recommendation(extra: dict, aria: dict):
    """
    추천 종목을 두 곳에 저장:
    1. data/jackal_watchlist.json  → ARIA Hunter가 읽어 뉴스 검색
    2. jackal/recommendation_log.json → 24h 후 결과 확인용
    """
    import yfinance as _yf
    now = datetime.now(KST)
    entries = []
    for ticker, info in extra.items():
        price_now = None
        try:
            hist = _yf.Ticker(ticker).history(period="2d", interval="1d")
            if not hist.empty:
                price_now = float(hist["Close"].iloc[-1])
        except Exception:
            pass
        entries.append({
            "ticker":          ticker,
            "name":            info.get("name", ticker),
            "market":          info.get("market", "US"),
            "reason":          info.get("reason", ""),
            "price_at_rec":    price_now,
            "recommended_at":  now.isoformat(),
            "aria_regime":     aria.get("regime", ""),
            "aria_inflows":    aria.get("key_inflows", []),
            "aria_trend":      aria.get("trend", ""),
            "outcome_checked": False,
            "price_next_day":  None,
            "outcome_pct":     None,
            "outcome_correct": None,
        })

    # 1. jackal_watchlist.json (ARIA Hunter가 읽음)
    watchlist = {
        "updated_at": now.isoformat(),
        "regime":     aria.get("regime", ""),
        "tickers":    [e["ticker"] for e in entries],
        "details":    {e["ticker"]: {"name": e["name"], "reason": e["reason"]} for e in entries},
    }
    try:
        JACKAL_WATCHLIST.parent.mkdir(exist_ok=True)
        JACKAL_WATCHLIST.write_text(
            json.dumps(watchlist, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        log.info(f"   jackal_watchlist.json 저장: {watchlist['tickers']}")
    except Exception as e:
        log.error(f"   watchlist 저장 실패: {e}")

    # 2. recommendation_log.json (Evolution이 읽음)
    logs: list = []
    if RECOMMEND_LOG_FILE.exists():
        try:
            logs = json.loads(RECOMMEND_LOG_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    logs.extend(entries)
    logs = logs[-200:]
    RECOMMEND_LOG_FILE.write_text(
        json.dumps(logs, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _load_jackal_news() -> str:
    """ARIA가 수집한 Jackal 추천 종목 뉴스 → 프롬프트용 문자열."""
    if not JACKAL_NEWS_FILE.exists():
        return ""
    try:
        data  = json.loads(JACKAL_NEWS_FILE.read_text(encoding="utf-8"))
        items = data.get("news_items", [])
        if not items:
            return ""
        lines = ["\n[ARIA 수집 뉴스 — Jackal 추천 종목]"]
        for item in items[:5]:
            lines.append(f"  • {item.get('ticker','')}: {item.get('headline','')[:60]}")
        return "\n".join(lines)
    except Exception:
        return ""


def _send_aria_extra_message(extra: dict, aria: dict):
    """ARIA 분석 기반 추천 종목 전송 + 추적 저장."""
    if not extra:
        return

    _save_recommendation(extra, aria)

    now_str = datetime.now(KST).strftime("%m/%d %H:%M")
    regime  = aria.get("regime", "")
    inflows = aria.get("key_inflows", [])

    lines = [
        "💡 <b>ARIA 기반 관심 종목 추천</b>",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    if regime:
        lines.append(f"🌐 레짐: {regime[:30]}")
    if inflows:
        lines.append(f"📈 유입 섹터: {', '.join(inflows[:3])}")
    lines.append("")

    for ticker, info in extra.items():
        icon   = "🇺🇸" if info.get("market") == "US" else "🇰🇷"
        reason = info.get("reason", "")
        lines.append(f"{icon} <b>{info['name']}</b> ({ticker})")
        if reason:
            lines.append(f"   └ {reason}")

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━",
        "📰 ARIA가 관련 뉴스 수집 예정 (내일 아침 반영)",
        f"⏰ {now_str} KST | Jackal × ARIA",
    ]
    _send_telegram("\n".join(lines))
    log.info(f"   ARIA 추천 전송: {list(extra.keys())}")

def _build_summary_message(results: list, macro: dict, aria: dict) -> str:
    """타점 없을 때 스캔 결과 요약"""
    now_str = datetime.now(KST).strftime("%m/%d %H:%M")
    fred    = macro.get("fred", {})

    # 최고 점수 종목
    top_score = max((r["final_score"] for r in results), default=0)

    lines = [
        "📊 <b>Jackal Hunter — 타점 없음</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"최고점수: {top_score:.0f}/100 (임계값 {ALERT_THRESHOLD})",
        "",
    ]

    # 신호 아이콘: 반등가능 > 중립 > 매도주의
    def _sig_icon(sig: str, score: float) -> str:
        if sig == "강한매수":  return "🟢"
        if sig == "매수검토":  return "🟡"
        if sig == "매도주의":  return "🔴"
        return "⚪"

    # 점수 내림차순 정렬
    sorted_r = sorted(results, key=lambda x: x["final_score"], reverse=True)

    for r in sorted_r:
        sig    = r.get("signal_type", "관망")
        icon   = _sig_icon(sig, r["final_score"])
        ticker = r["ticker"]
        name   = r["name"]
        # 한국 숫자 티커면 이름만
        label  = f"<b>{name}</b> ({ticker})" if not ticker[:6].isdigit() else f"<b>{name}</b>"
        dv     = r.get("devil_verdict", "")
        dv_str = f" | Devil {dv}" if dv else ""
        # 신호 한글 간략화
        sig_short = {"강한매수": "강한매수", "매수검토": "반등가능", "관망": "중립", "매도주의": "약세"}.get(sig, sig)
        lines.append(
            f"{icon} {label}  {r['final_score']:.0f}점"
            f" | RSI {r['rsi']} | 5일 {r.get('change_5d','N/A')}%"
            f" | {sig_short}{dv_str}"
        )

    lines.append("━━━━━━━━━━━━━━━━━━━━")

    # 매크로 요약
    fred_parts = []
    if fred.get("vix"):       fred_parts.append(f"VIX {fred['vix']}")
    if fred.get("hy_spread"): fred_parts.append(f"HY {fred['hy_spread']}%")
    if fred_parts:
        lines.append("📈 " + " | ".join(fred_parts))

    if aria.get("regime"):
        lines.append(f"🌐 {aria['regime'][:35]}")
    lines.append(f"⏰ {now_str} KST | Jackal Hunter")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════
# 스캔 로그 (Evolution 학습용 — 풍부한 메타데이터 포함)
# ══════════════════════════════════════════════════════════════════

def _save_log(entry: dict):
    logs: list = []
    if SCAN_LOG_FILE.exists():
        try:
            logs = json.loads(SCAN_LOG_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    logs.append(entry)
    logs = logs[-500:]
    SCAN_LOG_FILE.write_text(json.dumps(logs, ensure_ascii=False, indent=2), encoding="utf-8")

def _save_shadow_log(entry: dict):
    """
    Claude 호출 스킵된 신호 별도 저장 (Doc3: ARIA accuracy와 분리).
    scan_log.json과 혼용 금지 — Evolution이 live/shadow를 별도 집계해야 함.
    """
    logs: list = []
    if JACKAL_SHADOW_LOG.exists():
        try:
            logs = json.loads(JACKAL_SHADOW_LOG.read_text(encoding="utf-8"))
        except Exception:
            pass
    logs.append(entry)
    logs = logs[-300:]   # shadow는 별도 한도
    JACKAL_SHADOW_LOG.write_text(json.dumps(logs, ensure_ascii=False, indent=2), encoding="utf-8")


# ══════════════════════════════════════════════════════════════════
# 메인 스캔
# ══════════════════════════════════════════════════════════════════

def _suggest_extra_tickers(aria: dict, portfolio: dict) -> dict:
    """
    ARIA 분석에서 타점 가능성 높은 추가 5종목 추천.
    Claude Haiku가 섹터 유입/헤드라인 기반으로 추천.
    포트폴리오에 이미 있는 종목은 제외.
    """
    existing = set(portfolio.keys())
    inflows  = aria.get("key_inflows", [])
    outflows = aria.get("key_outflows", [])
    regime   = aria.get("regime", "")
    one_line = aria.get("one_line", "")
    top_sec  = aria.get("top_sector", "")

    if not inflows and not regime:
        return {}

    prompt = f"""당신은 주식 종목 추천 전문가입니다.
ARIA 시장 분석 결과를 보고 타점이 생길 가능성이 높은 종목 5개를 추천하세요.
이미 보유 중인 종목({', '.join(existing)})은 제외하세요.

[ARIA 분석]
레짐: {regime}
요약: {one_line[:80]}
주요 유입 섹터: {', '.join(inflows)}
주요 유출 섹터: {', '.join(outflows)}
강세 섹터: {top_sec}

조건:
- yfinance로 조회 가능한 실제 티커 심볼 사용
- 미국 주식: TICKER 형식 (예: TSM, AMD, QCOM)
- 한국 주식: 6자리+.KS 형식 (예: 012450.KS)
- 유입 섹터와 연관된 종목 우선
- 현재 레짐에서 수혜 가능한 종목

JSON만 반환하세요:
{{
  "recommendations": [
    {{"ticker": "TSM", "name": "TSMC", "market": "US", "currency": "$", "reason": "AI 반도체 수혜"}},
    {{"ticker": "AMD", "name": "AMD", "market": "US", "currency": "$", "reason": "GPU 경쟁"}},
    {{"ticker": "012450.KS", "name": "한화에어로스페이스", "market": "KR", "currency": "₩", "reason": "방산 섹터 유입"}}
  ]
}}"""

    try:
        resp = Anthropic().messages.create(
            model=MODEL_H, max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        raw  = re.sub(r"", "", resp.content[0].text).strip()
        m    = re.search(r"\{{[\s\S]*\}}", raw)
        if not m:
            return {}
        data  = json.loads(m.group())
        extra = {}
        for r in data.get("recommendations", [])[:5]:
            t = r.get("ticker", "")
            if t and t not in existing:
                extra[t] = {
                    "name":      r.get("name", t),
                    "avg_cost":  None,
                    "market":    r.get("market", "US"),
                    "currency":  r.get("currency", "$"),
                    "portfolio": False,
                    "reason":    r.get("reason", ""),
                }
        log.info(f"   ARIA 추가 추천: {list(extra.keys())}")
        return extra
    except Exception as e:
        log.error(f"추가 종목 추천 실패: {e}")
        return {}


def run_scan(force: bool = False) -> dict:
    now_kst = datetime.now(KST)
    us_open = _is_us_open()
    kr_open = _is_kr_open()

    log.info(f"📡 Jackal Scanner | {now_kst.strftime('%Y-%m-%d %H:%M KST')}")
    log.info(f"   미국장 {'✅' if us_open else '❌'} | 한국장 {'✅' if kr_open else '❌'}")

    # 공통 데이터 수집 (1회)
    macro = fetch_all()
    aria  = _load_aria_context()

    log.info(f"   ARIA 레짐: {aria['regime'][:20] if aria['regime'] else '정보없음'} | "
             f"센티먼트: {aria['sentiment_score']}점")

    # 포트폴리오 로드
    portfolio = _load_portfolio()
    log.info(f"   포트폴리오: {len(portfolio)}종목")

    # ARIA 기반 추가 5종목 추천 → 별도 메시지로 즉시 전송
    extra = _suggest_extra_tickers(aria, portfolio)
    if extra:
        _send_aria_extra_message(extra, aria)

    # 스캔 대상은 포트폴리오만 (extra는 별도 메시지로 처리됨)
    watchlist = portfolio

    scanned = 0
    alerted = 0
    results: list = []

    for ticker, info in watchlist.items():
        market = info["market"]

        if not force:
            if market == "US" and not us_open:
                continue
            if market == "KR" and not kr_open:
                continue

        tech = fetch_technicals(ticker)
        if not tech:
            continue

        if _is_on_cooldown(ticker,
                           quality_score=0,
                           vol_ratio=tech.get("vol_ratio", 0),
                           change_1d=tech.get("change_1d", 0)):
            log.info(f"  {ticker}: 쿨다운 — 스킵")
            continue

        tech = fetch_technicals(ticker)
        if not tech:
            continue

        log.info(f"  {ticker} ({info['name']}): RSI={tech['rsi']} BB={tech['bb_pos']}% vol={tech['vol_ratio']:.1f}x")

        # ── 신호 품질 사전 평가 (Claude 호출 전) ─────────────────
        # 기술 신호 사전 감지 (백테스트와 동일한 기준)
        _RULES_PRE = {
            # ── 독립 트리거 신호 (단독으로 의미 있는 신호) ──────────
            "rsi_oversold":    lambda t: t["rsi"] < 32,
            "bb_touch":        lambda t: t["bb_pos"] < 15,
            "volume_climax":   lambda t: t["vol_ratio"] > 1.8 and t["change_1d"] < -1.0,
            "momentum_dip":    lambda t: t["change_5d"] < -4.0,
            "sector_rebound":  lambda t: t["rsi"] < 40 and t.get("change_3d", t.get("change_5d", 0)) < -2.0,
            "rsi_divergence":  lambda t: t.get("rsi_divergence", False),
            "52w_low_zone":    lambda t: t.get("52w_pos", 50) < 15,
            "vol_accumulation":lambda t: t.get("vol_accumulation", False),
            # ── ma_support: 보조 신호 (다른 독립 신호와 함께일 때만 의미)
            # Doc3: 단독 발동 시 1일 54%, 스윙 61.8% → 독립 트리거로는 약함
            # 아래 필터에서 ma_support 단독이면 신호 목록에서 제거
            "ma_support":      lambda t: (t["ma50"] is not None and
                                          abs(t["price"] - t["ma50"]) / t["ma50"] < 0.025),
        }

        # ma_support 단독 제거 필터
        # 다른 강한 신호 없이 ma_support만 있으면 신호 불발
        _STRONG_SIGNALS = {"rsi_oversold","bb_touch","volume_climax",
                           "sector_rebound","vol_accumulation","52w_low_zone"}
        signals_fired_pre = [sig for sig, rule in _RULES_PRE.items() if rule(tech)]

        # ma_support 단독 발동 필터: 강한 신호 없으면 제거 (독립 트리거 금지)
        if signals_fired_pre == ["ma_support"]:
            log.debug(f"  {ticker} ma_support 단독 → 독립 트리거 제외 (보조 신호 전용)")
            signals_fired_pre = []
        elif "ma_support" in signals_fired_pre and not (_STRONG_SIGNALS & set(signals_fired_pre)):
            # ma_support + momentum_dip만 있고 강한 신호 없으면 제외
            signals_fired_pre = [s for s in signals_fired_pre if s != "ma_support"]

        quality = _calc_signal_quality(
            signals  = signals_fired_pre,
            tech     = tech,
            aria     = aria,
            ticker   = ticker,
            weights  = _load_weights(),
        )
        info["_quality"] = quality  # agent_analyst에서 접근

        log.info(
            f"    신호품질: {quality['quality_score']}점({quality['quality_label']})"
            f" | family:{quality['signal_family']}"
            f" | 임계:{quality['skip_threshold']}"
            f" | vix:{quality['vix_used']:.0f}"
            f" | 신호:{signals_fired_pre}"
        )
        log.info(f"    근거: {', '.join(quality['reasons'][:3])}")
        if quality.get("negative_veto"):
            log.info(f"    ⚠️ NegVeto: {quality.get('negative_reasons', [])}")

        # 품질 45 미만 → Claude 호출 스킵 (Doc2/3 반박 수용)
        # shadow_record는 반드시 저장 → 나중에 "버린 신호의 실제 성과" 추적 가능
        if quality["skip"]:
            log.info(
                f"    ⛔ 신호품질미달 {quality['quality_score']}점"
                f" (임계:{quality['skip_threshold']}, family:{quality['signal_family']})"
                f" → 스킵+shadow저장"
            )
            scanned += 1
            results.append({
                "ticker":        ticker,
                "name":          info["name"],
                "final_score":   quality["quality_score"],
                "signal_type":   "관망",
                "devil_verdict":  "",
                "rsi":           tech["rsi"],
                "change_5d":     tech.get("change_5d", "N/A"),
                "is_portfolio":  info.get("portfolio", True),
                "aria_reason":   "신호품질미달",
                "quality_score": quality["quality_score"],
                "quality_label": quality["quality_label"],
            })
            # shadow_record: 별도 파일 저장 (Doc3: ARIA accuracy/scan_log와 완전 분리)
            _save_shadow_log({
                "timestamp":        now_kst.isoformat(),
                "ticker":           ticker,
                "name":             info["name"],
                "market":           info["market"],
                "price_at_scan":    tech["price"],
                "rsi":              tech["rsi"],
                "bb_pos":           tech["bb_pos"],
                "vol_ratio":        tech["vol_ratio"],
                "vix":              macro["fred"].get("vix"),
                "hy_spread":        macro["fred"].get("hy_spread"),
                "yield_curve":      macro["fred"].get("yield_curve"),
                "aria_regime":      aria["regime"],
                "aria_sentiment":   aria["sentiment_score"],
                "aria_trend":       aria["trend"],
                # shadow: Claude 미호출, 품질 미달
                "analyst_score":    None,
                "analyst_confidence": None,
                "signals_fired":    signals_fired_pre,
                "bull_case":        None,
                "devil_score":      None,
                "devil_verdict":    None,
                "devil_objections": [],
                "thesis_killer_hit": False,
                "killer_detail":    "",
                "final_score":      quality["quality_score"],
                "signal_type":      "관망",
                "is_entry":         False,
                "reason":           f"신호품질미달({quality['quality_score']}점)",
                "quality_score":    quality["quality_score"],
                "quality_label":    quality["quality_label"],
                "quality_reasons":  quality["reasons"],
                "signal_family":    quality["signal_family"],
                "skip_threshold":   quality["skip_threshold"],
                "rebound_bonus":    quality.get("rebound_bonus", 0),
                "vix_used":         quality.get("vix_used", 0),
                "shadow_record":    True,     # Evolution이 이 필드로 구분 (scan_log와 분리)
                "shadow_log_path":  str(JACKAL_SHADOW_LOG),
                "alerted":          False,
                "outcome_checked":  False,
                "outcome_price":    None,
                "outcome_pct":      None,
                "outcome_correct":  None,
            })
            continue

        # ── Agent 1: Analyst ─────────────────────────────────────
        analyst = agent_analyst(ticker, info, tech, macro, aria)
        log.info(f"    Analyst: {analyst['analyst_score']}점 | {analyst['confidence']} | {analyst.get('signals_fired', [])}")

        # ── Agent 2: Devil ───────────────────────────────────────
        devil = agent_devil(ticker, info, tech, macro, aria, analyst)
        log.info(f"    Devil: {devil['verdict']} | {devil['devil_score']}점 | TK:{devil['thesis_killer_hit']}")

        # ── Final 판단 ───────────────────────────────────────────
        final = _final_judgment(analyst, devil)
        scanned += 1

        log.info(
            f"    Final: {final['final_score']:.0f}점 | {final['signal_type']} | is_entry={final['is_entry']}"
            f" | 품질:{quality['quality_score']}({quality['quality_label']})"
        )

        results.append({
            "ticker":       ticker,
            "name":         info["name"],
            "final_score":  final["final_score"],
            "signal_type":  final["signal_type"],
            "devil_verdict": devil.get("verdict", ""),
            "rsi":          tech["rsi"],
            "change_5d":    tech.get("change_5d", "N/A"),
            "is_portfolio": info.get("portfolio", True),
            "aria_reason":  info.get("reason", ""),
        })

        # ── 알림 발송 ─────────────────────────────────────────────
        if final["is_entry"] and final["final_score"] >= ALERT_THRESHOLD:
            msg = _build_alert_message(ticker, info, tech, analyst, devil, final, aria)
            ok  = _send_telegram(msg)
            if ok:
                _set_cooldown(ticker,
                             final.get("signals_fired", signals_fired_pre),
                             quality_score=quality.get("quality_score", 0))
                alerted += 1
                log.info(f"    ✅ 텔레그램 발송 완료")

        # ── 로그 저장 (Evolution 학습용) ──────────────────────────
        _save_log({
            "timestamp":        now_kst.isoformat(),
            "ticker":           ticker,
            "name":             info["name"],
            "market":           market,
            # 기술 지표
            "price_at_scan":    tech["price"],
            "rsi":              tech["rsi"],
            "bb_pos":           tech["bb_pos"],
            "vol_ratio":        tech["vol_ratio"],
            # 매크로
            "vix":              macro["fred"].get("vix"),
            "hy_spread":        macro["fred"].get("hy_spread"),
            "yield_curve":      macro["fred"].get("yield_curve"),
            # ARIA 컨텍스트
            "aria_regime":      aria["regime"],
            "aria_sentiment":   aria["sentiment_score"],
            "aria_trend":       aria["trend"],
            # Analyst
            "analyst_score":    analyst["analyst_score"],
            "analyst_confidence": analyst.get("confidence",""),
            "signals_fired":    analyst.get("signals_fired", []),
            "bull_case":        analyst.get("bull_case",""),
            # Devil
            "devil_score":      devil["devil_score"],
            "devil_verdict":    devil.get("verdict",""),
            "devil_objections": devil.get("objections", []),
            "thesis_killer_hit": devil.get("thesis_killer_hit", False),
            "killer_detail":    devil.get("killer_detail",""),
            # Final
            "final_score":      final["final_score"],
            "signal_type":      final["signal_type"],
            "is_entry":         final["is_entry"],
            "reason":           final.get("reason",""),
            # 결과 (Evolution이 4시간 후 채움)
            "alerted":          final["is_entry"] and final["final_score"] >= ALERT_THRESHOLD,
            "outcome_checked":  False,
            "outcome_price":    None,
            "outcome_pct":      None,
            "outcome_correct":  None,
        })

    log.info(f"📡 완료 | 분석 {scanned}종목 | 알림 {alerted}건")

    # 장이 열려있는데 타점 없으면 요약 발송
    any_open = (us_open or kr_open) or force
    if any_open and alerted == 0 and scanned > 0:
        _send_telegram(_build_summary_message(results, macro, aria))

    return {"scanned": scanned, "alerted": alerted}
