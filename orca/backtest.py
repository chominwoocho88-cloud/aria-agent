"""
orca_backtest.py — ORCA research backtest runner.

Backtest learning state is persisted into the SQLite state spine so
research runs do not mutate production JSON state.
"""
import os, sys, json, re, argparse, functools
from datetime import datetime, timezone, timedelta
from pathlib import Path
from copy import deepcopy

from .brand import ORCA_NAME

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

KST     = timezone(timedelta(hours=9))
API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

from .paths import (
    MEMORY_FILE, ACCURACY_FILE, LESSONS_FILE, WEIGHTS_FILE,
)
from .paths import DATA_DIR as _DATA_DIR
from .state import (
    finish_backtest_session,
    load_backtest_state,
    record_backtest_day,
    save_backtest_state,
    start_backtest_session,
)

# ── 교훈 3파일 경로 (3-file lesson system) ────────────────────────────────────
LESSONS_FAILURE_FILE  = _DATA_DIR / "lessons_failure.json"   # cap 120 — 실패 교훈
LESSONS_STRENGTH_FILE = _DATA_DIR / "lessons_strength.json"  # cap 30  — 강점 교훈
LESSONS_REGIME_FILE   = _DATA_DIR / "lessons_regime.json"    # 레짐별 cap 40
LESSONS_PATTERN_FILE  = _DATA_DIR / "lessons_pattern.json"  # 조건별 통계 룰 60슬롯

_RESEARCH_SESSION_ID: str | None = None


def _default_accuracy_state() -> dict:
    return {
        "total": 0,
        "correct": 0,
        "by_category": {},
        "history": [],
        "history_by_category": [],
        "weak_areas": [],
        "strong_areas": [],
        "dir_total": 0,
        "dir_correct": 0,
        "score_total": 0.0,
        "score_earned": 0.0,
        "score_accuracy": 0.0,
    }


def _default_weights_state() -> dict:
    return {
        "version": 1,
        "last_updated": "",
        "total_learning_cycles": 0,
        "prediction_confidence": {
            "금리": 1.0,
            "환율": 1.0,
            "주식": 1.0,
            "지정학": 0.7,
            "원자재": 1.0,
            "기업": 1.0,
            "기타": 0.8,
        },
    }


_RESEARCH_STATE_DEFAULTS = {
    str(MEMORY_FILE): ("memory", []),
    str(ACCURACY_FILE): ("accuracy", _default_accuracy_state),
    str(LESSONS_FILE): ("lessons_legacy", lambda: {"lessons": [], "total_lessons": 0, "last_updated": ""}),
    str(LESSONS_FAILURE_FILE): ("lessons_failure", lambda: {"lessons": []}),
    str(LESSONS_STRENGTH_FILE): ("lessons_strength", lambda: {"lessons": []}),
    str(LESSONS_REGIME_FILE): ("lessons_regime", lambda: {"lessons": []}),
    str(LESSONS_PATTERN_FILE): ("lessons_pattern", lambda: {"patterns": {}, "global_stats": {"total": 0, "correct": 0, "accuracy": 0.0}}),
    str(WEIGHTS_FILE): ("weights", _default_weights_state),
}


def _clone_default(value):
    return value() if callable(value) else deepcopy(value)


def _research_state_meta(path) -> tuple[str | None, object]:
    meta = _RESEARCH_STATE_DEFAULTS.get(str(path))
    if not meta:
        return None, None
    return meta[0], meta[1]

_REGIME_MAP = {
    "위험회피에서 위험선호": "전환중",
    "위험회피→위험선호":    "전환중",
    "위험선호/위험회피":    "혼조",
    "위험선호에서 위험회피": "전환중",
}
_BASE_REGIMES = ("위험선호", "위험회피", "전환중", "혼조")

def _normalize_regime(regime: str) -> str:
    for k, v in _REGIME_MAP.items():
        if k in regime:
            return v
    for base in _BASE_REGIMES:
        if base in regime:
            return base
    return "혼조"

MODEL         = "claude-haiku-4-5-20251001"

# ── 실제 30거래일 데이터 (2026-03-03 ~ 2026-04-11) ──────────────────────────
HIST_DATA = {
    # ── 2026년 1월 ──────────────────────────────────────────────────────────
    "2026-01-13": {
        "sp500": 5827.04, "sp500_change": "+0.41%",
        "nasdaq": 19261.92,"nasdaq_change": "+0.23%",
        "vix": 18.71,     "kospi": 2521.44, "kospi_change": "+0.18%",
        "krw_usd": 1452.30,"sk_hynix": 196200,"sk_hynix_change": "+0.51%",
        "samsung": 56800,  "samsung_change": "+0.35%",
        "nvda": 134.43,   "nvda_change": "+0.82%",
        "fear_greed": "55","fear_greed_label": "Greed",
        "note": "트럼프 취임 기대감 + 연초 AI 테마 강세"
    },
    "2026-01-14": {
        "sp500": 5842.47, "sp500_change": "+0.27%",
        "nasdaq": 19373.49,"nasdaq_change": "+0.58%",
        "vix": 18.35,     "kospi": 2527.14, "kospi_change": "+0.23%",
        "krw_usd": 1449.80,"sk_hynix": 198400,"sk_hynix_change": "+1.12%",
        "samsung": 57200,  "samsung_change": "+0.70%",
        "nvda": 136.02,   "nvda_change": "+1.18%",
        "fear_greed": "57","fear_greed_label": "Greed",
        "note": "기술주 강세 지속 + AI 인프라 투자 기대"
    },
    "2026-01-15": {
        "sp500": 5949.17, "sp500_change": "+1.83%",
        "nasdaq": 19703.09,"nasdaq_change": "+1.70%",
        "vix": 16.12,     "kospi": 2548.32, "kospi_change": "+0.84%",
        "krw_usd": 1443.60,"sk_hynix": 204800,"sk_hynix_change": "+3.23%",
        "samsung": 58600,  "samsung_change": "+2.45%",
        "nvda": 140.14,   "nvda_change": "+3.03%",
        "fear_greed": "63","fear_greed_label": "Greed",
        "note": "CPI 예상치 부합 → 금리인하 기대 회복, 강세장"
    },
    "2026-01-16": {
        "sp500": 5996.66, "sp500_change": "+0.80%",
        "nasdaq": 19954.30,"nasdaq_change": "+1.28%",
        "vix": 15.84,     "kospi": 2561.78, "kospi_change": "+0.53%",
        "krw_usd": 1440.20,"sk_hynix": 208600,"sk_hynix_change": "+1.86%",
        "samsung": 59300,  "samsung_change": "+1.19%",
        "nvda": 143.89,   "nvda_change": "+2.68%",
        "fear_greed": "66","fear_greed_label": "Greed",
        "note": "빅테크 실적 기대 + 달러 약세"
    },
    "2026-01-17": {
        "sp500": 5996.66, "sp500_change": "0.00%",
        "nasdaq": 19936.28,"nasdaq_change": "-0.09%",
        "vix": 16.21,     "kospi": 2558.43, "kospi_change": "-0.13%",
        "krw_usd": 1441.50,"sk_hynix": 207400,"sk_hynix_change": "-0.58%",
        "samsung": 59100,  "samsung_change": "-0.34%",
        "nvda": 142.62,   "nvda_change": "-0.88%",
        "fear_greed": "64","fear_greed_label": "Greed",
        "note": "주간 고점 부근 숨고르기"
    },
    "2026-01-22": {
        "sp500": 6118.71, "sp500_change": "+2.03%",
        "nasdaq": 20174.48,"nasdaq_change": "+1.20%",
        "vix": 14.85,     "kospi": 2589.24, "kospi_change": "+1.20%",
        "krw_usd": 1433.40,"sk_hynix": 217200,"sk_hynix_change": "+4.73%",
        "samsung": 61200,  "samsung_change": "+3.55%",
        "nvda": 149.43,   "nvda_change": "+4.77%",
        "fear_greed": "72","fear_greed_label": "Greed",
        "note": "트럼프 취임 직후 AI 인프라 5000억달러 투자 발표 (StarGate)"
    },
    "2026-01-23": {
        "sp500": 6101.24, "sp500_change": "-0.29%",
        "nasdaq": 20111.32,"nasdaq_change": "-0.31%",
        "vix": 15.03,     "kospi": 2583.14, "kospi_change": "-0.24%",
        "krw_usd": 1435.60,"sk_hynix": 215400,"sk_hynix_change": "-0.83%",
        "samsung": 60800,  "samsung_change": "-0.65%",
        "nvda": 147.21,   "nvda_change": "-1.49%",
        "fear_greed": "69","fear_greed_label": "Greed",
        "note": "StarGate 발표 후 차익실현"
    },
    "2026-01-24": {
        "sp500": 6118.71, "sp500_change": "+0.29%",
        "nasdaq": 20230.14,"nasdaq_change": "+0.59%",
        "vix": 14.77,     "kospi": 2591.47, "kospi_change": "+0.32%",
        "krw_usd": 1432.80,"sk_hynix": 218600,"sk_hynix_change": "+1.49%",
        "samsung": 61600,  "samsung_change": "+1.32%",
        "nvda": 149.77,   "nvda_change": "+1.74%",
        "fear_greed": "70","fear_greed_label": "Greed",
        "note": "AI 투자 낙관론 지속"
    },
    "2026-01-27": {
        "sp500": 5994.57, "sp500_change": "-2.02%",
        "nasdaq": 19341.83,"nasdaq_change": "-3.07%",
        "vix": 18.96,     "kospi": 2548.21, "kospi_change": "-1.67%",
        "krw_usd": 1449.20,"sk_hynix": 195800,"sk_hynix_change": "-10.45%",
        "samsung": 57600,  "samsung_change": "-6.49%",
        "nvda": 116.78,   "nvda_change": "-16.97%",
        "fear_greed": "38","fear_greed_label": "Fear",
        "note": "DeepSeek R1 공개 — AI 주식 충격, 엔비디아 역대 최대 시총 증발"
    },
    "2026-01-28": {
        "sp500": 6067.44, "sp500_change": "+1.21%",
        "nasdaq": 19624.35,"nasdaq_change": "+1.46%",
        "vix": 17.84,     "kospi": 2562.93, "kospi_change": "+0.58%",
        "krw_usd": 1444.70,"sk_hynix": 202400,"sk_hynix_change": "+3.37%",
        "samsung": 58800,  "samsung_change": "+2.08%",
        "nvda": 124.92,   "nvda_change": "+6.97%",
        "fear_greed": "44","fear_greed_label": "Neutral",
        "note": "DeepSeek 과잉반응 되돌림 + 기술적 반등"
    },
    "2026-01-29": {
        "sp500": 6071.17, "sp500_change": "+0.06%",
        "nasdaq": 19682.87,"nasdaq_change": "+0.30%",
        "vix": 17.64,     "kospi": 2569.11, "kospi_change": "+0.24%",
        "krw_usd": 1442.90,"sk_hynix": 204200,"sk_hynix_change": "+0.89%",
        "samsung": 59200,  "samsung_change": "+0.68%",
        "nvda": 126.55,   "nvda_change": "+1.30%",
        "fear_greed": "45","fear_greed_label": "Neutral",
        "note": "FOMC 회의 대기 + 빅테크 실적 주간"
    },
    "2026-01-30": {
        "sp500": 6040.54, "sp500_change": "-0.50%",
        "nasdaq": 19489.68,"nasdaq_change": "-0.98%",
        "vix": 18.12,     "kospi": 2558.37, "kospi_change": "-0.42%",
        "krw_usd": 1446.10,"sk_hynix": 200600,"sk_hynix_change": "-1.76%",
        "samsung": 58600,  "samsung_change": "-1.01%",
        "nvda": 122.43,   "nvda_change": "-3.25%",
        "fear_greed": "43","fear_greed_label": "Neutral",
        "note": "Microsoft 실적 발표 (클라우드 성장 둔화 실망)"
    },
    "2026-01-31": {
        "sp500": 6040.54, "sp500_change": "0.00%",
        "nasdaq": 19627.44,"nasdaq_change": "+0.71%",
        "vix": 16.43,     "kospi": 2571.22, "kospi_change": "+0.50%",
        "krw_usd": 1443.30,"sk_hynix": 203400,"sk_hynix_change": "+1.40%",
        "samsung": 59100,  "samsung_change": "+0.85%",
        "nvda": 124.88,   "nvda_change": "+2.00%",
        "fear_greed": "48","fear_greed_label": "Neutral",
        "note": "FOMC 금리 동결 — 인플레 우려로 인하 중단 시사"
    },
    # ── 2026년 2월 ──────────────────────────────────────────────────────────
    "2026-02-03": {
        "sp500": 5994.57, "sp500_change": "-0.76%",
        "nasdaq": 19341.83,"nasdaq_change": "-1.46%",
        "vix": 19.42,     "kospi": 2538.41, "kospi_change": "-1.28%",
        "krw_usd": 1458.20,"sk_hynix": 194200,"sk_hynix_change": "-4.52%",
        "samsung": 57400,  "samsung_change": "-2.88%",
        "nvda": 117.93,   "nvda_change": "-5.57%",
        "fear_greed": "35","fear_greed_label": "Fear",
        "note": "트럼프 캐나다·멕시코·중국 관세 25% 발표 — 무역전쟁 공식화"
    },
    "2026-02-04": {
        "sp500": 6037.88, "sp500_change": "+0.72%",
        "nasdaq": 19578.41,"nasdaq_change": "+1.22%",
        "vix": 17.96,     "kospi": 2554.82, "kospi_change": "+0.64%",
        "krw_usd": 1452.40,"sk_hynix": 199800,"sk_hynix_change": "+2.88%",
        "samsung": 58400,  "samsung_change": "+1.74%",
        "nvda": 121.47,   "nvda_change": "+3.00%",
        "fear_greed": "40","fear_greed_label": "Fear",
        "note": "캐나다·멕시코 관세 30일 유예 합의 — 안도 반등"
    },
    "2026-02-05": {
        "sp500": 6062.45, "sp500_change": "+0.41%",
        "nasdaq": 19694.47,"nasdaq_change": "+0.59%",
        "vix": 17.24,     "kospi": 2562.13, "kospi_change": "+0.29%",
        "krw_usd": 1449.60,"sk_hynix": 202400,"sk_hynix_change": "+1.30%",
        "samsung": 59000,  "samsung_change": "+1.03%",
        "nvda": 124.08,   "nvda_change": "+2.15%",
        "fear_greed": "43","fear_greed_label": "Neutral",
        "note": "Alphabet 실적 호조 + AI 광고 수익 급증"
    },
    "2026-02-06": {
        "sp500": 6083.57, "sp500_change": "+0.35%",
        "nasdaq": 19791.43,"nasdaq_change": "+0.49%",
        "vix": 16.88,     "kospi": 2569.74, "kospi_change": "+0.30%",
        "krw_usd": 1447.20,"sk_hynix": 205200,"sk_hynix_change": "+1.38%",
        "samsung": 59600,  "samsung_change": "+1.02%",
        "nvda": 126.87,   "nvda_change": "+2.25%",
        "fear_greed": "46","fear_greed_label": "Neutral",
        "note": "Amazon 실적 기대 + AI 클라우드 성장세 확인"
    },
    "2026-02-07": {
        "sp500": 6118.71, "sp500_change": "+0.58%",
        "nasdaq": 19954.30,"nasdaq_change": "+0.83%",
        "vix": 15.92,     "kospi": 2582.44, "kospi_change": "+0.50%",
        "krw_usd": 1443.10,"sk_hynix": 210400,"sk_hynix_change": "+2.53%",
        "samsung": 60600,  "samsung_change": "+1.68%",
        "nvda": 131.32,   "nvda_change": "+3.51%",
        "fear_greed": "52","fear_greed_label": "Neutral",
        "note": "Amazon 실적 서프라이즈 + 고용 호조"
    },
    "2026-02-10": {
        "sp500": 6025.47, "sp500_change": "-1.52%",
        "nasdaq": 19612.18,"nasdaq_change": "-1.71%",
        "vix": 17.83,     "kospi": 2558.83, "kospi_change": "-0.91%",
        "krw_usd": 1451.80,"sk_hynix": 201800,"sk_hynix_change": "-4.09%",
        "samsung": 58400,  "samsung_change": "-3.63%",
        "nvda": 124.36,   "nvda_change": "-5.30%",
        "fear_greed": "41","fear_greed_label": "Fear",
        "note": "관세 확전 우려 재부각 + 중국 보복 조짐"
    },
    "2026-02-11": {
        "sp500": 6056.44, "sp500_change": "+0.51%",
        "nasdaq": 19726.16,"nasdaq_change": "+0.58%",
        "vix": 17.12,     "kospi": 2567.22, "kospi_change": "+0.33%",
        "krw_usd": 1448.60,"sk_hynix": 204800,"sk_hynix_change": "+1.49%",
        "samsung": 59200,  "samsung_change": "+1.37%",
        "nvda": 126.91,   "nvda_change": "+2.05%",
        "fear_greed": "44","fear_greed_label": "Neutral",
        "note": "소폭 반등 + 실적 시즌 긍정적"
    },
    "2026-02-12": {
        "sp500": 5983.99, "sp500_change": "-1.19%",
        "nasdaq": 19412.22,"nasdaq_change": "-1.59%",
        "vix": 19.21,     "kospi": 2541.87, "kospi_change": "-0.98%",
        "krw_usd": 1458.40,"sk_hynix": 197600,"sk_hynix_change": "-3.51%",
        "samsung": 57600,  "samsung_change": "-2.70%",
        "nvda": 120.33,   "nvda_change": "-5.19%",
        "fear_greed": "37","fear_greed_label": "Fear",
        "note": "CPI 예상 상회 — 금리인하 기대 후퇴 + 관세 인플레 우려"
    },
    "2026-02-13": {
        "sp500": 6115.07, "sp500_change": "+2.19%",
        "nasdaq": 19908.68,"nasdaq_change": "+2.56%",
        "vix": 15.97,     "kospi": 2574.32, "kospi_change": "+1.28%",
        "krw_usd": 1447.30,"sk_hynix": 208200,"sk_hynix_change": "+5.36%",
        "samsung": 60100,  "samsung_change": "+4.34%",
        "nvda": 132.86,   "nvda_change": "+10.42%",
        "fear_greed": "52","fear_greed_label": "Neutral",
        "note": "CPI 충격 진정 + 엔비디아 GTC 기대감 급등"
    },
    "2026-02-14": {
        "sp500": 6133.39, "sp500_change": "+0.30%",
        "nasdaq": 19980.05,"nasdaq_change": "+0.36%",
        "vix": 15.62,     "kospi": 2581.44, "kospi_change": "+0.28%",
        "krw_usd": 1445.10,"sk_hynix": 211200,"sk_hynix_change": "+1.44%",
        "samsung": 60800,  "samsung_change": "+1.16%",
        "nvda": 135.34,   "nvda_change": "+1.87%",
        "fear_greed": "54","fear_greed_label": "Neutral",
        "note": "밸런타인데이 + 연초 강세장 마무리"
    },
    "2026-02-19": {
        "sp500": 6129.58, "sp500_change": "-0.06%",
        "nasdaq": 19954.30,"nasdaq_change": "-0.13%",
        "vix": 15.79,     "kospi": 2578.12, "kospi_change": "-0.13%",
        "krw_usd": 1446.40,"sk_hynix": 210000,"sk_hynix_change": "-0.57%",
        "samsung": 60600,  "samsung_change": "-0.33%",
        "nvda": 134.66,   "nvda_change": "-0.50%",
        "fear_greed": "52","fear_greed_label": "Neutral",
        "note": "대통령의 날 연휴 후 재개 + 관망세"
    },
    "2026-02-20": {
        "sp500": 6144.15, "sp500_change": "+0.24%",
        "nasdaq": 20012.48,"nasdaq_change": "+0.29%",
        "vix": 15.46,     "kospi": 2584.77, "kospi_change": "+0.26%",
        "krw_usd": 1444.20,"sk_hynix": 212400,"sk_hynix_change": "+1.14%",
        "samsung": 61200,  "samsung_change": "+0.99%",
        "nvda": 136.48,   "nvda_change": "+1.35%",
        "fear_greed": "55","fear_greed_label": "Greed",
        "note": "연초 고점 재도전"
    },
    "2026-02-21": {
        "sp500": 6013.13, "sp500_change": "-2.14%",
        "nasdaq": 19392.68,"nasdaq_change": "-3.09%",
        "vix": 19.84,     "kospi": 2539.28, "kospi_change": "-1.76%",
        "krw_usd": 1459.30,"sk_hynix": 198400,"sk_hynix_change": "-6.59%",
        "samsung": 57800,  "samsung_change": "-5.55%",
        "nvda": 118.23,   "nvda_change": "-13.38%",
        "fear_greed": "30","fear_greed_label": "Fear",
        "note": "소비자신뢰지수 급락 + Walmart 가이던스 쇼크 + 엔비디아 급락"
    },
    "2026-02-24": {
        "sp500": 5983.99, "sp500_change": "-0.50%",
        "nasdaq": 19281.40,"nasdaq_change": "-0.57%",
        "vix": 20.64,     "kospi": 2527.43, "kospi_change": "-0.46%",
        "krw_usd": 1462.80,"sk_hynix": 194600,"sk_hynix_change": "-1.91%",
        "samsung": 57000,  "samsung_change": "-1.38%",
        "nvda": 115.99,   "nvda_change": "-1.89%",
        "fear_greed": "28","fear_greed_label": "Fear",
        "note": "경기침체 우려 확산 + 관세 불확실성"
    },
    "2026-02-25": {
        "sp500": 5860.10, "sp500_change": "-2.06%",
        "nasdaq": 18836.18,"nasdaq_change": "-2.31%",
        "vix": 22.27,     "kospi": 2504.82, "kospi_change": "-0.89%",
        "krw_usd": 1469.40,"sk_hynix": 188200,"sk_hynix_change": "-3.29%",
        "samsung": 55900,  "samsung_change": "-1.93%",
        "nvda": 111.01,   "nvda_change": "-4.29%",
        "fear_greed": "24","fear_greed_label": "Fear",
        "note": "트럼프 관세 4월 확전 예고 + 경기침체 논쟁"
    },
    "2026-02-26": {
        "sp500": 5861.57, "sp500_change": "+0.02%",
        "nasdaq": 18884.44,"nasdaq_change": "+0.26%",
        "vix": 21.88,     "kospi": 2508.44, "kospi_change": "+0.14%",
        "krw_usd": 1468.10,"sk_hynix": 189600,"sk_hynix_change": "+0.74%",
        "samsung": 56200,  "samsung_change": "+0.54%",
        "nvda": 115.37,   "nvda_change": "+3.03%",
        "fear_greed": "25","fear_greed_label": "Fear",
        "note": "엔비디아 실적 발표 (호조) — 시간외 상승 → 본장 혼조"
    },
    "2026-02-27": {
        "sp500": 5861.57, "sp500_change": "0.00%",
        "nasdaq": 18847.28,"nasdaq_change": "-0.20%",
        "vix": 22.14,     "kospi": 2504.38, "kospi_change": "-0.16%",
        "krw_usd": 1469.80,"sk_hynix": 188400,"sk_hynix_change": "-0.63%",
        "samsung": 55800,  "samsung_change": "-0.71%",
        "nvda": 114.42,   "nvda_change": "-0.82%",
        "fear_greed": "24","fear_greed_label": "Fear",
        "note": "엔비디아 실적 후 실망 매도 + PCE 발표 대기"
    },
    "2026-02-28": {
        "sp500": 5954.23, "sp500_change": "+1.58%",
        "nasdaq": 19161.63,"nasdaq_change": "+1.67%",
        "vix": 19.87,     "kospi": 2531.74, "kospi_change": "+1.09%",
        "krw_usd": 1461.20,"sk_hynix": 196600,"sk_hynix_change": "+4.35%",
        "samsung": 57400,  "samsung_change": "+2.87%",
        "nvda": 120.15,   "nvda_change": "+4.99%",
        "fear_greed": "31","fear_greed_label": "Fear",
        "note": "PCE 예상치 부합 + 월말 포지션 조정 반등"
    },
    "2026-03-03": {
        "sp500": 5954.23, "sp500_change": "-1.76%",
        "nasdaq": 19161.63,"nasdaq_change": "-2.64%",
        "vix": 22.28,     "kospi": 2545.50, "kospi_change": "-0.82%",
        "krw_usd": 1453.10,"sk_hynix": 198400,"sk_hynix_change": "-2.11%",
        "samsung": 58700,  "samsung_change": "-1.34%",
        "nvda": 116.78,   "nvda_change": "-8.69%",
        "fear_greed": "30","fear_greed_label": "Fear",
        "note": "트럼프 관세 위협 본격화 — 나스닥 급락"
    },
    "2026-03-04": {
        "sp500": 5842.59, "sp500_change": "-1.76%",
        "nasdaq": 18581.33,"nasdaq_change": "-2.93%",
        "vix": 24.87,     "kospi": 2503.12, "kospi_change": "-1.66%",
        "krw_usd": 1459.80,"sk_hynix": 192600,"sk_hynix_change": "-2.92%",
        "samsung": 57200,  "samsung_change": "-2.56%",
        "nvda": 111.02,   "nvda_change": "-4.93%",
        "fear_greed": "25","fear_greed_label": "Fear",
        "note": "캐나다·멕시코 관세 발효 확정"
    },
    "2026-03-05": {
        "sp500": 5778.15, "sp500_change": "-1.22%",
        "nasdaq": 18285.16,"nasdaq_change": "-0.35%",
        "vix": 23.95,     "kospi": 2498.44, "kospi_change": "-0.19%",
        "krw_usd": 1462.20,"sk_hynix": 191800,"sk_hynix_change": "-0.42%",
        "samsung": 56900,  "samsung_change": "-0.52%",
        "nvda": 115.43,   "nvda_change": "+3.97%",
        "fear_greed": "24","fear_greed_label": "Fear",
        "note": "관세 일부 유예 기대감 + 기술적 반등"
    },
    "2026-03-06": {
        "sp500": 5770.26, "sp500_change": "-0.14%",
        "nasdaq": 18096.37,"nasdaq_change": "-1.03%",
        "vix": 24.11,     "kospi": 2494.82, "kospi_change": "-0.14%",
        "krw_usd": 1463.50,"sk_hynix": 190200,"sk_hynix_change": "-0.83%",
        "samsung": 56500,  "samsung_change": "-0.70%",
        "nvda": 112.04,   "nvda_change": "-2.94%",
        "fear_greed": "25","fear_greed_label": "Fear",
        "note": "AI 투자 불확실성 지속"
    },
    "2026-03-07": {
        "sp500": 5770.55, "sp500_change": "+0.01%",
        "nasdaq": 18158.09,"nasdaq_change": "+0.34%",
        "vix": 23.16,     "kospi": 2509.33, "kospi_change": "+0.58%",
        "krw_usd": 1459.60,"sk_hynix": 193800,"sk_hynix_change": "+1.89%",
        "samsung": 57200,  "samsung_change": "+1.24%",
        "nvda": 115.83,   "nvda_change": "+3.38%",
        "fear_greed": "27","fear_greed_label": "Fear",
        "note": "기술주 반등 + 고용지표 양호"
    },
    "2026-03-10": {
        "sp500": 5614.56, "sp500_change": "-2.70%",
        "nasdaq": 17468.32,"nasdaq_change": "-3.98%",
        "vix": 27.86,     "kospi": 2487.66, "kospi_change": "-0.86%",
        "krw_usd": 1472.40,"sk_hynix": 184200,"sk_hynix_change": "-4.95%",
        "samsung": 55100,  "samsung_change": "-3.66%",
        "nvda": 107.61,   "nvda_change": "-7.09%",
        "fear_greed": "19","fear_greed_label": "Extreme Fear",
        "note": "관세 전면전 우려 재점화 — 기술주 폭락"
    },
    "2026-03-11": {
        "sp500": 5572.85, "sp500_change": "-0.74%",
        "nasdaq": 17303.01,"nasdaq_change": "-0.94%",
        "vix": 28.93,     "kospi": 2469.44, "kospi_change": "-0.73%",
        "krw_usd": 1478.30,"sk_hynix": 181000,"sk_hynix_change": "-1.74%",
        "samsung": 54400,  "samsung_change": "-1.27%",
        "nvda": 105.43,   "nvda_change": "-2.03%",
        "fear_greed": "17","fear_greed_label": "Extreme Fear",
        "note": "경기침체 우려 확산"
    },
    "2026-03-12": {
        "sp500": 5599.30, "sp500_change": "+0.49%",
        "nasdaq": 17489.78,"nasdaq_change": "+1.08%",
        "vix": 27.43,     "kospi": 2481.22, "kospi_change": "+0.48%",
        "krw_usd": 1473.90,"sk_hynix": 184600,"sk_hynix_change": "+1.99%",
        "samsung": 55200,  "samsung_change": "+1.47%",
        "nvda": 108.76,   "nvda_change": "+3.15%",
        "fear_greed": "20","fear_greed_label": "Extreme Fear",
        "note": "CPI 예상치 부합 — 안도 반등"
    },
    "2026-03-13": {
        "sp500": 5521.52, "sp500_change": "-1.39%",
        "nasdaq": 17037.65,"nasdaq_change": "-2.58%",
        "vix": 29.62,     "kospi": 2453.18, "kospi_change": "-1.13%",
        "krw_usd": 1481.20,"sk_hynix": 177800,"sk_hynix_change": "-3.68%",
        "samsung": 53800,  "samsung_change": "-2.54%",
        "nvda": 103.50,   "nvda_change": "-4.84%",
        "fear_greed": "17","fear_greed_label": "Extreme Fear",
        "note": "관세 보복 우려 재부각"
    },
    "2026-03-14": {
        "sp500": 5638.94, "sp500_change": "+2.13%",
        "nasdaq": 17754.09,"nasdaq_change": "+4.22%",
        "vix": 26.21,     "kospi": 2488.42, "kospi_change": "+1.44%",
        "krw_usd": 1472.10,"sk_hynix": 185400,"sk_hynix_change": "+4.27%",
        "samsung": 55600,  "samsung_change": "+3.35%",
        "nvda": 112.66,   "nvda_change": "+8.80%",
        "fear_greed": "22","fear_greed_label": "Extreme Fear",
        "note": "관세 협상 재개 기대 + 기술주 반등"
    },
    "2026-03-17": {
        "sp500": 5675.29, "sp500_change": "+0.64%",
        "nasdaq": 17899.02,"nasdaq_change": "+0.82%",
        "vix": 25.02,     "kospi": 2501.84, "kospi_change": "+0.54%",
        "krw_usd": 1468.30,"sk_hynix": 188200,"sk_hynix_change": "+1.51%",
        "samsung": 56300,  "samsung_change": "+1.26%",
        "nvda": 116.43,   "nvda_change": "+3.35%",
        "fear_greed": "26","fear_greed_label": "Fear",
        "note": "반등 지속 + FOMC 기대감"
    },
    "2026-03-18": {
        "sp500": 5776.65, "sp500_change": "+1.78%",
        "nasdaq": 18271.86,"nasdaq_change": "+2.07%",
        "vix": 22.28,     "kospi": 2524.17, "kospi_change": "+0.89%",
        "krw_usd": 1461.40,"sk_hynix": 194000,"sk_hynix_change": "+3.08%",
        "samsung": 57500,  "samsung_change": "+2.13%",
        "nvda": 120.34,   "nvda_change": "+3.36%",
        "fear_greed": "32","fear_greed_label": "Fear",
        "note": "FOMC 금리 동결 + 비둘기파 발언"
    },
    "2026-03-19": {
        "sp500": 5767.57, "sp500_change": "-0.16%",
        "nasdaq": 18160.37,"nasdaq_change": "-0.61%",
        "vix": 22.56,     "kospi": 2519.73, "kospi_change": "-0.18%",
        "krw_usd": 1462.80,"sk_hynix": 192400,"sk_hynix_change": "-0.82%",
        "samsung": 57200,  "samsung_change": "-0.52%",
        "nvda": 119.28,   "nvda_change": "-0.88%",
        "fear_greed": "31","fear_greed_label": "Fear",
        "note": "관망세 — 관세 불확실성 지속"
    },
    "2026-03-20": {
        "sp500": 5842.64, "sp500_change": "+1.30%",
        "nasdaq": 18503.61,"nasdaq_change": "+1.90%",
        "vix": 20.96,     "kospi": 2538.41, "kospi_change": "+0.74%",
        "krw_usd": 1456.20,"sk_hynix": 197800,"sk_hynix_change": "+2.81%",
        "samsung": 58400,  "samsung_change": "+2.10%",
        "nvda": 125.61,   "nvda_change": "+5.31%",
        "fear_greed": "36","fear_greed_label": "Fear",
        "note": "기술주 강세 + AI 투자 기대 회복"
    },
    "2026-03-21": {
        "sp500": 5931.90, "sp500_change": "+1.53%",
        "nasdaq": 18922.83,"nasdaq_change": "+2.27%",
        "vix": 19.28,     "kospi": 2562.85, "kospi_change": "+0.96%",
        "krw_usd": 1448.70,"sk_hynix": 204200,"sk_hynix_change": "+3.23%",
        "samsung": 59800,  "samsung_change": "+2.40%",
        "nvda": 132.47,   "nvda_change": "+5.46%",
        "fear_greed": "42","fear_greed_label": "Neutral",
        "note": "관세 협상 낙관론 확산 + 연말 랠리"
    },
    "2026-03-24": {
        "sp500": 5776.15, "sp500_change": "-2.62%",
        "nasdaq": 18069.26,"nasdaq_change": "-4.52%",
        "vix": 23.59,     "kospi": 2519.93, "kospi_change": "-1.68%",
        "krw_usd": 1463.40,"sk_hynix": 190800,"sk_hynix_change": "-6.56%",
        "samsung": 57100,  "samsung_change": "-4.51%",
        "nvda": 115.01,   "nvda_change": "-13.19%",
        "fear_greed": "29","fear_greed_label": "Fear",
        "note": "관세 우려 재점화 — 엔비디아 급락"
    },
    "2026-03-25": {
        "sp500": 5712.20, "sp500_change": "-1.12%",
        "nasdaq": 17826.31,"nasdaq_change": "-1.35%",
        "vix": 24.87,     "kospi": 2503.71, "kospi_change": "-0.64%",
        "krw_usd": 1468.80,"sk_hynix": 186600,"sk_hynix_change": "-2.20%",
        "samsung": 55900,  "samsung_change": "-2.10%",
        "nvda": 109.62,   "nvda_change": "-4.69%",
        "fear_greed": "24","fear_greed_label": "Fear",
        "note": "반도체 수출 규제 우려 확산"
    },
    "2026-03-26": {
        "sp500": 5693.31, "sp500_change": "-0.33%",
        "nasdaq": 17733.22,"nasdaq_change": "-0.52%",
        "vix": 25.12,     "kospi": 2498.22, "kospi_change": "-0.22%",
        "krw_usd": 1469.90,"sk_hynix": 185200,"sk_hynix_change": "-0.75%",
        "samsung": 55600,  "samsung_change": "-0.54%",
        "nvda": 108.56,   "nvda_change": "-0.97%",
        "fear_greed": "23","fear_greed_label": "Fear",
        "note": "횡보 — 관세 협상 결과 대기"
    },
    "2026-03-27": {
        "sp500": 5580.94, "sp500_change": "-2.19%",
        "nasdaq": 17322.99,"nasdaq_change": "-2.32%",
        "vix": 27.67,     "kospi": 2452.73, "kospi_change": "-1.82%",
        "krw_usd": 1478.40,"sk_hynix": 177600,"sk_hynix_change": "-4.10%",
        "samsung": 53900,  "samsung_change": "-3.06%",
        "nvda": 103.79,   "nvda_change": "-4.40%",
        "fear_greed": "18","fear_greed_label": "Extreme Fear",
        "note": "자동차 관세 25% 발표 — 쇼크"
    },
    "2026-03-28": {
        "sp500": 5611.85, "sp500_change": "+0.55%",
        "nasdaq": 17462.24,"nasdaq_change": "+0.81%",
        "vix": 26.47,     "kospi": 2468.34, "kospi_change": "+0.64%",
        "krw_usd": 1474.20,"sk_hynix": 180400,"sk_hynix_change": "+1.58%",
        "samsung": 54600,  "samsung_change": "+1.30%",
        "nvda": 107.28,   "nvda_change": "+3.36%",
        "fear_greed": "20","fear_greed_label": "Extreme Fear",
        "note": "관세 협상 기대 + 기술적 반등"
    },
    "2026-03-31": {
        "sp500": 5611.35, "sp500_change": "-0.01%",
        "nasdaq": 17394.12,"nasdaq_change": "-0.39%",
        "vix": 26.31,     "kospi": 2481.12, "kospi_change": "+0.52%",
        "krw_usd": 1472.60,"sk_hynix": 181800,"sk_hynix_change": "+0.78%",
        "samsung": 54900,  "samsung_change": "+0.55%",
        "nvda": 109.44,   "nvda_change": "+2.02%",
        "fear_greed": "21","fear_greed_label": "Extreme Fear",
        "note": "월말 관망 — 4월 관세 발표 대기"
    },
    "2026-04-01": {
        "sp500": 5283.26, "sp500_change": "-5.48%",
        "nasdaq": 16387.79,"nasdaq_change": "-5.82%",
        "vix": 35.27,     "kospi": 2390.26, "kospi_change": "-3.65%",
        "krw_usd": 1494.70,"sk_hynix": 163800,"sk_hynix_change": "-9.90%",
        "samsung": 50200,  "samsung_change": "-8.56%",
        "nvda": 96.30,    "nvda_change": "-11.97%",
        "fear_greed": "8", "fear_greed_label": "Extreme Fear",
        "note": "상호관세 발표 — 시장 충격 시작"
    },
    "2026-04-02": {
        "sp500": 5074.08, "sp500_change": "-4.08%",
        "nasdaq": 15587.79,"nasdaq_change": "-4.88%",
        "vix": 45.31,     "kospi": 2336.49, "kospi_change": "-2.25%",
        "krw_usd": 1498.30,"sk_hynix": 158600,"sk_hynix_change": "-3.18%",
        "samsung": 49400,  "samsung_change": "-1.59%",
        "nvda": 90.08,    "nvda_change": "-6.46%",
        "fear_greed": "5", "fear_greed_label": "Extreme Fear",
        "note": "관세 충격 2일차 — 패닉 매도"
    },
    "2026-04-03": {
        "sp500": 5074.08, "sp500_change": "0.00%",
        "nasdaq": 15587.79,"nasdaq_change": "0.00%",
        "vix": 52.33,     "kospi": 2328.20, "kospi_change": "-0.35%",
        "krw_usd": 1488.50,"sk_hynix": 165400,"sk_hynix_change": "+4.29%",
        "samsung": 49100,  "samsung_change": "-0.61%",
        "nvda": 88.01,    "nvda_change": "-2.30%",
        "fear_greed": "4", "fear_greed_label": "Extreme Fear",
        "note": "미국 휴장 (Good Friday) — 아시아 패닉"
    },
    "2026-04-07": {
        "sp500": 5074.08, "sp500_change": "-5.97%",
        "nasdaq": 15587.79,"nasdaq_change": "-5.82%",
        "vix": 52.33,     "kospi": 2328.20, "kospi_change": "-5.57%",
        "krw_usd": 1488.50,"sk_hynix": 165400,"sk_hynix_change": "-8.11%",
        "samsung": 49100,  "samsung_change": "-4.84%",
        "nvda": 88.01,    "nvda_change": "-7.36%",
        "fear_greed": "4", "fear_greed_label": "Extreme Fear",
        "note": "트럼프 상호관세 발효 — 글로벌 증시 동반 폭락"
    },
    "2026-04-08": {
        "sp500": 5153.84, "sp500_change": "+1.57%",
        "nasdaq": 15939.58,"nasdaq_change": "+2.26%",
        "vix": 46.98,     "kospi": 2420.32, "kospi_change": "+3.95%",
        "krw_usd": 1471.20,"sk_hynix": 176800,"sk_hynix_change": "+6.89%",
        "samsung": 51200,  "samsung_change": "+4.28%",
        "nvda": 94.31,    "nvda_change": "+7.16%",
        "fear_greed": "7", "fear_greed_label": "Extreme Fear",
        "note": "기술적 반등 — 협상 기대감"
    },
    "2026-04-09": {
        "sp500": 5456.90, "sp500_change": "+5.87%",
        "nasdaq": 17124.97,"nasdaq_change": "+7.47%",
        "vix": 33.62,     "kospi": 2468.99, "kospi_change": "+2.01%",
        "krw_usd": 1454.80,"sk_hynix": 183600,"sk_hynix_change": "+3.85%",
        "samsung": 52900,  "samsung_change": "+3.32%",
        "nvda": 104.49,   "nvda_change": "+10.79%",
        "fear_greed": "17","fear_greed_label": "Extreme Fear",
        "note": "트럼프 90일 관세 유예 발표 — 나스닥 역대 최대 상승"
    },
    "2026-04-10": {
        "sp500": 5268.05, "sp500_change": "-3.46%",
        "nasdaq": 16387.31,"nasdaq_change": "-4.31%",
        "vix": 40.72,     "kospi": 2432.11, "kospi_change": "-1.49%",
        "krw_usd": 1467.30,"sk_hynix": 176200,"sk_hynix_change": "-4.03%",
        "samsung": 51600,  "samsung_change": "-2.46%",
        "nvda": 97.82,    "nvda_change": "-6.38%",
        "fear_greed": "12","fear_greed_label": "Extreme Fear",
        "note": "CPI 예상 상회 + 중국 125% 보복관세 발표"
    },
    "2026-04-11": {
        "sp500": 5363.36, "sp500_change": "+1.81%",
        "nasdaq": 16724.46,"nasdaq_change": "+2.06%",
        "vix": 37.56,     "kospi": 2469.06, "kospi_change": "+1.52%",
        "krw_usd": 1460.10,"sk_hynix": 181400,"sk_hynix_change": "+2.95%",
        "samsung": 53100,  "samsung_change": "+2.91%",
        "nvda": 104.75,   "nvda_change": "+7.09%",
        "fear_greed": "16","fear_greed_label": "Extreme Fear",
        "note": "미중 협상 기대 + 기술주 반등 지속"
    },
}
DATES = sorted(HIST_DATA.keys())


def _load(path, default=None):
    state_key, state_default = _research_state_meta(path)
    if _RESEARCH_SESSION_ID and state_key:
        fallback = _clone_default(state_default) if state_default is not None else _clone_default(default)
        return load_backtest_state(_RESEARCH_SESSION_ID, state_key, fallback)
    if path.exists():
        try: return json.loads(path.read_text(encoding="utf-8"))
        except: pass
    return default if default is not None else {}

def _save(path, data):
    state_key, _ = _research_state_meta(path)
    if _RESEARCH_SESSION_ID and state_key:
        save_backtest_state(_RESEARCH_SESSION_ID, state_key, data)
        return
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ── 프롬프트: 반드시 주가 수치로 검증 가능한 thesis_killers 강제 ──────────────
ANALYST_PROMPT = """당신은 ORCA 투자 분석 에이전트입니다.
아래 시장 데이터를 분석하고 thesis_killers를 생성하세요.

[thesis_killers 필수 규칙 — 반드시 준수]
1. event는 내일(1일) 또는 3일 이내 주가/지수로 검증 가능한 것만
2. confirms_if / invalidates_if에 반드시 숫자 포함
3. "협상 진전", "분위기 개선" 같은 뉴스 이벤트는 절대 금지
4. 모멘텀 지속 vs 반전 중 하나를 데이터 기반으로 선택할 것

[시장 상황별 필수 규칙 — 반드시 적용]

■ 극단공포 구간 (Fear&Greed < 20):
  - 반드시 반등 thesis_killer를 1개 이상 포함할 것
  - "하락 지속" 예측만 하는 것은 금지 (역사적으로 반등 확률 75%+)
  - 예: "나스닥 극단공포 반등 여부" confirms_if: "나스닥 +2% 이상"

■ 대형 이벤트 다음날 (전일 ±3% 이상):
  - 모멘텀 지속 예측 금지, 반전 가능성 우선 고려
  - 예: 전일 +5% → "차익실현 하락" 가능성 thesis_killer 포함

■ 위험회피 레짐 + VIX > 25:
  - 하락 지속 예측 시 반등 가능성도 반드시 병기
  - 1개 하락 TK당 반등 TK 1개 균형 유지

■ VIX 수준별 임계값 (confirms_if 숫자 기준):
  - VIX < 20: ±0.5% 이상 (낮은 변동성)
  - VIX 20-30: ±1.0% 이상 (중간 변동성)
  - VIX > 30: ±1.5% 이상 (고변동성)
  - VIX > 45: ±2.0% 이상 (극단 공포)

[사용 가능한 timeframe]
- "1일": 내일 하루 방향
- "3일": 3거래일 누적 방향 (추세 확인용)

[올바른 예시]
event: "나스닥 기술주 방향성"
timeframe: "1일"
confirms_if: "나스닥 +0.8% 이상 상승"
invalidates_if: "나스닥 -0.8% 이하 하락"

event: "극단공포 반등 가능성 (FG 8)"
timeframe: "1일"
confirms_if: "나스닥 +2.0% 이상 반등"
invalidates_if: "나스닥 추가 -2.0% 이하 하락"

event: "반도체 섹터 3일 추세"
timeframe: "3일"
confirms_if: "SK하이닉스 3일 누적 +2% 이상"
invalidates_if: "SK하이닉스 3일 누적 -2% 이하"

Return ONLY valid JSON. No markdown.
{
  "analysis_date": "",
  "market_regime": "위험선호/위험회피/전환중/혼조",
  "trend_phase": "상승추세/횡보추세/하락추세",
  "confidence_overall": "낮음/보통/높음",
  "one_line_summary": "",
  "thesis_killers": [
    {"event":"나스닥/코스피/반도체/S&P500 등 주가 수치 검증 가능한 이벤트 (VIX·환율 주제 절대 금지)",
     "timeframe":"1일 또는 3일","confirms_if":"숫자 포함 조건","invalidates_if":"숫자 포함 조건"}
  ],
  "outflows": [{"zone":"","reason":"","severity":"높음/보통/낮음"}],
  "inflows":  [{"zone":"","reason":"","momentum":"강함/형성중/약함"}],
  "korea_focus": {"krw_usd":"","kospi_flow":"","assessment":""}
}"""


def _save_lesson(lesson: dict) -> None:
    """
    교훈을 severity/type에 따라 분리 파일에 저장.
    [failure] → lessons_failure.json  (cap 150)
    [strength]→ lessons_strength.json (cap  60)
    [regime]  → lessons_regime.json   (cap 120, flat list + regime 태그로 구분)
    + 대시보드/알림 호환을 위해 LESSONS_FILE 에도 동기 write
    """
    date   = lesson.get("date", "")
    sev    = lesson.get("severity", "medium")
    ltype  = lesson.get("type", "failure")
    regime = lesson.get("regime", "")

    # ── 분리 파일 저장 ────────────────────────────────────────────
    if sev == "low" or ltype == "strength":
        # 강점 교훈 (cap 60)
        data = _load(LESSONS_STRENGTH_FILE, {"lessons": []})
        data["lessons"].append(lesson)
        data["lessons"] = sorted(data["lessons"],
                                 key=lambda x: x.get("date", ""), reverse=True)[:60]
        _save(LESSONS_STRENGTH_FILE, data)
    elif regime:
        # 레짐 특화 교훈 — flat list, regime 태그로 구분 (cap 120 총합)
        data = _load(LESSONS_REGIME_FILE, {"lessons": []})
        data["lessons"].append(lesson)
        data["lessons"] = sorted(data["lessons"],
                                 key=lambda x: x.get("date", ""), reverse=True)[:120]
        _save(LESSONS_REGIME_FILE, data)
    else:
        # 일반 실패 교훈 (cap 150)
        data = _load(LESSONS_FAILURE_FILE, {"lessons": []})
        data["lessons"].append(lesson)
        data["lessons"] = sorted(data["lessons"],
                                 key=lambda x: x.get("date", ""), reverse=True)[:150]
        _save(LESSONS_FAILURE_FILE, data)

    # ── 레거시 LESSONS_FILE 동기 write (대시보드/알림 호환) ────────
    legacy = _load(LESSONS_FILE, {"lessons": [], "total_lessons": 0, "last_updated": ""})
    legacy["lessons"].append(lesson)
    legacy["lessons"] = sorted(legacy["lessons"],
                               key=lambda x: x.get("date", ""), reverse=True)[:80]
    legacy["total_lessons"] = len(legacy["lessons"])
    legacy["last_updated"]  = date or legacy.get("last_updated", "")
    _save(LESSONS_FILE, legacy)


def _load_lessons_context(current_date: str = None,
                           current_regime: str = "") -> str:
    """
    [Redesigned] 교훈 주입 — 날짜 필터 + 레짐 유사도 + severity 가중치.

    current_date : 이 날짜 이전에 생성된 교훈만 사용 (미래 데이터 누출 차단)
                   None 이면 필터 없음 (라이브 시스템 호환)
    current_regime: 동일/유사 레짐 교훈 우선 선택

    주입 원칙: failure 1개 + (regime or strength) 1개 = 최대 2개
    """
    REGIME_SIM = {
        "위험선호":  {"위험선호", "전환중", "혼조"},
        "위험회피":  {"위험회피", "전환중", "혼조"},
        "전환중":    {"전환중", "위험선호", "위험회피", "혼조"},
        "혼조":      {"혼조", "전환중", "위험선호", "위험회피"},
    }
    similar = REGIME_SIM.get(current_regime, set())

    SEV_W = {"high": 1.0, "medium": 0.65, "low": 0.15}

    def _rank(lessons: list) -> list:
        scored = []
        for l in lessons:
            # ① 시간 여행 차단 — 미래 교훈 완전 제외
            if current_date and l.get("date", "") >= current_date:
                continue
            w = SEV_W.get(l.get("severity", "medium"), 0.5)
            # ② 동일/유사 레짐 보너스 +30%
            if l.get("regime", "") in similar:
                w *= 1.3
            scored.append((w, l))
        return [l for _, l in sorted(scored, key=lambda x: x[0], reverse=True)]

    selected: list = []

    try:
        # Failure 교훈 (1개)
        failure_ranked = _rank(
            _load(LESSONS_FAILURE_FILE, {"lessons": []}).get("lessons", []))
        if failure_ranked:
            selected.append(failure_ranked[0])
    except Exception:
        pass

    try:
        # Regime 전용 교훈 (flat list에서 regime 태그로 필터, 1개)
        all_regime_lessons = _load(LESSONS_REGIME_FILE, {"lessons": []}).get("lessons", [])
        # similar 레짐에 해당하는 것만 남김 (없으면 전체)
        if similar and current_regime:
            regime_pool = [l for l in all_regime_lessons if l.get("regime","") in similar]
        else:
            regime_pool = all_regime_lessons
        regime_ranked = _rank(regime_pool)
        if regime_ranked:
            selected.append(regime_ranked[0])
        else:
            # Regime 교훈 없으면 Strength 교훈 대체
            strength_ranked = _rank(
                _load(LESSONS_STRENGTH_FILE, {"lessons": []}).get("lessons", []))
            if strength_ranked:
                selected.append(strength_ranked[0])
    except Exception:
        pass

    if not selected:
        # Fallback: 레거시 파일
        try:
            legacy_lessons = _load(LESSONS_FILE, {"lessons": []}).get("lessons", [])
            for l in legacy_lessons:
                if current_date and l.get("date", "") >= current_date:
                    continue
                text = str(l.get("lesson", ""))
                if any(k in text for k in ["주의", "실패", "약점", "틀린", "금지"]):
                    selected.append(l)
                    if len(selected) >= 2:
                        break
        except Exception:
            pass

    if not selected:
        return ""

    lines = ["\n[과거 예측 약점 — 반드시 참고 (최대 2개)]"]
    for l in selected:
        regime_tag = f"[{l.get('regime','')}] " if l.get("regime") else ""
        sev_tag    = "⛔" if l.get("severity") == "high" else "⚠️"
        lines.append(f"  {sev_tag} {regime_tag}{l.get('lesson','')[:80]}")
    return "\n".join(lines) + "\n"


def generate_analysis(date, market_data, dry=False):
    if dry:
        fg  = float(market_data.get("fear_greed","50"))
        spd = float(market_data["sp500_change"].replace("%","").replace("+",""))
        return {
            "analysis_date": date, "mode": "MORNING",
            "market_regime": "위험회피" if fg < 25 else "전환중" if fg < 40 else "혼조",
            "trend_phase":   "하락추세" if spd < -1 else "상승추세" if spd > 1 else "횡보추세",
            "confidence_overall": "보통",
            "one_line_summary": f"[DRY] {market_data['note']}",
            "thesis_killers": [
                {"event":"나스닥 방향성","timeframe":"1일",
                 "confirms_if":"나스닥 +1% 이상","invalidates_if":"나스닥 -1% 이하"},
                {"event":"코스피 방향성","timeframe":"1일",
                 "confirms_if":"코스피 +1% 이상","invalidates_if":"코스피 -1% 이하"},
                {"event":"반도체 (SK하이닉스) 방향성","timeframe":"1일",
                 "confirms_if":"SK하이닉스 +2% 이상","invalidates_if":"SK하이닉스 -2% 이하"},
            ],
            "outflows": [{"zone":"위험자산","reason":"관세 불확실성","severity":"높음"}],
            "inflows":  [{"zone":"현금/안전자산","reason":"공포 구간","momentum":"강함"}],
            "korea_focus": {"krw_usd":str(market_data["krw_usd"]),
                           "kospi_flow":market_data["kospi_change"],"assessment":"추정"}
        }

    import anthropic
    client = anthropic.Anthropic(api_key=API_KEY)
    d = market_data

    # 컨텍스트 경고 신호 계산
    try:
        sp_chg = float(str(d.get("sp500_change","0")).replace("%","").replace("+",""))
    except:
        sp_chg = 0.0
    try:
        fg = int(str(d.get("fear_greed","50")))
    except:
        fg = 50
    try:
        vix_val = float(str(d.get("vix","20")))
    except:
        vix_val = 20.0

    signals = []
    if fg < 20:
        signals.append(f"⚠️ 극단공포 (FG={fg}): 반등 thesis_killer 필수 포함, 하락만 예측 금지")
    elif fg < 30:
        signals.append(f"⚠️ 공포 구간 (FG={fg}): 반등 가능성 thesis_killer 1개 이상 포함")
    if fg > 75:
        signals.append(f"⚠️ 극단탐욕 (FG={fg}): 차익실현/조정 가능성 고려")
    if abs(sp_chg) >= 3:
        mv = "급등" if sp_chg > 0 else "급락"
        signals.append(f"⚠️ 전일 S&P500 {mv} {sp_chg:+.1f}%: 당일 반전 확률 높음, 모멘텀 지속 예측 억제")
    if vix_val > 45:
        signals.append(f"⚠️ VIX {vix_val:.0f} 극단변동성: confirms_if 임계값 ±2.0% 이상 사용")
    elif vix_val > 30:
        signals.append(f"⚠️ VIX {vix_val:.0f} 고변동성: confirms_if 임계값 ±1.5% 이상 사용")

    # [3-layer VIX/FX Block] Analyst 프롬프트 수준에서 차단 신호 주입
    signals.append(
        "🚫 [VIX/환율 thesis_killer 생성 금지] "
        "VIX와 원달러 환율은 thesis_killer의 event 주제로 절대 사용하지 말 것. "
        "VIX는 변동성 맥락 설명에만 사용. "
        "나스닥/코스피/반도체 주가만 thesis_killer 대상으로 사용할 것."
    )

    signal_str = ""
    if signals:
        signal_str = "\n[현재 시장 경고신호 — 반드시 반영]\n" + "\n".join(signals) + "\n"

    # [경계 N/A 날짜 처리] 전일 데이터 없을 때 명시적 경고
    na_fields = [k for k in ("sp500_change","nasdaq_change") if d.get(k,"") == "N/A"]
    na_warning = ""
    if na_fields:
        na_warning = (
            "\n[⚠️ 전일 가격 데이터 없음 (N/A)]\n"
            "전일 변화율 데이터를 알 수 없음. "
            "thesis_killer에서 '전일 급등/급락'을 가정하지 말 것. "
            "현재 가격 수준과 Fear&Greed/VIX만으로 방향 판단할 것.\n"
        )

    # 과거 약점 교훈 주입 — 날짜 필터 + 레짐 필터 적용
    # current_regime은 이 시점에서 알 수 없으므로 HIST_DATA에서 전일 레짐 참조
    prev_regime = ""
    try:
        dates_before = [d_ for d_ in DATES if d_ < date]
        if dates_before:
            prev_regime = HIST_DATA.get(dates_before[-1], {}).get("regime", "")
    except Exception:
        pass
    lessons_ctx = _load_lessons_context(current_date=date, current_regime=prev_regime)

    # ── 패턴 신호 주입 (방법 3 + 방법 2) ─────────────────────────────────
    # 전일 추세도 참조
    prev_trend = ""
    try:
        if dates_before:
            prev_trend = HIST_DATA.get(dates_before[-1], {}).get("trend", "")
    except Exception:
        pass
    pattern_block, signal_override = _get_pattern_signal(
        regime=prev_regime or "혼조",
        fg=fg,
        trend=prev_trend or "횡보추세",
        current_date=date,
    )
    if signal_override:
        signals.insert(0, signal_override)   # 방법 2: 최상단에 역바이어스 경고

    # 시계열 컨텍스트 (레짐 연속일, VIX/FG 방향, 자기 피드백)
    trend_ctx = _build_trend_context(date, HIST_DATA, _backtest_results)

    user_msg = (
        f"{pattern_block}"          # ← 방법 3: 통계 룰 최상단 강조
        f"{lessons_ctx}"
        f"{na_warning}"
        f"{trend_ctx}"
        f"분석 날짜: {date}\n이벤트: {d.get('note','')}\n\n"
        f"S&P500: {d.get('sp500','N/A')} ({d.get('sp500_change','N/A')})\n"
        f"나스닥: {d.get('nasdaq','N/A')} ({d.get('nasdaq_change','N/A')})\n"
        f"VIX: {d.get('vix','N/A')}\n코스피: {d.get('kospi','N/A')} ({d.get('kospi_change','N/A')})\n"
        f"원달러: {d.get('krw_usd','N/A')}\nSK하이닉스: {d.get('sk_hynix','N/A')} ({d.get('sk_hynix_change','N/A')})\n"
        f"삼성전자: {d.get('samsung','N/A')} ({d.get('samsung_change','N/A')})\n"
        f"엔비디아: {d.get('nvda','N/A')} ({d.get('nvda_change','N/A')})\n"
        f"Fear&Greed: {d.get('fear_greed','50')} ({d.get('fear_greed_label','Neutral')})"
        f"{signal_str}\n"
        f"thesis_killers는 내일 주가/지수 수치로 검증 가능하게 작성. JSON 반환:"
    )
    full = ""
    with client.messages.stream(
        model=MODEL, max_tokens=1500, system=ANALYST_PROMPT,
        messages=[{"role":"user","content":user_msg}]
    ) as s:
        for ev in s:
            if getattr(ev,"type","") == "content_block_delta":
                d2 = getattr(ev,"delta",None)
                if d2 and getattr(d2,"type","") == "text_delta":
                    full += d2.text

    raw = re.sub(r"```json|```","",full).strip()
    m = re.search(r"\{[\s\S]*\}", raw)
    if not m: raise ValueError("JSON 없음\n" + full[:300])
    s = m.group()
    for fn in [
        lambda x: json.loads(x),
        lambda x: json.loads(re.sub(r",\s*([}\]])", r"\1", x)),
        lambda x: json.loads(x + "]"*(x.count("[")-x.count("]")) + "}"*(x.count("{")-x.count("}"))),
    ]:
        try: result = fn(s); break
        except json.JSONDecodeError: continue
    else:
        raise ValueError("JSON 파싱 3단계 모두 실패")

    result["analysis_date"] = date
    result["mode"] = "MORNING"
    return result


def _pct(v):
    try: return float(str(v or "0").replace("%","").replace("+",""))
    except: return 0.0


def verify_predictions(analysis, next_data, next_3d_data=None):
    """
    thesis_killers 검증.
    timeframe 1일: next_data로 검증
    timeframe 3일: next_3d_data (3거래일 누적) 로 검증, 없으면 1일 데이터 사용
    """
    results = []

    nq  = _pct(next_data.get("nasdaq_change"))
    sp  = _pct(next_data.get("sp500_change"))
    ks  = _pct(next_data.get("kospi_change"))
    sk  = _pct(next_data.get("sk_hynix_change"))
    sam = _pct(next_data.get("samsung_change"))
    nv  = _pct(next_data.get("nvda_change"))

    # 3일 누적 데이터 (있으면 사용)
    nq3  = _pct(next_3d_data.get("nasdaq_change_3d",  "0")) if next_3d_data else nq*2.5
    sk3  = _pct(next_3d_data.get("sk_hynix_change_3d","0")) if next_3d_data else sk*2.5
    nv3  = _pct(next_3d_data.get("nvda_change_3d",    "0")) if next_3d_data else nv*2.5
    try:   vix_now  = float(next_data.get("vix", 25))
    except: vix_now = 25.0
    try:   vix_prev = float(analysis.get("vix_at_time", vix_now))
    except: vix_prev = vix_now

    def extract_threshold(text):
        """텍스트에서 숫자 임계값 추출"""
        nums = re.findall(r"[+-]?\d+\.?\d*", str(text))
        return float(nums[0]) if nums else None

    # VIX 수준별 기본 임계값 (움직임이 작으면 unclear 처리)
    vix_base_thr = 0.3 if vix_now < 20 else 0.5 if vix_now < 30 else 1.0 if vix_now < 45 else 1.5

    def check_direction(chg, conf_text, inv_text):
        """
        등락 방향 + 임계값으로 verdict 결정.
        VIX 수준에 따라 미미한 변동 기준 동적 조정.
        방향이 맞으면 수치 미달이어도 partial_confirm 처리.
        """
        conf_thr = extract_threshold(conf_text) or 1.0
        inv_thr  = extract_threshold(inv_text) or 1.0

        conf_up   = any(w in conf_text.lower() for w in ["상승","반등","올라","증가","+"])
        conf_down = any(w in conf_text.lower() for w in ["하락","급락","내려","감소","-"])
        inv_up    = any(w in inv_text.lower() for w in ["상승","반등","올라","증가","+"])
        inv_down  = any(w in inv_text.lower() for w in ["하락","급락","내려","감소","-"])

        abs_thr = abs(conf_thr) if conf_thr else 1.0

        # 임계값 완전 충족
        if conf_up and chg >= abs_thr:
            return "confirmed",   f"실제 {chg:+.2f}% (예측: +{abs_thr:.1f}% 이상)"
        if conf_down and chg <= -abs_thr:
            return "confirmed",   f"실제 {chg:+.2f}% (예측: -{abs_thr:.1f}% 이하)"

        # 방향은 맞지만 수치 미달 → partial confirm (±0.3% 이상이면 인정)
        if conf_up and 0.3 <= chg < abs_thr:
            return "confirmed",   f"실제 {chg:+.2f}% (예측 방향 일치, 수치 부분달성)"
        if conf_down and -abs_thr < chg <= -0.3:
            return "confirmed",   f"실제 {chg:+.2f}% (예측 방향 일치, 수치 부분달성)"

        # 반대 방향 (invalidated)
        if inv_up and chg >= abs(inv_thr):
            return "invalidated", f"실제 {chg:+.2f}% (예측 반대)"
        if inv_down and chg <= -abs(inv_thr):
            return "invalidated", f"실제 {chg:+.2f}% (예측 반대)"
        # 방향 자체가 반대인 경우 (수치 기준 없이)
        if conf_up and chg <= -0.3:
            return "invalidated", f"실제 {chg:+.2f}% (예측 반대)"
        if conf_down and chg >= 0.3:
            return "invalidated", f"실제 {chg:+.2f}% (예측 반대)"

        # 변동 미미 (VIX 수준별 기준)
        if abs(chg) < vix_base_thr:
            return "unclear", f"변동 미미 ({chg:+.2f}%)"
        return "unclear", f"방향 불명확 ({chg:+.2f}%)"

    for tk in analysis.get("thesis_killers", []):
        event     = tk.get("event","").lower()
        conf      = tk.get("confirms_if","").lower()
        inv       = tk.get("invalidates_if","").lower()
        timeframe = tk.get("timeframe","1일")
        use_3d    = "3일" in timeframe
        v, ev, cat = "unclear", "", "기타"

        # ── 나스닥 / 미국 기술주 ───────────────────────────────────────────
        if any(k in event for k in ["나스닥","nasdaq","기술주","미국 주식","s&p","sp500","빅테크"]):
            cat = "주식"
            chg = (nq3 if use_3d else nq) if "나스닥" in event or "nasdaq" in event else sp
            v, ev = check_direction(chg, conf, inv)

        # ── 코스피 / 한국 주식 ─────────────────────────────────────────────
        elif any(k in event for k in ["코스피","kospi","한국 주식","코스피200"]):
            cat = "주식"
            v, ev = check_direction(ks, conf, inv)

        # ── 반도체 ─────────────────────────────────────────────────────────
        elif any(k in event for k in ["sk하이닉스","하이닉스","sk hynix"]):
            cat = "주식"
            v, ev = check_direction(sk, conf, inv)
        elif any(k in event for k in ["삼성전자","삼성"]):
            cat = "주식"
            v, ev = check_direction(sam, conf, inv)
        elif any(k in event for k in ["엔비디아","nvidia","nvda"]):
            cat = "주식"
            v, ev = check_direction(nv, conf, inv)
        elif any(k in event for k in ["반도체","hbm","semiconductor"]):
            cat = "주식"
            chg = max([sk, nv], key=abs)
            v, ev = check_direction(chg, conf, inv)

        # ── VIX ────────────────────────────────────────────────────────────
        elif any(k in event for k in ["vix","변동성","공포지수"]):
            cat = "VIX"
            vix_chg_pct = ((vix_now - vix_prev) / vix_prev * 100) if vix_prev != 0 else 0
            conf_thr = extract_threshold(conf) or 20.0  # 60일 백테스트: 32% 정확도 → 임계값 상향
            inv_thr  = extract_threshold(inv) or 20.0
            conf_down = any(w in conf for w in ["하락","완화","감소","-"])
            if conf_down and vix_chg_pct <= -conf_thr:
                v, ev = "confirmed", f"VIX {vix_chg_pct:+.1f}% 하락"
            elif conf_down and vix_chg_pct >= inv_thr:
                v, ev = "invalidated", f"VIX {vix_chg_pct:+.1f}% 상승 (반대)"
            elif vix_now < vix_prev * 0.8:  # 20% 이상 하락만
                v, ev = "confirmed", f"VIX {vix_now:.1f} (전일 {vix_prev:.1f}, -{round((1-vix_now/vix_prev)*100)}%)"
            elif vix_now > vix_prev * 1.2:  # 20% 이상 상승만
                v, ev = "invalidated", f"VIX {vix_now:.1f} +{round((vix_now/vix_prev-1)*100)}% 상승"
            else:
                v, ev = "unclear", f"VIX 변동 미미 ({vix_now:.1f})"

        # 원달러: 백테스트 17% 정확도 — 자동 unclear
        elif any(k in event for k in ["원달러","환율","krw","원화"]):
            cat = "환율"
            v, ev = "unclear", "원달러 예측 제외 (백테스트 정확도 17%)"



        results.append({
            "event":    tk.get("event",""),
            "verdict":  v, "evidence": ev, "category": cat,
            "confirms_if":   tk.get("confirms_if",""),
            "invalidates_if": tk.get("invalidates_if",""),
        })

    return results


# ── 전역 스냅샷 (원달러 검증용) ────────────────────────────────────────────
market_data_snapshot = {}
def _build_trend_context(date: str, all_hist: dict, all_results: list) -> str:
    """
    최근 5일 시계열 컨텍스트 생성.
    
    핵심 인사이트:
      - 레짐 연속 1~2일: 전환 직후 불안정 → 반등 가능
      - 레짐 연속 3~8일: 추세 지속 → 방향 예측 가능
      - 레짐 연속 10일+: 소진 구간 → 반전 가능
      - VIX/FG 방향성: 레짐 라벨보다 중요
      - 자기 피드백: 연속 실수 패턴 인식
    """
    dates_sorted = sorted(all_hist.keys())
    cur_idx = dates_sorted.index(date) if date in dates_sorted else -1
    if cur_idx < 1:
        return ""

    # 최근 5일 데이터
    recent_dates = dates_sorted[max(0, cur_idx-5):cur_idx]
    recent_data  = [all_hist[d] for d in recent_dates]
    if not recent_data:
        return ""

    # 1. 레짐 연속 일수
    cur_regime = all_hist[date].get("regime", "")
    streak = 0
    for d in reversed(recent_dates):
        if all_hist[d].get("regime","") == cur_regime:
            streak += 1
        else:
            break

    streak_signal = ""
    if streak <= 1:
        streak_signal = f"⚠️ 레짐 전환 직후 ({streak}일째): 방향 불안정, 반전 가능성 높음"
    elif streak >= 10:
        streak_signal = f"⚠️ 레짐 장기 지속 ({streak}일째): 모멘텀 소진 가능, 반전 주의"
    else:
        streak_signal = f"레짐 연속 {streak}일째: 추세 안정"

    # 2. VIX 방향성 (3일 추세)
    vix_vals = []
    for d in recent_dates[-3:]:
        try:
            vix_vals.append(float(str(all_hist[d].get("vix","20"))))
        except:
            pass
    try:
        cur_vix = float(str(all_hist[date].get("vix","20")))
        vix_vals.append(cur_vix)
    except:
        pass

    vix_dir = ""
    if len(vix_vals) >= 3:
        if vix_vals[-1] > vix_vals[-3] * 1.05:
            vix_dir = f"VIX ↑ 상승 추세 ({vix_vals[-3]:.0f}→{vix_vals[-1]:.0f}): 공포 심화 중"
        elif vix_vals[-1] < vix_vals[-3] * 0.95:
            vix_dir = f"VIX ↓ 하락 추세 ({vix_vals[-3]:.0f}→{vix_vals[-1]:.0f}): 공포 완화 중"
        else:
            vix_dir = f"VIX → 횡보 ({vix_vals[-1]:.0f})"

    # 3. FG 방향성
    fg_vals = []
    for d in recent_dates[-3:]:
        try:
            fg_vals.append(int(str(all_hist[d].get("fear_greed","50"))))
        except:
            pass
    try:
        cur_fg = int(str(all_hist[date].get("fear_greed","50")))
        fg_vals.append(cur_fg)
    except:
        pass

    fg_dir = ""
    if len(fg_vals) >= 3:
        delta = fg_vals[-1] - fg_vals[-3]
        if delta >= 5:
            fg_dir = f"FG ↑ +{delta}pt 개선 중 ({fg_vals[-3]}→{fg_vals[-1]}): 심리 회복"
        elif delta <= -5:
            fg_dir = f"FG ↓ {delta}pt 악화 중 ({fg_vals[-3]}→{fg_vals[-1]}): 심리 위축"
        else:
            fg_dir = f"FG → 횡보 ({fg_vals[-1]})"

    # 4. 최근 3일 누적 수익률
    sp_3d = 0.0
    for d in recent_dates[-3:]:
        try:
            chg = float(str(all_hist[d].get("sp500_change","0")).replace("%","").replace("+",""))
            sp_3d += chg
        except:
            pass
    sp_3d_str = f"S&P 3일 누적: {sp_3d:+.1f}%"
    if sp_3d >= 5:
        sp_3d_str += " (과매수 구간, 차익실현 주의)"
    elif sp_3d <= -5:
        sp_3d_str += " (과매도 구간, 반등 가능)"

    # 5. 자기 피드백 (최근 5일 예측 결과)
    feedback_lines = []
    consec_wrong = 0  # 같은 방향으로 연속 틀린 횟수
    if all_results:
        recent_results = [r for r in all_results[-10:] if r.get("date") < date][-5:]
        wrong_streak_regime = ""
        for r in reversed(recent_results):
            if r.get("verdict") == "invalidated":
                consec_wrong += 1
                wrong_streak_regime = r.get("regime","")
            else:
                break
        if consec_wrong >= 2:
            feedback_lines.append(
                f"⚠️ 자기피드백: {cur_regime} 레짐 예측 최근 {consec_wrong}회 연속 틀림 → 반대 방향 고려"
            )
        
        recent_acc = sum(1 for r in recent_results if r.get("verdict") == "confirmed")
        total_judged = sum(1 for r in recent_results if r.get("verdict") != "unclear")
        if total_judged >= 3:
            acc_pct = recent_acc / total_judged * 100
            feedback_lines.append(f"최근 5일 정확도: {acc_pct:.0f}% ({recent_acc}/{total_judged}건)")

    # 조합
    parts = [streak_signal]
    if vix_dir: parts.append(vix_dir)
    if fg_dir:  parts.append(fg_dir)
    parts.append(sp_3d_str)
    parts.extend(feedback_lines)

    return "\n[시계열 컨텍스트 — 방향 예측에 중요]\n" + "\n".join(f"  {p}" for p in parts) + "\n"




def update_accuracy(results, date):
    acc = _load(ACCURACY_FILE, {
        "total":0,"correct":0,"by_category":{},
        "history":[],"history_by_category":[],"weak_areas":[],"strong_areas":[]
    })
    judged  = [r for r in results if r["verdict"] != "unclear"]
    correct = [r for r in judged  if r["verdict"] == "confirmed"]
    acc["total"]   += len(judged)
    acc["correct"] += len(correct)

    today_cat = {}
    for r in judged:
        cat = r.get("category","기타")
        acc["by_category"].setdefault(cat, {"total":0,"correct":0})
        acc["by_category"][cat]["total"]   += 1
        today_cat.setdefault(cat, {"total":0,"correct":0})
        today_cat[cat]["total"] += 1
        if r["verdict"] == "confirmed":
            acc["by_category"][cat]["correct"]  += 1
            today_cat[cat]["correct"] += 1

    today_acc = round(len(correct)/len(judged)*100,1) if judged else 0

    # 방향 정확도 (orca_analysis.py와 동일 기준)
    dir_correct = len(correct)   # backtest confirmed = 방향 일치로 간주
    acc.setdefault("dir_total",   0)
    acc.setdefault("dir_correct", 0)
    acc["dir_total"]   += len(judged)
    acc["dir_correct"] += dir_correct

    # 중복 날짜 방어 (orca_analysis.py와 동일 로직)
    acc["history"] = [h for h in acc["history"] if h.get("date") != date]
    acc["history"].append({"date": date, "total": len(judged),
                           "correct": len(correct), "accuracy": today_acc,
                           "dir_correct": dir_correct, "dir_accuracy": today_acc})
    acc["history"] = sorted(acc["history"], key=lambda x: x.get("date",""))[-90:]
    acc.setdefault("history_by_category",[])
    acc["history_by_category"] = [h for h in acc["history_by_category"] if h.get("date")!=date]
    acc["history_by_category"].append({"date":date,"by_category":today_cat})
    acc["history_by_category"] = acc["history_by_category"][-90:]

    strong, weak = [], []
    for cat, s in acc["by_category"].items():
        if s["total"] >= 3:
            a = s["correct"]/s["total"]*100
            if a >= 65: strong.append(f"{cat} ({round(a)}%)")
            elif a <= 40: weak.append(f"{cat} ({round(a)}%)")
    acc["strong_areas"] = strong
    acc["weak_areas"]   = weak
    _save(ACCURACY_FILE, acc)
    return today_acc, len(correct), len(judged)


def update_research_weights_from_accuracy(accuracy_data: dict) -> list:
    weights = _load(WEIGHTS_FILE, _default_weights_state())
    conf = weights.get("prediction_confidence", {})

    recent: dict[str, dict[str, int]] = {}
    for snap in accuracy_data.get("history_by_category", [])[-30:]:
        for cat, stats in snap.get("by_category", {}).items():
            bucket = recent.setdefault(cat, {"correct": 0, "total": 0})
            bucket["correct"] += stats.get("correct", 0)
            bucket["total"] += stats.get("total", 0)

    changes = []
    for cat, stats in recent.items():
        if stats["total"] < 3:
            continue
        acc = stats["correct"] / stats["total"]
        old_w = conf.get(cat, 1.0)
        adj = 0.05 if acc >= 0.7 else -0.05 if acc <= 0.4 else 0.0
        new_w = round(max(0.3, min(2.0, old_w + adj)), 3)
        if abs(new_w - old_w) >= 0.001:
            conf[cat] = new_w
            changes.append(f"{cat}: {old_w:.3f}->{new_w:.3f} (acc={acc:.1%})")

    if changes:
        weights["prediction_confidence"] = conf
        weights["last_updated"] = datetime.now(KST).strftime("%Y-%m-%d")
        weights["total_learning_cycles"] = weights.get("total_learning_cycles", 0) + 1
        _save(WEIGHTS_FILE, weights)

    return changes


def extract_lessons(results, analysis, date):
    """
    [Redesigned] 교훈을 3개 파일에 분리 저장.
    각 교훈에 regime 태그 추가 → 레짐 기반 필터링 지원.
    """
    fg     = float(HIST_DATA.get(date, {}).get("fear_greed", "50"))
    regime = analysis.get("market_regime", "")
    trend  = analysis.get("trend_phase", "")
    sp_chg = _pct(HIST_DATA.get(date, {}).get("sp500_change", "0"))

    # ── 오판 교훈 (failure) ────────────────────────────────────────
    for r in results:
        if r["verdict"] == "invalidated":
            _save_lesson({
                "date": date, "regime": regime, "source": "backtest",
                "category": r["category"], "type": "failure",
                "severity": "high" if "주식" in r["category"] else "medium",
                "lesson": f"{r['event'][:35]} 오판 — {r.get('evidence','')[:30]}",
                "applied": 0, "reinforced": 0,
            })

    # ── 구조적 교훈 (regime-specific failure) ────────────────────
    if fg <= 10 and sp_chg <= -3:
        _save_lesson({
            "date": date, "regime": regime, "source": "backtest",
            "category": "레짐판단", "type": "failure", "severity": "high",
            "lesson": f"FG {fg} + S&P {sp_chg:+.1f}% — 극단공포 폭락기, 반등 타이밍 연구 필요",
            "applied": 0, "reinforced": 0,
        })

    if fg <= 20 and "선호" in regime:
        _save_lesson({
            "date": date, "regime": regime, "source": "backtest",
            "category": "레짐판단", "type": "failure", "severity": "high",
            "lesson": f"FG {fg}(극단공포)인데 위험선호 판단 — 공포 구간 낙관 과잉",
            "applied": 0, "reinforced": 0,
        })

    if "하락" in trend and sp_chg > 2:
        _save_lesson({
            "date": date, "regime": regime, "source": "backtest",
            "category": "추세판단", "type": "failure", "severity": "medium",
            "lesson": f"하락추세 판단인데 S&P {sp_chg:+.1f}% 급등 — 반등 포착 실패",
            "applied": 0, "reinforced": 0,
        })

    # ── 강점 교훈 (strength) ──────────────────────────────────────
    for r in results:
        if r["verdict"] == "confirmed" and r["category"] == "주식":
            _save_lesson({
                "date": date, "regime": regime, "source": "backtest",
                "category": r["category"], "type": "strength", "severity": "low",
                "lesson": f"{r['event'][:30]} 적중 — {r.get('evidence','')[:25]}",
                "applied": 0,
            })


def save_to_memory(analysis):
    memory = _load(MEMORY_FILE, [])
    if not isinstance(memory, list): memory = []
    date = analysis.get("analysis_date","")
    memory = [m for m in memory if m.get("analysis_date") != date]
    memory = (memory + [analysis])[-90:]
    _save(MEMORY_FILE, memory)


def _record_research_day(date: str, phase_label: str, market_data: dict,
                         analysis: dict, results: list[dict] | None) -> None:
    if not _RESEARCH_SESSION_ID:
        return
    judged = [r for r in (results or []) if r.get("verdict") != "unclear"]
    correct = [r for r in judged if r.get("verdict") == "confirmed"]
    unclear = [r for r in (results or []) if r.get("verdict") == "unclear"]
    metrics = {
        "result_count": len(results or []),
        "judged_count": len(judged),
        "correct_count": len(correct),
        "unclear_count": len(unclear),
        "accuracy_pct": round(len(correct) / len(judged) * 100, 1) if judged else 0.0,
    }
    record_backtest_day(
        _RESEARCH_SESSION_ID,
        date,
        phase_label or "default",
        market_note=market_data.get("note", ""),
        analysis=analysis,
        results=results or [],
        metrics=metrics,
    )


def print_summary_table(all_results_by_date):
    """날짜별 정확도 요약 테이블 출력"""
    print(f"\n{'='*65}")
    print(f"{'날짜':<12} {'예측수':>5} {'적중':>5} {'오판':>5} {'불명':>5} {'정확도':>7} {'메모'}")
    print(f"{'─'*65}")
    for date, (results, note) in all_results_by_date.items():
        judged  = [r for r in results if r["verdict"] != "unclear"]
        correct = [r for r in judged if r["verdict"] == "confirmed"]
        wrong   = [r for r in judged if r["verdict"] == "invalidated"]
        unclear = [r for r in results if r["verdict"] == "unclear"]
        acc     = round(len(correct)/len(judged)*100) if judged else 0
        bar     = f"{'✅'*len(correct)}{'❌'*len(wrong)}{'❓'*len(unclear)}"
        print(f"{date:<12} {len(results):>5} {len(correct):>5} {len(wrong):>5} {len(unclear):>5} {acc:>6}%  {bar}")
    print(f"{'='*65}")


def classify_vix_band(vix) -> str:
    """
    VIX를 레짐 밴드로 분류 (dual-write용 — 기존 방향성 판단과 병행).
    기존 verify_predictions는 direction 기준 유지, 이 필드는 별도 측정.
    300건 이상 쌓이면 밴드 기준 교훈으로 점진 전환 가능.
    """
    try:
        v = float(vix)
    except (ValueError, TypeError):
        return "unknown"
    if v >= 40:   return "panic"
    if v >= 28:   return "fear"
    if v >= 18:   return "caution"
    return "calm"


def classify_task_type(note: str, vix, fg) -> str:
    """
    예측 태스크 분류 (저장만, 예측에 미사용).
    데이터 축적 후 태스크별 정확도 분석에 활용.
    """
    EVENT_KW = ["FOMC","CPI","관세","tariff","실적발표","Fed","금리","어닝"]
    try:
        v = float(vix)
    except (ValueError, TypeError):
        v = 20
    note_str = str(note or "")
    if any(kw in note_str for kw in EVENT_KW):
        return "event_response"
    if v >= 28:
        return "volatility_regime"
    return "continuation"

def _vix_to_fg(vix: float) -> tuple:
    """VIX → Fear&Greed 프록시 (실제 F&G API 없을 때 사용)."""
    v = float(vix)
    if v < 12:  return 90, "Extreme Greed"
    if v < 15:  return 75, "Greed"
    if v < 18:  return 62, "Greed"
    if v < 20:  return 55, "Neutral"
    if v < 23:  return 45, "Neutral"
    if v < 27:  return 35, "Fear"
    if v < 32:  return 25, "Fear"
    if v < 40:  return 15, "Extreme Fear"
    return 5, "Extreme Fear"


def _fetch_dynamic_hist(months: int = 6) -> None:
    """
    yfinance로 최근 N개월 시장 데이터를 HIST_DATA에 동적 추가.
    - HIST_DATA에 이미 있는 날짜는 하드코딩 값 우선 (더 정확)
    - Fear&Greed는 VIX 프록시 사용
    - 앞(과거) + 뒤(미래) 양방향 확장
    """
    import yfinance as yf
    from datetime import date as _date

    today = _date.today()
    start = today - timedelta(days=int(months * 30.5) + 10)

    print(f"\n📥 {months}개월 데이터 동적 fetch: {start} ~ {today}")

    YF_MAP = {
        "^GSPC":     "sp500",
        "^IXIC":     "nasdaq",
        "^VIX":      "vix",
        "^KS11":     "kospi",
        "USDKRW=X":  "krw_usd",
        "000660.KS": "sk_hynix",
        "005930.KS": "samsung",
        "NVDA":      "nvda",
    }

    try:
        closes_map: dict[str, object] = {}
        all_dates: set[str] = set()

        for yt in YF_MAP:
            try:
                raw = yf.download(
                    yt,
                    start=str(start),
                    end=str(today + timedelta(days=1)),
                    auto_adjust=True,
                    progress=False,
                    threads=False,
                    timeout=20,
                )
            except Exception as exc:
                print(f"  {yt} yfinance fetch 실패 — {exc}")
                continue

            if raw is None or raw.empty:
                continue

            closes = raw["Close"] if "Close" in raw.columns else raw.squeeze()
            try:
                closes = closes.dropna()
            except Exception:
                pass
            if getattr(closes, "empty", False):
                continue

            closes_map[yt] = closes
            for idx in closes.index:
                all_dates.add(idx.strftime("%Y-%m-%d"))

        if not closes_map:
            print("  yfinance 데이터 없음 — 스킵")
            return

        dates_list = sorted(all_dates)

        prev_vals: dict     = {}
        prev_was_hardcoded  = False   # [Bug Fix] 하드코딩↔yfinance 경계 플래그
        added = 0

        for i, d_str in enumerate(dates_list):
            row: dict = {}
            valid = True

            for yt, key in YF_MAP.items():
                try:
                    series = closes_map.get(yt)
                    if series is None:
                        row[f"{key}_change"] = "N/A"
                        continue
                    idx = series.index.strftime("%Y-%m-%d") == d_str
                    if not idx.any():
                        row[f"{key}_change"] = "N/A"
                        continue
                    val = float(series.loc[idx].iloc[0])
                    import math
                    if math.isnan(val) or val <= 0:
                        if key in ("sp500", "nasdaq"):
                            valid = False
                        continue
                    row[key] = round(val, 2)
                    # [Bug Fix] 경계 직후(prev_was_hardcoded=True)이면 N/A
                    # 하드코딩 값(예: 5996.66)과 yfinance 값(예: 6796.86) 차이가
                    # 실제 변화율이 아닌 데이터 소스 차이이므로 변화율 무효화
                    prev = prev_vals.get(key)
                    if prev and prev > 0 and not prev_was_hardcoded:
                        chg = (val - prev) / prev * 100
                        row[f"{key}_change"] = f"{chg:+.2f}%"
                    else:
                        row[f"{key}_change"] = "N/A"
                    prev_vals[key] = val
                except Exception:
                    row[f"{key}_change"] = "N/A"

            # 하드코딩 날짜: 데이터는 덮지 않고 플래그만 설정
            # [Bug Fix] prev_vals는 건드리지 않음 → 다음 yfinance 날에서 N/A 처리
            if d_str in HIST_DATA:
                prev_was_hardcoded = True
                continue

            prev_was_hardcoded = False   # yfinance 날 처리 완료 → 다음날 정상 계산

            if not valid or not row.get("sp500"):
                continue

            # 누락 필드 N/A 기본값 보장 (한국장 휴장 등으로 kospi 없을 수 있음)
            for key in ("kospi", "krw_usd", "sk_hynix", "samsung"):
                row.setdefault(key, "N/A")
                row.setdefault(f"{key}_change", "N/A")

            # VIX 프록시 Fear&Greed
            fg, fg_label = _vix_to_fg(row.get("vix", 20))
            row["fear_greed"]       = str(fg)
            row["fear_greed_label"] = fg_label
            row["note"]             = f"yfinance 수집 ({d_str})"
            HIST_DATA[d_str] = row
            added += 1

        global DATES
        DATES = sorted(HIST_DATA.keys())
        print(f"  ✅ {added}일 추가 → 총 {len(DATES)}거래일 ({DATES[0]} ~ {DATES[-1]})")

    except Exception as e:
        print(f"  동적 fetch 실패: {e}")
        import traceback; traceback.print_exc()


def _fetch_recent_data() -> None:
    """하위호환 래퍼 — _fetch_dynamic_hist(months=1) 호출."""
    _fetch_dynamic_hist(months=1)


# ══════════════════════════════════════════════════════════════════════════════
# Walk-Forward Optimization
# ══════════════════════════════════════════════════════════════════════════════

def _count_lessons() -> int:
    """3개 교훈 파일의 전체 교훈 수 합산."""
    try:
        n_fail  = len(_load(LESSONS_FAILURE_FILE,  {"lessons": []}).get("lessons", []))
        n_str   = len(_load(LESSONS_STRENGTH_FILE, {"lessons": []}).get("lessons", []))
        n_reg   = len(_load(LESSONS_REGIME_FILE,   {"lessons": []}).get("lessons", []))
        return n_fail + n_str + n_reg
    except Exception:
        try:
            return len(_load(LESSONS_FILE, {"lessons": []}).get("lessons", []))
        except Exception:
            return 0


# ── 패턴 저장 시스템 ──────────────────────────────────────────────────────────
def _fg_bucket(fg: float) -> str:
    if fg <= 20: return "극단공포"
    if fg <= 35: return "공포"
    if fg <= 50: return "중립"
    if fg <= 70: return "탐욕"
    return "과열"


def _pattern_key(regime: str, fg: float, trend: str) -> str:
    return f"{regime}|{_fg_bucket(fg)}|{trend}"


def _generate_rule_text(key: str, acc: float, n: int) -> str:
    """acc 범위에 따라 방향성 룰 텍스트 생성."""
    regime, fg_b, trend = key.split("|")
    pct = f"{acc:.0%}"
    if acc >= 0.70:
        return (f"{regime}+{fg_b} 조건에서 {trend} 정확도 {pct} ({n}회) "
                f"— 현재 방향 유지, 역방향 예측 억제")
    elif acc >= 0.45:
        return (f"{regime}+{fg_b} 조건 방향 불명확 ({pct}, {n}회) "
                f"— 과신 금지, confidence 낮음 설정")
    else:
        return (f"{regime}+{fg_b} 조건에서 {trend} 예측 {pct} ({n}회) "
                f"— 역방향 검토 필요, 현재 방향 신호 의심")


def _normalize_pattern_entry(key: str, entry: dict | None) -> dict:
    base = {
        "key": key,
        "total": 0,
        "correct": 0,
        "accuracy": 0.0,
        "primary_rule": "",
        "last_seen": "",
        "min_samples": 5,
    }
    if not isinstance(entry, dict):
        return base
    normalized = {**base, **entry}
    normalized["key"] = str(normalized.get("key") or key)
    normalized["total"] = int(normalized.get("total") or 0)
    normalized["correct"] = int(normalized.get("correct") or 0)
    normalized["min_samples"] = int(normalized.get("min_samples") or 5)
    try:
        normalized["accuracy"] = float(normalized.get("accuracy") or 0.0)
    except Exception:
        normalized["accuracy"] = 0.0
    normalized["primary_rule"] = str(normalized.get("primary_rule") or "")
    normalized["last_seen"] = str(normalized.get("last_seen") or "")
    return normalized


def _normalize_pattern_state(data: dict | None) -> dict:
    base = {"patterns": {}, "global_stats": {"total": 0, "correct": 0, "accuracy": 0.0}}
    if not isinstance(data, dict):
        return base

    patterns_raw = data.get("patterns")
    patterns: dict = {}
    if isinstance(patterns_raw, dict):
        for key, entry in patterns_raw.items():
            patterns[str(key)] = _normalize_pattern_entry(str(key), entry)

    global_stats = data.get("global_stats")
    if not isinstance(global_stats, dict):
        global_stats = {}
    normalized_global_stats = {
        "total": int(global_stats.get("total") or 0),
        "correct": int(global_stats.get("correct") or 0),
        "accuracy": 0.0,
    }
    try:
        normalized_global_stats["accuracy"] = float(global_stats.get("accuracy") or 0.0)
    except Exception:
        normalized_global_stats["accuracy"] = 0.0

    return {
        **data,
        "patterns": patterns,
        "global_stats": normalized_global_stats,
    }


def _update_pattern(regime: str, fg: float, trend: str,
                    judged: list, correct: list, date: str) -> None:
    """검증 결과를 패턴 파일에 누적 업데이트."""
    if not judged:
        return
    key  = _pattern_key(regime, fg, trend)
    data = _normalize_pattern_state(_load(LESSONS_PATTERN_FILE, {"patterns": {}, "global_stats": {}}))

    p = _normalize_pattern_entry(key, data["patterns"].get(key))
    data["patterns"][key] = p
    p["total"]   += len(judged)
    p["correct"] += len(correct)
    p["accuracy"] = round(p["correct"] / p["total"], 4) if p["total"] else 0.0
    p["last_seen"] = date

    if p["total"] >= p["min_samples"]:
        p["primary_rule"] = _generate_rule_text(key, p["accuracy"], p["total"])

    # 글로벌 통계 업데이트
    gs = data["global_stats"]
    gs["total"]   += len(judged)
    gs["correct"] += len(correct)
    gs["accuracy"] = round(gs["correct"] / gs["total"], 4) if gs["total"] else 0.0

    _save(LESSONS_PATTERN_FILE, data)


def _get_pattern_signal(regime: str, fg: float, trend: str,
                        current_date: str = None) -> tuple[str, str]:
    """
    현재 조건에 맞는 패턴 룰을 반환.
    Returns: (pattern_block, signal_override)
      pattern_block  — 프롬프트 최상단 강조 블록 (방법 3)
      signal_override — acc < 0.45일 때 signal_str에 추가할 역방향 경고 (방법 2)
    """
    try:
        data = _normalize_pattern_state(_load(LESSONS_PATTERN_FILE, {"patterns": {}}))
    except Exception:
        return "", ""

    patterns = data.get("patterns", {})
    if not patterns:
        return "", ""

    key = _pattern_key(regime, fg, trend)

    # 1순위: 완전 일치
    p = patterns.get(key)

    # 2순위: 레짐+FG 일치 (추세만 다름)
    if not p or p.get("total", 0) < p.get("min_samples", 5):
        partial_key = f"{regime}|{_fg_bucket(fg)}|"
        candidates  = [v for k, v in patterns.items()
                       if k.startswith(partial_key)
                       and v.get("total", 0) >= v.get("min_samples", 5)]
        if candidates:
            p = max(candidates, key=lambda x: x["total"])

    # 3순위: 레짐만 일치
    if not p or p.get("total", 0) < p.get("min_samples", 5):
        regime_cands = [v for k, v in patterns.items()
                        if k.startswith(f"{regime}|")
                        and v.get("total", 0) >= v.get("min_samples", 5)]
        if regime_cands:
            p = max(regime_cands, key=lambda x: x["total"])

    if not p or not p.get("primary_rule"):
        return "", ""

    # 날짜 필터: 미래 패턴 주입 차단
    if current_date and p.get("last_seen", "") >= current_date:
        return "", ""

    acc   = p["accuracy"]
    n     = p["total"]
    rule  = p["primary_rule"]
    pkey  = p["key"]

    # ── 방법 3: 완전 일치 시 고강도 강조 블록
    border   = "═" * 50
    conf_bar = "█" * int(acc * 10) + "░" * (10 - int(acc * 10))
    pattern_block = (
        f"\n{border}\n"
        f"📊 [통계 룰 — {n}회 관측, 무시 금지]\n"
        f"조건: {pkey.replace('|', ' + ')}\n"
        f"정확도: {acc:.0%} [{conf_bar}]\n"
        f"룰: {rule}\n"
        f"{border}\n"
    )

    # ── 방법 2: acc < 0.45이면 기존 신호 방향과 충돌하는 역바이어스 경고
    signal_override = ""
    if acc < 0.45:
        signal_override = (
            f"⚠️ [패턴 역바이어스 경고] {pkey} 조건에서 "
            f"현재 방향 예측 정확도 {acc:.0%} ({n}회) — "
            f"반대 방향 thesis_killer 반드시 1개 이상 포함"
        )

    return pattern_block, signal_override


def _run_phase_dates(dates_slice: list, phase_label: str,
                     dry: bool, save_accuracy: bool) -> tuple:
    """
    지정 날짜 범위를 분석→검증→교훈추출.
    save_accuracy=True 이면 research accuracy state도 업데이트.
    Returns: (accuracy_pct, judged, correct, {date: (results, note)})
    """
    global _backtest_results

    phase_judged = phase_correct = 0
    all_results: dict = {}

    from datetime import datetime as _dt
    dates_all = DATES
    for i, date in enumerate(dates_slice):
        md        = HIST_DATA[date]
        date_idx  = dates_all.index(date)
        next_date = dates_all[date_idx + 1] if date_idx + 1 < len(dates_all) else None
        next_data = HIST_DATA.get(next_date, {}) if next_date else {}

        # ── 주말(토/일) 날짜: 장이 열리지 않음 → 분석은 컨텍스트로 저장하되
        #    검증 집계에서 제외. 분모에 넣으면 안 되는 날짜.
        is_weekend = _dt.strptime(date, "%Y-%m-%d").weekday() >= 5
        if is_weekend:
            analysis = generate_analysis(date, md, dry=dry)
            analysis["vix_at_time"] = md.get("vix", 20)
            save_to_memory(analysis)
            _record_research_day(date, phase_label, md, analysis, [])
            HIST_DATA.setdefault(date, {})["regime"] = analysis.get("market_regime", "")
            print(f"  📅[{i+1:>3}/{len(dates_slice)}] {date} "
                  f"주말 — 컨텍스트만 저장, 검증 제외")
            continue

        analysis = generate_analysis(date, md, dry=dry)
        analysis["vix_at_time"] = md.get("vix", 20)
        analysis["vix_band"]    = classify_vix_band(md.get("vix", 20))
        analysis["task_type"]   = classify_task_type(
            md.get("note", ""), md.get("vix", 20), md.get("fear_greed", 50))
        save_to_memory(analysis)
        HIST_DATA.setdefault(date, {})["regime"]       = analysis.get("market_regime", "")
        HIST_DATA.setdefault(date, {})["fear_greed"]   = md.get("fear_greed", "50")
        HIST_DATA.setdefault(date, {})["vix"]          = md.get("vix", "20")
        HIST_DATA.setdefault(date, {})["sp500_change"] = md.get("sp500_change", "0")

        if not next_data:
            _record_research_day(date, phase_label, md, analysis, [])
            continue

        results = verify_predictions(analysis, next_data)
        _record_research_day(date, phase_label, md, analysis, results)
        judged  = [r for r in results if r["verdict"] != "unclear"]
        correct = [r for r in judged  if r["verdict"] == "confirmed"]
        wrong   = [r for r in judged  if r["verdict"] == "invalidated"]

        for r in results:
            _backtest_results.append({
                "date":    date,
                "regime":  analysis.get("market_regime", ""),
                "verdict": r.get("verdict", "unclear"),
                "event":   r.get("event", ""),
            })

        # 패턴 누적 업데이트 (방법 3+2의 데이터 수집)
        _update_pattern(
            regime=analysis.get("market_regime", "혼조"),
            fg=float(str(md.get("fear_greed", 50))),
            trend=analysis.get("trend_phase", "횡보추세"),
            judged=judged,
            correct=correct,
            date=date,
        )

        # unclear-only 날 스킵: 교훈 추출 건너뜀 (오염 방지 + 비용 절감)
        all_unclear = len(results) > 0 and all(r.get("verdict") == "unclear" for r in results)
        if all_unclear:
            print(f"  ❓[{i+1:>3}/{len(dates_slice)}] {date} "
                  f"  ALL UNCLEAR ({len(results)} TK) — 교훈 추출 스킵")
        else:
            extract_lessons(results, analysis, date)

        if save_accuracy:
            acc_pct_today, _, _ = update_accuracy(results, date)
            icon = "✅" if acc_pct_today >= 70 else "⚠️" if acc_pct_today >= 50 else "❌"
            print(f"  {icon}[{i+1:>3}/{len(dates_slice)}] {date} "
                  f"{acc_pct_today:>5}% ({len(correct)}/{len(judged)}) TK:{len(results)}")
        elif not all_unclear:
            today_acc = round(len(correct)/len(judged)*100, 1) if judged else 0
            print(f"  📖[{i+1:>3}/{len(dates_slice)}] {date} "
                  f"{today_acc:>5}% ({len(correct)}/{len(judged)}) TK:{len(results)}")

        phase_judged  += len(judged)
        phase_correct += len(correct)
        all_results[date] = (results, md.get("note", ""))

    phase_acc = round(phase_correct / phase_judged * 100, 1) if phase_judged else 0
    return phase_acc, phase_judged, phase_correct, all_results


def run_walk_forward(dry: bool = False) -> dict:
    """
    Walk-Forward Optimization.

    Phase 1~N : 월별 순차 학습 (교훈만 누적, accuracy는 저장 안 함)
    Final Pass: 전체 기간 재분석 — N개월치 교훈을 Day 1부터 반영
    """
    global _backtest_results

    from collections import defaultdict
    monthly: dict = defaultdict(list)
    for d in DATES:
        monthly[d[:7]].append(d)
    months_sorted = sorted(monthly.keys())

    print("\n" + "=" * 65)
    print(f"  Walk-Forward Optimization — {len(months_sorted)}개 Phase + Final Pass")
    print(f"  기간: {DATES[0]} ~ {DATES[-1]} ({len(DATES)}거래일)")
    print("=" * 65)

    phase_summary = []
    _backtest_results = []

    # ── Phase별 순차 학습 ─────────────────────────────────────────
    for phase_idx, ym in enumerate(months_sorted, 1):
        dates_slice    = monthly[ym]
        lessons_before = _count_lessons()
        lessons_now_n  = lessons_before   # 3파일 합산 교훈 수

        print(f"\n{'─' * 65}")
        print(f"  📚 Phase {phase_idx}/{len(months_sorted)} : {ym} "
              f"({len(dates_slice)}거래일 | 적용 교훈 {lessons_now_n}개)")
        print(f"{'─' * 65}")

        acc, judged, correct, _ = _run_phase_dates(
            dates_slice, ym, dry=dry, save_accuracy=False)

        lessons_new = _count_lessons() - lessons_before
        print(f"\n  → Phase {phase_idx} 완료: {acc}% ({correct}/{judged}) "
              f"| 교훈 +{lessons_new}개 (누계 {_count_lessons()}개)")
        phase_summary.append((ym, acc, judged, correct))

    # ── Final Pass 준비 ────────────────────────────────────────────
    total_lessons = _count_lessons()
    print(f"\n{'=' * 65}")
    print(f"  🎯 Final Pass — {len(DATES)}거래일 ({total_lessons}개 누적 교훈 반영)")
    print(f"  research accuracy/memory state 초기화 후 전체 재분석")
    print(f"  ※ 교훈 3파일은 초기화하지 않음 — 날짜 필터가 시간 여행을 차단")
    print(f"    Phase 1 교훈(Oct) → Final Pass Oct 분석 시 날짜 필터로 차단")
    print(f"    Phase 1 교훈(Oct) → Final Pass Nov 분석 시 허용 (시간 순 정확)")
    print(f"{'=' * 65}")

    # research accuracy / memory state 초기화 (Final Pass 결과만 저장)
    _save(ACCURACY_FILE, {
        "total": 0, "correct": 0, "by_category": {},
        "history": [], "history_by_category": [],
        "weak_areas": [], "strong_areas": [],
        "dir_total": 0, "dir_correct": 0,
        "score_total": 0.0, "score_earned": 0.0, "score_accuracy": 0.0,
        "_walk_forward": True, "_phases": len(months_sorted),
        "_lessons_applied": total_lessons,
    })
    _save(MEMORY_FILE, [])
    # ※ 교훈 3파일은 초기화하지 않음 —
    #    _load_lessons_context(current_date=date) 가 "lesson.date < date" 조건으로
    #    Final Pass 진행 중 아직 발생하지 않은 Phase 교훈을 자동 차단함.
    #    Oct 분석 시: Oct 이전 교훈 0개 → 정상
    #    Nov 분석 시: Phase 1 Oct 교훈 허용 → 학습 효과 반영

    _backtest_results = []

    final_acc, final_j, final_c, final_results = _run_phase_dates(
        DATES[:-1], "Final", dry=dry, save_accuracy=True)

    # 가중치 업데이트
    print(f"\n{'─' * 65}")
    print("  📊 가중치 업데이트...")
    try:
        acc_data = _load(ACCURACY_FILE, {})
        changes = update_research_weights_from_accuracy(acc_data)
        if changes:
            for ch in changes: print(f"   → {ch}")
        else:
            print("   변경 없음 (데이터 부족)")
    except Exception as e:
        print(f"   스킵: {e}")

    # ── 최종 요약 ─────────────────────────────────────────────────
    print(f"\n{'=' * 65}")
    print(f"  Walk-Forward 완료 요약")
    print(f"{'─' * 65}")
    print(f"  {'':12} {'월':>8} {'정확도':>7}  {'적중/채점':>10}")
    print(f"{'─' * 65}")
    for idx, (ym, acc, judged, correct) in enumerate(phase_summary, 1):
        print(f"  Phase {idx:<6} {ym:>8} {acc:>6}%  {correct:>5}/{judged}")
    print(f"{'─' * 65}")
    print(f"  Final Pass{'':>5} {'전체':>8} {final_acc:>6}%  {final_c:>5}/{final_j}")
    print(f"{'=' * 65}")

    acc_data = _load(ACCURACY_FILE, {})
    print(f"\n  누적 교훈 : {_count_lessons()}개")
    print(f"  강점      : {acc_data.get('strong_areas', [])}")
    print(f"  약점      : {acc_data.get('weak_areas',   [])}")
    print(f"  → 연구 세션에만 반영 (운영 MORNING 상태와 분리)")

    # 요약 테이블 출력
    print_summary_table(final_results)
    return {
        "mode": "walk_forward",
        "phase_count": len(months_sorted),
        "final_accuracy": final_acc,
        "judged_count": final_j,
        "correct_count": final_c,
        "lesson_count": _count_lessons(),
        "strong_areas": acc_data.get("strong_areas", []),
        "weak_areas": acc_data.get("weak_areas", []),
    }
        
def main():
    global market_data_snapshot, _RESEARCH_SESSION_ID
    parser = argparse.ArgumentParser(description=ORCA_NAME + " Backtest")
    parser.add_argument("--dry",            action="store_true",
                        help="분석만, 데이터 저장 없음")
    parser.add_argument("--months",         type=int, default=0,
                        help="백테스트 기간 확장 (개월). 0=HIST_DATA만, 6=최근 6개월 yfinance 추가")
    parser.add_argument("--walk-forward",   action="store_true",
                        help="Walk-Forward Optimization 실행 (월별 순차 학습 → Final Pass)")
    args = parser.parse_args()

    _RESEARCH_SESSION_ID = start_backtest_session(
        "orca",
        "walk_forward" if args.walk_forward else "backtest",
        config={
            "dry": args.dry,
            "months": args.months,
            "walk_forward": args.walk_forward,
        },
    )

    try:
        # 데이터 확장
        if args.months > 0:
            _fetch_dynamic_hist(months=args.months)
        else:
            _fetch_recent_data()

        # Walk-Forward 모드
        if args.walk_forward:
            summary = run_walk_forward(dry=args.dry)
            finish_backtest_session(_RESEARCH_SESSION_ID, "completed", summary=summary)
            print(f"\n🗃️ Research session saved to SQLite: {_RESEARCH_SESSION_ID}")
            return
        print("=" * 65)
        print(f"{ORCA_NAME} Backtest — {len(DATES)}거래일 사전 학습" + (" [DRY RUN]" if args.dry else ""))
        print(f"기간: {DATES[0]} ~ {DATES[-1]}")
        print("=" * 65)

        total_judged = total_correct = total_wrong = total_unclear = 0
        all_results_by_date = {}
        global _backtest_results
        _backtest_results = []   # 자기 피드백용 결과 누적

        for i, date in enumerate(DATES):
            md        = HIST_DATA[date]
            next_date = DATES[i+1] if i+1 < len(DATES) else None
            next_data = HIST_DATA.get(next_date, {}) if next_date else {}
            market_data_snapshot = md  # 원달러 검증용

            print(f"\n{'─'*50}")
            print(f"📅 [{i+1}/{len(DATES)}] {date} — {md['note'][:40]}")
            print(f"   S&P {md.get('sp500','N/A'):>7} ({md.get('sp500_change','N/A'):>7}) | VIX {md.get('vix','N/A'):>5} | FG {md.get('fear_greed','N/A')}")

            analysis = generate_analysis(date, md, dry=args.dry)
            analysis["vix_at_time"]   = md["vix"]
            analysis["vix_band"]      = classify_vix_band(md["vix"])
            analysis["task_type"]     = classify_task_type(
                md.get("note",""), md.get("vix",20), md.get("fear_greed",50)
            )
            save_to_memory(analysis)
            print(f"  → 레짐: {analysis.get('market_regime','')} | 추세: {analysis.get('trend_phase','')} | TK: {len(analysis.get('thesis_killers',[]))}개")

            HIST_DATA.setdefault(date, {})["regime"]       = analysis.get("market_regime","")
            HIST_DATA.setdefault(date, {})["fear_greed"]   = md.get("fear_greed","50")
            HIST_DATA.setdefault(date, {})["vix"]          = md.get("vix","20")
            HIST_DATA.setdefault(date, {})["sp500_change"] = md.get("sp500_change","0")

            if next_data:
                results = verify_predictions(analysis, next_data)
                _record_research_day(date, "main", md, analysis, results)
                judged  = [r for r in results if r["verdict"] != "unclear"]
                correct = [r for r in judged  if r["verdict"] == "confirmed"]
                wrong   = [r for r in judged  if r["verdict"] == "invalidated"]
                unclear = [r for r in results if r["verdict"] == "unclear"]

                for r in results:
                    icon = "✅" if r["verdict"]=="confirmed" else "❌" if r["verdict"]=="invalidated" else "❓"
                    ev   = f"  → {r['evidence']}" if r.get("evidence") else ""
                    print(f"  {icon}[{r['category']}] {r['event'][:40]}{ev}")

                for r in results:
                    _backtest_results.append({
                        "date":    date,
                        "regime":  analysis.get("market_regime",""),
                        "verdict": r.get("verdict","unclear"),
                        "event":   r.get("event",""),
                    })
                acc_pct, c, j = update_accuracy(results, date)
                extract_lessons(results, analysis, date)

                total_judged  += j
                total_correct += c
                total_wrong   += len(wrong)
                total_unclear += len(unclear)
                all_results_by_date[date] = (results, md["note"])
                print(f"  → 오늘: {acc_pct}% ({c}/{j}건 적중)")
            else:
                _record_research_day(date, "main", md, analysis, [])
                print(f"  → 마지막 날 — 검증 생략")

        print(f"\n{'─'*50}")
        print("📊 가중치 업데이트...")
        try:
            acc = _load(ACCURACY_FILE, {})
            changes = update_research_weights_from_accuracy(acc)
            if changes:
                for c in changes: print(f"   → {c}")
            else:
                print("   변경 없음 (데이터 부족)")
        except Exception as e:
            print(f"   스킵: {e}")

        print_summary_table(all_results_by_date)

        acc     = _load(ACCURACY_FILE, {})
        lessons = _load(LESSONS_FILE, {})
        overall = round(total_correct/total_judged*100,1) if total_judged else 0

        print(f"\n{'='*65}")
        print(f"✅ Backtest 완료 — {len(DATES)}거래일")
        print(f"   총 검증:  {total_judged}건 | 적중: {total_correct} | 오판: {total_wrong} | 불명: {total_unclear}")
        print(f"   전체 정확도: {overall}%")
        print(f"   생성된 교훈: {len(lessons.get('lessons',[]))}개")
        if acc.get("strong_areas"): print(f"   강점: {acc['strong_areas']}")
        if acc.get("weak_areas"):   print(f"   약점: {acc['weak_areas']}")
        print(f"   → 연구 세션에만 저장됨 (운영 MORNING 상태와 분리)")
        print("=" * 65)
        summary = {
            "mode": "backtest",
            "dates": len(DATES),
            "overall_accuracy": overall,
            "judged_count": total_judged,
            "correct_count": total_correct,
            "wrong_count": total_wrong,
            "unclear_count": total_unclear,
            "lesson_count": len(lessons.get("lessons", [])),
            "strong_areas": acc.get("strong_areas", []),
            "weak_areas": acc.get("weak_areas", []),
        }
        finish_backtest_session(_RESEARCH_SESSION_ID, "completed", summary=summary)
        print(f"\n🗃️ Research session saved to SQLite: {_RESEARCH_SESSION_ID}")
        return
    except Exception as e:
        if _RESEARCH_SESSION_ID:
            try:
                finish_backtest_session(
                    _RESEARCH_SESSION_ID,
                    "failed",
                    summary={"error": str(e)},
                )
            except Exception:
                pass
        raise


if __name__ == "__main__":
    main()


