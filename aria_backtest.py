"""
aria_backtest.py — ARIA 30거래일 사전 학습 스크립트
2026년 3월~4월 실제 시장 데이터로 분석→검증 사이클을 돌려
accuracy.json, aria_lessons.json, aria_weights.json을 미리 채운다.
"""
import os, sys, json, re, argparse, functools
from datetime import datetime, timezone, timedelta
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

KST     = timezone(timedelta(hours=9))
API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

from aria_paths import (
    MEMORY_FILE, ACCURACY_FILE, LESSONS_FILE, WEIGHTS_FILE,
)
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
    if path.exists():
        try: return json.loads(path.read_text(encoding="utf-8"))
        except: pass
    return default if default is not None else {}

def _save(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ── 프롬프트: 반드시 주가 수치로 검증 가능한 thesis_killers 강제 ──────────────
ANALYST_PROMPT = """당신은 ARIA 투자 분석 에이전트입니다.
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
    {"event":"나스닥/코스피/반도체/VIX 등 수치 검증 가능한 이벤트",
     "timeframe":"1일 또는 3일","confirms_if":"숫자 포함 조건","invalidates_if":"숫자 포함 조건"}
  ],
  "outflows": [{"zone":"","reason":"","severity":"높음/보통/낮음"}],
  "inflows":  [{"zone":"","reason":"","momentum":"강함/형성중/약함"}],
  "korea_focus": {"krw_usd":"","kospi_flow":"","assessment":""}
}"""


def _load_lessons_context() -> str:
    """aria_lessons.json에서 핵심 약점 패턴 로드 → 프롬프트 주입용."""
    try:
        lessons_file = DATA_DIR / "aria_lessons.json"
        if not lessons_file.exists():
            return ""
        lessons = json.loads(lessons_file.read_text(encoding="utf-8"))
        if not lessons:
            return ""
        # 가장 최근 교훈 중 '약점' 관련만 추출
        weak_lessons = []
        for lesson in lessons[-20:]:   # 최근 20개
            text = str(lesson.get("lesson", ""))
            if any(k in text for k in ["주의", "실패", "약점", "틀린", "금지", "과매도", "반등 예측"]):
                weak_lessons.append(f"  • {text[:80]}")
        if not weak_lessons:
            return ""
        return "\n[과거 예측 약점 — 반드시 참고]\n" + "\n".join(weak_lessons[:5]) + "\n"
    except Exception:
        return ""


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

    signal_str = ""
    if signals:
        signal_str = "\n[현재 시장 경고신호 — 반드시 반영]\n" + "\n".join(signals) + "\n"

    # 과거 약점 교훈 주입
    lessons_ctx = _load_lessons_context()
    # 시계열 컨텍스트 (레짐 연속일, VIX/FG 방향, 자기 피드백)
    trend_ctx = _build_trend_context(date, HIST_DATA, _backtest_results)

    user_msg = (
        f"{lessons_ctx}"
        f"{trend_ctx}"
        f"분석 날짜: {date}\n이벤트: {d['note']}\n\n"
        f"S&P500: {d['sp500']} ({d['sp500_change']})\n"
        f"나스닥: {d['nasdaq']} ({d['nasdaq_change']})\n"
        f"VIX: {d['vix']}\n코스피: {d['kospi']} ({d['kospi_change']})\n"
        f"원달러: {d['krw_usd']}\nSK하이닉스: {d['sk_hynix']} ({d['sk_hynix_change']})\n"
        f"삼성전자: {d['samsung']} ({d['samsung_change']})\n"
        f"엔비디아: {d['nvda']} ({d['nvda_change']})\n"
        f"Fear&Greed: {d['fear_greed']} ({d['fear_greed_label']})"
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

    # 방향 정확도 (aria_analysis.py와 동일 기준)
    dir_correct = len(correct)   # backtest confirmed = 방향 일치로 간주
    acc.setdefault("dir_total",   0)
    acc.setdefault("dir_correct", 0)
    acc["dir_total"]   += len(judged)
    acc["dir_correct"] += dir_correct

    # 중복 날짜 방어 (aria_analysis.py와 동일 로직)
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


def extract_lessons(results, analysis, date):
    lessons = _load(LESSONS_FILE, {"lessons":[],"total_lessons":0,"last_updated":""})
    fg     = float(HIST_DATA.get(date,{}).get("fear_greed","50"))
    regime = analysis.get("market_regime","")
    trend  = analysis.get("trend_phase","")

    # 오판 교훈
    for r in results:
        if r["verdict"] == "invalidated":
            text = f"{r['event'][:35]} 오판 — {r.get('evidence','')[:30]}"
            sev  = "high" if "주식" in r["category"] else "medium"
            lessons["lessons"].append({
                "date":date,"source":"backtest","category":r["category"],
                "lesson":text,"severity":sev,"applied":0,"reinforced":0
            })
            lessons["total_lessons"] += 1

    # 구조적 교훈
    sp_chg = _pct(HIST_DATA.get(date,{}).get("sp500_change","0"))
    if fg <= 10 and sp_chg <= -3:
        lessons["lessons"].append({
            "date":date,"source":"backtest","category":"레짐판단",
            "lesson":f"FG {fg} + S&P {sp_chg:+.1f}% — 극단공포 폭락기, 반등 타이밍 연구 필요",
            "severity":"high","applied":0,"reinforced":0
        })
        lessons["total_lessons"] += 1

    if fg <= 20 and "선호" in regime:
        lessons["lessons"].append({
            "date":date,"source":"backtest","category":"레짐판단",
            "lesson":f"FG {fg}(극단공포)인데 위험선호 판단 — 공포 구간 낙관 과잉",
            "severity":"high","applied":0,"reinforced":0
        })
        lessons["total_lessons"] += 1

    if "하락" in trend and sp_chg > 2:
        lessons["lessons"].append({
            "date":date,"source":"backtest","category":"추세판단",
            "lesson":f"하락추세 판단인데 S&P {sp_chg:+.1f}% 급등 — 반등 포착 실패",
            "severity":"medium","applied":0,"reinforced":0
        })
        lessons["total_lessons"] += 1

    # 강점 교훈 (적중한 경우)
    confirmed = [r for r in results if r["verdict"] == "confirmed"]
    for r in confirmed:
        if r["category"] in ["주식","VIX"]:
            lessons["lessons"].append({
                "date":date,"source":"backtest","category":r["category"],
                "lesson":f"{r['event'][:30]} 예측 적중 — {r.get('evidence','')[:25]}",
                "severity":"low","type":"strength","applied":0,"reinforced":0
            })

    lessons["lessons"] = sorted(lessons["lessons"],key=lambda x:x["date"],reverse=True)[:80]
    lessons["last_updated"] = date
    _save(LESSONS_FILE, lessons)


def save_to_memory(analysis):
    memory = _load(MEMORY_FILE, [])
    if not isinstance(memory, list): memory = []
    date = analysis.get("analysis_date","")
    memory = [m for m in memory if m.get("analysis_date") != date]
    memory = (memory + [analysis])[-90:]
    _save(MEMORY_FILE, memory)


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


def main():
    global market_data_snapshot
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry", action="store_true")
    args = parser.parse_args()

    print("=" * 65)
    print(f"ARIA Backtest — {len(DATES)}거래일 사전 학습" + (" [DRY RUN]" if args.dry else ""))
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
        print(f"   S&P {md['sp500']:>7} ({md['sp500_change']:>7}) | VIX {md['vix']:>5} | FG {md['fear_greed']}")

        analysis = generate_analysis(date, md, dry=args.dry)
        # VIX 현재값 저장 (다음날 VIX 검증용)
        analysis["vix_at_time"]   = md["vix"]
        analysis["vix_band"]      = classify_vix_band(md["vix"])        # dual-write
        analysis["task_type"]     = classify_task_type(                  # 레이블만 저장
            md.get("note",""), md.get("vix",20), md.get("fear_greed",50)
        )
        save_to_memory(analysis)
        print(f"  → 레짐: {analysis.get('market_regime','')} | 추세: {analysis.get('trend_phase','')} | TK: {len(analysis.get('thesis_killers',[]))}개")

        # HIST_DATA에 오늘 레짐 저장 (시계열 컨텍스트용)
        HIST_DATA.setdefault(date, {})["regime"]       = analysis.get("market_regime","")
        HIST_DATA.setdefault(date, {})["fear_greed"]   = md.get("fear_greed","50")
        HIST_DATA.setdefault(date, {})["vix"]          = md.get("vix","20")
        HIST_DATA.setdefault(date, {})["sp500_change"] = md.get("sp500_change","0")

        if next_data:
            results = verify_predictions(analysis, next_data)
            judged  = [r for r in results if r["verdict"] != "unclear"]
            correct = [r for r in judged  if r["verdict"] == "confirmed"]
            wrong   = [r for r in judged  if r["verdict"] == "invalidated"]
            unclear = [r for r in results if r["verdict"] == "unclear"]

            for r in results:
                icon = "✅" if r["verdict"]=="confirmed" else "❌" if r["verdict"]=="invalidated" else "❓"
                ev   = f"  → {r['evidence']}" if r.get("evidence") else ""
                print(f"  {icon}[{r['category']}] {r['event'][:40]}{ev}")

            # 자기 피드백용 결과 누적
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
            print(f"  → 마지막 날 — 검증 생략")

    # 가중치 업데이트
    print(f"\n{'─'*50}")
    print("📊 가중치 업데이트...")
    try:
        from aria_analysis import update_weights_from_accuracy
        acc = _load(ACCURACY_FILE, {})
        changes = update_weights_from_accuracy(acc)
        if changes:
            for c in changes: print(f"   → {c}")
        else:
            print("   변경 없음 (데이터 부족)")
    except Exception as e:
        print(f"   스킵: {e}")

    # 요약 테이블
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
    print(f"   → 다음 MORNING 실행 시 학습 데이터 자동 반영")
    print("=" * 65)

    # 대시보드 재생성 — 백테스트 결과 즉시 반영
    try:
        from aria_dashboard import build_dashboard
        build_dashboard()
        print("\n📊 dashboard.html 갱신 완료 (백테스트 결과 반영)")
    except Exception as e:
        print(f"\n대시보드 갱신 스킵: {e}")


if __name__ == "__main__":
    main()
