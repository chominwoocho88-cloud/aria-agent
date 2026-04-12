"""
aria_backtest.py — ARIA 30거래일 사전 학습 스크립트
2026년 3월~4월 실제 시장 데이터로 분석→검증 사이클을 돌려
accuracy.json, aria_lessons.json, aria_weights.json을 미리 채운다.
"""
import os, sys, json, re, argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

KST     = timezone(timedelta(hours=9))
API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

MEMORY_FILE   = Path("memory.json")
ACCURACY_FILE = Path("accuracy.json")
LESSONS_FILE  = Path("aria_lessons.json")
WEIGHTS_FILE  = Path("aria_weights.json")
MODEL         = "claude-sonnet-4-6"

# ── 실제 30거래일 데이터 (2026-03-03 ~ 2026-04-11) ──────────────────────────
HIST_DATA = {
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
1. event는 반드시 내일 주가/지수로 검증 가능한 것만 (나스닥, 코스피, 반도체주, VIX, 원달러)
2. confirms_if / invalidates_if에 반드시 숫자 포함 (예: "나스닥 +1% 이상", "코스피 -1% 이하")
3. "협상 진전", "분위기 개선" 같은 뉴스 이벤트는 절대 금지
4. 검증 불가능한 event는 생성 금지

[올바른 예시]
event: "나스닥 기술주 방향성"
confirms_if: "나스닥 +1% 이상 상승"
invalidates_if: "나스닥 -1% 이하 하락"

event: "반도체 섹터 (SK하이닉스) 방향성"
confirms_if: "SK하이닉스 +2% 이상"
invalidates_if: "SK하이닉스 -2% 이하"

event: "VIX 공포지수 완화"
confirms_if: "VIX 현재보다 10% 이상 하락"
invalidates_if: "VIX 현재보다 10% 이상 상승"

Return ONLY valid JSON. No markdown.
{
  "analysis_date": "",
  "market_regime": "위험선호/위험회피/전환중/혼조",
  "trend_phase": "상승추세/횡보추세/하락추세",
  "confidence_overall": "낮음/보통/높음",
  "one_line_summary": "",
  "thesis_killers": [
    {"event":"나스닥/코스피/반도체/VIX 등 수치 검증 가능한 이벤트",
     "timeframe":"1일","confirms_if":"숫자 포함 조건","invalidates_if":"숫자 포함 조건"}
  ],
  "outflows": [{"zone":"","reason":"","severity":"높음/보통/낮음"}],
  "inflows":  [{"zone":"","reason":"","momentum":"강함/형성중/약함"}],
  "korea_focus": {"krw_usd":"","kospi_flow":"","assessment":""}
}"""


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
    user_msg = (
        f"분석 날짜: {date}\n이벤트: {d['note']}\n\n"
        f"S&P500: {d['sp500']} ({d['sp500_change']})\n"
        f"나스닥: {d['nasdaq']} ({d['nasdaq_change']})\n"
        f"VIX: {d['vix']}\n코스피: {d['kospi']} ({d['kospi_change']})\n"
        f"원달러: {d['krw_usd']}\nSK하이닉스: {d['sk_hynix']} ({d['sk_hynix_change']})\n"
        f"삼성전자: {d['samsung']} ({d['samsung_change']})\n"
        f"엔비디아: {d['nvda']} ({d['nvda_change']})\n"
        f"Fear&Greed: {d['fear_greed']} ({d['fear_greed_label']})\n\n"
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


def verify_predictions(analysis, next_data):
    """유연한 thesis_killers 검증 — 수치 기반 + 키워드 확장"""
    results = []

    nq  = _pct(next_data.get("nasdaq_change"))
    sp  = _pct(next_data.get("sp500_change"))
    ks  = _pct(next_data.get("kospi_change"))
    sk  = _pct(next_data.get("sk_hynix_change"))
    sam = _pct(next_data.get("samsung_change"))
    nv  = _pct(next_data.get("nvda_change"))
    try:   vix_now  = float(next_data.get("vix", 25))
    except: vix_now = 25.0
    try:   vix_prev = float(analysis.get("vix_at_time", vix_now))
    except: vix_prev = vix_now

    def extract_threshold(text):
        """텍스트에서 숫자 임계값 추출"""
        nums = re.findall(r"[+-]?\d+\.?\d*", str(text))
        return float(nums[0]) if nums else None

    def check_direction(chg, conf_text, inv_text):
        """등락 방향 + 임계값으로 verdict 결정"""
        conf_thr = extract_threshold(conf_text) or 1.0
        inv_thr  = extract_threshold(inv_text) or 1.0

        # confirms_if 방향 판단
        conf_up   = any(w in conf_text.lower() for w in ["상승","반등","올라","증가","+"])
        conf_down = any(w in conf_text.lower() for w in ["하락","급락","내려","감소","-"])
        inv_up    = any(w in inv_text.lower() for w in ["상승","반등","올라","증가","+"])
        inv_down  = any(w in inv_text.lower() for w in ["하락","급락","내려","감소","-"])

        abs_thr = abs(conf_thr) if conf_thr else 1.0

        if conf_up and chg >= abs_thr:
            return "confirmed",   f"실제 {chg:+.2f}% (예측: {conf_thr:+.1f}% 이상)"
        if conf_down and chg <= -abs_thr:
            return "confirmed",   f"실제 {chg:+.2f}% (예측: -{abs_thr:.1f}% 이하)"
        if inv_up and chg >= abs(inv_thr):
            return "invalidated", f"실제 {chg:+.2f}% (예측 반대)"
        if inv_down and chg <= -abs(inv_thr):
            return "invalidated", f"실제 {chg:+.2f}% (예측 반대)"

        # 임계값 미달 (변동 미미)
        if abs(chg) < 0.5:
            return "unclear", f"변동 미미 ({chg:+.2f}%)"
        return "unclear", f"방향 불명확 ({chg:+.2f}%)"

    for tk in analysis.get("thesis_killers", []):
        event = tk.get("event","").lower()
        conf  = tk.get("confirms_if","").lower()
        inv   = tk.get("invalidates_if","").lower()
        v, ev, cat = "unclear", "", "기타"

        # ── 나스닥 / 미국 기술주 ───────────────────────────────────────────
        if any(k in event for k in ["나스닥","nasdaq","기술주","미국 주식","s&p","sp500","빅테크"]):
            cat = "주식"
            chg = nq if "나스닥" in event or "nasdaq" in event else sp
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
            conf_thr = extract_threshold(conf) or 10.0
            inv_thr  = extract_threshold(inv) or 10.0
            conf_down = any(w in conf for w in ["하락","완화","감소","-"])
            if conf_down and vix_chg_pct <= -conf_thr:
                v, ev = "confirmed", f"VIX {vix_chg_pct:+.1f}% 하락"
            elif conf_down and vix_chg_pct >= inv_thr:
                v, ev = "invalidated", f"VIX {vix_chg_pct:+.1f}% 상승 (반대)"
            elif vix_now < vix_prev * 0.9:
                v, ev = "confirmed", f"VIX {vix_now:.1f} (전일 {vix_prev:.1f})"
            elif vix_now > vix_prev * 1.1:
                v, ev = "invalidated", f"VIX {vix_now:.1f} 상승"
            else:
                v, ev = "unclear", f"VIX 변동 미미 ({vix_now:.1f})"

        # ── 원달러 ─────────────────────────────────────────────────────────
        elif any(k in event for k in ["원달러","환율","krw","원화"]):
            cat = "환율"
            try:
                krw_now  = float(next_data.get("krw_usd", 1470))
                krw_prev = float(market_data_snapshot.get("krw_usd", krw_now))
                krw_chg  = krw_now - krw_prev
                conf_down = any(w in conf for w in ["하락","강세","낮아"])
                conf_up   = any(w in conf for w in ["상승","약세","높아"])
                thr = extract_threshold(conf) or 5.0
                if conf_down and krw_chg <= -thr:
                    v, ev = "confirmed", f"원달러 {krw_now:.0f} ({krw_chg:+.0f}원)"
                elif conf_up and krw_chg >= thr:
                    v, ev = "confirmed", f"원달러 {krw_now:.0f} ({krw_chg:+.0f}원)"
                elif abs(krw_chg) < 3:
                    v, ev = "unclear", f"원달러 소폭 변동 ({krw_chg:+.0f}원)"
                else:
                    v, ev = "invalidated", f"원달러 {krw_now:.0f} (반대)"
            except: v, ev = "unclear", "환율 데이터 파싱 오류"

        results.append({
            "event":    tk.get("event",""),
            "verdict":  v, "evidence": ev, "category": cat,
            "confirms_if":   tk.get("confirms_if",""),
            "invalidates_if": tk.get("invalidates_if",""),
        })

    return results


# ── 전역 스냅샷 (원달러 검증용) ────────────────────────────────────────────
market_data_snapshot = {}


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
    acc["history"].append({"date":date,"total":len(judged),
                           "correct":len(correct),"accuracy":today_acc})
    acc["history"] = acc["history"][-90:]
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
        analysis["vix_at_time"] = md["vix"]
        save_to_memory(analysis)
        print(f"  → 레짐: {analysis.get('market_regime','')} | 추세: {analysis.get('trend_phase','')} | TK: {len(analysis.get('thesis_killers',[]))}개")

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
        for c in changes: print(f"   → {c}")
    except Exception as e:
        print(f"   스킵 ({e})")

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


if __name__ == "__main__":
    main()
