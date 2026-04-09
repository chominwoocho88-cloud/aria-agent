import os
import sys
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

KST = timezone(timedelta(hours=9))

PORTFOLIO = {
    "holdings": [
        {"name": "엔비디아",      "ticker": "NVDA",   "weight": 35.0, "type": "US_stock", "sector": "반도체/AI"},
        {"name": "SK하이닉스",    "ticker": "000660", "weight": 15.0, "type": "KR_stock", "sector": "반도체"},
        {"name": "삼성전자",      "ticker": "005930", "weight": 10.0, "type": "KR_stock", "sector": "반도체/전자"},
        {"name": "브로드컴",      "ticker": "AVGO",   "weight": 10.0, "type": "US_stock", "sector": "반도체/AI"},
        {"name": "카카오",        "ticker": "035720", "weight": 5.0,  "type": "KR_stock", "sector": "플랫폼/IT"},
        {"name": "한국고배당ETF", "ticker": "KODEX",  "weight": 10.0, "type": "KR_ETF",   "sector": "배당"},
        {"name": "SCHD",         "ticker": "SCHD",   "weight": 10.0, "type": "US_ETF",   "sector": "배당"},
        {"name": "현금",          "ticker": "CASH",   "weight": 5.0,  "type": "cash",     "sector": "현금"},
    ]
}


def analyze_portfolio(report):
    outflows = report.get("outflows", [])
    inflows  = report.get("inflows", [])
    regime   = report.get("market_regime", "")
    trend    = report.get("trend_phase", "")
    korea    = report.get("korea_focus", {})

    outflow_text = " ".join([o.get("zone", "") for o in outflows]).lower()
    inflow_text  = " ".join([i.get("zone", "") for i in inflows]).lower()

    results = []
    total_risk        = 0.0
    total_opportunity = 0.0

    for h in PORTFOLIO["holdings"]:
        name   = h["name"]
        weight = h["weight"]
        sector = h["sector"].lower()
        ticker = h["ticker"]
        status = "neutral"
        reason = ""

        if "반도체" in sector or "ai" in sector:
            if "반도체" in outflow_text or "nvidia" in outflow_text or "엔비디아" in outflow_text:
                status = "risk"
                reason = "반도체 섹터 자금 유출 감지"
            elif "반도체" in inflow_text or "ai" in inflow_text:
                status = "opportunity"
                reason = "반도체/AI 섹터 자금 유입"

            if ticker == "000660" and korea.get("sk_hynix"):
                sk = korea["sk_hynix"]
                if "-" in sk or "하락" in sk:
                    status = "risk"
                    reason = "SK하이닉스 약세: " + sk

            if ticker == "005930" and korea.get("samsung"):
                sam = korea["samsung"]
                if "-" in sam or "하락" in sam:
                    status = "risk"
                    reason = "삼성전자 약세: " + sam

        elif "플랫폼" in sector or "it" in sector:
            if "선호" in regime:
                status = "opportunity"
                reason = "위험선호 환경 수혜"
            elif "회피" in regime:
                status = "risk"
                reason = "위험회피 환경 약세"

        elif "배당" in sector:
            if "하락" in trend or "회피" in regime:
                status = "opportunity"
                reason = "하락장 방어주 수혜"

        elif sector == "현금":
            if "하락" in trend:
                status = "opportunity"
                reason = "하락장 현금 가치 상승"

        if status == "risk":
            total_risk += weight
        elif status == "opportunity":
            total_opportunity += weight

        results.append({
            "name":   name,
            "ticker": ticker,
            "weight": weight,
            "status": status,
            "reason": reason,
        })

    if total_risk >= 40:
        portfolio_risk = "높음"
    elif total_risk >= 20:
        portfolio_risk = "보통"
    else:
        portfolio_risk = "낮음"

    actions = []
    if total_risk > 40:
        actions.append("위험 노출도 높음 - 현금 비중 확대 고려")
    if "극단공포" in report.get("volatility_index", {}).get("level", ""):
        actions.append("극단 공포 구간 - 분할매수 기회 점검")
    if "회피" in regime and "하락" in trend:
        actions.append("하락추세 + 위험회피 - 방어주/배당ETF 비중 유지")

    return {
        "date":               datetime.now(KST).strftime("%Y-%m-%d"),
        "holdings":           results,
        "total_risk":         round(total_risk, 1),
        "total_opportunity":  round(total_opportunity, 1),
        "portfolio_risk":     portfolio_risk,
        "actions":            actions,
        "regime":             regime,
        "trend":              trend,
    }


def send_portfolio_report(analysis):
    try:
        from aria_telegram import send_message
    except ImportError:
        print("aria_telegram not found")
        return

    risk       = analysis["portfolio_risk"]
    risk_emoji = "🔴" if risk == "높음" else "🟡" if risk == "보통" else "🟢"

    lines = [
        "<b>💼 포트폴리오 위험 분석</b>",
        "<code>" + analysis["date"] + "</code>",
        "",
        risk_emoji + " 전체 위험도: <b>" + risk + "</b>",
        "위험 노출: " + str(analysis["total_risk"]) + "% | 기회: " + str(analysis["total_opportunity"]) + "%",
        "",
    ]

    for h in analysis["holdings"]:
        if h["ticker"] == "CASH":
            continue
        if h["status"] == "risk":
            emoji = "🔴"
        elif h["status"] == "opportunity":
            emoji = "🟢"
        else:
            emoji = "⚪"

        lines.append(emoji + " <b>" + h["name"] + "</b> (" + str(h["weight"]) + "%)")
        if h["reason"]:
            lines.append("   <i>" + h["reason"] + "</i>")

    if analysis["actions"]:
        lines.append("")
        lines.append("📌 <b>권장 액션</b>")
        for a in analysis["actions"]:
            lines.append("  - " + a)

    send_message("\n".join(lines))
    print("Portfolio report sent")


def run_portfolio(report):
    analysis = analyze_portfolio(report)
    send_portfolio_report(analysis)
    return analysis


if __name__ == "__main__":
    test = {
        "market_regime": "위험회피",
        "trend_phase": "하락추세",
        "outflows": [{"zone": "반도체/AI 섹터"}],
        "inflows":  [{"zone": "배당주/방어주"}],
        "volatility_index": {"level": "공포"},
        "korea_focus": {"sk_hynix": "-3.2%", "samsung": "+0.5%"},
    }
    result = run_portfolio(test)
    for h in result["holdings"]:
        print(h["name"] + ": " + h["status"])
