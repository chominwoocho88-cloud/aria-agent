import os
import sys
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

KST           = timezone(timedelta(hours=9))
BASELINE_FILE = Path("morning_baseline.json")


def save_baseline(report: dict, market_data: dict = None):
    """아침 분석 결과를 기준점으로 저장"""
    today = datetime.now(KST).strftime("%Y-%m-%d")

    baseline = {
        "date":            today,
        "saved_at":        datetime.now(KST).strftime("%H:%M KST"),
        "market_regime":   report.get("market_regime", ""),
        "trend_phase":     report.get("trend_phase", ""),
        "confidence":      report.get("confidence_overall", ""),
        "one_line_summary": report.get("one_line_summary", ""),
        "thesis_killers":  report.get("thesis_killers", []),
        "key_outflows":    [
            {"zone": o["zone"], "severity": o.get("severity", "")}
            for o in report.get("outflows", [])[:3]
        ],
        "key_inflows": [
            {"zone": i["zone"], "momentum": i.get("momentum", "")}
            for i in report.get("inflows", [])[:3]
        ],
        "korea_focus":     report.get("korea_focus", {}),
        "volatility": {
            "vix":        report.get("volatility_index", {}).get("vix", ""),
            "vkospi":     report.get("volatility_index", {}).get("vkospi", ""),
            "fear_greed": report.get("volatility_index", {}).get("fear_greed", ""),
            "level":      report.get("volatility_index", {}).get("level", ""),
        },
        "market_snapshot": {
            "sp500":    market_data.get("sp500", "N/A") if market_data else "N/A",
            "nasdaq":   market_data.get("nasdaq", "N/A") if market_data else "N/A",
            "vix":      market_data.get("vix", "N/A") if market_data else "N/A",
            "kospi":    market_data.get("kospi", "N/A") if market_data else "N/A",
            "krw_usd":  market_data.get("krw_usd", "N/A") if market_data else "N/A",
            "nvda":     market_data.get("nvda", "N/A") if market_data else "N/A",
        },
        "actionable_watch": report.get("actionable_watch", []),
    }

    BASELINE_FILE.write_text(
        json.dumps(baseline, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("Baseline saved: " + today + " " + baseline["saved_at"])
    return baseline


def load_baseline() -> dict:
    """오늘 아침 기준점 로드. 없으면 빈 dict"""
    if not BASELINE_FILE.exists():
        return {}
    try:
        data  = json.loads(BASELINE_FILE.read_text(encoding="utf-8"))
        today = datetime.now(KST).strftime("%Y-%m-%d")
        if data.get("date") != today:
            print("Baseline is from " + data.get("date", "?") + " — not today, skipping")
            return {}
        return data
    except:
        return {}


def build_baseline_context(mode: str) -> str:
    """Analyst/Reporter 프롬프트에 주입할 기준점 텍스트"""
    baseline = load_baseline()
    if not baseline:
        return ""

    if mode == "MORNING":
        return ""  # 아침은 기준점 없이 독립 분석

    snap     = baseline.get("market_snapshot", {})
    vol      = baseline.get("volatility", {})
    outflows = baseline.get("key_outflows", [])
    inflows  = baseline.get("key_inflows", [])
    tks      = baseline.get("thesis_killers", [])

    lines = [
        "\n\n## 오늘 아침 기준점 (07:30 MORNING 분석 결과)",
        "이 기준점과 비교해서 무엇이 변했는지 분석하세요.",
        "",
        "아침 레짐: " + baseline.get("market_regime", ""),
        "아침 추세: " + baseline.get("trend_phase", ""),
        "아침 신뢰도: " + baseline.get("confidence", ""),
        "아침 요약: " + baseline.get("one_line_summary", ""),
        "",
        "아침 시장 수치:",
        "- S&P500: " + snap.get("sp500", "N/A"),
        "- 나스닥:  " + snap.get("nasdaq", "N/A"),
        "- VIX:    " + snap.get("vix", "N/A") + " (" + vol.get("level", "") + ")",
        "- 코스피:  " + snap.get("kospi", "N/A"),
        "- 원달러:  " + snap.get("krw_usd", "N/A"),
        "- 엔비디아: " + snap.get("nvda", "N/A"),
        "",
        "아침 핵심 유출: " + ", ".join([o["zone"] for o in outflows]),
        "아침 핵심 유입: " + ", ".join([i["zone"] for i in inflows]),
    ]

    if tks:
        lines.append("")
        lines.append("아침 테제 킬러 (오늘 확인 필요):")
        for tk in tks[:3]:
            lines.append("- " + tk.get("event", "") + " [" + tk.get("timeframe", "") + "]")
            lines.append("  확인: " + tk.get("confirms_if", "")[:50])

    if mode == "AFTERNOON":
        lines += [
            "",
            "## 오후 분석 지시",
            "- 아침 대비 달라진 것만 집중 분석하세요",
            "- 레짐이 바뀌었다면 반드시 명확한 근거를 제시하세요",
            "- 아침과 같은 레짐이면 '아침 기조 유지'로 표현하세요",
            "- 아침 테제 킬러 중 확인된 것이 있으면 보고하세요",
        ]
    elif mode == "EVENING":
        lines += [
            "",
            "## 저녁 분석 지시",
            "- 오늘 하루를 총정리하세요",
            "- 아침 예측이 실제로 맞았는지 평가하세요",
            "- 내일 아침 준비 포인트를 명확히 제시하세요",
            "- 아침 테제 킬러가 확인됐는지 최종 평가하세요",
        ]
    elif mode == "DAWN":
        lines += [
            "",
            "## 새벽 분석 지시",
            "- 어제 아침 예측 대비 미국 마감 결과를 평가하세요",
            "- 오늘 아침 분석을 위한 글로벌 세팅을 제공하세요",
        ]

    return "\n".join(lines)


def get_regime_drift(current_regime: str) -> str:
    """아침 대비 레짐 변화 감지"""
    baseline = load_baseline()
    if not baseline:
        return ""
    morning_regime = baseline.get("market_regime", "")
    if not morning_regime or not current_regime:
        return ""
    if morning_regime == current_regime:
        return "STABLE"
    return "DRIFT: " + morning_regime + " → " + current_regime


if __name__ == "__main__":
    b = load_baseline()
    if b:
        print("Today's baseline:")
        print("  Regime: " + b.get("market_regime", ""))
        print("  Trend:  " + b.get("trend_phase", ""))
        print("  Saved:  " + b.get("saved_at", ""))
    else:
        print("No baseline for today")
