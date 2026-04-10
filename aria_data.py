import os
import sys
import json
import time
import httpx
from datetime import datetime, timezone, timedelta
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

KST       = timezone(timedelta(hours=9))
COST_FILE = Path("aria_cost.json")
DATA_FILE = Path("aria_market_data.json")


# ── Yahoo Finance 데이터 수집 ──────────────────────────────────────────────────
def fetch_yahoo_data() -> dict:
    result = {}
    tickers = {
        "^GSPC":  "sp500",
        "^IXIC":  "nasdaq",
        "^VIX":   "vix",
        "^KS11":  "kospi",
        "KRW=X":  "krw_usd",
        "^TNX":   "us_10y",
        "000660.KS": "sk_hynix",
        "005930.KS": "samsung",
        "NVDA":   "nvda",
        "AVGO":   "avgo",
        "SCHD":   "schd",
    }

    for ticker, key in tickers.items():
        try:
            url = "https://query1.finance.yahoo.com/v8/finance/chart/" + ticker
            r = httpx.get(
                url,
                params={"interval": "1d", "range": "1d"},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            data  = r.json()
            meta  = data.get("chart", {}).get("result", [{}])[0].get("meta", {})
            price = meta.get("regularMarketPrice", "")
            prev  = meta.get("chartPreviousClose", "")

            if price and prev and float(str(prev)) != 0:
                change = round((float(price) - float(prev)) / float(prev) * 100, 2)
                result[key]             = str(round(float(price), 2))
                result[key + "_change"] = ("+" if change >= 0 else "") + str(change) + "%"
                print("  " + ticker + ": " + result[key] + " (" + result[key + "_change"] + ")")
            else:
                result[key] = "N/A"
                print("  " + ticker + ": 데이터 없음")

            time.sleep(0.3)

        except Exception as e:
            print("  " + ticker + " 실패: " + str(e))
            result[key] = "N/A"

    return result


# ── 변동성 체크 (적응형 스케줄) ───────────────────────────────────────────────
def check_volatility_alert(data: dict) -> dict:
    try:
        vix = float(str(data.get("vix", "0")).replace(",", ""))
    except:
        vix = 0

    try:
        kospi_chg = data.get("kospi_change", "0%")
        kospi_pct = float(str(kospi_chg).replace("%", "").replace("+", ""))
    except:
        kospi_pct = 0

    try:
        sp500_chg = data.get("sp500_change", "0%")
        sp500_pct = float(str(sp500_chg).replace("%", "").replace("+", ""))
    except:
        sp500_pct = 0

    alerts = []
    level  = "normal"

    if vix >= 40:
        alerts.append("VIX " + str(vix) + " — 극단공포 (금융위기 수준)")
        level = "critical"
    elif vix >= 30:
        alerts.append("VIX " + str(vix) + " — 공포 구간")
        level = "elevated"
    elif vix >= 25:
        alerts.append("VIX " + str(vix) + " — 경계 수준")
        if level == "normal":
            level = "elevated"

    if abs(kospi_pct) >= 5:
        alerts.append("코스피 " + str(kospi_pct) + "% 급변")
        level = "critical"
    elif abs(kospi_pct) >= 3:
        alerts.append("코스피 " + str(kospi_pct) + "% 변동")
        if level == "normal":
            level = "elevated"

    if abs(sp500_pct) >= 4:
        alerts.append("S&P500 " + str(sp500_pct) + "% 급변")
        level = "critical"
    elif abs(sp500_pct) >= 2:
        alerts.append("S&P500 " + str(sp500_pct) + "% 변동")
        if level == "normal":
            level = "elevated"

    return {
        "level":          level,
        "alerts":         alerts,
        "should_run_now": level in ["elevated", "critical"],
        "vix":            vix,
        "kospi_change":   kospi_pct,
        "sp500_change":   sp500_pct,
    }


# ── 비용 추적 ──────────────────────────────────────────────────────────────────
def load_cost() -> dict:
    if COST_FILE.exists():
        return json.loads(COST_FILE.read_text(encoding="utf-8"))
    return {"total_runs": 0, "monthly_runs": {}, "estimated_cost_usd": 0.0, "last_run": ""}


def update_cost(mode: str = "MORNING"):
    cost      = load_cost()
    now       = datetime.now(KST)
    month_key = now.strftime("%Y-%m")

    cost["total_runs"] += 1
    cost["last_run"]    = now.strftime("%Y-%m-%d %H:%M KST")

    if month_key not in cost["monthly_runs"]:
        cost["monthly_runs"][month_key] = {"runs": 0, "estimated_usd": 0.0}

    cost["monthly_runs"][month_key]["runs"] += 1

    cost_per_run = {"MORNING": 1.2, "AFTERNOON": 0.7, "EVENING": 0.7, "DAWN": 0.9}
    run_cost = cost_per_run.get(mode, 0.8)
    cost["estimated_cost_usd"] = round(cost.get("estimated_cost_usd", 0) + run_cost, 2)
    cost["monthly_runs"][month_key]["estimated_usd"] = round(
        cost["monthly_runs"][month_key].get("estimated_usd", 0) + run_cost, 2
    )

    months = sorted(cost["monthly_runs"].keys())
    if len(months) > 3:
        for old in months[:-3]:
            del cost["monthly_runs"][old]

    COST_FILE.write_text(json.dumps(cost, ensure_ascii=False, indent=2), encoding="utf-8")
    return cost


def get_monthly_cost_summary() -> str:
    cost  = load_cost()
    month = datetime.now(KST).strftime("%Y-%m")
    m     = cost.get("monthly_runs", {}).get(month, {})
    runs  = m.get("runs", 0)
    usd   = m.get("estimated_usd", 0.0)
    krw   = round(usd * 1480)
    return "이번달 " + str(runs) + "회 실행 | 추정 비용 $" + str(usd) + " (약 " + f"{krw:,}" + "원)"


# ── 전체 수집 ──────────────────────────────────────────────────────────────────
def fetch_all_market_data() -> dict:
    print("\n[Yahoo Finance 실시간 데이터 수집]")
    now   = datetime.now(KST)
    yahoo = fetch_yahoo_data()

    # KIS 미연결 - Yahoo로 대체
    print("[KIS API] 미연결 - Yahoo Finance로 대체")

    data = {
        "fetched_at":     now.strftime("%Y-%m-%d %H:%M KST"),
        "sp500":          yahoo.get("sp500", "N/A"),
        "sp500_change":   yahoo.get("sp500_change", ""),
        "nasdaq":         yahoo.get("nasdaq", "N/A"),
        "nasdaq_change":  yahoo.get("nasdaq_change", ""),
        "vix":            yahoo.get("vix", "N/A"),
        "vix_change":     yahoo.get("vix_change", ""),
        "us_10y":         yahoo.get("us_10y", "N/A"),
        "kospi":          yahoo.get("kospi", "N/A"),
        "kospi_change":   yahoo.get("kospi_change", ""),
        "krw_usd":        yahoo.get("krw_usd", "N/A"),
        "sk_hynix":       yahoo.get("sk_hynix", "N/A"),
        "sk_hynix_change": yahoo.get("sk_hynix_change", ""),
        "samsung":        yahoo.get("samsung", "N/A"),
        "samsung_change": yahoo.get("samsung_change", ""),
        "nvda":           yahoo.get("nvda", "N/A"),
        "nvda_change":    yahoo.get("nvda_change", ""),
        "avgo":           yahoo.get("avgo", "N/A"),
        "avgo_change":    yahoo.get("avgo_change", ""),
        "schd":           yahoo.get("schd", "N/A"),
        "schd_change":    yahoo.get("schd_change", ""),
        "source":         "Yahoo Finance",
    }

    data["volatility_alert"] = check_volatility_alert(data)
    DATA_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print("저장 완료: " + DATA_FILE.name)
    return data


def load_market_data() -> dict:
    if DATA_FILE.exists():
        return json.loads(DATA_FILE.read_text(encoding="utf-8"))
    return {}


def format_for_hunter(data: dict) -> str:
    if not data:
        return ""

    alert     = data.get("volatility_alert", {})
    alert_str = ""
    if alert.get("alerts"):
        alert_str = "\n경보: " + " | ".join(alert["alerts"])

    krw = data.get("krw_usd", "N/A")
    if krw != "N/A":
        try:
            krw = str(round(float(krw))) + " KRW/USD"
        except:
            pass

    return (
        "\n\n## 실시간 시장 데이터 (API 직접 수집 — 이 수치를 그대로 사용하세요. 추정 금지)\n"
        "수집: " + data.get("fetched_at", "") + "\n\n"
        "### 미국\n"
        "- S&P 500: " + data.get("sp500", "N/A") + " (" + data.get("sp500_change", "") + ")\n"
        "- 나스닥:  " + data.get("nasdaq", "N/A") + " (" + data.get("nasdaq_change", "") + ")\n"
        "- VIX:     " + data.get("vix", "N/A") + " (" + data.get("vix_change", "") + ")\n"
        "- 미국10Y: " + data.get("us_10y", "N/A") + "%\n\n"
        "### 한국\n"
        "- 코스피:     " + data.get("kospi", "N/A") + " (" + data.get("kospi_change", "") + ")\n"
        "- 원/달러:    " + krw + "\n"
        "- SK하이닉스: " + data.get("sk_hynix", "N/A") + " (" + data.get("sk_hynix_change", "") + ")\n"
        "- 삼성전자:   " + data.get("samsung", "N/A") + " (" + data.get("samsung_change", "") + ")\n\n"
        "### 포트폴리오\n"
        "- 엔비디아: " + data.get("nvda", "N/A") + " (" + data.get("nvda_change", "") + ")\n"
        "- 브로드컴: " + data.get("avgo", "N/A") + " (" + data.get("avgo_change", "") + ")\n"
        "- SCHD:     " + data.get("schd", "N/A") + " (" + data.get("schd_change", "") + ")"
        + alert_str
    )


if __name__ == "__main__":
    data = fetch_all_market_data()
    print("\n--- 수집 결과 ---")
    print("VIX:      " + data.get("vix", "N/A"))
    print("코스피:   " + data.get("kospi", "N/A"))
    print("원달러:   " + data.get("krw_usd", "N/A"))
    print("엔비디아: " + data.get("nvda", "N/A"))
    print("변동성:   " + data.get("volatility_alert", {}).get("level", "normal"))
    print("\n" + get_monthly_cost_summary())
