import os, sys, json, re, time, httpx
from datetime import datetime, timezone, timedelta
from pathlib import Path
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
KST=timezone(timedelta(hours=9)); COST_FILE=Path("aria_cost.json"); DATA_FILE=Path("aria_market_data.json")
_CORE={"sp500","nasdaq","vix","kospi"}

def _fetch_one(ticker,retries=2):
    for i in range(retries+1):
        try:
            r=httpx.get("https://query1.finance.yahoo.com/v8/finance/chart/"+ticker,params={"interval":"1d","range":"1d"},headers={"User-Agent":"Mozilla/5.0"},timeout=10)
            meta=r.json().get("chart",{}).get("result",[{}])[0].get("meta",{})
            price=meta.get("regularMarketPrice",""); prev=meta.get("chartPreviousClose","")
            if price and prev and float(str(prev))!=0:
                chg=round((float(price)-float(prev))/float(prev)*100,2)
                return str(round(float(price),2)),("+" if chg>=0 else "")+str(chg)+"%"
        except Exception as e:
            if i<retries: time.sleep(1.0*(i+1))
            else: print("  "+ticker+" 실패("+str(retries+1)+"회): "+str(e))
    return "N/A",""

def fetch_yahoo_data():
    result={}
    tickers={"^GSPC":"sp500","^IXIC":"nasdaq","^VIX":"vix","^KS11":"kospi","KRW=X":"krw_usd","^TNX":"us_10y",
             "000660.KS":"sk_hynix","005930.KS":"samsung","035720.KS":"kakao","466920.KS":"kodex",
             "NVDA":"nvda","AVGO":"avgo","SCHD":"schd"}
    for ticker,key in tickers.items():
        val,chg=_fetch_one(ticker); result[key]=val; result[key+"_change"]=chg
        print("  "+ticker+": "+(val+" ("+chg+")" if val!="N/A" else "데이터 없음")); time.sleep(0.3)
    core_na=sum(1 for k in _CORE if result.get(k)=="N/A")
    result["data_quality"]="poor" if core_na>=2 else "ok"
    if core_na>=2: print("⚠️ 핵심 티커 "+str(core_na)+"개 N/A")
    return result

def fetch_fear_greed(yahoo_data: dict = None) -> dict:
    """Fear&Greed 지수 수집 — 소스 우선순위:
    1. FearGreedChart.com (무료·무인증·주식시장 기반)
    2. CNN 직접 API (GitHub Actions에서 대부분 차단)
    3. VIX+모멘텀 자체계산 (항상 작동하는 폴백)
    """
    result = {"value": "N/A", "rating": "N/A", "prev_close": "N/A"}

    # 1. FearGreedChart.com — 무료·무인증·주식시장 기반
    try:
        r = httpx.get(
            "https://feargreedchart.com/api/?action=all",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        d = r.json()
        score = d.get("score", {}).get("score")
        if score is not None:
            score = int(score)
            if score <= 20:    rate = "Extreme Fear"
            elif score <= 40:  rate = "Fear"
            elif score <= 60:  rate = "Neutral"
            elif score <= 80:  rate = "Greed"
            else:              rate = "Extreme Greed"
            # 전일 값: history 마지막 2개에서 계산
            prev = "N/A"
            hist = d.get("recent", [])
            if len(hist) >= 2:
                prev = str(hist[-2].get("score", "N/A"))
            result.update({"value": str(score), "rating": rate,
                           "prev_close": prev,
                           "source": "feargreedchart", "confidence": "높음"})
            print("  Fear&Greed (FearGreedChart.com): " + str(score) + " (" + rate + ")")
            return result
    except Exception as e:
        print("  FearGreedChart 실패: " + str(e)[:60])

    # 2. CNN 직접 API (GitHub Actions에서 대부분 차단됨)
    try:
        r = httpx.get(
            "https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.cnn.com/markets/fear-and-greed"},
            timeout=15
        )
        d = r.json(); fg = d.get("fear_and_greed", {})
        score = fg.get("score", ""); rate = fg.get("rating", ""); prev = fg.get("previous_close", "")
        if score:
            result.update({"value": str(round(float(score), 1)), "rating": rate,
                           "prev_close": str(round(float(prev), 1)) if prev else "N/A",
                           "source": "cnn", "confidence": "높음"})
            print("  Fear&Greed (CNN): " + result["value"] + " (" + rate + ")")
            return result
    except Exception as e:
        print("  CNN 실패: " + str(e)[:60])

    # 2. VIX + 시장 모멘텀 기반 자체 계산 (주식시장 기반 → alternative.me 암호화폐 지수보다 정확)
    try:
        d = yahoo_data or {}
        def _f(k):
            try: return float(str(d.get(k, "0") or "0").replace("%","").replace("+",""))
            except: return 0.0

        vix     = _f("vix")
        sp_chg  = _f("sp500_change")
        nq_chg  = _f("nasdaq_change")
        ks_chg  = _f("kospi_change")

        # VIX 기반 기본 점수 (역방향)
        if vix >= 35:    score = 10
        elif vix >= 30:  score = 20
        elif vix >= 25:  score = 30
        elif vix >= 22:  score = 38
        elif vix >= 18:  score = 48
        elif vix >= 15:  score = 58
        elif vix >= 12:  score = 68
        else:            score = 78

        # 시장 모멘텀 보정 (-15 ~ +15)
        momentum = (sp_chg * 2 + nq_chg * 1.5 + ks_chg * 0.5) / 3
        score = max(0, min(100, round(score + max(-15, min(15, momentum * 3)))))

        if score <= 20:    rate = "Extreme Fear"
        elif score <= 40:  rate = "Fear"
        elif score <= 60:  rate = "Neutral"
        elif score <= 80:  rate = "Greed"
        else:              rate = "Extreme Greed"

        result.update({"value": str(score), "rating": rate, "prev_close": "N/A",
                          "source": "vix_proxy", "confidence": "보통",
                          "note": "VIX+모멘텀 자체계산 (CNN 미연결)"})
        print("  Fear&Greed (VIX+모멘텀 대체치, CNN 미연결): " + str(score) + " (" + rate + ") | VIX=" + str(vix))
        return result

    except Exception as e:
        print("  Fear&Greed 계산 실패: " + str(e))
        return result

def fetch_korea_news():
    results=[]
    for q in ["오늘 기관 외국인 순매수 코스피","오늘 한국 주요 공시 대형주","오늘 외국인 매매 동향 반도체"]:
        try:
            r=httpx.get("https://search.naver.com/search.naver",params={"where":"news","query":q,"sort":"1"},headers={"User-Agent":"Mozilla/5.0"},timeout=8)
            for t in re.findall(r'class="news_tit"[^>]*title="([^"]+)"',r.text)[:2]: results.append({"query":q,"headline":t})
        except Exception as e: print("  한국 뉴스 실패: "+str(e))
    return results[:6]

def check_volatility_alert(data):
    def f(k):
        try: return float(str(data.get(k,"0") or "0").replace("%","").replace("+","").replace(",",""))
        except: return 0.0
    vix,kp,sp=f("vix"),f("kospi_change"),f("sp500_change"); alerts,level=[],  "normal"
    if vix>=40: alerts.append("VIX "+str(vix)+" 극단공포"); level="critical"
    elif vix>=30: alerts.append("VIX "+str(vix)+" 공포"); level="elevated"
    elif vix>=25: alerts.append("VIX "+str(vix)+" 경계"); level="elevated" if level=="normal" else level
    if abs(kp)>=5: alerts.append("코스피 "+str(kp)+"% 급변"); level="critical"
    elif abs(kp)>=3: level="elevated" if level=="normal" else level; alerts.append("코스피 "+str(kp)+"% 변동")
    if abs(sp)>=4: alerts.append("S&P500 "+str(sp)+"% 급변"); level="critical"
    elif abs(sp)>=2: level="elevated" if level=="normal" else level; alerts.append("S&P500 "+str(sp)+"% 변동")
    return {"level":level,"alerts":alerts,"should_run_now":level in["elevated","critical"],"vix":vix,"kospi_change":kp,"sp500_change":sp}

def load_cost():
    if COST_FILE.exists(): return json.loads(COST_FILE.read_text(encoding="utf-8"))
    return {"total_runs":0,"monthly_runs":{},"estimated_cost_usd":0.0,"last_run":""}

def update_cost(mode="MORNING"):
    cost=load_cost(); now=datetime.now(KST); mk=now.strftime("%Y-%m")
    cost["total_runs"]+=1; cost["last_run"]=now.strftime("%Y-%m-%d %H:%M KST")
    cost.setdefault("monthly_runs",{}).setdefault(mk,{"runs":0,"estimated_usd":0.0})
    cost["monthly_runs"][mk]["runs"]+=1
    rc={"MORNING":1.2,"AFTERNOON":0.5,"EVENING":0.5,"DAWN":0.7}.get(mode,0.8)
    cost["estimated_cost_usd"]=round(cost.get("estimated_cost_usd",0)+rc,2)
    cost["monthly_runs"][mk]["estimated_usd"]=round(cost["monthly_runs"][mk].get("estimated_usd",0)+rc,2)
    months=sorted(cost["monthly_runs"].keys())
    for old in months[:-3]: del cost["monthly_runs"][old]
    COST_FILE.write_text(json.dumps(cost,ensure_ascii=False,indent=2),encoding="utf-8"); return cost

def get_monthly_cost_summary():
    cost=load_cost(); month=datetime.now(KST).strftime("%Y-%m"); m=cost.get("monthly_runs",{}).get(month,{})
    usd=m.get("estimated_usd",0.0)
    return "이번달 "+str(m.get("runs",0))+"회 실행 | 추정 $"+str(usd)+" (약 "+f"{round(usd*1480):,}"+"원)"

def _get_market_status(now):
    """주말/휴장 여부 반환 — 데이터 freshness 표시용"""
    wd = now.weekday()
    if wd == 6: return "closed", "직전 종가 기준 (2거래일 전, 일요일)"
    if wd == 5: return "closed", "직전 종가 기준 (1거래일 전, 토요일)"
    h = now.hour
    if h < 9 or h >= 16: return "after_hours", "전일 종가 기준"
    return "open", "실시간"


def fetch_all_market_data():
    now=datetime.now(KST)
    market_status, data_label = _get_market_status(now)
    print("[Yahoo Finance] (" + data_label + ")"); yahoo=fetch_yahoo_data()
    print("[Fear & Greed]"); fg=fetch_fear_greed(yahoo)  # yahoo 데이터 전달 → VIX 기반 계산
    print("[한국 특수 뉴스]"); kr_n=fetch_korea_news()
    print("[KIS API] 미연결")
    keys=["sp500","sp500_change","nasdaq","nasdaq_change","vix","vix_change","us_10y","kospi","kospi_change","krw_usd","sk_hynix","sk_hynix_change","samsung","samsung_change","kakao","kakao_change","kodex","kodex_change","nvda","nvda_change","avgo","avgo_change","schd","schd_change"]
    data={"fetched_at":now.strftime("%Y-%m-%d %H:%M KST"),"market_status":market_status,"data_label":data_label,"data_quality":yahoo.get("data_quality","ok"),
          **{k:yahoo.get(k,"N/A") for k in keys},
          "fear_greed_value":fg.get("value","N/A"),"fear_greed_rating":fg.get("rating","N/A"),"fear_greed_prev":fg.get("prev_close","N/A"),
          "fear_greed_source":fg.get("source","unknown"),"fear_greed_confidence":fg.get("confidence","낮음"),
          "korea_special_news":kr_n,"source":"Yahoo Finance + CNN Fear&Greed"}
    data["volatility_alert"]=check_volatility_alert(data)
    if data["data_quality"]=="poor":
        try:
            from aria_notify import send_message
            send_message("⚠️ <b>ARIA 데이터 경고</b>\n핵심 데이터 수집 불량 — 분석 신뢰도 낮음")
        except: pass
    DATA_FILE.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding="utf-8")
    print("저장 완료: "+DATA_FILE.name); return data

def load_market_data():
    if DATA_FILE.exists(): return json.loads(DATA_FILE.read_text(encoding="utf-8"))
    return {}

def format_for_hunter(data):
    if not data: return ""
    alerts=data.get("volatility_alert",{}).get("alerts",[])
    alert_str=("\n⚠️ 경보: "+" | ".join(alerts)) if alerts else ""
    qual_str="\n⚠️ 데이터 품질 불량" if data.get("data_quality")=="poor" else ""

    # 데이터 신뢰도 경고
    market_status = data.get("market_status", "open")
    data_label    = data.get("data_label", "실시간")
    fg_source_val = data.get("fear_greed_source", "unknown")
    fg_conf_val   = data.get("fear_greed_confidence", "낮음")
    fg_note       = data.get("fear_greed_note", "")

    status_str = ""
    if market_status == "closed":
        status_str = "\n⚠️ 주의: 현재 시장 휴장 중 — " + data_label + " (최신 데이터 아님)"
    elif market_status == "after_hours":
        status_str = "\n📌 " + data_label

    kis_str = "\n⚠️ KIS 미연결: 한국 외국인 수급·VKOSPI 실데이터 없음. 한국 수급 관련 단정 표현 금지."

    fg_str2 = ""
    if fg_source_val == "vix_proxy":
        fg_str2 = f"\n📌 Fear&Greed: VIX+모멘텀 자체계산 (CNN 차단, 공식 지수 아님 · 신뢰도:{fg_conf_val})"
    try: krw = str(round(float(str(data.get("krw_usd","0")).replace(",","")))) + " KRW/USD"
    except: krw = str(data.get("krw_usd","N/A"))
    fg=data.get("fear_greed_value","N/A")
    fg_str=(fg+" / "+data.get("fear_greed_rating","")+" (전일: "+data.get("fear_greed_prev","N/A")+")") if fg!="N/A" else "N/A"
    kr_n=data.get("korea_special_news",[])
    kr_str=("\n\n### 한국 특수 뉴스\n"+"".join("- "+n.get("headline","")+"\n" for n in kr_n)) if kr_n else ""
    def v(k): return data.get(k,"N/A")
    def vc(k): return data.get(k+"_change","")
    return (
        "\n\n## 시장 데이터 ("+data_label+")\n수집: "+v("fetched_at")+qual_str+status_str+kis_str+fg_str2+"\n\n"
        "### 미국\n- S&P500: "+v("sp500")+" ("+vc("sp500")+")\n- 나스닥: "+v("nasdaq")+" ("+vc("nasdaq")+")\n"
        "- VIX: "+v("vix")+" ("+vc("vix")+")\n- 미국10Y: "+v("us_10y")+"%\n- Fear&Greed: "+fg_str+"\n\n"
        "### 한국\n- 코스피: "+v("kospi")+" ("+vc("kospi")+")\n- 원/달러: "+krw+"\n"
        "- SK하이닉스: "+v("sk_hynix")+" ("+vc("sk_hynix")+")\n- 삼성전자: "+v("samsung")+" ("+vc("samsung")+")\n"
        "- 카카오: "+v("kakao")+" ("+vc("kakao")+")\n- SOL고배당: "+v("kodex")+" ("+vc("kodex")+")\n\n"
        "### 포트폴리오\n- 엔비디아: "+v("nvda")+" ("+vc("nvda")+")\n- 브로드컴: "+v("avgo")+" ("+vc("avgo")+")\n"
        "- SCHD: "+v("schd")+" ("+vc("schd")+")"+alert_str+kr_str
    )

if __name__=="__main__":
    d=fetch_all_market_data()
    for k in ["vix","kospi","krw_usd","kakao","kodex","nvda","fear_greed_value","data_quality"]: print(k+": "+str(d.get(k,"N/A")))
    print(get_monthly_cost_summary())
