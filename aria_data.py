import os, sys, json, re, time, httpx
from datetime import datetime, timezone, timedelta
from pathlib import Path
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
KST=timezone(timedelta(hours=9))
from aria_paths import COST_FILE, DATA_FILE
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

def fetch_krx_flow() -> dict:
    """KRX OpenAPI — 실제 제공 데이터로 한국 시장 보조 지표 수집
    ※ 투자자별 매매동향은 KRX OpenAPI 미제공 (유료 별도 상품)
    대신: KOSPI 지수 시세 (Yahoo Finance 교차검증용)

    엔드포인트: data-dbg.krx.co.kr/svc/apis/idx/kospi_dd_trd
    AUTH_KEY 헤더, basDd=YYYYMMDD 파라미터, GET 방식
    """
    result = {
        "foreign_net": "N/A", "institution_net": "N/A", "individual_net": "N/A",
        "foreign_buy": "N/A", "foreign_sell": "N/A",
        "source": "none", "date": "N/A",
        "krx_kospi_close": "N/A", "krx_kospi_change": "N/A",
    }
    api_key = os.environ.get("KRX_API_KEY", "")
    if not api_key:
        print("  KRX API 키 없음 (KRX_API_KEY 미설정)")
        return result

    now = datetime.now(KST)
    # 직전 거래일 계산
    d = now if now.hour >= 18 else now - timedelta(days=1)
    for _ in range(7):
        if d.weekday() < 5:
            break
        d = d - timedelta(days=1)
    date_str = d.strftime("%Y%m%d")

    headers = {
        "AUTH_KEY": api_key.strip(),
        "Accept": "application/json",
    }

    # KOSPI 일별 시세 (실제 제공 엔드포인트)
    try:
        r = httpx.get(
            "https://data-dbg.krx.co.kr/svc/apis/idx/kospi_dd_trd",
            headers=headers,
            params={"basDd": date_str},
            timeout=12,
            follow_redirects=True,
        )
        if r.status_code == 200:
            rows = r.json().get("OutBlock_1", [])
            if rows:
                # 종합(KOSPI) 행 찾기
                for row in rows:
                    nm = str(row.get("IDX_NM", "") or row.get("idxNm",""))
                    if "종합" in nm or "KOSPI" in nm.upper():
                        close = str(row.get("CLSPRC","") or row.get("clsPrc",""))
                        fluc  = str(row.get("FLUC_RT","") or row.get("flucRt",""))
                        if close:
                            result["krx_kospi_close"]  = close
                            result["krx_kospi_change"] = fluc
                            result["source"]           = "krx_api"
                            result["date"]             = date_str
                            print("  KRX KOSPI(" + date_str + "): " + close + " (" + fluc + "%)")
                            break
        else:
            print("  KRX API → " + str(r.status_code))
    except Exception as e:
        print("  KRX API 실패: " + str(e)[:60])

    # 투자자별 수급은 미제공 — Hunter 웹서치로 보완
    if result["source"] == "none":
        print("  KRX 투자자 수급: OpenAPI 미제공 (유료 별도 상품)")

    return result


def fetch_yahoo_data():
    result={}
    tickers={
        "^GSPC":"sp500","^IXIC":"nasdaq","^VIX":"vix","^KS11":"kospi",
        "KRW=X":"krw_usd","^TNX":"us_10y",
        "000660.KS":"sk_hynix","005930.KS":"samsung","035720.KS":"kakao",
        "466920.KS":"kodex",
        "NVDA":"nvda","AVGO":"avgo","SCHD":"schd",
        # 외국인 수급 프록시
        "EWY":"ewy",        # iShares MSCI Korea ETF — 외국인 한국 투자 수요 지표
        "122630.KS":"kodex_lev",  # KODEX 레버리지 — 국내 수급 활동성
    }
    for ticker,key in tickers.items():
        val,chg=_fetch_one(ticker); result[key]=val; result[key+"_change"]=chg
        if val != "N/A":
            print("  "+ticker+": "+val+" ("+chg+")")
        time.sleep(0.3)
    core_na=sum(1 for k in _CORE if result.get(k)=="N/A")
    result["data_quality"]="poor" if core_na>=2 else "ok"
    if core_na>=2: print("⚠️ 핵심 티커 "+str(core_na)+"개 N/A")
    return result

def fetch_put_call_ratio() -> dict:
    """SPY·QQQ 풋/콜 비율 (PCR) — CNN F&G 구성요소 직접 계산
    PCR > 1.0 → 공포(헤지 수요 급증), < 0.7 → 탐욕(콜 과열)
    yfinance 이미 설치됨 → 추가 패키지 불필요
    """
    result = {"pcr_spy": None, "pcr_qqq": None, "pcr_avg": None, "pcr_signal": "N/A"}
    try:
        import yfinance as yf
        pcrs = []
        for ticker in ["SPY", "QQQ"]:
            try:
                tk = yf.Ticker(ticker)
                exps = tk.options[:2]   # 가까운 만기 2개만 (속도 최적화)
                total_put = total_call = 0
                for exp in exps:
                    chain = tk.option_chain(exp)
                    total_put  += chain.puts["volume"].fillna(0).sum()
                    total_call += chain.calls["volume"].fillna(0).sum()
                if total_call > 0:
                    pcr = round(total_put / total_call, 3)
                    pcrs.append(pcr)
                    result["pcr_" + ticker.lower()] = pcr
                    time.sleep(1.0)   # rate limit 방지용 대기 늘림
                else:
                    print("  PCR " + ticker + ": 옵션 거래량 0 (장 마감 후 or 데이터 없음)")
            except Exception as e:
                err = str(e)
                if "Rate" in err or "429" in err or "Too Many" in err:
                    print("  PCR " + ticker + ": Rate limit — 다음 실행에서 재시도")
                else:
                    print("  PCR " + ticker + " 실패: " + err[:70])

        if pcrs:
            avg = round(sum(pcrs) / len(pcrs), 3)
            result["pcr_avg"] = avg
            if avg >= 1.2:    result["pcr_signal"] = "극단공포"
            elif avg >= 0.9:  result["pcr_signal"] = "공포"
            elif avg >= 0.7:  result["pcr_signal"] = "중립"
            elif avg >= 0.5:  result["pcr_signal"] = "탐욕"
            else:             result["pcr_signal"] = "극단탐욕"
            print("  PCR: SPY=" + str(result.get("pcr_spy","N/A"))
                  + " QQQ=" + str(result.get("pcr_qqq","N/A"))
                  + " 평균=" + str(avg) + " → " + result["pcr_signal"])
    except ImportError:
        print("  PCR: yfinance 미설치")
    except Exception as e:
        print("  PCR 실패: " + str(e)[:60])
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
            timeout=3,  # 3초 초과 시 즉시 VIX fallback
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


def fetch_fred_indicators() -> dict:
    """FRED API — 4개 매크로 지표 수집
    - VIXCLS:       VIX 공식 일별 (Yahoo보다 정확한 종가)
    - BAMLH0A0HYM2: 하이일드 스프레드 (CNN F&G 구성요소)
    - T10Y2Y:       장단기 금리차 (경기침체 선행지표)
    - UMCSENT:      미시간 소비자심리지수
    """
    result = {
        "vix_fred":      None,
        "hy_spread":     None,   # 하이일드 스프레드 (높을수록 공포)
        "yield_curve":   None,   # 장단기 금리차 (음수=침체 신호)
        "consumer_sent": None,   # 미시간 소비자심리 (높을수록 낙관)
        "rrp":           None,   # 역레포 잔고 (낮아지면 유동성 공급 → 상승 선행)
        "dxy":           None,   # 달러인덱스 (높을수록 신흥국 부담)
        "fred_source":   False,
    }
    api_key = os.environ.get("FRED_API_KEY", "")
    if not api_key:
        print("  FRED API 키 없음 (FRED_API_KEY 미설정)")
        return result

    SERIES = {
        "VIXCLS":          "vix_fred",
        "BAMLH0A0HYM2":    "hy_spread",
        "T10Y2Y":          "yield_curve",
        "UMCSENT":         "consumer_sent",
        "RRPONTSYD":       "rrp",       # 역레포 잔고 (조달러)
        "DTWEXBGS":        "dxy",       # 달러인덱스
    }
    base = "https://api.stlouisfed.org/fred/series/observations"
    success = 0
    for series_id, key in SERIES.items():
        for attempt in range(2):   # 500 오류 시 1회 재시도
            try:
                r = httpx.get(base, params={
                    "series_id":  series_id,
                    "api_key":    api_key,
                    "sort_order": "desc",
                    "limit":      5,
                    "file_type":  "json",
                }, timeout=8)
                if r.status_code == 500 and attempt == 0:
                    time.sleep(1.5)   # 500이면 잠깐 대기 후 재시도
                    continue
                if r.status_code != 200:
                    print("  FRED " + series_id + " → " + str(r.status_code))
                    break
                obs = [o for o in r.json().get("observations", []) if o.get("value","") not in (".", "")]
                if obs:
                    val = round(float(obs[0]["value"]), 2)
                    result[key] = val
                    print("  FRED " + series_id + ": " + str(val))
                    success += 1
                break
            except Exception as e:
                print("  FRED " + series_id + " 실패: " + str(e)[:50])
                break

    result["fred_source"] = success >= 2
    return result


def fetch_fsc_data() -> dict:
    """금융위원회 공공데이터 API — 테스트 확인된 오퍼레이션만 사용
    - getStockPriceInfo:  삼성전자·SK하이닉스 종가 (Yahoo 백업)
    - getGoldPriceInfo:   금시세 (안전자산 흐름)
    - getOilPriceInfo:    국내 유류가 (경유·휘발유, 에너지 비용 지표)

    ※ KRX 투자자 수급 (외국인/기관) — stub, 나중에 연결
    """
    result = {
        "samsung_fsc": None, "sk_hynix_fsc": None,
        "gold_price": None, "oil_price_diesel": None, "oil_price_gasoline": None,
        "fsc_source": False,
    }
    api_key = os.environ.get("FSCAPI_KEY", "")
    if not api_key:
        print("  FSC API 키 없음 (FSCAPI_KEY 미설정)")
        return result

    BASE = "https://apis.data.go.kr/1160100/service"
    now  = datetime.now(KST)
    # 전일 거래일 계산
    d = now - timedelta(days=1)
    for _ in range(7):
        if d.weekday() < 5:
            break
        d -= timedelta(days=1)
    date_str = d.strftime("%Y%m%d")

    def _get(endpoint, params):
        try:
            r = httpx.get(BASE + endpoint, params={
                "serviceKey": api_key.strip(), "numOfRows": "5",
                "pageNo": "1", "resultType": "json", **params,
            }, timeout=8)
            if r.status_code != 200:
                return []
            items = r.json().get("response",{}).get("body",{}).get("items",{})
            item  = items.get("item",[]) if isinstance(items, dict) else []
            return item if isinstance(item, list) else [item]
        except Exception as e:
            print("  FSC 실패: " + str(e)[:50])
            return []

    success = 0

    # ── 1. 삼성전자·SK하이닉스 종가 (Yahoo 백업용) ─────────────────────
    for code, key in [("005930", "samsung_fsc"), ("000660", "sk_hynix_fsc")]:
        rows = _get("/GetStockSecuritiesInfoService/getStockPriceInfo",
                    {"likeSrtnCd": code, "basDd": date_str})
        if rows:
            clpr = rows[0].get("clpr","")
            if clpr:
                result[key] = str(clpr)
                success += 1

    # ── 2. 금시세 ───────────────────────────────────────────────────────
    rows = _get("/GetGeneralProductInfoService/getGoldPriceInfo", {"basDd": date_str})
    if rows:
        # 금 99.99 1kg 행 찾기
        for row in rows:
            if "99.99" in str(row.get("itmsNm","")) and "1kg" in str(row.get("itmsNm","")):
                result["gold_price"] = str(row.get("clpr",""))
                success += 1
                break
        if not result["gold_price"] and rows:
            result["gold_price"] = str(rows[0].get("clpr",""))
            success += 1

    # ── 3. 국내 유류가 ──────────────────────────────────────────────────
    rows = _get("/GetGeneralProductInfoService/getOilPriceInfo", {"basDd": date_str})
    for row in rows:
        ctg = str(row.get("oilCtg",""))
        prc = str(row.get("wtAvgPrcCptn","") or row.get("clpr",""))
        if "경유" in ctg and prc and prc != "0":
            result["oil_price_diesel"] = prc
        elif "휘발유" in ctg and prc and prc != "0":
            result["oil_price_gasoline"] = prc

    # ── 4. KRX 투자자 수급 — stub (나중에 연결) ─────────────────────────
    # TODO: 외국인/기관/개인 순매수 연결 예정
    # result["krx_foreign_net_fsc"]   = None
    # result["krx_institution_net_fsc"] = None

    result["fsc_source"] = success >= 2
    if result["fsc_source"]:
        parts = []
        if result["samsung_fsc"]:    parts.append("삼성전자 " + result["samsung_fsc"] + "원")
        if result["sk_hynix_fsc"]:   parts.append("SK하이닉스 " + result["sk_hynix_fsc"] + "원")
        if result["gold_price"]:     parts.append("금 " + result["gold_price"] + "원/kg")
        if result["oil_price_diesel"]: parts.append("경유 " + result["oil_price_diesel"] + "원/L")
        print("  FSC: " + " | ".join(parts))
    else:
        print("  FSC 데이터 부족 (success=" + str(success) + ")")

    return result


def fetch_all_market_data():
    now=datetime.now(KST)
    market_status, data_label = _get_market_status(now)
    print("[Yahoo Finance] (" + data_label + ")"); yahoo=fetch_yahoo_data()
    print("[Fear & Greed]"); fg=fetch_fear_greed(yahoo)
    print("[풋/콜 비율]"); pcr=fetch_put_call_ratio()
    print("[KRX 투자자 수급]"); krx_flow=fetch_krx_flow()
    print("[FRED 매크로지표]"); fred=fetch_fred_indicators()
    print("[FSC 금융위 API]"); fsc=fetch_fsc_data()
    print("[한국 특수 뉴스]"); kr_n=fetch_korea_news()
    keys=["sp500","sp500_change","nasdaq","nasdaq_change","vix","vix_change","us_10y","kospi","kospi_change","krw_usd","sk_hynix","sk_hynix_change","samsung","samsung_change","kakao","kakao_change","kodex","kodex_change","nvda","nvda_change","avgo","avgo_change","schd","schd_change"]
    data={"fetched_at":now.strftime("%Y-%m-%d %H:%M KST"),"market_status":market_status,"data_label":data_label,"data_quality":yahoo.get("data_quality","ok"),
          **{k:yahoo.get(k,"N/A") for k in keys},
          "fear_greed_value":fg.get("value","N/A"),"fear_greed_rating":fg.get("rating","N/A"),"fear_greed_prev":fg.get("prev_close","N/A"),
          "fear_greed_source":fg.get("source","unknown"),"fear_greed_confidence":fg.get("confidence","낮음"),
          "krx_foreign_net":krx_flow.get("foreign_net","N/A"),
          "krx_institution_net":krx_flow.get("institution_net","N/A"),
          "krx_individual_net":krx_flow.get("individual_net","N/A"),
          "krx_foreign_buy":krx_flow.get("foreign_buy","N/A"),
          "krx_foreign_sell":krx_flow.get("foreign_sell","N/A"),
          "krx_flow_source":krx_flow.get("source","none"),
          "krx_flow_date":krx_flow.get("date","N/A"),
          "fred_vix":         fred.get("vix_fred"),
          "fred_hy_spread":   fred.get("hy_spread"),
          "fred_yield_curve": fred.get("yield_curve"),
          "fred_consumer":    fred.get("consumer_sent"),
          "fred_rrp":         fred.get("rrp"),
          "fred_dxy":         fred.get("dxy"),
          "fred_source":      fred.get("fred_source", False),
          "pcr_spy":          pcr.get("pcr_spy"),
          "pcr_qqq":          pcr.get("pcr_qqq"),
          "pcr_avg":          pcr.get("pcr_avg"),
          "pcr_signal":       pcr.get("pcr_signal","N/A"),
          "fsc_samsung":      fsc.get("samsung_fsc"),
          "fsc_sk_hynix":     fsc.get("sk_hynix_fsc"),
          "fsc_gold":         fsc.get("gold_price"),
          "fsc_oil_diesel":   fsc.get("oil_price_diesel"),
          "fsc_oil_gasoline": fsc.get("oil_price_gasoline"),
          "fsc_source":       fsc.get("fsc_source", False),
          "korea_special_news":kr_n,"source":"Yahoo Finance + FearGreedChart + KRX + FRED + FSC"}
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
    alerts    = data.get("volatility_alert",{}).get("alerts",[])
    alert_str = ("\n⚠️ 경보: " + " | ".join(alerts)) if alerts else ""
    qual_str  = "\n⚠️ 데이터 품질 불량" if data.get("data_quality") == "poor" else ""

    market_status = data.get("market_status", "open")
    data_label    = data.get("data_label", "실시간")
    fg_source_val = data.get("fear_greed_source", "unknown")
    fg_conf_val   = data.get("fear_greed_confidence", "낮음")

    status_str = ""
    if market_status == "closed":
        status_str = "\n⚠️ 주의: 현재 시장 휴장 중 — " + data_label
    elif market_status == "after_hours":
        status_str = "\n📌 " + data_label

    # 외국인 수급 — KRX 실데이터 우선, 없으면 EWY 프록시
    krx_src = data.get("krx_flow_source", "none")
    if krx_src == "krx_api":
        krx_date = data.get("krx_flow_date", "")
        flow_str = (
            "\n📊 외국인수급(KRX실데이터 " + krx_date + "):"
            " 외국인 " + data.get("krx_foreign_net","N/A") +
            " | 기관 " + data.get("krx_institution_net","N/A") +
            " | 개인 " + data.get("krx_individual_net","N/A")
        )
    else:
        ewy     = data.get("ewy", "N/A")
        ewy_chg = data.get("ewy_change", "")
        if ewy != "N/A":
            flow_str = ("\n📌 외국인수급프록시: EWY(한국ETF) " + ewy + " (" + ewy_chg + ")"
                        " — 직접수급 아님, KRX API 연결 확인 필요")
        else:
            flow_str = "\n⚠️ 외국인 수급 없음 (KRX API 미연결, EWY도 N/A)"

    fg_str2 = ""
    if fg_source_val == "vix_proxy":
        fg_str2 = "\n📌 Fear&Greed: VIX+모멘텀 자체계산 (신뢰도:" + fg_conf_val + ")"

    # PCR + 역레포 + 달러인덱스
    macro_extras = []
    pcr_avg = data.get("pcr_avg")
    pcr_sig = data.get("pcr_signal", "")
    if pcr_avg is not None:
        macro_extras.append("PCR=" + str(pcr_avg) + "(" + pcr_sig + ")")
    rrp = data.get("fred_rrp")
    if rrp is not None:
        macro_extras.append("역레포=" + str(rrp) + "조달러")
    dxy = data.get("fred_dxy")
    if dxy is not None:
        macro_extras.append("DXY=" + str(dxy))
    macro_str = ("\n📊 " + " | ".join(macro_extras)) if macro_extras else ""

    try: krw = str(round(float(str(data.get("krw_usd","0")).replace(",","")))) + " KRW/USD"
    except: krw = str(data.get("krw_usd","N/A"))

    fg  = data.get("fear_greed_value","N/A")
    fg_str = (fg + " / " + data.get("fear_greed_rating","") +
              " (전일: " + data.get("fear_greed_prev","N/A") + ")") if fg != "N/A" else "N/A"

    kr_n   = data.get("korea_special_news",[])
    kr_str = ("\n\n### 한국 특수 뉴스\n" +
              "".join("- " + n.get("headline","") + "\n" for n in kr_n)) if kr_n else ""

    lev     = data.get("kodex_lev", "N/A")
    lev_chg = data.get("kodex_lev_change", "")
    lev_str = ("\n- KODEX레버리지: " + lev + " (" + lev_chg + ") — 국내 단기 수급 활동성") if lev != "N/A" else ""

    # FSC 금시세·유류가
    fsc_str = ""
    if data.get("fsc_source"):
        parts = []
        if data.get("fsc_gold"):
            parts.append("금시세 " + str(data["fsc_gold"]) + "원/kg")
        if data.get("fsc_oil_diesel"):
            parts.append("경유 " + str(data["fsc_oil_diesel"]) + "원/L")
        if data.get("fsc_oil_gasoline"):
            parts.append("휘발유 " + str(data["fsc_oil_gasoline"]) + "원/L")
        if parts:
            fsc_str = "\n- " + " | ".join(parts)

    def v(k): return data.get(k,"N/A")
    def vc(k): return data.get(k+"_change","")

    return (
        "\n\n## 시장 데이터 (" + data_label + ")\n수집: " + v("fetched_at")
        + qual_str + status_str + flow_str + fg_str2 + macro_str + "\n\n"
        "### 미국\n- S&P500: " + v("sp500") + " (" + vc("sp500") + ")\n"
        "- 나스닥: " + v("nasdaq") + " (" + vc("nasdaq") + ")\n"
        "- VIX: " + v("vix") + " (" + vc("vix") + ")\n"
        "- 미국10Y: " + v("us_10y") + "%\n"
        "- Fear&Greed: " + fg_str + "\n\n"
        "### 한국\n- 코스피: " + v("kospi") + " (" + vc("kospi") + ")\n"
        "- 원/달러: " + krw + "\n"
        "- SK하이닉스: " + v("sk_hynix") + " (" + vc("sk_hynix") + ")\n"
        "- 삼성전자: " + v("samsung") + " (" + vc("samsung") + ")\n"
        "- 카카오: " + v("kakao") + " (" + vc("kakao") + ")\n"
        "- SOL고배당: " + v("kodex") + " (" + vc("kodex") + ")"
        + lev_str + fsc_str + "\n\n"
        "### 포트폴리오\n- 엔비디아: " + v("nvda") + " (" + vc("nvda") + ")\n"
        "- 브로드컴: " + v("avgo") + " (" + vc("avgo") + ")\n"
        "- SCHD: " + v("schd") + " (" + vc("schd") + ")"
        + alert_str + kr_str
    )

if __name__=="__main__":
    d=fetch_all_market_data()
    for k in ["vix","kospi","krw_usd","kakao","kodex","nvda","fear_greed_value","data_quality"]: print(k+": "+str(d.get(k,"N/A")))
    print(get_monthly_cost_summary())
