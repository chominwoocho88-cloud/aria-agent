"""
금융위원회 공공데이터 API 테스트 스크립트
GitHub Actions에서 수동 실행하여 오퍼레이션명 + 지수명 확인
"""
import os, httpx, json
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))
KEY = os.environ.get("FSCAPI_KEY", "")
BASE = "https://apis.data.go.kr/1160100/service"

# 직전 거래일 계산
d = datetime.now(KST) - timedelta(days=1)
while d.weekday() >= 5:
    d -= timedelta(days=1)
date_str = d.strftime("%Y%m%d")
print(f"조회일자: {date_str}")
print(f"키 설정: {'✅' if KEY else '❌ FSCAPI_KEY 미설정'}")
print()

def test(label, url, extra_params={}):
    params = {
        "serviceKey": KEY,
        "numOfRows": "5",
        "pageNo": "1",
        "resultType": "json",
        "basDd": date_str,
        **extra_params,
    }
    try:
        r = httpx.get(url, params=params, timeout=10)
        if r.status_code != 200:
            print(f"  [{label}] {r.status_code}: {r.text[:100]}")
            return
        j = r.json()
        body  = j.get("response", {}).get("body", {})
        total = body.get("totalCount", 0)
        items = body.get("items", {})
        item  = items.get("item", []) if isinstance(items, dict) else []
        items_list = item if isinstance(item, list) else [item]
        print(f"  [{label}] ✅ total={total}")
        if items_list:
            first = items_list[0]
            for k, v in list(first.items())[:6]:
                print(f"    {k}: {v}")
    except Exception as e:
        print(f"  [{label}] 실패: {e}")
    print()

print("=" * 50)
print("① 지수시세정보 — GetMarketIndexInfoService")
print("=" * 50)
test("주가지수(KOSPI 등)", BASE + "/GetMarketIndexInfoService/getStockMarketIndex")
test("파생상품지수(VKOSPI)", BASE + "/GetMarketIndexInfoService/getDerivativeProductIndex")
test("채권지수", BASE + "/GetMarketIndexInfoService/getBondMarketIndex")

print("=" * 50)
print("② 주식시세정보 — GetStockSecuritiesInfoService")
print("=" * 50)
# 삼성전자 종목코드 005930
test("삼성전자 시세", BASE + "/GetStockSecuritiesInfoService/getStockPriceInfo",
     {"likeSrtnCd": "005930"})
# SK하이닉스 000660
test("SK하이닉스 시세", BASE + "/GetStockSecuritiesInfoService/getStockPriceInfo",
     {"likeSrtnCd": "000660"})

print("=" * 50)
print("③ 기업재무정보 — GetFinaStatInfoService_V2")
print("=" * 50)
# 삼성전자 corp_code
test("삼성전자 재무요약", BASE + "/GetFinaStatInfoService_V2/getSummFinaStat",
     {"crno": "1301110006246", "bizYear": "2025"})

print("=" * 50)
print("④ 일반상품시세정보 — GetGeneralProductInfoService")
print("=" * 50)
test("금시세", BASE + "/GetGeneralProductInfoService/getGoldPriceInfo")
test("원유시세", BASE + "/GetGeneralProductInfoService/getCrudeOilPriceInfo")
