"""
aria_backtest.py — ARIA 사전 학습 스크립트
실제 2026년 4월 최근 5거래일 데이터로 분석→검증 사이클을 돌려
accuracy.json, aria_lessons.json, aria_weights.json을 미리 채운다.

사용법:
  python aria_backtest.py       # 실제 API 호출 (비용 약 $0.5)
  python aria_backtest.py --dry # API 없이 구조만 확인
"""
import os, sys, json, re, argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

KST = timezone(timedelta(hours=9))
API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

MEMORY_FILE   = Path("memory.json")
ACCURACY_FILE = Path("accuracy.json")
LESSONS_FILE  = Path("aria_lessons.json")
WEIGHTS_FILE  = Path("aria_weights.json")
MODEL         = "claude-sonnet-4-6"

# ── 실제 2026년 4월 5거래일 데이터 ──────────────────────────────────────────
HIST_DATA = {
    "2026-04-07": {
        "sp500": 5074.08,  "sp500_change": "-5.97%",
        "nasdaq": 15587.79,"nasdaq_change": "-5.82%",
        "vix": 52.33,      "kospi": 2328.20,  "kospi_change": "-5.57%",
        "krw_usd": 1488.50,"sk_hynix": 165400,"sk_hynix_change": "-8.11%",
        "samsung": 49100,  "samsung_change": "-4.84%",
        "nvda": 88.01,     "nvda_change": "-7.36%",
        "fear_greed": "4", "fear_greed_label": "Extreme Fear",
        "note": "트럼프 상호관세 발효 — 글로벌 증시 동반 폭락"
    },
    "2026-04-08": {
        "sp500": 5153.84,  "sp500_change": "+1.57%",
        "nasdaq": 15939.58,"nasdaq_change": "+2.26%",
        "vix": 46.98,      "kospi": 2420.32,  "kospi_change": "+3.95%",
        "krw_usd": 1471.20,"sk_hynix": 176800,"sk_hynix_change": "+6.89%",
        "samsung": 51200,  "samsung_change": "+4.28%",
        "nvda": 94.31,     "nvda_change": "+7.16%",
        "fear_greed": "7", "fear_greed_label": "Extreme Fear",
        "note": "기술적 반등 — 협상 기대감"
    },
    "2026-04-09": {
        "sp500": 5456.90,  "sp500_change": "+5.87%",
        "nasdaq": 17124.97,"nasdaq_change": "+7.47%",
        "vix": 33.62,      "kospi": 2468.99,  "kospi_change": "+2.01%",
        "krw_usd": 1454.80,"sk_hynix": 183600,"sk_hynix_change": "+3.85%",
        "samsung": 52900,  "samsung_change": "+3.32%",
        "nvda": 104.49,    "nvda_change": "+10.79%",
        "fear_greed": "17","fear_greed_label": "Extreme Fear",
        "note": "트럼프 90일 관세 유예 발표 — 나스닥 역대 최대 상승"
    },
    "2026-04-10": {
        "sp500": 5268.05,  "sp500_change": "-3.46%",
        "nasdaq": 16387.31,"nasdaq_change": "-4.31%",
        "vix": 40.72,      "kospi": 2432.11,  "kospi_change": "-1.49%",
        "krw_usd": 1467.30,"sk_hynix": 176200,"sk_hynix_change": "-4.03%",
        "samsung": 51600,  "samsung_change": "-2.46%",
        "nvda": 97.82,     "nvda_change": "-6.38%",
        "fear_greed": "12","fear_greed_label": "Extreme Fear",
        "note": "CPI 예상 상회 + 중국 125% 보복관세 발표"
    },
    "2026-04-11": {
        "sp500": 5363.36,  "sp500_change": "+1.81%",
        "nasdaq": 16724.46,"nasdaq_change": "+2.06%",
        "vix": 37.56,      "kospi": 2469.06,  "kospi_change": "+1.52%",
        "krw_usd": 1460.10,"sk_hynix": 181400,"sk_hynix_change": "+2.95%",
        "samsung": 53100,  "samsung_change": "+2.91%",
        "nvda": 104.75,    "nvda_change": "+7.09%",
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


ANALYST_PROMPT = """당신은 ARIA 투자 분석 에이전트입니다.
아래 시장 데이터를 보고 해당 날짜의 시장 분석을 수행하세요.
thesis_killers는 다음날 주가로 검증 가능한 구체적 수치 조건을 포함해야 합니다.
Return ONLY valid JSON. No markdown.
{
  "analysis_date": "",
  "market_regime": "위험선호/위험회피/전환중/혼조",
  "trend_phase": "상승추세/횡보추세/하락추세",
  "confidence_overall": "낮음/보통/높음",
  "one_line_summary": "",
  "thesis_killers": [
    {"event":"","timeframe":"1~2일","confirms_if":"수치 조건","invalidates_if":"수치 조건"}
  ],
  "outflows": [{"zone":"","reason":"","severity":"높음/보통/낮음"}],
  "inflows":  [{"zone":"","reason":"","momentum":"강함/형성중/약함"}],
  "korea_focus": {"krw_usd":"","kospi_flow":"","assessment":""}
}"""


def generate_analysis(date, market_data, dry=False):
    if dry:
        fg = float(market_data.get("fear_greed","50"))
        sp_chg = float(market_data["sp500_change"].replace("%","").replace("+",""))
        return {
            "analysis_date": date, "mode": "MORNING",
            "market_regime": "위험회피" if fg < 20 else "혼조",
            "trend_phase": "하락추세" if sp_chg < 0 else "상승추세",
            "confidence_overall": "보통",
            "one_line_summary": f"[DRY] {market_data['note']}",
            "thesis_killers": [
                {"event": "나스닥 방향성", "timeframe": "1일",
                 "confirms_if": "나스닥 +1% 이상", "invalidates_if": "나스닥 -1% 이하"},
                {"event": "코스피 외국인 수급", "timeframe": "1일",
                 "confirms_if": "코스피 +1% 이상", "invalidates_if": "코스피 -1% 이하"}
            ],
            "outflows": [{"zone": "위험자산", "reason": "관세 불확실성", "severity": "높음"}],
            "inflows":  [{"zone": "현금/안전자산", "reason": "공포 구간", "momentum": "강함"}],
            "korea_focus": {"krw_usd": str(market_data["krw_usd"]),
                           "kospi_flow": market_data["kospi_change"], "assessment": "추정"}
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
        f"Fear&Greed: {d['fear_greed']} ({d['fear_greed_label']})\n\nJSON 반환:"
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
    if not m: raise ValueError("JSON 파싱 실패\n" + full[:200])
    result = json.loads(m.group())
    result["analysis_date"] = date
    result["mode"] = "MORNING"
    return result


def verify_predictions(analysis, next_data):
    results = []
    def pct(k): 
        try: return float(str(next_data.get(k,"0")).replace("%","").replace("+",""))
        except: return 0.0

    nq = pct("nasdaq_change"); sp = pct("sp500_change")
    ks = pct("kospi_change");  sk = pct("sk_hynix_change"); nv = pct("nvda_change")

    for tk in analysis.get("thesis_killers",[]):
        event = tk.get("event","").lower()
        conf  = tk.get("confirms_if","").lower()
        inv   = tk.get("invalidates_if","").lower()
        v, ev, cat = "unclear", "", "기타"

        if any(k in event for k in ["나스닥","nasdaq","기술주","미국"]):
            cat = "주식"; chg = nq
            if   chg >= 1.0 and any(w in conf for w in ["상승","반등","+1"]): v,ev = "confirmed",   f"나스닥 {chg:+.2f}%"
            elif chg <= -1.0 and any(w in conf for w in ["하락","급락","-1"]): v,ev = "confirmed",  f"나스닥 {chg:+.2f}%"
            elif chg >= 1.0 and any(w in inv for w in ["상승","반등"]):         v,ev = "invalidated",f"나스닥 {chg:+.2f}% (반대)"
            elif chg <= -1.0 and any(w in inv for w in ["하락","급락"]):        v,ev = "invalidated",f"나스닥 {chg:+.2f}% (반대)"
            elif abs(chg) < 1.0: v,ev = "unclear", f"나스닥 변동 미미 ({chg:+.2f}%)"

        elif any(k in event for k in ["코스피","kospi","한국"]):
            cat = "주식"; chg = ks
            if   chg >= 1.0 and any(w in conf for w in ["상승","반등"]): v,ev = "confirmed",   f"코스피 {chg:+.2f}%"
            elif chg <= -1.0 and any(w in conf for w in ["하락","급락"]): v,ev = "confirmed",  f"코스피 {chg:+.2f}%"
            elif chg >= 1.0 and "하락" in inv:  v,ev = "invalidated", f"코스피 {chg:+.2f}% (반대)"
            elif chg <= -1.0 and "상승" in inv: v,ev = "invalidated", f"코스피 {chg:+.2f}% (반대)"

        elif any(k in event for k in ["반도체","하이닉스","엔비디아","nvidia"]):
            cat = "주식"; chg = sk if "하이닉스" in event else nv
            if   chg >= 2.0 and any(w in conf for w in ["상승","강세"]): v,ev = "confirmed",   f"반도체 {chg:+.2f}%"
            elif chg <= -2.0 and any(w in conf for w in ["하락","약세"]): v,ev = "confirmed",  f"반도체 {chg:+.2f}%"
            elif chg >= 2.0 and "하락" in inv:  v,ev = "invalidated", f"반도체 {chg:+.2f}% (반대)"
            elif chg <= -2.0 and "상승" in inv: v,ev = "invalidated", f"반도체 {chg:+.2f}% (반대)"

        elif any(k in event for k in ["환율","원달러","krw"]): cat = "환율"

        results.append({"event":tk.get("event",""),"verdict":v,"evidence":ev,
                        "category":cat,"confirms_if":tk.get("confirms_if",""),
                        "invalidates_if":tk.get("invalidates_if","")})
    return results


def update_accuracy(results, date):
    acc = _load(ACCURACY_FILE, {"total":0,"correct":0,"by_category":{},
                                "history":[],"history_by_category":[],"weak_areas":[],"strong_areas":[]})
    judged  = [r for r in results if r["verdict"] != "unclear"]
    correct = [r for r in judged  if r["verdict"] == "confirmed"]
    acc["total"]   += len(judged)
    acc["correct"] += len(correct)

    today_cat = {}
    for r in judged:
        cat = r.get("category","기타")
        acc["by_category"].setdefault(cat,{"total":0,"correct":0})
        acc["by_category"][cat]["total"] += 1
        today_cat.setdefault(cat,{"total":0,"correct":0})
        today_cat[cat]["total"] += 1
        if r["verdict"] == "confirmed":
            acc["by_category"][cat]["correct"] += 1
            today_cat[cat]["correct"] += 1

    today_acc = round(len(correct)/len(judged)*100,1) if judged else 0
    acc["history"].append({"date":date,"total":len(judged),"correct":len(correct),"accuracy":today_acc})
    acc["history"] = acc["history"][-90:]
    acc.setdefault("history_by_category",[])
    acc["history_by_category"] = [h for h in acc["history_by_category"] if h.get("date")!=date]
    acc["history_by_category"].append({"date":date,"by_category":today_cat})
    acc["history_by_category"] = acc["history_by_category"][-90:]

    strong, weak = [], []
    for cat,s in acc["by_category"].items():
        if s["total"] >= 2:
            a = s["correct"]/s["total"]*100
            if a >= 70: strong.append(f"{cat} ({round(a)}%)")
            elif a <= 40: weak.append(f"{cat} ({round(a)}%)")
    acc["strong_areas"] = strong; acc["weak_areas"] = weak
    _save(ACCURACY_FILE, acc)
    return today_acc, len(correct), len(judged)


def extract_lessons(results, analysis, date):
    lessons = _load(LESSONS_FILE, {"lessons":[],"total_lessons":0,"last_updated":""})
    fg = float(HIST_DATA.get(date,{}).get("fear_greed","50"))
    regime = analysis.get("market_regime","")

    for r in results:
        if r["verdict"] == "invalidated":
            text = f"{r['event'][:30]} 예측 실패 — 실제: {r.get('evidence','')[:25]}"
            sev  = "high" if "주식" in r["category"] else "medium"
            lessons["lessons"].append({
                "date":date,"source":"backtest","category":r["category"],
                "lesson":text,"severity":sev,"applied":0,"reinforced":0
            })
            lessons["total_lessons"] += 1

    if fg <= 15 and "선호" in regime:
        lessons["lessons"].append({
            "date":date,"source":"backtest","category":"레짐판단",
            "lesson":f"FG {fg}(극단공포)에서 위험선호 판단 오류 — 관세 충격기",
            "severity":"high","applied":0,"reinforced":0
        })
        lessons["total_lessons"] += 1

    lessons["lessons"] = sorted(lessons["lessons"],key=lambda x:x["date"],reverse=True)[:60]
    lessons["last_updated"] = date
    _save(LESSONS_FILE, lessons)


def save_to_memory(analysis):
    memory = _load(MEMORY_FILE, [])
    if not isinstance(memory, list): memory = []
    date = analysis.get("analysis_date","")
    memory = [m for m in memory if m.get("analysis_date") != date]
    memory = (memory + [analysis])[-90:]
    _save(MEMORY_FILE, memory)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("ARIA Backtest — 5거래일 사전 학습" + (" [DRY RUN]" if args.dry else ""))
    print("=" * 60)

    total_judged = total_correct = 0

    for i, date in enumerate(DATES):
        md        = HIST_DATA[date]
        next_date = DATES[i+1] if i+1 < len(DATES) else None
        next_data = HIST_DATA.get(next_date,{}) if next_date else {}

        print(f"\n{'─'*50}")
        print(f"📅 {date} — {md['note']}")
        print(f"   S&P {md['sp500']} ({md['sp500_change']}) | VIX {md['vix']} | FG {md['fear_greed']}")

        print("  [1] 분석 생성 중...")
        analysis = generate_analysis(date, md, dry=args.dry)
        save_to_memory(analysis)
        print(f"      레짐: {analysis.get('market_regime','')} | 추세: {analysis.get('trend_phase','')} | TK: {len(analysis.get('thesis_killers',[]))}개")
        print(f"  [2] memory.json 저장 완료")

        if next_data:
            print(f"  [3] 검증 — {next_date} 실제 데이터 기준")
            results = verify_predictions(analysis, next_data)

            for r in results:
                icon = "✅" if r["verdict"]=="confirmed" else "❌" if r["verdict"]=="invalidated" else "❓"
                print(f"      {icon} [{r['category']}] {r['event'][:38]}")
                if r.get("evidence"): print(f"          → {r['evidence']}")

            acc_pct, c, j = update_accuracy(results, date)
            total_judged  += j; total_correct += c
            extract_lessons(results, analysis, date)
            print(f"  [4] 정확도: {acc_pct}% ({c}/{j}) | accuracy.json + lessons 업데이트")
        else:
            print(f"  [3] 마지막 날 — 검증 생략 (오늘이 다음날)")

    print(f"\n{'─'*50}")
    print("📊 가중치 업데이트 중...")
    try:
        from aria_analysis import update_weights_from_accuracy
        acc = _load(ACCURACY_FILE, {})
        changes = update_weights_from_accuracy(acc)
        for c in changes: print(f"   → {c}")
    except Exception as e:
        print(f"   스킵: {e}")

    acc     = _load(ACCURACY_FILE, {})
    lessons = _load(LESSONS_FILE, {})
    overall = round(total_correct/total_judged*100,1) if total_judged else 0

    print(f"\n{'='*60}")
    print(f"✅ Backtest 완료")
    print(f"   검증: {total_judged}건 | 적중: {total_correct}건 | 정확도: {overall}%")
    print(f"   생성된 교훈: {len(lessons.get('lessons',[]))}개")
    if acc.get("strong_areas"): print(f"   강점: {acc['strong_areas']}")
    if acc.get("weak_areas"):   print(f"   약점: {acc['weak_areas']}")
    print(f"   → 다음 MORNING 실행 시 학습 데이터 자동 반영")
    print("=" * 60)


if __name__ == "__main__":
    main()
