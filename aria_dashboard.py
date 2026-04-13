"""
aria_dashboard.py — ARIA 정적 HTML 대시보드 생성 (리디자인 v2)
"""
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

KST = timezone(timedelta(hours=9))

from aria_paths import (
    SENTIMENT_FILE, ACCURACY_FILE, ROTATION_FILE,
    MEMORY_FILE, COST_FILE, DASHBOARD_FILE as OUTPUT_FILE,
    PATTERN_DB_FILE, DATA_FILE,
)

try:
    from aria_paths import PORTFOLIO_FILE
except ImportError:
    PORTFOLIO_FILE = Path("portfolio.json")


def _load(path, default=None):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return default or {}

def _j(v):   return json.dumps(v, ensure_ascii=False)
def _esc(s): return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")


def build_dashboard():
    now = datetime.now(KST)

    sent    = _load(SENTIMENT_FILE, {"history": [], "current": {}})
    acc     = _load(ACCURACY_FILE,  {"total": 0, "correct": 0, "by_category": {}, "history": []})
    rotation= _load(ROTATION_FILE,  {"ranking": [], "today_flows": {}})
    memory  = _load(MEMORY_FILE,    [])
    cost    = _load(COST_FILE,      {})
    pattern = _load(PATTERN_DB_FILE,{})
    market  = _load(DATA_FILE,      {})

    current  = sent.get("current", {})
    hist_30  = sent.get("history", [])[-30:]
    latest   = memory[-1] if isinstance(memory, list) and memory else {}

    sent_dates  = [h["date"][5:] for h in hist_30]
    sent_scores = [h["score"] for h in hist_30]
    sent_score  = current.get("score", 50)
    sent_level  = current.get("level", "중립")
    sent_emoji  = current.get("emoji", "😐")

    acc_total   = acc.get("total", 0)
    acc_correct = acc.get("correct", 0)
    acc_pct     = round(acc_correct / acc_total * 100, 1) if acc_total > 0 else 0
    dir_pct     = acc.get("dir_accuracy_pct", 0)
    by_cat      = acc.get("by_category", {})
    cat_labels  = list(by_cat.keys())
    cat_pcts    = [round(v["correct"]/v["total"]*100) if v["total"]>0 else 0 for v in by_cat.values()]

    pat_summary = pattern.get("summary", [])[:4]
    blackswan   = pattern.get("blackswan", {})

    ranking    = rotation.get("ranking", [])
    rot_labels = [r[0] for r in ranking][:8]
    rot_values = [r[1] for r in ranking][:8]
    top_inflow  = ranking[0][0] if ranking and ranking[0][1] > 0 else "—"
    top_outflow = next((r[0] for r in reversed(ranking) if r[1] < 0), "—")

    regime      = latest.get("market_regime", "")
    confidence  = latest.get("confidence_overall", "")
    summary     = latest.get("one_line_summary", "")
    trend       = latest.get("trend_phase", "")
    strategy    = latest.get("trend_strategy", {})
    caution     = strategy.get("caution", "") if isinstance(strategy, dict) else ""
    recommended = strategy.get("recommended", "") if isinstance(strategy, dict) else ""
    analysis_date = latest.get("analysis_date", "")
    counterargs = latest.get("counterarguments", [])
    main_risk   = counterargs[0].get("against", "—") if counterargs else "—"

    if PORTFOLIO_FILE.exists():
        holdings = json.loads(PORTFOLIO_FILE.read_text(encoding="utf-8")).get("holdings", [])
    else:
        holdings = []
    port_rows = []
    for h in holdings:
        if h.get("ticker") == "cash": continue
        chg_str = market.get(str(h.get("ticker","")) + "_change", "0%")
        try:    chg = float(str(chg_str).replace("%","").replace("+",""))
        except: chg = 0.0
        port_rows.append((h.get("name",""), chg))

    month_key  = now.strftime("%Y-%m")
    month_cost = cost.get("monthly_runs", {}).get(month_key, {})
    cost_usd   = month_cost.get("estimated_usd", 0.0)
    cost_krw   = round(cost_usd * 1480)
    cost_runs  = month_cost.get("runs", 0)

    def rc(r):
        if "선호" in r: return "#0FC47D"
        if "회피" in r: return "#F04949"
        return "#F5A623"
    def rl(r):
        if "선호" in r: return "위험선호"
        if "회피" in r: return "위험회피"
        return "혼조"
    def sc_fn(s):
        if s<=25: return "#F04949"
        if s<=45: return "#F5A623"
        if s<=60: return "#888780"
        if s<=75: return "#0FC47D"
        return "#0DAE6B"
    def conf_badge(c):
        m = {"높음":("#E8F9F3","#0DAE6B"),"보통":("#FEF5E7","#CC8800"),"낮음":("#FEE8E8","#C0392B")}
        return m.get(c,("#F0F0EE","#888780"))

    _rc  = rc(regime)
    _rl  = rl(regime)
    _sc  = sc_fn(sent_score)
    cbg, cfg = conf_badge(confidence)
    kpi_sent_cls = "green" if sent_score>60 else "amber" if sent_score>40 else "red"
    kpi_acc_cls  = "green" if acc_pct>=55 else "amber" if acc_pct>=45 else "red"

    def flow_items(direction):
        if direction == "in":
            items = [(r[0],r[1]) for r in ranking if r[1]>0][:3]
        else:
            items = [(r[0],r[1]) for r in reversed(ranking) if r[1]<0][:3]
        if not items:
            return '<span class="no-data">데이터 없음</span>'
        out = ""
        for label,val in items:
            cls = "pos" if val>=0 else "neg"
            sign = "+" if val>0 else ""
            out += f'<div class="flow-item"><span class="flow-label">{_esc(label)}</span><span class="flow-val {cls}">{sign}{val}</span></div>'
        return out

    def pat_chips():
        if not pat_summary:
            return '<p class="no-data">패턴 데이터 없음</p>'
        out = '<div class="pat-grid">'
        for p in pat_summary:
            parts = p.split("→")
            label = parts[0].strip() if parts else p
            prob  = parts[1].strip() if len(parts)>1 else ""
            out += f'<div class="pat-chip"><div class="pat-label">{_esc(label)}</div>'
            if prob: out += f'<div class="pat-prob">{_esc(prob)}</div>'
            out += '</div>'
        if blackswan.get("reversal_count",0)>0:
            cnt = blackswan["reversal_count"]
            avg = blackswan.get("avg_streak_before_reversal",0)
            out += f'<div class="pat-chip pat-swan"><div class="pat-label">🦢 블랙스완</div><div class="pat-prob">{cnt}회 · 평균 {avg}일 후 반전</div></div>'
        out += '</div>'
        return out

    def port_html():
        if not port_rows:
            return '<div class="no-data-card">포트폴리오 데이터 없음</div>'
        out = ""
        for name,chg in port_rows:
            cls = "pos" if chg>0 else "neg" if chg<0 else "neu"
            sign = "+" if chg>0 else ""
            out += f'<div class="port-row"><span class="port-name">{_esc(name)}</span><span class="port-chg {cls}">{sign}{chg}%</span></div>'
        return out

    mr_short = _esc(main_risk[:28]+"…") if len(main_risk)>28 else _esc(main_risk)
    caut_short = _esc(caution[:100]+"…") if len(caution)>100 else _esc(caution)

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<title>ARIA · 시장 대시보드</title>
<style>
:root{{--bg:#F2F1EC;--card:#FFF;--card2:#F7F6F2;--text:#18181A;--sub:#6B6B6E;--border:rgba(0,0,0,.07);--green:#0FC47D;--green-dk:#0DAE6B;--red:#F04949;--amber:#F5A623;--blue:#3B82F6;--r:18px;--r-sm:12px;}}
@media(prefers-color-scheme:dark){{:root{{--bg:#131312;--card:#1E1E1C;--card2:#252523;--text:#EEEDE6;--sub:#888782;--border:rgba(255,255,255,.07);}}}}
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0;}}
body{{font-family:-apple-system,'Helvetica Neue',sans-serif;background:var(--bg);color:var(--text);max-width:430px;margin:0 auto;padding-bottom:calc(env(safe-area-inset-bottom)+32px);-webkit-font-smoothing:antialiased;}}
.header{{padding:20px 20px 0;display:flex;align-items:flex-start;justify-content:space-between;gap:12px;}}
.header-wordmark{{font-size:13px;font-weight:700;letter-spacing:3px;color:var(--sub);text-transform:uppercase;}}
.header-time{{font-size:11px;color:var(--sub);margin-top:3px;}}
.regime-pill{{font-size:12px;font-weight:600;padding:6px 14px;border-radius:100px;color:#fff;white-space:nowrap;margin-top:2px;}}
.summary-wrap{{padding:14px 16px 0;}}
.summary-card{{background:var(--card);border-radius:var(--r);padding:18px 20px;border:.5px solid var(--border);}}
.summary-meta{{display:flex;align-items:center;gap:8px;margin-bottom:10px;}}
.conf-badge{{font-size:11px;font-weight:600;padding:3px 9px;border-radius:100px;}}
.trend-tag{{font-size:11px;font-weight:500;color:var(--sub);background:var(--card2);padding:3px 8px;border-radius:6px;}}
.summary-text{{font-size:15px;line-height:1.55;color:var(--text);letter-spacing:-.2px;}}
.kpi-wrap{{padding:12px 16px 0;}}
.kpi-grid{{display:grid;grid-template-columns:1fr 1fr;gap:10px;}}
.kpi-card{{background:var(--card);border-radius:var(--r);border:.5px solid var(--border);padding:16px 16px 14px;position:relative;overflow:hidden;}}
.kpi-card::before{{content:'';position:absolute;top:0;left:0;right:0;height:3px;border-radius:var(--r) var(--r) 0 0;}}
.kpi-card.green::before{{background:var(--green);}}
.kpi-card.red::before{{background:var(--red);}}
.kpi-card.amber::before{{background:var(--amber);}}
.kpi-label{{font-size:11px;font-weight:500;color:var(--sub);text-transform:uppercase;letter-spacing:.6px;margin-bottom:8px;}}
.kpi-value{{font-size:30px;font-weight:700;letter-spacing:-1.5px;line-height:1;margin-bottom:5px;}}
.kpi-sub{{font-size:11px;color:var(--sub);line-height:1.4;}}
.sec{{padding:20px 16px 0;}}
.sec-title{{font-size:11px;font-weight:600;color:var(--sub);text-transform:uppercase;letter-spacing:.8px;margin-bottom:12px;}}
.card{{background:var(--card);border-radius:var(--r);border:.5px solid var(--border);padding:18px;margin-bottom:10px;}}
.conclusion-grid{{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px;}}
.flow-card{{background:var(--card);border-radius:var(--r);border:.5px solid var(--border);padding:14px;}}
.flow-header{{display:flex;align-items:center;gap:6px;margin-bottom:10px;}}
.flow-dot{{width:8px;height:8px;border-radius:50%;flex-shrink:0;}}
.flow-title{{font-size:12px;font-weight:600;}}
.flow-item{{display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:.5px solid var(--border);}}
.flow-item:last-child{{border-bottom:none;}}
.flow-label{{font-size:12px;font-weight:500;}}
.flow-val{{font-size:12px;font-weight:700;}}
.action-card{{background:var(--text);border-radius:var(--r);padding:16px 18px;margin-bottom:10px;}}
.action-title{{font-size:11px;font-weight:600;color:var(--green);text-transform:uppercase;letter-spacing:.8px;margin-bottom:8px;}}
.action-text{{font-size:14px;line-height:1.55;color:var(--bg);}}
.caution-card{{background:rgba(245,166,35,.08);border:.5px solid rgba(245,166,35,.25);border-radius:var(--r);padding:14px 16px;display:flex;align-items:flex-start;gap:10px;}}
.caution-icon{{font-size:16px;flex-shrink:0;margin-top:1px;}}
.caution-text{{font-size:13px;line-height:1.5;}}
.chart-card{{background:var(--card);border-radius:var(--r);border:.5px solid var(--border);padding:18px 18px 14px;margin-bottom:10px;}}
.chart-header{{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:14px;}}
.chart-title{{font-size:14px;font-weight:600;}}
.chart-stat{{font-size:11px;color:var(--sub);}}
.chart-wrap{{position:relative;}}
.pat-grid{{display:grid;grid-template-columns:1fr 1fr;gap:8px;}}
.pat-chip{{background:var(--card2);border-radius:var(--r-sm);padding:12px 14px;border:.5px solid var(--border);}}
.pat-label{{font-size:12px;font-weight:500;margin-bottom:4px;line-height:1.3;}}
.pat-prob{{font-size:11px;color:var(--sub);}}
.pat-swan{{grid-column:span 2;background:rgba(244,67,54,.05);border-color:rgba(244,67,54,.15);}}
.port-row{{display:flex;justify-content:space-between;align-items:center;padding:11px 0;border-bottom:.5px solid var(--border);}}
.port-row:last-child{{border-bottom:none;}}
.port-name{{font-size:14px;font-weight:500;}}
.port-chg{{font-size:16px;font-weight:700;}}
.cost-row{{display:flex;justify-content:space-between;align-items:center;padding:9px 0;border-bottom:.5px solid var(--border);font-size:13px;}}
.cost-row:last-child{{border-bottom:none;}}
.cost-label{{color:var(--sub);}}
.cost-val{{font-weight:600;}}
.pos{{color:var(--green);}} .neg{{color:var(--red);}} .neu{{color:var(--sub);}}
.no-data{{font-size:13px;color:var(--sub);padding:8px 0;}}
.no-data-card{{background:var(--card2);border-radius:var(--r-sm);padding:20px;text-align:center;font-size:13px;color:var(--sub);border:.5px solid var(--border);}}
.footer{{text-align:center;font-size:11px;color:var(--sub);padding:20px 16px 8px;line-height:1.6;}}
</style>
</head>
<body>

<div class="header">
  <div>
    <div class="header-wordmark">ARIA</div>
    <div class="header-time">{now.strftime("%Y.%m.%d %H:%M KST")}</div>
  </div>
  <div class="regime-pill" style="background:{_rc};">{_rl}</div>
</div>

<div class="summary-wrap">
  <div class="summary-card">
    <div class="summary-meta">
      {"" if not confidence else f'<span class="conf-badge" style="background:{cbg};color:{cfg};">신뢰도 {_esc(confidence)}</span>'}
      {"" if not trend else f'<span class="trend-tag">{_esc(trend)}</span>'}
    </div>
    <p class="summary-text">{_esc(summary) if summary else "분석 데이터를 불러오는 중입니다."}</p>
  </div>
</div>

<div class="kpi-wrap">
  <div class="kpi-grid">
    <div class="kpi-card {kpi_sent_cls}">
      <div class="kpi-label">감정지수</div>
      <div class="kpi-value" style="color:{_sc};">{sent_score}</div>
      <div class="kpi-sub">{sent_emoji} {_esc(sent_level)}</div>
    </div>
    <div class="kpi-card {kpi_acc_cls}">
      <div class="kpi-label">예측 정확도</div>
      <div class="kpi-value">{acc_pct}%</div>
      <div class="kpi-sub">방향 {dir_pct}% · {acc_correct}/{acc_total}건</div>
    </div>
    <div class="kpi-card green">
      <div class="kpi-label">자금유입 1위</div>
      <div class="kpi-value" style="font-size:18px;letter-spacing:-.5px;color:var(--green);">{_esc(top_inflow)}</div>
      <div class="kpi-sub">30일 누적 기준</div>
    </div>
    <div class="kpi-card amber">
      <div class="kpi-label">주요 리스크</div>
      <div class="kpi-value" style="font-size:14px;letter-spacing:-.3px;line-height:1.2;padding-top:4px;color:var(--amber);">{mr_short}</div>
      <div class="kpi-sub">Devil 에이전트</div>
    </div>
  </div>
</div>

<div class="sec">
  <div class="sec-title">오늘의 결론</div>
  <div class="conclusion-grid">
    <div class="flow-card">
      <div class="flow-header"><div class="flow-dot" style="background:var(--green);"></div><span class="flow-title">자금 유입</span></div>
      {flow_items("in")}
    </div>
    <div class="flow-card">
      <div class="flow-header"><div class="flow-dot" style="background:var(--red);"></div><span class="flow-title">자금 유출</span></div>
      {flow_items("out")}
    </div>
  </div>
  {"" if not recommended else f'<div class="action-card"><div class="action-title">▶ 행동 가이드</div><div class="action-text">{_esc(recommended)}</div></div>'}
  {"" if not caution else f'<div class="caution-card"><span class="caution-icon">⚠️</span><span class="caution-text">{caut_short}</span></div>'}
</div>

<div class="sec">
  <div class="sec-title">감정지수 추이</div>
  <div class="chart-card">
    <div class="chart-header"><span class="chart-title">30일 추이</span><span class="chart-stat">현재 {sent_score} / {_esc(sent_level)}</span></div>
    <div class="chart-wrap" style="height:150px;"><canvas id="sentChart"></canvas></div>
  </div>
</div>

{"" if not cat_labels else f'<div class="sec"><div class="sec-title">예측 정확도</div><div class="chart-card"><div class="chart-header"><span class="chart-title">카테고리별</span><span class="chart-stat">종합 {acc_pct}%</span></div><div class="chart-wrap" style="height:{max(100,len(cat_labels)*38)}px;"><canvas id="accChart"></canvas></div></div></div>'}

{"" if not rot_labels else f'<div class="sec"><div class="sec-title">섹터 자금흐름 (30일)</div><div class="chart-card"><div class="chart-header"><span class="chart-title">누적 집계</span><span class="chart-stat">상위 {len(rot_labels)}개 섹터</span></div><div class="chart-wrap" style="height:{max(140,len(rot_labels)*34)}px;"><canvas id="rotChart"></canvas></div></div></div>'}

{"" if not pat_summary else f'<div class="sec"><div class="sec-title">레짐 전환 패턴</div><div class="card">{pat_chips()}</div></div>'}

<div class="sec">
  <div class="sec-title">포트폴리오 오늘 손익</div>
  <div class="card" style="padding:8px 18px;">{port_html()}</div>
</div>

<div class="sec">
  <div class="sec-title">이번 달 API 비용</div>
  <div class="card" style="padding:6px 18px;">
    <div class="cost-row"><span class="cost-label">추정 비용</span><span class="cost-val">${cost_usd:.2f} · 약 {cost_krw:,}원</span></div>
    <div class="cost-row"><span class="cost-label">실행 횟수</span><span class="cost-val">{cost_runs}회</span></div>
  </div>
</div>

<div class="footer">
  분석일 {_esc(analysis_date)} · ARIA Multi-Agent v2<br>
  Yahoo Finance · FRED · FSC · FearGreedChart
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script>
const dk=matchMedia('(prefers-color-scheme:dark)').matches;
const tc=dk?'rgba(255,255,255,.4)':'rgba(0,0,0,.38)';
const gc=dk?'rgba(255,255,255,.06)':'rgba(0,0,0,.05)';
const tt={{backgroundColor:dk?'rgba(30,30,28,.95)':'rgba(255,255,255,.95)',titleColor:dk?'#eee':'#111',bodyColor:dk?'#aaa':'#555',borderColor:dk?'rgba(255,255,255,.1)':'rgba(0,0,0,.08)',borderWidth:1,padding:10,cornerRadius:8}};
const bO={{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}},tooltip:{{mode:'index',intersect:false,...tt}}}},scales:{{x:{{ticks:{{color:tc,font:{{size:9}},maxRotation:0,autoSkip:true,maxTicksLimit:6}},grid:{{color:gc}}}},y:{{ticks:{{color:tc,font:{{size:9}}}},grid:{{color:gc}}}}}}}};
const sEl=document.getElementById('sentChart');
if(sEl)new Chart(sEl,{{type:'line',data:{{labels:{_j(sent_dates)},datasets:[{{data:{_j(sent_scores)},borderColor:'#0FC47D',backgroundColor:'rgba(15,196,125,.07)',fill:true,tension:.45,pointRadius:0,pointHoverRadius:4,borderWidth:2}},{{data:Array({len(sent_scores)}).fill(50),borderColor:'rgba(136,135,128,.4)',borderDash:[4,3],borderWidth:1.5,pointRadius:0}}]}},options:{{...bO,scales:{{x:{{ticks:{{color:tc,font:{{size:9}},maxRotation:0,autoSkip:true,maxTicksLimit:6}},grid:{{color:gc}}}},y:{{min:0,max:100,ticks:{{color:tc,font:{{size:9}},stepSize:25}},grid:{{color:gc}}}}}}}}}});
const aEl=document.getElementById('accChart');
if(aEl)new Chart(aEl,{{type:'bar',data:{{labels:{_j(cat_labels)},datasets:[{{data:{_j(cat_pcts)},backgroundColor:{_j(cat_pcts)}.map(v=>v>=65?'#0FC47D':v<=40?'#F04949':'#3B82F6'),borderRadius:5,barPercentage:.55}}]}},options:{{...bO,scales:{{x:{{ticks:{{color:tc,font:{{size:10}}}},grid:{{display:false}}}},y:{{min:0,max:100,ticks:{{color:tc,font:{{size:9}},callback:v=>v+'%'}},grid:{{color:gc}}}}}}}}}});
const rEl=document.getElementById('rotChart');
if(rEl)new Chart(rEl,{{type:'bar',data:{{labels:{_j(rot_labels)},datasets:[{{data:{_j(rot_values)},backgroundColor:{_j(rot_values)}.map(v=>v>=0?'rgba(15,196,125,.8)':'rgba(240,73,73,.75)'),borderRadius:4,barPercentage:.6}}]}},options:{{indexAxis:'y',responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}},tooltip:{{...tt}}}},scales:{{x:{{ticks:{{color:tc,font:{{size:9}}}},grid:{{color:gc}}}},y:{{ticks:{{color:tc,font:{{size:10}}}},grid:{{display:false}}}}}}}}}});
</script>
</body>
</html>"""

    OUTPUT_FILE.write_text(html, encoding="utf-8")
    print("dashboard.html 생성 완료: " + str(OUTPUT_FILE))
    return html


if __name__ == "__main__":
    build_dashboard()
    print("완료")
