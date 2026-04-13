"""
aria_dashboard.py — ARIA 정적 HTML 대시보드 생성
MORNING 실행 후 자동 호출 → dashboard.html 생성 → GitHub Pages 서빙
iPhone 17 Pro (393pt) 최적화
"""
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

KST = timezone(timedelta(hours=9))

SENTIMENT_FILE = Path("sentiment.json")
ACCURACY_FILE  = Path("accuracy.json")
ROTATION_FILE  = Path("rotation.json")
MEMORY_FILE    = Path("memory.json")
COST_FILE      = Path("aria_cost.json")
OUTPUT_FILE    = Path("dashboard.html")


def _load(path, default=None):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return default or {}


def _j(v):
    return json.dumps(v, ensure_ascii=False)


def build_dashboard():
    now = datetime.now(KST)

    sent     = _load(SENTIMENT_FILE, {"history": [], "current": {}})
    acc      = _load(ACCURACY_FILE,  {"total": 0, "correct": 0, "by_category": {}, "history": []})
    rotation = _load(ROTATION_FILE,  {"ranking": [], "today_flows": {}})
    memory   = _load(MEMORY_FILE,    [])
    cost     = _load(COST_FILE,      {})
    pattern  = _load(Path("pattern_db.json"), {})

    current  = sent.get("current", {})
    hist_30  = sent.get("history", [])[-30:]
    latest   = memory[-1] if isinstance(memory, list) and memory else {}

    # ── 감정지수 데이터
    sent_dates  = [h["date"][5:] for h in hist_30]
    sent_scores = [h["score"] for h in hist_30]
    sent_score  = current.get("score", 50)
    sent_level  = current.get("level", "중립")
    sent_emoji  = current.get("emoji", "😐")

    # ── 예측 정확도
    acc_total   = acc.get("total", 0)
    acc_correct = acc.get("correct", 0)
    acc_pct     = round(acc_correct / acc_total * 100, 1) if acc_total > 0 else 0
    dir_pct     = acc.get("dir_accuracy_pct", 0)          # 방향 정확도 (신규)
    by_cat      = acc.get("by_category", {})
    cat_labels  = list(by_cat.keys())
    cat_pcts    = [round(v["correct"] / v["total"] * 100) if v["total"] > 0 else 0
                   for v in by_cat.values()]

    # ── 패턴 DB
    pat_summary = pattern.get("summary", [])[:4]          # 상위 4개 패턴
    blackswan   = pattern.get("blackswan", {})

    # ── 섹터 로테이션
    ranking = rotation.get("ranking", [])
    rot_labels = [r[0] for r in ranking][:8]
    rot_values = [r[1] for r in ranking][:8]

    # ── 포트폴리오
    market = _load(Path("aria_market_data.json"), {})
    pf_file = Path("portfolio.json")
    if pf_file.exists():
        holdings = json.loads(pf_file.read_text(encoding="utf-8")).get("holdings", [])
    else:
        holdings = []

    port_names  = [h["name"] for h in holdings if h["ticker"] != "cash"]
    port_values = []
    for h in holdings:
        if h["ticker"] == "cash":
            continue
        chg_str = market.get(h["ticker"] + "_change", "0%")
        try:
            chg = float(str(chg_str).replace("%", "").replace("+", ""))
        except Exception:
            chg = 0.0
        port_values.append(round(chg, 2))

    # ── 레짐
    regime     = latest.get("market_regime", "데이터 없음")
    confidence = latest.get("confidence_overall", "")
    summary    = latest.get("one_line_summary", "")[:60]
    analysis_date = latest.get("analysis_date", "")

    # ── 비용
    month_key = now.strftime("%Y-%m")
    month_cost = cost.get("monthly_runs", {}).get(month_key, {})
    cost_usd   = month_cost.get("estimated_usd", 0.0)
    cost_krw   = round(cost_usd * 1480)

    # ── 색상 로직
    regime_color = "#1D9E75" if "선호" in regime else "#E24B4A" if "회피" in regime else "#BA7517"
    sent_color   = ("#E24B4A" if sent_score <= 30 else
                    "#BA7517" if sent_score <= 45 else
                    "#888780" if sent_score <= 60 else
                    "#1D9E75")

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<title>ARIA Dashboard</title>
<style>
  :root {{
    --bg:     #f5f5f0;
    --card:   #ffffff;
    --text:   #1a1a1a;
    --muted:  #666660;
    --border: rgba(0,0,0,0.08);
    --radius: 16px;
    --grid:   rgba(0,0,0,0.06);
  }}
  @media (prefers-color-scheme: dark) {{
    :root {{
      --bg:     #111110;
      --card:   #1c1c1a;
      --text:   #f0efe8;
      --muted:  #888780;
      --border: rgba(255,255,255,0.08);
      --grid:   rgba(255,255,255,0.06);
    }}
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', sans-serif;
    background: var(--bg);
    color: var(--text);
    max-width: 430px;
    margin: 0 auto;
    padding: env(safe-area-inset-top) 0 env(safe-area-inset-bottom);
  }}
  .header {{
    padding: 20px 20px 12px;
    display: flex;
    align-items: center;
    justify-content: space-between;
  }}
  .header-left h1 {{ font-size: 22px; font-weight: 600; letter-spacing: -0.5px; }}
  .header-left p  {{ font-size: 12px; color: var(--muted); margin-top: 2px; }}
  .regime-badge {{
    font-size: 11px;
    font-weight: 500;
    padding: 5px 10px;
    border-radius: 20px;
    color: #fff;
  }}
  .section {{ padding: 0 16px 16px; }}
  .card {{
    background: var(--card);
    border-radius: var(--radius);
    border: 0.5px solid var(--border);
    padding: 16px;
    margin-bottom: 12px;
  }}
  .card-title {{
    font-size: 12px;
    font-weight: 500;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 12px;
  }}
  .metrics {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 10px;
    margin-bottom: 12px;
  }}
  .metric {{
    background: var(--card);
    border-radius: 12px;
    border: 0.5px solid var(--border);
    padding: 14px;
  }}
  .metric-label {{ font-size: 11px; color: var(--muted); margin-bottom: 4px; }}
  .metric-value {{ font-size: 26px; font-weight: 600; letter-spacing: -1px; }}
  .metric-sub   {{ font-size: 11px; color: var(--muted); margin-top: 2px; }}
  .summary-card {{
    background: var(--card);
    border-radius: var(--radius);
    border: 0.5px solid var(--border);
    padding: 14px 16px;
    margin-bottom: 12px;
  }}
  .summary-text {{ font-size: 14px; line-height: 1.5; color: var(--text); }}
  .chart-wrap {{ position: relative; }}
  .legend {{
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    margin-bottom: 8px;
  }}
  .legend-item {{
    display: flex;
    align-items: center;
    gap: 5px;
    font-size: 11px;
    color: var(--muted);
  }}
  .legend-dot {{
    width: 8px; height: 8px;
    border-radius: 2px;
    flex-shrink: 0;
  }}
  .port-row {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 9px 0;
    border-bottom: 0.5px solid var(--border);
    font-size: 14px;
  }}
  .port-row:last-child {{ border-bottom: none; }}
  .port-name  {{ color: var(--text); font-weight: 500; }}
  .port-chg   {{ font-weight: 600; font-size: 15px; }}
  .pos {{ color: #1D9E75; }}
  .neg {{ color: #E24B4A; }}
  .neu {{ color: var(--muted); }}
  .cost-row {{
    display: flex;
    justify-content: space-between;
    font-size: 13px;
    padding: 4px 0;
    color: var(--muted);
  }}
  .cost-val {{ color: var(--text); font-weight: 500; }}
  .footer {{
    text-align: center;
    font-size: 11px;
    color: var(--muted);
    padding: 8px 16px 32px;
  }}
</style>
</head>
<body>

<div class="header">
  <div class="header-left">
    <h1>ARIA</h1>
    <p>{now.strftime("%Y.%m.%d %H:%M KST")}</p>
  </div>
  <div class="regime-badge" style="background:{regime_color};">
    {"위험선호" if "선호" in regime else "위험회피" if "회피" in regime else "혼조"}
  </div>
</div>

<div class="section">

  <!-- 요약 -->
  {'<div class="summary-card"><p class="summary-text">💡 ' + summary + '</p></div>' if summary else ''}

  <!-- 핵심 지표 -->
  <div class="metrics">
    <div class="metric">
      <div class="metric-label">감정지수</div>
      <div class="metric-value" style="color:{sent_color};">{sent_score}</div>
      <div class="metric-sub">{sent_emoji} {sent_level}</div>
    </div>
    <div class="metric">
      <div class="metric-label">종합 / 방향 정확도</div>
      <div class="metric-value">{acc_pct}%</div>
      <div class="metric-sub">방향 {dir_pct}% · {acc_correct}/{acc_total}건</div>
    </div>
  </div>

  <!-- 감정지수 추이 -->
  <div class="card">
    <div class="card-title">감정지수 30일 추이</div>
    <div class="legend">
      <span class="legend-item"><span class="legend-dot" style="background:#1D9E75;"></span>감정지수</span>
      <span class="legend-item"><span class="legend-dot" style="background:#888780; border-radius:0;height:2px;"></span>중립(50)</span>
    </div>
    <div class="chart-wrap" style="height:160px;">
      <canvas id="sentChart" role="img" aria-label="30일 감정지수 추이">감정지수 {sent_score} / {sent_level}</canvas>
    </div>
  </div>

  <!-- 예측 정확도 -->
  {'<div class="card"><div class="card-title">카테고리별 예측 정확도</div><div class="chart-wrap" style="height:' + str(max(120, len(cat_labels)*36)) + 'px;"><canvas id="accChart" role="img" aria-label="카테고리별 예측 정확도">정확도 차트</canvas></div></div>' if cat_labels else ''}

  <!-- 패턴 통계 -->
  {'<div class="card"><div class="card-title">레짐 전환 패턴</div>' + ''.join(['<div class="cost-row"><span style="font-size:12px;color:var(--text);">' + p + '</span></div>' for p in pat_summary]) + ('<div class="cost-row" style="margin-top:6px;"><span>블랙스완 전례</span><span class="cost-val">' + str(blackswan.get("reversal_count",0)) + '회 (평균 ' + str(blackswan.get("avg_streak_before_reversal",0)) + '일 연속 후 반전)</span></div>' if blackswan.get("reversal_count",0) > 0 else '') + '</div>' if pat_summary else ''}

  <!-- 포트폴리오 손익 -->
  <div class="card">
    <div class="card-title">포트폴리오 오늘 손익</div>
    {''.join([
      f'<div class="port-row"><span class="port-name">{n}</span><span class="port-chg {"pos" if v > 0 else "neg" if v < 0 else "neu"}">'
      + ('+' if v > 0 else '') + str(v) + '%</span></div>'
      for n, v in zip(port_names, port_values)
    ]) if port_names else '<p style="font-size:13px;color:var(--muted);">데이터 없음</p>'}
  </div>

  <!-- 섹터 로테이션 -->
  {'<div class="card"><div class="card-title">섹터 자금흐름 (30일 누적)</div><div class="chart-wrap" style="height:' + str(max(120, len(rot_labels)*32)) + 'px;"><canvas id="rotChart" role="img" aria-label="섹터 자금흐름">섹터 로테이션</canvas></div></div>' if rot_labels else ''}

  <!-- 이번 달 비용 -->
  <div class="card">
    <div class="card-title">이번 달 API 비용</div>
    <div class="cost-row">
      <span>추정 비용</span>
      <span class="cost-val">${cost_usd:.2f} (약 {cost_krw:,}원)</span>
    </div>
    <div class="cost-row">
      <span>실행 횟수</span>
      <span class="cost-val">{month_cost.get("runs", 0)}회</span>
    </div>
  </div>

</div>

<div class="footer">
  분석일: {analysis_date} · ARIA Multi-Agent
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script>
const isDark = matchMedia('(prefers-color-scheme: dark)').matches;
const tc  = isDark ? 'rgba(255,255,255,0.45)' : 'rgba(0,0,0,0.40)';
const gc  = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';
const bOpts = {{
  responsive:true, maintainAspectRatio:false,
  plugins:{{legend:{{display:false}},tooltip:{{mode:'index',intersect:false}}}},
  scales:{{
    x:{{ticks:{{color:tc,font:{{size:10}}}},grid:{{color:gc}}}},
    y:{{ticks:{{color:tc,font:{{size:10}}}},grid:{{color:gc}}}}
  }}
}};

// 감정지수
const sentEl = document.getElementById('sentChart');
if(sentEl) new Chart(sentEl, {{
  type:'line',
  data:{{
    labels:{_j(sent_dates)},
    datasets:[
      {{data:{_j(sent_scores)},borderColor:'#1D9E75',backgroundColor:'rgba(29,158,117,0.08)',
        fill:true,tension:0.4,pointRadius:2,borderWidth:2}},
      {{data:Array({len(sent_scores)}).fill(50),borderColor:'#888780',
        borderDash:[4,3],borderWidth:1.5,pointRadius:0}}
    ]
  }},
  options:{{...bOpts,scales:{{
    x:{{ticks:{{color:tc,font:{{size:9}},maxRotation:0,autoSkip:true,maxTicksLimit:6}},grid:{{color:gc}}}},
    y:{{min:0,max:100,ticks:{{color:tc,font:{{size:10}},stepSize:25}},grid:{{color:gc}}}}
  }}}}
}});

// 정확도
const accEl = document.getElementById('accChart');
if(accEl) new Chart(accEl, {{
  type:'bar',
  data:{{
    labels:{_j(cat_labels)},
    datasets:[{{
      data:{_j(cat_pcts)},
      backgroundColor:{_j(cat_pcts)}.map(v => v>=70?'#1D9E75':v<=40?'#E24B4A':'#378ADD'),
      borderRadius:4,barPercentage:0.6
    }}]
  }},
  options:{{...bOpts,scales:{{
    x:{{ticks:{{color:tc,font:{{size:10}}}},grid:{{display:false}}}},
    y:{{min:0,max:100,ticks:{{color:tc,font:{{size:10}},callback:v=>v+'%'}},grid:{{color:gc}}}}
  }}}}
}});

// 섹터 로테이션
const rotEl = document.getElementById('rotChart');
if(rotEl) new Chart(rotEl, {{
  type:'bar',
  data:{{
    labels:{_j(rot_labels)},
    datasets:[{{
      data:{_j(rot_values)},
      backgroundColor:{_j(rot_values)}.map(v=>v>=0?'#1D9E75':'#E24B4A'),
      borderRadius:3,barPercentage:0.65
    }}]
  }},
  options:{{
    indexAxis:'y',responsive:true,maintainAspectRatio:false,
    plugins:{{legend:{{display:false}}}},
    scales:{{
      x:{{ticks:{{color:tc,font:{{size:9}}}},grid:{{color:gc}}}},
      y:{{ticks:{{color:tc,font:{{size:10}}}},grid:{{display:false}}}}
    }}
  }}
}});
</script>
</body>
</html>"""

    OUTPUT_FILE.write_text(html, encoding="utf-8")
    print("dashboard.html 생성 완료: " + str(OUTPUT_FILE))
    return html


if __name__ == "__main__":
    build_dashboard()
    print("완료")
