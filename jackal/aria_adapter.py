"""aria_dashboard.py — ARIA Dashboard v4.1 (Dark Warm Fix)"""
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

# ── Jackal 경로 (aria_paths에 없으므로 직접 정의) ─────────────────
HUNT_LOG_FILE      = Path("jackal") / "hunt_log.json"
JACKAL_WEIGHTS_FILE = Path("jackal") / "jackal_weights.json"

def _load(path, default=None):
    if path.exists():
        try: return json.loads(path.read_text(encoding="utf-8"))
        except: pass
    return default or {}

def _j(v): return json.dumps(v, ensure_ascii=False)
def _e(s): return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
def _trim(s, n): t=str(s); return _e(t[:n]+("…" if len(t)>n else ""))

def build_dashboard():
    now    = datetime.now(KST)
    sent   = _load(SENTIMENT_FILE, {"history":[],"current":{}})
    acc    = _load(ACCURACY_FILE,  {"total":0,"correct":0,"by_category":{}})
    rot    = _load(ROTATION_FILE,  {"ranking":[]})
    memory = _load(MEMORY_FILE,    [])
    cost   = _load(COST_FILE,      {})
    pat    = _load(PATTERN_DB_FILE,{})
    mkt    = _load(DATA_FILE,      {})

    # ── Jackal 데이터 ────────────────────────────────────────────
    hunt_log = _load(HUNT_LOG_FILE, [])
    jweights = _load(JACKAL_WEIGHTS_FILE, {})

    cur    = sent.get("current",{})
    h30    = sent.get("history",[])[-30:]
    latest = memory[-1] if isinstance(memory,list) and memory else {}

    sd = [h["date"][5:] for h in h30]; ss = [h["score"] for h in h30]
    sc = cur.get("score",50); sl = cur.get("level","중립"); se = cur.get("emoji","😐")

    at = acc.get("total",0); ac2 = acc.get("correct",0)
    ap = round(ac2/at*100,1) if at>0 else 0; adp = acc.get("dir_accuracy_pct",0)
    bcat = acc.get("by_category",{})

    rnk = rot.get("ranking",[])
    rl8 = [r[0] for r in rnk][:8]; rv8 = [r[1] for r in rnk][:8]
    ti  = rnk[0][0] if rnk and rnk[0][1]>0 else "—"

    regime  = latest.get("market_regime","")
    conf    = latest.get("confidence_overall","")
    summ    = latest.get("one_line_summary","")
    trend   = latest.get("trend_phase","")
    strat   = latest.get("trend_strategy",{})
    rec     = strat.get("recommended","") if isinstance(strat,dict) else ""
    caut    = strat.get("caution","")     if isinstance(strat,dict) else ""
    adt     = latest.get("analysis_date","")
    cargs   = latest.get("counterarguments",[])
    dc      = len(cargs)
    mrisk   = cargs[0].get("against","—") if cargs else "—"
    psm     = pat.get("summary",[])[:4]; bsw=pat.get("blackswan",{})

    port_rows = []
    if PORTFOLIO_FILE.exists():
        try:
            hs = json.loads(PORTFOLIO_FILE.read_text(encoding="utf-8")).get("holdings",[])
            for h in hs:
                if h.get("ticker")=="cash": continue
                cs = mkt.get(str(h.get("ticker",""))+"_change","0%")
                try: cv=float(str(cs).replace("%","").replace("+",""))
                except: cv=0.0
                port_rows.append((h.get("name",""),cv))
        except: pass

    mk2=now.strftime("%Y-%m"); mc=cost.get("monthly_runs",{}).get(mk2,{})
    cu=mc.get("estimated_usd",0.0); ck=round(cu*1480); cr=mc.get("runs",0)

    # 시장 데이터
    vix_val = mkt.get("vix","")
    pcr_val = mkt.get("pcr_avg")
    pcrs    = mkt.get("pcr_signal","")
    hy_val  = mkt.get("fred_hy_spread")
    rrp_val = mkt.get("fred_rrp")
    dxy_val = mkt.get("fred_dxy")
    fg_val  = mkt.get("fear_greed_value","")

    # ── 색상 헬퍼
    def _rc(r):
        if "선호" in r: return "#14E87A"
        if "회피" in r: return "#FF5252"
        return "#FFB547"
    def _rl(r):
        if "선호" in r: return "위험선호"
        if "회피" in r: return "위험회피"
        return "혼조"
    def _sc_col(s):
        if s<=25: return "#FF5252"
        if s<=45: return "#FFB547"
        if s<=60: return "#909090"
        return "#14E87A"
    def _cfg(c):
        return {"높음":"#14E87A","보통":"#FFB547","낮음":"#FF5252"}.get(c,"#909090")
    def _cbg(c):
        return {"높음":"rgba(20,232,122,.15)","보통":"rgba(255,181,71,.15)","낮음":"rgba(255,82,82,.15)"}.get(c,"rgba(144,144,144,.1)")

    rco=_rc(regime); rla=_rl(regime); sco=_sc_col(sc)
    cfg3=_cfg(conf); cbg=_cbg(conf)

    # KPI 클래스
    kpi_s = "gr" if sc>60 else "am" if sc>40 else "rd"
    kpi_a = "gr" if ap>=55 else "am" if ap>=45 else "rd"

    # ── 자금흐름 블록
    def flow_block(direction):
        if direction=="in":
            items=[(r[0],r[1]) for r in rnk if r[1]>0][:3]
        else:
            items=[(r[0],r[1]) for r in reversed(rnk) if r[1]<0][:3]
        if not items:
            return '<div class="fe">데이터 없음</div>'
        o=""
        for lbl,val in items:
            col="#14E87A" if val>=0 else "#FF5252"
            bw=min(abs(val)*12,80)
            sign="+" if val>=0 else ""
            # 섹터명 5글자 제한 (/ 이전만)
            short = lbl.split("/")[0][:5]
            o+=f'<div class="frow"><div class="fn">{_e(short)}</div><div class="fbg"><div class="fb" style="width:{bw}%;background:{col};"></div></div><span class="fv" style="color:{col};">{sign}{val}</span></div>'
        return o

    # ── 정확도 카테고리
    def acc_block():
        if not bcat: return '<div class="fe">카테고리 없음</div>'
        o=""
        for lbl,v in bcat.items():
            t=v.get("total",0); c=v.get("correct",0)
            p=round(c/t*100) if t>0 else 0
            col="#14E87A" if p>=60 else "#FF5252" if p<40 else "#FFB547"
            o+=f'<div class="arow"><div class="am2"><div class="albl">{_e(lbl)}</div><div class="asub">{c}/{t}건</div></div><div class="abg"><div class="afill" style="width:{p}%;background:{col};"></div></div><span class="apct" style="color:{col};">{p}%</span></div>'
        return o

    # ── 리스크 판단
    def risk_block():
        items=[]
        if pcr_val:
            try:
                p=float(pcr_val)
                if p>=1.2: items.append(("PCR 극단공포",f"PCR {p} · 헤지 수요 폭발","#FF5252"))
                elif p>=1.0: items.append(("PCR 공포",f"PCR {p} · 풋옵션 우세","#FFB547"))
            except: pass
        try:
            v=float(str(vix_val))
            if v>=25: items.append(("VIX 경고",f"VIX {v} · 변동성 급등","#FF5252"))
            elif v>=20: items.append(("VIX 주의",f"VIX {v} · 경계선 진입","#FFB547"))
        except: pass
        if hy_val:
            try:
                h=float(hy_val)
                if h>=4.0: items.append(("신용위험",f"HY스프레드 {h}% · 리스크오프","#FF5252"))
            except: pass
        if not items: items=[("특이 리스크 없음","주요 지표 정상 범위","#14E87A")]
        o=""
        for title,desc,col in items[:2]:
            o+=f'<div class="ritem" style="border-left-color:{col};"><div class="rtitle" style="color:{col};">⬤ {_e(title)}</div><div class="rdesc">{_e(desc[:70])}</div></div>'
        return o

    # ── 거시지표 칩 (개별 div로 안전하게)
    mchips_html = ""
    if pcr_val is not None:
        try:
            p = float(pcr_val)
            pcr_col = "#FF5252" if p>=1.0 else "#14E87A"
            mchips_html += f'<div class="mchip"><div class="mlbl">PCR</div><div class="mval" style="color:{pcr_col};">{p}</div><div class="msub">{_e(pcrs)}</div></div>'
        except: pass
    if rrp_val is not None:
        mchips_html += f'<div class="mchip"><div class="mlbl">역레포</div><div class="mval">{rrp_val}조$</div></div>'
    if dxy_val is not None:
        mchips_html += f'<div class="mchip"><div class="mlbl">달러지수</div><div class="mval">{dxy_val}</div></div>'
    if hy_val is not None:
        try:
            h=float(hy_val)
            hcol="#FFB547" if h>=3.5 else "#909090"
            mchips_html += f'<div class="mchip"><div class="mlbl">HY스프레드</div><div class="mval" style="color:{hcol};">{hy_val}%</div></div>'
        except: pass
    if fg_val:
        mchips_html += f'<div class="mchip"><div class="mlbl">공포탐욕</div><div class="mval">{_e(str(fg_val))}</div></div>'

    # ── 패턴 칩
    def pat_block():
        if not psm: return '<div class="fe">데이터 없음</div>'
        o='<div class="patg">'
        for p in psm:
            pts=p.split("→"); lbl=pts[0].strip(); pr=pts[1].strip() if len(pts)>1 else ""
            o+=f'<div class="pchip"><div class="plbl">{_e(lbl)}</div>'
            if pr: o+=f'<div class="ppr">{_e(pr)}</div>'
            o+='</div>'
        if bsw.get("reversal_count",0)>0:
            cnt=bsw["reversal_count"]; avg=bsw.get("avg_streak_before_reversal",0)
            o+=f'<div class="pchip pswan"><div class="plbl">🦢 블랙스완</div><div class="ppr">{cnt}회 · 평균 {avg}일</div></div>'
        o+='</div>'
        return o

    # ── Jackal Hunter 섹션 ──────────────────────────────────────
    def jackal_block():
        """hunt_log.json 기반 최근 5건 타점 행 + 통계 생성."""
        if not hunt_log:
            return (
                '<div style="padding:16px;text-align:center;color:var(--mu);font-size:12px;">'
                '🦊 Jackal 미실행 — hunt_log.json 없음</div>',
                "", ""
            )

        # 최신순 정렬, 최대 5건
        recent = sorted(hunt_log, key=lambda e: e.get("timestamp",""), reverse=True)[:5]

        rows_html = ""
        for e in recent:
            ticker = _e(e.get("ticker", "—"))
            name   = _e(e.get("name", e.get("ticker", ""))[:12])
            score  = e.get("final_score") or e.get("analyst_score") or 0
            try: score = float(score)
            except: score = 0.0
            is_entry = bool(e.get("is_entry") or e.get("alerted"))

            # 점수 색상
            sc_col = "#14E87A" if score>=65 else "#FFB547" if score>=50 else "#909090"

            # 뱃지
            if is_entry:
                badge_cls = "jb-entry"; badge_txt = "🔥 타점"
            else:
                badge_cls = "jb-pass";  badge_txt = "⚪ 관망"

            # 메타 정보
            rsi    = e.get("rsi")
            chg5   = e.get("change_5d")
            peak   = e.get("peak_pct")
            peak_d = e.get("peak_day")
            ts     = (e.get("timestamp",""))[5:10].replace("-","/")
            div    = "★" if ("rsi_divergence" in (e.get("signals_fired") or []) or
                             e.get("bullish_div")) else ""

            meta_parts = []
            if rsi   is not None: meta_parts.append(f"RSI {rsi:.0f}")
            if chg5  is not None:
                meta_parts.append(f'<span class="{"neg" if chg5<0 else "pos"}">'
                                  f'{chg5:+.1f}%</span>')
            if peak  is not None: meta_parts.append(f'<span class="pos">Peak+{peak:.1f}% D{peak_d}</span>')
            if ts: meta_parts.append(ts)
            meta_html = " · ".join(meta_parts)

            rows_html += (
                f'<div class="jrow">'
                f'<span class="jtk">{ticker}</span>'
                f'<div style="flex:1;min-width:0;">'
                f'<div class="jname">{name}{div}</div>'
                f'<div class="jmeta">{meta_html}</div>'
                f'</div>'
                f'<span class="jscore" style="color:{sc_col};">{score:.0f}</span>'
                f'<span class="jbadge {badge_cls}">{badge_txt}</span>'
                f'</div>'
            )

        # 통계
        sw = jweights.get("swing_accuracy")
        d1 = jweights.get("d1_accuracy")
        n  = jweights.get("total_tracked")
        stat_html = (
            f'<div class="jstat">'
            f'<span class="js">스윙 <b>{sw:.1f}%</b></span>'
            f'<span class="js">1일 <b>{d1:.1f}%</b></span>'
            f'<span class="js">추적 <b>{n}건</b></span>'
            f'</div>'
        ) if (sw is not None and d1 is not None and n is not None) else ""

        # Macro Gate 배지
        mg = jweights.get("last_macro_gate", {})
        if mg:
            lvl = mg.get("risk_level","normal")
            gate_cls = ("jgate-danger" if lvl=="extreme"
                        else "jgate-warn" if lvl=="elevated"
                        else "jgate-ok")
            gate_icon = "🚨" if lvl=="extreme" else "⚠️" if lvl=="elevated" else "✅"
            vix_v  = mg.get("vix","—")
            yc_v   = mg.get("yield_curve")
            hy_v   = mg.get("hy_chg5")
            yc_str = f"{yc_v:+.2f}%" if yc_v is not None else "—"
            hy_str = f"{hy_v:+.1f}%" if hy_v is not None else "—"
            reason = _e(mg.get("reason","")[:40])
            gate_html = (
                f'<div class="jgate {gate_cls}">'
                f'{gate_icon} Macro Gate &nbsp;|&nbsp; '
                f'VIX <b>{vix_v}</b> · YC <b>{yc_str}</b> · HY <b>{hy_str}</b>'
                f'</div>'
            )
        else:
            gate_html = ""

        return rows_html, stat_html, gate_html

    j_rows, j_stat, j_gate = jackal_block()

    # ── 포트폴리오
    # ── 포트폴리오
    if port_rows:
        port_html=""
        for nm,cv in port_rows:
            cls="pos" if cv>0 else "neg" if cv<0 else "neu"; sign="+" if cv>0 else ""
            port_html+=f'<div class="prow"><span class="pname">{_e(nm)}</span><span class="pval {cls}">{sign}{cv}%</span></div>'
    else:
        port_html='<div class="pempty"><div class="pe-icon">📊</div><div class="pe-txt">포트폴리오 미연동</div><div class="pe-sub">연동 시 오늘 손익·비중 표시</div></div>'

    # ── 반론 강도
    devil_col = "#FF5252" if dc>=4 else "#FFB547" if dc>=2 else "#14E87A"
    devil_txt = "강" if dc>=4 else "보통" if dc>=2 else "약"

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<title>ARIA</title>
<style>
/* ─── 팔레트: 화이트 ─── */
:root{{
  --bg:  #F2F1ED;   /* 연한 웜 화이트 배경 */
  --s1:  #FFFFFF;   /* 카드 */
  --s2:  #F0EFEB;   /* 카드 2레벨 */
  --s3:  #E8E7E2;   /* 강조 배경 */
  --tx:  #18181A;   /* 메인 텍스트 */
  --mu:  #6A6A6D;   /* 뮤트 텍스트 */
  --gr:  #0DAE6B;
  --rd:  #E03030;
  --am:  #D98A00;
  --bd:  rgba(0,0,0,.08);
  --r:   16px;
}}
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0;}}
body{{
  font-family:-apple-system,'Helvetica Neue',sans-serif;
  background:var(--bg);color:var(--tx);
  max-width:430px;margin:0 auto;
  padding-bottom:calc(env(safe-area-inset-bottom)+44px);
  -webkit-font-smoothing:antialiased;
}}

/* ── 헤더 */
.hdr{{padding:20px 18px 0;display:flex;align-items:center;justify-content:space-between;}}
.brand{{font-size:11px;font-weight:800;letter-spacing:3.5px;color:var(--mu);text-transform:uppercase;}}
.hdr-time{{font-size:11px;color:var(--mu);margin-top:3px;}}
.regime-badge{{font-size:12px;font-weight:700;padding:7px 16px;border-radius:100px;color:#fff;letter-spacing:.3px;}}

/* ── 결론 패널 */
.sp{{margin:14px 16px 0;background:var(--s1);border-radius:var(--r);border:.5px solid var(--bd);overflow:hidden;}}
.sp-top{{padding:16px 18px 14px;}}
.sp-meta{{display:flex;gap:8px;align-items:center;margin-bottom:10px;flex-wrap:wrap;}}
.conf-pill{{font-size:11px;font-weight:700;padding:3px 10px;border-radius:100px;}}
.tag{{font-size:11px;color:var(--mu);background:var(--s3);padding:3px 9px;border-radius:6px;}}
.tag-devil{{font-size:11px;color:var(--am);background:rgba(255,181,71,.1);padding:3px 9px;border-radius:6px;border:.5px solid rgba(255,181,71,.2);}}
.summ{{font-size:14px;line-height:1.65;letter-spacing:-.1px;}}
.action-bar{{background:rgba(13,174,107,.08);border-top:.5px solid rgba(13,174,107,.2);padding:12px 18px;display:flex;gap:9px;align-items:flex-start;}}
.ai{{color:var(--gr);font-size:12px;flex-shrink:0;margin-top:2px;font-weight:700;}}
.at{{font-size:13px;line-height:1.55;color:var(--tx);}}

/* ── KPI */
.kw{{padding:12px 16px 0;}}
.kg{{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px;}}
.kpi{{
  background:var(--s1);border-radius:var(--r);border:.5px solid var(--bd);
  padding:15px 14px 13px;position:relative;overflow:hidden;
  min-width:0;   /* 중요: 그리드 오버플로우 방지 */
}}
.kpi::before{{content:'';position:absolute;top:0;left:0;right:0;height:2.5px;border-radius:var(--r) var(--r) 0 0;}}
.kpi.gr::before{{background:var(--gr);}} .kpi.rd::before{{background:var(--rd);}} .kpi.am::before{{background:var(--am);}}
.kl{{font-size:10px;font-weight:700;color:var(--mu);text-transform:uppercase;letter-spacing:.8px;margin-bottom:9px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
.kn{{font-size:32px;font-weight:800;letter-spacing:-2px;line-height:1;margin-bottom:5px;}}
.ks{{font-size:10px;color:var(--mu);line-height:1.35;word-break:break-all;}}
.km{{font-size:17px;font-weight:800;letter-spacing:-.3px;line-height:1.2;padding-top:2px;margin-bottom:5px;word-break:break-word;}}
.kr{{font-size:12px;font-weight:600;line-height:1.3;padding-top:2px;margin-bottom:5px;word-break:break-word;}}

/* ── 섹션 */
.sec{{padding:20px 16px 0;}}
.sh{{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;}}
.st{{font-size:11px;font-weight:700;color:var(--mu);text-transform:uppercase;letter-spacing:.8px;}}
.sn{{font-size:11px;color:var(--mu);opacity:.65;}}
.card{{background:var(--s1);border-radius:var(--r);border:.5px solid var(--bd);padding:16px;}}

/* ── 자금흐름 */
.fg{{display:grid;grid-template-columns:1fr 1fr;gap:10px;}}
.fc{{background:var(--s1);border-radius:var(--r);border:.5px solid var(--bd);padding:14px;min-width:0;}}
.fh{{display:flex;align-items:center;gap:7px;margin-bottom:11px;}}
.fd{{width:7px;height:7px;border-radius:50%;flex-shrink:0;}}
.fhl{{font-size:12px;font-weight:700;}}
.frow{{display:flex;align-items:center;gap:5px;padding:3.5px 0;}}
.fn{{font-size:11px;font-weight:500;width:44px;flex-shrink:0;line-height:1.2;overflow:hidden;white-space:nowrap;}}
.fbg{{flex:1;height:4px;background:var(--s3);border-radius:2px;overflow:hidden;}}
.fb{{height:100%;border-radius:2px;}}
.fv{{font-size:11px;font-weight:800;width:30px;text-align:right;flex-shrink:0;}}
.fe{{font-size:12px;color:var(--mu);padding:4px 0;}}

/* ── 차트 */
.cc{{background:var(--s1);border-radius:var(--r);border:.5px solid var(--bd);padding:16px 16px 12px;}}
.ch2{{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:12px;}}
.cht{{font-size:14px;font-weight:700;}}
.chs{{font-size:13px;color:var(--mu);}}
.cw{{position:relative;}}

/* ── 정확도 카테고리 */
.arow{{display:flex;align-items:center;gap:8px;padding:8px 0;border-bottom:.5px solid var(--bd);}}
.arow:last-child{{border-bottom:none;}}
.am2{{display:flex;flex-direction:column;width:52px;flex-shrink:0;}}
.albl{{font-size:12px;font-weight:700;}}
.asub{{font-size:10px;color:var(--mu);margin-top:2px;}}
.abg{{flex:1;height:5px;background:var(--s3);border-radius:3px;overflow:hidden;}}
.afill{{height:100%;border-radius:3px;}}
.apct{{font-size:13px;font-weight:800;width:38px;text-align:right;flex-shrink:0;}}

/* ── 시장상태 */
.sg{{display:grid;grid-template-columns:1fr 1fr;gap:8px;}}
.si{{background:var(--s2);border-radius:10px;padding:12px;border:.5px solid var(--bd);}}
.sl{{font-size:10px;font-weight:700;color:var(--mu);text-transform:uppercase;letter-spacing:.6px;margin-bottom:6px;}}
.sv{{font-size:15px;font-weight:800;line-height:1.2;}}

/* ── 거시지표 */
.mr{{display:flex;flex-wrap:wrap;gap:8px;}}
.mchip{{background:var(--s2);border-radius:10px;padding:10px 13px;border:.5px solid var(--bd);min-width:70px;}}
.mlbl{{font-size:10px;font-weight:700;color:var(--mu);text-transform:uppercase;letter-spacing:.4px;margin-bottom:4px;}}
.mval{{font-size:15px;font-weight:800;line-height:1.2;}}
.msub{{font-size:10px;color:var(--mu);margin-top:3px;}}

/* ── 리스크 */
.ritem{{background:var(--s1);border-radius:12px;padding:12px 14px;margin-bottom:8px;border:.5px solid var(--bd);border-left-width:3px;}}
.rtitle{{font-size:12px;font-weight:700;margin-bottom:4px;}}
.rdesc{{font-size:12px;color:var(--mu);line-height:1.45;}}
.cbar{{background:rgba(217,138,0,.07);border:.5px solid rgba(217,138,0,.2);border-radius:12px;padding:11px 16px;display:flex;gap:9px;align-items:flex-start;}}
.ci{{font-size:14px;flex-shrink:0;}}
.ct{{font-size:12px;line-height:1.55;color:var(--tx);}}

/* ── 패턴 */
.patg{{display:grid;grid-template-columns:1fr 1fr;gap:8px;}}
.pchip{{background:var(--s2);border-radius:10px;padding:11px 13px;border:.5px solid var(--bd);}}
.plbl{{font-size:12px;font-weight:600;margin-bottom:3px;line-height:1.3;}}
.ppr{{font-size:11px;color:var(--mu);}}
.pswan{{grid-column:span 2;background:rgba(255,82,82,.05);border-color:rgba(255,82,82,.15);}}

/* ── 포트폴리오 */
.prow{{display:flex;justify-content:space-between;align-items:center;padding:11px 0;border-bottom:.5px solid var(--bd);}}
.prow:last-child{{border-bottom:none;}}
.pname{{font-size:14px;font-weight:600;}}
.pval{{font-size:17px;font-weight:800;}}
.pempty{{display:flex;flex-direction:column;align-items:center;padding:24px;gap:6px;}}
.pe-icon{{font-size:26px;}}
.pe-txt{{font-size:14px;font-weight:600;color:var(--mu);}}
.pe-sub{{font-size:11px;color:var(--mu);opacity:.6;}}

/* ── 비용 */
.crow{{display:flex;justify-content:space-between;align-items:center;padding:9px 0;border-bottom:.5px solid var(--bd);font-size:13px;}}
.crow:last-child{{border-bottom:none;}}
.clbl{{color:var(--mu);}}
.cval{{font-weight:700;}}

/* ── 유틸 */
.pos{{color:var(--gr);}} .neg{{color:var(--rd);}} .neu{{color:var(--mu);}}
.footer{{text-align:center;font-size:11px;color:var(--mu);padding:20px 16px 8px;line-height:1.75;opacity:.6;}}
/* ── Jackal Hunter */
.jrow{{display:flex;align-items:center;padding:8px 0;border-bottom:.5px solid var(--bd);gap:8px;font-size:13px;}}
.jrow:last-child{{border-bottom:none;}}
.jtk{{font-weight:700;min-width:52px;font-size:12px;flex-shrink:0;}}
.jname{{font-weight:600;font-size:13px;}}
.jmeta{{font-size:10px;color:var(--mu);margin-top:2px;}}
.jscore{{font-weight:800;font-size:15px;min-width:36px;text-align:right;flex-shrink:0;}}
.jbadge{{font-size:10px;padding:3px 7px;border-radius:10px;font-weight:600;white-space:nowrap;flex-shrink:0;}}
.jb-entry{{background:rgba(20,232,122,.15);color:#0DAE6B;}}
.jb-pass{{background:rgba(100,100,110,.1);color:var(--mu);}}
.jstat{{display:flex;gap:12px;padding:10px 0 4px;border-top:.5px solid var(--bd);margin-top:4px;}}
.js{{font-size:11px;color:var(--mu);}}
.js b{{color:var(--tx);font-weight:700;}}
.jgate{{display:flex;align-items:center;gap:6px;padding:7px 12px;font-size:11px;border-radius:10px;margin-bottom:8px;}}
.jgate-ok{{background:rgba(20,232,122,.08);color:#0DAE6B;}}
.jgate-warn{{background:rgba(255,181,71,.1);color:#D4872A;}}
.jgate-danger{{background:rgba(255,82,82,.1);color:#E03030;}}
</style>
</head>
<body>

<!-- ① 헤더 -->
<div class="hdr">
  <div>
    <div class="brand">ARIA</div>
    <div class="hdr-time">{now.strftime("%Y.%m.%d %H:%M KST")}</div>
  </div>
  <div class="regime-badge" style="background:{rco};">{_e(rla)}</div>
</div>

<!-- ② 결론 패널 -->
<div class="sp">
  <div class="sp-top">
    <div class="sp-meta">
      {"" if not conf else f'<span class="conf-pill" style="background:{cbg};color:{cfg3};">신뢰도 {_e(conf)}</span>'}
      {"" if not trend else f'<span class="tag">{_e(trend)}</span>'}
      {"" if dc==0 else f'<span class="tag-devil">반론 {dc}개</span>'}
    </div>
    <p class="summ">{_e(summ) if summ else "분석 데이터 로딩 중"}</p>
  </div>
  {"" if not rec else f'<div class="action-bar"><span class="ai">▶</span><span class="at">{_e(rec[:130])}</span></div>'}
</div>

<!-- ③ KPI 4개 (라벨 짧게 고정) -->
<div class="kw">
  <div class="kg">
    <div class="kpi {kpi_s}">
      <div class="kl">감정지수</div>
      <div class="kn" style="color:{sco};">{sc}</div>
      <div class="ks">{se} {_e(sl)}</div>
    </div>
    <div class="kpi {kpi_a}">
      <div class="kl">정확도</div>
      <div class="kn">{ap}%</div>
      <div class="ks">방향 {adp}% · {ac2}/{at}건</div>
    </div>
  </div>
  <div class="kg">
    <div class="kpi gr">
      <div class="kl">유입 1위 섹터</div>
      <div class="km" style="color:var(--gr);">{_e(ti)}</div>
      <div class="ks">30일 누적 선두</div>
    </div>
    <div class="kpi am">
      <div class="kl">주요 리스크</div>
      <div class="kr" style="color:var(--am);">{_trim(mrisk,26)}</div>
      <div class="ks">Devil 분석</div>
    </div>
  </div>
</div>

<!-- ④ 자금 흐름 -->
<div class="sec">
  <div class="sh"><span class="st">자금 흐름</span><span class="sn">30일 누적</span></div>
  <div class="fg">
    <div class="fc">
      <div class="fh"><div class="fd" style="background:var(--gr);"></div><span class="fhl">유입</span></div>
      {flow_block("in")}
    </div>
    <div class="fc">
      <div class="fh"><div class="fd" style="background:var(--rd);"></div><span class="fhl">유출</span></div>
      {flow_block("out")}
    </div>
  </div>
</div>

<!-- ⑤ 감정지수 추이 -->
<div class="sec">
  <div class="sh"><span class="st">감정지수 추이</span><span class="sn">30일</span></div>
  <div class="cc">
    <div class="ch2"><span class="cht">현재 {sc} · {_e(sl)}</span><span class="chs">{se}</span></div>
    <div class="cw" style="height:125px;"><canvas id="sentChart"></canvas></div>
  </div>
</div>

<!-- ⑥ 정확도 상세 -->
<div class="sec">
  <div class="sh"><span class="st">예측 정확도</span><span class="sn">카테고리 · 샘플 수</span></div>
  <div class="card" style="padding:8px 16px;">{acc_block()}</div>
</div>

<!-- ⑦ 시장 상태 -->
<div class="sec">
  <div class="sh"><span class="st">시장 상태</span></div>
  <div class="card" style="padding:12px;">
    <div class="sg">
      <div class="si"><div class="sl">레짐</div><div class="sv" style="color:{rco};">{_e(rla)}</div></div>
      <div class="si"><div class="sl">추세</div><div class="sv">{_e(trend) if trend else "—"}</div></div>
      <div class="si"><div class="sl">확신도</div><div class="sv" style="color:{cfg3};">{_e(conf) if conf else "—"}</div></div>
      <div class="si"><div class="sl">반론 강도</div><div class="sv" style="color:{devil_col};">{devil_txt}({dc})</div></div>
    </div>
  </div>
</div>

<!-- ⑧ 거시지표 -->
{"" if not mchips_html else f'<div class="sec"><div class="sh"><span class="st">거시지표</span></div><div class="card" style="padding:12px;"><div class="mr">{mchips_html}</div></div></div>'}

<!-- ⑨ 경고 -->
<div class="sec">
  <div class="sh"><span class="st">오늘의 경고</span></div>
  {risk_block()}
  {"" if not caut else f'<div class="cbar"><span class="ci">⚠️</span><span class="ct">{_e(caut[:110])}</span></div>'}
</div>

<!-- ⑩ 섹터 자금흐름 차트 -->
{"" if not rl8 else f'<div class="sec"><div class="sh"><span class="st">섹터 자금흐름</span><span class="sn">30일 누적</span></div><div class="cc"><div class="cw" style="height:{max(130,len(rl8)*32)}px;"><canvas id="rotChart"></canvas></div></div></div>'}

<!-- ⑪ 레짐 전환 패턴 -->
{"" if not psm else f'<div class="sec"><div class="sh"><span class="st">레짐 전환 패턴</span></div><div class="card" style="padding:12px;">{pat_block()}</div></div>'}

<!-- ⑫ 포트폴리오 -->
<div class="sec">
  <div class="sh"><span class="st">포트폴리오</span><span class="sn">오늘 손익</span></div>
  <div class="card" style="padding:{'8px 16px' if port_rows else '0'};">{port_html}</div>
</div>


<!-- ⑬ Jackal Hunter 타점 -->
{"" if not hunt_log else f'''<div class="sec">
  <div class="sh"><span class="st">🦊 Jackal 스윙 타점</span><span class="sn">최근 알림 · 100→5단계</span></div>
  {j_gate}<div class="card" style="padding:8px 16px;">{j_rows}</div>{j_stat}
</div>'''}

<!-- ⑭ 비용 -->
<!-- ⑬ 비용 -->
<div class="sec">
  <div class="sh"><span class="st">이번 달 비용</span></div>
  <div class="card" style="padding:6px 16px;">
    <div class="crow"><span class="clbl">추정 비용</span><span class="cval">${cu:.2f} · 약 {ck:,}원</span></div>
    <div class="crow"><span class="clbl">실행 횟수</span><span class="cval">{cr}회</span></div>
  </div>
</div>

<div class="footer">
  분석일 {_e(adt)} · ARIA Multi-Agent v4.1<br>
  Yahoo Finance · FRED · FSC · FearGreedChart · PCR
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script>
const tc='rgba(0,0,0,.35)',gc='rgba(0,0,0,.06)';
const tip={{backgroundColor:'rgba(255,255,255,.97)',titleColor:'#18181A',bodyColor:'#6A6A6D',
  borderColor:'rgba(0,0,0,.1)',borderWidth:1,padding:10,cornerRadius:8}};
const sEl=document.getElementById('sentChart');
if(sEl)new Chart(sEl,{{type:'line',
  data:{{labels:{_j(sd)},datasets:[
    {{data:{_j(ss)},borderColor:'#0DAE6B',backgroundColor:'rgba(13,174,107,.08)',
     fill:true,tension:.4,pointRadius:0,pointHoverRadius:5,borderWidth:2.5}},
    {{data:Array({len(ss)}).fill(50),borderColor:'rgba(0,0,0,.2)',borderDash:[4,3],borderWidth:1.5,pointRadius:0}}
  ]}},
  options:{{responsive:true,maintainAspectRatio:false,
    plugins:{{legend:{{display:false}},tooltip:{{mode:'index',intersect:false,...tip}}}},
    scales:{{
      x:{{ticks:{{color:tc,font:{{size:9}},maxRotation:0,autoSkip:true,maxTicksLimit:6}},grid:{{color:gc}}}},
      y:{{min:0,max:100,ticks:{{color:tc,font:{{size:9}},stepSize:25}},grid:{{color:gc}}}}
    }}
  }}
}});
const rEl=document.getElementById('rotChart');
if(rEl)new Chart(rEl,{{type:'bar',
  data:{{labels:{_j(rl8)},datasets:[{{
    data:{_j(rv8)},
    backgroundColor:{_j(rv8)}.map(v=>v>=0?'rgba(20,232,122,.72)':'rgba(255,82,82,.68)'),
    borderRadius:4,barPercentage:.6
  }}]}},
  options:{{indexAxis:'y',responsive:true,maintainAspectRatio:false,
    plugins:{{legend:{{display:false}},tooltip:{{...tip}}}},
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
