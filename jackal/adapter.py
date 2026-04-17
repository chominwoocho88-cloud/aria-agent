"""
adapter.py — ORCA ↔ JACKAL 인터페이스 레이어
"""
import json
import logging
from pathlib import Path

from orca.state import load_latest_jackal_weight_snapshot

log = logging.getLogger("jackal_adapter")

# ── 경로 — 항상 repo root 기준 절대경로 ──────────────────────────
_JACKAL_DIR = Path(__file__).parent   # jackal/
_REPO_ROOT  = _JACKAL_DIR.parent      # repo root
DATA_DIR    = _REPO_ROOT / "data"

ORCA_BASELINE = DATA_DIR / "morning_baseline.json"
ORCA_MEMORY   = DATA_DIR / "memory.json"
JACKAL_NEWS   = DATA_DIR / "jackal_news.json"
_JACKAL_WEIGHTS = _JACKAL_DIR / "jackal_weights.json"


# ══════════════════════════════════════════════════════════════════
# Fallback 레짐 — baseline 없을 때 대체 판단
# ══════════════════════════════════════════════════════════════════

def _get_fallback_regime() -> str:
    """
    morning_baseline.json 없을 때 대체 레짐 결정.

    우선순위:
      1. jackal_weights.json의 last_macro_gate (가장 최신 거시 지표)
      2. data/memory.json 최신 MORNING 리포트 레짐
      3. 기본값 "혼조"

    Jackal이 ARIA baseline 없이도 Universe 필터링 가능.
    """
    # 1순위: last_macro_gate (Hunter가 매 실행마다 업데이트)
    snapshot = load_latest_jackal_weight_snapshot()
    if isinstance(snapshot, dict):
        try:
            gate = snapshot.get("last_macro_gate", {})
            risk = gate.get("risk_level", "")
            if risk == "extreme":
                log.info("  Fallback 레짐: macro_gate=extreme → 위험회피")
                return "위험회피"
            elif risk == "elevated":
                log.info("  Fallback 레짐: macro_gate=elevated → 위험중립")
                return "위험중립"
            elif risk == "normal":
                vix = gate.get("vix", 20)
                regime = "위험선호" if vix < 18 else "중립"
                log.info(f"  Fallback 레짐: snapshot macro_gate vix={vix} → {regime}")
                return regime
        except Exception as e:
            log.debug(f"  snapshot macro_gate 읽기 실패: {e}")

    if _JACKAL_WEIGHTS.exists():
        try:
            w    = json.loads(_JACKAL_WEIGHTS.read_text(encoding="utf-8"))
            gate = w.get("last_macro_gate", {})
            risk = gate.get("risk_level", "")
            if risk == "extreme":
                log.info("  Fallback 레짐: macro_gate=extreme → 위험회피")
                return "위험회피"
            elif risk == "elevated":
                log.info("  Fallback 레짐: macro_gate=elevated → 전환중")
                return "전환중"
            elif risk == "normal":
                # VIX 수치 기반 세분화
                vix = gate.get("vix", 20)
                regime = "위험선호" if vix < 18 else "혼조"
                log.info(f"  Fallback 레짐: macro_gate=normal vix={vix} → {regime}")
                return regime
        except Exception as e:
            log.debug(f"  macro_gate 읽기 실패: {e}")

    # 2순위: memory.json 최신 MORNING 레짐
    if ORCA_MEMORY.exists():
        try:
            mem = json.loads(ORCA_MEMORY.read_text(encoding="utf-8"))
            morning = [m for m in mem if m.get("mode") == "MORNING"]
            if morning:
                last   = sorted(morning, key=lambda x: x.get("analysis_date", ""))[-1]
                regime = last.get("market_regime", "")
                date   = last.get("analysis_date", "")
                if regime:
                    log.info(f"  Fallback 레짐: memory 최신 ({date}) → {regime[:20]}")
                    return regime
        except Exception as e:
            log.debug(f"  memory 읽기 실패: {e}")

    log.info("  Fallback 레짐: 기본값 → 혼조")
    return "혼조"


def load_orca_context() -> dict:
    ctx: dict = {
        "one_line":        "",
        "regime":          "",
        "top_headlines":   [],
        "key_inflows":     [],
        "key_outflows":    [],
        "thesis_killers":  [],
        "actionable":      [],
        "inflows_detail":  [],
        "outflows_detail": [],
        "all_headlines":   [],
        "jackal_news":     {},
        "regime_source":   "none",   # 레짐 출처 추적 (baseline/memory/fallback)
    }
    try:
        if ORCA_BASELINE.exists():
            b = json.loads(ORCA_BASELINE.read_text(encoding="utf-8"))
            ctx["one_line"]       = b.get("one_line_summary", "")
            ctx["regime"]         = b.get("market_regime", "")
            ctx["top_headlines"]  = [h.get("headline", "") for h in b.get("top_headlines", [])[:5]]
            ctx["key_inflows"]    = [i.get("zone", "") for i in b.get("inflows", [])[:3]]
            ctx["key_outflows"]   = [o.get("zone", "") for o in b.get("outflows", [])[:3]]
            ctx["thesis_killers"] = b.get("thesis_killers", [])
            ctx["actionable"]     = b.get("actionable_watch", [])[:5]
            if ctx["regime"]:
                ctx["regime_source"] = "baseline"
    except Exception as e:
        log.warning(f"ARIA baseline 로드 실패: {e}")
    try:
        if ORCA_MEMORY.exists():
            mem = json.loads(ORCA_MEMORY.read_text(encoding="utf-8"))
            if mem:
                last = sorted(mem, key=lambda x: x.get("analysis_date", ""))[-1]
                ctx["all_headlines"]   = last.get("top_headlines", [])[:8]
                ctx["inflows_detail"]  = last.get("inflows", [])[:4]
                ctx["outflows_detail"] = last.get("outflows", [])[:3]
                if not ctx["regime"]:
                    ctx["regime"] = last.get("market_regime", "")
                    if ctx["regime"]:
                        ctx["regime_source"] = "memory"
                if not ctx["top_headlines"]:
                    ctx["top_headlines"] = [h.get("headline", "") for h in ctx["all_headlines"]]
                if not ctx["key_inflows"]:
                    ctx["key_inflows"] = [i.get("zone", "") for i in ctx["inflows_detail"][:3]]
    except Exception as e:
        log.warning(f"ARIA memory 로드 실패: {e}")

    # ── Fallback: baseline + memory 모두 실패 시 거시 지표 기반 레짐 추정 ──
    if not ctx["regime"]:
        ctx["regime"]        = _get_fallback_regime()
        ctx["regime_source"] = "fallback"
        log.warning(
            f"⚠️  ARIA baseline 없음 — fallback 레짐 사용: "
            f"'{ctx['regime']}'"
        )

    try:
        if JACKAL_NEWS.exists():
            jn = json.loads(JACKAL_NEWS.read_text(encoding="utf-8"))
            for item in jn.get("news_items", []):
                t = item.get("ticker", "")
                if t:
                    ctx["jackal_news"].setdefault(t, []).append(item)
    except Exception:
        pass
    return ctx


def orca_baseline_exists() -> bool:
    """
    baseline 파일 존재 여부.
    주의: False여도 run_hunt()는 fallback 레짐으로 동작 가능.
    Jackal을 완전 중단시키려면 이 함수 대신 ctx["regime_source"] 확인 권장.
    """
    return ORCA_BASELINE.exists()


def get_orca_regime() -> str:
    try:
        if ORCA_BASELINE.exists():
            b = json.loads(ORCA_BASELINE.read_text(encoding="utf-8"))
            return b.get("market_regime", "")
    except Exception:
        pass
    return ""


def get_orca_inflows() -> list:
    try:
        if ORCA_BASELINE.exists():
            b = json.loads(ORCA_BASELINE.read_text(encoding="utf-8"))
            return [i.get("zone", "") for i in b.get("inflows", [])[:3]]
    except Exception:
        pass
    return []



