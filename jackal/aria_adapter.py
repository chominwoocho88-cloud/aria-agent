"""
aria_adapter.py — ARIA ↔ Jackal 인터페이스 레이어

역할:
  - ARIA 데이터 로딩의 단일 진입점
  - 경로/구조 변경은 이 파일만 수정
  - Jackal 모듈은 이 adapter만 의존
"""

import json
import logging
from pathlib import Path

log = logging.getLogger("aria_adapter")

# ── 경로 — 항상 repo root 기준 절대경로 ──────────────────────────
_JACKAL_DIR = Path(__file__).parent   # jackal/
_REPO_ROOT  = _JACKAL_DIR.parent      # repo root
DATA_DIR    = _REPO_ROOT / "data"

ARIA_BASELINE = DATA_DIR / "morning_baseline.json"
ARIA_MEMORY   = DATA_DIR / "memory.json"
JACKAL_NEWS   = DATA_DIR / "jackal_news.json"


def load_aria_context() -> dict:
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
    }
    try:
        if ARIA_BASELINE.exists():
            b = json.loads(ARIA_BASELINE.read_text(encoding="utf-8"))
            ctx["one_line"]       = b.get("one_line_summary", "")
            ctx["regime"]         = b.get("market_regime", "")
            ctx["top_headlines"]  = [h.get("headline", "") for h in b.get("top_headlines", [])[:5]]
            ctx["key_inflows"]    = [i.get("zone", "") for i in b.get("inflows", [])[:3]]
            ctx["key_outflows"]   = [o.get("zone", "") for o in b.get("outflows", [])[:3]]
            ctx["thesis_killers"] = b.get("thesis_killers", [])
            ctx["actionable"]     = b.get("actionable_watch", [])[:5]
    except Exception as e:
        log.warning(f"ARIA baseline 로드 실패: {e}")
    try:
        if ARIA_MEMORY.exists():
            mem = json.loads(ARIA_MEMORY.read_text(encoding="utf-8"))
            if mem:
                last = sorted(mem, key=lambda x: x.get("analysis_date", ""))[-1]
                ctx["all_headlines"]   = last.get("top_headlines", [])[:8]
                ctx["inflows_detail"]  = last.get("inflows", [])[:4]
                ctx["outflows_detail"] = last.get("outflows", [])[:3]
                if not ctx["regime"]:
                    ctx["regime"] = last.get("market_regime", "")
                if not ctx["top_headlines"]:
                    ctx["top_headlines"] = [h.get("headline", "") for h in ctx["all_headlines"]]
                if not ctx["key_inflows"]:
                    ctx["key_inflows"] = [i.get("zone", "") for i in ctx["inflows_detail"][:3]]
    except Exception as e:
        log.warning(f"ARIA memory 로드 실패: {e}")
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


def aria_baseline_exists() -> bool:
    return ARIA_BASELINE.exists()


def get_aria_regime() -> str:
    try:
        if ARIA_BASELINE.exists():
            b = json.loads(ARIA_BASELINE.read_text(encoding="utf-8"))
            return b.get("market_regime", "")
    except Exception:
        pass
    return ""


def get_aria_inflows() -> list:
    try:
        if ARIA_BASELINE.exists():
            b = json.loads(ARIA_BASELINE.read_text(encoding="utf-8"))
            return [i.get("zone", "") for i in b.get("inflows", [])[:3]]
    except Exception:
        pass
    return []
