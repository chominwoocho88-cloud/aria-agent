"""
JACKAL shield module.
Jackal Shield - 蹂댁븞 + 鍮꾩슜 ?먮룞 泥댄겕 ?쒖뒪??
[Bug Fix 2] _check_budget()??compact_log留??쎌뼱 ??API 鍮꾩슜 誘몄쭛怨????섏젙
  - jackal_usage_log.json ?좉퇋 ?꾩엯
  - log_usage() ?ы띁 異붽? (Hunter/Scanner/Evolution?먯꽌 ?몄텧)
  - _check_budget() / _detect_spike() 紐⑤몢 usage_log ?곗꽑 ?ъ슜

寃????ぉ:
  1. API ???몄텧 (.env, *.py, *.json, *.yml ?먯꽌 ?⑦꽩 ?먯깋)
  2. ?쇱씪 ?좏겙 ?덉궛 珥덇낵 ?щ? (usage_log 湲곕컲 ????API 鍮꾩슜)
  3. 鍮꾩젙???좏겙 湲됱쬆 媛먯? (?꾩씪 ?鍮?300% ?댁긽)
  4. skills/ ?붾젆?좊━ 鍮꾩젙???뚯씪 ?먯?
"""

import json
import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path

log = logging.getLogger("jackal_shield")

_BASE      = Path(__file__).parent
_REPO_ROOT = _BASE.parent   # repo root ??API ???ㅼ틪 踰붿쐞

# ??? ?ㅼ젙 ?????????????????????????????????????????????????????????
_DAILY_TOKEN_BUDGET = int(os.getenv("JACKAL_DAILY_BUDGET", "500000"))
_SPIKE_MULTIPLIER   = float(os.getenv("JACKAL_SPIKE_MULTIPLIER", "3.0"))
_SECRET_PATTERNS = [
    re.compile(r"sk-ant-[A-Za-z0-9\-_]{20,}", re.I),
    re.compile(r"sk-[A-Za-z0-9]{20,}", re.I),
    # [Fix] os.environ.get ?뺥깭???ㅼ젣 ??媛믪씠 ?꾨땲誘濡??쒖쇅
    re.compile(r"ANTHROPIC_API_KEY\s*=\s*['\"][A-Za-z0-9\-_]{20,}['\"]", re.I),
    re.compile(r"api[_\-]?key\s*[:=]\s*['\"][A-Za-z0-9\-_]{20,}['\"]", re.I),
]
_EXCLUDE_DIRS    = {".git", "__pycache__", "node_modules", ".venv", "venv"}
_SCAN_EXTENSIONS = {".py", ".json", ".yml", ".yaml", ".env", ".txt", ".md"}

# [Fix] usage_log 寃쎈줈 異붽?
_USAGE_LOG = _BASE / "jackal_usage_log.json"


class JackalShield:
    """Run repository secret scans and lightweight JACKAL budget checks."""

    def __init__(self, scan_root: Path = _REPO_ROOT):
        self.scan_root  = Path(scan_root)
        self.compact_log = _BASE / "compact_log.json"

    # ?? 怨듦컻 硫붿꽌??????????????????????????????????????????????????
    def scan(self) -> dict:
        """
        ?꾩껜 ?ㅼ틪 ?ㅽ뻾.
        Returns: {issues, abort, stats}
        """
        issues = []
        stats  = {}

        # 1. API ???몄텧 ?ㅼ틪
        leaked = self._scan_secrets()
        for item in leaked:
            issues.append(f"?뵎 API???몄텧 ?섏떖: {item}")

        # 2. ?쇱씪 ?좏겙 ?덉궛 泥댄겕 (usage_log 湲곕컲)
        budget = self._check_budget()
        stats["today_tokens"]  = budget["today_tokens"]
        stats["daily_budget"]  = _DAILY_TOKEN_BUDGET
        stats["budget_source"] = budget["source"]
        if budget["exceeded"]:
            issues.append(
                f"?뮯 ?쇱씪 ?좏겙 ?덉궛 珥덇낵: "
                f"{budget['today_tokens']:,} / {_DAILY_TOKEN_BUDGET:,} "
                f"[{budget['source']}]"
            )

        # 3. ?좏겙 湲됱쬆 媛먯?
        spike = self._detect_spike()
        stats["spike_ratio"] = spike["ratio"]
        if spike["detected"]:
            issues.append(f"?뱢 ?좏겙 湲됱쬆 媛먯?: ?꾩씪 ?鍮?{spike['ratio']:.1f}諛?利앷?")

        # 4. skills/ ?댁긽 ?뚯씪 ?먯?
        for s in self._check_skills():
            issues.append(f"?좑툘  skills/ ?댁긽 ?뚯씪: {s}")

        # abort 議곌굔: API ???몄텧 OR ?덉궛 2諛?珥덇낵
        abort = bool(leaked) or budget["today_tokens"] > _DAILY_TOKEN_BUDGET * 2

        return {
            "issues":     issues,
            "abort":      abort,
            "stats":      stats,
            "scanned_at": datetime.now().isoformat(),
        }

    # ?? API ???몄텧 ?ㅼ틪 ???????????????????????????????????????????
    def _scan_secrets(self) -> list:
        found = []
        for path in self._iter_files():
            try:
                content = path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            for pattern in _SECRET_PATTERNS:
                if pattern.search(content):
                    rel = str(path.relative_to(self.scan_root))
                    if rel not in found:
                        found.append(rel)
                    break
        return found

    def _iter_files(self):
        for p in self.scan_root.rglob("*"):
            if any(excl in p.parts for excl in _EXCLUDE_DIRS):
                continue
            if p.suffix in _SCAN_EXTENSIONS and p.is_file():
                yield p

    # ?? ?좏겙 ?덉궛 泥댄겕 (Bug Fix: usage_log ?곗꽑) ??????????????????
    def _check_budget(self) -> dict:
        """
        [Fix] jackal_usage_log.json?먯꽌 ?ㅻ뒛 ??API ?좏겙 ?⑹궛.
        usage_log ?놁쑝硫?compact_log濡??대갚 (援щ쾭???명솚).
        """
        today = datetime.now().date().isoformat()

        # 1?쒖쐞: usage_log (?ㅼ젣 API ?좏겙)
        usage_logs = self._load_usage_log()
        if usage_logs:
            today_tokens = sum(
                e.get("total_tokens", 0)
                for e in usage_logs
                if e.get("timestamp", "")[:10] == today
            )
            return {
                "today_tokens": today_tokens,
                "exceeded":     today_tokens > _DAILY_TOKEN_BUDGET,
                "source":       "usage_log",
            }

        # ?대갚: compact_log
        compact_logs = self._load_compact_log()
        today_tokens = sum(
            e.get("tokens_before", 0)
            for e in compact_logs
            if e.get("timestamp", "")[:10] == today
        )
        return {
            "today_tokens": today_tokens,
            "exceeded":     today_tokens > _DAILY_TOKEN_BUDGET,
            "source":       "compact_log(fallback)",
        }

    # ?? 湲됱쬆 媛먯? (Bug Fix: usage_log ?곗꽑) ??????????????????????
    def _detect_spike(self) -> dict:
        today     = datetime.now().date()
        yesterday = (today - timedelta(days=1)).isoformat()
        today_str = today.isoformat()

        usage_logs = self._load_usage_log()
        if usage_logs:
            token_key = "total_tokens"
            logs      = usage_logs
        else:
            token_key = "tokens_before"
            logs      = self._load_compact_log()

        today_t = sum(e.get(token_key, 0) for e in logs
                      if e.get("timestamp", "")[:10] == today_str)
        yest_t  = sum(e.get(token_key, 0) for e in logs
                      if e.get("timestamp", "")[:10] == yesterday)

        if yest_t == 0:
            return {"detected": False, "ratio": 0.0}
        ratio = today_t / yest_t
        return {"detected": ratio >= _SPIKE_MULTIPLIER, "ratio": round(ratio, 2)}

    # ?? skills/ ?댁긽 ?먯? ?????????????????????????????????????????
    def _check_skills(self) -> list:
        skills_dir = _BASE / "skills"
        if not skills_dir.exists():
            # ?좉퇋 ?ㅼ튂 ?먮뒗 ?꾩쭅 Evolution 誘몄떎?????댁뒋 ?꾨떂, ?붾쾭洹몃쭔
            log.debug("skills/ ?붾젆?좊━ ?놁쓬 (Evolution 誘몄떎??or ?좉퇋 ?ㅼ튂)")
            return []
        files = list(skills_dir.iterdir())
        if not files:
            log.debug("skills/ 鍮꾩뼱?덉쓬 (?꾩쭅 Skill 誘몄깮??")
            return []
        issues = []
        for p in files:
            if p.suffix != ".json":
                issues.append(f"{p.name} (鍮껲SON ?뚯씪)")
                continue
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                issues.append(f"{p.name} (?뚯떛 ?ㅻ쪟)")
                continue
            missing = {"name", "description", "trigger", "action"} - set(data.keys())
            if missing:
                issues.append(f"{p.name} (?꾨뱶 ?꾨씫: {missing})")
        return issues

    # ?? ?좏떥 ???????????????????????????????????????????????????????
    def _load_compact_log(self) -> list:
        if not self.compact_log.exists():
            return []
        try:
            return json.loads(self.compact_log.read_text(encoding="utf-8"))
        except Exception:
            return []

    def _load_usage_log(self) -> list:
        """jackal_usage_log.json 濡쒕뱶. ?놁쑝硫?鍮?由ъ뒪??"""
        if not _USAGE_LOG.exists():
            return []
        try:
            return json.loads(_USAGE_LOG.read_text(encoding="utf-8"))
        except Exception:
            return []


# ?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧
# [Bug Fix] log_usage() ?ы띁 ??紐⑤뱢 理쒗븯??(?대옒???몃?)
# Hunter / Scanner / Evolution?먯꽌 import?댁꽌 API ?몄텧 ???ㅽ뻾
# ?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧?먥븧

def log_usage(caller: str, input_tokens: int, output_tokens: int,
              model: str = "unknown") -> None:
    """
    API ?몄텧 ?좏겙??jackal_usage_log.json??湲곕줉.
    Shield._check_budget()?????뚯씪???쎌뼱 ?ㅻ퉬?⑹쓣 異붿쟻?쒕떎.

    ?ъ슜踰?
        from .shield import log_usage
        resp = client.messages.create(model=MODEL_H, ...)
        log_usage("hunter_stage3", resp.usage.input_tokens,
                   resp.usage.output_tokens, model=MODEL_H)

    Args:
        caller: ?몄텧 ?꾩튂 ?앸퀎??(?? "hunter_stage3", "evolution_review")
        input_tokens: resp.usage.input_tokens
        output_tokens: resp.usage.output_tokens
        model: ?ъ슜??紐⑤뜽紐?(鍮꾩슜 ?뺥솗???μ긽)
    """
    # 紐⑤뜽蹂??④? (USD/token)
    _PRICES = {
        "claude-haiku-4-5-20251001":  (0.0000008,  0.000004),   # $0.80/$4.00 per M
        "claude-sonnet-4-6":          (0.000003,   0.000015),   # $3.00/$15.00 per M
        "claude-opus-4-6":            (0.000015,   0.000075),   # $15/$75 per M
    }
    in_price, out_price = _PRICES.get(model, (0.000003, 0.000015))  # 湲곕낯媛? Sonnet

    entry = {
        "timestamp":      datetime.now().isoformat(),
        "caller":         caller,
        "model":          model,
        "input_tokens":   input_tokens,
        "output_tokens":  output_tokens,
        "total_tokens":   input_tokens + output_tokens,
        "estimated_cost_usd": round(
            input_tokens * in_price + output_tokens * out_price, 6
        ),
    }
    logs: list = []
    if _USAGE_LOG.exists():
        try:
            logs = json.loads(_USAGE_LOG.read_text(encoding="utf-8"))
        except Exception:
            pass
    logs.append(entry)
    logs = logs[-2000:]   # 理쒓렐 2000嫄?(??30?쇱튂)
    try:
        _USAGE_LOG.write_text(
            json.dumps(logs, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception as e:
        log.warning(f"usage_log 湲곕줉 ?ㅽ뙣: {e}")


# ??? ?⑤룆 ?ㅽ뻾 ????????????????????????????????????????????????????
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
    shield = JackalShield()
    result = shield.scan()

    print(f"\n{'='*50}")
    print("?썳截? Jackal Shield ?ㅼ틪 寃곌낵")
    print(f"{'='*50}")
    if result["issues"]:
        print(f"?좑툘  諛쒓껄???댁뒋 {len(result['issues'])}嫄?")
        for issue in result["issues"]:
            print(f"  {issue}")
    else:
        print("  ???댁긽 ?놁쓬")

    print(f"\n  ?듦퀎:")
    for k, v in result["stats"].items():
        print(f"    {k}: {v:,}" if isinstance(v, int) else f"    {k}: {v}")
    print(f"  abort: {result['abort']}")
    print(f"{'='*50}\n")



