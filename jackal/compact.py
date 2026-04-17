"""
JACKAL compact module.
Jackal Compact - Context Rot 諛⑹? ?먮룞 ?뺤텞 ?쒖뒪??
??븷:
  - ?ㅻ뒛 ?ㅼ젣 API ?ъ슜??usage_log)???꾧퀎移?珥덇낵 ???먮룞 ?뺤텞 ?ㅽ뻾
  - ?듭떖 ?뺣낫(理쒓렐 ?좏샇, ?숈뒿 ?곗씠??留?蹂댁〈
  - ?뺤텞 寃곌낵瑜?compact_log.json??湲곕줉

[Bug Fix 6] check_and_compact()媛 usage_log?먯꽌 ?ㅻ뒛 ?좏겙 ?먯껜 怨꾩궛
  湲곗〈: ?몃??먯꽌 current_tokens瑜?諛쏆쓬 ??Actions?먯꽌 0???꾨떖????긽 skip
  ?섏젙: current_tokens=0?대㈃ jackal_usage_log.json?먯꽌 ?ㅻ뒛 ?ㅼ궗?⑸웾 ?⑹궛
"""

import json
import logging
import os
from datetime import datetime, date
from pathlib import Path
from anthropic import Anthropic

log = logging.getLogger("jackal_compact")

_BASE          = Path(__file__).parent
_REPO_ROOT     = _BASE.parent          # jackal/ ??repo root
_COMPACT_LOG   = _BASE / "compact_log.json"
_COMPACT_CACHE = _BASE / "compact_cache.json"
_USAGE_LOG     = _BASE / "jackal_usage_log.json"   # Bug Fix 2 ?곕룞

_COMPACT_THRESHOLD = int(os.getenv("JACKAL_COMPACT_THRESHOLD", "60000"))
_TARGET_RATIO      = 0.30

_MODEL = os.getenv("SUBAGENT_MODEL", "claude-haiku-4-5-20251001")


class JackalCompact:
    """Compact JACKAL usage logs and cache recent high-signal context."""

    def __init__(self):
        self.client     = Anthropic()
        self.log_path   = _COMPACT_LOG
        self.cache_path = _COMPACT_CACHE

    # ?? 怨듦컻 硫붿꽌??????????????????????????????????????????????????
    def check_and_compact(self, current_tokens: int = 0) -> dict:
        """
        ?ㅻ뒛 ?ㅼ젣 API ?좏겙???꾧퀎移?珥덇낵 ???먮룞 ?뺤텞 ?ㅽ뻾.

        [Bug Fix 6] current_tokens=0?대㈃ usage_log ?먯껜 怨꾩궛.
        GitHub Actions?먯꽌 --tokens ?놁씠 ?ㅽ뻾?쇰룄 ?뺤긽 ?숈옉.

        Args:
            current_tokens: 紐낆떆???좏겙 ?? 0?대㈃ usage_log?먯꽌 怨꾩궛.
        """
        # 0?대㈃ usage_log?먯꽌 ?ㅻ뒛 ?ㅼ궗?⑸웾 怨꾩궛
        if current_tokens == 0:
            current_tokens = self._get_today_tokens()
            if current_tokens > 0:
                log.info(f"  usage_log 湲곕컲 ?ㅻ뒛 ?ъ슜?? {current_tokens:,} ?좏겙")

        if current_tokens < _COMPACT_THRESHOLD:
            log.debug(f"Compact ?ㅽ궢: {current_tokens:,} < {_COMPACT_THRESHOLD:,}")
            return {
                "compacted":      False,
                "current_tokens": current_tokens,
                "threshold":      _COMPACT_THRESHOLD,
                "saved_tokens":   0,
                "summary":        "",
            }

        log.info(f"???좏겙 {current_tokens:,} >= {_COMPACT_THRESHOLD:,} ???뺤텞 ?쒖옉")
        return self._compact(current_tokens)

    def force_compact(self) -> dict:
        """媛뺤젣 ?뺤텞 ?ㅽ뻾 (?좏겙 ??臾닿?)"""
        log.info("??媛뺤젣 ?뺤텞 ?ㅽ뻾")
        return self._compact(current_tokens=0, forced=True)

    # ?? ?ㅻ뒛 ?좏겙 ?먯껜 怨꾩궛 ????????????????????????????????????????
    def _get_today_tokens(self) -> int:
        """
        jackal_usage_log.json?먯꽌 ?ㅻ뒛 ?ㅼ젣 ?뚮え ?좏겙 ?⑹궛.
        ?뚯씪 ?놁쑝硫?0 諛섑솚.
        """
        if not _USAGE_LOG.exists():
            return 0
        try:
            logs  = json.loads(_USAGE_LOG.read_text(encoding="utf-8"))
            today = date.today().isoformat()
            return sum(
                e.get("total_tokens", 0)
                for e in logs
                if e.get("timestamp", "")[:10] == today
            )
        except Exception as e:
            log.warning(f"usage_log ?쎄린 ?ㅽ뙣: {e}")
            return 0

    # ?? ?뺤텞 濡쒖쭅 ??????????????????????????????????????????????????
    def _compact(self, current_tokens: int, forced: bool = False) -> dict:
        raw_data = self._collect_compressible_data()

        if not raw_data:
            log.warning("?뺤텞???곗씠?곌? ?놁뒿?덈떎.")
            return {"compacted": False, "saved_tokens": 0, "summary": "no data"}

        summary, token_usage = self._summarize(raw_data)
        self._save_cache(summary)

        estimated_saved = int(current_tokens * (1 - _TARGET_RATIO))
        self._append_log({
            "timestamp":        datetime.now().isoformat(),
            "forced":           forced,
            "tokens_before":    current_tokens,
            "estimated_saved":  estimated_saved,
            "summary_chars":    len(summary),
            "prompt_tokens":    token_usage["prompt_tokens"],
            "response_tokens":  token_usage["response_tokens"],
            "total_api_tokens": token_usage["total_api_tokens"],
            "cost_usd":         token_usage["estimated_cost_usd"],
        })

        log.info(f"  ?뺤텞 ?꾨즺 ????{estimated_saved:,} ?좏겙 ?덉빟 ?덉긽")
        return {
            "compacted":      True,
            "current_tokens": current_tokens,
            "threshold":      _COMPACT_THRESHOLD,
            "saved_tokens":   estimated_saved,
            "summary":        summary[:500] + ("..." if len(summary) > 500 else ""),
        }

    # ?? ?곗씠???섏쭛 ????????????????????????????????????????????????
    def _collect_compressible_data(self) -> dict:
        data = {}

        acc_path = _REPO_ROOT / "data" / "accuracy.json"   # data/accuracy.json (repo root 湲곗?)
        if acc_path.exists():
            try:
                data["accuracy"] = json.loads(acc_path.read_text(encoding="utf-8"))
            except Exception:
                pass

        lessons_dir = _BASE / "lessons"
        if lessons_dir.exists():
            lessons = []
            for p in sorted(lessons_dir.glob("*.json"))[-10:]:
                try:
                    lessons.append(json.loads(p.read_text(encoding="utf-8")))
                except Exception:
                    continue
            if lessons:
                data["recent_lessons"] = lessons

        skills_dir = _BASE / "skills"
        if skills_dir.exists():
            data["skill_names"] = [p.stem for p in skills_dir.glob("*.json")]

        if self.cache_path.exists():
            try:
                prev = json.loads(self.cache_path.read_text(encoding="utf-8"))
                data["previous_summary"] = prev.get("summary", "")
            except Exception:
                pass

        return data

    # ?? Claude ?붿빟 ????????????????????????????????????????????????
    def _summarize(self, raw_data: dict) -> tuple:
        prompt = f"""
?덈뒗 ARIA ?ъ옄 遺꾩꽍 ?먯씠?꾪듃??而⑦뀓?ㅽ듃 ?뺤텞湲곕떎.
?꾨옒 ?곗씠?곕? 遺꾩꽍?섏뿬 ?듭떖 ?뺣낫留?500 ?좏겙 ?대궡濡??뺤텞?섎씪.

蹂댁〈 ?곗꽑?쒖쐞:
1. 理쒓렐 ?뺥솗??accuracy) ?섏튂
2. 理쒓렐 Instinct 寃쎄퀬 (?ㅽ뙣 ?⑦꽩)
3. ?쒖꽦?붾맂 Skill 紐⑸줉
4. ?댁쟾 ?붿빟蹂몄쓽 ?듭떖

?쒓굅 ???
- ?ㅻ옒???쒖옣 ?곹솴 ?ㅻ챸
- 以묐났 ?뺣낫
- ?깃낵 ?녿뒗 ?⑦꽩

?낅젰 ?곗씠??
{json.dumps(raw_data, ensure_ascii=False, indent=2)[:4000]}

異쒕젰: ?쒓뎅?? 遺덈┸ ?ъ씤???뺤떇, 500???대궡
""".strip()

        resp = self.client.messages.create(
            model=_MODEL,
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}],
        )
        token_usage = {
            "prompt_tokens":      resp.usage.input_tokens,
            "response_tokens":    resp.usage.output_tokens,
            "total_api_tokens":   resp.usage.input_tokens + resp.usage.output_tokens,
            "estimated_cost_usd": round(
                resp.usage.input_tokens  * 0.00000080
                + resp.usage.output_tokens * 0.00000400,
                6,
            ),
        }
        return resp.content[0].text.strip(), token_usage

    def _save_cache(self, summary: str):
        self.cache_path.write_text(
            json.dumps({"summary": summary, "updated_at": datetime.now().isoformat()},
                       ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _append_log(self, entry: dict):
        logs = []
        if self.log_path.exists():
            try:
                logs = json.loads(self.log_path.read_text(encoding="utf-8"))
            except Exception:
                pass
        logs.append(entry)
        self.log_path.write_text(
            json.dumps(logs[-100:], ensure_ascii=False, indent=2), encoding="utf-8"
        )


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
    parser = argparse.ArgumentParser(description="Jackal Compact Runner")
    parser.add_argument("--tokens", type=int, default=0)
    parser.add_argument("--force",  action="store_true")
    args = parser.parse_args()

    compact = JackalCompact()
    result  = compact.force_compact() if args.force else compact.check_and_compact(args.tokens)
    print(json.dumps(result, ensure_ascii=False, indent=2))



