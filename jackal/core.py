"""
JACKAL core module.
Jackal Core — 새 스윙 기회 탐색 엔진

역할: 포트폴리오 모니터링 X, 항상 새 종목 발굴
매시간 실행 흐름:
  1. Shield   → 비용/보안 체크
  2. Hunter   → ARIA 뉴스 기반 스윙 5종목 탐색 + 알림
  3. Compact  → 필요시 로그 압축
  4. Evolution → 24시간마다 hunt_log.json 자체 학습
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path

from orca.state import load_latest_jackal_weight_snapshot

os.environ["PYTHONIOENCODING"] = "utf-8"
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from .shield import JackalShield
from .compact import JackalCompact
from .hunter import run_hunt
from .evolution import JackalEvolution


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Jackal] %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("jackal_core")
_BASE = Path(__file__).resolve().parent


class JackalCore:

    def __init__(self):
        self.shield    = JackalShield()
        self.compact   = JackalCompact()
        self.evolution = JackalEvolution()

    def run(self, force_hunt: bool = False, force_evolve: bool = False,
            context_tokens: int = 0) -> dict:

        log.info("🦊 Jackal Core 시작")
        start = datetime.now()

        # 1. Shield
        log.info("🛡️  Shield Scan...")
        shield = self.shield.scan()
        if shield.get("abort"):
            log.warning("⛔ Shield 중단")
            return {"status": "aborted"}
        if shield["issues"]:
            for i in shield["issues"]:
                log.warning(f"  {i}")
        else:
            log.info("  이상 없음 ✅")

        # 2. Hunter
        log.info("🎯 Hunter 실행...")
        hunt = run_hunt(force=force_hunt)

        # 3. Compact
        compact = self.compact.check_and_compact(context_tokens)
        if compact.get("compacted"):
            log.info(f"📦 Compact: {compact['saved_tokens']:,} 토큰 절약")

        # 4. Evolution (24시간마다 자체 학습)
        evolve = {}
        if force_evolve or self._should_evolve():
            log.info("🧬 Evolution 실행...")
            evolve = self.evolution.evolve()
            self.evolution.save_weights()
            log.info(
                f"  학습: {evolve.get('learned', 0)}건 | "
                f"Skill: {len(evolve.get('new_skills', []))}개"
            )
        else:
            log.info("🧬 Evolution: 스킵 (24h 미경과)")

        elapsed = round((datetime.now() - start).total_seconds(), 2)
        self._print_summary(hunt, evolve, elapsed)

        return {
            "status":  "ok",
            "elapsed": elapsed,
            "hunt":    hunt,
            "evolution": {
                "ran":     bool(evolve),
                "learned": evolve.get("learned", 0),
                "skills":  len(evolve.get("new_skills", [])),
            },
        }

    def _should_evolve(self) -> bool:
        """jackal_weights.json["last_evolved_at"] 기반 24h 판단"""
        weights = load_latest_jackal_weight_snapshot()
        if not isinstance(weights, dict):
            weights_file = _BASE / "jackal_weights.json"
            if not weights_file.exists():
                return True
            try:
                weights = json.loads(weights_file.read_text(encoding="utf-8"))
            except Exception as e:
                log.warning(f"🧬 Evolution 체크 오류: {e}")
                return True
        try:
            last_str = weights.get("last_evolved_at", "")
            if not last_str:
                return True
            last    = datetime.fromisoformat(last_str)
            elapsed = (datetime.now() - last).total_seconds() / 3600
            should  = elapsed >= 24
            log.info(
                f"🧬 Evolution: 마지막 {elapsed:.1f}h 전 "
                f"→ {'실행' if should else f'스킵 ({24-elapsed:.1f}h 남음)'}"
            )
            return should
        except Exception as e:
            log.warning(f"🧬 Evolution 체크 오류: {e}")
            return True

    def _print_summary(self, hunt: dict, evolve: dict, elapsed: float):
        print("\n" + "=" * 54)
        print(f"  🦊 Jackal | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("=" * 54)
        print(f"  소요      : {elapsed}s")
        print(f"  Hunter    : 분석 {hunt.get('hunted', 0)}종목 | 알림 {hunt.get('alerted', 0)}건")
        if evolve:
            print(f"  Evolution : 학습 {evolve.get('learned', 0)}건 | "
                  f"Skill {len(evolve.get('new_skills', []))}개")
        else:
            print("  Evolution : ⏭️  skip")
        print("=" * 54 + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force-hunt",   action="store_true",
                        help="장 마감 무시 강제 실행")
    parser.add_argument("--force-evolve", action="store_true",
                        help="24h 미경과해도 Evolution 즉시 실행")
    parser.add_argument("--tokens",       type=int, default=0)
    args = parser.parse_args()

    JackalCore().run(
        force_hunt=args.force_hunt,
        force_evolve=args.force_evolve,
        context_tokens=args.tokens,
    )



if __name__ == "__main__":
    main()
