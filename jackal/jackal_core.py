"""
jackal_core.py
Jackal Core — 완전 독립 오케스트레이터 (ARIA와 무관)

매시간 실행 흐름:
  1. Shield  → 보안/비용 체크
  2. Scanner → Claude Haiku로 타점 판단 + 텔레그램
  3. Compact → 필요시 압축
  4. Evolution → 24시간마다 자체 학습
                 (scan_log.json 기반 — ARIA 파일 읽지 않음)
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from jackal_shield    import JackalShield
from jackal_compact   import JackalCompact
from jackal_scanner   import run_scan
from jackal_hunter    import run_hunt
from jackal_evolution import JackalEvolution

_BASE = Path(__file__).parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Jackal] %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("jackal_core")


class JackalCore:

    def __init__(self):
        self.shield    = JackalShield()
        self.compact   = JackalCompact()
        self.evolution = JackalEvolution()

    def run(self, force_scan: bool = False, force_evolve: bool = False,
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

        # 2. Scanner — 포트폴리오 모니터링
        log.info("📡 Scanner 실행 (포트폴리오)...")
        scan = run_scan(force=force_scan)

        # 3. Hunter — ARIA 뉴스 기반 스윙 기회 탐색
        log.info("🎯 Hunter 실행 (ARIA 스윙 탐색)...")
        hunt = run_hunt(force=force_scan)

        # 4. Compact
        compact = self.compact.check_and_compact(context_tokens)
        if compact.get("compacted"):
            log.info(f"📦 Compact: {compact['saved_tokens']:,} 토큰 절약")

        # 4. Evolution (24시간마다 자체 학습)
        evolve = {}
        if force_evolve or self._should_evolve():
            log.info("🧬 Evolution 실행 (자체 학습)...")
            evolve = self.evolution.evolve()
            self.evolution.save_weights()
            log.info(
                f"  스캔 학습: {evolve.get('scan_learned', 0)}건 | "
                f"Skill: {len(evolve.get('new_skills', []))}개 | "
                f"Instinct: {len(evolve.get('new_instincts', []))}개"
            )
            for c in evolve.get("weight_changes", []):
                log.info(f"  ⚖️  {c}")
        else:
            log.info("🧬 Evolution: 스킵 (24h 미경과)")

        elapsed = round((datetime.now() - start).total_seconds(), 2)
        self._print_summary(scan, evolve, elapsed)

        return {
            "status":  "ok",
            "elapsed": elapsed,
            "scan":    scan,
            "hunt":    hunt,
            "evolution": {
                "ran":     bool(evolve),
                "learned": evolve.get("scan_learned", 0),
                "skills":  len(evolve.get("new_skills", [])),
            },
        }

    def _should_evolve(self) -> bool:
        marker = _BASE / ".last_evolve"
        if not marker.exists():
            return True
        try:
            last    = datetime.fromisoformat(marker.read_text().strip())
            elapsed = (datetime.now() - last).total_seconds() / 3600
            return elapsed >= 24
        except Exception:
            return True

    def _print_summary(self, scan: dict, evolve: dict, elapsed: float):
        print("\n" + "=" * 54)
        print(f"  🦊 Jackal | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("=" * 54)
        print(f"  소요      : {elapsed}s")
        print(f"  Scanner   : 분석 {scan['scanned']}종목 | 알림 {scan['alerted']}건")
        print(f"  Hunter    : 분석 {hunt.get('hunted',0)}종목 | 알림 {hunt.get('alerted',0)}건")
        if evolve:
            print(f"  Evolution : 학습 {evolve.get('scan_learned',0)}건 | "
                  f"Skill {len(evolve.get('new_skills',[]))}개")
        else:
            print("  Evolution : ⏭️  skip")
        print("=" * 54 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force-scan",   action="store_true")
    parser.add_argument("--force-evolve", action="store_true")
    parser.add_argument("--tokens",       type=int, default=0)
    args = parser.parse_args()

    JackalCore().run(
        force_scan=args.force_scan,
        force_evolve=args.force_evolve,
        context_tokens=args.tokens,
    )
