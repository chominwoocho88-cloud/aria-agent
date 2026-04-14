"""
jackal_core.py
Jackal Core — 메인 오케스트레이터

실행 흐름 (매시간):
  1. Shield Scan  → 보안/비용 체크
  2. Scanner      → 타점 계산 + 텔레그램 알림
  3. Evolution    → 24시간마다 과거 알림 결과 학습 + 가중치 조정
  4. Compact      → 필요시 컨텍스트 압축
"""

import os
import sys
import json
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
from jackal_evolution import JackalEvolution

BASE_DIR = Path(__file__).parent

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
        shield_result = self.shield.scan()
        if shield_result.get("abort"):
            log.warning("⛔ Shield 중단 요청")
            return {"status": "aborted", "reason": shield_result}
        if shield_result["issues"]:
            for issue in shield_result["issues"]:
                log.warning(f"  {issue}")
        else:
            log.info("  Shield: 이상 없음 ✅")

        # 2. Scanner (매시간 핵심)
        log.info("📡 Scanner 실행...")
        scan_result = run_scan(force=force_scan)
        log.info(
            f"  분석 {scan_result['scanned']}종목 | "
            f"알림 {scan_result['alerted']}건"
        )

        # 3. Evolution (24시간마다 OR 강제)
        evolve_result = {}
        if force_evolve or self._should_evolve():
            log.info("🧬 Evolution 실행...")
            evolve_result = self.evolution.evolve()
            self.evolution.save_weights()
            self._mark_evolve()
        else:
            log.info("🧬 Evolution: 스킵 (24h 미경과)")

        # 4. Compact (필요시)
        compact_result = self.compact.check_and_compact(context_tokens)
        if compact_result.get("compacted"):
            log.info(f"📦 Compact: {compact_result['saved_tokens']:,} 토큰 절약")

        elapsed = (datetime.now() - start).total_seconds()

        report = {
            "status":    "ok",
            "elapsed":   round(elapsed, 2),
            "shield":    shield_result,
            "scan":      {"scanned": scan_result["scanned"], "alerted": scan_result["alerted"]},
            "evolution": evolve_result,
            "compact":   compact_result.get("compacted", False),
            "timestamp": datetime.now().isoformat(),
        }

        self._print_summary(report)
        return report

    def _should_evolve(self) -> bool:
        marker = BASE_DIR / ".last_evolve"
        if not marker.exists():
            return True
        try:
            last    = datetime.fromisoformat(marker.read_text().strip())
            elapsed = (datetime.now() - last).total_seconds() / 3600
            return elapsed >= 24
        except Exception:
            return True

    def _mark_evolve(self):
        (BASE_DIR / ".last_evolve").write_text(
            datetime.now().isoformat(), encoding="utf-8"
        )

    def _print_summary(self, r: dict):
        scan = r["scan"]
        ev   = r.get("evolution", {})
        print("\n" + "=" * 52)
        print(f"  🦊 Jackal | {r['timestamp'][:19]}")
        print("=" * 52)
        print(f"  상태      : {r['status'].upper()}")
        print(f"  소요      : {r['elapsed']}s")
        print(f"  Shield    : {'⚠️ ' + str(len(r['shield']['issues'])) + '건' if r['shield']['issues'] else '✅ 이상 없음'}")
        print(f"  Scanner   : 분석 {scan['scanned']}종목 | 알림 {scan['alerted']}건")
        if ev:
            print(f"  Evolution : 학습 {ev.get('learned', 0)}건 | 가중치 변경 {len(ev.get('weight_changes', []))}개")
        else:
            print("  Evolution : ⏭️  skip")
        print(f"  Compact   : {'✅' if r['compact'] else '⏭️  skip'}")
        print("=" * 52 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Jackal Core Runner")
    parser.add_argument("--force-scan",   action="store_true", help="장 마감 무시하고 강제 스캔")
    parser.add_argument("--force-evolve", action="store_true", help="Evolution 강제 실행")
    parser.add_argument("--tokens",       type=int, default=0, help="현재 세션 토큰 수")
    args = parser.parse_args()

    core = JackalCore()
    core.run(
        force_scan=args.force_scan,
        force_evolve=args.force_evolve,
        context_tokens=args.tokens,
    )
