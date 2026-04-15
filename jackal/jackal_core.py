"""
jackal_core.py — _should_evolve() Bug Fix

[Bug Fix 3] .last_evolve 로컬 파일 → jackal_weights.json["last_evolved_at"] 기반
  - .last_evolve: GitHub Actions fresh checkout마다 사라짐 → 매 시간 Evolution 실행
  - jackal_weights.json: aria_jackal.yml에서 git add/commit/push 됨 → 영구 보존
  - Evolution 완료 시 weights["last_evolved_at"] 갱신 → 다음 실행에서 24h 체크

변경 범위:
  - import json 추가 (현재 jackal_core.py에 없음)
  - _should_evolve() 전체 교체
  - jackal_evolution.py: _mark_last_evolve() 교체 필요 (별도 파일)
"""

# ══════════════════════════════════════════════════════════
# jackal_core.py 상단 import 블록에 추가
# ══════════════════════════════════════════════════════════

IMPORT_ADD = "import json   # Bug Fix 3: _should_evolve()에서 jackal_weights.json 읽기"

# ══════════════════════════════════════════════════════════
# _should_evolve() 전체 교체
# ══════════════════════════════════════════════════════════

BEFORE = '''\
    def _should_evolve(self) -> bool:
        marker = _BASE / ".last_evolve"
        if not marker.exists():
            return True
        try:
            last    = datetime.fromisoformat(marker.read_text().strip())
            elapsed = (datetime.now() - last).total_seconds() / 3600
            return elapsed >= 24
        except Exception:
            return True'''

AFTER = '''\
    def _should_evolve(self) -> bool:
        """
        [Bug Fix 3] jackal_weights.json["last_evolved_at"] 기반 24h 판단.
        .last_evolve 로컬 파일 의존 제거 — GitHub Actions fresh checkout 안전.
        jackal_weights.json은 aria_jackal.yml에서 git add/push 됨.
        """
        weights_file = _BASE / "jackal_weights.json"
        if not weights_file.exists():
            log.info("🧬 Evolution 체크: weights 없음 → 실행")
            return True
        try:
            weights  = json.loads(weights_file.read_text(encoding="utf-8"))
            last_str = weights.get("last_evolved_at", "")
            if not last_str:
                log.info("🧬 Evolution 체크: last_evolved_at 없음 → 실행")
                return True
            last    = datetime.fromisoformat(last_str)
            elapsed = (datetime.now() - last).total_seconds() / 3600
            should  = elapsed >= 24
            log.info(
                f"🧬 Evolution 체크: 마지막 실행 {elapsed:.1f}h 전 "
                f"→ {'실행' if should else f'스킵 (잔여 {24 - elapsed:.1f}h)'}"
            )
            return should
        except Exception as e:
            log.warning(f"🧬 Evolution 체크 오류: {e} → 실행")
            return True'''

# ══════════════════════════════════════════════════════════
# jackal_evolution.py: _mark_last_evolve() 교체
# ══════════════════════════════════════════════════════════
# (jackal_evolution.py에 _mark_last_evolve가 명시적으로 없는 경우
#  evolve() 메서드 안에 인라인으로 있을 수 있음)
#
# 현재 evolve()에서:
#   self._mark_last_evolve()
#   self.weights["last_updated"] = datetime.now().isoformat()
#   self._save_weights()
#
# _mark_last_evolve()가 없다면 evolve() 내에 직접 삽입:

EVOLVE_PATCH = '''\
        # [Bug Fix 3] .last_evolve 대신 weights["last_evolved_at"] 갱신
        self.weights["last_evolved_at"] = datetime.now().isoformat()
        # (기존 .last_evolve 파일 쓰기 코드 제거)'''

# ── 검증 스크립트 ─────────────────────────────────────────────────

VERIFY = '''
import json
from pathlib import Path

# jackal_weights.json에 last_evolved_at이 있는지 확인
w_file = Path("jackal/jackal_weights.json")
if w_file.exists():
    w = json.loads(w_file.read_text())
    if w.get("last_evolved_at"):
        print(f"✅ last_evolved_at: {w['last_evolved_at']}")
    else:
        print("⚠️  last_evolved_at 없음 (Evolution 미실행)")
else:
    print("⚠️  jackal_weights.json 없음")

# .last_evolve 마커 파일 상태 확인 (이제 무시해도 됨)
marker = Path("jackal/.last_evolve")
if marker.exists():
    print(f"ℹ️  .last_evolve 존재 (무시됨): {marker.read_text().strip()}")
else:
    print("ℹ️  .last_evolve 없음 (정상 — 이제 사용 안 함)")
'''

if __name__ == "__main__":
    print("=== jackal_core.py + jackal_evolution.py 수정 가이드 ===")
    print()
    print("[1] jackal_core.py 상단에 추가:")
    print(f"    {IMPORT_ADD}")
    print()
    print("[2] _should_evolve() 교체:")
    print("  Before:", BEFORE[:80], "...")
    print("  After:", AFTER[:80], "...")
    print()
    print("[3] jackal_evolution.py evolve() 내 last_evolved_at 갱신:")
    print(EVOLVE_PATCH)
    print()
    print("[검증]")
    print(VERIFY)
