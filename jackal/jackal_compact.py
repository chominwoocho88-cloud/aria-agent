"""
jackal_compact.py
Jackal Compact - Context Rot 방지 자동 압축 시스템

역할:
  - 현재 세션 토큰 수가 임계치 초과 시 자동 /compact 실행
  - 핵심 정보(최근 신호, 포트폴리오 상태)만 보존
  - 압축 결과를 compact_log.json에 기록
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from anthropic import Anthropic

log = logging.getLogger("jackal_compact")

_BASE = Path(__file__).parent
_COMPACT_LOG = _BASE / "compact_log.json"
_COMPACT_CACHE = _BASE / "compact_cache.json"

# 토큰 임계치: 환경변수로 조정 가능
_COMPACT_THRESHOLD = int(os.getenv("JACKAL_COMPACT_THRESHOLD", "60000"))
# 압축 후 목표 토큰 비율 (임계치의 30% 수준으로 요약)
_TARGET_RATIO = 0.30

_MODEL = os.getenv("SUBAGENT_MODEL", "claude-haiku-4-5-20251001")


class JackalCompact:
    """Context Rot 방지 자동 압축기"""

    def __init__(self):
        self.client = Anthropic()
        self.log_path = _COMPACT_LOG
        self.cache_path = _COMPACT_CACHE

    # ── 공개 메서드 ────────────────────────────────────────────────
    def check_and_compact(self, current_tokens: int) -> dict:
        """
        토큰 임계치 초과 시 자동 압축 실행.

        Args:
            current_tokens: 현재 세션 누적 토큰 수

        Returns:
            {compacted: bool, saved_tokens: int, summary: str}
        """
        if current_tokens < _COMPACT_THRESHOLD:
            return {
                "compacted": False,
                "current_tokens": current_tokens,
                "threshold": _COMPACT_THRESHOLD,
                "saved_tokens": 0,
                "summary": "",
            }

        log.info(f"⚡ 토큰 {current_tokens:,} >= 임계치 {_COMPACT_THRESHOLD:,} → 압축 시작")
        return self._compact(current_tokens)

    def force_compact(self) -> dict:
        """강제 압축 실행 (토큰 수 무관)"""
        log.info("⚡ 강제 압축 실행")
        return self._compact(current_tokens=0, forced=True)

    # ── 압축 로직 ──────────────────────────────────────────────────
    def _compact(self, current_tokens: int, forced: bool = False) -> dict:
        # 1. 압축 대상 데이터 수집
        raw_data = self._collect_compressible_data()

        if not raw_data:
            log.warning("압축할 데이터가 없습니다.")
            return {"compacted": False, "saved_tokens": 0, "summary": "no data"}

        # 2. Claude Haiku에게 핵심만 요약 요청 (저비용)
        summary = self._summarize(raw_data)

        # 3. 요약본을 캐시에 저장
        self._save_cache(summary)

        # 4. 로그 기록
        estimated_saved = int(current_tokens * (1 - _TARGET_RATIO))
        self._append_log(
            {
                "timestamp": datetime.now().isoformat(),
                "forced": forced,
                "tokens_before": current_tokens,
                "estimated_saved": estimated_saved,
                "summary_chars": len(summary),
            }
        )

        log.info(f"  압축 완료 → 약 {estimated_saved:,} 토큰 절약 예상")
        return {
            "compacted": True,
            "current_tokens": current_tokens,
            "threshold": _COMPACT_THRESHOLD,
            "saved_tokens": estimated_saved,
            "summary": summary[:500] + ("..." if len(summary) > 500 else ""),
        }

    # ── 데이터 수집 ────────────────────────────────────────────────
    def _collect_compressible_data(self) -> dict:
        """lessons/, skills/, accuracy.json 등에서 압축 가능 데이터 로드"""
        data = {}

        # accuracy.json
        acc_path = _BASE / "accuracy.json"
        if acc_path.exists():
            try:
                data["accuracy"] = json.loads(acc_path.read_text(encoding="utf-8"))
            except Exception:
                pass

        # 최근 lessons (최대 10개)
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

        # skills 목록
        skills_dir = _BASE / "skills"
        if skills_dir.exists():
            data["skill_names"] = [p.stem for p in skills_dir.glob("*.json")]

        # 기존 compact_cache (이전 압축본 포함)
        if self.cache_path.exists():
            try:
                prev = json.loads(self.cache_path.read_text(encoding="utf-8"))
                data["previous_summary"] = prev.get("summary", "")
            except Exception:
                pass

        return data

    # ── Claude 요약 ────────────────────────────────────────────────
    def _summarize(self, raw_data: dict) -> str:
        prompt = f"""
너는 ARIA 투자 분석 에이전트의 컨텍스트 압축기다.
아래 데이터를 분석하여 **핵심 정보만** 500 토큰 이내로 압축하라.

보존 우선순위:
1. 최근 정확도(accuracy) 수치
2. 최근 Instinct 경고 (실패 패턴)
3. 활성화된 Skill 목록
4. 이전 요약본의 핵심

제거 대상:
- 오래된 시장 상황 설명
- 중복 정보
- 성과 없는 패턴

입력 데이터:
{json.dumps(raw_data, ensure_ascii=False, indent=2)[:4000]}

출력: 한국어, 불릿 포인트 형식, 500자 이내
""".strip()

        resp = self.client.messages.create(
            model=_MODEL,
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text.strip()

    # ── 캐시 저장 ──────────────────────────────────────────────────
    def _save_cache(self, summary: str):
        cache = {
            "summary": summary,
            "updated_at": datetime.now().isoformat(),
        }
        self.cache_path.write_text(
            json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    # ── 로그 추가 ──────────────────────────────────────────────────
    def _append_log(self, entry: dict):
        logs = []
        if self.log_path.exists():
            try:
                logs = json.loads(self.log_path.read_text(encoding="utf-8"))
            except Exception:
                pass
        logs.append(entry)
        logs = logs[-100:]  # 최근 100건만 보관
        self.log_path.write_text(
            json.dumps(logs, ensure_ascii=False, indent=2), encoding="utf-8"
        )


# ─── 단독 실행 ────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
    parser = argparse.ArgumentParser(description="Jackal Compact Runner")
    parser.add_argument("--tokens", type=int, default=0)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    compact = JackalCompact()
    if args.force:
        result = compact.force_compact()
    else:
        result = compact.check_and_compact(args.tokens)

    print(json.dumps(result, ensure_ascii=False, indent=2))
