"""
jackal_evolution.py
Jackal Evolution Engine - ARIA 자동 학습 + Skill 승격 시스템

동작 원리:
  1. accuracy.json + lessons/ 에서 최근 7일 성과 로드
  2. Claude(Sonnet)에게 패턴 분석 요청 → JSON 응답
  3. 성공 패턴 → skills/ 에 .json Skill 파일로 저장
  4. 실패 패턴 → lessons/ 에 Instinct(경고) 파일로 저장
  5. jackal_weights.json 자동 업데이트
"""

import json
import os
import re
import logging
from datetime import datetime, timedelta
from pathlib import Path
from anthropic import Anthropic

log = logging.getLogger("jackal_evolution")

# ─── 기본 경로 (JackalCore가 주입하지 않을 때 fallback) ───────────
_BASE = Path(__file__).parent
_DEFAULT_SKILLS = _BASE / "skills"
_DEFAULT_LESSONS = _BASE / "lessons"
_DEFAULT_WEIGHTS = _BASE / "jackal_weights.json"

# ─── 모델 설정 ────────────────────────────────────────────────────
_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
_MAX_TOKENS = int(os.getenv("MAX_THINKING_TOKENS", "8000"))


class JackalEvolution:
    def __init__(
        self,
        skills_dir: Path = _DEFAULT_SKILLS,
        lessons_dir: Path = _DEFAULT_LESSONS,
        weights_file: Path = _DEFAULT_WEIGHTS,
    ):
        self.client = Anthropic()
        self.skills_dir = Path(skills_dir)
        self.lessons_dir = Path(lessons_dir)
        self.weights_file = Path(weights_file)
        self.weights = self._load_weights()

    # ── 공개 메서드 ────────────────────────────────────────────────
    def evolve(self) -> dict:
        """Evolution 1회 실행"""
        log.info("🧬 Evolution 시작")

        context = self._build_context()
        raw = self._ask_claude(context)
        result = self._parse_response(raw)

        self._save_skills(result.get("new_skills", []))
        self._save_instincts(result.get("new_instincts", []))
        self._update_weights(result)
        self._mark_last_evolve()

        log.info("🧬 Evolution 완료")
        return result

    def save_weights(self):
        """현재 weights를 파일에 저장"""
        self.weights_file.write_text(
            json.dumps(self.weights, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    # ── 컨텍스트 구성 ──────────────────────────────────────────────
    def _build_context(self) -> dict:
        """최근 7일 데이터를 수집해 컨텍스트 딕셔너리로 반환"""
        accuracy = self._load_json_safe(_BASE / "accuracy.json", default={})
        recent_lessons = self._load_recent_lessons(days=7)
        skill_names = [p.stem for p in self.skills_dir.glob("*.json")]
        weight_summary = {k: round(v, 3) for k, v in self.weights.items()}

        return {
            "accuracy": accuracy,
            "recent_lessons": recent_lessons,
            "existing_skills": skill_names,
            "weights": weight_summary,
        }

    def _load_recent_lessons(self, days: int = 7) -> list[dict]:
        cutoff = datetime.now() - timedelta(days=days)
        lessons = []
        for p in sorted(self.lessons_dir.glob("*.json")):
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
                ts = datetime.fromisoformat(data.get("timestamp", "2000-01-01"))
                if ts >= cutoff:
                    lessons.append(data)
            except Exception:
                continue
        return lessons[-30:]  # 최대 30건

    # ── Claude 호출 ────────────────────────────────────────────────
    def _ask_claude(self, context: dict) -> str:
        prompt = f"""
너는 Jackal, ARIA 투자 분석 에이전트의 자동 진화 엔진이다.
아래 최근 7일 성과 데이터를 분석하고, 다음 4가지를 JSON으로만 반환하라.
마크다운 코드블록, 설명 없이 순수 JSON만 출력할 것.

### 입력 데이터
{json.dumps(context, ensure_ascii=False, indent=2)}

### 반환 형식 (JSON)
{{
  "new_skills": [
    {{
      "name": "skill_이름(snake_case)",
      "description": "어떤 상황에서 쓰는 Skill인지 1줄 설명",
      "trigger": "이 Skill이 발동되어야 하는 조건",
      "action": "구체적인 분석/판단 방법"
    }}
  ],
  "new_instincts": [
    {{
      "name": "instinct_이름",
      "warning": "피해야 할 패턴",
      "reason": "왜 실패했는가"
    }}
  ],
  "prompt_improvements": "Hunter/Analyst/Devil 프롬프트에 추가하면 좋을 개선 사항 (없으면 빈 문자열)",
  "cost_saving_tip": "이번 주 비용 절감을 위한 실질적 제안 (없으면 빈 문자열)",
  "weight_adjustments": {{
    "fear_greed_weight": 0.0,
    "technical_weight": 0.0,
    "fundamental_weight": 0.0
  }}
}}

규칙:
- new_skills, new_instincts는 기존 데이터와 중복되지 않는 것만 포함
- 데이터가 부족하면 new_skills/new_instincts를 빈 배열로 반환
- weight_adjustments는 -0.1 ~ +0.1 범위의 조정값 (절대값 아님)
""".strip()

        resp = self.client.messages.create(
            model=_MODEL,
            max_tokens=_MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text

    # ── 응답 파싱 ──────────────────────────────────────────────────
    def _parse_response(self, raw: str) -> dict:
        # JSON 블록 추출 (마크다운 fence 제거)
        cleaned = re.sub(r"```(?:json)?|```", "", raw).strip()
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError as e:
            log.error(f"Evolution 응답 파싱 실패: {e}\n원문: {raw[:300]}")
            return {
                "new_skills": [],
                "new_instincts": [],
                "prompt_improvements": "",
                "cost_saving_tip": "",
                "weight_adjustments": {},
            }

    # ── Skill 저장 ─────────────────────────────────────────────────
    def _save_skills(self, skills: list[dict]):
        for skill in skills:
            name = skill.get("name", "").strip()
            if not name:
                continue
            path = self.skills_dir / f"{name}.json"
            skill["created_at"] = datetime.now().isoformat()
            path.write_text(json.dumps(skill, ensure_ascii=False, indent=2), encoding="utf-8")
            log.info(f"  ✅ Skill 생성: {name}")

    # ── Instinct 저장 ──────────────────────────────────────────────
    def _save_instincts(self, instincts: list[dict]):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        for i, inst in enumerate(instincts):
            name = inst.get("name", f"instinct_{i}").strip()
            path = self.lessons_dir / f"{ts}_{name}.json"
            inst["timestamp"] = datetime.now().isoformat()
            path.write_text(json.dumps(inst, ensure_ascii=False, indent=2), encoding="utf-8")
            log.info(f"  ⚠️  Instinct 등록: {name}")

    # ── 가중치 업데이트 ────────────────────────────────────────────
    def _update_weights(self, result: dict):
        adjustments = result.get("weight_adjustments", {})
        for key, delta in adjustments.items():
            if key in self.weights:
                self.weights[key] = round(
                    max(0.0, min(1.0, self.weights[key] + float(delta))), 4
                )
        self.weights["last_updated"] = datetime.now().isoformat()

    # ── 마지막 실행 시각 기록 ──────────────────────────────────────
    def _mark_last_evolve(self):
        marker = self.lessons_dir / ".last_evolve"
        marker.write_text(datetime.now().isoformat(), encoding="utf-8")

    # ── 유틸 ───────────────────────────────────────────────────────
    def _load_weights(self) -> dict:
        default = {
            "fear_greed_weight": 0.35,
            "technical_weight": 0.40,
            "fundamental_weight": 0.25,
            "last_updated": "",
        }
        if self.weights_file.exists():
            try:
                loaded = json.loads(self.weights_file.read_text(encoding="utf-8"))
                default.update(loaded)
            except Exception:
                pass
        return default

    @staticmethod
    def _load_json_safe(path: Path, default):
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                pass
        return default


# ─── 단독 실행 ────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
    ev = JackalEvolution()
    result = ev.evolve()
    ev.save_weights()
    print(json.dumps(result, ensure_ascii=False, indent=2))
