"""
jackal_shield.py
Jackal Shield - 보안 + 비용 자동 체크 시스템

[Bug Fix 2] _check_budget()이 compact_log만 읽어 실 API 비용 미집계 → 수정
  - jackal_usage_log.json 신규 도입
  - log_usage() 헬퍼 추가 (Hunter/Scanner/Evolution에서 호출)
  - _check_budget() / _detect_spike() 모두 usage_log 우선 사용

검사 항목:
  1. API 키 노출 (.env, *.py, *.json, *.yml 에서 패턴 탐색)
  2. 일일 토큰 예산 초과 여부 (usage_log 기반 — 실 API 비용)
  3. 비정상 토큰 급증 감지 (전일 대비 300% 이상)
  4. skills/ 디렉토리 비정상 파일 탐지
"""

import json
import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path

log = logging.getLogger("jackal_shield")

_BASE      = Path(__file__).parent
_REPO_ROOT = _BASE.parent   # repo root — API 키 스캔 범위

# ─── 설정 ─────────────────────────────────────────────────────────
_DAILY_TOKEN_BUDGET = int(os.getenv("JACKAL_DAILY_BUDGET", "500000"))
_SPIKE_MULTIPLIER   = float(os.getenv("JACKAL_SPIKE_MULTIPLIER", "3.0"))
_SECRET_PATTERNS = [
    re.compile(r"sk-ant-[A-Za-z0-9\-_]{20,}", re.I),
    re.compile(r"sk-[A-Za-z0-9]{20,}", re.I),
    # [Fix] os.environ.get 형태는 실제 키 값이 아니므로 제외
    re.compile(r"ANTHROPIC_API_KEY\s*=\s*['\"][A-Za-z0-9\-_]{20,}['\"]", re.I),
    re.compile(r"api[_\-]?key\s*[:=]\s*['\"][A-Za-z0-9\-_]{20,}['\"]", re.I),
]
_EXCLUDE_DIRS    = {".git", "__pycache__", "node_modules", ".venv", "venv"}
_SCAN_EXTENSIONS = {".py", ".json", ".yml", ".yaml", ".env", ".txt", ".md"}

# [Fix] usage_log 경로 추가
_USAGE_LOG = _BASE / "jackal_usage_log.json"


class JackalShield:
    """보안 + 비용 스캐너"""

    def __init__(self, scan_root: Path = _REPO_ROOT):
        self.scan_root  = Path(scan_root)
        self.compact_log = _BASE / "compact_log.json"

    # ── 공개 메서드 ────────────────────────────────────────────────
    def scan(self) -> dict:
        """
        전체 스캔 실행.
        Returns: {issues, abort, stats}
        """
        issues = []
        stats  = {}

        # 1. API 키 노출 스캔
        leaked = self._scan_secrets()
        for item in leaked:
            issues.append(f"🔑 API키 노출 의심: {item}")

        # 2. 일일 토큰 예산 체크 (usage_log 기반)
        budget = self._check_budget()
        stats["today_tokens"]  = budget["today_tokens"]
        stats["daily_budget"]  = _DAILY_TOKEN_BUDGET
        stats["budget_source"] = budget["source"]
        if budget["exceeded"]:
            issues.append(
                f"💸 일일 토큰 예산 초과: "
                f"{budget['today_tokens']:,} / {_DAILY_TOKEN_BUDGET:,} "
                f"[{budget['source']}]"
            )

        # 3. 토큰 급증 감지
        spike = self._detect_spike()
        stats["spike_ratio"] = spike["ratio"]
        if spike["detected"]:
            issues.append(f"📈 토큰 급증 감지: 전일 대비 {spike['ratio']:.1f}배 증가")

        # 4. skills/ 이상 파일 탐지
        for s in self._check_skills():
            issues.append(f"⚠️  skills/ 이상 파일: {s}")

        # abort 조건: API 키 노출 OR 예산 2배 초과
        abort = bool(leaked) or budget["today_tokens"] > _DAILY_TOKEN_BUDGET * 2

        return {
            "issues":     issues,
            "abort":      abort,
            "stats":      stats,
            "scanned_at": datetime.now().isoformat(),
        }

    # ── API 키 노출 스캔 ───────────────────────────────────────────
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

    # ── 토큰 예산 체크 (Bug Fix: usage_log 우선) ──────────────────
    def _check_budget(self) -> dict:
        """
        [Fix] jackal_usage_log.json에서 오늘 실 API 토큰 합산.
        usage_log 없으면 compact_log로 폴백 (구버전 호환).
        """
        today = datetime.now().date().isoformat()

        # 1순위: usage_log (실제 API 토큰)
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

        # 폴백: compact_log
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

    # ── 급증 감지 (Bug Fix: usage_log 우선) ──────────────────────
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

    # ── skills/ 이상 탐지 ─────────────────────────────────────────
    def _check_skills(self) -> list:
        skills_dir = _BASE / "skills"
        if not skills_dir.exists():
            return []
        issues = []
        for p in skills_dir.iterdir():
            if p.suffix != ".json":
                issues.append(f"{p.name} (비JSON 파일)")
                continue
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                issues.append(f"{p.name} (파싱 오류)")
                continue
            missing = {"name", "description", "trigger", "action"} - set(data.keys())
            if missing:
                issues.append(f"{p.name} (필드 누락: {missing})")
        return issues

    # ── 유틸 ───────────────────────────────────────────────────────
    def _load_compact_log(self) -> list:
        if not self.compact_log.exists():
            return []
        try:
            return json.loads(self.compact_log.read_text(encoding="utf-8"))
        except Exception:
            return []

    def _load_usage_log(self) -> list:
        """jackal_usage_log.json 로드. 없으면 빈 리스트."""
        if not _USAGE_LOG.exists():
            return []
        try:
            return json.loads(_USAGE_LOG.read_text(encoding="utf-8"))
        except Exception:
            return []


# ══════════════════════════════════════════════════════════════════
# [Bug Fix] log_usage() 헬퍼 — 모듈 최하단 (클래스 외부)
# Hunter / Scanner / Evolution에서 import해서 API 호출 후 실행
# ══════════════════════════════════════════════════════════════════

def log_usage(caller: str, input_tokens: int, output_tokens: int,
              model: str = "unknown") -> None:
    """
    API 호출 토큰을 jackal_usage_log.json에 기록.
    Shield._check_budget()이 이 파일을 읽어 실비용을 추적한다.

    사용법:
        from jackal_shield import log_usage
        resp = client.messages.create(model=MODEL_H, ...)
        log_usage("hunter_stage3", resp.usage.input_tokens,
                   resp.usage.output_tokens, model=MODEL_H)

    Args:
        caller: 호출 위치 식별자 (예: "hunter_stage3", "evolution_review")
        input_tokens: resp.usage.input_tokens
        output_tokens: resp.usage.output_tokens
        model: 사용한 모델명 (비용 정확도 향상)
    """
    # 모델별 단가 (USD/token)
    _PRICES = {
        "claude-haiku-4-5-20251001":  (0.0000008,  0.000004),   # $0.80/$4.00 per M
        "claude-sonnet-4-6":          (0.000003,   0.000015),   # $3.00/$15.00 per M
        "claude-opus-4-6":            (0.000015,   0.000075),   # $15/$75 per M
    }
    in_price, out_price = _PRICES.get(model, (0.000003, 0.000015))  # 기본값: Sonnet

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
    logs = logs[-2000:]   # 최근 2000건 (약 30일치)
    try:
        _USAGE_LOG.write_text(
            json.dumps(logs, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception as e:
        log.warning(f"usage_log 기록 실패: {e}")


# ─── 단독 실행 ────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
    shield = JackalShield()
    result = shield.scan()

    print(f"\n{'='*50}")
    print("🛡️  Jackal Shield 스캔 결과")
    print(f"{'='*50}")
    if result["issues"]:
        print(f"⚠️  발견된 이슈 {len(result['issues'])}건:")
        for issue in result["issues"]:
            print(f"  {issue}")
    else:
        print("  ✅ 이상 없음")

    print(f"\n  통계:")
    for k, v in result["stats"].items():
        print(f"    {k}: {v:,}" if isinstance(v, int) else f"    {k}: {v}")
    print(f"  abort: {result['abort']}")
    print(f"{'='*50}\n")
