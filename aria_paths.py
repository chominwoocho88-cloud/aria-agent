"""
aria_paths.py — ARIA 경로 중앙 관리
모든 파일 경로를 한 곳에서 정의. 경로 변경 시 이 파일만 수정하면 됩니다.
"""
from pathlib import Path

# ── 절대경로 기반 디렉토리 ────────────────────────────────────────────────────
_REPO_ROOT  = Path(__file__).parent
DATA_DIR    = _REPO_ROOT / "data"
REPORTS_DIR = _REPO_ROOT / "reports"


def ensure_dirs() -> None:
    """data/, reports/ 디렉토리 보장"""
    DATA_DIR.mkdir(exist_ok=True)
    REPORTS_DIR.mkdir(exist_ok=True)


ensure_dirs()

# ── 누적 데이터 (Git 추적) ─────────────────────────────────────────────────────
MEMORY_FILE     = DATA_DIR / "memory.json"
ACCURACY_FILE   = DATA_DIR / "accuracy.json"
SENTIMENT_FILE  = DATA_DIR / "sentiment.json"
ROTATION_FILE   = DATA_DIR / "rotation.json"
WEIGHTS_FILE    = DATA_DIR / "aria_weights.json"
LESSONS_FILE    = DATA_DIR / "aria_lessons.json"
COST_FILE       = DATA_DIR / "aria_cost.json"
PORTFOLIO_FILE  = DATA_DIR / "portfolio.json"
PATTERN_DB_FILE = DATA_DIR / "pattern_db.json"

# ── 런타임 임시 파일 (Git 무시 권장) ───────────────────────────────────────────
BASELINE_FILE  = DATA_DIR / "morning_baseline.json"
DATA_FILE      = DATA_DIR / "aria_market_data.json"
BREAKING_FILE  = DATA_DIR / "breaking_sent.json"

# ── 출력 파일 ─────────────────────────────────────────────────────────────────
DASHBOARD_FILE = _REPO_ROOT / "dashboard.html"
