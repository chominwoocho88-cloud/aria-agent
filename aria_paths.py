"""
aria_paths.py — ARIA 경로 중앙 관리
"""
from pathlib import Path

_REPO_ROOT  = Path(__file__).parent
DATA_DIR    = _REPO_ROOT / "data"
REPORTS_DIR = _REPO_ROOT / "reports"


def ensure_dirs() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    REPORTS_DIR.mkdir(exist_ok=True)


ensure_dirs()

MEMORY_FILE     = DATA_DIR / "memory.json"
ACCURACY_FILE   = DATA_DIR / "accuracy.json"
SENTIMENT_FILE  = DATA_DIR / "sentiment.json"
ROTATION_FILE   = DATA_DIR / "rotation.json"
WEIGHTS_FILE    = DATA_DIR / "aria_weights.json"
LESSONS_FILE    = DATA_DIR / "aria_lessons.json"
COST_FILE       = DATA_DIR / "aria_cost.json"
PORTFOLIO_FILE  = DATA_DIR / "portfolio.json"
PATTERN_DB_FILE = DATA_DIR / "pattern_db.json"
BASELINE_FILE   = DATA_DIR / "morning_baseline.json"
DATA_FILE       = DATA_DIR / "aria_market_data.json"
BREAKING_FILE   = DATA_DIR / "breaking_sent.json"
DASHBOARD_FILE  = _REPO_ROOT / "dashboard.html"
