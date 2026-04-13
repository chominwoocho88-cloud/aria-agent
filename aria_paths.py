"""
aria_paths.py — ARIA 경로 중앙 관리
모든 파일 경로를 한 곳에서 정의. 경로 변경 시 이 파일만 수정하면 됩니다.
"""
from pathlib import Path

# ── 디렉토리 ──────────────────────────────────────────────────────────────────
DATA_DIR    = Path("data")       # 상태 JSON 저장소
REPORTS_DIR = Path("reports")   # 일별 리포트 (분석 아카이브)

DATA_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)

# ── 누적 데이터 (Git 추적) ─────────────────────────────────────────────────────
MEMORY_FILE    = DATA_DIR / "memory.json"
ACCURACY_FILE  = DATA_DIR / "accuracy.json"
SENTIMENT_FILE = DATA_DIR / "sentiment.json"
ROTATION_FILE  = DATA_DIR / "rotation.json"
WEIGHTS_FILE   = DATA_DIR / "aria_weights.json"
LESSONS_FILE   = DATA_DIR / "aria_lessons.json"
COST_FILE      = DATA_DIR / "aria_cost.json"
PORTFOLIO_FILE = DATA_DIR / "portfolio.json"
PATTERN_DB_FILE = DATA_DIR / "pattern_db.json"

# ── 런타임 임시 파일 (Git 무시 권장) ───────────────────────────────────────────
BASELINE_FILE  = DATA_DIR / "morning_baseline.json"   # 당일용
DATA_FILE      = DATA_DIR / "aria_market_data.json"   # 매 실행 덮어쓰기
BREAKING_FILE  = DATA_DIR / "breaking_sent.json"      # 속보 중복 방지용

# ── 출력 파일 ─────────────────────────────────────────────────────────────────
DASHBOARD_FILE = Path("dashboard.html")               # GitHub Pages 서빙
