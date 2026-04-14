"""
aria_main.py — ARIA 메인 오케스트레이터 (Jackal 통합 버전)
Hunter → Analyst → Devil → Reporter + Jackal 자동 성장 엔진
rich 미사용 - 순수 print로 동작
"""
import os
import sys
import json
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

from aria_agents   import agent_hunter, agent_analyst, agent_devil, agent_reporter
from aria_analysis import (
    run_sentiment, run_portfolio, run_rotation,
    save_baseline, get_regime_drift,
    run_verification, build_lessons_prompt, extract_dawn_lessons,
)
from aria_notify   import (
    send_start_notification, send_report, send_error,
)
from aria_data     import (
    fetch_all_market_data, update_cost, get_monthly_cost_summary,
)

# ─── 기본 설정 ────────────────────────────────────────────────────
KST         = timezone(timedelta(hours=9))
MEMORY_FILE = Path("memory.json")
REPORTS_DIR = Path("reports")
MODE        = os.environ.get("ARIA_MODE", "MORNING")


# ─── 출력 유틸 (rich 없이) ────────────────────────────────────────
def _now() -> datetime:
    return datetime.now(KST)

def _box(title: str, body: str = ""):
    w = 54
    print("\n" + "=" * w)
    print(f"  {title}")
    if body:
        for line in body.split("\n"):
            print(f"  {line}")
    print("=" * w)

def _table(headers: list, rows: list, title: str = ""):
    if title:
        print(f"\n  [{title}]")
    col_w = [max(len(h), max((len(str(r[i])) for r in rows), default=0)) for i, h in enumerate(headers)]
    fmt   = "  " + "  ".join(f"{{:<{w}}}" for w in col_w)
    print(fmt.format(*headers))
    print("  " + "-" * (sum(col_w) + len(col_w) * 2))
    for row in rows:
        print(fmt.format(*[str(v) for v in row]))


# ─── 메모리 관리 ─────────────────────────────────────────────────
def load_memory() -> list:
    if not MEMORY_FILE.exists():
        return []
    try:
        data = json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        backup = MEMORY_FILE.with_suffix(".bak.json")
        MEMORY_FILE.rename(backup)
        print(f"⚠️  memory.json 손상 → {backup} 백업 후 초기화")
        return []

def save_memory(memory: list, analysis: dict):
    memory = [m for m in memory if m.get("analysis_date") != analysis.get("analysis_date")]
    memory = (memory + [analysis])[-90:]
    MEMORY_FILE.write_text(json.dumps(memory, ensure_ascii=False, indent=2), encoding="utf-8")

def save_report(analysis: dict) -> Path:
    REPORTS_DIR.mkdir(exist_ok=True)
    date = analysis.get("analysis_date", _now().strftime("%Y-%m-%d"))
    mode = analysis.get("mode", "MORNING").lower()
    path = REPORTS_DIR / f"{date}_{mode}.json"
    path.write_text(json.dumps(analysis, ensure_ascii=False, indent=2), encoding="utf-8")
    return path

def get_todays_analyses() -> list:
    today = _now().strftime("%Y-%m-%d")
    reports = []
    if REPORTS_DIR.exists():
        for f in REPORTS_DIR.glob(f"{today}_*.json"):
            try:
                reports.append(json.loads(f.read_text(encoding="utf-8")))
            except Exception:
                pass
    return reports


# ─── 리포트 출력 ──────────────────────────────────────────────────
def print_report(report: dict, run_n: int):
    regime    = report.get("market_regime", "?")
    signal    = report.get("signal", "?")
    conf      = report.get("confidence_overall", "?")
    one_liner = report.get("one_line_summary", "")
    agreements= report.get("agent_agreements", [])
    conflicts = report.get("agent_conflicts", [])
    actions   = report.get("recommended_actions", [])

    _box(
        f"ARIA {MODE} #{run_n}",
        f"레짐: {regime}  |  신호: {signal}  |  확신도: {conf}\n{one_liner}",
    )

    if agreements:
        print("\n  ✅ 에이전트 합의:")
        for a in agreements:
            print(f"    • {a}")

    if conflicts:
        print("\n  ⚡ 에이전트 이견:")
        for c in conflicts:
            print(f"    • {c}")

    if actions:
        rows = [(a.get("ticker",""), a.get("direction",""), a.get("rationale","")) for a in actions]
        _table(["종목", "방향", "근거"], rows, title="추천 액션")


# ─── Jackal 실행 ──────────────────────────────────────────────────
def run_jackal(total_tokens: int = 0):
    if os.environ.get("JACKAL_AUTOCOMPACT", "true").lower() == "false":
        return None

    jackal_dir = Path(__file__).parent / "jackal"
    if not jackal_dir.exists():
        print("Jackal: jackal/ 디렉토리 없음 → skip")
        return None

    try:
        sys.path.insert(0, str(jackal_dir))
        from jackal_core import JackalCore
        print("\n🦊 Jackal 자동 성장 엔진 시작")
        result = JackalCore().run(context_tokens=total_tokens, force_evolve=False)
        return result
    except ImportError as e:
        print(f"Jackal import 실패: {e}")
    except Exception as e:
        print(f"Jackal 실행 오류 (ARIA는 정상 완료): {e}")
    return None


# ─── 메인 파이프라인 ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="ARIA Multi-Agent Pipeline")
    parser.add_argument("--mode", default=None, help="DAWN/MORNING/AFTERNOON/EVENING")
    parser.add_argument("--dry-run", action="store_true", help="Telegram 전송 없이 출력만")
    args = parser.parse_args()

    global MODE
    if args.mode:
        MODE = args.mode.upper()

    today = _now().strftime("%Y-%m-%d")

    # 중복 실행 방지
    duplicate = REPORTS_DIR / f"{today}_{MODE.lower()}.json"
    if duplicate.exists() and not args.dry_run:
        print(f"⚠️  오늘 {MODE} 이미 실행됨 → skip")
        return

    _box(
        "ARIA Multi-Agent Pipeline",
        f"Mode: {MODE}  |  {_now().strftime('%Y-%m-%d %H:%M KST')}\nHunter → Analyst → Devil → Reporter",
    )

    memory = load_memory()
    total_tokens = 0

    try:
        # ── 실시간 데이터 수집 ──────────────────────────────────────
        print("\n📡 실시간 시장 데이터 수집")
        market_data = fetch_all_market_data()
        update_cost(MODE)
        print(get_monthly_cost_summary())

        # ── DAWN: 교훈 추출 ─────────────────────────────────────────
        if MODE == "DAWN":
            try:
                todays = get_todays_analyses()
                if todays:
                    extract_dawn_lessons(todays, "market outcomes today")
                    print("Lessons: 오늘 교훈 추출 완료")
            except Exception as e:
                print(f"교훈 추출 오류: {e}")

        # ── MORNING: 어제 예측 채점 ─────────────────────────────────
        accuracy = {}
        if MODE == "MORNING":
            print("\n📊 어제 예측 채점")
            accuracy = run_verification()

        # ── 교훈 로드 ───────────────────────────────────────────────
        lessons_prompt = ""
        if MODE == "MORNING":
            try:
                lessons_prompt = build_lessons_prompt()
                if lessons_prompt:
                    print("Lessons: 교훈 주입 완료")
            except Exception as e:
                print(f"교훈 로드 오류: {e}")

        # ── Telegram 시작 알림 ──────────────────────────────────────
        if not args.dry_run:
            send_start_notification()

        # ── 4-에이전트 파이프라인 ────────────────────────────────────
        print("\n--- 에이전트 파이프라인 실행 ---")
        hunter  = agent_hunter(today, MODE, market_data)
        analyst = agent_analyst(hunter, MODE, lessons_prompt)
        devil   = agent_devil(analyst, memory, MODE)
        report  = agent_reporter(hunter, analyst, devil, memory, accuracy, MODE)

        # ── 보조 분석 ────────────────────────────────────────────────
        print("\n📈 보조 분석 실행")
        sentiment = run_sentiment(market_data)
        portfolio = run_portfolio(report)
        rotation  = run_rotation(market_data)
        drift     = get_regime_drift(memory, report)

        report["sentiment"]    = sentiment
        report["portfolio"]    = portfolio
        report["rotation"]     = rotation
        report["regime_drift"] = drift

        # ── 저장 + 출력 ──────────────────────────────────────────────
        print_report(report, len(memory) + 1)
        path = save_report(report)
        save_memory(memory, report)
        save_baseline(report)
        print(f"\n💾 저장: {path}")

        # ── Telegram 리포트 전송 ─────────────────────────────────────
        if not args.dry_run:
            send_report(report, len(memory) + 1)

        # ── 토큰 추산 ────────────────────────────────────────────────
        total_tokens = sum([
            len(json.dumps(hunter,  ensure_ascii=False)),
            len(json.dumps(analyst, ensure_ascii=False)),
            len(json.dumps(devil,   ensure_ascii=False)),
            len(json.dumps(report,  ensure_ascii=False)),
        ]) // 4

    except Exception as e:
        print(f"❌ 오류: {e}")
        if not args.dry_run:
            send_error(str(e), MODE)
        raise

    # ── Jackal ───────────────────────────────────────────────────
    jackal_result = run_jackal(total_tokens=total_tokens)
    if jackal_result:
        status = jackal_result.get("status", "")
        if status == "ok":
            print("🦊 Jackal 완료 - ARIA가 스스로 학습했습니다.")
        elif status == "aborted":
            print("⛔ Jackal Shield 경고 - 로그를 확인하세요.")

    print(f"\n✅ ARIA {MODE} 완료")


if __name__ == "__main__":
    main()
