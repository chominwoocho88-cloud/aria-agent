"""
aria_reset.py — ARIA / Jackal 학습 데이터 초기화 스크립트

GitHub Actions 또는 로컬에서 실행:
  python aria_reset.py --aria               # ARIA 데이터만 초기화
  python aria_reset.py --jackal             # Jackal 데이터만 초기화
  python aria_reset.py --aria --jackal      # 전체 초기화
  python aria_reset.py --aria --dry-run     # 어떤 파일이 초기화되는지 미리보기
"""
import argparse
import json
import shutil
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT      = Path(__file__).parent
DATA_DIR  = ROOT / "data"
JACKAL_DIR = ROOT / "jackal"

KST = timezone(timedelta(hours=9))

def _now_str() -> str:
    return datetime.now(KST).isoformat()


# ── 초기 기본값 정의 ──────────────────────────────────────────────────────────

ARIA_ACCURACY_DEFAULT = {
    "total": 0, "correct": 0, "by_category": {},
    "history": [], "history_by_category": [],
    "weak_areas": [], "strong_areas": [],
    "dir_total": 0, "dir_correct": 0, "dir_accuracy_pct": 0,
    "score_total": 0.0, "score_earned": 0.0, "score_accuracy": 0.0,
    "_reset_at": "",
    "_note": "aria_reset.py 실행으로 초기화됨",
}

ARIA_WEIGHTS_DEFAULT = {
    "version": 1,
    "last_updated": "",
    "total_learning_cycles": 0,
    "sentiment": {
        "시장레짐": 1.0, "추세방향": 1.0, "변동성지수": 1.2,
        "자금흐름": 1.0, "반론강도": 1.0, "한국시장": 0.8, "숨은시그널": 0.7,
    },
    "prediction_confidence": {
        "금리": 0.8, "환율": 0.5, "주식": 1.0,
        "지정학": 0.7, "원자재": 0.9, "기업": 1.0, "기타": 0.6,
        "VIX": 0.3,   # 백테스트 22.8% → 낮은 초기값
    },
    "learning_log": [],
    "component_accuracy": {
        "시장레짐":   {"correct": 0, "total": 0},
        "추세방향":   {"correct": 0, "total": 0},
        "변동성지수": {"correct": 0, "total": 0},
        "자금흐름":   {"correct": 0, "total": 0},
    },
    "_reset_at": "",
    "_note": "VIX 0.3 / 환율 0.5 — 백테스트 실증 초기값",
}

ARIA_LESSONS_DEFAULT = {
    "lessons": [], "total_lessons": 0, "last_updated": "",
    "_reset_at": "",
}

ARIA_SENTIMENT_DEFAULT = {
    "history": [], "current": None, "trend": {}, "last_updated": "",
    "_reset_at": "",
}

ARIA_ROTATION_DEFAULT = {
    "ranking": [], "rotation_signal": {}, "history": [], "last_updated": "",
    "_reset_at": "",
}

JACKAL_WEIGHTS_DEFAULT = {
    "signal_weights": {
        "bb_touch": 1.0, "rsi_oversold": 1.0, "volume_climax": 1.0,
        "ma_support": 1.0, "momentum_dip": 1.0, "vol_accumulation": 1.0,
        "sector_rebound": 1.0, "rsi_divergence": 1.0, "sector_inflow": 1.0,
        "golden_cross": 1.0, "fear_regime": 1.0, "bullish_div": 1.0,
        "volume_surge": 1.0,
    },
    "regime_weights": {
        "위험선호": 1.1, "혼조": 1.0, "위험회피": 0.8, "전환중": 0.9,
    },
    "devil_weights": {
        "동의": 1.1, "부분동의": 0.9, "반대": 0.6,
    },
    "signal_accuracy": {}, "regime_accuracy": {},
    "ticker_accuracy": {}, "devil_accuracy": {
        "동의":     {"correct": 0, "total": 0},
        "부분동의": {"correct": 0, "total": 0},
        "반대":     {"correct": 0, "total": 0},
    },
    "last_updated": "", "last_evolved_at": "",
    "last_macro_gate": {},
    "rule_registry_status": {
        "sector_rebound_base":   {"active": True, "min_accuracy": 0.75, "recent_accuracy": 0.0, "sample_n": 0, "review_after_n": 50},
        "volume_climax_base":    {"active": True, "min_accuracy": 0.65, "recent_accuracy": 0.0, "sample_n": 0, "review_after_n": 20},
        "ma_support_solo_pen":   {"active": True, "min_accuracy": None, "recent_accuracy": 0.0, "sample_n": 0, "review_after_n": 30},
        "crash_rebound_pattern": {"active": True, "min_accuracy": 0.70, "recent_accuracy": 0.0, "sample_n": 0, "review_after_n": 30},
        "heuristic_gate":        {"active": True, "min_accuracy": None, "recent_accuracy": 0.0, "sample_n": 0, "review_after_n": 30},
    },
    "_reset_at": "",
    "_note": "aria_reset.py 실행으로 초기화됨",
}


# ── 초기화 함수 ───────────────────────────────────────────────────────────────

def _write(path: Path, data: dict, dry: bool) -> None:
    now = _now_str()
    if "_reset_at" in data:
        data["_reset_at"] = now
    if dry:
        print(f"  [DRY] {path.relative_to(ROOT)}")
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        # .bak 백업
        if path.exists():
            bak = path.with_suffix(".json.bak")
            shutil.copy2(path, bak)
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"  ✅ {path.relative_to(ROOT)}")


def reset_aria(dry: bool = False) -> None:
    print("\n🔄 ARIA 데이터 초기화")
    DATA_DIR.mkdir(exist_ok=True)

    _write(DATA_DIR / "accuracy.json",     ARIA_ACCURACY_DEFAULT.copy(), dry)
    _write(DATA_DIR / "aria_lessons.json", ARIA_LESSONS_DEFAULT.copy(),  dry)
    _write(DATA_DIR / "aria_weights.json", ARIA_WEIGHTS_DEFAULT.copy(),  dry)
    _write(DATA_DIR / "sentiment.json",    ARIA_SENTIMENT_DEFAULT.copy(), dry)
    _write(DATA_DIR / "rotation.json",     ARIA_ROTATION_DEFAULT.copy(), dry)

    # [3-file lesson system] 분리 파일도 초기화
    empty_lessons = {"lessons": [], "last_updated": "", "_reset_at": ""}
    for fname in ("lessons_failure.json", "lessons_strength.json"):
        _write(DATA_DIR / fname, empty_lessons.copy(), dry)
    _write(DATA_DIR / "lessons_regime.json",
           {"regimes": {}, "last_updated": "", "_reset_at": ""}, dry)

    # memory.json은 list 타입이라 별도 처리
    if not dry:
        mem_path = DATA_DIR / "memory.json"
        if mem_path.exists():
            shutil.copy2(mem_path, mem_path.with_suffix(".json.bak"))
        mem_path.write_text("[]", encoding="utf-8")
        print(f"  ✅ data/memory.json")

    # pattern_db.json도 초기화
    pdb = DATA_DIR / "pattern_db.json"
    if pdb.exists():
        _write(pdb, {}, dry)

    print(f"  {'[DRY] ' if dry else ''}초기화 완료 — 기존 파일 .bak 백업됨")


def reset_jackal(dry: bool = False) -> None:
    print("\n🦊 Jackal 데이터 초기화")
    JACKAL_DIR.mkdir(exist_ok=True)

    # hunt_log.json
    hl = JACKAL_DIR / "hunt_log.json"
    if dry:
        print(f"  [DRY] jackal/hunt_log.json")
    else:
        if hl.exists(): shutil.copy2(hl, hl.with_suffix(".json.bak"))
        hl.write_text("[]", encoding="utf-8")
        print(f"  ✅ jackal/hunt_log.json")

    # hunt_cooldown.json
    hc = JACKAL_DIR / "hunt_cooldown.json"
    if dry:
        print(f"  [DRY] jackal/hunt_cooldown.json")
    else:
        hc.write_text("{}", encoding="utf-8")
        print(f"  ✅ jackal/hunt_cooldown.json")

    # jackal_weights.json
    _write(JACKAL_DIR / "jackal_weights.json", JACKAL_WEIGHTS_DEFAULT.copy(), dry)

    # skills/, lessons/ 비우기
    for subdir in ["skills", "lessons"]:
        d = JACKAL_DIR / subdir
        if d.exists():
            files = list(d.glob("*.json"))
            if dry:
                print(f"  [DRY] jackal/{subdir}/ ({len(files)}개 파일 삭제 예정)")
            else:
                for f in files:
                    f.unlink()
                print(f"  ✅ jackal/{subdir}/ ({len(files)}개 파일 삭제)")

    # usage_log 초기화
    ul = JACKAL_DIR / "jackal_usage_log.json"
    if ul.exists():
        if dry:
            print(f"  [DRY] jackal/jackal_usage_log.json")
        else:
            if ul.exists(): shutil.copy2(ul, ul.with_suffix(".json.bak"))
            ul.write_text("[]", encoding="utf-8")
            print(f"  ✅ jackal/jackal_usage_log.json")

    print(f"  {'[DRY] ' if dry else ''}초기화 완료")


# ── 엔트리포인트 ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="ARIA/Jackal 학습 데이터 초기화")
    parser.add_argument("--aria",    action="store_true", help="ARIA 데이터 초기화")
    parser.add_argument("--jackal",  action="store_true", help="Jackal 데이터 초기화")
    parser.add_argument("--dry-run", action="store_true", help="실제 파일 수정 없이 미리보기")
    args = parser.parse_args()

    if not args.aria and not args.jackal:
        parser.print_help()
        print("\n⚠️  --aria 또는 --jackal 중 하나 이상 지정하세요.")
        return

    dry = args.dry_run
    if dry:
        print("\n🔍 DRY RUN — 파일을 수정하지 않습니다")

    if args.aria:
        reset_aria(dry=dry)
    if args.jackal:
        reset_jackal(dry=dry)

    print("\n✅ 완료" + (" (DRY RUN — 실제 변경 없음)" if dry else ""))
    if not dry:
        print("   다음 단계: aria_backtest.py --months 6 실행 권장")


if __name__ == "__main__":
    main()
