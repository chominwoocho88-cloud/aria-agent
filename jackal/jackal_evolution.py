"""
jackal_evolution.py — Bug Fix Summary
======================================
[Fix 1] MODEL_S 기본값: "claude-sonnet-4-20250514" → "claude-sonnet-4-6"
         + 환경변수 우선순위: ANTHROPIC_MODEL > SUBAGENT_MODEL > 기본값

[Fix 2] DEFAULT_WEIGHTS["signal_weights"] 중복 키 제거 + jackal_weights.json 실 키와 동기화
         - 제거: sector_inflow 중복, golden_cross/fear_regime (실 운용에서 미사용)
         - 추가: momentum_dip, vol_accumulation, sector_rebound, rsi_divergence (실 운용 키)

[Fix 3] _load_weights() 병합 로직: signal_weights를 loaded 우선 보존
         - 기존: DEFAULT로 시작 후 loaded로 update → 학습된 키가 DEFAULT 없으면 유지,
                 DEFAULT에 있으면 learned 값이 보존됨 (실제론 괜찮았으나)
         - 수정: signal_weights만 loaded를 전부 보존하고 DEFAULT 신규 키만 추가
                 → 학습된 가중치가 DEFAULT 재정의로 덮어써지는 엣지케이스 방지

[Fix 4] DEFAULT_WEIGHTS에 last_evolved_at 추가 (Bug Fix 3 연동)

적용 방법: 아래 diff를 jackal/jackal_evolution.py에 적용
"""

# ══════════════════════════════════════════════════════════
# DIFF 1: MODEL_S 선언 (25번째 줄 근처)
# ══════════════════════════════════════════════════════════

DIFF_1 = """
- MODEL_S = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
+ MODEL_S = os.environ.get("ANTHROPIC_MODEL", os.environ.get("SUBAGENT_MODEL", "claude-sonnet-4-6"))
"""

# ══════════════════════════════════════════════════════════
# DIFF 2: DEFAULT_WEIGHTS["signal_weights"] 블록
# ══════════════════════════════════════════════════════════

DIFF_2_BEFORE = '''\
DEFAULT_WEIGHTS = {
    # 신호별 가중치
    "signal_weights": {
        "rsi_oversold":   1.0,
        "bb_touch":       1.0,
        "volume_surge":   1.0,
        "volume_climax":  1.0,
        "ma_support":     1.0,
        "bullish_div":    1.0,
        "sector_inflow":  1.0,
        "golden_cross":   1.0,
        "fear_regime":    1.0,
        "sector_inflow":  1.0,
    },'''

DIFF_2_AFTER = '''\
DEFAULT_WEIGHTS = {
    # 신호별 가중치 — jackal_weights.json 실운용 키와 동기화
    # jackal_scanner.py signals_fired 가능 값 전체 포함
    "signal_weights": {
        # ── 실운용 핵심 키 (jackal_weights.json 기준) ──────────
        "bb_touch":         1.0,   # 볼린저 하단 터치
        "rsi_oversold":     1.0,   # RSI 과매도
        "volume_climax":    1.0,   # 거래량 클라이맥스
        "ma_support":       1.0,   # MA 지지
        "momentum_dip":     1.0,   # 모멘텀 딥
        "vol_accumulation": 1.0,   # 거래량 매집
        "sector_rebound":   1.0,   # 섹터 반등
        "rsi_divergence":   1.0,   # RSI 강세 다이버전스
        # ── 보조 신호 (scanner 프롬프트 후보) ─────────────────
        "sector_inflow":    1.0,   # 섹터 자금 유입  ← 중복 제거됨
        "golden_cross":     1.0,   # 골든 크로스
        "fear_regime":      1.0,   # 공포 레짐 반등
        "bullish_div":      1.0,   # 강세 다이버전스
        "volume_surge":     1.0,   # 거래량 급등
    },'''

# ══════════════════════════════════════════════════════════
# DIFF 3: DEFAULT_WEIGHTS에 last_evolved_at 추가
#         (last_updated 바로 위에 삽입)
# ══════════════════════════════════════════════════════════

DIFF_3_BEFORE = '''\
    "learning_log": [],'''

DIFF_3_AFTER = '''\
    "last_evolved_at": "",   # Bug Fix 3: _should_evolve()가 읽는 타임스탬프
    "learning_log": [],'''

# ══════════════════════════════════════════════════════════
# DIFF 4: _load_weights() 병합 로직
# ══════════════════════════════════════════════════════════

DIFF_4_BEFORE = '''\
    def _load_weights(self) -> dict:
        if not WEIGHTS_FILE.exists():
            return DEFAULT_WEIGHTS.copy()
        try:
            loaded = json.loads(WEIGHTS_FILE.read_text(encoding="utf-8"))
            # 새 키 병합
            merged = DEFAULT_WEIGHTS.copy()
            for k, v in loaded.items():
                if k in merged and isinstance(v, dict) and isinstance(merged[k], dict):
                    merged[k].update(v)
                else:
                    merged[k] = v
            return merged
        except Exception:
            return DEFAULT_WEIGHTS.copy()'''

DIFF_4_AFTER = '''\
    def _load_weights(self) -> dict:
        if not WEIGHTS_FILE.exists():
            return DEFAULT_WEIGHTS.copy()
        try:
            loaded = json.loads(WEIGHTS_FILE.read_text(encoding="utf-8"))
            merged = DEFAULT_WEIGHTS.copy()
            for k, v in loaded.items():
                if k == "signal_weights" and isinstance(v, dict):
                    # signal_weights: loaded(학습값) 우선 보존,
                    # DEFAULT에만 있는 새 신호 키만 추가
                    # → 학습된 가중치를 DEFAULT 기본값으로 덮어쓰지 않음
                    merged["signal_weights"] = {
                        **merged["signal_weights"],  # DEFAULT 신규 키
                        **v,                          # loaded 값이 항상 우선
                    }
                elif k in merged and isinstance(v, dict) and isinstance(merged[k], dict):
                    merged[k].update(v)
                else:
                    merged[k] = v
            return merged
        except Exception:
            return DEFAULT_WEIGHTS.copy()'''

# ══════════════════════════════════════════════════════════
# DIFF 5: _mark_last_evolve() — .last_evolve 대신 weights에 기록
# ══════════════════════════════════════════════════════════

DIFF_5_BEFORE = '''\
    def _mark_last_evolve(self):
        marker = _BASE / ".last_evolve"
        marker.write_text(datetime.now().isoformat())'''

# .last_evolve가 명시적 메서드로 없을 경우 evolve() 말미에 인라인으로 있을 수 있음.
# 검색 범위: "last_evolve" 키워드

DIFF_5_AFTER = '''\
    def _mark_last_evolve(self):
        """
        Evolution 완료 시각을 jackal_weights.json["last_evolved_at"]에 기록.
        .last_evolve 로컬 파일 의존 제거 → GitHub Actions fresh checkout에서 안전.
        """
        self.weights["last_evolved_at"] = datetime.now().isoformat()
        # weights 저장은 evolve() → jackal_core.py에서 save_weights() 호출로 처리됨'''

# ══════════════════════════════════════════════════════════
# 검증 스크립트 (적용 후 실행)
# ══════════════════════════════════════════════════════════

VERIFY_SCRIPT = '''
import ast, sys

with open("jackal/jackal_evolution.py", "r", encoding="utf-8") as f:
    src = f.read()

# 1. 중복 키 검사
tree = ast.parse(src)
# Python 3.12+는 ast에서 중복 키 경고 가능, 하지만 직접 텍스트 검사로도 충분
import re
signal_weights_block = re.search(
    r'"signal_weights"\s*:\s*\{([^}]+)\}', src, re.DOTALL
)
if signal_weights_block:
    keys = re.findall(r'"(\w+)"\s*:', signal_weights_block.group(1))
    dupes = [k for k in keys if keys.count(k) > 1]
    if dupes:
        print(f"❌ 중복 키 여전히 존재: {set(dupes)}")
        sys.exit(1)
    else:
        print(f"✅ signal_weights 중복 없음 ({len(keys)}개 키)")

# 2. 모델명 검사
if "claude-sonnet-4-20250514" in src:
    print("❌ 잘못된 모델명 여전히 존재")
    sys.exit(1)
else:
    print("✅ 모델명 정상")

# 3. last_evolved_at 존재 검사
if "last_evolved_at" in src:
    print("✅ last_evolved_at 추가됨")
else:
    print("❌ last_evolved_at 누락")
    sys.exit(1)

print("\\n✅ jackal_evolution.py 모든 검사 통과")
'''

if __name__ == "__main__":
    print("=== jackal_evolution.py 수정 가이드 ===")
    print()
    print("[DIFF 1] MODEL_S:")
    print(DIFF_1)
    print()
    print("[DIFF 2] DEFAULT_WEIGHTS signal_weights:")
    print("  Before:", DIFF_2_BEFORE[:100], "...")
    print("  After:", DIFF_2_AFTER[:100], "...")
    print()
    print("[DIFF 3] last_evolved_at 추가")
    print("[DIFF 4] _load_weights 병합 로직")
    print("[DIFF 5] _mark_last_evolve")
    print()
    print("=== 검증 스크립트 ===")
    print(VERIFY_SCRIPT)
