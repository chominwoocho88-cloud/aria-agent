"""
jackal_evolution.py
Jackal Evolution — 강화된 자체 학습

학습 데이터:
  - 신호별 정확도:  rsi_oversold가 맞은 횟수/전체
  - 레짐별 정확도:  위험선호 레짐에서 매수 신호가 맞은 횟수
  - Devil 정확도:  Devil이 반대했을 때 실제로 실패한 횟수
  - 티커별 정확도: 종목별 타점 적중률
  - 주간 패턴 리뷰: Claude Sonnet이 전체 데이터 분석 → Skill/Instinct 생성
"""

import json
import re
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

import yfinance as yf
from anthropic import Anthropic

log = logging.getLogger("jackal_evolution")

_BASE         = Path(__file__).parent
WEIGHTS_FILE  = _BASE / "jackal_weights.json"
HUNT_LOG_FILE = _BASE / "hunt_log.json"         # Hunter 결과 (새 구조)
SCAN_LOG_FILE = _BASE / "scan_log.json"         # 레거시 (하위 호환)
SKILLS_DIR    = _BASE / "skills"
LESSONS_DIR   = _BASE / "lessons"

SKILLS_DIR.mkdir(exist_ok=True)
LESSONS_DIR.mkdir(exist_ok=True)

MODEL_S = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")

OUTCOME_HOURS      = 4
SUCCESS_PCT        = 0.5    # 이 % 이상 올라야 성공
WEIGHT_ADJUST_UP   = 0.04
WEIGHT_ADJUST_DOWN = 0.03
WEIGHT_MIN         = 0.3
WEIGHT_MAX         = 2.5

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
    },
    # 레짐별 신뢰도 보정
    "regime_weights": {
        "위험선호": 1.1,
        "혼조":     1.0,
        "위험회피": 0.8,
        "전환중":   0.9,
    },
    # Devil 판정별 신뢰도
    "devil_weights": {
        "동의":     1.1,
        "부분동의": 0.9,
        "반대":     0.6,
    },
    # 신호별 정확도 (학습으로 채워짐)
    "signal_accuracy": {},
    # 레짐별 정확도
    "regime_accuracy": {},
    # 티커별 정확도
    "ticker_accuracy": {},
    # Devil 판정 정확도
    "devil_accuracy": {
        "동의":     {"correct": 0, "total": 0},
        "부분동의": {"correct": 0, "total": 0},
        "반대":     {"correct": 0, "total": 0},
    },
    "last_updated": "",
}


class JackalEvolution:

    def __init__(self):
        self.client  = Anthropic()
        self.weights = self._load_weights()
        self._logs: list = []

    def evolve(self) -> dict:
        log.info("🧬 Evolution 시작")

        # 1. 타점 알림 결과 확인 + 학습
        learn_result = self._learn_from_outcomes()

        # 2. ARIA 추천 종목 결과 확인 + 학습 (24h 경과)
        rec_result = self._learn_from_recommendations()

        # 3. Claude Sonnet 주간 패턴 리뷰
        context  = self._build_context(rec_result)
        raw      = self._ask_claude(context)
        analysis = self._parse_response(raw)

        self._save_skills(analysis.get("new_skills", []))
        self._save_instincts(analysis.get("new_instincts", []))
        self._apply_claude_adjustments(analysis)
        self._mark_last_evolve()

        self.weights["last_updated"] = datetime.now().isoformat()
        self._save_weights()

        log.info(f"🧬 완료 | 타점학습 {learn_result['learned']}건 | "
                 f"추천학습 {rec_result['learned']}건 | "
                 f"Skill {len(analysis.get('new_skills',[]))}개")

        return {
            "learned":            learn_result["learned"],
            "rec_learned":        rec_result["learned"],
            "weight_changes":     learn_result["changes"],
            "new_skills":         analysis.get("new_skills", []),
            "new_instincts":      analysis.get("new_instincts", []),
            "improvements":       analysis.get("prompt_improvements", ""),
            "accuracy_summary":   learn_result.get("accuracy_summary", {}),
            "rec_accuracy":       rec_result.get("accuracy", {}),
        }

    def save_weights(self):
        self._save_weights()

    # ══════════════════════════════════════════════════════════════
    # 결과 학습 — 신호별 / 레짐별 / Devil / 티커별
    # ══════════════════════════════════════════════════════════════

    def _learn_from_outcomes(self) -> dict:
        """
        두 가지 학습 포인트:
        ① 1일 정확도: 다음날 방향이 맞았는가 (진입 타이밍)
        ② 스윙 정확도: 3~7일 내 Peak +1% 이상 (보유 기간 성공)
        """
        log_file = HUNT_LOG_FILE if HUNT_LOG_FILE.exists() else SCAN_LOG_FILE
        if not log_file.exists():
            return {"learned": 0, "changes": [], "accuracy_summary": {}}

        try:
            self._logs = json.loads(log_file.read_text(encoding="utf-8"))
        except Exception:
            return {"learned": 0, "changes": [], "accuracy_summary": {}}

        cutoff_1d   = datetime.now() - timedelta(hours=28)   # 1일 체크: 28시간 후
        cutoff_swing = datetime.now() - timedelta(days=8)    # 스윙 체크: 8일 후

        pending = [
            e for e in self._logs
            if (e.get("alerted") or e.get("is_entry"))
            and not e.get("outcome_checked")
            and datetime.fromisoformat(e.get("timestamp", "2000-01-01")) < cutoff_1d
        ]

        learned  = 0
        changes  = []
        sig_acc  = defaultdict(lambda: {"correct": 0, "total": 0})
        reg_acc  = defaultdict(lambda: {"correct": 0, "total": 0})
        tkr_acc  = defaultdict(lambda: {"correct": 0, "total": 0})
        dev_acc  = defaultdict(lambda: {"correct": 0, "total": 0})

        stype_acc  = defaultdict(lambda: {
            "correct": 0, "total": 0, "peak_days": [], "peak_pcts": [],
            # 두 학습 포인트 추가
            "d1_correct": 0, "d1_total": 0,
            "swing_correct": 0, "swing_total": 0,
        })

        for entry in pending:
            try:
                ticker      = entry["ticker"]
                price_entry = entry.get("price_at_hunt") or entry.get("price_at_scan", 0)
                if not price_entry:
                    continue

                now         = datetime.now()
                entry_ts    = datetime.fromisoformat(entry["timestamp"])
                days_since  = (now - entry_ts).days

                hist = yf.Ticker(ticker).history(period="15d", interval="1d")
                if hist.empty or len(hist) < 2:
                    continue

                future = hist[hist.index > entry_ts.strftime("%Y-%m-%d")]
                if future.empty:
                    future = hist.iloc[-min(10, len(hist)):]

                closes  = [float(c) for c in future["Close"]]
                returns = [(c - price_entry) / price_entry * 100 for c in closes]
                if not returns:
                    continue

                # ── ① 1일 정확도 ──────────────────────────────
                d1_pct     = returns[0] if len(returns) >= 1 else None
                d1_correct = (d1_pct > 0.3) if d1_pct is not None else None

                entry["price_1d_later"]  = round(closes[0], 4) if closes else None
                entry["outcome_1d_pct"]  = round(d1_pct, 2) if d1_pct is not None else None
                entry["outcome_1d_hit"]  = d1_correct

                # ── ② 스윙 정확도 (3~7일 Peak) ────────────────
                swing_window = min(7, len(returns))
                swing_returns = returns[:swing_window]
                peak_pct  = max(swing_returns) if swing_returns else 0
                peak_day  = swing_returns.index(peak_pct) + 1 if swing_returns else 1
                swing_hit = peak_pct >= 1.0   # +1% 이상 = 스윙 성공

                # 스윙 체크는 7일 이상 경과 후에만 확정
                swing_checked = days_since >= 7

                entry["price_peak"]        = round(closes[peak_day-1], 4) if closes else None
                entry["peak_day"]          = peak_day
                entry["peak_pct"]          = round(peak_pct, 2)
                entry["outcome_swing_hit"] = swing_hit if swing_checked else None

                # outcome_checked: 1일은 즉시, 스윙은 7일 후
                entry["outcome_checked"] = swing_checked
                # 레거시 필드 호환
                entry["outcome_pct"]     = round(returns[-1], 2) if returns else None
                entry["outcome_correct"] = swing_hit

                stype   = entry.get("swing_type", "기술적과매도")
                regime  = entry.get("aria_regime", "")
                verdict = entry.get("devil_verdict", "")

                # ── 신호별 정확도 (스윙 기준) ─────────────────
                for sig in entry.get("signals_fired", []):
                    sig_acc[sig]["total"]   += 1
                    sig_acc[sig]["correct"] += int(swing_hit)

                # ── 스윙 타입별 — 두 포인트 모두 ───────────────
                stype_acc[stype]["total"]        += 1
                stype_acc[stype]["correct"]      += int(swing_hit)
                stype_acc[stype]["peak_days"].append(peak_day)
                stype_acc[stype]["peak_pcts"].append(peak_pct)
                stype_acc[stype]["swing_total"]  += 1
                stype_acc[stype]["swing_correct"] += int(swing_hit)
                if d1_correct is not None:
                    stype_acc[stype]["d1_total"]   += 1
                    stype_acc[stype]["d1_correct"]  += int(d1_correct)

                # ── 레짐별 ────────────────────────────────────
                if regime:
                    reg_acc[regime]["total"]   += 1
                    reg_acc[regime]["correct"] += int(swing_hit)

                # ── 티커별 ────────────────────────────────────
                tkr_acc[ticker]["total"]   += 1
                tkr_acc[ticker]["correct"] += int(swing_hit)

                # ── Devil 정확도 ──────────────────────────────
                if verdict:
                    dev_acc[verdict]["total"]   += 1
                    dev_acc[verdict]["correct"] += int(not swing_hit if verdict == "반대" else swing_hit)

                # ── 가중치 조정 (스윙 + 1일 복합) ─────────────
                # 스윙 성공: +0.04 / 1일도 맞음: 추가 +0.01
                if swing_checked:
                    adj = WEIGHT_ADJUST_UP if swing_hit else -WEIGHT_ADJUST_DOWN
                    if d1_correct:
                        adj += 0.01   # 1일도 맞았으면 추가 보너스
                    sw_w = self.weights["signal_weights"]
                    for sig in entry.get("signals_fired", []):
                        if sig in sw_w:
                            old_w = sw_w[sig]
                            new_w = round(max(WEIGHT_MIN, min(WEIGHT_MAX, old_w + adj)), 4)
                            sw_w[sig] = new_w
                            if abs(old_w - new_w) > 0.001:
                                changes.append(
                                    f"{sig}: {old_w:.3f}→{new_w:.3f} "
                                    f"[{ticker} 1일:{'+' if d1_correct else '-'} "
                                    f"스윙D{peak_day} {peak_pct:+.1f}%]"
                                )

                learned += 1
                d1_str  = f"{d1_pct:+.1f}%" if d1_pct is not None else "대기"
                log.info(
                    f"  {ticker} [{stype}]: "
                    f"1일 {d1_str} {'✅' if d1_correct else '❌' if d1_correct is not None else '⏳'} | "
                    f"스윙 Peak D{peak_day} {peak_pct:+.1f}% {'✅' if swing_hit else '❌'}"
                    + (" (확정)" if swing_checked else " (집계중)")
                )

            except Exception as e:
                log.error(f"  학습 실패: {e}")

        # ── 스윙 타입별 두 정확도 저장 ────────────────────────────
        opt_days = self.weights.setdefault("swing_type_optimal", {})
        for stype, v in stype_acc.items():
            if not v["peak_days"]:
                continue
            avg_peak  = round(sum(v["peak_days"]) / len(v["peak_days"]), 1)
            avg_gain  = round(sum(v["peak_pcts"]) / len(v["peak_pcts"]), 2)
            swing_acc = round(v["swing_correct"] / v["swing_total"] * 100, 1) if v["swing_total"] else 0
            d1_acc    = round(v["d1_correct"]    / v["d1_total"]    * 100, 1) if v["d1_total"]    else 0

            opt_days[stype] = {
                "avg_peak_day":   avg_peak,
                "avg_peak_gain":  avg_gain,
                "swing_accuracy": swing_acc,   # 3~7일 스윙 성공률
                "day1_accuracy":  d1_acc,       # 1일 방향 정확도
                "sample":         v["total"],
            }
            log.info(
                f"  [{stype}] Peak D{avg_peak} ({avg_gain:+.2f}%) | "
                f"1일 {d1_acc}% | 스윙 {swing_acc}%"
            )

        # ── 누적 정확도 업데이트 ───────────────────────────────
        self._update_accuracy("signal_accuracy",  sig_acc)
        self._update_accuracy("regime_accuracy",  reg_acc)
        self._update_accuracy("ticker_accuracy",  tkr_acc)
        self._update_devil_accuracy(dev_acc)

        # 로그 저장
        if pending:
            log_file.write_text(
                json.dumps(self._logs, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        # 정확도 요약
        acc_summary = {}
        for key in ["signal_accuracy", "regime_accuracy", "ticker_accuracy"]:
            acc = self.weights.get(key, {})
            acc_summary[key] = {
                k: {"accuracy": round(v["correct"]/v["total"]*100, 1),
                    "total": v["total"]}
                for k, v in acc.items() if v.get("total", 0) >= 3
            }

        return {"learned": learned, "changes": changes, "accuracy_summary": acc_summary}

    def _update_accuracy(self, key: str, new_data: dict):
        """누적 정확도 딕셔너리 업데이트"""
        acc = self.weights.setdefault(key, {})
        for k, v in new_data.items():
            if k not in acc:
                acc[k] = {"correct": 0, "total": 0}
            acc[k]["correct"] += v["correct"]
            acc[k]["total"]   += v["total"]
            acc[k]["accuracy"] = round(
                acc[k]["correct"] / acc[k]["total"] * 100, 1
            ) if acc[k]["total"] > 0 else 0

    def _update_devil_accuracy(self, new_data: dict):
        da = self.weights.setdefault("devil_accuracy", {})
        for verdict, v in new_data.items():
            if verdict not in da:
                da[verdict] = {"correct": 0, "total": 0}
            da[verdict]["correct"] += v["correct"]
            da[verdict]["total"]   += v["total"]
            da[verdict]["accuracy"] = round(
                da[verdict]["correct"] / da[verdict]["total"] * 100, 1
            ) if da[verdict]["total"] > 0 else 0

    # ══════════════════════════════════════════════════════════════
    # Claude Sonnet 주간 패턴 리뷰
    # ══════════════════════════════════════════════════════════════

    def _learn_from_recommendations(self) -> dict:
        """
        ARIA 추천 종목의 24시간 후 결과 확인 + 학습.
        - 어떤 레짐/섹터 패턴의 추천이 맞았는가?
        - jackal_news.json의 뉴스와 결과 연관성
        """
        rec_file = _BASE / "recommendation_log.json"
        if not rec_file.exists():
            return {"learned": 0, "accuracy": {}}

        try:
            logs = json.loads(rec_file.read_text(encoding="utf-8"))
        except Exception:
            return {"learned": 0, "accuracy": {}}

        cutoff  = datetime.now() - timedelta(hours=24)
        pending = [
            e for e in logs
            if not e.get("outcome_checked")
            and datetime.fromisoformat(e["timestamp"] if "timestamp" in e
                                        else e.get("recommended_at", "2000-01-01"))
               < cutoff
        ]

        learned = 0
        regime_acc   = {}
        inflow_acc   = {}
        ticker_acc   = {}

        for entry in pending:
            try:
                import yfinance as _yf
                ticker      = entry["ticker"]
                price_rec   = entry.get("price_at_rec")
                if not price_rec:
                    continue

                hist = _yf.Ticker(ticker).history(period="5d", interval="1d")
                if len(hist) < 2:
                    continue

                # 추천 다음 거래일 종가
                price_next = float(hist["Close"].iloc[-1])
                pct        = (price_next - price_rec) / price_rec * 100
                correct    = pct >= 0.5

                entry["outcome_checked"] = True
                entry["price_next_day"]  = round(price_next, 4)
                entry["outcome_pct"]     = round(pct, 2)
                entry["outcome_correct"] = correct

                # 레짐별 정확도 집계
                regime = entry.get("aria_regime", "")
                if regime:
                    if regime not in regime_acc:
                        regime_acc[regime] = {"correct": 0, "total": 0}
                    regime_acc[regime]["total"]   += 1
                    regime_acc[regime]["correct"] += int(correct)

                # 유입섹터별 정확도
                for inflow in entry.get("aria_inflows", []):
                    if inflow not in inflow_acc:
                        inflow_acc[inflow] = {"correct": 0, "total": 0}
                    inflow_acc[inflow]["total"]   += 1
                    inflow_acc[inflow]["correct"] += int(correct)

                # 종목별 정확도
                if ticker not in ticker_acc:
                    ticker_acc[ticker] = {"correct": 0, "total": 0}
                ticker_acc[ticker]["total"]   += 1
                ticker_acc[ticker]["correct"] += int(correct)

                learned += 1
                log.info(f"  추천확인 {ticker}: {pct:+.1f}% {'✅' if correct else '❌'} "
                         f"[레짐:{regime}]")

            except Exception as e:
                log.error(f"  추천 결과 확인 실패: {e}")

        # 업데이트된 로그 저장
        if pending:
            rec_file.write_text(
                json.dumps(logs, ensure_ascii=False, indent=2), encoding="utf-8"
            )

        # 가중치에 추천 정확도 기록
        self.weights.setdefault("recommendation_accuracy", {
            "by_regime":  {},
            "by_inflow":  {},
            "by_ticker":  {},
        })
        ra = self.weights["recommendation_accuracy"]
        for k, v in regime_acc.items():
            if k not in ra["by_regime"]:
                ra["by_regime"][k] = {"correct": 0, "total": 0}
            ra["by_regime"][k]["correct"] += v["correct"]
            ra["by_regime"][k]["total"]   += v["total"]
            ra["by_regime"][k]["accuracy"] = round(
                ra["by_regime"][k]["correct"] / ra["by_regime"][k]["total"] * 100, 1
            )
        for k, v in inflow_acc.items():
            if k not in ra["by_inflow"]:
                ra["by_inflow"][k] = {"correct": 0, "total": 0}
            ra["by_inflow"][k]["correct"] += v["correct"]
            ra["by_inflow"][k]["total"]   += v["total"]
            ra["by_inflow"][k]["accuracy"] = round(
                ra["by_inflow"][k]["correct"] / ra["by_inflow"][k]["total"] * 100, 1
            )

        accuracy_summary = {
            "regime":  {k: v["accuracy"] for k, v in ra["by_regime"].items() if v.get("total",0) >= 2},
            "inflow":  {k: v["accuracy"] for k, v in ra["by_inflow"].items() if v.get("total",0) >= 2},
        }
        return {"learned": learned, "accuracy": accuracy_summary}

    def _build_context(self, rec_result: dict = None) -> dict:
        """7일치 스캔 데이터 + 현재 정확도 + 추천 정확도 요약"""
        recent = self._load_recent_logs(days=7)
        alerted = [e for e in recent if e.get("alerted")]
        correct = [e for e in alerted if e.get("outcome_correct") is True]
        wrong   = [e for e in alerted if e.get("outcome_correct") is False]

        # 신호 조합 분석
        sig_combos = defaultdict(lambda: {"correct": 0, "total": 0})
        for e in alerted:
            sigs = tuple(sorted(e.get("signals_fired", [])))
            sig_combos[str(sigs)]["total"] += 1
            if e.get("outcome_correct"):
                sig_combos[str(sigs)]["correct"] += 1

        # 레짐별 성과
        regime_perf = defaultdict(lambda: {"correct": 0, "total": 0})
        for e in alerted:
            r = e.get("aria_regime", "")
            if r:
                regime_perf[r]["total"] += 1
                if e.get("outcome_correct"):
                    regime_perf[r]["correct"] += 1

        # 추천 정확도 포함
        rec_accuracy = (rec_result or {}).get("accuracy", {})

        # 스윙타입별 최적 보유기간 포함
        swing_opt = self.weights.get("swing_type_optimal", {})

        return {
            "period": "7일",
            "recommendation_accuracy": rec_accuracy,
            "swing_type_optimal": swing_opt,
            "scan_summary": {
                "total_scans":   len(recent),
                "total_alerted": len(alerted),
                "correct":       len(correct),
                "wrong":         len(wrong),
                "accuracy_pct":  round(len(correct)/len(alerted)*100, 1) if alerted else 0,
            },
            "signal_accuracy": {
                k: v for k, v in self.weights.get("signal_accuracy", {}).items()
                if v.get("total", 0) >= 2
            },
            "regime_accuracy": {
                k: v for k, v in self.weights.get("regime_accuracy", {}).items()
            },
            "ticker_accuracy": {
                k: v for k, v in self.weights.get("ticker_accuracy", {}).items()
            },
            "devil_accuracy": self.weights.get("devil_accuracy", {}),
            "top_signal_combos": dict(list(sig_combos.items())[:5]),
            "regime_performance": dict(regime_perf),
            "existing_skills":   [p.stem for p in SKILLS_DIR.glob("*.json")],
            "current_weights":   {
                k: round(v, 3) if isinstance(v, (int, float)) else v
                for k, v in self.weights.get("signal_weights", {}).items()
            },
            "recent_correct": [
                {"ticker": e["ticker"], "signals": e.get("signals_fired",[]),
                 "regime": e.get("aria_regime",""), "pct": e.get("outcome_pct")}
                for e in correct[-5:]
            ],
            "recent_wrong": [
                {"ticker": e["ticker"], "signals": e.get("signals_fired",[]),
                 "devil": e.get("devil_verdict",""), "pct": e.get("outcome_pct")}
                for e in wrong[-5:]
            ],
        }

    def _ask_claude(self, context: dict) -> str:
        prompt = f"""
너는 Jackal, 주식 타점 분석 AI의 자동 진화 엔진이다.
아래 7일간 타점 분석 성과를 보고 패턴을 파악해 JSON으로만 반환하라.

### 성과 데이터
{json.dumps(context, ensure_ascii=False, indent=2)[:4000]}

### 반환 형식
{{
  "new_skills": [
    {{
      "name": "snake_case 이름",
      "description": "어떤 상황에서 쓰는 Skill",
      "trigger": "발동 조건 (구체적 수치 포함)",
      "action": "판단 방법"
    }}
  ],
  "new_instincts": [
    {{
      "name": "instinct_이름",
      "warning": "피해야 할 패턴",
      "reason": "왜 실패했는가",
      "regime_context": "어떤 레짐에서 발생했는가"
    }}
  ],
  "prompt_improvements": {{
    "analyst": "Analyst 프롬프트 개선사항",
    "devil": "Devil 프롬프트 개선사항"
  }},
  "weight_adjustments": {{
    "rsi_oversold": 0.0,
    "bb_touch": 0.0,
    "volume_surge": 0.0,
    "golden_cross": 0.0,
    "sector_inflow": 0.0
  }},
  "regime_insights": "레짐별 성과에서 발견된 패턴",
  "devil_insights": "Devil 판정 정확도에서 발견된 패턴"
}}

규칙:
- 기존 Skill과 중복 제외
- 데이터 3건 미만이면 빈 배열
- weight_adjustments는 -0.15 ~ +0.15 범위
- 정확도가 60% 이상인 신호는 가중치 ↑, 40% 미만은 ↓ 권장
- recommendation_accuracy를 보고 어떤 레짐/섹터에서 추천이 잘 맞는지 분석
- swing_type_optimal의 두 정확도 분석:
  day1_accuracy:  1일 방향 정확도 → 진입 타이밍이 좋은 신호
  swing_accuracy: 스윙 성공률 → 보유 기간 기준
  예: "패닉셀반등 1일60% + 스윙85% → 진입하면 며칠 들고가야"
  예: "MA지지 1일45% + 스윙62% → 진입 타이밍 불확실, 분할 추천"
- day1 높고 swing 낮음 → "당일 스캘핑" 스킬
- day1 낮고 swing 높음 → "분할 진입, X일 보유" 스킬
- 둘 다 낮음 → 해당 신호 타입 경고로 추가
- 추천이 잘 맞는 패턴은 new_skills에 포함
- 추천이 잘 안 맞는 패턴은 new_instincts의 warning에 포함
""".strip()

        resp = self.client.messages.create(
            model=MODEL_S,
            max_tokens=3000,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text

    def _parse_response(self, raw: str) -> dict:
        cleaned = re.sub(r"```(?:json)?|```", "", raw).strip()
        try:
            return json.loads(cleaned)
        except Exception as e:
            log.error(f"파싱 실패: {e}")
            return {"new_skills": [], "new_instincts": [],
                    "prompt_improvements": {}, "weight_adjustments": {}}

    # ══════════════════════════════════════════════════════════════
    # Skill / Instinct 저장
    # ══════════════════════════════════════════════════════════════

    def _save_skills(self, skills: list):
        for skill in skills:
            name = skill.get("name", "").strip()
            if not name:
                continue
            path = SKILLS_DIR / f"{name}.json"
            skill["created_at"] = datetime.now().isoformat()
            path.write_text(json.dumps(skill, ensure_ascii=False, indent=2), encoding="utf-8")
            log.info(f"  ✅ Skill 생성: {name}")

    def _save_instincts(self, instincts: list):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        for i, inst in enumerate(instincts):
            name = inst.get("name", f"instinct_{i}").strip()
            path = LESSONS_DIR / f"{ts}_{name}.json"
            inst["timestamp"] = datetime.now().isoformat()
            path.write_text(json.dumps(inst, ensure_ascii=False, indent=2), encoding="utf-8")
            log.info(f"  ⚠️  Instinct 등록: {name}")

    def _apply_claude_adjustments(self, result: dict):
        """Claude가 제안한 가중치 조정 적용"""
        sw = self.weights["signal_weights"]
        for key, delta in result.get("weight_adjustments", {}).items():
            if key in sw:
                old = sw[key]
                new = round(max(WEIGHT_MIN, min(WEIGHT_MAX, old + float(delta))), 4)
                sw[key] = new
                if abs(old - new) > 0.001:
                    log.info(f"  Claude 조정: {key} {old:.3f}→{new:.3f}")

    def _mark_last_evolve(self):
        (_BASE / ".last_evolve").write_text(datetime.now().isoformat(), encoding="utf-8")

    # ══════════════════════════════════════════════════════════════
    # 유틸
    # ══════════════════════════════════════════════════════════════

    def _load_recent_logs(self, days: int = 7) -> list:
        if not SCAN_LOG_FILE.exists():
            return []
        try:
            logs   = json.loads(SCAN_LOG_FILE.read_text(encoding="utf-8"))
            cutoff = datetime.now() - timedelta(days=days)
            return [e for e in logs
                    if datetime.fromisoformat(e["timestamp"]) >= cutoff]
        except Exception:
            return []

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
            return DEFAULT_WEIGHTS.copy()

    def _save_weights(self):
        WEIGHTS_FILE.write_text(
            json.dumps(self.weights, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
    ev = JackalEvolution()
    print(json.dumps(ev.evolve(), ensure_ascii=False, indent=2))
    ev.save_weights()
