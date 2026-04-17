"""
JACKAL evolution module.
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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import defaultdict

import yfinance as yf
from anthropic import Anthropic
from orca.paths import atomic_write_json
from orca.state import (
    list_jackal_recommendations,
    list_jackal_live_events,
    list_pending_jackal_shadow_signals,
    load_latest_jackal_weight_snapshot,
    record_jackal_shadow_accuracy_batch,
    record_jackal_weight_snapshot,
    resolve_jackal_shadow_signal,
    sync_jackal_recommendations,
    sync_jackal_live_events,
)

log = logging.getLogger("jackal_evolution")

_BASE         = Path(__file__).parent
WEIGHTS_FILE  = _BASE / "jackal_weights.json"
HUNT_LOG_FILE = _BASE / "hunt_log.json"
SCAN_LOG_FILE = _BASE / "scan_log.json"
SKILLS_DIR    = _BASE / "skills"
LESSONS_DIR   = _BASE / "lessons"

SKILLS_DIR.mkdir(exist_ok=True)
LESSONS_DIR.mkdir(exist_ok=True)

# [Bug Fix 1-B] 잘못된 모델명 수정
MODEL_S = os.environ.get("ANTHROPIC_MODEL", os.environ.get("SUBAGENT_MODEL", "claude-sonnet-4-6"))

OUTCOME_HOURS      = 4
SUCCESS_PCT        = 0.5
WEIGHT_ADJUST_UP   = 0.04
WEIGHT_ADJUST_DOWN = 0.03
WEIGHT_MIN         = 0.3
WEIGHT_MAX         = 2.5

DEFAULT_WEIGHTS = {
    # [Bug Fix 1-A] signal_weights 중복 키 제거 + 실운용 키 동기화
    "signal_weights": {
        "bb_touch":         1.0,
        "rsi_oversold":     1.0,
        "volume_climax":    1.0,
        "ma_support":       1.0,
        "momentum_dip":     1.0,
        "vol_accumulation": 1.0,
        "sector_rebound":   1.0,
        "rsi_divergence":   1.0,
        "sector_inflow":    1.0,
        "golden_cross":     1.0,
        "fear_regime":      1.0,
        "bullish_div":      1.0,
        "volume_surge":     1.0,
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
    "rule_registry_status": {
        "sector_rebound_base":   {"active": True, "min_accuracy": 0.75, "recent_accuracy": 0.0, "sample_n": 0, "review_after_n": 50},
        "volume_climax_base":    {"active": True, "min_accuracy": 0.65, "recent_accuracy": 0.0, "sample_n": 0, "review_after_n": 20},
        "ma_support_solo_pen":   {"active": True, "min_accuracy": None, "recent_accuracy": 0.0, "sample_n": 0, "review_after_n": 30},
        "crash_rebound_pattern": {"active": True, "min_accuracy": 0.70, "recent_accuracy": 0.0, "sample_n": 0, "review_after_n": 30},
        "heuristic_gate":        {"active": True, "min_accuracy": None, "recent_accuracy": 0.0, "sample_n": 0, "review_after_n": 30},
    },
    "ticker_reliability": {
        "NVDA":       {"level": "high",   "sample": 0, "accuracy": 0.0},
        "AVGO":       {"level": "medium", "sample": 0, "accuracy": 0.0},
        "000660.KS":  {"level": "medium", "sample": 0, "accuracy": 0.0},
        "005930.KS":  {"level": "medium", "sample": 0, "accuracy": 0.0},
        "035720.KS":  {"level": "low",    "sample": 0, "accuracy": 0.0},
    },
    "signal_details": {
        "bb_touch":         {"peak_day": "D4~5", "swing_acc": "97%",
                             "mae_avg": -3.8, "mae_median": -2.9, "mae_std": 2.1,
                             "peak_avg": 9.33, "peak_median": 8.1,
                             "sample_count": 36, "mae_source": "backtest_60d"},
        "sector_rebound":   {"peak_day": "D4~5", "swing_acc": "93%",
                             "mae_avg": -2.1, "mae_median": -1.8, "mae_std": 1.5,
                             "peak_avg": 9.22, "peak_median": 8.7,
                             "sample_count": 29, "mae_source": "backtest_60d"},
        "rsi_oversold":     {"peak_day": "D4~5", "swing_acc": "88%",
                             "mae_avg": -2.9, "mae_median": -2.4, "mae_std": 1.8,
                             "peak_avg": 8.49, "peak_median": 7.2,
                             "sample_count": 17, "mae_source": "backtest_60d"},
        "vol_accumulation": {"peak_day": "D5",   "swing_acc": "84%",
                             "mae_avg": -3.2, "mae_median": -2.7, "mae_std": 2.3,
                             "peak_avg": 7.15, "peak_median": 6.2,
                             "sample_count": 39, "mae_source": "backtest_60d"},
        "volume_climax":    {"peak_day": "D4~5", "swing_acc": "80%",
                             "mae_avg": -4.5, "mae_median": -3.8, "mae_std": 3.1,
                             "peak_avg": 9.40, "peak_median": 8.2,
                             "sample_count": 5,  "mae_source": "backtest_60d"},
        "momentum_dip":     {"peak_day": "D4~5", "swing_acc": "78%",
                             "mae_avg": -4.1, "mae_median": -3.5, "mae_std": 2.8,
                             "peak_avg": 6.08, "peak_median": 5.1,
                             "sample_count": 76, "mae_source": "backtest_60d"},
        "ma_support":       {"peak_day": "D3~4", "swing_acc": "67%",
                             "mae_avg": -1.8, "mae_median": -1.4, "mae_std": 1.2,
                             "peak_avg": 4.89, "peak_median": 3.9,
                             "sample_count": 48, "mae_source": "backtest_60d"},
        "rsi_divergence":   {"peak_day": "D4",   "swing_acc": "52%",
                             "mae_avg": -2.3, "mae_median": -1.9, "mae_std": 1.7,
                             "peak_avg": 1.34, "peak_median": 0.8,
                             "sample_count": 23, "mae_source": "backtest_60d"},
    },
}


class JackalEvolution:

    def __init__(self):
        self.client  = Anthropic()
        self.weights = self._load_weights()
        self._logs: list = []

    def evolve(self) -> dict:
        log.info("🧬 Evolution 시작")

        learn_result = self._learn_from_outcomes()
        rec_result   = self._learn_from_recommendations()

        context  = self._build_context(rec_result)
        raw      = self._ask_claude(context)
        analysis = self._parse_response(raw)

        self._save_skills(analysis.get("new_skills", []))
        self._save_instincts(analysis.get("new_instincts", []))
        self._apply_claude_adjustments(analysis)

        disabled_rules = self._check_rule_auto_disable()

        # [Bug Fix 3/8] _mark_last_evolve: .last_evolve 파일 쓰기 → weights에 기록
        self._mark_last_evolve()

        self.weights["last_updated"] = datetime.now().isoformat()
        self._save_weights()

        log.info(f"🧬 완료 | 타점학습 {learn_result['learned']}건 | "
                 f"추천학습 {rec_result['learned']}건 | "
                 f"Skill {len(analysis.get('new_skills',[]))}개")

        return {
            "learned":          learn_result["learned"],
            "rec_learned":      rec_result["learned"],
            "weight_changes":   learn_result["changes"],
            "new_skills":       analysis.get("new_skills", []),
            "new_instincts":    analysis.get("new_instincts", []),
            "improvements":     analysis.get("prompt_improvements", ""),
            "accuracy_summary": learn_result.get("accuracy_summary", {}),
            "rec_accuracy":     rec_result.get("accuracy", {}),
            "disabled_rules":   disabled_rules,
        }

    def save_weights(self):
        self._save_weights()

    # ══════════════════════════════════════════════════════════════
    # 결과 학습
    # ══════════════════════════════════════════════════════════════

    def _learn_from_outcomes(self) -> dict:
        log_file = HUNT_LOG_FILE if HUNT_LOG_FILE.exists() else SCAN_LOG_FILE
        self._logs = list(reversed(list_jackal_live_events("hunt", limit=500)))
        if not self._logs and log_file.exists():
            try:
                self._logs = json.loads(log_file.read_text(encoding="utf-8"))
            except Exception:
                return {"learned": 0, "changes": [], "accuracy_summary": {}}
        if not self._logs:
            return {"learned": 0, "changes": [], "accuracy_summary": {}}

        # [Bug Fix] 타임존 통일: KST-aware cutoff으로 비교
        KST_TZ    = timezone(timedelta(hours=9))
        cutoff_1d = datetime.now(KST_TZ) - timedelta(hours=28)

        def _parse_ts_aware(ts_str: str) -> datetime:
            """타임존 포함/미포함 ISO 문자열을 KST-aware datetime으로 변환."""
            dt = datetime.fromisoformat(ts_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=KST_TZ)
            return dt.astimezone(KST_TZ)

        pending_live = [
            e for e in self._logs
            if (e.get("alerted") or e.get("is_entry"))
            and not e.get("outcome_checked")
            and _parse_ts_aware(e.get("timestamp", "2000-01-01T00:00:00")) < cutoff_1d
        ]
        pending_shadow = list_pending_jackal_shadow_signals(cutoff_1d.isoformat())

        pending = pending_live
        log.info(f"  outcome 체크 대상: live {len(pending_live)}건 / shadow {len(pending_shadow)}건")

        learned = 0
        changes = []
        sig_acc = defaultdict(lambda: {"correct": 0, "total": 0})
        reg_acc = defaultdict(lambda: {"correct": 0, "total": 0})
        tkr_acc = defaultdict(lambda: {"correct": 0, "total": 0})
        dev_acc = defaultdict(lambda: {"correct": 0, "total": 0})
        stype_acc = defaultdict(lambda: {
            "correct": 0, "total": 0, "peak_days": [], "peak_pcts": [],
            "d1_correct": 0, "d1_total": 0,
            "swing_correct": 0, "swing_total": 0,
        })

        for entry in pending:
            try:
                ticker      = entry["ticker"]
                price_entry = entry.get("price_at_hunt") or entry.get("price_at_scan", 0)
                if not price_entry:
                    continue

                hist = yf.Ticker(ticker).history(period="15d", interval="1d")
                if hist.empty:
                    continue

                entry_ts = _parse_ts_aware(entry.get("timestamp", "2000-01-01T00:00:00"))
                future   = hist[hist.index > entry_ts.strftime("%Y-%m-%d")]
                if future.empty:
                    continue

                closes  = [float(c) for c in future["Close"]]
                returns = [(c - price_entry) / price_entry * 100 for c in closes]

                d1_pct      = round(returns[0], 2) if returns else None
                d1_correct  = (d1_pct > 0.3) if d1_pct is not None else None

                sw_window  = returns[:7]
                peak_pct   = round(max(sw_window), 2) if sw_window else 0.0
                peak_day   = sw_window.index(max(sw_window)) + 1 if sw_window else 1
                swing_hit  = peak_pct >= 1.0
                swing_checked = len(sw_window) >= 3

                entry["outcome_checked"]  = True
                entry["price_1d_later"]   = round(float(future["Close"].iloc[0]), 4) if len(future) >= 1 else None
                entry["outcome_1d_pct"]   = d1_pct
                entry["outcome_1d_hit"]   = d1_correct
                entry["peak_day"]         = peak_day
                entry["peak_pct"]         = peak_pct
                entry["outcome_swing_hit"]= swing_hit
                entry["outcome_correct"]  = swing_hit

                stype  = entry.get("swing_type", "기술적과매도")
                regime = entry.get("orca_regime", "")
                verdict= entry.get("devil_verdict", "")

                for sig in entry.get("signals_fired", []):
                    sig_acc[sig]["total"]   += 1
                    sig_acc[sig]["correct"] += int(swing_hit)

                stype_acc[stype]["total"]         += 1
                stype_acc[stype]["correct"]       += int(swing_hit)
                stype_acc[stype]["peak_days"].append(peak_day)
                stype_acc[stype]["peak_pcts"].append(peak_pct)
                stype_acc[stype]["swing_total"]   += 1
                stype_acc[stype]["swing_correct"] += int(swing_hit)
                if d1_correct is not None:
                    stype_acc[stype]["d1_total"]   += 1
                    stype_acc[stype]["d1_correct"] += int(d1_correct)

                if regime:
                    reg_acc[regime]["total"]   += 1
                    reg_acc[regime]["correct"] += int(swing_hit)

                tkr_acc[ticker]["total"]   += 1
                tkr_acc[ticker]["correct"] += int(swing_hit)

                if verdict:
                    dev_acc[verdict]["total"]   += 1
                    dev_acc[verdict]["correct"] += int(not swing_hit if verdict == "반대" else swing_hit)

                if swing_checked:
                    adj = WEIGHT_ADJUST_UP if swing_hit else -WEIGHT_ADJUST_DOWN
                    if d1_correct:
                        adj += 0.01
                    sw_w = self.weights["signal_weights"]
                    for sig in entry.get("signals_fired", []):
                        if sig in sw_w:
                            old = sw_w[sig]
                            new = round(max(WEIGHT_MIN, min(WEIGHT_MAX, old + adj)), 4)
                            sw_w[sig] = new
                            if abs(old - new) > 0.001:
                                changes.append(
                                    f"{sig}: {old:.3f}→{new:.3f} "
                                    f"[{ticker} 스윙D{peak_day} {peak_pct:+.1f}%]"
                                )

                learned += 1
                d1_str = f"{d1_pct:+.1f}%" if d1_pct is not None else "대기"
                log.info(
                    f"  {ticker} [{stype}]: "
                    f"1일 {d1_str} | Peak D{peak_day} {peak_pct:+.1f}% {'✅' if swing_hit else '❌'}"
                )

            except Exception as e:
                log.error(f"  학습 실패: {e}")

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
                "swing_accuracy": swing_acc,
                "day1_accuracy":  d1_acc,
                "sample":         v["total"],
            }
            log.info(f"  [{stype}] Peak D{avg_peak} ({avg_gain:+.2f}%) | 1일 {d1_acc}% | 스윙 {swing_acc}%")

        self._update_accuracy("signal_accuracy", sig_acc)
        self._update_accuracy("regime_accuracy", reg_acc)
        self._update_accuracy("ticker_accuracy", tkr_acc)
        self._update_devil_accuracy(dev_acc)

        if pending:
            atomic_write_json(log_file, self._logs)
            sync_jackal_live_events("hunt", self._logs[-500:])

        if pending_shadow:
            shadow_stats = {"total": 0, "would_have_worked": 0}
            for e in pending_shadow:
                try:
                    ticker      = e.get("ticker", "")
                    price_entry = e.get("price_at_scan", 0)
                    if not ticker or not price_entry:
                        continue
                    entry_ts = _parse_ts_aware(e.get("timestamp", "2000-01-01T00:00:00"))
                    hist = yf.Ticker(ticker).history(period="10d", interval="1d")
                    if hist.empty:
                        continue
                    future = hist[hist.index > entry_ts.strftime("%Y-%m-%d")]
                    if future.empty:
                        continue
                    closes  = [float(c) for c in future["Close"]]
                    returns = [(c - price_entry) / price_entry * 100 for c in closes]
                    if not returns:
                        continue
                    swing_ret = max(returns[:7]) if len(returns) >= 1 else 0
                    swing_ok  = swing_ret >= 1.0
                    resolve_jackal_shadow_signal(
                        e["shadow_id"],
                        {
                            "shadow_swing_pct": round(swing_ret, 2),
                            "shadow_swing_ok": swing_ok,
                        },
                        payload_updates={
                            "outcome_checked": True,
                            "shadow_evaluated_at": datetime.now().isoformat(),
                        },
                    )
                    shadow_stats["total"] += 1
                    shadow_stats["would_have_worked"] += int(swing_ok)
                except Exception:
                    pass

            if shadow_stats["total"] > 0:
                shadow_acc = round(shadow_stats["would_have_worked"] / shadow_stats["total"] * 100, 1)
                log.info(
                    f"  shadow outcome: {shadow_stats['total']}건 체크 "
                    f"→ 실제론 {shadow_acc}% 맞았을 신호 (스킵됐지만)"
                )
                self._record_shadow_accuracy(shadow_stats)

        acc_summary = {}
        for key in ["signal_accuracy", "regime_accuracy", "ticker_accuracy"]:
            acc = self.weights.get(key, {})
            acc_summary[key] = {
                k: {"accuracy": round(v["correct"]/v["total"]*100, 1), "total": v["total"]}
                for k, v in acc.items()
                if isinstance(v, dict) and v.get("total", 0) >= 3 and "correct" in v
            }

        return {"learned": learned, "changes": changes, "accuracy_summary": acc_summary}

    def _update_accuracy(self, key: str, new_data: dict):
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
    # 추천 종목 학습
    # ══════════════════════════════════════════════════════════════

    def _learn_from_recommendations(self) -> dict:
        rec_file = _BASE / "recommendation_log.json"
        logs = list_jackal_recommendations(limit=200)
        if not logs:
            if not rec_file.exists():
                return {"learned": 0, "accuracy": {}}
            try:
                logs = json.loads(rec_file.read_text(encoding="utf-8"))
            except Exception:
                return {"learned": 0, "accuracy": {}}

        # [Bug Fix] 타임존 통일: KST-aware cutoff
        KST_TZ  = timezone(timedelta(hours=9))
        cutoff  = datetime.now(KST_TZ) - timedelta(hours=24)
        pending = [
            e for e in logs
            if not e.get("outcome_checked")
            and (lambda ts: (
                datetime.fromisoformat(ts).replace(tzinfo=KST_TZ)
                if datetime.fromisoformat(ts).tzinfo is None
                else datetime.fromisoformat(ts).astimezone(KST_TZ)
            ))(e.get("timestamp") or e.get("recommended_at", "2000-01-01T00:00:00")) < cutoff
        ]

        learned    = 0
        regime_acc = {}
        inflow_acc = {}
        ticker_acc = {}

        for entry in pending:
            try:
                ticker    = entry["ticker"]
                price_rec = entry.get("price_at_rec")
                if not price_rec:
                    continue

                hist = yf.Ticker(ticker).history(period="5d", interval="1d")
                if len(hist) < 2:
                    continue

                price_next = float(hist["Close"].iloc[-1])
                pct        = (price_next - price_rec) / price_rec * 100
                correct    = pct >= 0.5

                entry["outcome_checked"] = True
                entry["price_next_day"]  = round(price_next, 4)
                entry["outcome_pct"]     = round(pct, 2)
                entry["outcome_correct"] = correct

                regime = entry.get("orca_regime", "")
                if regime:
                    if regime not in regime_acc:
                        regime_acc[regime] = {"correct": 0, "total": 0}
                    regime_acc[regime]["total"]   += 1
                    regime_acc[regime]["correct"] += int(correct)

                for inflow in entry.get("orca_inflows", []):
                    if inflow not in inflow_acc:
                        inflow_acc[inflow] = {"correct": 0, "total": 0}
                    inflow_acc[inflow]["total"]   += 1
                    inflow_acc[inflow]["correct"] += int(correct)

                if ticker not in ticker_acc:
                    ticker_acc[ticker] = {"correct": 0, "total": 0}
                ticker_acc[ticker]["total"]   += 1
                ticker_acc[ticker]["correct"] += int(correct)

                learned += 1
                log.info(f"  추천확인 {ticker}: {pct:+.1f}% {'✅' if correct else '❌'} [레짐:{regime}]")

            except Exception as e:
                log.error(f"  추천 결과 확인 실패: {e}")

        if pending:
            atomic_write_json(rec_file, logs)
            sync_jackal_recommendations(logs)

        self.weights.setdefault("recommendation_accuracy", {
            "by_regime": {}, "by_inflow": {}, "by_ticker": {},
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
            "regime": {k: v["accuracy"] for k, v in ra["by_regime"].items() if v.get("total", 0) >= 2},
            "inflow": {k: v["accuracy"] for k, v in ra["by_inflow"].items() if v.get("total", 0) >= 2},
        }
        return {"learned": learned, "accuracy": accuracy_summary}

    # ══════════════════════════════════════════════════════════════
    # Claude 주간 패턴 리뷰
    # ══════════════════════════════════════════════════════════════

    def _build_context(self, rec_result: dict = None) -> dict:
        recent  = self._load_recent_logs(days=7)
        alerted = [e for e in recent if e.get("alerted")]
        correct = [e for e in alerted if e.get("outcome_correct") is True]
        wrong   = [e for e in alerted if e.get("outcome_correct") is False]

        sig_combos  = defaultdict(lambda: {"correct": 0, "total": 0})
        regime_perf = defaultdict(lambda: {"correct": 0, "total": 0})

        for e in alerted:
            sigs = tuple(sorted(e.get("signals_fired", [])))
            sig_combos[str(sigs)]["total"] += 1
            if e.get("outcome_correct"):
                sig_combos[str(sigs)]["correct"] += 1
            r = e.get("orca_regime", "")
            if r:
                regime_perf[r]["total"] += 1
                if e.get("outcome_correct"):
                    regime_perf[r]["correct"] += 1

        return {
            "period": "7일",
            "recommendation_accuracy": (rec_result or {}).get("accuracy", {}),
            "swing_type_optimal": self.weights.get("swing_type_optimal", {}),
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
            "regime_accuracy":  dict(self.weights.get("regime_accuracy", {})),
            "ticker_accuracy":  dict(self.weights.get("ticker_accuracy", {})),
            "devil_accuracy":   self.weights.get("devil_accuracy", {}),
            "top_signal_combos": dict(list(sig_combos.items())[:5]),
            "regime_performance": dict(regime_perf),
            "existing_skills":   [p.stem for p in SKILLS_DIR.glob("*.json")],
            "current_weights": {
                k: round(v, 3) if isinstance(v, (int, float)) else v
                for k, v in self.weights.get("signal_weights", {}).items()
            },
            "recent_correct": [
                {"ticker": e["ticker"], "signals": e.get("signals_fired", []),
                 "regime": e.get("orca_regime", ""), "pct": e.get("outcome_pct")}
                for e in correct[-5:]
            ],
            "recent_wrong": [
                {"ticker": e["ticker"], "signals": e.get("signals_fired", []),
                 "devil": e.get("devil_verdict", ""), "pct": e.get("outcome_pct")}
                for e in wrong[-5:]
            ],
        }

    def _ask_claude(self, context: dict) -> str:
        prompt = f"""You are a Jackal trading signal AI evolution engine.
Analyze the 7-day performance data below and return patterns as JSON.

CRITICAL: Return ONLY valid JSON. No markdown, no code blocks, no explanation text.
All string values must use escaped quotes inside JSON strings.
Do NOT use unescaped double quotes inside string values.

### Performance Data
{json.dumps(context, ensure_ascii=False, indent=2)[:4000]}

### Required JSON format (return EXACTLY this structure):
{{
  "new_skills": [
    {{
      "name": "snake_case_name",
      "description": "skill description",
      "trigger": "trigger condition with numbers",
      "action": "action to take"
    }}
  ],
  "new_instincts": [
    {{
      "name": "instinct_name",
      "warning": "pattern to avoid",
      "reason": "why it fails",
      "regime_context": "which regime"
    }}
  ],
  "prompt_improvements": {{
    "analyst": "improvement suggestion",
    "devil": "improvement suggestion"
  }},
  "weight_adjustments": {{
    "rsi_oversold": 0.0,
    "bb_touch": 0.0,
    "volume_surge": 0.0,
    "golden_cross": 0.0,
    "sector_inflow": 0.0
  }},
  "regime_insights": "pattern found in regime performance",
  "devil_insights": "pattern found in devil accuracy"
}}

Rules:
- Skip new_skills/instincts if fewer than 3 data samples
- weight_adjustments range: -0.15 to +0.15
- Signals with accuracy >= 60% get positive adjustment
- Signals with accuracy <= 40% get negative adjustment""".strip()

        resp = self.client.messages.create(
            model=MODEL_S,
            max_tokens=3000,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text

    def _parse_response(self, raw: str) -> dict:
        """
        [Bug Fix] 다단계 JSON 파싱 폴백.
        Claude가 JSON 내부 문자열에 따옴표/개행을 넣어 파싱 실패하는 케이스 대응.
        """
        empty = {"new_skills": [], "new_instincts": [],
                 "prompt_improvements": {}, "weight_adjustments": {}}

        # 1단계: 코드블록 제거 후 바로 파싱
        cleaned = re.sub(r"```(?:json)?|```", "", raw).strip()
        try:
            return json.loads(cleaned)
        except Exception:
            pass

        # 2단계: 첫 { ~ 마지막 } 추출
        m = re.search(r"\{[\s\S]*\}", cleaned)
        if not m:
            log.error(f"Evolution 파싱 실패: JSON 객체 미발견")
            return empty
        s = m.group()

        # 3단계: trailing comma 제거
        s2 = re.sub(r",\s*([}\]])", r"\1", s)
        try:
            return json.loads(s2)
        except Exception:
            pass

        # 4단계: 문자열 내 개행 이스케이프
        s3 = re.sub(r'(?<!\\)\n(?=[^"]*"(?:[^"]*"[^"]*")*[^"]*$)', r'\\n', s2)
        try:
            return json.loads(s3)
        except Exception:
            pass

        # 5단계: 핵심 필드만 개별 추출 (weight_adjustments는 살릴 수 있음)
        result = empty.copy()
        wa_m = re.search(r'"weight_adjustments"\s*:\s*(\{[^}]+\})', s2)
        if wa_m:
            try:
                result["weight_adjustments"] = json.loads(wa_m.group(1))
                log.warning("Evolution 파싱 부분 성공: weight_adjustments만 복구")
                return result
            except Exception:
                pass

        log.error(f"Evolution 파싱 5단계 모두 실패 — 빈 결과 반환")
        return empty

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
            atomic_write_json(path, skill)
            log.info(f"  ✅ Skill 생성: {name}")

    def _save_instincts(self, instincts: list):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        for i, inst in enumerate(instincts):
            name = inst.get("name", f"instinct_{i}").strip()
            path = LESSONS_DIR / f"{ts}_{name}.json"
            inst["timestamp"] = datetime.now().isoformat()
            atomic_write_json(path, inst)
            log.info(f"  ⚠️  Instinct 등록: {name}")

    # ══════════════════════════════════════════════════════════════
    # Rule Auto-Disable
    # ══════════════════════════════════════════════════════════════

    def _check_rule_auto_disable(self) -> list:
        rule_signal_map = {
            "sector_rebound_base":   ["sector_rebound"],
            "volume_climax_base":    ["volume_climax"],
            "crash_rebound_pattern": ["sector_rebound", "volume_climax", "vol_accumulation"],
        }
        if self._logs:
            registry = self.weights.setdefault("rule_registry_status", {})
            for rule_name, signals in rule_signal_map.items():
                entries = [
                    e for e in self._logs
                    if any(s in e.get("signals_fired", []) for s in signals)
                    and e.get("outcome_checked")
                ]
                if entries and rule_name in registry:
                    n       = len(entries)
                    correct = sum(
                        1 for e in entries
                        if e.get("outcome_swing_hit") or e.get("outcome_correct")
                    )
                    registry[rule_name]["sample_n"]        = n
                    registry[rule_name]["recent_accuracy"] = round(correct / n, 3) if n > 0 else 0.0

        disabled = []
        registry = self.weights.get("rule_registry_status", {})

        for rule_name, status in registry.items():
            if not status.get("active", True):
                continue
            min_acc = status.get("min_accuracy")
            if min_acc is None:
                continue
            sample_n   = status.get("sample_n", 0)
            review_n   = status.get("review_after_n", 50)
            recent_acc = status.get("recent_accuracy", 0.0)

            if sample_n >= review_n and recent_acc < min_acc:
                status["active"]         = False
                status["disabled_at"]    = datetime.now().isoformat()
                status["disable_reason"] = (
                    f"정확도 {recent_acc:.1%} < 기준 {min_acc:.1%} "
                    f"(샘플 {sample_n}건 / 임계 {review_n}건)"
                )
                disabled.append(rule_name)
                log.warning(
                    f"  🚫 Rule 자동 비활성화: {rule_name} "
                    f"({recent_acc:.1%} < {min_acc:.1%}, n={sample_n})"
                )

        if not disabled:
            log.info("  ✅ Rule Auto-Disable: 모든 Rule 정상 범위")
        else:
            self.weights.setdefault("auto_disabled_log", []).append({
                "timestamp": datetime.now().isoformat(),
                "rules":     disabled,
            })

        return disabled

    # ══════════════════════════════════════════════════════════════
    # Shadow → ARIA Accuracy 연동
    # ══════════════════════════════════════════════════════════════

    def _legacy_record_shadow_accuracy(self, shadow_stats: dict):
        if shadow_stats.get("total", 0) == 0:
            return

        try:
            shadow_accuracy = json.loads(
                JACKAL_SHADOW_ACCURACY_FILE.read_text(encoding="utf-8")
            ) if JACKAL_SHADOW_ACCURACY_FILE.exists() else {}
        except Exception:
            shadow_accuracy = {}

        total = shadow_stats["total"]
        worked = shadow_stats["would_have_worked"]
        rate = round(worked / total * 100, 1) if total > 0 else 0

        shadow_accuracy.setdefault(
            "description",
            "Jackal shadow performance is stored separately from ARIA production accuracy.",
        )
        shadow_accuracy["correct"] = int(shadow_accuracy.get("correct", 0)) + worked
        shadow_accuracy["total"] = int(shadow_accuracy.get("total", 0)) + total
        shadow_accuracy["accuracy"] = round(
            shadow_accuracy["correct"] / shadow_accuracy["total"] * 100, 1
        ) if shadow_accuracy["total"] > 0 else 0
        shadow_accuracy["last_updated"] = datetime.now().isoformat()
        shadow_accuracy["last_batch"] = {"total": total, "worked": worked, "rate": rate}

        history = shadow_accuracy.get("history", [])
        history.append({
            "timestamp": shadow_accuracy["last_updated"],
            "total": total,
            "worked": worked,
            "rate": rate,
        })
        shadow_accuracy["history"] = history[-90:]

        atomic_write_json(JACKAL_SHADOW_ACCURACY_FILE, shadow_accuracy)
        log.info(
            f"  ?뱻 Jackal shadow metrics saved separately: "
            f"{worked}/{total} ({rate}%)"
        )

    def _legacy_sync_shadow_accuracy(self, shadow_stats: dict):
        self._record_shadow_accuracy(shadow_stats)
        return

        if shadow_stats.get("total", 0) == 0:
            return

        acc_file = _BASE.parent / "data" / "accuracy.json"
        if not acc_file.exists():
            log.debug("  aria accuracy.json 없음 — shadow 연동 스킵")
            return

        try:
            accuracy = json.loads(acc_file.read_text(encoding="utf-8"))
        except Exception:
            accuracy = {}

        total  = shadow_stats["total"]
        worked = shadow_stats["would_have_worked"]
        rate   = round(worked / total * 100, 1) if total > 0 else 0

        entry = accuracy.setdefault("jackal_shadow", {
            "correct": 0, "total": 0, "accuracy": 0,
            "description": "Jackal 발송 스킵된 신호의 실제 스윙 성공률",
        })
        entry["correct"]     += worked
        entry["total"]       += total
        entry["accuracy"]     = round(entry["correct"] / entry["total"] * 100, 1) if entry["total"] > 0 else 0
        entry["last_updated"] = datetime.now().isoformat()
        entry["last_batch"]   = {"total": total, "worked": worked, "rate": rate}

        acc_file.write_text(
            json.dumps(accuracy, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        log.info(
            f"  📡 ARIA accuracy 연동: shadow {total}건 → "
            f"스윙 성공 {worked}건 ({rate}%)"
        )

    def _apply_claude_adjustments(self, result: dict):
        sw = self.weights["signal_weights"]
        for key, delta in result.get("weight_adjustments", {}).items():
            if key in sw:
                old = sw[key]
                new = round(max(WEIGHT_MIN, min(WEIGHT_MAX, old + float(delta))), 4)
                sw[key] = new
                if abs(old - new) > 0.001:
                    log.info(f"  Claude 조정: {key} {old:.3f}→{new:.3f}")

    def _mark_last_evolve(self):
        """[Bug Fix 3/8] .last_evolve 파일 대신 weights에 기록"""
        self.weights["last_evolved_at"] = datetime.now().isoformat()

    def _record_shadow_accuracy(self, shadow_stats: dict):
        if shadow_stats.get("total", 0) == 0:
            return

        total = int(shadow_stats["total"])
        worked = int(shadow_stats["would_have_worked"])
        rate = round(worked / total * 100, 1) if total > 0 else 0.0
        record_jackal_shadow_accuracy_batch(
            total,
            worked,
            metadata={"source": "jackal_evolution"},
        )
        log.info(
            f"  Jackal shadow metrics saved to SQLite: "
            f"{worked}/{total} ({rate}%)"
        )

    def _sync_shadow_accuracy(self, shadow_stats: dict):
        self._record_shadow_accuracy(shadow_stats)
        return

    # ══════════════════════════════════════════════════════════════
    # 유틸
    # ══════════════════════════════════════════════════════════════

    def _load_recent_logs(self, days: int = 7) -> list:
        """
        [Bug Fix] SCAN_LOG_FILE(존재하지 않음) → HUNT_LOG_FILE로 수정.
        hunt_log.json의 alerted=True 항목 중 최근 N일치 반환.
        타임존 aware 비교로 통일.
        """
        KST_TZ = timezone(timedelta(hours=9))
        cutoff = datetime.now(KST_TZ) - timedelta(days=days)
        logs = list_jackal_live_events("hunt", alerted_only=True, limit=500)
        if not logs and HUNT_LOG_FILE.exists():
            try:
                logs = json.loads(HUNT_LOG_FILE.read_text(encoding="utf-8"))
            except Exception:
                return []

        result = []
        for e in logs:
            ts_str = e.get("timestamp", "")
            if not ts_str:
                continue
            try:
                dt = datetime.fromisoformat(ts_str)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=KST_TZ)
                if dt >= cutoff:
                    result.append(e)
            except Exception:
                continue
        return result

    def _load_weights(self) -> dict:
        snapshot = load_latest_jackal_weight_snapshot()
        if isinstance(snapshot, dict):
            loaded = snapshot
        elif not WEIGHTS_FILE.exists():
            return DEFAULT_WEIGHTS.copy()
        else:
            try:
                loaded = json.loads(WEIGHTS_FILE.read_text(encoding="utf-8"))
            except Exception:
                return DEFAULT_WEIGHTS.copy()
        try:
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
        """
        [Bug Fix] Atomic write 패턴: temp 파일 write → rename.
        중간 crash 시 기존 weights.json 보존 (partial write 방지).
        저장 전 .bak 백업 생성 (최근 1개).
        """
        import shutil
        # 기존 파일 백업 (crash 복구용)
        if WEIGHTS_FILE.exists():
            bak = WEIGHTS_FILE.with_suffix(".json.bak")
            try:
                shutil.copy2(WEIGHTS_FILE, bak)
            except Exception as e:
                log.debug(f"weights 백업 실패 (무시): {e}")

        # Atomic write: temp → rename
        tmp = WEIGHTS_FILE.with_suffix(".json.tmp")
        try:
            tmp.write_text(
                json.dumps(self.weights, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            tmp.replace(WEIGHTS_FILE)  # OS-level atomic on Unix/Windows
            record_jackal_weight_snapshot(self.weights, source="evolution")
        except Exception as e:
            log.error(f"weights 저장 실패: {e}")
            if tmp.exists():
                tmp.unlink(missing_ok=True)
            raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
    ev = JackalEvolution()
    print(json.dumps(ev.evolve(), ensure_ascii=False, indent=2))
    ev.save_weights()



