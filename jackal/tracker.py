"""
JACKAL tracker module.
Jackal Outcome Tracker — 타점 결과 추적 + 가중치 실시간 학습

역할:
  1. hunt_log.json의 outcome_checked=False 항목 탐색
  2. yfinance 가격 조회 → 1일/스윙/5일 outcome 기록 (비용 $0)
  3. jackal_weights.json 정확도 데이터 실시간 업데이트
     signal_accuracy / ticker_accuracy / devil_accuracy / regime_accuracy
  4. 실행 완료 후 요약 텔레그램 발송 (선택)

Evolution과의 역할 분리:
  Tracker  (이 파일) : 가격 조회 → outcome 필드 채우기 (6시간마다)
  Evolution           : Claude 분석 → Skill/Instinct/Weight 생성 (24시간마다)
  → Tracker가 미리 outcome을 채워놔야 Evolution이 실제 데이터로 학습 가능

Outcome 필드:
  price_1d_later   : 1 거래일 후 종가
  outcome_1d_pct   : 1일 수익률 (%)
  outcome_1d_hit   : 1일 +0.5% 이상 → True
  price_peak       : 탐색 기간(1~7거래일) 최고 종가
  peak_day         : 최고가 발생 거래일 번호 (1=익일)
  peak_pct         : 진입가 대비 최고 수익률 (%)
  outcome_swing_hit: peak_pct >= +1.0% → True
  price_5d_later   : 5 거래일 후 종가
  outcome_pct      : 5일 수익률 (%)
  outcome_correct  : outcome_swing_hit (Evolution 학습 기준)
  outcome_checked  : True (5거래일 데이터 확보 시 확정)

실행:
  python -m jackal.tracker            # 일반 실행
  python -m jackal.tracker --dry-run  # 저장 없이 결과 미리보기
  python -m jackal.tracker --all      # alerted 여부 무관 전체 처리
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import yfinance as yf
import pandas as pd
from orca.paths import atomic_write_json
from orca.state import (
    list_jackal_live_events,
    load_latest_jackal_weight_snapshot,
    record_jackal_weight_snapshot,
    sync_jackal_live_events,
)

os.environ["PYTHONIOENCODING"] = "utf-8"
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

log = logging.getLogger("jackal_tracker")

_BASE         = Path(__file__).parent          # jackal/
_REPO_ROOT    = _BASE.parent                   # repo root
HUNT_LOG_FILE = _BASE / "hunt_log.json"
WEIGHTS_FILE  = _BASE / "jackal_weights.json"

KST = timezone(timedelta(hours=9))

# ── 설정 ─────────────────────────────────────────────────────────
MIN_ELAPSED_HOURS = 26    # 이 시간이 지난 항목만 처리 (장 마감 보장)
SWING_DAYS        = 7     # 스윙 추적 최대 거래일
SWING_HIT_PCT     = 1.0   # 스윙 성공 기준 (%)
D1_HIT_PCT        = 0.5   # 1일 성공 기준 (%)
MIN_SWING_ROWS    = 3     # 스윙 확정에 필요한 최소 거래일 데이터
YFINANCE_DELAY    = 0.4   # 종목간 호출 딜레이 (Rate limit 방지)

# 가중치 조정 범위
WEIGHT_ADJ_UP   = 0.04
WEIGHT_ADJ_DOWN = 0.03
WEIGHT_MIN      = 0.3
WEIGHT_MAX      = 2.5
MIN_SAMPLES_ADJ = 5       # 가중치 조정 최소 샘플 수


# ══════════════════════════════════════════════════════════════════
# 유틸
# ══════════════════════════════════════════════════════════════════

def _now() -> datetime:
    return datetime.now(KST)


def _parse_ts(ts_str: str) -> datetime | None:
    """타임존 포함/미포함 ISO 문자열을 KST-aware datetime으로 변환."""
    if not ts_str:
        return None
    try:
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        return dt.astimezone(KST)
    except Exception:
        return None


def _hours_elapsed(ts_str: str) -> float:
    """hunt timestamp 이후 경과 시간(시간 단위)."""
    dt = _parse_ts(ts_str)
    if dt is None:
        return 0.0
    return (_now() - dt).total_seconds() / 3600


def _fetch_post_hunt_closes(ticker: str, hunt_ts: str,
                             max_days: int = SWING_DAYS + 3) -> pd.Series | None:
    """
    hunt_ts 이후의 일봉 종가 시리즈 반환.
    hunt 당일은 제외 (익일 종가부터).
    실패 시 None.
    """
    dt = _parse_ts(hunt_ts)
    if dt is None:
        return None

    # yfinance start/end — 캘린더일 기준 충분히 넓게
    start_date = dt.date()
    end_date   = (dt + timedelta(days=max_days * 2 + 5)).date()

    try:
        df = yf.download(
            ticker,
            start=str(start_date),
            end=str(end_date),
            interval="1d",
            progress=False,
            auto_adjust=True,
        )
    except Exception as e:
        log.warning(f"  yfinance 실패 [{ticker}]: {e}")
        return None

    if df is None or df.empty:
        return None

    # MultiIndex 처리 (yfinance ≥ 0.2.x)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    if "Close" not in df.columns:
        return None

    # hunt 당일 이후만 (> 당일)
    hunt_date = pd.Timestamp(start_date)
    series = df.loc[df.index > hunt_date, "Close"].dropna()

    return series if not series.empty else None


# ══════════════════════════════════════════════════════════════════
# Outcome 계산
# ══════════════════════════════════════════════════════════════════

def _calc_outcomes(entry: dict, closes: pd.Series) -> dict:
    """
    종가 시리즈에서 outcome 필드 계산.

    Returns:
        dict with keys:
          confirmed (bool) : True면 outcome_checked=True 확정
          partial   (bool) : 1d 데이터만 있어 부분 기록
          ...outcome fields...
    """
    price_at_hunt = float(entry.get("price_at_hunt") or 0)
    if price_at_hunt <= 0:
        return {"confirmed": False, "partial": False}

    n_rows = len(closes)
    result: dict = {"confirmed": False, "partial": False}

    # ── 1일 결과 ─────────────────────────────────────────────────
    if n_rows >= 1:
        p1d   = float(closes.iloc[0])
        pct1d = round((p1d / price_at_hunt - 1) * 100, 3)
        result.update({
            "price_1d_later": round(p1d, 4),
            "outcome_1d_pct": pct1d,
            "outcome_1d_hit": pct1d >= D1_HIT_PCT,
        })
        result["partial"] = True

    # ── 스윙/5일 결과 ─────────────────────────────────────────────
    if n_rows >= MIN_SWING_ROWS:
        swing_window = closes.iloc[:SWING_DAYS]
        peak_val     = float(swing_window.max())
        peak_idx     = int(swing_window.values.argmax()) + 1   # 1-based

        peak_pct     = round((peak_val / price_at_hunt - 1) * 100, 3)
        swing_hit    = peak_pct >= SWING_HIT_PCT

        p5d_idx      = min(4, n_rows - 1)
        p5d          = float(closes.iloc[p5d_idx])
        pct5d        = round((p5d / price_at_hunt - 1) * 100, 3)

        result.update({
            "price_peak":         round(peak_val, 4),
            "peak_day":           peak_idx,
            "peak_pct":           peak_pct,
            "outcome_swing_hit":  swing_hit,
            "price_5d_later":     round(p5d, 4),
            "outcome_pct":        pct5d,
            "outcome_correct":    swing_hit,
            "outcome_checked":    True,
            "confirmed":          True,
            "partial":            False,
        })

    return result


# ══════════════════════════════════════════════════════════════════
# 가중치 업데이트
# ══════════════════════════════════════════════════════════════════

def _update_weights(weights: dict, entry: dict) -> list[str]:
    """
    완전히 확정된 entry 1건으로 정확도 DB + 신호 가중치 업데이트.

    alerted=True (발송 완료 신호)만 가중치 조정에 반영.
    모든 outcome_correct 항목은 정확도 통계에 포함.

    Returns: 변경 로그 (문자열 리스트)
    """
    changes: list[str] = []
    correct    = bool(entry.get("outcome_correct"))
    d1_correct = bool(entry.get("outcome_1d_hit"))
    alerted    = bool(entry.get("alerted"))
    ticker     = entry.get("ticker", "")
    signals    = entry.get("signals_fired", [])
    verdict    = entry.get("devil_verdict", "")
    regime     = entry.get("orca_regime", "")
    peak_pct   = entry.get("peak_pct", 0.0) or 0.0
    peak_day   = entry.get("peak_day", 0) or 0

    sw = weights.setdefault("signal_weights", {})

    # ── 1. signal_accuracy ──────────────────────────────────────
    sig_acc = weights.setdefault("signal_accuracy", {})
    for sig in signals:
        # 길이 >40 또는 빈 신호 (런타임 잡신호) 무시
        if not sig or len(sig) > 40:
            continue
        rec = sig_acc.setdefault(sig, {
            "total": 0, "swing_correct": 0, "swing_accuracy": 0.0,
            "d1_correct": 0, "d1_accuracy": 0.0,
        })
        rec["total"]          += 1
        rec["swing_correct"]  += int(correct)
        rec["d1_correct"]     += int(d1_correct)
        n = rec["total"]
        rec["swing_accuracy"] = round(rec["swing_correct"] / n * 100, 1)
        rec["d1_accuracy"]    = round(rec["d1_correct"]    / n * 100, 1)

        # 가중치 조정 (alerted 신호 + 최소 샘플 이상)
        if alerted and n >= MIN_SAMPLES_ADJ and sig in sw:
            acc = rec["swing_correct"] / n
            adj = WEIGHT_ADJ_UP if acc >= 0.70 else -WEIGHT_ADJ_DOWN if acc <= 0.40 else 0.0
            if adj != 0.0:
                old = sw[sig]
                new = round(max(WEIGHT_MIN, min(WEIGHT_MAX, old + adj)), 4)
                sw[sig] = new
                if abs(old - new) > 0.001:
                    changes.append(
                        f"signal[{sig}]: {old:.3f}→{new:.3f} "
                        f"(acc={acc:.0%}, n={n}, "
                        f"peak={entry.get('peak_pct', 0):+.1f}%)"
                    )

    # ── 2. ticker_accuracy ──────────────────────────────────────
    if ticker:
        tkr = weights.setdefault("ticker_accuracy", {})
        rec = tkr.setdefault(ticker, {
            "total": 0, "correct": 0, "accuracy": 0.0, "avg_peak_day": 0.0,
        })
        n = rec["total"] + 1
        rec["total"]   = n
        rec["correct"] += int(correct)
        rec["accuracy"] = round(rec["correct"] / n * 100, 1)
        if peak_day:
            # 누적 이동 평균
            rec["avg_peak_day"] = round(
                (rec.get("avg_peak_day", 0.0) * (n - 1) + peak_day) / n, 1
            )

    # ── 3. devil_accuracy ───────────────────────────────────────
    if verdict in ("동의", "부분동의", "반대"):
        dev = weights.setdefault("devil_accuracy", {
            "동의": {"correct": 0, "total": 0},
            "부분동의": {"correct": 0, "total": 0},
            "반대": {"correct": 0, "total": 0},
        })
        rec = dev.setdefault(verdict, {"correct": 0, "total": 0})
        rec["total"] += 1
        # Devil 정확도:
        #   "동의/부분동의" → 실제 성공이면 correct (Devil이 맞음)
        #   "반대"          → 실제 실패이면 correct (Devil의 반론이 맞음)
        if verdict == "반대":
            if not correct:
                rec["correct"] += 1
        else:
            if correct:
                rec["correct"] += 1

    # ── 4. regime_accuracy ──────────────────────────────────────
    if regime:
        reg = weights.setdefault("regime_accuracy", {})
        # 레짐 문자열이 길면 키 압축 (앞 25자)
        reg_key = regime[:25].strip()
        rec = reg.setdefault(reg_key, {
            "total": 0, "correct": 0, "accuracy": 0.0, "avg_peak": 0.0,
        })
        n = rec["total"] + 1
        rec["total"]   = n
        rec["correct"] += int(correct)
        rec["accuracy"] = round(rec["correct"] / n * 100, 1)
        if peak_pct:
            rec["avg_peak"] = round(
                (rec.get("avg_peak", 0.0) * (n - 1) + peak_pct) / n, 1
            )

    return changes


# ══════════════════════════════════════════════════════════════════
# 메인 추적 루프
# ══════════════════════════════════════════════════════════════════

def run_tracker(dry_run: bool = False, all_entries: bool = False) -> dict:
    """
    outcome 추적 메인 루프.

    Args:
        dry_run    : True면 파일 저장 없이 결과 출력만
        all_entries: True면 alerted 여부 무관 전체 미확정 항목 처리

    Returns:
        {total_pending, confirmed, partial, skipped, weight_changes, errors}
    """
    log.info("📍 Jackal Tracker 시작")

    logs = list(reversed(list_jackal_live_events("hunt", limit=500)))
    if not logs:
        if not HUNT_LOG_FILE.exists():
            log.info("  hunt_log.json 없음 — 종료")
            return {"total_pending": 0, "confirmed": 0, "partial": 0,
                    "skipped": 0, "weight_changes": [], "errors": 0}

        try:
            logs = json.loads(HUNT_LOG_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            log.error(f"  hunt_log.json 읽기 실패: {e}")
            return {"total_pending": 0, "confirmed": 0, "partial": 0,
                    "skipped": 0, "weight_changes": [], "errors": 1}

    # ── 처리 대상 필터링 ───────────────────────────────────────────
    pending = [
        e for e in logs
        if not e.get("outcome_checked")
        and _hours_elapsed(e.get("timestamp", "")) >= MIN_ELAPSED_HOURS
        and (all_entries or e.get("alerted") or e.get("is_entry"))
    ]

    total_all = sum(1 for e in logs if not e.get("outcome_checked"))
    log.info(
        f"  미확정 전체 {total_all}건 / 처리 대상 {len(pending)}건 "
        f"({'전체 모드' if all_entries else 'alerted 모드'})"
    )

    if not pending:
        log.info("  추적할 항목 없음")
        return {"total_pending": total_all, "confirmed": 0, "partial": 0,
                "skipped": 0, "weight_changes": [], "errors": 0}

    # ── 가중치 로드 ────────────────────────────────────────────────
    weights: dict = {}
    snapshot = load_latest_jackal_weight_snapshot()
    if isinstance(snapshot, dict):
        weights = snapshot
    elif WEIGHTS_FILE.exists():
        try:
            weights = json.loads(WEIGHTS_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning(f"  weights 로드 실패: {e}")

    # ── 티커별 그룹핑 (yfinance 호출 최소화) ─────────────────────
    ticker_map: dict[str, list] = {}
    for entry in pending:
        t = entry.get("ticker", "")
        if t:
            ticker_map.setdefault(t, []).append(entry)

    stats = {
        "total_pending": total_all,
        "confirmed": 0,
        "partial": 0,
        "skipped": 0,
        "weight_changes": [],
        "errors": 0,
    }
    all_weight_changes: list[str] = []

    for ticker, entries in ticker_map.items():
        # 가장 오래된 항목 기준으로 데이터 조회
        oldest_ts = min(e["timestamp"] for e in entries)

        log.info(f"\n  [{ticker}] {len(entries)}건 처리 중...")
        time.sleep(YFINANCE_DELAY)

        closes_full = _fetch_post_hunt_closes(ticker, oldest_ts)
        if closes_full is None:
            log.warning(f"    데이터 없음 — 스킵")
            stats["skipped"] += len(entries)
            continue

        log.info(
            f"    {len(closes_full)}거래일치 확보 "
            f"({closes_full.index[0].date()} ~ {closes_full.index[-1].date()})"
        )

        for entry in entries:
            ts    = entry.get("timestamp", "")
            hunt_dt = _parse_ts(ts)
            if hunt_dt is None:
                stats["skipped"] += 1
                continue

            # 이 항목의 hunt 날짜 이후 데이터 슬라이싱
            hunt_date = pd.Timestamp(hunt_dt.date())
            entry_closes = closes_full[closes_full.index > hunt_date].copy()

            if entry_closes.empty:
                log.debug(f"    {ticker} [{ts[:10]}]: hunt 이후 데이터 없음")
                stats["skipped"] += 1
                continue

            outcome = _calc_outcomes(entry, entry_closes)

            if not outcome.get("partial") and not outcome.get("confirmed"):
                # 1일 데이터도 없음
                stats["skipped"] += 1
                continue

            if not dry_run:
                # 1일 결과 (항상 덮어씀)
                if "price_1d_later" in outcome:
                    entry["price_1d_later"]  = outcome["price_1d_later"]
                    entry["outcome_1d_pct"]  = outcome["outcome_1d_pct"]
                    entry["outcome_1d_hit"]  = outcome["outcome_1d_hit"]

                if outcome.get("confirmed"):
                    # 스윙 결과 확정
                    entry["price_peak"]        = outcome["price_peak"]
                    entry["peak_day"]           = outcome["peak_day"]
                    entry["peak_pct"]           = outcome["peak_pct"]
                    entry["outcome_swing_hit"]  = outcome["outcome_swing_hit"]
                    entry["price_5d_later"]     = outcome["price_5d_later"]
                    entry["outcome_pct"]        = outcome["outcome_pct"]
                    entry["outcome_correct"]    = outcome["outcome_correct"]
                    entry["outcome_checked"]    = True
                    entry["outcome_tracked_at"] = _now().isoformat()

                    stats["confirmed"] += 1
                    icon = "✅" if outcome["outcome_swing_hit"] else "❌"
                    log.info(
                        f"    {icon} {ticker} [{ts[:10]}]: "
                        f"swing={outcome['outcome_swing_hit']} "
                        f"peak={outcome['peak_pct']:+.1f}% D{outcome['peak_day']} "
                        f"| 1d={outcome.get('outcome_1d_pct', 0):+.1f}% "
                        f"| 5d={outcome.get('outcome_pct', 0):+.1f}%"
                    )

                    # 가중치 업데이트
                    changes = _update_weights(weights, entry)
                    all_weight_changes.extend(changes)

                elif outcome.get("partial"):
                    stats["partial"] += 1
                    log.info(
                        f"    ⏳ {ticker} [{ts[:10]}]: "
                        f"1d만 기록 "
                        f"({outcome.get('outcome_1d_pct', 0):+.1f}%), "
                        f"스윙 대기 ({len(entry_closes)}/{MIN_SWING_ROWS} rows)"
                    )
            else:
                # dry-run
                if outcome.get("confirmed"):
                    icon = "✅" if outcome.get("outcome_swing_hit") else "❌"
                    log.info(
                        f"    [dry] {icon} {ticker}: "
                        f"peak={outcome.get('peak_pct', 0):+.1f}% "
                        f"D{outcome.get('peak_day',0)}"
                    )
                elif outcome.get("partial"):
                    log.info(
                        f"    [dry] ⏳ {ticker}: "
                        f"1d={outcome.get('outcome_1d_pct', 0):+.1f}%"
                    )
                stats["confirmed"] += int(outcome.get("confirmed", False))
                stats["partial"]   += int(outcome.get("partial", False) and not outcome.get("confirmed", False))

    stats["weight_changes"] = all_weight_changes

    # ── 저장 ──────────────────────────────────────────────────────
    if not dry_run:
        if stats["confirmed"] > 0 or stats["partial"] > 0:
            try:
                retained_logs = logs[-500:]
                atomic_write_json(HUNT_LOG_FILE, retained_logs)
                sync_jackal_live_events("hunt", retained_logs)
                log.info(f"\n  💾 hunt_log.json 저장 ({stats['confirmed']}건 확정, {stats['partial']}건 부분)")
            except Exception as e:
                log.error(f"  hunt_log.json 저장 실패: {e}")
                stats["errors"] += 1

        if all_weight_changes and weights:
            try:
                weights["last_updated"] = _now().isoformat()
                atomic_write_json(WEIGHTS_FILE, weights)
                record_jackal_weight_snapshot(weights, source="tracker")
                log.info(f"  💾 jackal_weights.json 저장 ({len(all_weight_changes)}건 변경)")
            except Exception as e:
                log.error(f"  jackal_weights.json 저장 실패: {e}")
                stats["errors"] += 1

    return stats


# ══════════════════════════════════════════════════════════════════
# 텔레그램 요약 발송 (선택)
# ══════════════════════════════════════════════════════════════════

def _send_tracker_summary(stats: dict):
    """
    추적 결과를 텔레그램으로 요약 발송.
    확정된 항목이 있을 때만 발송.
    """
    if stats["confirmed"] == 0:
        return

    token   = os.environ.get("TELEGRAM_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return

    try:
        import httpx
        now_str = _now().strftime("%m/%d %H:%M")
        changes_str = (
            "\n".join(f"  • {c}" for c in stats["weight_changes"][:5])
            if stats["weight_changes"] else "  없음"
        )
        text = (
            f"📍 <b>Jackal Tracker</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ 확정: {stats['confirmed']}건  "
            f"⏳ 부분: {stats['partial']}건\n"
            f"🔧 가중치 변경:\n{changes_str}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⏰ {now_str} KST"
        )
        httpx.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"텔레그램 발송 실패: {e}")


# ══════════════════════════════════════════════════════════════════
# 콘솔 출력
# ══════════════════════════════════════════════════════════════════

def print_summary(stats: dict):
    print("\n" + "=" * 54)
    print("  📍 Jackal Tracker — 실행 결과")
    print("=" * 54)
    print(f"  미확정 전체  : {stats['total_pending']}건")
    print(f"  스윙 확정    : {stats['confirmed']}건  ✅")
    print(f"  1일만 기록   : {stats['partial']}건   ⏳")
    print(f"  스킵         : {stats['skipped']}건   (데이터 부족/조건 미달)")
    print(f"  오류         : {stats['errors']}건")

    if stats["weight_changes"]:
        print(f"\n  가중치 변경 {len(stats['weight_changes'])}건:")
        for ch in stats["weight_changes"]:
            print(f"    {ch}")
    else:
        print("\n  가중치 변경: 없음 (샘플 부족 또는 변화 없음)")

    print("=" * 54 + "\n")


# ══════════════════════════════════════════════════════════════════
# 진입점
# ══════════════════════════════════════════════════════════════════

def main() -> None:
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    parser = argparse.ArgumentParser(description="Jackal Outcome Tracker")
    parser.add_argument("--dry-run", action="store_true",
                        help="저장 없이 결과만 미리보기")
    parser.add_argument("--all",     action="store_true",
                        help="alerted 여부 무관 전체 미확정 항목 처리")
    parser.add_argument("--notify",  action="store_true",
                        help="완료 후 텔레그램 요약 발송")
    args = parser.parse_args()

    result = run_tracker(dry_run=args.dry_run, all_entries=args.all)
    print_summary(result)

    if args.notify and not args.dry_run:
        _send_tracker_summary(result)


if __name__ == "__main__":
    main()

