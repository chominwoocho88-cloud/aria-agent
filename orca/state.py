"""SQLite-backed state spine for ORCA runs, predictions, and outcomes."""
from __future__ import annotations

import json
import sqlite3
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from jackal.families import canonical_family_key, family_label

from .paths import STATE_DB_FILE

KST = timezone(timedelta(hours=9))


def _now() -> datetime:
    return datetime.now(KST)


def _now_iso() -> str:
    return datetime.now(KST).isoformat()


def _json(data: Any) -> str | None:
    if data is None:
        return None
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def _candidate_systems(system: str) -> list[str]:
    return [system]


def _connect() -> sqlite3.Connection:
    STATE_DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(STATE_DB_FILE, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def init_state_db() -> None:
    with _connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                system TEXT NOT NULL,
                mode TEXT,
                analysis_date TEXT,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                status TEXT NOT NULL,
                data_quality TEXT,
                report_path TEXT,
                report_summary TEXT,
                metadata_json TEXT
            );

            CREATE TABLE IF NOT EXISTS predictions (
                prediction_id TEXT PRIMARY KEY,
                external_key TEXT NOT NULL UNIQUE,
                run_id TEXT NOT NULL,
                system TEXT NOT NULL,
                analysis_date TEXT NOT NULL,
                mode TEXT,
                prediction_kind TEXT NOT NULL,
                subject TEXT,
                category TEXT,
                event_name TEXT,
                direction TEXT,
                confidence TEXT,
                market_regime TEXT,
                trend_phase TEXT,
                summary_json TEXT,
                created_at TEXT NOT NULL,
                resolved_at TEXT,
                last_outcome_id TEXT,
                status TEXT NOT NULL DEFAULT 'open',
                FOREIGN KEY(run_id) REFERENCES runs(run_id)
            );

            CREATE TABLE IF NOT EXISTS outcomes (
                outcome_id TEXT PRIMARY KEY,
                prediction_id TEXT NOT NULL UNIQUE,
                analysis_date TEXT NOT NULL,
                verdict TEXT NOT NULL,
                evidence TEXT,
                category TEXT,
                resolved_at TEXT NOT NULL,
                metadata_json TEXT,
                FOREIGN KEY(prediction_id) REFERENCES predictions(prediction_id)
            );

            CREATE INDEX IF NOT EXISTS idx_runs_system_date
                ON runs(system, analysis_date);

            CREATE INDEX IF NOT EXISTS idx_predictions_lookup
                ON predictions(system, analysis_date, prediction_kind, event_name);

            CREATE INDEX IF NOT EXISTS idx_predictions_status
                ON predictions(status, analysis_date);

            CREATE INDEX IF NOT EXISTS idx_outcomes_analysis_date
                ON outcomes(analysis_date, verdict);

            CREATE TABLE IF NOT EXISTS backtest_sessions (
                session_id TEXT PRIMARY KEY,
                system TEXT NOT NULL,
                label TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                status TEXT NOT NULL,
                config_json TEXT,
                summary_json TEXT
            );

            CREATE TABLE IF NOT EXISTS backtest_state (
                session_id TEXT NOT NULL,
                state_key TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(session_id, state_key),
                FOREIGN KEY(session_id) REFERENCES backtest_sessions(session_id)
            );

            CREATE TABLE IF NOT EXISTS backtest_daily_results (
                session_id TEXT NOT NULL,
                analysis_date TEXT NOT NULL,
                phase_label TEXT NOT NULL,
                market_note TEXT,
                analysis_json TEXT,
                results_json TEXT,
                metrics_json TEXT,
                created_at TEXT NOT NULL,
                PRIMARY KEY(session_id, analysis_date, phase_label),
                FOREIGN KEY(session_id) REFERENCES backtest_sessions(session_id)
            );

            CREATE INDEX IF NOT EXISTS idx_backtest_sessions_system
                ON backtest_sessions(system, started_at);

            CREATE INDEX IF NOT EXISTS idx_backtest_daily_session
                ON backtest_daily_results(session_id, analysis_date);

            CREATE TABLE IF NOT EXISTS backtest_pick_results (
                session_id TEXT NOT NULL,
                system TEXT NOT NULL,
                source_session_id TEXT,
                analysis_date TEXT NOT NULL,
                phase_label TEXT NOT NULL,
                selection_stage TEXT NOT NULL,
                rank_index INTEGER NOT NULL,
                ticker TEXT NOT NULL,
                regime TEXT,
                scores_json TEXT,
                indicators_json TEXT,
                outcome_json TEXT,
                created_at TEXT NOT NULL,
                PRIMARY KEY(session_id, analysis_date, selection_stage, rank_index, ticker),
                FOREIGN KEY(session_id) REFERENCES backtest_sessions(session_id)
            );

            CREATE INDEX IF NOT EXISTS idx_backtest_picks_session
                ON backtest_pick_results(session_id, analysis_date);

            CREATE TABLE IF NOT EXISTS jackal_shadow_signals (
                shadow_id TEXT PRIMARY KEY,
                external_key TEXT NOT NULL UNIQUE,
                signal_timestamp TEXT NOT NULL,
                analysis_date TEXT NOT NULL,
                ticker TEXT NOT NULL,
                market TEXT,
                signal_family TEXT,
                quality_label TEXT,
                quality_score REAL,
                status TEXT NOT NULL DEFAULT 'open',
                payload_json TEXT NOT NULL,
                outcome_json TEXT,
                created_at TEXT NOT NULL,
                resolved_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_jackal_shadow_pending
                ON jackal_shadow_signals(status, signal_timestamp);

            CREATE TABLE IF NOT EXISTS jackal_shadow_batches (
                batch_id TEXT PRIMARY KEY,
                recorded_at TEXT NOT NULL,
                total INTEGER NOT NULL,
                worked INTEGER NOT NULL,
                rate REAL NOT NULL,
                metadata_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_jackal_shadow_batches_recorded_at
                ON jackal_shadow_batches(recorded_at);

            CREATE TABLE IF NOT EXISTS jackal_live_events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                external_key TEXT NOT NULL UNIQUE,
                ticker TEXT NOT NULL,
                event_timestamp TEXT NOT NULL,
                analysis_date TEXT NOT NULL,
                alerted INTEGER NOT NULL DEFAULT 0,
                is_entry INTEGER NOT NULL DEFAULT 0,
                outcome_checked INTEGER NOT NULL DEFAULT 0,
                payload_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_jackal_live_events_lookup
                ON jackal_live_events(event_type, event_timestamp);

            CREATE INDEX IF NOT EXISTS idx_jackal_live_events_pending
                ON jackal_live_events(event_type, outcome_checked, alerted, is_entry);

            CREATE TABLE IF NOT EXISTS jackal_weight_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                weights_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_jackal_weight_snapshots_latest
                ON jackal_weight_snapshots(captured_at DESC);

            CREATE TABLE IF NOT EXISTS jackal_cooldowns (
                cooldown_key TEXT PRIMARY KEY,
                ticker TEXT NOT NULL,
                signal_family TEXT,
                cooldown_at TEXT NOT NULL,
                quality_score REAL,
                last_override_at TEXT,
                override_reason TEXT,
                override_quality REAL,
                override_count INTEGER NOT NULL DEFAULT 0,
                payload_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_jackal_cooldowns_lookup
                ON jackal_cooldowns(ticker, signal_family, cooldown_at DESC);

            CREATE TABLE IF NOT EXISTS jackal_recommendations (
                recommendation_id TEXT PRIMARY KEY,
                external_key TEXT NOT NULL UNIQUE,
                ticker TEXT NOT NULL,
                market TEXT,
                recommended_at TEXT NOT NULL,
                analysis_date TEXT NOT NULL,
                outcome_checked INTEGER NOT NULL DEFAULT 0,
                outcome_pct REAL,
                outcome_correct INTEGER,
                payload_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_jackal_recommendations_lookup
                ON jackal_recommendations(recommended_at DESC, outcome_checked, ticker);

            CREATE TABLE IF NOT EXISTS jackal_accuracy_projection (
                projection_id TEXT PRIMARY KEY,
                snapshot_id TEXT NOT NULL,
                source TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                family TEXT NOT NULL,
                scope TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                correct REAL,
                total REAL,
                accuracy REAL,
                metrics_json TEXT,
                updated_at TEXT NOT NULL,
                UNIQUE(snapshot_id, family, scope, entity_key)
            );

            CREATE INDEX IF NOT EXISTS idx_jackal_accuracy_projection_lookup
                ON jackal_accuracy_projection(family, scope, captured_at DESC, entity_key);

            CREATE VIEW IF NOT EXISTS jackal_accuracy_current AS
            SELECT p.projection_id,
                   p.snapshot_id,
                   p.source,
                   p.captured_at,
                   p.family,
                   p.scope,
                   p.entity_key,
                   p.correct,
                   p.total,
                   p.accuracy,
                   p.metrics_json,
                   p.updated_at
              FROM jackal_accuracy_projection p
              JOIN (
                    SELECT family, scope, entity_key, MAX(captured_at) AS captured_at
                      FROM jackal_accuracy_projection
                     GROUP BY family, scope, entity_key
                   ) latest
                ON p.family = latest.family
               AND p.scope = latest.scope
               AND p.entity_key = latest.entity_key
               AND p.captured_at = latest.captured_at;

            CREATE TABLE IF NOT EXISTS candidate_registry (
                candidate_id TEXT PRIMARY KEY,
                external_key TEXT NOT NULL UNIQUE,
                source_system TEXT NOT NULL,
                source_event_type TEXT NOT NULL,
                source_event_id TEXT,
                source_run_id TEXT,
                source_session_id TEXT,
                ticker TEXT NOT NULL,
                name TEXT,
                market TEXT,
                detected_at TEXT NOT NULL,
                analysis_date TEXT NOT NULL,
                signal_family TEXT,
                quality_label TEXT,
                quality_score REAL,
                orca_alignment TEXT,
                status TEXT NOT NULL DEFAULT 'open',
                payload_json TEXT NOT NULL,
                latest_outcome_horizon TEXT,
                latest_outcome_at TEXT,
                latest_outcome_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_candidate_registry_status
                ON candidate_registry(status, detected_at DESC);

            CREATE INDEX IF NOT EXISTS idx_candidate_registry_lookup
                ON candidate_registry(source_system, source_event_type, analysis_date, ticker);

            CREATE TABLE IF NOT EXISTS candidate_reviews (
                review_id TEXT PRIMARY KEY,
                candidate_id TEXT NOT NULL,
                run_id TEXT,
                analysis_date TEXT NOT NULL,
                reviewed_at TEXT NOT NULL,
                alignment TEXT,
                review_verdict TEXT,
                orca_regime TEXT,
                orca_trend TEXT,
                confidence TEXT,
                thesis_killer TEXT,
                review_json TEXT NOT NULL,
                UNIQUE(candidate_id, run_id),
                FOREIGN KEY(candidate_id) REFERENCES candidate_registry(candidate_id),
                FOREIGN KEY(run_id) REFERENCES runs(run_id)
            );

            CREATE INDEX IF NOT EXISTS idx_candidate_reviews_candidate
                ON candidate_reviews(candidate_id, reviewed_at DESC);

            CREATE TABLE IF NOT EXISTS candidate_outcomes (
                outcome_id TEXT PRIMARY KEY,
                candidate_id TEXT NOT NULL,
                horizon_label TEXT NOT NULL,
                status TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                price_reference REAL,
                price_outcome REAL,
                return_pct REAL,
                hit INTEGER,
                outcome_json TEXT NOT NULL,
                UNIQUE(candidate_id, horizon_label),
                FOREIGN KEY(candidate_id) REFERENCES candidate_registry(candidate_id)
            );

            CREATE INDEX IF NOT EXISTS idx_candidate_outcomes_candidate
                ON candidate_outcomes(candidate_id, observed_at DESC);

            CREATE TABLE IF NOT EXISTS candidate_lessons (
                lesson_id TEXT PRIMARY KEY,
                candidate_id TEXT NOT NULL,
                outcome_id TEXT,
                lesson_type TEXT NOT NULL,
                label TEXT,
                lesson_value REAL,
                lesson_timestamp TEXT NOT NULL,
                lesson_json TEXT NOT NULL,
                FOREIGN KEY(candidate_id) REFERENCES candidate_registry(candidate_id),
                FOREIGN KEY(outcome_id) REFERENCES candidate_outcomes(outcome_id)
            );

            CREATE INDEX IF NOT EXISTS idx_candidate_lessons_candidate
                ON candidate_lessons(candidate_id, lesson_timestamp DESC);
            """
        )


def start_run(system: str, mode: str, analysis_date: str, metadata: dict | None = None) -> str:
    init_state_db()
    run_id = f"run_{uuid4().hex}"
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO runs (
                run_id, system, mode, analysis_date, started_at, status, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, system, mode, analysis_date, _now_iso(), "running", _json(metadata)),
        )
    return run_id


def finish_run(
    run_id: str,
    status: str,
    *,
    data_quality: str | None = None,
    report_path: str | None = None,
    report_summary: str | None = None,
    metadata: dict | None = None,
) -> None:
    init_state_db()
    with _connect() as conn:
        row = conn.execute(
            "SELECT metadata_json FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        merged_meta: dict[str, Any] = {}
        if row and row["metadata_json"]:
            try:
                merged_meta = json.loads(row["metadata_json"])
            except Exception:
                merged_meta = {}
        if metadata:
            merged_meta.update(metadata)

        conn.execute(
            """
            UPDATE runs
               SET ended_at = ?,
                   status = ?,
                   data_quality = COALESCE(?, data_quality),
                   report_path = COALESCE(?, report_path),
                   report_summary = COALESCE(?, report_summary),
                   metadata_json = ?
             WHERE run_id = ?
            """,
            (
                _now_iso(),
                status,
                data_quality,
                report_path,
                report_summary,
                _json(merged_meta),
                run_id,
            ),
        )


def _upsert_prediction(conn: sqlite3.Connection, record: dict[str, Any]) -> str:
    existing = conn.execute(
        "SELECT prediction_id, status, resolved_at, last_outcome_id FROM predictions WHERE external_key = ?",
        (record["external_key"],),
    ).fetchone()

    if existing:
        prediction_id = existing["prediction_id"]
        conn.execute(
            """
            UPDATE predictions
               SET run_id = ?,
                   system = ?,
                   analysis_date = ?,
                   mode = ?,
                   prediction_kind = ?,
                   subject = ?,
                   category = ?,
                   event_name = ?,
                   direction = ?,
                   confidence = ?,
                   market_regime = ?,
                   trend_phase = ?,
                   summary_json = ?,
                   status = ?,
                   resolved_at = ?,
                   last_outcome_id = ?
             WHERE prediction_id = ?
            """,
            (
                record["run_id"],
                record["system"],
                record["analysis_date"],
                record["mode"],
                record["prediction_kind"],
                record["subject"],
                record["category"],
                record["event_name"],
                record["direction"],
                record["confidence"],
                record["market_regime"],
                record["trend_phase"],
                _json(record["summary_json"]),
                existing["status"],
                existing["resolved_at"],
                existing["last_outcome_id"],
                prediction_id,
            ),
        )
        return prediction_id

    prediction_id = f"pred_{uuid4().hex}"
    conn.execute(
        """
        INSERT INTO predictions (
            prediction_id, external_key, run_id, system, analysis_date, mode,
            prediction_kind, subject, category, event_name, direction, confidence,
            market_regime, trend_phase, summary_json, created_at, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            prediction_id,
            record["external_key"],
            record["run_id"],
            record["system"],
            record["analysis_date"],
            record["mode"],
            record["prediction_kind"],
            record["subject"],
            record["category"],
            record["event_name"],
            record["direction"],
            record["confidence"],
            record["market_regime"],
            record["trend_phase"],
            _json(record["summary_json"]),
            _now_iso(),
            "open",
        ),
    )
    return prediction_id


def record_report_predictions(run_id: str, report: dict) -> dict[str, Any]:
    init_state_db()
    analysis_date = str(report.get("analysis_date", "")) or _now_iso()[:10]
    mode = str(report.get("mode", "MORNING"))
    regime = str(report.get("market_regime", ""))
    trend = str(report.get("trend_phase", ""))
    confidence = str(report.get("confidence_overall", ""))

    created = 0
    with _connect() as conn:
        summary_payload = {
            "one_line_summary": report.get("one_line_summary", ""),
            "consensus_level": report.get("consensus_level", ""),
            "tomorrow_setup": report.get("tomorrow_setup", ""),
            "counterarguments": report.get("counterarguments", [])[:3],
        }
        _upsert_prediction(
            conn,
            {
                "external_key": f"aria:{analysis_date}:{mode}:report_summary",
                "run_id": run_id,
                "system": "aria",
                "analysis_date": analysis_date,
                "mode": mode,
                "prediction_kind": "report_summary",
                "subject": "market_outlook",
                "category": "report",
                "event_name": str(report.get("one_line_summary", ""))[:160],
                "direction": trend or regime,
                "confidence": confidence,
                "market_regime": regime,
                "trend_phase": trend,
                "summary_json": summary_payload,
            },
        )
        created += 1

        for idx, tk in enumerate(report.get("thesis_killers", [])):
            event_name = str(tk.get("event", "")).strip()
            if not event_name:
                continue
            _upsert_prediction(
                conn,
                {
                    "external_key": f"aria:{analysis_date}:{mode}:thesis:{event_name}",
                    "run_id": run_id,
                    "system": "aria",
                    "analysis_date": analysis_date,
                    "mode": mode,
                    "prediction_kind": "thesis_killer",
                    "subject": event_name,
                    "category": str(tk.get("quality", "")) or "thesis_killer",
                    "event_name": event_name,
                    "direction": str(tk.get("confirms_if", ""))[:160],
                    "confidence": confidence,
                    "market_regime": regime,
                    "trend_phase": trend,
                    "summary_json": {
                        "index": idx,
                        "timeframe": tk.get("timeframe", ""),
                        "confirms_if": tk.get("confirms_if", ""),
                        "invalidates_if": tk.get("invalidates_if", ""),
                        "quality": tk.get("quality", ""),
                    },
                },
            )
            created += 1

    return {"count": created, "analysis_date": analysis_date}


def resolve_verification_outcomes(
    source_analysis_date: str,
    results: list[dict],
    *,
    resolved_analysis_date: str,
    metadata: dict | None = None,
) -> dict[str, Any]:
    init_state_db()
    if not source_analysis_date:
        return {"matched": 0, "unmatched": [], "updated": 0}

    matched = 0
    updated = 0
    unmatched: list[str] = []
    resolved_at = _now_iso()

    systems = _candidate_systems("orca")

    with _connect() as conn:
        for result in results:
            event_name = str(result.get("event", "")).strip()
            if not event_name:
                continue

            prediction = conn.execute(
                """
                SELECT prediction_id
                  FROM predictions
                 WHERE system IN (?, ?)
                   AND analysis_date = ?
                   AND prediction_kind = 'thesis_killer'
                   AND event_name = ?
                 ORDER BY CASE system WHEN 'orca' THEN 0 ELSE 1 END, created_at DESC
                 LIMIT 1
                """,
                (systems[0], systems[1], source_analysis_date, event_name),
            ).fetchone()

            if not prediction:
                unmatched.append(event_name)
                continue

            prediction_id = prediction["prediction_id"]
            existing = conn.execute(
                "SELECT outcome_id FROM outcomes WHERE prediction_id = ?",
                (prediction_id,),
            ).fetchone()
            outcome_id = existing["outcome_id"] if existing else f"out_{uuid4().hex}"

            if existing:
                conn.execute(
                    """
                    UPDATE outcomes
                       SET analysis_date = ?,
                           verdict = ?,
                           evidence = ?,
                           category = ?,
                           resolved_at = ?,
                           metadata_json = ?
                     WHERE prediction_id = ?
                    """,
                    (
                        resolved_analysis_date,
                        result.get("verdict", "unclear"),
                        result.get("evidence", ""),
                        result.get("category", ""),
                        resolved_at,
                        _json(metadata),
                        prediction_id,
                    ),
                )
                updated += 1
            else:
                conn.execute(
                    """
                    INSERT INTO outcomes (
                        outcome_id, prediction_id, analysis_date, verdict,
                        evidence, category, resolved_at, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        outcome_id,
                        prediction_id,
                        resolved_analysis_date,
                        result.get("verdict", "unclear"),
                        result.get("evidence", ""),
                        result.get("category", ""),
                        resolved_at,
                        _json(metadata),
                    ),
                )
                matched += 1

            conn.execute(
                """
                UPDATE predictions
                   SET status = ?,
                       resolved_at = ?,
                       last_outcome_id = ?
                 WHERE prediction_id = ?
                """,
                (
                    "resolved",
                    resolved_at,
                    outcome_id,
                    prediction_id,
                ),
            )

    return {"matched": matched, "updated": updated, "unmatched": unmatched}


def start_backtest_session(
    system: str,
    label: str,
    *,
    config: dict | None = None,
) -> str:
    init_state_db()
    session_id = f"bt_{uuid4().hex}"
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO backtest_sessions (
                session_id, system, label, started_at, status, config_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (session_id, system, label, _now_iso(), "running", _json(config)),
        )
    return session_id


def load_backtest_state(session_id: str, state_key: str, default: Any = None) -> Any:
    init_state_db()
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT payload_json
              FROM backtest_state
             WHERE session_id = ? AND state_key = ?
            """,
            (session_id, state_key),
        ).fetchone()
    if not row or not row["payload_json"]:
        return deepcopy(default)
    try:
        return json.loads(row["payload_json"])
    except Exception:
        return deepcopy(default)


def save_backtest_state(session_id: str, state_key: str, payload: Any) -> None:
    init_state_db()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO backtest_state (
                session_id, state_key, payload_json, updated_at
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(session_id, state_key) DO UPDATE SET
                payload_json = excluded.payload_json,
                updated_at = excluded.updated_at
            """,
            (session_id, state_key, _json(payload), _now_iso()),
        )


def record_backtest_day(
    session_id: str,
    analysis_date: str,
    phase_label: str,
    *,
    market_note: str = "",
    analysis: dict | None = None,
    results: list[dict] | None = None,
    metrics: dict | None = None,
) -> None:
    init_state_db()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO backtest_daily_results (
                session_id, analysis_date, phase_label, market_note,
                analysis_json, results_json, metrics_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id, analysis_date, phase_label) DO UPDATE SET
                market_note = excluded.market_note,
                analysis_json = excluded.analysis_json,
                results_json = excluded.results_json,
                metrics_json = excluded.metrics_json,
                created_at = excluded.created_at
            """,
            (
                session_id,
                analysis_date,
                phase_label or "default",
                market_note,
                _json(analysis or {}),
                _json(results or []),
                _json(metrics or {}),
                _now_iso(),
            ),
        )


def finish_backtest_session(
    session_id: str,
    status: str,
    *,
    summary: dict | None = None,
) -> None:
    init_state_db()
    with _connect() as conn:
        row = conn.execute(
            "SELECT summary_json FROM backtest_sessions WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        merged_summary: dict[str, Any] = {}
        if row and row["summary_json"]:
            try:
                merged_summary = json.loads(row["summary_json"])
            except Exception:
                merged_summary = {}
        if summary:
            merged_summary.update(summary)

        conn.execute(
            """
            UPDATE backtest_sessions
               SET ended_at = ?,
                   status = ?,
                   summary_json = ?
             WHERE session_id = ?
            """,
            (_now_iso(), status, _json(merged_summary), session_id),
        )


def get_latest_backtest_session(
    system: str,
    *,
    label: str | None = None,
    status: str = "completed",
) -> dict[str, Any] | None:
    init_state_db()
    systems = _candidate_systems(system)
    query = """
        SELECT session_id, system, label, started_at, ended_at, status, config_json, summary_json
          FROM backtest_sessions
         WHERE system IN ({system_placeholders})
           AND status = ?
    """
    query = query.format(system_placeholders=", ".join("?" for _ in systems))
    params: list[Any] = [*systems, status]
    if label:
        query += " AND label = ?"
        params.append(label)
    query += (
        " ORDER BY CASE system "
        + " ".join(
            f"WHEN '{name}' THEN {idx}"
            for idx, name in enumerate(systems)
        )
        + " ELSE 99 END, started_at DESC LIMIT 1"
    )

    with _connect() as conn:
        row = conn.execute(query, params).fetchone()
    if not row:
        return None

    def _decode(value: Any) -> Any:
        if not value:
            return {}
        try:
            return json.loads(value)
        except Exception:
            return {}

    return {
        "session_id": row["session_id"],
        "system": row["system"],
        "label": row["label"],
        "started_at": row["started_at"],
        "ended_at": row["ended_at"],
        "status": row["status"],
        "config": _decode(row["config_json"]),
        "summary": _decode(row["summary_json"]),
    }


def list_backtest_sessions(
    system: str,
    *,
    label: str | None = None,
    status: str = "completed",
    limit: int = 10,
) -> list[dict[str, Any]]:
    init_state_db()
    systems = _candidate_systems(system)
    query = """
        SELECT session_id, system, label, started_at, ended_at, status, config_json, summary_json
          FROM backtest_sessions
         WHERE system IN ({system_placeholders})
           AND status = ?
    """
    query = query.format(system_placeholders=", ".join("?" for _ in systems))
    params: list[Any] = [*systems, status]
    if label:
        query += " AND label = ?"
        params.append(label)
    query += (
        " ORDER BY CASE system "
        + " ".join(
            f"WHEN '{name}' THEN {idx}"
            for idx, name in enumerate(systems)
        )
        + " ELSE 99 END, started_at DESC LIMIT ?"
    )
    params.append(limit)

    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()

    def _decode(value: Any) -> Any:
        if not value:
            return {}
        try:
            return json.loads(value)
        except Exception:
            return {}

    return [
        {
            "session_id": row["session_id"],
            "system": row["system"],
            "label": row["label"],
            "started_at": row["started_at"],
            "ended_at": row["ended_at"],
            "status": row["status"],
            "config": _decode(row["config_json"]),
            "summary": _decode(row["summary_json"]),
        }
        for row in rows
    ]


def list_backtest_days(
    session_id: str,
    *,
    phase_label: str | None = None,
) -> list[dict[str, Any]]:
    init_state_db()
    query = """
        SELECT analysis_date, phase_label, market_note, analysis_json, results_json, metrics_json
          FROM backtest_daily_results
         WHERE session_id = ?
    """
    params: list[Any] = [session_id]
    if phase_label:
        query += " AND phase_label = ?"
        params.append(phase_label)
    query += " ORDER BY analysis_date ASC"

    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()

    def _decode(value: Any, default: Any) -> Any:
        if not value:
            return deepcopy(default)
        try:
            return json.loads(value)
        except Exception:
            return deepcopy(default)

    return [
        {
            "analysis_date": row["analysis_date"],
            "phase_label": row["phase_label"],
            "market_note": row["market_note"] or "",
            "analysis": _decode(row["analysis_json"], {}),
            "results": _decode(row["results_json"], []),
            "metrics": _decode(row["metrics_json"], {}),
        }
        for row in rows
    ]


def record_backtest_pick_results(
    session_id: str,
    system: str,
    analysis_date: str,
    phase_label: str,
    picks: list[dict[str, Any]],
    *,
    source_session_id: str | None = None,
    selection_stage: str = "top5",
) -> None:
    init_state_db()
    created_at = _now_iso()
    with _connect() as conn:
        conn.execute(
            """
            DELETE FROM backtest_pick_results
             WHERE session_id = ? AND analysis_date = ? AND phase_label = ? AND selection_stage = ?
            """,
            (session_id, analysis_date, phase_label, selection_stage),
        )
        for idx, pick in enumerate(picks, start=1):
            conn.execute(
                """
                INSERT INTO backtest_pick_results (
                    session_id, system, source_session_id, analysis_date, phase_label,
                    selection_stage, rank_index, ticker, regime, scores_json,
                    indicators_json, outcome_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    system,
                    source_session_id,
                    analysis_date,
                    phase_label,
                    selection_stage,
                    int(pick.get("rank_index", idx)),
                    str(pick.get("ticker", "")),
                    str(pick.get("regime", "")),
                    _json(pick.get("scores", {})),
                    _json(pick.get("indicators", {})),
                    _json(pick.get("outcome", {})),
                    created_at,
                ),
            )


def record_jackal_shadow_signal(entry: dict[str, Any]) -> str:
    init_state_db()
    signal_timestamp = str(entry.get("timestamp") or _now_iso())
    analysis_date = signal_timestamp.split("T", 1)[0]
    ticker = str(entry.get("ticker", ""))
    signal_family = str(entry.get("signal_family", ""))
    quality_score = entry.get("quality_score")
    external_key = str(
        entry.get("external_key")
        or f"{signal_timestamp}|{ticker}|{signal_family}|{quality_score}"
    )

    with _connect() as conn:
        existing = conn.execute(
            """
            SELECT shadow_id
              FROM jackal_shadow_signals
             WHERE external_key = ?
            """,
            (external_key,),
        ).fetchone()
        if existing:
            shadow_id = existing["shadow_id"]
        else:
            shadow_id = f"shadow_{uuid4().hex}"
            conn.execute(
                """
                INSERT INTO jackal_shadow_signals (
                    shadow_id, external_key, signal_timestamp, analysis_date,
                    ticker, market, signal_family, quality_label, quality_score,
                    status, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    shadow_id,
                    external_key,
                    signal_timestamp,
                    analysis_date,
                    ticker,
                    str(entry.get("market", "")),
                    signal_family,
                    str(entry.get("quality_label", "")),
                    float(quality_score) if quality_score is not None else None,
                    "open",
                    _json(entry) or "{}",
                    _now_iso(),
                ),
            )
    record_candidate(
        entry,
        source_system="jackal",
        source_event_type="shadow",
        source_external_key=external_key,
        source_event_id=shadow_id,
    )
    return shadow_id


def list_pending_jackal_shadow_signals(
    older_than: str,
    *,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    init_state_db()
    query = """
        SELECT shadow_id, external_key, signal_timestamp, analysis_date,
               ticker, market, signal_family, quality_label, quality_score,
               status, payload_json, outcome_json, created_at, resolved_at
          FROM jackal_shadow_signals
         WHERE status = 'open'
           AND signal_timestamp < ?
         ORDER BY signal_timestamp ASC
    """
    params: list[Any] = [older_than]
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        payload = {}
        outcome = {}
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except Exception:
            payload = {}
        try:
            outcome = json.loads(row["outcome_json"] or "{}")
        except Exception:
            outcome = {}
        item = dict(payload)
        item.update(outcome)
        item.update(
            {
                "shadow_id": row["shadow_id"],
                "external_key": row["external_key"],
                "timestamp": row["signal_timestamp"],
                "analysis_date": row["analysis_date"],
                "ticker": row["ticker"],
                "market": row["market"] or payload.get("market", ""),
                "signal_family": row["signal_family"] or payload.get("signal_family", ""),
                "quality_label": row["quality_label"] or payload.get("quality_label", ""),
                "quality_score": row["quality_score"],
                "status": row["status"],
                "created_at": row["created_at"],
                "resolved_at": row["resolved_at"],
            }
        )
        items.append(item)
    return items


def resolve_jackal_shadow_signal(
    shadow_id: str,
    outcome: dict[str, Any],
    *,
    payload_updates: dict[str, Any] | None = None,
) -> None:
    init_state_db()
    source_external_key: str | None = None
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT external_key, payload_json
              FROM jackal_shadow_signals
             WHERE shadow_id = ?
            """,
            (shadow_id,),
        ).fetchone()
        payload: dict[str, Any] = {}
        if row and row["payload_json"]:
            try:
                payload = json.loads(row["payload_json"])
            except Exception:
                payload = {}
        if row:
            source_external_key = row["external_key"]
        if payload_updates:
            payload.update(payload_updates)
        payload.update(outcome)

        conn.execute(
            """
            UPDATE jackal_shadow_signals
               SET status = 'resolved',
                   payload_json = ?,
                   outcome_json = ?,
                   resolved_at = ?
                WHERE shadow_id = ?
            """,
            (_json(payload) or "{}", _json(outcome) or "{}", _now_iso(), shadow_id),
        )
    if source_external_key:
        record_candidate(
            payload,
            source_system="jackal",
            source_event_type="shadow",
            source_external_key=source_external_key,
            source_event_id=shadow_id,
        )


def record_jackal_shadow_accuracy_batch(
    total: int,
    worked: int,
    *,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    init_state_db()
    rate = round(worked / total * 100, 1) if total > 0 else 0.0
    batch_id = f"shadow_batch_{uuid4().hex}"
    recorded_at = _now_iso()

    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO jackal_shadow_batches (
                batch_id, recorded_at, total, worked, rate, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (batch_id, recorded_at, total, worked, rate, _json(metadata)),
        )

        aggregate = conn.execute(
            """
            SELECT COALESCE(SUM(total), 0) AS total_sum,
                   COALESCE(SUM(worked), 0) AS worked_sum
              FROM jackal_shadow_batches
            """
        ).fetchone()
        history_rows = conn.execute(
            """
            SELECT recorded_at, total, worked, rate
              FROM jackal_shadow_batches
             ORDER BY recorded_at DESC
             LIMIT 90
            """
        ).fetchall()

    total_sum = int(aggregate["total_sum"]) if aggregate else 0
    worked_sum = int(aggregate["worked_sum"]) if aggregate else 0
    accuracy = round(worked_sum / total_sum * 100, 1) if total_sum > 0 else 0.0

    return {
        "description": "Jackal shadow performance is stored separately from ARIA production accuracy.",
        "correct": worked_sum,
        "total": total_sum,
        "accuracy": accuracy,
        "last_updated": recorded_at,
        "last_batch": {"total": total, "worked": worked, "rate": rate},
        "history": [
            {
                "timestamp": row["recorded_at"],
                "total": int(row["total"]),
                "worked": int(row["worked"]),
                "rate": float(row["rate"]),
            }
            for row in reversed(history_rows)
        ],
    }


def list_jackal_shadow_batches(limit: int = 90) -> list[dict[str, Any]]:
    init_state_db()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT batch_id, recorded_at, total, worked, rate, metadata_json
              FROM jackal_shadow_batches
             ORDER BY recorded_at DESC
             LIMIT ?
            """,
            (limit,),
        ).fetchall()

    result: list[dict[str, Any]] = []
    for row in rows:
        metadata: dict[str, Any] = {}
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except Exception:
            metadata = {}
        result.append(
            {
                "batch_id": row["batch_id"],
                "recorded_at": row["recorded_at"],
                "total": int(row["total"]),
                "worked": int(row["worked"]),
                "rate": float(row["rate"]),
                "metadata": metadata,
            }
        )
    return result


def _jackal_event_external_key(event_type: str, entry: dict[str, Any]) -> str:
    ts = str(entry.get("timestamp", ""))
    ticker = str(entry.get("ticker", ""))
    score = entry.get("final_score")
    if event_type == "hunt":
        return f"{event_type}|{ts}|{ticker}"
    return f"{event_type}|{ts}|{ticker}|{score}"


def sync_jackal_live_events(
    event_type: str,
    entries: list[dict[str, Any]],
) -> int:
    init_state_db()
    updated_at = _now_iso()
    synced = 0
    candidate_jobs: list[tuple[dict[str, Any], str, str]] = []
    with _connect() as conn:
        for entry in entries:
            ts = str(entry.get("timestamp", ""))
            ticker = str(entry.get("ticker", ""))
            if not ts or not ticker:
                continue
            analysis_date = ts.split("T", 1)[0]
            external_key = _jackal_event_external_key(event_type, entry)
            existing = conn.execute(
                """
                SELECT event_id
                  FROM jackal_live_events
                 WHERE external_key = ?
                """,
                (external_key,),
            ).fetchone()
            event_id = existing["event_id"] if existing else f"live_{uuid4().hex}"
            conn.execute(
                """
                INSERT INTO jackal_live_events (
                    event_id, event_type, external_key, ticker, event_timestamp,
                    analysis_date, alerted, is_entry, outcome_checked,
                    payload_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(external_key) DO UPDATE SET
                    ticker = excluded.ticker,
                    event_timestamp = excluded.event_timestamp,
                    analysis_date = excluded.analysis_date,
                    alerted = excluded.alerted,
                    is_entry = excluded.is_entry,
                    outcome_checked = excluded.outcome_checked,
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at
                """,
                (
                    event_id,
                    event_type,
                    external_key,
                    ticker,
                    ts,
                    analysis_date,
                    int(bool(entry.get("alerted"))),
                    int(bool(entry.get("is_entry"))),
                    int(bool(entry.get("outcome_checked"))),
                    _json(entry) or "{}",
                    updated_at,
                ),
            )
            candidate_jobs.append((deepcopy(entry), event_id, external_key))
            synced += 1
    for entry, event_id, external_key in candidate_jobs:
        record_candidate(
            entry,
            source_system="jackal",
            source_event_type=event_type,
            source_external_key=external_key,
            source_event_id=event_id,
        )
    return synced


def list_jackal_live_events(
    event_type: str,
    *,
    unresolved_only: bool = False,
    alerted_only: bool = False,
    limit: int = 500,
) -> list[dict[str, Any]]:
    init_state_db()
    query = """
        SELECT payload_json
          FROM jackal_live_events
         WHERE event_type = ?
    """
    params: list[Any] = [event_type]
    if unresolved_only:
        query += " AND outcome_checked = 0"
    if alerted_only:
        query += " AND (alerted = 1 OR is_entry = 1)"
    query += " ORDER BY event_timestamp DESC LIMIT ?"
    params.append(limit)

    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()

    result: list[dict[str, Any]] = []
    for row in rows:
        try:
            result.append(json.loads(row["payload_json"] or "{}"))
        except Exception:
            continue
    return result


def record_jackal_weight_snapshot(
    weights: dict[str, Any],
    *,
    source: str,
) -> str:
    init_state_db()
    snapshot_id = f"weights_{uuid4().hex}"
    captured_at = _now_iso()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO jackal_weight_snapshots (
                snapshot_id, source, captured_at, weights_json
            ) VALUES (?, ?, ?, ?)
            """,
            (snapshot_id, source, captured_at, _json(weights) or "{}"),
        )
    sync_jackal_accuracy_projection(
        snapshot_id,
        weights,
        source=source,
        captured_at=captured_at,
    )
    return snapshot_id


def load_latest_jackal_weight_snapshot() -> dict[str, Any] | None:
    init_state_db()
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT weights_json
              FROM jackal_weight_snapshots
             ORDER BY captured_at DESC
             LIMIT 1
            """
        ).fetchone()
    if not row or not row["weights_json"]:
        return None
    try:
        return json.loads(row["weights_json"])
    except Exception:
        return None


def load_jackal_cooldown_state() -> dict[str, Any]:
    init_state_db()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT ticker, signal_family, cooldown_at, quality_score,
                   last_override_at, override_reason, override_quality, override_count
              FROM jackal_cooldowns
             ORDER BY cooldown_at DESC
            """
        ).fetchall()

    state: dict[str, Any] = {}
    for row in rows:
        ticker = row["ticker"]
        signal_family = row["signal_family"]
        cooldown_at = row["cooldown_at"]
        if not ticker or not cooldown_at:
            continue
        if signal_family:
            family_key = f"{ticker}:{signal_family}"
            state[family_key] = cooldown_at
            if row["quality_score"] is not None:
                state[f"{family_key}:quality"] = float(row["quality_score"])
            if row["last_override_at"]:
                state[f"{family_key}:last_override"] = row["last_override_at"]
            if row["override_reason"]:
                state[f"{family_key}:override_reason"] = row["override_reason"]
            if row["override_quality"] is not None:
                state[f"{family_key}:override_quality"] = float(row["override_quality"])
            if row["override_count"]:
                state[f"{family_key}:override_count"] = int(row["override_count"])
        else:
            state[ticker] = cooldown_at
    return state


def sync_jackal_cooldown_state(state: dict[str, Any]) -> int:
    init_state_db()
    updated_at = _now_iso()
    legacy_rows: dict[str, str] = {}
    family_rows: dict[str, dict[str, Any]] = {}

    for key, value in (state or {}).items():
        if ":" not in key:
            if value:
                legacy_rows[str(key)] = str(value)
            continue

        base_key = key
        field = "cooldown_at"
        for suffix in (
            "quality",
            "last_override",
            "override_reason",
            "override_quality",
            "override_count",
        ):
            marker = f":{suffix}"
            if key.endswith(marker):
                base_key = key[: -len(marker)]
                field = suffix
                break

        if ":" not in base_key:
            continue
        ticker, signal_family = base_key.split(":", 1)
        record = family_rows.setdefault(
            base_key,
            {
                "ticker": ticker,
                "signal_family": signal_family,
                "cooldown_at": "",
                "quality_score": None,
                "last_override_at": None,
                "override_reason": None,
                "override_quality": None,
                "override_count": 0,
            },
        )

        if field == "cooldown_at":
            record["cooldown_at"] = str(value)
        elif field == "quality":
            try:
                record["quality_score"] = float(value)
            except Exception:
                pass
        elif field == "last_override":
            record["last_override_at"] = str(value)
        elif field == "override_reason":
            record["override_reason"] = str(value)
        elif field == "override_quality":
            try:
                record["override_quality"] = float(value)
            except Exception:
                pass
        elif field == "override_count":
            try:
                record["override_count"] = int(value)
            except Exception:
                pass

    rows_to_upsert: list[dict[str, Any]] = []
    target_keys: list[str] = []

    for ticker, cooldown_at in legacy_rows.items():
        payload = {"ticker": ticker, "cooldown_at": cooldown_at, "legacy": True}
        cooldown_key = f"{ticker}:__legacy__"
        rows_to_upsert.append(
            {
                "cooldown_key": cooldown_key,
                "ticker": ticker,
                "signal_family": None,
                "cooldown_at": cooldown_at,
                "quality_score": None,
                "last_override_at": None,
                "override_reason": None,
                "override_quality": None,
                "override_count": 0,
                "payload_json": _json(payload) or "{}",
            }
        )
        target_keys.append(cooldown_key)

    for family_key, record in family_rows.items():
        if not record["cooldown_at"]:
            continue
        payload = {
            "ticker": record["ticker"],
            "signal_family": record["signal_family"],
            "cooldown_at": record["cooldown_at"],
            "quality_score": record["quality_score"],
            "last_override_at": record["last_override_at"],
            "override_reason": record["override_reason"],
            "override_quality": record["override_quality"],
            "override_count": record["override_count"],
        }
        rows_to_upsert.append(
            {
                "cooldown_key": family_key,
                "ticker": record["ticker"],
                "signal_family": record["signal_family"],
                "cooldown_at": record["cooldown_at"],
                "quality_score": record["quality_score"],
                "last_override_at": record["last_override_at"],
                "override_reason": record["override_reason"],
                "override_quality": record["override_quality"],
                "override_count": record["override_count"],
                "payload_json": _json(payload) or "{}",
            }
        )
        target_keys.append(family_key)

    with _connect() as conn:
        if target_keys:
            placeholders = ",".join("?" for _ in target_keys)
            conn.execute(
                f"DELETE FROM jackal_cooldowns WHERE cooldown_key NOT IN ({placeholders})",
                target_keys,
            )
        else:
            conn.execute("DELETE FROM jackal_cooldowns")

        for row in rows_to_upsert:
            conn.execute(
                """
                INSERT INTO jackal_cooldowns (
                    cooldown_key, ticker, signal_family, cooldown_at,
                    quality_score, last_override_at, override_reason,
                    override_quality, override_count, payload_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cooldown_key) DO UPDATE SET
                    ticker = excluded.ticker,
                    signal_family = excluded.signal_family,
                    cooldown_at = excluded.cooldown_at,
                    quality_score = excluded.quality_score,
                    last_override_at = excluded.last_override_at,
                    override_reason = excluded.override_reason,
                    override_quality = excluded.override_quality,
                    override_count = excluded.override_count,
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at
                """,
                (
                    row["cooldown_key"],
                    row["ticker"],
                    row["signal_family"],
                    row["cooldown_at"],
                    row["quality_score"],
                    row["last_override_at"],
                    row["override_reason"],
                    row["override_quality"],
                    row["override_count"],
                    row["payload_json"],
                    updated_at,
                ),
            )
    return len(rows_to_upsert)


def sync_jackal_recommendations(entries: list[dict[str, Any]]) -> int:
    init_state_db()
    updated_at = _now_iso()
    synced = 0
    with _connect() as conn:
        for entry in entries:
            recommended_at = str(entry.get("recommended_at") or entry.get("timestamp") or "")
            ticker = str(entry.get("ticker", ""))
            if not recommended_at or not ticker:
                continue
            analysis_date = recommended_at.split("T", 1)[0]
            external_key = f"recommend|{recommended_at}|{ticker}"
            existing = conn.execute(
                """
                SELECT recommendation_id
                  FROM jackal_recommendations
                 WHERE external_key = ?
                """,
                (external_key,),
            ).fetchone()
            recommendation_id = (
                existing["recommendation_id"] if existing else f"recommend_{uuid4().hex}"
            )
            outcome_correct = entry.get("outcome_correct")
            conn.execute(
                """
                INSERT INTO jackal_recommendations (
                    recommendation_id, external_key, ticker, market,
                    recommended_at, analysis_date, outcome_checked,
                    outcome_pct, outcome_correct, payload_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(external_key) DO UPDATE SET
                    ticker = excluded.ticker,
                    market = excluded.market,
                    recommended_at = excluded.recommended_at,
                    analysis_date = excluded.analysis_date,
                    outcome_checked = excluded.outcome_checked,
                    outcome_pct = excluded.outcome_pct,
                    outcome_correct = excluded.outcome_correct,
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at
                """,
                (
                    recommendation_id,
                    external_key,
                    ticker,
                    entry.get("market"),
                    recommended_at,
                    analysis_date,
                    int(bool(entry.get("outcome_checked"))),
                    entry.get("outcome_pct"),
                    None if outcome_correct is None else int(bool(outcome_correct)),
                    _json(entry) or "{}",
                    updated_at,
                ),
            )
            synced += 1
    return synced


def list_jackal_recommendations(
    *,
    unresolved_only: bool = False,
    limit: int = 200,
) -> list[dict[str, Any]]:
    init_state_db()
    query = """
        SELECT payload_json
          FROM jackal_recommendations
    """
    params: list[Any] = []
    if unresolved_only:
        query += " WHERE outcome_checked = 0"
    query += " ORDER BY recommended_at DESC LIMIT ?"
    params.append(limit)

    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()

    results: list[dict[str, Any]] = []
    for row in rows:
        try:
            results.append(json.loads(row["payload_json"] or "{}"))
        except Exception:
            continue
    return results


def _metric_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _append_accuracy_row(
    rows: list[dict[str, Any]],
    *,
    snapshot_id: str,
    source: str,
    captured_at: str,
    family: str,
    scope: str,
    entity_key: str,
    total: Any,
    correct: Any,
    accuracy: Any,
    metrics: dict[str, Any],
) -> None:
    if not entity_key:
        return
    rows.append(
        {
            "projection_id": f"acc_{uuid4().hex}",
            "snapshot_id": snapshot_id,
            "source": source,
            "captured_at": captured_at,
            "family": family,
            "scope": scope,
            "entity_key": entity_key,
            "correct": _metric_number(correct),
            "total": _metric_number(total),
            "accuracy": _metric_number(accuracy),
            "metrics_json": _json(metrics) or "{}",
            "updated_at": _now_iso(),
        }
    )


def _build_jackal_accuracy_projection_rows(
    snapshot_id: str,
    weights: dict[str, Any],
    *,
    source: str,
    captured_at: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    signal_accuracy = weights.get("signal_accuracy", {})
    if isinstance(signal_accuracy, dict):
        for signal_name, metrics in signal_accuracy.items():
            if not isinstance(metrics, dict):
                continue
            payload = deepcopy(metrics)
            if "accuracy" in metrics or "correct" in metrics:
                _append_accuracy_row(
                    rows,
                    snapshot_id=snapshot_id,
                    source=source,
                    captured_at=captured_at,
                    family="signal",
                    scope="overall",
                    entity_key=str(signal_name),
                    total=metrics.get("total"),
                    correct=metrics.get("correct"),
                    accuracy=metrics.get("accuracy"),
                    metrics=payload,
                )
            if "swing_accuracy" in metrics or "swing_correct" in metrics:
                _append_accuracy_row(
                    rows,
                    snapshot_id=snapshot_id,
                    source=source,
                    captured_at=captured_at,
                    family="signal",
                    scope="swing",
                    entity_key=str(signal_name),
                    total=metrics.get("total"),
                    correct=metrics.get("swing_correct"),
                    accuracy=metrics.get("swing_accuracy"),
                    metrics=payload,
                )
            if "d1_accuracy" in metrics or "d1_correct" in metrics:
                _append_accuracy_row(
                    rows,
                    snapshot_id=snapshot_id,
                    source=source,
                    captured_at=captured_at,
                    family="signal",
                    scope="d1",
                    entity_key=str(signal_name),
                    total=metrics.get("total") or metrics.get("d1_total"),
                    correct=metrics.get("d1_correct"),
                    accuracy=metrics.get("d1_accuracy"),
                    metrics=payload,
                )

    for family_name in ("regime_accuracy", "ticker_accuracy", "devil_accuracy"):
        family_data = weights.get(family_name, {})
        if not isinstance(family_data, dict):
            continue
        family = family_name.replace("_accuracy", "")
        for entity_key, metrics in family_data.items():
            if not isinstance(metrics, dict):
                continue
            _append_accuracy_row(
                rows,
                snapshot_id=snapshot_id,
                source=source,
                captured_at=captured_at,
                family=family,
                scope="overall",
                entity_key=str(entity_key),
                total=metrics.get("total"),
                correct=metrics.get("correct"),
                accuracy=metrics.get("accuracy"),
                metrics=deepcopy(metrics),
            )

    recommendation_accuracy = weights.get("recommendation_accuracy", {})
    if isinstance(recommendation_accuracy, dict):
        for scope_name, scope_data in recommendation_accuracy.items():
            if not isinstance(scope_data, dict):
                continue
            scope = scope_name.replace("by_", "")
            for entity_key, metrics in scope_data.items():
                if not isinstance(metrics, dict):
                    continue
                _append_accuracy_row(
                    rows,
                    snapshot_id=snapshot_id,
                    source=source,
                    captured_at=captured_at,
                    family="recommendation",
                    scope=scope,
                    entity_key=str(entity_key),
                    total=metrics.get("total"),
                    correct=metrics.get("correct"),
                    accuracy=metrics.get("accuracy"),
                    metrics=deepcopy(metrics),
                )

    return rows


def sync_jackal_accuracy_projection(
    snapshot_id: str,
    weights: dict[str, Any],
    *,
    source: str,
    captured_at: str,
) -> int:
    init_state_db()
    rows = _build_jackal_accuracy_projection_rows(
        snapshot_id,
        weights,
        source=source,
        captured_at=captured_at,
    )
    with _connect() as conn:
        conn.execute(
            "DELETE FROM jackal_accuracy_projection WHERE snapshot_id = ?",
            (snapshot_id,),
        )
        for row in rows:
            conn.execute(
                """
                INSERT INTO jackal_accuracy_projection (
                    projection_id, snapshot_id, source, captured_at,
                    family, scope, entity_key, correct, total,
                    accuracy, metrics_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(snapshot_id, family, scope, entity_key) DO UPDATE SET
                    source = excluded.source,
                    captured_at = excluded.captured_at,
                    correct = excluded.correct,
                    total = excluded.total,
                    accuracy = excluded.accuracy,
                    metrics_json = excluded.metrics_json,
                    updated_at = excluded.updated_at
                """,
                (
                    row["projection_id"],
                    row["snapshot_id"],
                    row["source"],
                    row["captured_at"],
                    row["family"],
                    row["scope"],
                    row["entity_key"],
                    row["correct"],
                    row["total"],
                    row["accuracy"],
                    row["metrics_json"],
                    row["updated_at"],
                ),
            )
    return len(rows)


def list_jackal_accuracy_projection(
    *,
    family: str | None = None,
    scope: str | None = None,
    current_only: bool = True,
    limit: int = 500,
) -> list[dict[str, Any]]:
    init_state_db()
    table_name = "jackal_accuracy_current" if current_only else "jackal_accuracy_projection"
    query = f"""
        SELECT snapshot_id, source, captured_at, family, scope, entity_key,
               correct, total, accuracy, metrics_json
          FROM {table_name}
         WHERE 1 = 1
    """
    params: list[Any] = []
    if family:
        query += " AND family = ?"
        params.append(family)
    if scope:
        query += " AND scope = ?"
        params.append(scope)
    query += " ORDER BY captured_at DESC, family, scope, entity_key LIMIT ?"
    params.append(limit)

    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()

    results: list[dict[str, Any]] = []
    for row in rows:
        metrics: dict[str, Any] = {}
        try:
            metrics = json.loads(row["metrics_json"] or "{}")
        except Exception:
            metrics = {}
        results.append(
            {
                "snapshot_id": row["snapshot_id"],
                "source": row["source"],
                "captured_at": row["captured_at"],
                "family": row["family"],
                "scope": row["scope"],
                "entity_key": row["entity_key"],
                "correct": row["correct"],
                "total": row["total"],
                "accuracy": row["accuracy"],
                "metrics": metrics,
            }
        )
    return results


def rebuild_latest_jackal_accuracy_projection() -> int:
    init_state_db()
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT snapshot_id, source, captured_at, weights_json
              FROM jackal_weight_snapshots
             ORDER BY captured_at DESC
             LIMIT 1
            """
        ).fetchone()
    if not row or not row["weights_json"]:
        return 0
    try:
        weights = json.loads(row["weights_json"])
    except Exception:
        return 0
    return sync_jackal_accuracy_projection(
        row["snapshot_id"],
        weights,
        source=row["source"],
        captured_at=row["captured_at"],
    )


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def _to_int_flag(value: Any) -> int | None:
    if value is None:
        return None
    return int(bool(value))


def _candidate_external_key(
    entry: dict[str, Any],
    *,
    source_system: str,
    source_event_type: str,
    source_external_key: str | None = None,
) -> str:
    existing = entry.get("candidate_external_key")
    if existing:
        return str(existing)
    if source_external_key:
        return f"candidate|{source_system}|{source_event_type}|{source_external_key}"
    timestamp = str(entry.get("timestamp") or entry.get("detected_at") or _now_iso())
    ticker = str(entry.get("ticker") or "").strip()
    signal_family = str(entry.get("signal_family") or "").strip()
    return f"candidate|{source_system}|{source_event_type}|{timestamp}|{ticker}|{signal_family}"


def _candidate_status(entry: dict[str, Any]) -> str:
    if (
        entry.get("outcome_checked")
        or entry.get("shadow_evaluated_at")
        or entry.get("shadow_swing_ok") is not None
    ):
        return "resolved"
    if entry.get("alerted") or entry.get("is_entry"):
        return "tracking"
    return "open"


def _candidate_quality_score(entry: dict[str, Any]) -> float | None:
    for key in ("quality_score", "final_score", "analyst_score"):
        score = _to_float(entry.get(key))
        if score is not None:
            return score
    return None


def _candidate_signal_family(entry: dict[str, Any]) -> str | None:
    signals = entry.get("signals_fired")
    if not isinstance(signals, list):
        signals = []
    raw_signal_family = str(entry.get("signal_family", "")).strip() or None
    swing_type = str(entry.get("swing_type", "")).strip() or None
    if raw_signal_family or swing_type or signals:
        return canonical_family_key(
            signal_family=raw_signal_family,
            swing_type=swing_type,
            signals_fired=signals,
        )
    return None


def _candidate_raw_signal_family(entry: dict[str, Any]) -> str | None:
    for key in ("signal_family_raw", "signal_family", "swing_type"):
        value = str(entry.get(key, "")).strip()
        if value:
            return value
    return None


def _candidate_signals_fired(entry: dict[str, Any]) -> list[str]:
    for key in ("signals_fired", "signals_fired_pre"):
        value = entry.get(key)
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
    return []


def _candidate_reference_price(entry: dict[str, Any]) -> float | None:
    for key in ("price_at_hunt", "price_at_scan", "price_at_rec"):
        price = _to_float(entry.get(key))
        if price is not None:
            return price
    return None


def _upsert_candidate_outcome(
    conn: sqlite3.Connection,
    *,
    candidate_id: str,
    horizon_label: str,
    status: str,
    observed_at: str,
    price_reference: float | None,
    price_outcome: float | None,
    return_pct: float | None,
    hit: Any,
    payload: dict[str, Any],
) -> str:
    existing = conn.execute(
        """
        SELECT outcome_id
          FROM candidate_outcomes
         WHERE candidate_id = ? AND horizon_label = ?
        """,
        (candidate_id, horizon_label),
    ).fetchone()
    outcome_id = existing["outcome_id"] if existing else f"candout_{uuid4().hex}"
    conn.execute(
        """
        INSERT INTO candidate_outcomes (
            outcome_id, candidate_id, horizon_label, status, observed_at,
            price_reference, price_outcome, return_pct, hit, outcome_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(candidate_id, horizon_label) DO UPDATE SET
            status = excluded.status,
            observed_at = excluded.observed_at,
            price_reference = excluded.price_reference,
            price_outcome = excluded.price_outcome,
            return_pct = excluded.return_pct,
            hit = excluded.hit,
            outcome_json = excluded.outcome_json
        """,
        (
            outcome_id,
            candidate_id,
            horizon_label,
            status,
            observed_at,
            price_reference,
            price_outcome,
            return_pct,
            _to_int_flag(hit),
            _json(payload) or "{}",
        ),
    )
    return outcome_id


def _sync_candidate_outcomes(
    conn: sqlite3.Connection,
    candidate_id: str,
    entry: dict[str, Any],
) -> dict[str, Any] | None:
    observed_at = str(
        entry.get("outcome_tracked_at")
        or entry.get("shadow_evaluated_at")
        or _now_iso()
    )
    price_reference = _candidate_reference_price(entry)
    latest: dict[str, Any] | None = None

    price_1d = _to_float(entry.get("price_1d_later"))
    pct_1d = _to_float(entry.get("outcome_1d_pct"))
    hit_1d = entry.get("outcome_1d_hit")
    if price_1d is not None or pct_1d is not None or hit_1d is not None:
        payload = {
            "price_1d_later": price_1d,
            "outcome_1d_pct": pct_1d,
            "outcome_1d_hit": hit_1d,
        }
        _upsert_candidate_outcome(
            conn,
            candidate_id=candidate_id,
            horizon_label="d1",
            status="observed",
            observed_at=observed_at,
            price_reference=price_reference,
            price_outcome=price_1d,
            return_pct=pct_1d,
            hit=hit_1d,
            payload=payload,
        )
        latest = {
            "horizon": "d1",
            "status": "observed",
            "observed_at": observed_at,
            "return_pct": pct_1d,
            "hit": bool(hit_1d) if hit_1d is not None else None,
        }

    peak_price = _to_float(entry.get("price_peak"))
    peak_pct = _to_float(entry.get("peak_pct"))
    swing_hit = entry.get("outcome_swing_hit")
    swing_payload: dict[str, Any] | None = None
    if peak_price is not None or peak_pct is not None or swing_hit is not None:
        swing_payload = {
            "price_peak": peak_price,
            "peak_day": entry.get("peak_day"),
            "peak_pct": peak_pct,
            "outcome_swing_hit": swing_hit,
        }
    elif entry.get("shadow_swing_pct") is not None or entry.get("shadow_swing_ok") is not None:
        peak_pct = _to_float(entry.get("shadow_swing_pct"))
        swing_hit = entry.get("shadow_swing_ok")
        swing_payload = {
            "shadow_swing_pct": peak_pct,
            "shadow_swing_ok": swing_hit,
        }
    if swing_payload:
        _upsert_candidate_outcome(
            conn,
            candidate_id=candidate_id,
            horizon_label="swing",
            status="confirmed",
            observed_at=observed_at,
            price_reference=price_reference,
            price_outcome=peak_price,
            return_pct=peak_pct,
            hit=swing_hit,
            payload=swing_payload,
        )
        latest = {
            "horizon": "swing",
            "status": "confirmed",
            "observed_at": observed_at,
            "return_pct": peak_pct,
            "hit": bool(swing_hit) if swing_hit is not None else None,
        }

    price_5d = _to_float(entry.get("price_5d_later"))
    pct_5d = _to_float(entry.get("outcome_pct"))
    hit_5d = entry.get("outcome_correct")
    if price_5d is not None:
        payload = {
            "price_5d_later": price_5d,
            "outcome_pct": pct_5d,
            "outcome_correct": hit_5d,
        }
        _upsert_candidate_outcome(
            conn,
            candidate_id=candidate_id,
            horizon_label="d5",
            status="confirmed" if entry.get("outcome_checked") else "observed",
            observed_at=observed_at,
            price_reference=price_reference,
            price_outcome=price_5d,
            return_pct=pct_5d,
            hit=hit_5d,
            payload=payload,
        )
        latest = {
            "horizon": "d5",
            "status": "confirmed" if entry.get("outcome_checked") else "observed",
            "observed_at": observed_at,
            "return_pct": pct_5d,
            "hit": bool(hit_5d) if hit_5d is not None else None,
        }

    return latest


def _latest_candidate_review(
    conn: sqlite3.Connection,
    candidate_id: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT review_id, analysis_date, reviewed_at, alignment, review_verdict,
               orca_regime, orca_trend, confidence, thesis_killer, review_json
          FROM candidate_reviews
         WHERE candidate_id = ?
         ORDER BY reviewed_at DESC
         LIMIT 1
        """,
        (candidate_id,),
    ).fetchone()
    if not row:
        return None
    payload: dict[str, Any] = {}
    try:
        payload = json.loads(row["review_json"] or "{}")
    except Exception:
        payload = {}
    return {
        "review_id": row["review_id"],
        "analysis_date": row["analysis_date"],
        "reviewed_at": row["reviewed_at"],
        "alignment": row["alignment"] or "",
        "review_verdict": row["review_verdict"] or "",
        "orca_regime": row["orca_regime"] or "",
        "orca_trend": row["orca_trend"] or "",
        "confidence": row["confidence"] or "",
        "thesis_killer": row["thesis_killer"] or "",
        "payload": payload,
    }


def _latest_candidate_outcome(
    conn: sqlite3.Connection,
    candidate_id: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT outcome_id, horizon_label, status, observed_at, price_reference,
               price_outcome, return_pct, hit, outcome_json
          FROM candidate_outcomes
         WHERE candidate_id = ?
         ORDER BY
             CASE horizon_label
                 WHEN 'd5' THEN 3
                 WHEN 'swing' THEN 2
                 WHEN 'd1' THEN 1
                 ELSE 0
             END DESC,
             observed_at DESC
         LIMIT 1
        """,
        (candidate_id,),
    ).fetchone()
    if not row:
        return None
    payload: dict[str, Any] = {}
    try:
        payload = json.loads(row["outcome_json"] or "{}")
    except Exception:
        payload = {}
    return {
        "outcome_id": row["outcome_id"],
        "horizon_label": row["horizon_label"],
        "status": row["status"],
        "observed_at": row["observed_at"],
        "price_reference": row["price_reference"],
        "price_outcome": row["price_outcome"],
        "return_pct": row["return_pct"],
        "hit": None if row["hit"] is None else bool(row["hit"]),
        "payload": payload,
    }


def _sync_candidate_probability_lesson(
    conn: sqlite3.Connection,
    candidate_id: str,
) -> str | None:
    review = _latest_candidate_review(conn, candidate_id)
    outcome = _latest_candidate_outcome(conn, candidate_id)
    if not review or not outcome:
        return None
    if outcome["status"] not in {"confirmed", "observed"}:
        return None
    if outcome["hit"] is None:
        return None

    alignment = review.get("alignment") or "unreviewed"
    hit = bool(outcome["hit"])
    lesson_type = f"{alignment}_{'win' if hit else 'loss'}"
    label = f"{alignment} {'win' if hit else 'loss'}"
    lesson_payload = {
        "candidate_id": candidate_id,
        "alignment": alignment,
        "review_verdict": review.get("review_verdict", ""),
        "outcome_horizon": outcome.get("horizon_label", ""),
        "return_pct": outcome.get("return_pct"),
        "hit": hit,
        "orca_regime": review.get("orca_regime", ""),
        "orca_trend": review.get("orca_trend", ""),
        "thesis_killer": review.get("thesis_killer", ""),
    }

    conn.execute(
        """
        DELETE FROM candidate_lessons
         WHERE candidate_id = ?
           AND (
                lesson_type LIKE 'aligned_%'
             OR lesson_type LIKE 'neutral_%'
             OR lesson_type LIKE 'opposed_%'
             OR lesson_type LIKE 'unreviewed_%'
           )
        """,
        (candidate_id,),
    )

    lesson_id = f"lesson_{uuid4().hex}"
    conn.execute(
        """
        INSERT INTO candidate_lessons (
            lesson_id, candidate_id, outcome_id, lesson_type,
            label, lesson_value, lesson_timestamp, lesson_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            lesson_id,
            candidate_id,
            outcome["outcome_id"],
            lesson_type,
            label,
            float(outcome.get("return_pct") or 0.0),
            _now_iso(),
            _json(lesson_payload) or "{}",
        ),
    )
    return lesson_id


def record_candidate(
    entry: dict[str, Any],
    *,
    source_system: str,
    source_event_type: str,
    source_event_id: str | None = None,
    source_external_key: str | None = None,
    source_run_id: str | None = None,
    source_session_id: str | None = None,
) -> str:
    init_state_db()
    detected_at = str(entry.get("timestamp") or entry.get("detected_at") or _now_iso())
    analysis_date = str(entry.get("analysis_date") or detected_at.split("T", 1)[0])
    external_key = _candidate_external_key(
        entry,
        source_system=source_system,
        source_event_type=source_event_type,
        source_external_key=source_external_key,
    )
    quality_score = _candidate_quality_score(entry)
    signal_family = _candidate_signal_family(entry)
    raw_signal_family = _candidate_raw_signal_family(entry)
    payload = deepcopy(entry)
    if raw_signal_family:
        payload["signal_family_raw"] = raw_signal_family
    if signal_family:
        payload["signal_family"] = signal_family
        payload["probability_family"] = signal_family
        payload["signal_family_label"] = family_label(signal_family)
    status = _candidate_status(payload)

    with _connect() as conn:
        existing = conn.execute(
            """
            SELECT candidate_id
              FROM candidate_registry
             WHERE external_key = ?
            """,
            (external_key,),
        ).fetchone()
        candidate_id = existing["candidate_id"] if existing else f"cand_{uuid4().hex}"
        conn.execute(
            """
            INSERT INTO candidate_registry (
                candidate_id, external_key, source_system, source_event_type,
                source_event_id, source_run_id, source_session_id,
                ticker, name, market, detected_at, analysis_date,
                signal_family, quality_label, quality_score, orca_alignment,
                status, payload_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(external_key) DO UPDATE SET
                source_system = excluded.source_system,
                source_event_type = excluded.source_event_type,
                source_event_id = COALESCE(excluded.source_event_id, candidate_registry.source_event_id),
                source_run_id = COALESCE(excluded.source_run_id, candidate_registry.source_run_id),
                source_session_id = COALESCE(excluded.source_session_id, candidate_registry.source_session_id),
                ticker = excluded.ticker,
                name = COALESCE(excluded.name, candidate_registry.name),
                market = COALESCE(excluded.market, candidate_registry.market),
                detected_at = excluded.detected_at,
                analysis_date = excluded.analysis_date,
                signal_family = COALESCE(excluded.signal_family, candidate_registry.signal_family),
                quality_label = COALESCE(excluded.quality_label, candidate_registry.quality_label),
                quality_score = COALESCE(excluded.quality_score, candidate_registry.quality_score),
                orca_alignment = COALESCE(excluded.orca_alignment, candidate_registry.orca_alignment),
                status = excluded.status,
                payload_json = excluded.payload_json,
                updated_at = excluded.updated_at
            """,
            (
                candidate_id,
                external_key,
                source_system,
                source_event_type,
                source_event_id,
                source_run_id,
                source_session_id,
                str(payload.get("ticker", "")),
                str(payload.get("name", "")).strip() or None,
                str(payload.get("market", "")).strip() or None,
                detected_at,
                analysis_date,
                signal_family,
                str(payload.get("quality_label", "")).strip() or None,
                quality_score,
                str(payload.get("orca_alignment", "")).strip() or None,
                status,
                _json(payload) or "{}",
                _now_iso(),
                _now_iso(),
            ),
        )
        latest_outcome = _sync_candidate_outcomes(conn, candidate_id, payload)
        if latest_outcome:
            resolved_status = "resolved" if latest_outcome["status"] == "confirmed" else "tracking"
            conn.execute(
                """
                UPDATE candidate_registry
                   SET status = ?,
                       latest_outcome_horizon = ?,
                       latest_outcome_at = ?,
                       latest_outcome_json = ?,
                       updated_at = ?
                 WHERE candidate_id = ?
                """,
                (
                    resolved_status,
                    latest_outcome["horizon"],
                    latest_outcome["observed_at"],
                    _json(latest_outcome) or "{}",
                    _now_iso(),
                    candidate_id,
                ),
            )
        _sync_candidate_probability_lesson(conn, candidate_id)
    return candidate_id


def list_candidates(
    *,
    source_system: str | None = None,
    source_event_type: str | None = None,
    status: str | None = None,
    unresolved_only: bool = False,
    limit: int = 200,
) -> list[dict[str, Any]]:
    init_state_db()
    query = """
        SELECT candidate_id, external_key, source_system, source_event_type,
               source_event_id, source_run_id, source_session_id, ticker, name,
               market, detected_at, analysis_date, signal_family, quality_label,
               quality_score, orca_alignment, status, payload_json,
               latest_outcome_horizon, latest_outcome_at, latest_outcome_json
          FROM candidate_registry
         WHERE 1 = 1
    """
    params: list[Any] = []
    if source_system:
        query += " AND source_system = ?"
        params.append(source_system)
    if source_event_type:
        query += " AND source_event_type = ?"
        params.append(source_event_type)
    if unresolved_only:
        query += " AND status != 'resolved'"
    elif status:
        query += " AND status = ?"
        params.append(status)
    query += " ORDER BY detected_at DESC LIMIT ?"
    params.append(limit)

    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()

    results: list[dict[str, Any]] = []
    for row in rows:
        payload: dict[str, Any] = {}
        latest_outcome: dict[str, Any] = {}
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except Exception:
            payload = {}
        try:
            latest_outcome = json.loads(row["latest_outcome_json"] or "{}")
        except Exception:
            latest_outcome = {}
        signals_fired = _candidate_signals_fired(payload)
        signal_family = canonical_family_key(
            signal_family=str(row["signal_family"] or payload.get("signal_family") or "").strip() or None,
            swing_type=str(payload.get("signal_family_raw") or payload.get("swing_type") or "").strip() or None,
            signals_fired=signals_fired,
        )
        signal_family_raw = (
            str(payload.get("signal_family_raw") or payload.get("swing_type") or row["signal_family"] or "").strip()
            or None
        )
        results.append(
            {
                "candidate_id": row["candidate_id"],
                "external_key": row["external_key"],
                "source_system": row["source_system"],
                "source_event_type": row["source_event_type"],
                "source_event_id": row["source_event_id"],
                "source_run_id": row["source_run_id"],
                "source_session_id": row["source_session_id"],
                "ticker": row["ticker"],
                "name": row["name"] or payload.get("name", ""),
                "market": row["market"] or payload.get("market", ""),
                "detected_at": row["detected_at"],
                "analysis_date": row["analysis_date"],
                "signal_family": signal_family,
                "signal_family_raw": signal_family_raw,
                "signal_family_label": payload.get("signal_family_label") or family_label(signal_family),
                "quality_label": row["quality_label"] or payload.get("quality_label", ""),
                "quality_score": row["quality_score"],
                "orca_alignment": row["orca_alignment"],
                "status": row["status"],
                "latest_outcome_horizon": row["latest_outcome_horizon"],
                "latest_outcome_at": row["latest_outcome_at"],
                "latest_outcome": latest_outcome,
                "payload": payload,
            }
        )
    return results


def list_candidate_outcomes(
    candidate_id: str,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    init_state_db()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT outcome_id, horizon_label, status, observed_at,
                   price_reference, price_outcome, return_pct, hit, outcome_json
              FROM candidate_outcomes
             WHERE candidate_id = ?
             ORDER BY observed_at DESC
             LIMIT ?
            """,
            (candidate_id, limit),
        ).fetchall()

    results: list[dict[str, Any]] = []
    for row in rows:
        payload: dict[str, Any] = {}
        try:
            payload = json.loads(row["outcome_json"] or "{}")
        except Exception:
            payload = {}
        results.append(
            {
                "outcome_id": row["outcome_id"],
                "horizon_label": row["horizon_label"],
                "status": row["status"],
                "observed_at": row["observed_at"],
                "price_reference": row["price_reference"],
                "price_outcome": row["price_outcome"],
                "return_pct": row["return_pct"],
                "hit": None if row["hit"] is None else bool(row["hit"]),
                "payload": payload,
            }
        )
    return results


def list_candidate_reviews(
    candidate_id: str,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    init_state_db()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT review_id, run_id, analysis_date, reviewed_at, alignment,
                   review_verdict, orca_regime, orca_trend, confidence,
                   thesis_killer, review_json
              FROM candidate_reviews
             WHERE candidate_id = ?
             ORDER BY reviewed_at DESC
             LIMIT ?
            """,
            (candidate_id, limit),
        ).fetchall()

    results: list[dict[str, Any]] = []
    for row in rows:
        payload: dict[str, Any] = {}
        try:
            payload = json.loads(row["review_json"] or "{}")
        except Exception:
            payload = {}
        results.append(
            {
                "review_id": row["review_id"],
                "run_id": row["run_id"],
                "analysis_date": row["analysis_date"],
                "reviewed_at": row["reviewed_at"],
                "alignment": row["alignment"],
                "review_verdict": row["review_verdict"],
                "orca_regime": row["orca_regime"],
                "orca_trend": row["orca_trend"],
                "confidence": row["confidence"],
                "thesis_killer": row["thesis_killer"],
                "payload": payload,
            }
        )
    return results


def summarize_candidate_probabilities(
    *,
    days: int = 90,
    min_samples: int = 5,
) -> dict[str, Any]:
    init_state_db()
    cutoff = (_now() - timedelta(days=days)).isoformat()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT c.candidate_id,
                   c.ticker,
                   c.analysis_date,
                   c.signal_family,
                   c.source_event_type,
                   l.lesson_type,
                   l.lesson_value,
                   l.lesson_json,
                   l.lesson_timestamp
              FROM candidate_lessons l
              JOIN candidate_registry c
                ON c.candidate_id = l.candidate_id
             WHERE l.lesson_timestamp >= ?
             ORDER BY l.lesson_timestamp DESC
            """,
            (cutoff,),
        ).fetchall()

    overall = {"wins": 0, "losses": 0, "total": 0}
    by_signal_family: dict[str, dict[str, Any]] = {}
    by_alignment: dict[str, dict[str, Any]] = {}
    by_family_alignment: dict[str, dict[str, dict[str, Any]]] = {}
    seen_sample_keys: set[tuple[str, str, str, str]] = set()
    raw_rows = len(rows)
    for row in rows:
        lesson_type = str(row["lesson_type"] or "")
        signal_family = canonical_family_key(signal_family=str(row["signal_family"] or "general_rebound"))
        alignment = lesson_type.rsplit("_", 1)[0] if "_" in lesson_type else lesson_type
        sample_date = str(row["analysis_date"] or row["lesson_timestamp"] or "")[:10]
        sample_ticker = str(row["ticker"] or row["candidate_id"] or "").strip().upper()
        sample_key = (sample_date, sample_ticker, signal_family, alignment)
        if sample_key in seen_sample_keys:
            continue
        seen_sample_keys.add(sample_key)
        is_win = lesson_type.endswith("_win")
        stats = by_signal_family.setdefault(
            signal_family,
            {"wins": 0, "losses": 0, "total": 0, "avg_return": 0.0, "source_event_type": row["source_event_type"] or ""},
        )
        align_stats = by_alignment.setdefault(alignment, {"wins": 0, "losses": 0, "total": 0, "avg_return": 0.0})
        family_alignment_stats = by_family_alignment.setdefault(alignment, {}).setdefault(
            signal_family,
            {"wins": 0, "losses": 0, "total": 0, "avg_return": 0.0, "source_event_type": row["source_event_type"] or ""},
        )
        lesson_value = float(row["lesson_value"] or 0.0)

        for bucket in (overall, stats, align_stats, family_alignment_stats):
            bucket["total"] += 1
            bucket["wins"] += int(is_win)
            bucket["losses"] += int(not is_win)
            bucket["avg_return"] = round(
                ((bucket.get("avg_return", 0.0) * (bucket["total"] - 1)) + lesson_value) / bucket["total"],
                3,
            )

    def _finalize(bucket: dict[str, Any]) -> dict[str, Any]:
        total = int(bucket.get("total", 0))
        wins = int(bucket.get("wins", 0))
        losses = int(bucket.get("losses", 0))
        win_rate = round(wins / total * 100, 1) if total > 0 else 0.0
        prior_total = 4
        prior_wins = 2
        effective_win_rate = round(((wins + prior_wins) / (total + prior_total)) * 100, 1) if total > 0 else 0.0
        return {
            **bucket,
            "wins": wins,
            "losses": losses,
            "total": total,
            "win_rate": win_rate,
            "effective_win_rate": effective_win_rate,
            "qualified": total >= min_samples,
        }

    signal_summary = {key: _finalize(val) for key, val in by_signal_family.items()}
    alignment_summary = {key: _finalize(val) for key, val in by_alignment.items()}
    family_alignment_summary = {
        alignment_key: {
            family_key: {
                "signal_family": family_key,
                "signal_family_label": family_label(family_key),
                **_finalize(family_stats),
            }
            for family_key, family_stats in family_map.items()
        }
        for alignment_key, family_map in by_family_alignment.items()
    }

    def _rank_family_map(family_map: dict[str, dict[str, Any]], *, reverse: bool) -> list[dict[str, Any]]:
        return sorted(
            (
                value
                for value in family_map.values()
                if value.get("qualified")
            ),
            key=lambda item: (item.get("effective_win_rate", item["win_rate"]), item["total"], item["avg_return"]),
            reverse=reverse,
        )

    best = sorted(
        (
            {"signal_family": key, "signal_family_label": family_label(key), **val}
            for key, val in signal_summary.items()
            if val["qualified"]
        ),
        key=lambda item: (item.get("effective_win_rate", item["win_rate"]), item["total"], item["avg_return"]),
        reverse=True,
    )
    weakest = sorted(
        (
            {"signal_family": key, "signal_family_label": family_label(key), **val}
            for key, val in signal_summary.items()
            if val["qualified"]
        ),
        key=lambda item: (item.get("effective_win_rate", item["win_rate"]), -item["total"], item["avg_return"]),
    )
    aligned_best = _rank_family_map(family_alignment_summary.get("aligned", {}), reverse=True)
    aligned_weakest = _rank_family_map(family_alignment_summary.get("aligned", {}), reverse=False)
    opposed_best = _rank_family_map(family_alignment_summary.get("opposed", {}), reverse=True)
    opposed_weakest = _rank_family_map(family_alignment_summary.get("opposed", {}), reverse=False)
    return {
        "window_days": days,
        "min_samples": min_samples,
        "raw_rows": raw_rows,
        "deduped_rows": len(seen_sample_keys),
        "duplicates_skipped": raw_rows - len(seen_sample_keys),
        "overall": _finalize(overall),
        "by_signal_family": signal_summary,
        "by_alignment": alignment_summary,
        "by_family_alignment": family_alignment_summary,
        "best_signal_families": best[:5],
        "weak_signal_families": weakest[:5],
        "best_aligned_families": aligned_best[:5],
        "weak_aligned_families": aligned_weakest[:5],
        "best_opposed_families": opposed_best[:5],
        "weak_opposed_families": opposed_weakest[:5],
    }


def backfill_candidate_signal_families(*, limit: int | None = None) -> int:
    init_state_db()
    query = """
        SELECT candidate_id, signal_family, payload_json
          FROM candidate_registry
         ORDER BY detected_at DESC
    """
    params: list[Any] = []
    if isinstance(limit, int) and limit > 0:
        query += " LIMIT ?"
        params.append(limit)

    updated = 0
    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()
        for row in rows:
            try:
                payload = json.loads(row["payload_json"] or "{}")
            except Exception:
                payload = {}
            signals_fired = _candidate_signals_fired(payload)
            canonical_signal_family = canonical_family_key(
                signal_family=str(row["signal_family"] or payload.get("signal_family") or "").strip() or None,
                swing_type=str(payload.get("signal_family_raw") or payload.get("swing_type") or "").strip() or None,
                signals_fired=signals_fired,
            )
            raw_signal_family = _candidate_raw_signal_family(payload)
            payload_changed = False
            if canonical_signal_family and payload.get("signal_family") != canonical_signal_family:
                payload["signal_family"] = canonical_signal_family
                payload_changed = True
            if canonical_signal_family and payload.get("probability_family") != canonical_signal_family:
                payload["probability_family"] = canonical_signal_family
                payload_changed = True
            if raw_signal_family and payload.get("signal_family_raw") != raw_signal_family:
                payload["signal_family_raw"] = raw_signal_family
                payload_changed = True
            if canonical_signal_family:
                label = family_label(canonical_signal_family)
                if payload.get("signal_family_label") != label:
                    payload["signal_family_label"] = label
                    payload_changed = True

            row_changed = canonical_signal_family and canonical_signal_family != (row["signal_family"] or None)
            if not payload_changed and not row_changed:
                continue

            conn.execute(
                """
                UPDATE candidate_registry
                   SET signal_family = ?,
                       payload_json = ?,
                       updated_at = ?
                 WHERE candidate_id = ?
                """,
                (
                    canonical_signal_family,
                    _json(payload) or "{}",
                    _now_iso(),
                    row["candidate_id"],
                ),
            )
            updated += 1
    return updated


def record_candidate_review(
    candidate_id: str,
    *,
    analysis_date: str,
    run_id: str | None = None,
    alignment: str | None = None,
    review_verdict: str | None = None,
    orca_regime: str | None = None,
    orca_trend: str | None = None,
    confidence: str | None = None,
    thesis_killer: str | None = None,
    review: dict[str, Any] | None = None,
) -> str:
    init_state_db()
    review_id = f"review_{uuid4().hex}"
    reviewed_at = _now_iso()
    with _connect() as conn:
        valid_run_id = run_id
        if run_id:
            run_row = conn.execute(
                "SELECT run_id FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if not run_row:
                valid_run_id = None
        if valid_run_id:
            existing = conn.execute(
                """
                SELECT review_id
                  FROM candidate_reviews
                 WHERE candidate_id = ? AND run_id = ?
                """,
                (candidate_id, valid_run_id),
            ).fetchone()
            if existing:
                review_id = existing["review_id"]
        conn.execute(
            """
            INSERT INTO candidate_reviews (
                review_id, candidate_id, run_id, analysis_date, reviewed_at,
                alignment, review_verdict, orca_regime, orca_trend,
                confidence, thesis_killer, review_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(candidate_id, run_id) DO UPDATE SET
                analysis_date = excluded.analysis_date,
                reviewed_at = excluded.reviewed_at,
                alignment = excluded.alignment,
                review_verdict = excluded.review_verdict,
                orca_regime = excluded.orca_regime,
                orca_trend = excluded.orca_trend,
                confidence = excluded.confidence,
                thesis_killer = excluded.thesis_killer,
                review_json = excluded.review_json
            """,
            (
                review_id,
                candidate_id,
                valid_run_id,
                analysis_date,
                reviewed_at,
                alignment,
                review_verdict,
                orca_regime,
                orca_trend,
                confidence,
                thesis_killer,
                _json(review or {}) or "{}",
            ),
        )
        if alignment:
            conn.execute(
                """
                UPDATE candidate_registry
                   SET orca_alignment = ?,
                       updated_at = ?
                 WHERE candidate_id = ?
                """,
                (alignment, reviewed_at, candidate_id),
            )
        _sync_candidate_probability_lesson(conn, candidate_id)
    return review_id


def record_candidate_lesson(
    candidate_id: str,
    *,
    lesson_type: str,
    label: str | None = None,
    lesson_value: float | None = None,
    lesson: dict[str, Any] | None = None,
    outcome_id: str | None = None,
) -> str:
    init_state_db()
    lesson_id = f"lesson_{uuid4().hex}"
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO candidate_lessons (
                lesson_id, candidate_id, outcome_id, lesson_type,
                label, lesson_value, lesson_timestamp, lesson_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                lesson_id,
                candidate_id,
                outcome_id,
                lesson_type,
                label,
                lesson_value,
                _now_iso(),
                _json(lesson or {}) or "{}",
            ),
        )
    return lesson_id

