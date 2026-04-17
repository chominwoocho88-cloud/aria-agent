"""Shared candidate-probability helpers for JACKAL."""
from __future__ import annotations

from copy import deepcopy
from typing import Any

from orca.state import summarize_candidate_probabilities


def load_probability_summary(*, days: int = 90, min_samples: int = 3) -> dict[str, Any] | None:
    try:
        return summarize_candidate_probabilities(days=days, min_samples=min_samples)
    except Exception:
        return None


def apply_probability_adjustment(
    final: dict[str, Any],
    signal_family: str,
    lesson_summary: dict[str, Any] | None,
    *,
    entry_threshold: float,
    blocked_verdict: str | None = None,
    blocked_mode_token: str | None = None,
) -> dict[str, Any]:
    updated = deepcopy(final)
    updated.setdefault("probability_adjustment", 0.0)
    updated.setdefault("probability_samples", 0)
    updated.setdefault("probability_win_rate", None)
    updated.setdefault("probability_signal_family", signal_family)
    if not lesson_summary or not signal_family:
        return updated

    family_stats = lesson_summary.get("by_signal_family", {}).get(signal_family)
    if not family_stats or not family_stats.get("qualified"):
        return updated

    total = int(family_stats.get("total", 0))
    win_rate = float(family_stats.get("win_rate", 0.0))
    adjustment = 0.0
    if total >= 5 and win_rate >= 70:
        adjustment = 4.0
    elif total >= 3 and win_rate >= 62:
        adjustment = 2.0
    elif total >= 5 and win_rate <= 35:
        adjustment = -5.0
    elif total >= 3 and win_rate <= 45:
        adjustment = -3.0

    if adjustment:
        updated["final_score"] = round(max(0, min(100, float(updated.get("final_score", 0)) + adjustment)), 1)
    updated["probability_adjustment"] = adjustment
    updated["probability_samples"] = total
    updated["probability_win_rate"] = win_rate

    blocked = False
    if blocked_verdict and str(updated.get("verdict", "")) == blocked_verdict:
        blocked = True
    if blocked_mode_token and blocked_mode_token in str(updated.get("mode", "")):
        blocked = True
    updated["is_entry"] = bool(updated.get("final_score", 0) >= entry_threshold) and not blocked
    return updated
