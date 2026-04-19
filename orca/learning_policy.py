"""Shared learning policy for ORCA and JACKAL."""
from __future__ import annotations

from typing import Literal

MIN_SAMPLES = 5
PRIOR_WINS = 2
PRIOR_TOTAL = 4
TRUSTED_EFFECTIVE_WIN_RATE = 0.58
CAUTIOUS_EFFECTIVE_WIN_RATE = 0.46
WEIGHT_ADJUSTMENT_LADDER = [(0.70, +0.05), (0.40, -0.05)]
PROBABILITY_SCORE_ADJUSTMENT_LADDER = [
    (12, 0.68, +4.0),
    (8, 0.62, +2.0),
    (5, 0.58, +1.0),
    (12, 0.38, -5.0),
    (8, 0.43, -3.0),
    (5, 0.46, -1.5),
]
POLICY_VERSION = "v1"
POLICY_SOURCE = "orca/learning_policy.py"


def effective_win_rate(wins: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return (wins + PRIOR_WINS) / (total + PRIOR_TOTAL)


def is_qualified(total: int, *, min_samples: int | None = None) -> bool:
    threshold = MIN_SAMPLES if min_samples is None else min_samples
    return total >= threshold


def classify_family(wins: int, total: int) -> Literal["trusted", "cautious", "neutral", "insufficient"]:
    if not is_qualified(total):
        return "insufficient"
    effective = effective_win_rate(wins, total)
    if effective >= TRUSTED_EFFECTIVE_WIN_RATE:
        return "trusted"
    if effective <= CAUTIOUS_EFFECTIVE_WIN_RATE:
        return "cautious"
    return "neutral"


def suggest_weight_delta(wins: int, total: int) -> float:
    if not is_qualified(total):
        return 0.0
    effective = effective_win_rate(wins, total)
    for threshold, delta in WEIGHT_ADJUSTMENT_LADDER:
        if delta > 0 and effective >= threshold:
            return delta
        if delta < 0 and effective <= threshold:
            return delta
    return 0.0


def suggest_probability_adjustment(wins: int, total: int) -> float:
    effective = effective_win_rate(wins, total)
    for gate, threshold, delta in PROBABILITY_SCORE_ADJUSTMENT_LADDER:
        if delta > 0 and total >= gate and effective >= threshold:
            return delta
        if delta < 0 and total >= gate and effective <= threshold:
            return delta
    return 0.0


def describe_policy() -> dict:
    return {
        "version": POLICY_VERSION,
        "policy_source": POLICY_SOURCE,
        "min_samples": MIN_SAMPLES,
        "prior_wins": PRIOR_WINS,
        "prior_total": PRIOR_TOTAL,
        "trusted_threshold": TRUSTED_EFFECTIVE_WIN_RATE,
        "cautious_threshold": CAUTIOUS_EFFECTIVE_WIN_RATE,
        "weight_adjustment_ladder": [
            {"threshold": threshold, "delta": delta}
            for threshold, delta in WEIGHT_ADJUSTMENT_LADDER
        ],
        "probability_score_adjustment_ladder": [
            {"min_samples": gate, "threshold": threshold, "delta": delta}
            for gate, threshold, delta in PROBABILITY_SCORE_ADJUSTMENT_LADDER
        ],
    }
