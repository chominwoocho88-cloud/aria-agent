"""Canonical JACKAL family taxonomy shared across Hunter, Scanner, and ORCA."""
from __future__ import annotations

from typing import Iterable


FAMILY_LABELS = {
    "rotation": "섹터로테이션",
    "panic_rebound": "패닉셀반등",
    "momentum_pullback": "모멘텀눌림목",
    "ma_reclaim": "MA지지반등",
    "divergence": "강세다이버전스",
    "oversold_rebound": "기술적과매도",
    "general_rebound": "일반반등",
}

_HUNTER_SWING_TYPE_MAP = {
    "강세다이버전스": "divergence",
    "섹터로테이션": "rotation",
    "패닉셀반등": "panic_rebound",
    "모멘텀눌림목": "momentum_pullback",
    "MA지지반등": "ma_reclaim",
    "기술적과매도": "oversold_rebound",
}


def family_label(family_key: str | None) -> str:
    if not family_key:
        return FAMILY_LABELS["general_rebound"]
    return FAMILY_LABELS.get(str(family_key), str(family_key))


def canonical_family_key(
    *,
    signal_family: str | None = None,
    swing_type: str | None = None,
    signals_fired: Iterable[str] | None = None,
) -> str:
    if swing_type:
        swing_text = str(swing_type).strip()
        mapped = _HUNTER_SWING_TYPE_MAP.get(swing_text)
        if mapped:
            return mapped
        swing_lower = swing_text.lower()
        if "섹터" in swing_text or "rotation" in swing_lower:
            return "rotation"
        if "패닉" in swing_text or "panic" in swing_lower:
            return "panic_rebound"
        if "모멘텀" in swing_text or "momentum" in swing_lower:
            return "momentum_pullback"
        if "ma" in swing_lower or "지지" in swing_text:
            return "ma_reclaim"
        if "다이버전스" in swing_text or "divergence" in swing_lower:
            return "divergence"
        if "과매도" in swing_text or "oversold" in swing_lower:
            return "oversold_rebound"

    raw_family = str(signal_family or "").strip()
    signals = {str(sig).strip() for sig in (signals_fired or []) if str(sig).strip()}

    if raw_family in FAMILY_LABELS:
        return raw_family
    if raw_family in _HUNTER_SWING_TYPE_MAP:
        return _HUNTER_SWING_TYPE_MAP[raw_family]

    if raw_family == "crash_rebound":
        if "sector_rebound" in signals:
            return "rotation"
        if "volume_climax" in signals:
            return "panic_rebound"
        if "vol_accumulation" in signals or "momentum_dip" in signals:
            return "momentum_pullback"
        if signals & {"52w_low_zone", "rsi_oversold", "bb_touch"}:
            return "oversold_rebound"
        return "general_rebound"

    if raw_family in {"ma_support_solo", "ma_support_weak"}:
        return "ma_reclaim"

    if raw_family == "general":
        if "rsi_divergence" in signals:
            return "divergence"
        if "momentum_dip" in signals:
            return "momentum_pullback"
        if signals & {"rsi_oversold", "bb_touch"}:
            return "oversold_rebound"
        return "general_rebound"

    if signals:
        if "rsi_divergence" in signals:
            return "divergence"
        if "bullish_div" in signals:
            return "divergence"
        if "sector_rebound" in signals:
            return "rotation"
        if "sector_inflow" in signals:
            return "rotation"
        if "volume_climax" in signals:
            return "panic_rebound"
        if "volume_surge" in signals:
            return "panic_rebound"
        if "momentum_dip" in signals or "vol_accumulation" in signals:
            return "momentum_pullback"
        if "ma_support" in signals:
            return "ma_reclaim"
        if signals & {"rsi_oversold", "bb_touch", "52w_low_zone"}:
            return "oversold_rebound"

    return "general_rebound"
