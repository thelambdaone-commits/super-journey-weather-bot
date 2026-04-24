"""
Edge and EV calculations with live bias adjustment.
"""
from __future__ import annotations

from dataclasses import dataclass

from ..weather.math import calc_ev
from ..probability.uncertainty import uncertainty_penalty


# Live bias cache for real-time adjustment
_LIVE_BIAS = {
    "ecmwf": 0.0,
    "hrrr": 0.0,
    "updated_at": None,
}


def update_live_bias(source: str, bias: float) -> None:
    """Update live bias for a forecast source."""
    _LIVE_BIAS[source] = bias
    _LIVE_BIAS["updated_at"] = __import__("datetime").datetime.now()


def get_live_bias(source: str) -> float:
    """Get live bias for a forecast source."""
    return _LIVE_BIAS.get(source, 0.0)


@dataclass
class EdgeEstimate:
    """Risk-adjusted edge result."""

    raw_ev: float
    adjusted_ev: float
    market_edge: float
    penalties: dict


class EdgeEngine:
    """Compute raw and adjusted edge with live bias."""

    def __init__(self):
        self.use_live_bias = True

    def compute(self, probability: float, ask: float, features: dict, source: str = "ecmwf") -> EdgeEstimate:
        """Compute EV with simple market penalties and live bias."""
        raw_ev = calc_ev(probability, ask)
        spread_penalty = min(float(features.get("spread") or 0.0) * 2.0, 0.15)
        liquidity = float(features.get("liquidity") or 0.0)
        liquidity_penalty = 0.0 if liquidity >= 5000 else min((5000 - liquidity) / 5000 * 0.10, 0.10)
        risk_penalty = uncertainty_penalty(features)
        
        # Apply live bias penalty if source has known bias
        bias_penalty = 0.0
        if self.use_live_bias:
            live_bias = get_live_bias(source)
            # Convert bias to edge penalty (1°C bias ≈ 5% edge error)
            bias_penalty = abs(live_bias) * 0.05
        
        adjusted_ev = round(raw_ev - spread_penalty - liquidity_penalty - risk_penalty - bias_penalty, 4)
        return EdgeEstimate(
            raw_ev=raw_ev,
            adjusted_ev=adjusted_ev,
            market_edge=round(probability - ask, 4),
            penalties={
                "spread": round(spread_penalty, 4),
                "liquidity": round(liquidity_penalty, 4),
                "uncertainty": round(risk_penalty, 4),
                "bias": round(bias_penalty, 4),
            },
        )

