"""
Edge and EV calculations.
"""
from __future__ import annotations

from dataclasses import dataclass

from ..weather.math import calc_ev
from ..probability.uncertainty import uncertainty_penalty


@dataclass
class EdgeEstimate:
    """Risk-adjusted edge result."""

    raw_ev: float
    adjusted_ev: float
    market_edge: float
    penalties: dict


class EdgeEngine:
    """Compute raw and adjusted edge."""

    def compute(self, probability: float, ask: float, features: dict) -> EdgeEstimate:
        """Compute EV with simple market penalties."""
        raw_ev = calc_ev(probability, ask)
        spread_penalty = min(float(features.get("spread") or 0.0) * 2.0, 0.15)
        liquidity = float(features.get("liquidity") or 0.0)
        liquidity_penalty = 0.0 if liquidity >= 5000 else min((5000 - liquidity) / 5000 * 0.10, 0.10)
        risk_penalty = uncertainty_penalty(features)
        adjusted_ev = round(raw_ev - spread_penalty - liquidity_penalty - risk_penalty, 4)
        return EdgeEstimate(
            raw_ev=raw_ev,
            adjusted_ev=adjusted_ev,
            market_edge=round(probability - ask, 4),
            penalties={
                "spread": round(spread_penalty, 4),
                "liquidity": round(liquidity_penalty, 4),
                "uncertainty": round(risk_penalty, 4),
            },
        )

