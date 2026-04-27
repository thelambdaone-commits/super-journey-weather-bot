"""
Edge and EV calculations with live bias adjustment + net EV.
"""
from __future__ import annotations

from dataclasses import dataclass

from ..weather.math import calc_ev
from ..probability.uncertainty import uncertainty_penalty


# === CONSTANTS FOR STRICTER FILTERS ===
MIN_EV = 0.06  # Minimum 6% EV to consider trade
MAX_SPREAD = 0.03  # Maximum 3% spread
MIN_VOLUME = 5000  # Minimum $5000 volume


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
    net_ev: float  # After spread/slippage/fees
    market_edge: float
    penalties: dict


class EdgeEngine:
    """Compute raw and adjusted edge with live bias and net EV."""
    
    def __init__(self):
        self.use_live_bias = True
        # Configuration
        self.min_ev = MIN_EV
        self.max_spread = MAX_SPREAD
        self.min_volume = MIN_VOLUME
        self.kelly_multiplier = 0.10  # Reduced from 0.25

    def compute(
        self, 
        probability: float, 
        ask: float, 
        features: dict, 
        source: str = "ecmwf",
        volume: float = 0,
    ) -> EdgeEstimate:
        """Compute EV with market penalties, live bias, and net EV."""
        
        # 1. Raw EV
        raw_ev = calc_ev(probability, ask)
        
        # 2. Spread penalty
        spread = float(features.get("spread") or 0.0)
        spread_penalty = min(spread * 2.0, 0.15)
        
        # 3. Liquidity penalty
        liquidity = float(features.get("liquidity") or volume)
        liquidity_penalty = 0.0 if liquidity >= self.min_volume else min(
            (self.min_volume - liquidity) / self.min_volume * 0.10, 0.10
        )
        
        # 4. Risk penalty (uncertainty)
        risk_penalty = uncertainty_penalty(features)
        
        # 5. Live bias penalty
        bias_penalty = 0.0
        if self.use_live_bias:
            live_bias = get_live_bias(source)
            bias_penalty = abs(live_bias) * 0.05
        
        # 6. NET EV (after all costs)
        slippage_estimate = spread * 0.5  # Assume 50% of spread as slippage
        fees_estimate = 0.001  # ~0.1% Polymarket fees
        net_price = ask + slippage_estimate + fees_estimate
        net_ev = calc_ev(probability, net_price) - spread_penalty - liquidity_penalty - risk_penalty
        
        adjusted_ev = round(raw_ev - spread_penalty - liquidity_penalty - risk_penalty - bias_penalty, 4)
        
        return EdgeEstimate(
            raw_ev=raw_ev,
            adjusted_ev=adjusted_ev,
            net_ev=net_ev,
            market_edge=round(probability - ask, 4),
            penalties={
                "spread": round(spread_penalty, 4),
                "liquidity": round(liquidity_penalty, 4),
                "uncertainty": round(risk_penalty, 4),
                "bias": round(bias_penalty, 4),
            },
        )
    
    def should_skip(
        self,
        probability: float,
        ask: float,
        features: dict,
        volume: float = 0,
    ) -> tuple[bool, str]:
        """Check if trade should be skipped due to filters."""
        
        spread = float(features.get("spread", 0.0))
        liquidity = float(features.get("liquidity", volume))
        
        # Spread filter
        if spread > self.max_spread:
            return True, f"spread_too_high ({spread:.1%})"
        
        # Volume filter  
        if liquidity > 0 and liquidity < self.min_volume:
            return True, f"volume_too_low ({liquidity:.0f})"
        
        # EV filter
        raw_ev = calc_ev(probability, ask)
        if raw_ev < self.min_ev:
            return True, f"ev_too_low ({raw_ev:.1%} < {self.min_ev:.1%})"
        
        return False, "ok"


# Audit: Includes fee and slippage awareness
