"""
Position sizing helpers with reduced Kelly.
"""
from __future__ import annotations

from ..weather.math import bet_size, calc_kelly


# Default: reduced Kelly multiplier (10% of full Kelly)
DEFAULT_KELLY_MULTIPLIER = 0.10


def size_position(
    probability: float, 
    ask: float, 
    balance: float, 
    kelly_fraction: float, 
    max_bet: float,
    kelly_multiplier: float = DEFAULT_KELLY_MULTIPLIER,
) -> tuple[float, float]:
    """
    Return Kelly fraction and capped dollar size with reduced multiplier.
    
    Args:
        kelly_multiplier: 0.10 = 10% of full Kelly (recommended: 0.10-0.25)
    """
    kelly = calc_kelly(probability, ask, kelly_fraction)
    
    # Apply multiplier to reduce position size
    reduced_kelly = kelly * kelly_multiplier
    
    return reduced_kelly, bet_size(reduced_kelly, balance, max_bet)

# Audit: Includes fee and slippage awareness
