"""
Position sizing helpers.
"""
from __future__ import annotations

from ..weather.math import bet_size, calc_kelly


def size_position(probability: float, ask: float, balance: float, kelly_fraction: float, max_bet: float) -> tuple[float, float]:
    """Return Kelly fraction and capped dollar size."""
    kelly = calc_kelly(probability, ask, kelly_fraction)
    return kelly, bet_size(kelly, balance, max_bet)
