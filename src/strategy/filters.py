"""
Strategy filters.
"""
from __future__ import annotations


def should_skip_outcome(config, outcome: dict, features: dict, adjusted_ev: float) -> bool:
    """Return True when the candidate market should be skipped."""
    if float(outcome.get("volume", 0)) < config.min_volume:
        return True
    if float(outcome.get("ask", 1.0)) >= config.max_price:
        return True
    if float(outcome.get("spread", 0.0)) > config.max_slippage:
        return True
    if adjusted_ev < config.min_ev:
        return True
    return False

