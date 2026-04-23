"""
Market feature builders.
"""
from __future__ import annotations


def build_market_features(outcomes: list[dict], selected_outcome: dict | None = None) -> dict:
    """Build market state features."""
    top = max(outcomes, key=lambda outcome: outcome["price"]) if outcomes else None
    target = selected_outcome or top
    liquidity = sum(float(outcome.get("volume", 0)) for outcome in outcomes)
    top_bucket = None
    if top:
        top_bucket = f"{top['range'][0]}-{top['range'][1]}"
    return {
        "market_price": None if not target else target.get("ask"),
        "market_implied_prob": None if not target else target.get("ask"),
        "liquidity": round(liquidity, 2),
        "spread": None if not target else target.get("spread"),
        "top_market_price": None if not top else top.get("price"),
        "top_bucket": top_bucket,
    }
