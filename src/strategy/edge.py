"""
Edge and EV calculations.

Net EV formula:
    net_ev = model_probability - entry_price - estimated_fee_probability - estimated_slippage_probability

All functions are pure where possible for testability.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class EdgeEstimate:
    """Result of edge calculation."""
    gross_edge: float
    net_ev: float
    fee_probability: float
    slippage_probability: float
    entry_price: float
    model_probability: float
    spread: float
    volume: float


def implied_probability_from_price(price: float) -> float:
    """Convert a market price (0-1) to implied probability."""
    return price


def gross_edge(model_probability: float, market_price: float) -> float:
    """Gross edge: model_prob - market_price (before fees/slippage)."""
    return model_probability - market_price


def estimate_fee(price: float, size: float, config) -> float:
    """Estimate fee in USD. Returns fee as probability-equivalent."""
    fee_bps = getattr(config, "estimated_fee_bps", 10.0)
    fee_usd = size * (fee_bps / 10000.0)
    # Convert to probability-equivalent: fee as fraction of position
    return fee_usd / size if size > 0 else fee_bps / 10000.0


def estimate_slippage(orderbook: dict, size: float, side: str = "buy") -> float:
    """
    Estimate slippage in USD using orderbook depth.
    Returns slippage as probability-equivalent.
    """
    if not orderbook:
        return 0.0
    levels = orderbook.get("asks" if side == "buy" else "bids", [])
    remaining = size
    total_cost = 0.0
    best_price = float(levels[0][0]) if levels else 0.0
    for level in levels:
        price = float(level[0])
        avail_usd = float(level[1]) * price
        take = min(remaining, avail_usd)
        total_cost += take * price
        remaining -= take
        if remaining <= 0:
            break
    if size <= 0:
        return 0.0
    avg_price = total_cost / size if total_cost > 0 else best_price
    slippage_prob = avg_price - best_price if best_price > 0 else 0.0
    return max(slippage_prob, 0.0)


def net_ev(model_probability: float, entry_price: float, fee: float, slippage: float) -> float:
    """
    Net EV after fees and slippage.
    fee and slippage are probability-equivalent (0-1 scale).
    """
    return model_probability - entry_price - fee - slippage


def should_bet(net_ev_value: float, min_edge: float) -> bool:
    """Determine if a bet should be placed."""
    return net_ev_value > min_edge


def compute_edge(
    model_probability: float,
    ask: float,
    bid: float,
    volume: float,
    size: float,
    orderbook: Optional[dict],
    config,
) -> EdgeEstimate:
    """
    Full edge computation matching the target architecture.
    Uses bid/ask spread, estimates fees and slippage.
    """
    spread = ask - bid if ask > bid else 0.0
    entry_price = ask  # conservative: assume we pay ask
    ge = gross_edge(model_probability, entry_price)
    fee_prob = estimate_fee(entry_price, size, config)
    slip_prob = estimate_slippage(orderbook or {}, size, side="buy")
    ne = net_ev(model_probability, entry_price, fee_prob, slip_prob)
    return EdgeEstimate(
        gross_edge=ge,
        net_ev=ne,
        fee_probability=fee_prob,
        slippage_probability=slip_prob,
        entry_price=entry_price,
        model_probability=model_probability,
        spread=spread,
        volume=volume,
    )
