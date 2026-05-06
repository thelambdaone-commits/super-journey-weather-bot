"""
Edge and EV calculations.

Net EV formula:
    net_ev = ((model_probability - entry_price) / entry_price) - fee_roi - slippage_roi

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


@dataclass
class RuntimeEdge:
    """Runtime edge used by the scanner's legacy opportunity flow."""

    raw_ev: float
    adjusted_ev: float
    penalties: dict[str, float]


class EdgeEngine:
    """Compute runtime EV with small confidence and data-quality haircuts."""

    def compute(
        self,
        model_probability: float,
        market_ask: float,
        features: dict,
        source: str | None,
        volume: float,
    ) -> RuntimeEdge:
        raw_ev = gross_edge(float(model_probability), float(market_ask))
        confidence = max(0.0, min(1.0, float(features.get("confidence", 0.5) or 0.0)))
        bias = abs(float(features.get("bias", features.get("source_bias", 0.0)) or 0.0))

        penalties = {
            "low_confidence": round((1.0 - confidence) * 0.02, 6),
            "source_bias": round(min(bias / 100.0, 0.03), 6),
            "low_volume": 0.01 if float(volume or 0.0) < 500.0 else 0.0,
        }
        adjusted_ev = raw_ev - sum(penalties.values())
        return RuntimeEdge(raw_ev=raw_ev, adjusted_ev=adjusted_ev, penalties=penalties)


def implied_probability_from_price(price: float) -> float:
    """Convert a market price (0-1) to implied probability."""
    return price


def gross_edge(model_probability: float, market_price: float) -> float:
    """Gross edge as ROI: (model_prob - market_price) / market_price (before fees/slippage)."""
    if market_price <= 0:
        return 0.0
    return (model_probability - market_price) / market_price


def estimate_fee(*args) -> float:
    """Estimate fee as ROI impact.

    Accepts both ``estimate_fee(config)`` and the legacy
    ``estimate_fee(price, size, config)`` call shape.
    """
    config = args[-1] if args else None
    fee_bps = getattr(config, "estimated_fee_bps", 10.0)
    return fee_bps / 10000.0


def estimate_slippage(orderbook: dict, size: float, side: str = "buy", entry_price: float = 0.0) -> float:
    """
    Estimate slippage as ROI impact (fraction of investment).
    Returns slippage as a fraction of entry price.
    size is in USD to spend.
    """
    if not orderbook:
        return 0.0
    levels = orderbook.get("asks" if side == "buy" else "bids", [])
    if not levels:
        return 0.0
    remaining_usd = size
    total_usd_spent = 0.0
    total_shares = 0.0
    if levels and isinstance(levels[0], dict):
        best_price = float(levels[0].get("price", 0.0))
    else:
        best_price = float(levels[0][0]) if levels else 0.0
    for level in levels:
        if isinstance(level, dict):
            price = float(level.get("price", 0.0))
            size_shares = float(level.get("size", 0.0))
        else:
            price = float(level[0])
            size_shares = float(level[1])
        avail_usd = size_shares * price
        take_usd = min(remaining_usd, avail_usd)
        shares_bought = take_usd / price if price > 0 else 0
        total_usd_spent += take_usd
        total_shares += shares_bought
        remaining_usd -= take_usd
        if remaining_usd <= 0:
            break
    if total_shares <= 0:
        return 0.0
    avg_price = total_usd_spent / total_shares
    # Convert to ROI terms: (avg_price - entry_price) / entry_price
    reference_price = entry_price if entry_price > 0 else best_price
    if reference_price > 0:
        slippage_roi = (avg_price - reference_price) / reference_price
    else:
        slippage_roi = 0.0
    return max(slippage_roi, 0.0)


def net_ev(model_probability: float, entry_price: float, fee: float, slippage: float) -> float:
    """
    Net EV as return on investment after fees and slippage.
    EV = (model_prob - entry_price) / entry_price - fee - slippage
    fee and slippage should be in ROI terms (fraction of investment).
    """
    if entry_price <= 0:
        return 0.0
    gross_roi = (model_probability - entry_price) / entry_price
    return gross_roi - fee - slippage


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
    All values now in ROI terms (return on investment).
    """
    spread = ask - bid if ask > bid else 0.0
    entry_price = ask  # conservative: assume we pay ask
    ge = gross_edge(model_probability, entry_price)
    fee_roi = estimate_fee(config)
    slip_roi = estimate_slippage(orderbook or {}, size, side="buy", entry_price=entry_price)
    ne = net_ev(model_probability, entry_price, fee_roi, slip_roi)
    return EdgeEstimate(
        gross_edge=ge,
        net_ev=ne,
        fee_probability=fee_roi,
        slippage_probability=slip_roi,
        entry_price=entry_price,
        model_probability=model_probability,
        spread=spread,
        volume=volume,
    )
