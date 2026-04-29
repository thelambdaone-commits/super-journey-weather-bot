"""
Position sizing with fractional Kelly and exposure caps.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class SizingResult:
    """Result of position sizing calculation."""
    kelly_fraction: float
    fractional_kelly: float
    raw_size: float
    capped_size: float
    final_size: float
    reason: str


def kelly_fraction(probability: float, price: float) -> float:
    """
    Full Kelly fraction: (p * (1 - price) - (1 - p) * price) / (price * (1 - price))
    Simplified: (p - price) / (price * (1 - price)) for binary outcome.
    """
    if price <= 0.0 or price >= 1.0:
        return 0.0
    return (probability - price) / (price * (1.0 - price))


def fractional_kelly(probability: float, price: float, fraction: float = 0.25) -> float:
    """Fractional Kelly: fraction * full_kelly (default 0.25)."""
    return fraction * kelly_fraction(probability, price)


def cap_position_size(
    raw_size: float,
    bankroll: float,
    max_position_pct: float,
    max_market_exposure_pct: float,
    current_market_exposure: float = 0.0,
) -> float:
    """Cap position size by bankroll % and market exposure %."""
    max_by_bankroll = bankroll * max_position_pct
    max_by_market = bankroll * max_market_exposure_pct - current_market_exposure
    cap = min(max_by_bankroll, max_by_market)
    return min(raw_size, max(0.0, cap))


def final_position_size(
    probability: float,
    entry_price: float,
    bankroll: float,
    config,
    current_market_exposure: float = 0.0,
    daily_pnl: float = 0.0,
) -> SizingResult:
    """
    Compute final position size with all constraints.
    """
    fraction = getattr(config, "kelly_fraction", 0.25)
    max_pos_pct = getattr(config, "max_position_pct", 0.02)
    max_market_pct = getattr(config, "max_market_exposure_pct", 0.05)
    max_dd = getattr(config, "max_daily_drawdown", 0.05)

    fk = fractional_kelly(probability, entry_price, fraction)
    raw_size = max(0.0, fk * bankroll)

    # Check daily drawdown
    if daily_pnl < -bankroll * max_dd:
        return SizingResult(
            kelly_fraction=kelly_fraction(probability, entry_price),
            fractional_kelly=fk,
            raw_size=raw_size,
            capped_size=0.0,
            final_size=0.0,
            reason=f"daily_drawdown_exceeded (pnl={daily_pnl:.2f})",
        )

    capped = cap_position_size(raw_size, bankroll, max_pos_pct, max_market_pct, current_market_exposure)
    final = max(0.0, capped)

    reason = "ok"
    if final <= 0:
        reason = "size_capped_to_zero"
    elif final < raw_size:
        reason = f"capped ({raw_size:.2f} -> {final:.2f})"

    return SizingResult(
        kelly_fraction=kelly_fraction(probability, entry_price),
        fractional_kelly=fk,
        raw_size=raw_size,
        capped_size=capped,
        final_size=final,
        reason=reason,
    )
