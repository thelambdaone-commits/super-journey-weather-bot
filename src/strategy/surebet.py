"""Surebet / dutching detection for exhaustive Polymarket weather buckets."""
from __future__ import annotations

from dataclasses import dataclass
from math import isclose


@dataclass
class SurebetLeg:
    market_id: str
    token_id: str | None
    bucket_low: float
    bucket_high: float
    ask: float
    stake: float
    payout: float


@dataclass
class SurebetOpportunity:
    legs: list[SurebetLeg]
    total_cost: float
    guaranteed_payout: float
    guaranteed_profit: float
    profit_pct: float
    implied_sum: float


def _sorted_ranges(outcomes: list[dict]) -> list[tuple[float, float]]:
    return sorted((float(o["range"][0]), float(o["range"][1])) for o in outcomes)


def has_exhaustive_temperature_coverage(outcomes: list[dict]) -> bool:
    """Return True when bucket ranges cover the full temperature event space."""
    ranges = _sorted_ranges(outcomes)
    if not ranges:
        return False
    if ranges[0][0] > -999 or ranges[-1][1] < 999:
        return False

    previous_high = ranges[0][1]
    for low, high in ranges[1:]:
        if low > previous_high and not isclose(low, previous_high + 1.0, abs_tol=1e-9):
            return False
        previous_high = max(previous_high, high)
    return True


def detect_surebet(
    outcomes: list[dict],
    *,
    max_total_stake: float,
    min_profit_pct: float = 0.01,
    fee_buffer_pct: float = 0.003,
    min_liquidity_usd: float = 1.0,
) -> SurebetOpportunity | None:
    """Detect a risk-free dutching opportunity across all exhaustive buckets."""
    tradeable = []
    for outcome in outcomes:
        ask = float(outcome.get("ask") or 0.0)
        bid = float(outcome.get("bid") or 0.0)
        if ask <= 0 or ask >= 1 or bid > ask:
            return None
        liquidity_usd = float(outcome.get("best_ask_size", 0.0) or 0.0) * ask
        if liquidity_usd < min_liquidity_usd:
            return None
        tradeable.append(outcome)

    if len(tradeable) < 2 or not has_exhaustive_temperature_coverage(tradeable):
        return None

    implied_sum = sum(float(outcome["ask"]) for outcome in tradeable)
    required_edge = 1.0 - min_profit_pct - fee_buffer_pct
    if implied_sum >= required_edge:
        return None

    guaranteed_payout = max_total_stake / implied_sum
    legs = []
    total_cost = 0.0
    for outcome in tradeable:
        ask = float(outcome["ask"])
        stake = guaranteed_payout * ask
        total_cost += stake
        low, high = outcome["range"]
        legs.append(
            SurebetLeg(
                market_id=str(outcome.get("market_id", "")),
                token_id=outcome.get("token_id"),
                bucket_low=float(low),
                bucket_high=float(high),
                ask=ask,
                stake=round(stake, 2),
                payout=round(guaranteed_payout, 2),
            )
        )

    total_cost = round(total_cost, 2)
    guaranteed_payout = round(guaranteed_payout, 2)
    guaranteed_profit = round(guaranteed_payout - total_cost, 2)
    profit_pct = round(guaranteed_profit / total_cost, 4) if total_cost else 0.0
    if profit_pct < min_profit_pct:
        return None

    return SurebetOpportunity(
        legs=legs,
        total_cost=total_cost,
        guaranteed_payout=guaranteed_payout,
        guaranteed_profit=guaranteed_profit,
        profit_pct=profit_pct,
        implied_sum=round(implied_sum, 4),
    )
