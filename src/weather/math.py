"""
Math utilities for trading.
"""
import math
from typing import Optional


def norm_cdf(x: float) -> float:
    """Standard normal CDF."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bucket_prob(forecast: float, t_low: float, t_high: float, sigma: float = 2.0) -> float:
    """Calculate probability of temperature falling in bucket."""
    if sigma <= 0:
        return 1.0 if in_bucket(forecast, t_low, t_high) else 0.0
    if t_low == -999:
        return norm_cdf((t_high - float(forecast)) / sigma)
    if t_high == 999:
        return 1.0 - norm_cdf((t_low - float(forecast)) / sigma)
    if t_low == t_high:
        lower = t_low - 0.5
        upper = t_high + 0.5
    else:
        lower = t_low
        upper = t_high
    probability = norm_cdf((upper - float(forecast)) / sigma) - norm_cdf((lower - float(forecast)) / sigma)
    return max(0.0, min(1.0, probability))


def in_bucket(forecast: float, t_low: float, t_high: float) -> bool:
    """Check if forecast falls in bucket."""
    if t_low == t_high:
        return round(float(forecast)) == round(t_low)
    return t_low <= float(forecast) <= t_high


def calc_ev(p: float, price: float) -> float:
    """Calculate expected value."""
    if price <= 0 or price >= 1:
        return 0.0
    return round(p * (1.0 / price - 1.0) - (1.0 - p), 4)


def calc_kelly(p: float, price: float, fraction: float = 0.25) -> float:
    """Calculate Kelly fraction bet size."""
    if price <= 0 or price >= 1:
        return 0.0
    b = 1.0 / price - 1.0
    f = (p * b - (1.0 - p)) / b
    return round(min(max(0.0, f) * fraction, 1.0), 4)


def bet_size(kelly: float, balance: float, max_bet: float) -> float:
    """Calculate bet amount."""
    return round(min(kelly * balance, max_bet), 2)


def calc_spread(bid: float, ask: float) -> float:
    """Calculate bid-ask spread."""
    return round(ask - bid, 4)
