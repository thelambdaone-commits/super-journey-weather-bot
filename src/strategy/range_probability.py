"""
Range Probability Engine - Core pricing engine.

Transforms the bot from a weather forecaster to a market pricing engine.
For EVERY bucket in a market, calculates P(temp in bucket) using normal CDF.
Compares model probability vs market price to find mispricings.
"""

from __future__ import annotations

import math
from typing import List, Dict, Optional
from datetime import datetime


def norm_cdf(x: float) -> float:
    """Standard normal CDF."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def calculate_bucket_prob(forecast_temp: float, sigma: float,
                           low: float, high: float, calibration_factor: float = 0.334) -> tuple[float, float]:
    """
    Calculate probability of temperature falling in bucket [low, high].
    Uses normal CDF: P(low <= T <= high)

    Special cases:
    - low = -999: "or below" bucket → P(T <= high)
    - high = 999: "or higher" bucket → P(T >= low)
    - low = high: point temperature → P(low-0.5 <= T <= high+0.5)

    Returns (raw_prob, calibrated_prob) for calibration tracking.
    """
    if sigma <= 0:
        # Degenerate case: temperature is known exactly
        if low <= forecast_temp <= high:
            return 1.0, 1.0
        return 0.0, 0.0

    if low == -999:
        # "X or below"
        prob = norm_cdf((high - forecast_temp) / sigma)
    elif high == 999:
        # "X or higher"
        prob = 1.0 - norm_cdf((low - forecast_temp) / sigma)
    else:
        if low == high:
            # Point temperature (e.g., "be 21°C")
            lower = low - 0.5
            upper = high + 0.5
        else:
            lower = low
            upper = high
        prob = norm_cdf((upper - forecast_temp) / sigma) - norm_cdf((lower - forecast_temp) / sigma)

    raw_prob = max(0.0, min(1.0, prob))
    factor = max(0.0, min(1.0, calibration_factor))
    calibrated_prob = 0.5 + ((raw_prob - 0.5) * factor)
    calibrated_prob = max(0.0, min(1.0, calibrated_prob))
    return raw_prob, calibrated_prob


def calculate_all_bucket_probs(forecast_temp: float, sigma: float,
                               outcomes: List[Dict], calibration_factor: float = 0.334) -> List[Dict]:
    """
    For EVERY bucket in the market, calculate model probability.
    Compare to market price to find edge.

    Returns sorted list by edge_net (highest first).

    Each result contains:
    - market_id: Polymarket market ID
    - bucket: string representation (e.g., "20-22°C")
    - prob_model: P(temp in bucket) from model (calibrated)
    - raw_prob: uncalibrated probability (for calibration tracking)
    - price_market: ask price from orderbook
    - edge_brut: ROI-based gross edge (prob - ask) / ask
    - edge_net: edge after estimated fees (ROI-based)
    - spread: bid-ask spread
    - outcome: original outcome dict
    """
    results = []

    for outcome in outcomes:
        low, high = outcome["range"]
        unit = outcome.get("unit", "C")

        # Calculate model probability for this bucket (returns raw and calibrated)
        raw_prob, prob = calculate_bucket_prob(forecast_temp, sigma, low, high, calibration_factor)

        # Get market price (use ask for conservative entry)
        ask = float(outcome.get("ask", outcome.get("price", 0.5)))
        bid = float(outcome.get("bid", ask))
        spread = abs(ask - bid) if ask > bid else 0.0

        # Calculate edge as ROI (Return on Investment)
        edge_brut = (prob - ask) / ask if ask > 0 else 0.0

        # Net edge after estimated fees (1% taker fee + slippage)
        estimated_fee = 0.01  # 1% conservative
        estimated_slippage = 0.015  # 1.5% conservative slippage
        edge_net = edge_brut - estimated_fee - estimated_slippage

        bucket_str = f"{low}-{high}{unit}" if low != high else f"{low}{unit}"

        results.append({
            "market_id": outcome.get("market_id", ""),
            "bucket": bucket_str,
            "prob_model": prob,
            "raw_prob": raw_prob,
            "price_market": ask,
            "bid": bid,
            "edge_brut": edge_brut,
            "edge_net": edge_net,
            "spread": spread,
            "volume": float(outcome.get("volume", 0)),
            "outcome": outcome
        })

    # Sort by net edge (highest first) - ROI-based sorting
    return sorted(results, key=lambda x: x["edge_net"], reverse=True)


def find_best_edge(results: List[Dict], min_edge: float = 0.05) -> Optional[Dict]:
    """
    Find the best opportunity from calculated bucket probabilities.

    Returns the first bucket with edge_net > min_edge (as ROI), or None.
    Default min_edge = 0.05 (5% ROI) for ROI-based calculation.
    """
    for r in results:
        if r["edge_net"] > min_edge:
            return r
    return None


def format_range_report(results: List[Dict], max_display: int = 10) -> str:
    """Format a report of all bucket probabilities for debugging."""
    lines = [
        f"\n{'=' * 60}",
        f"RANGE PROBABILITY REPORT - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        f"{'=' * 60}",
        f"{'Bucket':<15} {'Model Prob':>12} {'Market Price':>15} {'Edge':>10} {'Net Edge':>12}",
        f"{'-' * 60}"
    ]

    for i, r in enumerate(results[:max_display]):
        marker = " ← ★ BEST" if i == 0 and r["edge_brut"] > 0 else ""
        lines.append(
            f"{r['bucket']:<15} {r['prob_model']:>11.2%} "
            f"{r['price_market']:>14.3f} {r['edge_brut']:>+10.2%} "
            f"{r['edge_net']:>+11.2%}{marker}"
        )

    if len(results) > max_display:
        lines.append(f"... and {len(results) - max_display} more buckets")

    lines.append(f"{'=' * 60}")
    return "\n".join(lines)
