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
                          low: float, high: float) -> float:
    """
    Calculate probability of temperature falling in bucket [low, high].
    Uses normal CDF: P(low <= T <= high)
    
    Special cases:
    - low = -999: "or below" bucket → P(T <= high)
    - high = 999: "or higher" bucket → P(T >= low)
    - low = high: point temperature → P(low-0.5 <= T <= high+0.5)
    """
    if sigma <= 0:
        # Degenerate case: temperature is known exactly
        if low <= forecast_temp <= high:
            return 1.0
        return 0.0
    
    if low == -999:
        # "X or below"
        return norm_cdf((high - forecast_temp) / sigma)
    
    if high == 999:
        # "X or higher"
        return 1.0 - norm_cdf((low - forecast_temp) / sigma)
    
    if low == high:
        # Point temperature (e.g., "be 21°C")
        lower = low - 0.5
        upper = high + 0.5
    else:
        lower = low
        upper = high
    
    prob = norm_cdf((upper - forecast_temp) / sigma) - norm_cdf((lower - forecast_temp) / sigma)
    return max(0.0, min(1.0, prob))


def calculate_all_bucket_probs(forecast_temp: float, sigma: float,
                              outcomes: List[Dict]) -> List[Dict]:
    """
    For EVERY bucket in the market, calculate model probability.
    Compare to market price to find edge.
    
    Returns sorted list by edge (highest first).
    
    Each result contains:
    - market_id: Polymarket market ID
    - bucket: string representation (e.g., "20-22°C")
    - prob_model: P(temp in bucket) from model
    - price_market: ask price from orderbook
    - edge_brut: prob_model - price_market
    - edge_net: edge after estimated fees
    - spread: bid-ask spread
    - outcome: original outcome dict
    """
    results = []
    
    for outcome in outcomes:
        low, high = outcome["range"]
        unit = outcome.get("unit", "C")
        
        # Calculate model probability for this bucket
        prob = calculate_bucket_prob(forecast_temp, sigma, low, high)
        
        # Get market price (use ask for conservative entry)
        ask = float(outcome.get("ask", outcome.get("price", 0.5)))
        bid = float(outcome.get("bid", ask))
        spread = abs(ask - bid) if ask > bid else 0.0
        
        # Calculate edge (model vs market)
        edge_brut = prob - ask
        
        # Net edge after estimated fees (1% taker fee + slippage)
        estimated_fee = 0.01  # 1% conservative
        edge_net = edge_brut - estimated_fee
        
        bucket_str = f"{low}-{high}{unit}" if low != high else f"{low}{unit}"
        
        results.append({
            "market_id": outcome.get("market_id", ""),
            "bucket": bucket_str,
            "prob_model": prob,
            "price_market": ask,
            "bid": bid,
            "edge_brut": edge_brut,
            "edge_net": edge_net,
            "spread": spread,
            "volume": float(outcome.get("volume", 0)),
            "outcome": outcome
        })
    
    # Sort by edge (highest first)
    return sorted(results, key=lambda x: x["edge_brut"], reverse=True)


def find_best_edge(results: List[Dict], min_edge: float = 0.015) -> Optional[Dict]:
    """
    Find the best opportunity from calculated bucket probabilities.
    
    Returns the first bucket with edge > min_edge, or None.
    """
    for r in results:
        if r["edge_brut"] > min_edge:
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
