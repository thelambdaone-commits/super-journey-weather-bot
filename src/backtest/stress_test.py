"""
Fat-Tail Stress Testing and Scenario Analysis.
"""
from __future__ import annotations
import random
from typing import List, Dict, Any

def run_fat_tail_stress(trades: List[dict]) -> Dict[str, Any]:
    """
    Simulate 'Fat Tail' scenarios on a set of trades.
    """
    if not trades:
        return {}

    # Scenario 1: Correlation Spike (All trades in a region fail together)
    # Scenario 2: Error 4-sigma (MAE x 4)
    # Scenario 3: Liquidity Gap (Slippage increases 5x)
    
    scenarios = {}
    
    # 1. 5x Slippage Spike
    pnl_stressed = [t.get("pnl", 0) - (abs(t.get("cost", 0)) * 0.075) for t in trades] # 1.5% -> 7.5%
    scenarios["liquidity_shock"] = {
        "pnl_impact": sum(pnl_stressed) - sum(t.get("pnl", 0) for t in trades),
        "survival": sum(pnl_stressed) > -500.0 # Arbitrary threshold
    }
    
    # 2. Black Swan (Worst 10% of trades become 2x worse)
    worst_trades = sorted(trades, key=lambda x: x.get("pnl", 0))[:max(1, len(trades)//10)]
    impact = sum(t.get("pnl", 0) for t in worst_trades)
    scenarios["black_swan"] = {
        "pnl_impact": impact, # Additional loss
        "survival": (sum(t.get("pnl", 0) for t in trades) + impact) > -1000.0
    }

    return scenarios

def format_stress_report(scenarios: Dict[str, Any]) -> str:
    """Format stress test results."""
    if not scenarios:
        return "No trades to stress test."
        
    lines = ["🔥 *FAT-TAIL STRESS TEST*"]
    for name, data in scenarios.items():
        status = "✅ PASSED" if data["survival"] else "❌ FAILED"
        lines.append(f"- {name.replace('_', ' ').title()}: {status} (Impact: `${data['pnl_impact']:.2f}`)")
    return "\n".join(lines)
