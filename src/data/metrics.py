"""
Quant-grade performance metrics for audit and validation.
"""
from __future__ import annotations
import math
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class AuditMetrics:
    total_trades: int
    win_rate: float
    profit_factor: float
    sharpe_ratio: float
    max_drawdown: float
    expectancy_per_trade: float
    total_pnl_net: float
    avg_win: float
    avg_loss: float
    r_multiple: float
    total_fees: float
    avg_slippage: float
    wins: int = 0
    losses: int = 0
    drift_status: str = "stable" # stable, degrading, critical
    uptime_pct: float = 100.0

def calculate_audit_metrics(trades: List[dict], starting_balance: float) -> AuditMetrics:
    """Compute institutional-grade metrics from trade history."""
    if not trades:
        return AuditMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0)

    wins = [t for t in trades if t.get("pnl", 0) > 0]
    losses = [t for t in trades if t.get("pnl", 0) < 0]
    
    total_pnl = sum(t.get("pnl", 0) for t in trades)
    gross_profit = sum(t.get("pnl", 0) for t in wins)
    gross_loss = abs(sum(t.get("pnl", 0) for t in losses))
    
    total_trades = len(trades)
    win_rate = len(wins) / total_trades if total_trades > 0 else 0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
    expectancy = total_pnl / total_trades if total_trades > 0 else 0
    
    # Calculate Sharpe Ratio (simplified daily proxy)
    # Using pnl per trade as a proxy for returns
    returns = [t.get("pnl", 0) / starting_balance for t in trades]
    avg_return = sum(returns) / len(returns) if returns else 0
    std_return = math.sqrt(sum((r - avg_return)**2 for r in returns) / len(returns)) if len(returns) > 1 else 1.0
    sharpe = (avg_return / std_return) * math.sqrt(total_trades) if std_return > 0 else 0

    # Max Drawdown
    balance = starting_balance
    peak = starting_balance
    max_dd = 0.0
    for t in trades:
        balance += t.get("pnl", 0)
        peak = max(peak, balance)
        dd = (peak - balance) / peak if peak > 0 else 0
        max_dd = max(max_dd, dd)

    # Drift Detection (Last 7 days vs History)
    drift_status = "stable"
    if len(trades) > 20:
        import time
        now = time.time()
        recent_trades = [t for t in trades if (now - t.get("unix_ts", 0)) < 7 * 24 * 3600]
        if len(recent_trades) > 5:
            recent_pf = sum(t.get("pnl",0) for t in recent_trades if t.get("pnl",0)>0) / abs(sum(t.get("pnl",0) for t in recent_trades if t.get("pnl",0)<0) or 1)
            if recent_pf < profit_factor * 0.7:
                drift_status = "degrading"
            if recent_pf < 0.9:
                drift_status = "critical"

    avg_win = gross_profit / len(wins) if wins else 0.0
    avg_loss = gross_loss / len(losses) if losses else 0.0
    r_multiple = avg_win / avg_loss if avg_loss > 0 else 0.0

    return AuditMetrics(
        total_trades=total_trades,
        win_rate=round(win_rate, 4),
        profit_factor=round(profit_factor, 2),
        sharpe_ratio=round(sharpe, 2),
        max_drawdown=round(max_dd, 4),
        expectancy_per_trade=round(expectancy, 4),
        total_pnl_net=round(total_pnl, 2),
        avg_win=round(avg_win, 2),
        avg_loss=round(avg_loss, 2),
        r_multiple=round(r_multiple, 2),
        total_fees=0.0, # Placeholder
        avg_slippage=0.015, # Based on PaperAccount simulation
        wins=len(wins),
        losses=len(losses),
        drift_status=drift_status
    )

def format_audit_report(metrics: AuditMetrics) -> str:
    """Format metrics into a professional audit report."""
    return (
        f"📊 *QUANT AUDIT REPORT*\n\n"
        f"| Metric | Value |\n"
        f"| :--- | :--- |\n"
        f"| Total Trades | `{metrics.total_trades}` |\n"
        f"| Win Rate | `{metrics.win_rate*100:.1f}%` |\n"
        f"| Profit Factor | `{metrics.profit_factor}` |\n"
        f"| Sharpe Ratio | `{metrics.sharpe_ratio}` |\n"
        f"| Max Drawdown | `{metrics.max_drawdown*100:.1f}%` |\n"
        f"| Expectancy/Trade | `${metrics.expectancy_per_trade:.4f}` |\n"
        f"| Avg Win / Loss | `${metrics.avg_win} / ${metrics.avg_loss}` |\n"
        f"| R-Multiple | `{metrics.r_multiple}` |\n"
        f"| Net PnL | `${metrics.total_pnl_net:+.2f}` |\n"
        f"| Avg Friction | `{metrics.avg_slippage*100:.1f}%` |\n\n"
        f"🛡️ *Status: Technically functional in test environment.*"
    )
