"""
Performance Engine - Quantitative analytics for WeatherBot.
Tracks net PnL, fees, slippage, and risk metrics.
"""
from __future__ import annotations
import numpy as np
import polars as pl
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class PerformanceMetrics:
    total_trades: int
    win_rate: float
    net_pnl: float
    total_fees: float
    total_slippage: float
    profit_factor: float
    max_drawdown: float
    sharpe_ratio: float
    avg_win: float
    avg_loss: float
    expectancy: float

class PerformanceEngine:
    """
    Calculates key performance indicators from trade history.
    """
    def __init__(self, risk_free_rate: float = 0.0):
        self.risk_free_rate = risk_free_rate

    def calculate_metrics(self, trades: List[dict]) -> Optional[PerformanceMetrics]:
        if not trades:
            return None
            
        df = pl.DataFrame(trades)
        if df.is_empty():
            return None

        # Filter resolved trades
        resolved = df.filter(pl.col("status") == "resolved")
        if resolved.is_empty():
            return None

        # Basic stats
        pnl_series = resolved["pnl"].to_numpy()
        wins = pnl_series[pnl_series > 0]
        losses = pnl_series[pnl_series <= 0]
        
        total_trades = len(pnl_series)
        win_rate = len(wins) / total_trades if total_trades > 0 else 0.0
        net_pnl = float(pnl_series.sum())
        
        total_fees = float(resolved["fees"].sum()) if "fees" in resolved.columns else 0.0
        # Slippage is estimated as the difference between best_ask and realized_avg_price
        total_slippage = 0.0
        if "best_ask" in resolved.columns and "entry_price" in resolved.columns:
            total_slippage = float((resolved["entry_price"] - resolved["best_ask"]).sum())

        # Profit Factor
        gross_profit = float(wins.sum()) if len(wins) > 0 else 0.0
        gross_loss = abs(float(losses.sum())) if len(losses) > 0 else 0.0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else (gross_profit if gross_profit > 0 else 1.0)

        # Max Drawdown
        cum_pnl = np.cumsum(pnl_series)
        peak = np.maximum.accumulate(cum_pnl)
        drawdown = peak - cum_pnl
        max_dd = float(np.max(drawdown)) if len(drawdown) > 0 else 0.0

        # Sharpe Ratio (Assuming daily samples for simplicity)
        returns = pnl_series # In a real fund, this would be % returns per period
        if len(returns) > 1 and np.std(returns) > 0:
            sharpe = (np.mean(returns) - self.risk_free_rate) / np.std(returns) * np.sqrt(252) # Annualized
        else:
            sharpe = 0.0

        avg_win = float(np.mean(wins)) if len(wins) > 0 else 0.0
        avg_loss = float(np.mean(losses)) if len(losses) > 0 else 0.0
        expectancy = (win_rate * avg_win) - ((1 - win_rate) * abs(avg_loss))

        return PerformanceMetrics(
            total_trades=total_trades,
            win_rate=win_rate,
            net_pnl=net_pnl,
            total_fees=total_fees,
            total_slippage=total_slippage,
            profit_factor=profit_factor,
            max_drawdown=max_dd,
            sharpe_ratio=sharpe,
            avg_win=avg_win,
            avg_loss=avg_loss,
            expectancy=expectancy
        )

# Audit: Includes fee and slippage awareness
