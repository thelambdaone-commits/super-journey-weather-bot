"""
Paper trading account management with realistic simulation.
"""
from __future__ import annotations
import json
from pathlib import Path
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

@dataclass
class PaperStats:
    balance: float = 10000.0
    starting_balance: float = 10000.0
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    total_pnl: float = 0.0
    total_fees_paid: float = 0.0
    peak_balance: float = 10000.0
    drawdown: float = 0.0

class PaperAccount:
    """Manages paper trading state with realistic slippage and fee simulation."""
    
    # Simulation constants
    FEE_RATE = 0.005      # 0.5% transaction fee
    SLIPPAGE_RATE = 0.015 # 1.5% average slippage on entry/exit
    
    def __init__(self, data_dir: str = "data"):
        self.file_path = Path(data_dir) / "paper_account.json"
        self.stats = self._load()

    def _load(self) -> PaperStats:
        if self.file_path.exists():
            try:
                data = json.loads(self.file_path.read_text(encoding="utf-8"))
                return PaperStats(**data)
            except Exception:
                pass
        return PaperStats()

    def save(self):
        """Persist stats to disk."""
        self.file_path.write_text(
            json.dumps(asdict(self.stats), indent=2, ensure_ascii=False),
            encoding="utf-8"
        )

    def record_trade(self, cost: float):
        """Record a new paper trade entry and deduct simulated fees."""
        self.stats.total_trades += 1
        
        # Apply entry friction (fee + slippage)
        entry_friction = cost * (self.FEE_RATE + self.SLIPPAGE_RATE)
        self.stats.total_fees_paid += entry_friction
        self.stats.total_pnl -= entry_friction
        self.stats.balance = round(self.stats.balance - entry_friction, 2)
        
        self.save()

    def record_result(self, won: bool, pnl: float):
        """Record the result of a paper trade, applying exit friction."""
        # Friction on exit is usually lower for binary options (resolution is direct)
        # but slippage might have occurred if we sold before resolution.
        # Since we mostly resolve at settlement, we only apply a small settlement fee simulation if applicable.
        # However, to be conservative, let's apply a 0.5% "settlement/latency" cost.
        exit_friction = abs(pnl) * 0.005 
        
        actual_pnl = pnl - exit_friction
        
        if won:
            self.stats.wins += 1
        else:
            self.stats.losses += 1
        
        self.stats.total_fees_paid += exit_friction
        self.stats.total_pnl += actual_pnl
        self.stats.balance = round(self.stats.balance + actual_pnl, 2)
        self.stats.peak_balance = max(self.stats.peak_balance, self.stats.balance)
        
        if self.stats.peak_balance > 0:
            dd = (self.stats.peak_balance - self.stats.balance) / self.stats.peak_balance
            self.stats.drawdown = max(self.stats.drawdown, dd)
        
        self.save()

    def get_report(self) -> str:
        """Return a formatted report of paper performance."""
        wr = (self.stats.wins / self.stats.total_trades * 100) if self.stats.total_trades > 0 else 0
        roi = (self.stats.total_pnl / self.stats.starting_balance * 100)
        pf = (self.stats.wins / max(self.stats.losses, 1)) # Simplified Profit Factor for now
        
        return (
            f"📈 *RAPPORT PAPER TRADING (Réaliste)*\n\n"
            f"💰 Solde: `${self.stats.balance:,.2f}`\n"
            f"📊 PnL Net: `{self.stats.total_pnl:+.2f}` ({roi:+.1f}%)\n"
            f"💸 Frais/Slippage: `${self.stats.total_fees_paid:.2f}`\n"
            f"🔄 Trades: `{self.stats.total_trades}` | W: `{self.stats.wins}` | L: `{self.stats.losses}`\n"
            f"🎯 Win Rate: `{wr:.1f}%` | PF: `{pf:.2f}`\n"
            f"📉 Max Drawdown: `{self.stats.drawdown*100:.1f}%`"
        )
