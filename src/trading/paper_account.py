"""
Paper trading account management with realistic simulation.
"""
from __future__ import annotations
import json
from pathlib import Path
from dataclasses import dataclass, asdict

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
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.stats = self._load()

    def _load(self) -> PaperStats:
        if self.file_path.exists():
            try:
                data = json.loads(self.file_path.read_text(encoding="utf-8"))
                valid_fields = PaperStats.__dataclass_fields__
                filtered = {key: value for key, value in data.items() if key in valid_fields}
                return PaperStats(**filtered)
            except (Exception,):
                pass
        return PaperStats()

    def save(self):
        """Persist stats to disk."""
        self.file_path.write_text(
            json.dumps(asdict(self.stats), indent=2, ensure_ascii=False),
            encoding="utf-8"
        )

    def get_state(self) -> PaperStats:
        """Return current paper account stats."""
        return self.stats

    def record_trade(self, cost: float):
        """Record a new paper trade entry and lock stake plus entry friction."""
        if cost <= 0:
            raise ValueError("paper trade cost must be positive")

        self.stats.total_trades += 1
        
        # Apply entry friction (fee + slippage)
        entry_friction = cost * (self.FEE_RATE + self.SLIPPAGE_RATE)
        total_deduction = cost + entry_friction

        self.stats.total_fees_paid += entry_friction
        self.stats.total_pnl -= entry_friction
        self.stats.balance = round(self.stats.balance - total_deduction, 2)
        
        self.save()

    def record_result(self, won: bool, pnl: float, cost: float):
        """Record settlement of a paper trade.

        ``pnl`` is the resolver-calculated net result relative to stake. The
        stake was already locked by ``record_trade``, so settlement returns
        ``cost + pnl`` to the account.
        """
        if cost <= 0:
            raise ValueError("paper trade cost must be positive")

        settlement_cashflow = cost + pnl
        
        if won:
            self.stats.wins += 1
        else:
            self.stats.losses += 1
        
        self.stats.total_pnl += pnl
        self.stats.balance = round(self.stats.balance + settlement_cashflow, 2)
        self.stats.peak_balance = max(self.stats.peak_balance, self.stats.balance)
        
        if self.stats.peak_balance > 0:
            dd = (self.stats.peak_balance - self.stats.balance) / self.stats.peak_balance
            self.stats.drawdown = max(self.stats.drawdown, dd)
        
        self.save()

    def get_report(self) -> str:
        """Return a formatted report of paper performance."""
        settled = self.stats.wins + self.stats.losses
        wr = (self.stats.wins / settled * 100) if settled > 0 else 0
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

# Audit: Includes fee and slippage awareness
