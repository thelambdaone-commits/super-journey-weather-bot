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
    total_gains: float = 0.0
    total_losses: float = 0.0
    total_fees_paid: float = 0.0
    peak_balance: float = 10000.0
    peak_equity: float = 10000.0
    drawdown: float = 0.0
    locked_in_positions: float = 0.0  # Stake locked in open positions
    closed_trades: int = 0
    open_trades: int = 0
    cash_pnl: float = 0.0
    equity: float = 10000.0
    accounting_complete: bool = False
    accounting_note: str = ""

class PaperAccount:
    """Manages paper trading state with realistic slippage and fee simulation."""
    
    # Simulation constants
    FEE_RATE = 0.005      # 0.5% transaction fee
    SLIPPAGE_RATE = 0.015 # 1.5% average slippage on entry/exit
    
    def __init__(self, data_dir: str = "data"):
        self.file_path = Path(data_dir) / "paper_account.json"
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.stats = self._load()
        
        # Recalculate gains/losses from history
        self.recalc_gains_losses()

    def _load(self) -> PaperStats:
        if self.file_path.exists():
            try:
                data = json.loads(self.file_path.read_text(encoding="utf-8"))
                valid_fields = PaperStats.__dataclass_fields__
                filtered = {key: value for key, value in data.items() if key in valid_fields}
                stats = PaperStats(**filtered)
                self._refresh_equity_drawdown(stats)
                return stats
            except (Exception,):
                pass
        return PaperStats()

    @staticmethod
    def _refresh_equity_drawdown(stats: PaperStats) -> None:
        """Backfill equity-based drawdown for accounts created before peak_equity existed."""
        equity = stats.balance + stats.locked_in_positions
        stats.equity = round(equity, 2)
        stats.cash_pnl = round(stats.balance - stats.starting_balance, 2)
        stats.peak_equity = max(stats.peak_equity, equity)
        if stats.peak_equity > 0:
            stats.drawdown = max(stats.drawdown, (stats.peak_equity - equity) / stats.peak_equity)

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
        # Lock stake in open positions for equity calculation
        self.stats.locked_in_positions += cost
        # DO NOT update total_pnl here - only update on resolution
        self.stats.balance = round(self.stats.balance - total_deduction, 2)

        # Update peak equity (cash + locked positions)
        equity = self.get_equity()
        self.stats.peak_equity = max(self.stats.peak_equity, equity)
        if self.stats.peak_equity > 0:
            self.stats.drawdown = (self.stats.peak_equity - equity) / self.stats.peak_equity

        self.save()

    def record_result(self, won: bool, pnl: float, cost: float, market_info: dict = None):
        """Record settlement of a paper trade.

        ``pnl`` is the resolver-calculated net result relative to stake. The
        stake was already locked by ``record_trade``, so settlement returns
        ``cost + pnl`` to the account.
        """
        if cost <= 0:
            raise ValueError("paper trade cost must be positive")

        # Unlock stake from open positions
        self.stats.locked_in_positions -= cost
        if self.stats.locked_in_positions < 0:
            self.stats.locked_in_positions = 0.0

        settlement_cashflow = cost + pnl

        if pnl > 0:
            self.stats.wins += 1
            self.stats.total_gains += pnl
        elif pnl < 0:
            self.stats.losses += 1
            self.stats.total_losses += abs(pnl)

        self.stats.total_pnl += pnl
        self.stats.balance = round(self.stats.balance + settlement_cashflow, 2)

        # Save to trade history
        self._save_trade_history(won, pnl, cost, market_info)

        # Update peak equity (cash + locked positions)
        equity = self.get_equity()
        self.stats.peak_equity = max(self.stats.peak_equity, equity)
        self.stats.peak_balance = max(self.stats.peak_balance, self.stats.balance)

        if self.stats.peak_equity > 0:
            self.stats.drawdown = max(self.stats.drawdown, (self.stats.peak_equity - equity) / self.stats.peak_equity)

        self.save()

    def get_equity(self) -> float:
        """
        Calculate total equity = cash balance + value of open positions.
        For paper trading, open positions are valued at cost (conservative).
        """
        return round(self.stats.balance + self.stats.locked_in_positions, 2)

    def check_coherence(self) -> dict:
        """
        Verify PnL coherence:
        equity = cash_balance + locked_in_positions
        expected_equity = starting_balance + total_pnl - total_fees_paid
        """
        # Recalculate locked positions from actual market files
        self._recalc_locked_positions()
        
        expected_equity = (
            self.stats.starting_balance
            + self.stats.total_pnl
            - self.stats.total_fees_paid
        )
        actual_equity = self.get_equity()
        
        return {
            "expected_equity": round(expected_equity, 2),
            "actual_equity": actual_equity,
            "is_coherent": abs(expected_equity - actual_equity) < 1.0,
            "balance": self.stats.balance,
            "locked_in_positions": self.stats.locked_in_positions,
            "total_pnl": self.stats.total_pnl,
            "total_fees_paid": self.stats.total_fees_paid,
            "starting_balance": self.stats.starting_balance
        }

    def _recalc_locked_positions(self):
        """Recalculate locked positions from market files."""
        markets_dir = self.file_path.parent / "markets"
        if not markets_dir.exists():
            return
        
        total_locked = 0.0
        for market_file in markets_dir.glob("*.json"):
            try:
                data = json.loads(market_file.read_text(encoding="utf-8"))
                # Check paper position
                pp = data.get("paper_position")
                if pp and pp.get("status") in ("open", "paper"):
                    total_locked += float(pp.get("cost", 0.0))
                # Check live position (if not paper mode)
                pos = data.get("position")
                if pos and pos.get("status") == "open":
                    total_locked += float(pos.get("cost", 0.0))
            except (Exception,):
                pass
        
        self.stats.locked_in_positions = round(total_locked, 2)
        self.save()

    def _save_trade_history(self, won: bool, pnl: float, cost: float, market_info: dict = None):
        """Append closed trade to paper_trades.jsonl"""
        import json
        from datetime import datetime, timezone
        
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "won": won,
            "pnl": pnl,
            "stake": cost,
            "mode": "paper"
        }
        
        if market_info:
            record["city"] = market_info.get("city", "unknown")
            record["market_id"] = market_info.get("market_id", "unknown")
            record["odds"] = market_info.get("odds", 0.0)
        
        history_file = self.file_path.parent / "paper_trades.jsonl"
        with open(history_file, "a") as f:
            f.write(json.dumps(record) + "\n")

    def _trade_key(self, trade: dict) -> tuple[str, str, str]:
        """Return a stable de-duplication key for historical paper trades."""
        return (
            str(trade.get("market_id") or "unknown"),
            str(trade.get("city") or "unknown").lower(),
            str(trade.get("date") or ""),
        )

    def _market_trade_records(self) -> tuple[list[dict], list[dict]]:
        """Build detailed closed/open paper records from persisted market files."""
        markets_dir = self.file_path.parent / "markets"
        if not markets_dir.exists():
            return [], []

        closed_records = []
        open_records = []
        for market_file in sorted(markets_dir.glob("*.json")):
            try:
                data = json.loads(market_file.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue

            pos = data.get("paper_position") or {}
            status = pos.get("status")
            if status not in ("closed", "open", "paper"):
                continue

            try:
                stake = float(pos.get("cost") or pos.get("stake") or 0.0)
            except (TypeError, ValueError):
                continue

            record = {
                "timestamp": pos.get("closed_at") or pos.get("opened_at") or data.get("date") or "",
                "stake": stake,
                "mode": "paper",
                "city": data.get("city_name") or data.get("city") or "unknown",
                "date": data.get("date") or "",
                "market_id": pos.get("market_id") or data.get("market_id") or market_file.stem,
                "odds": pos.get("entry_price", 0.0),
                "reconstructed_from_market": True,
            }
            if status == "closed" and pos.get("pnl") is not None:
                try:
                    pnl = float(pos["pnl"])
                except (TypeError, ValueError):
                    continue
                record["pnl"] = pnl
                record["won"] = pnl > 0
                closed_records.append(record)
            elif status in ("open", "paper"):
                record["status"] = status
                open_records.append(record)

        # One market can be reachable under multiple names over time. Keep the
        # latest deterministic record per market/city/date key.
        closed_deduped = {}
        for record in closed_records:
            closed_deduped[self._trade_key(record)] = record
        open_deduped = {}
        for record in open_records:
            open_deduped[self._trade_key(record)] = record
        return list(closed_deduped.values()), list(open_deduped.values())

    def _closed_market_trade_records(self) -> list[dict]:
        """Build detailed closed-trade records from persisted market files."""
        closed_records, _ = self._market_trade_records()
        return closed_records

    def _append_missing_detailed_history(self, records: list[dict], existing_keys: set[tuple[str, str, str]]) -> bool:
        """Append reconstructed detailed records that are missing from history."""
        missing = [record for record in records if self._trade_key(record) not in existing_keys]
        if not missing:
            return False

        history_file = self.file_path.parent / "paper_trades.jsonl"
        with open(history_file, "a", encoding="utf-8") as f:
            for record in missing:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        return True

    def recalc_gains_losses(self):
        """Recalculate total_gains and total_losses from paper_trades.jsonl"""
        history_file = self.file_path.parent / "paper_trades.jsonl"

        total_gains = 0.0
        total_losses = 0.0
        has_detail = False
        has_legacy = False
        legacy_pnl_total = 0.0
        legacy_wins = 0
        legacy_losses = 0
        detail_trades = []
        existing_keys = set()

        if history_file.exists():
            with open(history_file, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        try:
                            trade = json.loads(line)

                            # Check if legacy entry
                            if trade.get("historical_reconstructed") or trade.get("estimated"):
                                has_legacy = True
                                legacy_pnl_total = trade.get("pnl_total", 0.0)
                                legacy_wins = trade.get("wins", 0)
                                legacy_losses = trade.get("losses", 0)
                                continue

                            if "pnl" in trade:
                                existing_keys.add(self._trade_key(trade))
                                detail_trades.append(trade)
                        except (json.JSONDecodeError, TypeError, ValueError):
                            pass

        market_records, open_records = self._market_trade_records()
        if market_records:
            market_wins = sum(1 for trade in market_records if float(trade.get("pnl", 0.0) or 0.0) > 0)
            market_losses = sum(1 for trade in market_records if float(trade.get("pnl", 0.0) or 0.0) < 0)
            covers_legacy = (
                has_legacy
                and legacy_wins > 0
                and legacy_losses > 0
                and market_wins == int(legacy_wins)
                and market_losses == int(legacy_losses)
            )
            if covers_legacy or not detail_trades:
                if self._append_missing_detailed_history(market_records, existing_keys):
                    detail_trades.extend(
                        record for record in market_records if self._trade_key(record) not in existing_keys
                    )

        # Normal entries with pnl. Gross gains/losses are based on the PnL
        # sign, not on the settlement label.
        for trade in detail_trades:
            try:
                pnl = float(trade["pnl"])
            except (TypeError, ValueError):
                continue
            has_detail = True
            if pnl > 0:
                total_gains += pnl
            elif pnl < 0:
                total_losses += abs(pnl)
        
        # If we have only legacy (no details), only split PnL when the split
        # is mathematically certain. Mixed wins/losses need detailed history.
        if has_legacy and not has_detail:
            if legacy_wins > 0 and legacy_losses > 0:
                total_gains = 0.0
                total_losses = 0.0
            elif legacy_pnl_total < 0:
                total_losses = abs(legacy_pnl_total)
                total_gains = 0.0
            else:
                total_gains = legacy_pnl_total
                total_losses = 0.0
        
        self.stats.total_gains = round(total_gains, 2)
        self.stats.total_losses = round(total_losses, 2)
        if has_detail:
            self.stats.total_pnl = round(total_gains - total_losses, 2)
            self.stats.wins = sum(
                1 for trade in detail_trades if float(trade.get("pnl", 0.0) or 0.0) > 0
            )
            self.stats.losses = sum(
                1 for trade in detail_trades if float(trade.get("pnl", 0.0) or 0.0) < 0
            )

        self.stats.closed_trades = self.stats.wins + self.stats.losses
        self.stats.open_trades = len(open_records)
        if open_records:
            self.stats.locked_in_positions = round(
                sum(float(trade.get("stake", 0.0) or 0.0) for trade in open_records), 2
            )

        detailed_trade_count = self.stats.closed_trades + self.stats.open_trades
        has_complete_detailed_count = has_detail and detailed_trade_count > 0
        if has_complete_detailed_count:
            entry_cost = sum(float(trade.get("stake", 0.0) or 0.0) for trade in detail_trades + open_records)
            self.stats.total_fees_paid = round(entry_cost * (self.FEE_RATE + self.SLIPPAGE_RATE), 2)
            self.stats.total_trades = detailed_trade_count
            self.stats.balance = round(
                self.stats.starting_balance
                + self.stats.total_pnl
                - self.stats.total_fees_paid
                - self.stats.locked_in_positions,
                2,
            )
            self.stats.equity = round(self.stats.balance + self.stats.locked_in_positions, 2)
            self.stats.cash_pnl = round(self.stats.balance - self.stats.starting_balance, 2)
            self.stats.peak_equity = max(self.stats.peak_equity, self.stats.equity)
            self.stats.peak_balance = max(self.stats.peak_balance, self.stats.balance)
            if self.stats.peak_equity > 0:
                self.stats.drawdown = max(
                    0.0,
                    (self.stats.peak_equity - self.stats.equity) / self.stats.peak_equity,
                )
            self.stats.accounting_complete = True
            self.stats.accounting_note = "reconciled_from_detailed_market_ledger"
        else:
            self.stats.equity = self.get_equity()
            self.stats.cash_pnl = round(self.stats.balance - self.stats.starting_balance, 2)
            self.stats.accounting_complete = False
            self.stats.accounting_note = "missing_detailed_pnl_history"
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
