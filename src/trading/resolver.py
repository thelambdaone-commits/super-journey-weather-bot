"""
Market resolution logic.
"""
from __future__ import annotations
import time
from datetime import datetime, timezone
from .polymarket import check_market_resolved
from ..weather.apis import get_actual_temp
from ..weather.locations import LOCATIONS

class MarketResolver:
    """Logic for resolving markets and calculating PnL."""
    
    def __init__(self, engine):
        self.engine = engine

    def resolve_market(self, market, balance: float):
        """Resolve a single market (live and paper)."""
        # 1. Check if we have anything to resolve
        if not market.position and not market.paper_position:
            return balance, None, None

        market_id = (market.position or market.paper_position)["market_id"]
        won = check_market_resolved(market_id)
        if won is None:
            return balance, None, None

        # Fetch actual temperature once
        if market.city and market.date and market.actual_temp is None:
            actual = get_actual_temp(market.city, market.date, self.engine.config.vc_key)
            if actual is not None:
                market.actual_temp = actual

        # 2. Resolve Live Position
        pnl = None
        if market.position and market.position.get("status") == "open":
            pos = market.position
            price, size, shares = pos["entry_price"], pos["cost"], pos["shares"]
            pnl = round(shares * (1 - price), 2) if won else round(-size, 2)
            balance = balance + size + pnl
            pos.update({
                "exit_price": 1.0 if won else 0.0,
                "pnl": pnl,
                "close_reason": "resolved",
                "closed_at": datetime.now(timezone.utc).isoformat(),
                "status": "closed"
            })
            market.pnl = pnl
            market.status = "resolved"
            market.resolved_outcome = "win" if won else "loss"
            
            self.engine.feedback_recorder.record_resolution(
                market=market,
                location=LOCATIONS[market.city],
                modes=self.engine.modes,
                pos=pos,
                outcome=market.resolved_outcome,
            )

        # 3. Resolve Paper Position
        paper_pnl = None
        if market.paper_position and market.paper_position.get("status") == "open":
            pos = market.paper_position
            price, size, shares = pos["entry_price"], pos["cost"], pos["shares"]
            paper_pnl = round(shares * (1 - price), 2) if won else round(-size, 2)
            
            pos.update({
                "exit_price": 1.0 if won else 0.0,
                "pnl": paper_pnl,
                "close_reason": "resolved",
                "closed_at": datetime.now(timezone.utc).isoformat(),
                "status": "closed"
            })
            # Update separate paper account
            self.engine.paper_account.record_result(won, paper_pnl)
            
            # If no live position, update market status based on paper
            if not market.position:
                market.status = "resolved"
                market.resolved_outcome = "win" if won else "loss"

        return balance, won, pnl

    def force_resolve_all(self) -> int:
        """Force resolve all open markets."""
        resolved = 0
        for market in self.engine.storage.load_all_markets():
            if market.status == "resolved":
                continue
            if not market.position and not market.paper_position:
                continue

            # Load state for balance
            state = self.engine.storage.load_state()
            new_balance, won, pnl = self.resolve_market(market, state.balance)
            if won is None:
                continue

            state.balance = new_balance
            if won: state.wins += 1
            else: state.losses += 1
            self.engine.storage.save_state(state)

            unit = "°F" if market.unit == "F" else "°C"
            # Prefer position if available, else paper
            pos = market.position or market.paper_position
            bucket = f"{pos['bucket_low']}-{pos['bucket_high']}{unit}"
            temp = f"{market.actual_temp}{unit}" if market.actual_temp is not None else "N/A"
            
            # Use specific PnL for notification (prioritize live)
            display_pnl = pnl if pnl is not None else (market.paper_position.get("pnl") if market.paper_position else 0)

            if won:
                self.engine.feedback.notify_trade_win(market.city_name, market.date, bucket, display_pnl, temp, state.balance)
            else:
                self.engine.feedback.notify_trade_loss(market.city_name, market.date, bucket, display_pnl, state.balance)

            self.engine.storage.save_market(market)
            self.engine.emit(f"[{'WIN' if won else 'LOSS'}] {market.city_name} {market.date} | {display_pnl:+.2f}")
            resolved += 1
            time.sleep(0.5)
        return resolved
