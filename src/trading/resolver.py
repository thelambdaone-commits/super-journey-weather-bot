"""
Market resolution logic with real-time actual polling.
"""
from __future__ import annotations
import time
from ..notifications.telegram_control_center import send_trust_update
from ..notifications.desk_metrics import log_event
from datetime import datetime, timezone
from ..utils.feature_flags import is_enabled
from ..settlement.station_map import get_station_info
from ..weather.apis import get_actual_temp
from ..weather.locations import LOCATIONS
from .polymarket import check_market_resolved

TRADING_FEE_PERCENT = 0.01  # 1% conservative estimate for taker fees/slippage

class MarketResolver:
    """Logic for resolving markets and calculating PnL with real-time actuals."""
    
    def __init__(self, engine):
        self.engine = engine
        self._last_poll_cache = {}
    
    def poll_actual(self, city_slug: str, date_str: str) -> float | None:
        """Poll actual temperature with caching."""
        cache_key = f"{city_slug}:{date_str}"
        if cache_key in self._last_poll_cache:
            return self._last_poll_cache[cache_key]
        
        station = "GENERIC"
        if is_enabled("V3_STATION_PRECISION"):
            station_info = get_station_info(city_slug)
            station = station_info["code"]
        
        actual = get_actual_temp(city_slug, date_str, station=station)
        if actual is not None:
            self._last_poll_cache[cache_key] = actual
        return actual

    @staticmethod
    def _outcome_from_actual(pos: dict, actual: float | None) -> bool | None:
        """Return whether the traded bucket matched the final actual."""
        if actual is None:
            return None
        low = pos.get("bucket_low")
        high = pos.get("bucket_high")
        if low is None or high is None:
            return None
        return float(low) <= float(actual) <= float(high)

    def finalize_closed_position(self, market) -> tuple[bool | None, float | None]:
        """Mark already-closed positions resolved once the final actual is known.

        Stops and manual closes already have realized PnL, but they still need a
        final weather outcome so feedback rows can teach the model whether the
        original forecast bucket was right.
        """
        if market.status == "resolved":
            return None, None

        pos = None
        for candidate in (market.position, market.paper_position):
            if candidate and candidate.get("status") == "closed":
                pos = candidate
                break
        if not pos:
            return None, None

        won = self._outcome_from_actual(pos, market.actual_temp)
        if won is None:
            return None, None

        pnl = pos.get("pnl")
        market.pnl = pnl
        if market.status != "resolved":
            market.status = "resolved"
            market.resolved_at = datetime.now(timezone.utc).isoformat()
        market.resolved_outcome = "win" if won else "loss"

        if hasattr(self.engine, "feedback_recorder") and market.city in LOCATIONS:
            self.engine.feedback_recorder.record_resolution(
                market=market,
                location=LOCATIONS[market.city],
                modes=self.engine.modes,
                pos=pos,
                outcome=market.resolved_outcome,
            )

        return won, pnl

    def _resolve_surebet_position(self, market, balance: float):
        """Resolve an open multi-leg surebet from actual weather."""
        pos = None
        account = None
        if market.position and market.position.get("status") == "open" and market.position.get("type") == "surebet":
            pos = market.position
            account = "live"
        elif (
            market.paper_position
            and market.paper_position.get("status") == "open"
            and market.paper_position.get("type") == "surebet"
        ):
            pos = market.paper_position
            account = "paper"

        if not pos:
            return balance, None, None

        if market.city and market.date and market.actual_temp is None:
            actual = get_actual_temp(market.city, market.date)
            if actual is not None:
                market.actual_temp = actual
        if market.actual_temp is None:
            return balance, None, None

        winning_leg = None
        for leg in pos.get("legs", []):
            if float(leg["bucket_low"]) <= float(market.actual_temp) <= float(leg["bucket_high"]):
                winning_leg = leg
                break

        cost = float(pos.get("cost", 0.0))
        payout = float(winning_leg.get("shares", 0.0)) if winning_leg else 0.0
        fee = cost * TRADING_FEE_PERCENT
        pnl = round(payout - cost - fee, 2)
        won = winning_leg is not None and pnl >= 0

        pos.update({
            "winning_market_id": winning_leg.get("market_id") if winning_leg else None,
            "exit_price": 1.0 if winning_leg else 0.0,
            "pnl": pnl,
            "close_reason": "surebet_resolved",
            "closed_at": datetime.now(timezone.utc).isoformat(),
            "status": "closed",
        })
        market.pnl = pnl
        if market.status != "resolved":
            market.status = "resolved"
            market.resolved_at = datetime.now(timezone.utc).isoformat()
        market.resolved_outcome = "win" if won else "loss"

        if account == "live":
            balance = balance + cost + pnl
        else:
            market_info = {
                "city": market.city_name,
                "market_id": pos.get("market_id") or getattr(market, "market_id", "unknown"),
                "odds": market.paper_position.get("entry_price", 0.0) if market.paper_position else 0.0,
            }
            try:
                self.engine.paper_account.record_result(
                    won, pnl, cost=cost, market_info=market_info
                )
            except TypeError:
                self.engine.paper_account.record_result(won, pnl, cost=cost)
            # SYNC: Update state.balance to match paper_account after resolution
            if hasattr(self.engine, "storage") and hasattr(self.engine.paper_account, "get_state"):
                state = self.engine.storage.load_state()
                state.balance = self.engine.paper_account.get_state().balance
                self.engine.storage.save_state(state)

        if hasattr(self.engine, "feedback_recorder") and market.city in LOCATIONS:
            self.engine.feedback_recorder.record_resolution(
                market=market,
                location=LOCATIONS[market.city],
                modes=self.engine.modes,
                pos=pos,
                outcome=market.resolved_outcome,
            )

        return balance, won, pnl
    
    def auto_resolve_pending(self) -> dict:
        """Auto-resolve all pending markets using real-time actuals and update state."""
        resolved_count = 0
        results = {"resolved": [], "failed": [], "pending": []}
        state = self.engine.storage.load_state()
        initial_balance = state.balance
        
        for market in self.engine.storage.load_all_markets():
            if market.status == "resolved":
                continue
            if not market.position and not market.paper_position:
                continue
            
            # Poll actual temperature
            if market.city and market.date:
                actual = self.poll_actual(market.city, market.date)
                if actual is not None:
                    market.actual_temp = actual
                    # Full resolution
                    had_live_open = bool(market.position and market.position.get("status") == "open")
                    new_balance, won, pnl = self.resolve_market(market, state.balance)
                    if won is not None:
                        if had_live_open:
                            state.balance = new_balance
                            if won: state.wins += 1
                            else: state.losses += 1
                        
                        # Trust Engine Update
                        resolved_pos = market.position or market.paper_position or {}
                        display_pnl = pnl if pnl is not None else resolved_pos.get("pnl", 0.0)
                        cost = resolved_pos.get("cost") or 0.0
                        result = "WIN" if won else "LOSS"
                        pnl_pct = (display_pnl / cost) * 100 if cost else 0
                        send_trust_update(market.city, f"{market.date}", result, pnl_pct)

                        # Desk Pro Resolution Logging
                        log_event(
                            "trade_resolved",
                            city=market.city,
                            confidence=resolved_pos.get("ml", {}).get("tier", "MEDIUM"),
                            setup=resolved_pos.get("setup", "divergence"),
                            net_pnl_pct=pnl_pct,
                            fees_pct=0.2, # Static placeholder
                            slippage_pct=0.1, # Static placeholder
                            realized_edge_pct=pnl_pct + 0.3, # Approx realized vs prediction
                        )

                        resolved_count += 1
                        results["resolved"].append({
                            "city": market.city,
                            "date": market.date,
                            "actual": actual,
                            "won": won,
                            "pnl": display_pnl
                        })
                    else:
                        won, pnl = self.finalize_closed_position(market)
                        if won is not None:
                            resolved_pos = market.position or market.paper_position or {}
                            resolved_count += 1
                            results["resolved"].append({
                                "city": market.city,
                                "date": market.date,
                                "actual": actual,
                                "won": won,
                                "pnl": pnl if pnl is not None else resolved_pos.get("pnl"),
                                "close_reason": resolved_pos.get("close_reason"),
                            })
                    self.engine.storage.save_market(market)
                else:
                    results["pending"].append({
                        "city": market.city,
                        "date": market.date
                    })
            else:
                results["failed"].append({
                    "city": market.city,
                    "reason": "missing_data"
                })
        
        if resolved_count > 0:
            state.peak_balance = max(state.peak_balance, state.balance)
            self.engine.storage.save_state(state)
            
        results["total"] = resolved_count
        results["pnl"] = state.balance - initial_balance
        return results

    def get_recent_errors(self, days: int = 7) -> dict:
        """Get recent forecast errors for edge adjustment."""
        from pathlib import Path
        from ..data.loader import load_rows
        
        errors_by_source = {}
        
        # Load dataset rows
        dataset_path = Path(self.engine.config.data_dir) / "dataset_rows.jsonl"
        rows = load_rows(dataset_path)
        
        for row in rows:
            if row.actual_temp is None:
                continue
            if row.forecast_temp is None:
                continue
            
            source = row.forecast_source or "unknown"
            err = row.forecast_temp - row.actual_temp
            if source not in errors_by_source:
                errors_by_source[source] = []
            errors_by_source[source].append(err)
        
        # Calculate stats
        result = {}
        import numpy as np
        for source, errors in errors_by_source.items():
            arr = np.array(errors)
            result[source] = {
                "mean": float(np.mean(arr)),
                "std": float(np.std(arr)),
                "mae": float(np.mean(np.abs(arr))),
                "n": len(errors)
            }
        
        return result

    def should_retrain(self, min_resolutions: int = 10) -> tuple[bool, str]:
        """Check if system should retrain based on recent resolutions."""
        recent_count = 0
        for market in self.engine.storage.load_all_markets():
            if market.status == "resolved" and market.actual_temp:
                recent_count += 1
        
        if recent_count >= min_resolutions:
            return True, f"{recent_count} résolutions récentes"
        
        return False, f"Pas assez: {recent_count}/{min_resolutions}"

    def check_and_trigger_retrain(self, min_resolutions: int = 10) -> tuple[bool, str]:
        """Check if should retrain and optionally trigger."""
        should_train, reason = self.should_retrain(min_resolutions)
        
        if should_train:
            # Get recent errors for adjustment
            errors = self.get_recent_errors(days=7)
            ecmwf_bias = errors.get('ecmwf', {}).get('mean', 0)
            hrrr_bias = errors.get('hrrr', {}).get('mean', 0)
            
            return True, f"{reason} | ECMWF: {ecmwf_bias:+.2f}°C | HRRR: {hrrr_bias:+.2f}°C"
        
        return False, reason

    def resolve_market(self, market, balance: float):
        """Resolve a single market (live and paper) using actual temperature."""
        new_balance, won, pnl = self._resolve_surebet_position(market, balance)
        if won is not None:
            return new_balance, won, pnl

        # 1. Check if we have anything to resolve
        live_open = bool(market.position and market.position.get("status") == "open")
        paper_open = bool(market.paper_position and market.paper_position.get("status") in ("open", "paper"))
        if not live_open and not paper_open:
            return balance, None, None

        # 2. Fetch actual temperature if not already available
        if market.city and market.date and market.actual_temp is None:
            actual = get_actual_temp(market.city, market.date)
            if actual is not None:
                market.actual_temp = actual

        # 3. Determine outcome per open position. A closed historical position
        # must not drive settlement for a still-open paper/live position.
        live_won = self._resolve_position_outcome(market.position, market.actual_temp) if live_open else None
        paper_won = self._resolve_position_outcome(market.paper_position, market.actual_temp) if paper_open else None

        if live_won is None and paper_won is None:
            return balance, None, None

        # 4. Resolve Live Position
        pnl = None
        if live_open and live_won is not None:
            pos = market.position
            price, size, shares = pos["entry_price"], pos["cost"], pos["shares"]
            fee = size * TRADING_FEE_PERCENT
            pnl = round(shares * (1 - price) - fee, 2) if live_won else round(-size - fee, 2)
            balance = balance + size + pnl
            pos.update({
                "exit_price": 1.0 if live_won else 0.0,
                "pnl": pnl,
                "close_reason": "resolved",
                "closed_at": datetime.now(timezone.utc).isoformat(),
                "status": "closed"
            })
            market.pnl = pnl
            market.resolved_outcome = "win" if live_won else "loss"

            self.engine.feedback_recorder.record_resolution(
                market=market,
                location=LOCATIONS[market.city],
                modes=self.engine.modes,
                pos=pos,
                outcome=market.resolved_outcome,
            )

        # 5. Resolve Paper Position
        paper_pnl = None
        if paper_open and paper_won is not None:
            pos = market.paper_position
            price, size, shares = pos["entry_price"], pos["cost"], pos["shares"]
            fee = size * TRADING_FEE_PERCENT
            paper_pnl = round(shares * (1 - price) - fee, 2) if paper_won else round(-size - fee, 2)

            pos.update({
                "exit_price": 1.0 if paper_won else 0.0,
                "pnl": paper_pnl,
                "close_reason": "resolved",
                "closed_at": datetime.now(timezone.utc).isoformat(),
                "status": "closed"
            })
            # Update separate paper account
            market_info = {
                "city": market.city_name,
                "market_id": pos.get("market_id") or getattr(market, "market_id", "unknown"),
                "odds": market.paper_position.get("entry_price", 0.0),
            }
            try:
                self.engine.paper_account.record_result(
                    paper_won, paper_pnl, cost=size, market_info=market_info
                )
            except TypeError:
                self.engine.paper_account.record_result(paper_won, paper_pnl, cost=size)
            # SYNC: Update state.balance to match paper_account after resolution
            if hasattr(self.engine, "storage") and hasattr(self.engine.paper_account, "get_state"):
                state = self.engine.storage.load_state()
                state.balance = self.engine.paper_account.get_state().balance
                self.engine.storage.save_state(state)

            if not live_open:
                market.pnl = paper_pnl
                market.resolved_outcome = "win" if paper_won else "loss"
                market.paper_state = None

        if (not live_open or live_won is not None) and (not paper_open or paper_won is not None):
            if market.status != "resolved":
                market.status = "resolved"
                market.resolved_at = datetime.now(timezone.utc).isoformat()

        display_won = live_won if live_won is not None else paper_won
        display_pnl = pnl if pnl is not None else paper_pnl
        return balance, display_won, display_pnl

    def _resolve_position_outcome(self, pos: dict | None, actual_temp: float | None) -> bool | None:
        """Resolve one open position from actual weather, then Polymarket as fallback."""
        if not pos:
            return None
        if actual_temp is not None:
            return self._outcome_from_actual(pos, actual_temp)
        return check_market_resolved(pos["market_id"])

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
            had_live_open = bool(market.position and market.position.get("status") == "open")
            new_balance, won, pnl = self.resolve_market(market, state.balance)
            if won is None:
                continue

            if had_live_open:
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

# Audit: Includes fee and slippage awareness
