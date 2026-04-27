"""
Market scanning and processing logic.
"""
from __future__ import annotations
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, List

from ..weather.apis import get_forecasts
from ..weather.locations import LOCATIONS, MONTHS
from ..weather.math import bucket_prob, in_bucket
from .polymarket import (
    get_polymarket_event,
    get_outcomes,
    hours_to_resolution,
    check_market_resolved,
    refresh_outcome_orderbook,
)
from .resolver import TRADING_FEE_PERCENT
from .trade_builder import build_trade_payload
from ..strategy.filters import should_skip_outcome
from ..strategy.sizing import size_position
from .helpers import (
    log_paper_trade, build_signal_marker, should_emit_marker,
    format_ai_note, format_ml_note, get_ai_trade_context
)
from .polymarket import get_vwap_for_size
from src.notifications.telegram_control_center import send_no_trade
from .types import ScanResult

if TYPE_CHECKING:
    from .engine import TradingEngine

from ..strategy.signal_quality import Signal
from ..utils.feature_flags import is_enabled
from ..data.moat_manager import get_moat
from ..weather.collectors.open_meteo import MultiModelCollector
from .idempotence import get_idempotence_manager

logger = logging.getLogger(__name__)

class MarketScanner:
    """Core logic for scanning markets and identifying opportunities."""
    
    def __init__(self, engine: TradingEngine):
        self.engine = engine
        self.idempotence = get_idempotence_manager()
        self.moat = get_moat()
        self.multi_collector = MultiModelCollector(LOCATIONS)

    def scan_and_update(self) -> ScanResult:
        """Run a full scan cycle."""
        now = datetime.now(timezone.utc)
        state = self.engine.storage.load_state()
        balance = state.balance
        result = ScanResult()
        pending_signals: list[dict] = []

        # --- V3 MOAT & MULTI-MODEL COLLECTION ---
        if is_enabled("V3_DATA_MOAT"):
            try:
                logger.info("[V3] Collecting multi-model forecasts for the Moat...")
                forecasts_df = self.multi_collector.fetch_all_forecasts()
                self.moat.save_forecasts(forecasts_df)
            except (Exception,) as e:
                logger.error(f"V3 Moat collection failed: {e}")

        for city_slug, loc in LOCATIONS.items():
            self.engine.emit(f" -> {loc.name}...")

            try:
                dates = [(now + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(4)]
                snapshots = get_forecasts(city_slug, dates)
                time.sleep(0.3)
            except (ValueError, KeyError) as e:
                logger.error(f"Data mapping error for {city_slug}: {e}")
                continue
            except (Exception,) as e:
                logger.exception(f"Unexpected crash while scanning {city_slug}")
                continue

            for i, date_str in enumerate(dates):
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                event = get_polymarket_event(city_slug, MONTHS[dt.month - 1], dt.day, dt.year)
                if not event:
                    continue
                
                outcomes = get_outcomes(event)
                for outcome in outcomes:
                    orderbook = refresh_outcome_orderbook(event["id"], outcome["name"])
                    best_ask = float(outcome["ask"])
                    vwap_ask = get_vwap_for_size(orderbook, target_usd=100.0, side="ask")
                    tick_size = 0.01 

                    # Save to Moat
                    self.moat.save_quote(event["id"], city_slug, float(outcome["bid"]), best_ask, vwap_ask, outcome["spread"], 5000.0, tick_size)

                    signal_dict = {
                        "market_id": event["id"],
                        "question": event.get("question", ""),
                        "best_ask": best_ask,
                        "vwap_ask": vwap_ask,
                        "spread": outcome["spread"],
                    }

                end_date = event.get("endDate", "")
                hours = hours_to_resolution(end_date) if end_date else 0
                horizon = f"D+{i}"

                market = self.engine.storage.load_market(city_slug, date_str)
                if market is None:
                    if hours < self.engine.config.min_hours or hours > self.engine.config.max_hours:
                        continue
                    from ..storage import Market
                    market = Market(
                        city=city_slug,
                        city_name=loc.name,
                        date=date_str,
                        unit=loc.unit,
                        station=loc.station,
                        event_end_date=end_date,
                        hours_at_discovery=round(hours, 1),
                        created_at=datetime.now(timezone.utc).isoformat(),
                    )

                if market.status == "resolved":
                    continue

                outcomes = get_outcomes(event)
                market.all_outcomes = outcomes

                snap = snapshots.get(date_str, {})
                base_features = self.engine.feature_engine.build(loc, snap, outcomes, hours)
                
                # Update snapshots
                market.forecast_snapshots.append({
                    "ts": snap.get("ts"),
                    "horizon": horizon,
                    "hours_left": round(hours, 1),
                    "source": snap.get("best_source"),
                    "temp": snap.get("best"),
                    "ecmwf": snap.get("ecmwf"),
                    "hrrr": snap.get("hrrr"),
                    "metar": snap.get("metar"),
                })

                top = max(outcomes, key=lambda outcome: outcome["price"]) if outcomes else None
                unit_sym = "°F" if loc.unit == "F" else "°C"
                market.market_snapshots.append({
                    "ts": snap.get("ts"),
                    "top_bucket": f"{top['range'][0]}-{top['range'][1]}{unit_sym}" if top else None,
                    "top_price": top["price"] if top else None,
                })

                forecast_temp = snap.get("best")
                best_source = snap.get("best_source")

                # Handle open positions (stops/trails)
                if market.position and market.position.get("status") == "open":
                    pos = market.position
                    current_price = None
                    for outcome in outcomes:
                        if outcome["market_id"] == pos["market_id"]:
                            if refresh_outcome_orderbook(outcome):
                                current_price = outcome.get("bid", outcome["price"])
                            break

                    if current_price is not None:
                        entry = pos["entry_price"]
                        stop = pos.get("stop_price", entry * 0.80)
                        if current_price >= entry * 1.20 and stop < entry:
                            pos["stop_price"] = entry
                        if current_price <= stop:
                            if self.engine.modes.live_trade and pos.get("token_id"):
                                tp_order_id = (
                                    pos.get("tp_order_id")
                                    or pos.get("execution", {}).get("tp_order_id")
                                )
                                cancel_res = self.engine.executor.cancel_order(tp_order_id)
                                if not cancel_res.get("success"):
                                    self.engine.emit(
                                        f"[LIVE-STOP-ERROR] {loc.name} {date_str} | "
                                        f"TP cancel failed: {cancel_res.get('reason')}"
                                    )
                                    continue
                                pos["tp_cancel"] = cancel_res

                                close_res = self.engine.executor.close_position_market(
                                    token_id=pos["token_id"],
                                    size=pos.get("shares", 0),
                                )
                                if not close_res.get("success"):
                                    self.engine.emit(
                                        f"[LIVE-STOP-ERROR] {loc.name} {date_str} | {close_res.get('reason')}"
                                    )
                                    continue
                                pos["close_order_id"] = close_res.get("order")

                            fee = pos["cost"] * TRADING_FEE_PERCENT
                            pnl = round((current_price - entry) * pos["shares"] - fee, 2)
                            balance += pos["cost"] + pnl
                            pos.update({
                                "closed_at": datetime.now(timezone.utc).isoformat(),
                                "close_reason": "stop",
                                "exit_price": current_price,
                                "pnl": pnl,
                                "status": "closed"
                            })
                            result.closed += 1
                            self.engine.emit(f"[STOP/TRAIL] {loc.name} {date_str} | {pnl:+.2f}")

                # Look for new opportunities
                if not market.position and forecast_temp is not None and hours >= self.engine.config.min_hours:
                    opportunity = self.find_opportunity(city_slug, loc, snap, outcomes, hours, base_features, balance)
                    if opportunity:
                        outcome, probability_estimate, edge_estimate, features = opportunity
                        signal = self.build_signal(outcome, probability_estimate, edge_estimate, features, 0.0, 0.0, forecast_temp, best_source)
                        market.last_analysis = {
                            "ev": signal["ev"],
                            "price": signal["entry_price"],
                            "conf": signal.get("ml", {}).get("confidence", 0),
                            "ts": datetime.now(timezone.utc).isoformat()
                        }
                        
                        # Continue with trade sizing and filters
                        kelly, size = size_position(
                            probability_estimate.probability,
                            outcome["ask"],
                            balance,
                            self.engine.config.kelly_fraction,
                            self.engine.config.max_bet,
                        )
                        
                        # Micro-Live Cap: Override Kelly if in live mode
                        if self.engine.config.live_trade:
                            cap = getattr(self.engine.config, 'max_live_bet_usd', 10.0)
                            if size > cap:
                                self.engine.emit(f"[MICRO-LIVE CAP] ${size:.2f} -> ${cap:.2f}")
                                size = cap

                        if size >= 0.50:
                            signal = self.build_signal(outcome, probability_estimate, edge_estimate, features, kelly, size, forecast_temp, best_source)
                            ai_review, flagged = get_ai_trade_context(loc.name, snap, signal, unit=loc.unit)
                            if ai_review:
                                signal["ai"] = ai_review
                            
                            if flagged:
                                reason = ai_review.get("anomaly", {}).get("reason", "anomalie inconnue")
                                self.record_decision(market, loc, snap, features, signal, probability_estimate, edge_estimate, "SKIP", "ai_flagged", horizon, outcome)
                                self.engine.emit(f"[AI-SKIP] {loc.name} {date_str} | {reason}")
                            else:
                                # Portfolio Risk Check
                                open_markets = self.engine.storage.load_all_markets()
                                risk_check = self.engine.risk_manager.check_new_trade(city_slug, signal["cost"], open_markets)
                                
                                if not risk_check["allowed"]:
                                    self.record_decision(market, loc, snap, features, signal, probability_estimate, edge_estimate, "SKIP", risk_check["reason"], horizon, outcome)
                                    self.engine.emit(f"[RISK-SKIP] {loc.name} {date_str} | {risk_check['reason']}")
                                else:
                                    self.record_decision(market, loc, snap, features, signal, probability_estimate, edge_estimate, "BUY", "edge_positive", horizon, outcome)
                                    note = format_ai_note(signal.get("ai")) + format_ml_note(signal.get("ml"))
                                    
                                    if self.engine.modes.live_trade:
                                        # Real Execution via CLOB
                                        if not signal.get("token_id"):
                                            self.engine.emit(f"[LIVE-ERROR] {loc.name} | token_id CLOB manquant")
                                            continue

                                        exec_res = self.engine.executor.place_bracket_order(
                                            token_id=signal["token_id"],
                                            side="BUY",
                                            price=signal["entry_price"],
                                            size=signal["cost"] / signal["entry_price"]
                                        )

                                        if exec_res.get("success"):
                                            signal["execution"] = {
                                                "buy_order_id": exec_res.get("buy_order"),
                                                "tp_order_id": exec_res.get("tp_order"),
                                                "stop_loss_mode": exec_res.get("stop_loss_mode"),
                                                "status": exec_res.get("status"),
                                            }
                                            signal["tp_price"] = exec_res.get("tp_price")
                                            signal["stop_price"] = exec_res.get("stop_price")
                                            balance -= signal["cost"] * (1 + TRADING_FEE_PERCENT)
                                            market.position = signal
                                            state.total_trades += 1
                                            result.new_trades += 1
                                            self.engine.feedback.notify_trade_open(
                                                loc.name, date_str, f"{signal['bucket_low']}-{signal['bucket_high']}{unit_sym}",
                                                signal["entry_price"], signal["ev"], signal["cost"], signal["forecast_src"],
                                                f"{note}\n🛡️ *TP ACTIF / STOP SURVEILLÉ* "
                                                f"(TP {exec_res.get('tp_price')}, SL {exec_res.get('stop_price')})"
                                            )
                                            self.engine.emit(
                                                f"[LIVE-BUY] {loc.name} | ${signal['entry_price']:.3f} | "
                                                f"EV {signal['ev']:+.2f} | buy={exec_res.get('buy_order')} "
                                                f"tp={exec_res.get('tp_order')} sl=synthetic"
                                            )
                                        else:
                                            self.engine.emit(f"[LIVE-ERROR] {loc.name} | Échec exécution CLOB: {exec_res.get('reason')}")

                                if self.engine.modes.paper_mode and should_emit_marker(market.paper_state, signal):
                                    log_paper_trade(loc.name, date_str, horizon, signal)
                                    self.engine.paper_account.record_trade(signal["cost"])
                                    market.paper_position = signal
                                    market.paper_state = build_signal_marker(signal)

                                if self.engine.modes.signal_mode and should_emit_marker(market.signal_state, signal):
                                    # Use the new Signal Quality Layer
                                    sql_signal = Signal.from_dict(loc.name, signal)
                                    quality_result = self.engine.signal_quality.validate(sql_signal)
                                    
                                    if quality_result["accepted"]:
                                        filter_decision = {
                                            "allowed": True,
                                            "priority": "NORMAL",
                                            "emoji": "🌡️",
                                            "signal_type": "edge-opportunity",
                                        }
                                        
                                        trade_context = build_trade_payload(
                                            city=loc.name, date_str=date_str, horizon=horizon,
                                            bucket=f"{signal['bucket_low']}-{signal['bucket_high']}{unit_sym}",
                                            unit=unit_sym, signal=signal, question=signal["question"],
                                            event_slug=event.get("slug"), priority=filter_decision["priority"], emoji=filter_decision["emoji"],
                                        )
                                        pending_signals.append({
                                            "market": market, "loc": loc, "date_str": date_str, "horizon": horizon,
                                            "unit_sym": unit_sym, "signal": signal, "filter_decision": filter_decision,
                                            "trade_context": trade_context, "note": note,
                                        })

                self.engine.storage.save_market(market)
                time.sleep(0.1)

        # Process and send top signals
        if self.engine.modes.signal_mode and pending_signals:
            self.engine.process_pending_signals(pending_signals)

        # Resolve markets
        self.resolve_pending_markets(balance, state, result)
        
        if self.engine.modes.live_trade or result.closed:
            state.balance = round(balance, 2)
            state.peak_balance = max(state.peak_balance, balance)
            self.engine.storage.save_state(state)

        return result

    def find_opportunity(self, city_slug, loc, snap, outcomes, hours, base_features, balance):
        """Find the best tradeable outcome in a market."""
        forecast_temp = snap.get("best")
        best_source = snap.get("best_source")
        
        best = None
        for candidate in outcomes:
            t_low, t_high = candidate["range"]
            estimate = self.engine.probability_engine.estimate_bucket(
                city_slug, best_source, forecast_temp, loc.unit, t_low, t_high
            )
            if not in_bucket(estimate.adjusted_temp, t_low, t_high):
                continue

            if not refresh_outcome_orderbook(candidate):
                continue
            
            candidate_features = dict(base_features)
            candidate_features.update(self.engine.feature_engine.build(loc, snap, outcomes, hours, candidate))
            candidate_features["confidence"] = estimate.confidence
            candidate_features["sigma"] = estimate.sigma
            volume = candidate.get("volume", 0)
            current_edge = self.engine.edge_engine.compute(estimate.probability, candidate["ask"], candidate_features, best_source, volume)
            
            # Check should skip with stricter filters
            if should_skip_outcome(self.engine.config, candidate, candidate_features, current_edge.adjusted_ev):
                if current_edge.raw_ev > 0.05:
                    send_no_trade(city_slug, [f"Edge too small ({current_edge.raw_ev:+.1%})", "Filter rejection"])
                continue

            if best is None or current_edge.adjusted_ev > best[2].adjusted_ev:
                best = (candidate, estimate, current_edge, candidate_features)
        return best

    def build_signal(self, outcome, estimate, edge, features, kelly, size, forecast_temp, best_source):
        """Build a signal dictionary."""
        return {
            "market_id": outcome["market_id"],
            "token_id": outcome.get("token_id"),
            "question": outcome["question"],
            "bucket_low": outcome["range"][0],
            "bucket_high": outcome["range"][1],
            "entry_price": outcome["ask"],
            "bid_at_entry": outcome["bid"],
            "spread": outcome["spread"],
            "shares": round(size / outcome["ask"], 2),
            "cost": size,
            "best_ask_size_usd": float(outcome.get("best_ask_size", 0.0)) * outcome["ask"],
            "raw_prob": round(bucket_prob(estimate.adjusted_temp, outcome["range"][0], outcome["range"][1], estimate.sigma), 4),
            "p": round(estimate.probability, 4),
            "ev": edge.adjusted_ev,
            "raw_ev": edge.raw_ev,
            "edge_penalties": edge.penalties,
            "kelly": kelly,
            "forecast_temp": estimate.adjusted_temp,
            "raw_forecast_temp": forecast_temp,
            "forecast_src": best_source or "ecmwf",
            "sigma": estimate.sigma,
            "ml": {
                "adjusted_temp": estimate.adjusted_temp,
                "sigma": estimate.sigma,
                "confidence": estimate.confidence,
                "bias": estimate.bias,
                "mae": estimate.mae,
                "n": estimate.n,
                "tier": estimate.tier,
                "mae": estimate.mae,
                "n": estimate.n,
                "tier": estimate.tier,
                "features": features, # Reference features directly
            },
            "features": features,
            "opened_at": datetime.now(timezone.utc).isoformat(),
            "status": "open",
        }

    def record_decision(self, market, loc, snap, features, signal, prob, edge, action, reason, horizon, outcome):
        """Record a trading decision in the feedback system."""
        self.engine.feedback_recorder.record_decision(
            market=market, location=loc, modes=self.engine.modes,
            snapshot=snap, features=features, signal=signal,
            probability_estimate=prob, edge_estimate=edge,
            action=action, reason=reason, horizon=horizon, outcome=outcome
        )

    def resolve_pending_markets(self, balance, state, result):
        """Check and resolve all open markets."""
        for market in self.engine.storage.load_all_markets():
            if market.status == "resolved" or not market.position or market.position.get("status") != "open":
                continue

            new_balance, won, pnl = self.engine.resolve_market(market, balance)
            if won is None or pnl is None:
                continue

            balance = new_balance
            result.resolved += 1
            if won: state.wins += 1
            else: state.losses += 1

            unit_sym = "°F" if market.unit == "F" else "°C"
            bucket = f"{market.position['bucket_low']}-{market.position['bucket_high']}{unit_sym}"
            temp_str = f"{market.actual_temp}{unit_sym}" if market.actual_temp is not None else "N/A"

            if won:
                self.engine.feedback.notify_trade_win(market.city_name, market.date, bucket, pnl, temp_str, balance)
            else:
                self.engine.feedback.notify_trade_loss(market.city_name, market.date, bucket, pnl, balance)

            self.engine.emit(f"[{'WIN' if won else 'LOSS'}] {market.city_name} {market.date} | {pnl:+.2f}")
            self.engine.storage.save_market(market)
            time.sleep(0.3)

# Audit: Includes fee and slippage awareness
