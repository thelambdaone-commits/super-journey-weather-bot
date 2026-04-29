"""
Market scanning and processing logic.
"""

from __future__ import annotations
import time
import logging
from copy import copy
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

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
from ..strategy.filters import run_all_filters
# from ..strategy.sizing import size_position  # TODO: use final_position_size instead
from .helpers import (
    log_paper_trade,
    build_signal_marker,
    should_emit_marker,
    format_ai_note,
    format_ml_note,
    get_ai_trade_context,
)
from .polymarket import get_vwap_for_size
from src.notifications.telegram_control_center import send_no_trade
from src.notifications.desk_metrics import log_event
from .types import ScanResult

if TYPE_CHECKING:
    from .engine import TradingEngine

from ..strategy.signal_quality import Signal
from ..strategy.surebet import detect_surebet
from ..utils.feature_flags import is_enabled
from ..data.moat_manager import get_moat
from ..weather.collectors.open_meteo import MultiModelCollector
from .idempotence import get_idempotence_manager
from .decision import DecisionEngine

logger = logging.getLogger(__name__)


class MarketScanner:
    """Core logic for scanning markets and identifying opportunities."""

    def __init__(self, engine: TradingEngine):
        self.engine = engine
        self.idempotence = get_idempotence_manager()
        self.moat = get_moat()
        self.multi_collector = MultiModelCollector(LOCATIONS)
        # Decision engine for clean trade decisions
        self.decision_engine = DecisionEngine(engine.config)

    def scan_and_update(self) -> ScanResult:
        """Run a full scan cycle."""
        now = datetime.now(timezone.utc)
        state = self.engine.storage.load_state()
        balance = state.balance
        result = ScanResult()

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
                event_slug = event.get("slug", "")

                # Filter 1: Minimum volume
                market_volume_raw = event.get("volume", 0) or 0
                try:
                    market_volume = float(market_volume_raw)
                except (ValueError, TypeError):
                    market_volume = 0.0

                min_volume = float(getattr(self.engine.config, "min_volume", 500))
                if market_volume < min_volume:
                    self.engine.emit(
                        f"[LOW-VOL] {loc.name} {date_str} | Vol: ${market_volume:.0f} < ${min_volume:.0f}"
                    )
                    continue

                outcomes = get_outcomes(event)
                for outcome in outcomes:
                    # CLOB guard for paper mode
                    if self.engine.modes.live_trade:
                        if not refresh_outcome_orderbook(outcome):
                            continue
                    else:
                        self.engine.emit("[PAPER] skipped CLOB refresh, using Gamma prices")

                    # Safety check bid/ask
                    bid = float(outcome.get("best_bid", 0) or 0)
                    ask = float(outcome.get("best_ask", 0) or 0)

                    if bid <= 0 or ask <= 0:
                        self.engine.emit(f"[NO-LIQUIDITY] {loc.name}")
                        continue

                    # Filter 2: Maximum spread (corrected formula)
                    spread = (ask - bid) / ((ask + bid) / 2)
                    max_spread = getattr(self.engine.config, "max_spread", 0.05)

                    if spread > max_spread:
                        self.engine.emit(
                            f"[HIGH-SPREAD] {loc.name} | Spread: {spread:.2%} > {max_spread:.0%}"
                        )
                        continue

                    best_ask = ask
                    orderbook = {"asks": [{"price": outcome["ask"], "size": outcome.get("best_ask_size", 0.0)}]}
                    vwap_ask = get_vwap_for_size(orderbook, target_usd=100.0, side="ask")
                    tick_size = 0.01

                    # Save to Moat
                    self.moat.save_quote(
                        event["id"],
                        city_slug,
                        float(outcome["bid"]),
                        best_ask,
                        vwap_ask,
                        outcome["spread"],
                        5000.0,
                        tick_size,
                    )

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
                if getattr(self.engine.config, "surebet_detection_enabled", True):
                    surebet_outcomes = []
                    for outcome in outcomes:
                        if refresh_outcome_orderbook(outcome):
                            surebet_outcomes.append(outcome)
                    surebet = detect_surebet(
                        surebet_outcomes,
                        max_total_stake=float(getattr(self.engine.config, "surebet_max_total_stake_usd", 50.0)),
                        min_profit_pct=float(getattr(self.engine.config, "surebet_min_profit_pct", 0.01)),
                        fee_buffer_pct=float(getattr(self.engine.config, "surebet_fee_buffer_pct", 0.003)),
                        min_liquidity_usd=float(getattr(self.engine.config, "surebet_min_liquidity_usd", 5.0)),
                    )
                    if surebet:
                        self.engine.emit(
                            f"[SUREBET-DETECTED] {loc.name} {date_str} | "
                            f"profit=${surebet.guaranteed_profit:.2f} "
                            f"({surebet.profit_pct:.2%}) | legs={len(surebet.legs)}"
                        )
                        if not market.position and not market.paper_position:
                            balance, executed = self._execute_surebet(
                                surebet,
                                market,
                                loc,
                                date_str,
                                balance,
                                state,
                                result,
                            )
                            if executed:
                                self.engine.storage.save_market(market)
                                continue
                market.all_outcomes = outcomes

                snap = snapshots.get(date_str, {})
                base_features = self.engine.feature_engine.build(loc, snap, outcomes, hours)

                # Update snapshots
                market.forecast_snapshots.append(
                    {
                        "ts": snap.get("ts"),
                        "horizon": horizon,
                        "hours_left": round(hours, 1),
                        "source": snap.get("best_source"),
                        "temp": snap.get("best"),
                        "ecmwf": snap.get("ecmwf"),
                        "hrrr": snap.get("hrrr"),
                        "metar": snap.get("metar"),
                    }
                )

                top = max(outcomes, key=lambda outcome: outcome["price"]) if outcomes else None
                unit_sym = "°F" if loc.unit == "F" else "°C"
                market.market_snapshots.append(
                    {
                        "ts": snap.get("ts"),
                        "top_bucket": f"{top['range'][0]}-{top['range'][1]}{unit_sym}" if top else None,
                        "top_price": top["price"] if top else None,
                    }
                )

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
                                tp_order_id = pos.get("tp_order_id") or pos.get("execution", {}).get("tp_order_id")
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
                            pos.update(
                                {
                                    "closed_at": datetime.now(timezone.utc).isoformat(),
                                    "close_reason": "stop",
                                    "exit_price": current_price,
                                    "pnl": pnl,
                                    "status": "closed",
                                }
                            )
                            result.closed += 1
                            self.engine.emit(f"[STOP/TRAIL] {loc.name} {date_str} | {pnl:+.2f}")

                # Look for new opportunities (check both live and paper positions)
                has_open_position = (
                    (market.position and market.position.get("status") == "open") or
                    (market.paper_position and market.paper_position.get("status") == "open")
                )
                if not has_open_position and forecast_temp is not None and hours >= self.engine.config.min_hours:
                    opportunity = self.find_opportunity(city_slug, loc, snap, outcomes, hours, base_features, balance)
                    if opportunity:
                        outcome, probability_estimate, edge_estimate, features = opportunity
                        
                        # Build context for DecisionEngine
                        context = {
                            "outcome": outcome,
                            "features": features,
                            "orderbook": None,  # TODO: pass real orderbook
                            "model_probability": probability_estimate.probability,
                            "bankroll": balance,
                            "event_slug": event_slug,
                            "location": loc.name,
                            "date": date_str,
                        }
                        
                        # Get decision from DecisionEngine
                        decision = self.decision_engine.evaluate(context)
                        
                        # Log the decision
                        self.engine.emit(
                            f"[DECISION] {loc.name} {date_str} | "
                            f"action={decision.action} | "
                            f"net_ev={decision.net_ev:.4f} | "
                            f"size={decision.suggested_size:.2f}"
                        )
                        
                        if decision.should_trade:
                            # Execute the trade (simplified - reuse existing logic)
                            signal = self.build_signal(
                                outcome,
                                probability_estimate,
                                edge_estimate,
                                features,
                                0.0,
                                decision.suggested_size,
                                forecast_temp,
                                best_source,
                            )
                            
                            # Risk check
                            open_markets = self.engine.storage.load_all_markets()
                            risk_check = self.engine.risk_manager.check_new_trade(
                                city_slug, signal["cost"], open_markets
                            )
                            
                            if not risk_check["allowed"]:
                                self.engine.emit(f"[RISK-SKIP] {loc.name} {date_str} | {risk_check['reason']}")
                                continue
                            
                            # Execute (simplified - call existing _execute_trade)
                            balance, executed = self._execute_trade(
                                signal,
                                market,
                                loc,
                                date_str,
                                horizon,
                                unit_sym,
                                "",
                                "DECISION_ENGINE",
                                balance,
                                state,
                                result,
                                event_slug=event_slug,
                            )
                            
                            if executed:
                                self.engine.emit(f"[EXECUTED] {loc.name} {date_str} | size={decision.suggested_size:.2f}")
                        else:
                            self.engine.emit(
                                f"[SKIPPED] {loc.name} {date_str} | "
                                f"reason={decision.rejected_reason}"
                            )

                self.engine.storage.save_market(market)
                time.sleep(0.1)

        # Resolve markets
        self.resolve_pending_markets(balance, state, result)

        if self.engine.modes.live_trade or result.closed:
            state.balance = round(balance, 2)
            state.peak_balance = max(state.peak_balance, balance)
            self.engine.storage.save_state(state)

        return result

    def _surebet_position(self, surebet, loc, date_str: str, mode: str, execution: dict | None = None) -> dict:
        legs = [
            {
                "market_id": leg.market_id,
                "token_id": leg.token_id,
                "bucket_low": leg.bucket_low,
                "bucket_high": leg.bucket_high,
                "ask": leg.ask,
                "stake": leg.stake,
                "shares": round(leg.stake / leg.ask, 4),
                "payout": leg.payout,
            }
            for leg in surebet.legs
        ]
        return {
            "type": "surebet",
            "market_id": f"surebet:{loc.slug}:{date_str}",
            "question": f"Surebet multi-bucket {loc.name} {date_str}",
            "cost": surebet.total_cost,
            "shares": sum(leg["shares"] for leg in legs),
            "entry_price": round(surebet.implied_sum / len(legs), 4),
            "bucket_low": min(leg["bucket_low"] for leg in legs),
            "bucket_high": max(leg["bucket_high"] for leg in legs),
            "legs": legs,
            "guaranteed_payout": surebet.guaranteed_payout,
            "guaranteed_profit": surebet.guaranteed_profit,
            "profit_pct": surebet.profit_pct,
            "implied_sum": surebet.implied_sum,
            "opened_at": datetime.now(timezone.utc).isoformat(),
            "status": "open",
            "mode": mode,
            "execution": execution or {},
        }

    def _execute_surebet(self, surebet, market, loc, date_str, balance, state, result):
        """Execute a surebet atomically in paper or live mode."""
        if self.engine.modes.live_trade and getattr(self.engine.config, "surebet_live_execution_enabled", False):
            legs = [
                {
                    "token_id": leg.token_id,
                    "stake": leg.stake,
                    "ask": leg.ask,
                }
                for leg in surebet.legs
            ]
            execution = self.engine.executor.place_surebet_atomic(legs)
            if not execution.get("success"):
                self.engine.emit(f"[SUREBET-LIVE-ERROR] {loc.name} {date_str} | {execution.get('reason')}")
                return balance, False

            market.position = self._surebet_position(surebet, loc, date_str, "live", execution)
            balance -= surebet.total_cost * (1 + TRADING_FEE_PERCENT)
            state.total_trades += 1
            result.new_trades += 1
            self.engine.emit(
                f"[SUREBET-LIVE] {loc.name} {date_str} | "
                f"cost=${surebet.total_cost:.2f} guaranteed=${surebet.guaranteed_profit:.2f}"
            )
            return balance, True

        if self.engine.modes.paper_mode and getattr(self.engine.config, "surebet_paper_execution_enabled", True):
            self.engine.paper_account.record_trade(surebet.total_cost)
            market.paper_position = self._surebet_position(surebet, loc, date_str, "paper")
            market.paper_state = {
                "market_id": market.paper_position["market_id"],
                "type": "surebet",
                "profit_pct": surebet.profit_pct,
                "recorded_at": datetime.now(timezone.utc).isoformat(),
            }
            result.new_trades += 1
            self.engine.emit(
                f"[SUREBET-PAPER] {loc.name} {date_str} | "
                f"cost=${surebet.total_cost:.2f} guaranteed=${surebet.guaranteed_profit:.2f}"
            )
            return balance, True

        return balance, False

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
            current_edge = self.engine.edge_engine.compute(
                estimate.probability, candidate["ask"], candidate_features, best_source, volume
            )

            # Check should skip with stricter filters
            if should_skip_outcome(
                self._filter_config_for_mode(), candidate, candidate_features, current_edge.adjusted_ev
            ):
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
            "raw_prob": round(
                bucket_prob(estimate.adjusted_temp, outcome["range"][0], outcome["range"][1], estimate.sigma), 4
            ),
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
                "features": features,  # Reference features directly
            },
            "features": features,
            "opened_at": datetime.now(timezone.utc).isoformat(),
            "status": "open",
        }

    def _ai_filters_pass(self, signal: dict, ai_review: dict) -> bool:
        """Check if signal passes AI flow filters."""
        config = self.engine.config

        # AI confidence filter
        ai_confidence = self._confidence_value(ai_review.get("confidence", 0))
        if ai_confidence < getattr(config, "ai_min_confidence", 0.50):
            return False

        # EV threshold (reject suspiciously high EV)
        if signal.get("ev", 0) > getattr(config, "ai_max_ev_threshold", 2.0):
            return False

        return True

    def _signal_filters_pass(self, signal: dict, quality_result: dict) -> bool:
        """Check if signal passes Signal flow filters."""
        config = self.engine.config
        paper_training = self._paper_training_enabled()
        min_quality = (
            getattr(config, "paper_training_min_quality_score", 0.25)
            if paper_training
            else getattr(config, "signal_min_quality_score", 0.40)
        )
        min_confidence = (
            getattr(config, "paper_training_min_confidence", 0.25)
            if paper_training
            else getattr(config, "signal_min_confidence", 0.30)
        )
        min_edge = (
            getattr(config, "paper_training_min_ev", 0.02)
            if paper_training
            else getattr(config, "signal_min_edge", 0.01)
        )

        # Quality score filter
        if quality_result.get("score", 0) < min_quality:
            return False

        # ML confidence filter
        ml_conf = signal.get("ml", {}).get("confidence", 0)
        if ml_conf < min_confidence:
            return False

        # Edge filter
        if signal.get("ev", 0) < min_edge:
            return False

        return True

    def _paper_training_enabled(self) -> bool:
        return (
            self.engine.modes.paper_mode
            and not self.engine.modes.live_trade
            and bool(getattr(self.engine.config, "paper_training_mode", False))
        )

    def _filter_config_for_mode(self):
        config = self.engine.config
        if not self._paper_training_enabled():
            return config

        paper_config = copy(config)
        paper_config.min_ev = float(getattr(config, "paper_training_min_ev", config.min_ev))
        paper_config.max_price = float(getattr(config, "paper_training_max_price", config.max_price))
        paper_config.max_slippage = float(getattr(config, "paper_training_max_spread", config.max_slippage))
        paper_config.min_volume = int(getattr(config, "paper_training_min_volume", config.min_volume))
        paper_config.min_confidence = float(getattr(config, "paper_training_min_confidence", 0.25))
        return paper_config

    def _max_bet_for_mode(self) -> float:
        if self._paper_training_enabled():
            return float(getattr(self.engine.config, "paper_training_max_bet_usd", 5.0))
        return float(self.engine.config.max_bet)

    def _min_trade_size_for_mode(self) -> float:
        if self._paper_training_enabled():
            return float(getattr(self.engine.config, "paper_training_min_bet_usd", 1.0))
        return 0.50

    def _validate_signal_for_mode(self, sql_signal: Signal, signal: dict) -> dict:
        if not self._paper_training_enabled():
            return self.engine.signal_quality.validate(sql_signal)

        hard_reason = self.engine.signal_quality.validate_hard_rules(sql_signal)
        if hard_reason in {"invalid_price", "stale_market"}:
            return {"accepted": False, "reason": hard_reason, "score": 0.0}

        quality_score = self.engine.signal_quality.compute_quality(sql_signal)
        min_quality = float(getattr(self.engine.config, "paper_training_min_quality_score", 0.25))
        return {
            "accepted": quality_score >= min_quality,
            "score": quality_score,
            "reason": f"paper_quality_{quality_score:.2f}" if quality_score < min_quality else "paper_training_ok",
        }

    @staticmethod
    def _confidence_value(raw_confidence) -> float:
        if isinstance(raw_confidence, str):
            return {"low": 0.25, "medium": 0.55, "high": 0.85}.get(raw_confidence.lower(), 0.0)
        return float(raw_confidence or 0.0)

    def _execute_trade(
        self,
        signal,
        market,
        loc,
        date_str,
        horizon,
        unit_sym,
        note,
        source,
        balance,
        state,
        result,
        event_slug="",
    ):
        """Execute trade based on mode (live/paper/signal)."""
        executed = False
        if self.engine.modes.live_trade:
            # Real Execution via CLOB
            if not signal.get("token_id"):
                self.engine.emit(f"[LIVE-ERROR] {loc.name} | token_id CLOB manquant")
                return balance, False

            exec_res = self.engine.executor.place_bracket_order(
                token_id=signal["token_id"],
                side="BUY",
                price=signal["entry_price"],
                size=signal["cost"] / signal["entry_price"],
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
                    loc.name,
                    date_str,
                    f"{signal['bucket_low']}-{signal['bucket_high']}{unit_sym}",
                    signal["entry_price"],
                    signal["ev"],
                    signal["cost"],
                    signal["forecast_src"],
                    f"{note}\n🛡️ *TP ACTIF / STOP SURVEILLÉ* "
                    f"(TP {exec_res.get('tp_price')}, SL {exec_res.get('stop_price')})",
                )
                self.engine.emit(
                    f"[LIVE-BUY] {loc.name} | ${signal['entry_price']:.3f} | "
                    f"EV {signal['ev']:+.2f} | buy={exec_res.get('buy_order')} "
                    f"tp={exec_res.get('tp_order')} sl=synthetic | Source: {source}"
                )
                executed = True
            else:
                self.engine.emit(f"[LIVE-ERROR] {loc.name} | Échec exécution CLOB: {exec_res.get('reason')}")
                return balance, False

        if self.engine.modes.paper_mode:
            if should_emit_marker(market.paper_state, signal):
                log_paper_trade(loc.name, date_str, horizon, signal)
                self.engine.paper_account.record_trade(signal["cost"])
                market.paper_position = signal
                market.paper_state = build_signal_marker(signal)
                result.new_trades += 1
                self.engine.emit(f"[PAPER-BUY] {loc.name} {date_str} | Source: {source}")
                self.engine.feedback.notify_trade_open(
                    loc.name,
                    date_str,
                    f"{signal['bucket_low']}-{signal['bucket_high']}{unit_sym}",
                    signal["entry_price"],
                    signal["ev"],
                    signal["cost"],
                    signal["forecast_src"],
                    f"{note}" if note else ""
                )
                executed = True

        if self.engine.modes.signal_mode and should_emit_marker(market.signal_state, signal):
            # Send signal notification
            filter_decision = {
                "allowed": True,
                "priority": "NORMAL",
                "emoji": "🌡️",
                "signal_type": "edge-opportunity",
            }
            trade_context = build_trade_payload(
                city=loc.name,
                date_str=date_str,
                horizon=horizon,
                bucket=f"{signal['bucket_low']}-{signal['bucket_high']}{unit_sym}",
                unit=unit_sym,
                signal=signal,
                question=signal["question"],
                event_slug=event_slug,
                priority=filter_decision["priority"],
                emoji=filter_decision["emoji"],
            )
            self.engine.feedback.notify_signal(
                loc.name,
                date_str,
                f"{signal['bucket_low']}-{signal['bucket_high']}{unit_sym}",
                signal["entry_price"],
                signal["ev"],
                signal["cost"],
                signal["forecast_src"],
                horizon,
                signal["question"],
                signal["market_id"],
                note,
                calibrated_prob=signal["p"],
                market_prob=signal["entry_price"],
                uncertainty=signal.get("edge_penalties", {}).get("uncertainty"),
                signal_type="edge-opportunity",
                quality=signal.get("ml", {}).get("confidence", 0),
                priority="NORMAL",
                emoji="🌡️",
                confidence_score=signal.get("ml", {}).get("confidence", 0),
                source_bias=signal.get("ml", {}).get("bias"),
                trade_context=trade_context,
            )
            self.engine.signal_quality.commit(Signal.from_dict(loc.name, signal))
            market.signal_state = build_signal_marker(signal)
            executed = True

        return balance, executed

    def record_decision(self, market, loc, snap, features, signal, prob, edge, action, reason, horizon, outcome):
        """Record a trading decision in the feedback system."""
        self.engine.feedback_recorder.record_decision(
            market=market,
            location=loc,
            modes=self.engine.modes,
            snapshot=snap,
            features=features,
            signal=signal,
            probability_estimate=prob,
            edge_estimate=edge,
            action=action,
            reason=reason,
            horizon=horizon,
            outcome=outcome,
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
            if won:
                state.wins += 1
            else:
                state.losses += 1

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
