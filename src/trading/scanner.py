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
from ..weather.math import bucket_prob
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
from ..notifications.telegram_control_center import send_no_trade
from ..notifications.desk_metrics import log_event
from .types import ScanResult

if TYPE_CHECKING:
    from .engine import TradingEngine

from ..strategy.signal_quality import Signal
from ..strategy.surebet import detect_surebet, has_exhaustive_temperature_coverage
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
        self._orderbook_cache: dict[str, dict | None] = {}
        self._clob_requests_used = 0
        self._ai_reviews_used = 0

    def scan_and_update(self) -> ScanResult:
        """Run a full scan cycle."""
        self._orderbook_cache = {}
        self._clob_requests_used = 0
        self._ai_reviews_used = 0
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
                logger.exception("V3 Moat collection traceback:")

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
                    if self._surebet_prefilter_passes(outcomes):
                        for outcome in outcomes:
                            if self._refresh_outcome_orderbook(outcome):
                                surebet_outcomes.append(outcome)
                    surebet = (
                        detect_surebet(
                            surebet_outcomes,
                            max_total_stake=float(getattr(self.engine.config, "surebet_max_total_stake_usd", 50.0)),
                            min_profit_pct=float(getattr(self.engine.config, "surebet_min_profit_pct", 0.01)),
                            fee_buffer_pct=float(getattr(self.engine.config, "surebet_fee_buffer_pct", 0.003)),
                            min_liquidity_usd=float(getattr(self.engine.config, "surebet_min_liquidity_usd", 5.0)),
                        )
                        if surebet_outcomes
                        else None
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
                            if self._refresh_outcome_orderbook(outcome):
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
                    # NEW: Score ALL buckets using range probability engine
                    opportunities = self.find_all_opportunities(
                        city_slug, loc, snap, outcomes, hours, base_features, balance
                    )

                    # Iterate through opportunities (sorted by edge, highest first)
                    for opp in opportunities:
                        outcome = opp["outcome"]
                        prob_model = opp["prob"]
                        market_price = opp["price"]
                        edge = opp["edge"]
                        ev = opp["ev"]
                        size = opp["size"]
                        bucket = opp["bucket"]
                        sigma = opp.get("sigma", 2.0)
                        features = opp.get("features", {})

                        # Build signal
                        from src.weather.math import bucket_prob
                        adjusted_temp = float(forecast_temp) - features.get("bias", 0.0)
                        signal = {
                            "market_id": outcome["market_id"],
                            "token_id": outcome.get("token_id"),
                            "question": outcome["question"],
                            "bucket_low": outcome["range"][0],
                            "bucket_high": outcome["range"][1],
                            "entry_price": market_price,
                            "bid_at_entry": outcome.get("bid", market_price),
                            "spread": outcome.get("spread", 0.05),
                            "shares": round(size / market_price, 2) if market_price > 0 else 0,
                            "cost": size,
                            "best_ask_size_usd": float(outcome.get("best_ask_size", 0.0)) * market_price,
                            "raw_prob": round(bucket_prob(adjusted_temp, outcome["range"][0], outcome["range"][1], sigma), 4),
                            "p": round(prob_model, 4),
                            "ev": round(ev, 4),
                            "raw_ev": round(edge, 4),
                            "edge_penalties": {"source_bias": 0.0, "low_confidence": 0.0, "low_volume": 0.0},
                            "kelly": round(size / balance, 4) if balance > 0 else 0,
                            "forecast_temp": adjusted_temp,
                            "raw_forecast_temp": forecast_temp,
                            "forecast_src": best_source or "ecmwf",
                            "sigma": sigma,
                            "ml": {
                                "adjusted_temp": adjusted_temp,
                                "sigma": sigma,
                                "confidence": features.get("confidence", 0.5),
                                "bias": features.get("bias", 0.0),
                                "mae": features.get("mae", 1.5),
                                "n": features.get("n", 0),
                                "tier": features.get("tier", "default"),
                                "features": features,
                            },
                            "features": features,
                            "opened_at": datetime.now(timezone.utc).isoformat(),
                            "status": "open",
                        }

                        # Risk check
                        open_markets = self.engine.storage.load_all_markets()
                        risk_check = self.engine.risk_manager.check_new_trade(
                            city_slug, signal["cost"], open_markets
                        )

                        if not risk_check["allowed"]:
                            self.engine.emit(f"[RISK-SKIP] {loc.name} {date_str} | {risk_check['reason']}")
                            continue

                        ai_note = ""
                        ai_result = self._review_signal_with_ai(loc, snap, signal, unit_sym)
                        if not ai_result["allowed"]:
                            self.engine.emit(f"[AI-SKIP] {loc.name} {date_str} | {ai_result['reason']}")
                            self._log_rejection(
                                city_slug,
                                outcome.get("question", ""),
                                bucket,
                                prob_model,
                                market_price,
                                edge,
                                f"ai_rejected:{ai_result['reason']}",
                            )
                            continue
                        if ai_result.get("review"):
                            signal["ai"] = ai_result["review"]
                            ai_note = format_ai_note(ai_result["review"])

                        # Execute the trade
                        balance, executed = self._execute_trade(
                            signal,
                            market,
                            loc,
                            date_str,
                            horizon,
                            unit_sym,
                            "\n".join(part for part in [f"edge={edge:.2%}", ai_note] if part),
                            "RANGE_PROBABILITY_ENGINE",
                            balance,
                            state,
                            result,
                            event_slug=event_slug,
                        )

                        if executed:
                            self.engine.emit(
                                f"[EXECUTED] {loc.name} {date_str} | "
                                f"bucket={bucket} | "
                                f"size=${size:.2f} | "
                                f"edge={edge:.2%}"
                            )
                            # Only execute the best opportunity per city/date
                            break
                        else:
                            self.engine.emit(
                                f"[SKIPPED] {loc.name} {date_str} | "
                                f"bucket={bucket} | "
                                f"reason=execution_failed"
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

    def _refresh_outcome_orderbook(self, outcome: dict) -> bool:
        """Refresh CLOB orderbook with per-scan cache and request budget."""
        token_id = str(outcome.get("token_id") or "")
        if not token_id:
            outcome["orderbook_status"] = "missing_token"
            return False

        if token_id in self._orderbook_cache:
            cached = self._orderbook_cache[token_id]
            if cached is None:
                outcome["orderbook_status"] = "cached_missing"
                return False
            outcome.update(copy(cached))
            return True

        max_requests = int(getattr(self.engine.config, "max_clob_requests_per_scan", 30))
        if self._clob_requests_used >= max_requests:
            outcome["orderbook_status"] = "budget_exhausted"
            return False

        self._clob_requests_used += 1
        if not refresh_outcome_orderbook(outcome):
            self._orderbook_cache[token_id] = None
            return False

        self._orderbook_cache[token_id] = copy(outcome)
        return True

    def _surebet_prefilter_passes(self, outcomes: list[dict]) -> bool:
        """Use free Gamma prices to avoid expensive full CLOB surebet scans."""
        if len(outcomes) < 2 or not has_exhaustive_temperature_coverage(outcomes):
            return False

        try:
            implied_sum = sum(float(outcome.get("ask") or 0.0) for outcome in outcomes)
        except (TypeError, ValueError):
            return False

        min_profit = float(getattr(self.engine.config, "surebet_min_profit_pct", 0.01))
        fee_buffer = float(getattr(self.engine.config, "surebet_fee_buffer_pct", 0.003))
        margin = float(getattr(self.engine.config, "surebet_prefilter_margin", 0.0))
        threshold = 1.0 - min_profit - fee_buffer + margin
        if implied_sum >= threshold:
            return False

        max_requests = int(getattr(self.engine.config, "max_clob_requests_per_scan", 30))
        return self._clob_requests_used + len(outcomes) <= max_requests

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

    def _calculate_dynamic_sigma(self, snap: dict, base_sigma: float = 2.0) -> float:
        """
        Calculate dynamic sigma based on model divergence.
        When ECMWF and GFS disagree, increase uncertainty (opportunity!).
        """
        ecmwf = snap.get("ecmwf")
        gfs = snap.get("gfs")
        hrrr = snap.get("hrrr")

        sigma = base_sigma

        if ecmwf is not None and gfs is not None:
            diff = abs(float(ecmwf) - float(gfs))
            if diff > 3.0:
                # Increase sigma when models diverge - market underestimates uncertainty
                sigma *= (1.0 + diff / 10.0)

        if ecmwf is not None and hrrr is not None:
            diff = abs(float(ecmwf) - float(hrrr))
            if diff > 4.0:
                sigma *= (1.0 + diff / 15.0)

        return max(base_sigma, sigma)

    def find_all_opportunities(self, city_slug, loc, snap, outcomes, hours, base_features, balance):
        """
        Score ALL buckets using range probability engine.
        This is the core of the pricing engine transformation.
        """
        from src.strategy.range_probability import calculate_all_bucket_probs, find_best_edge

        forecast_temp = snap.get("best")
        if forecast_temp is None:
            return []

        # Dynamic sigma based on model divergence
        base_sigma = 2.0 if loc.unit == "C" else 3.6
        sigma = self._calculate_dynamic_sigma(snap, base_sigma)

        # Calculate probabilities for ALL buckets
        all_probs = calculate_all_bucket_probs(float(forecast_temp), sigma, outcomes)

        opportunities = []
        min_edge = getattr(self.engine.config, "min_edge", 0.015)

        for item in all_probs:
            if item["edge_brut"] <= min_edge:
                continue

            outcome = item["outcome"]
            prob_model = item["prob_model"]
            market_price = item["price_market"]
            edge = item["edge_brut"]

            # Refresh orderbook for executable prices
            if not self._refresh_outcome_orderbook(outcome):
                continue

            # Build features for this candidate
            candidate_features = dict(base_features)
            candidate_features.update(
                self.engine.feature_engine.build(loc, snap, outcomes, hours, outcome)
            )
            candidate_features["confidence"] = min(1.0, max(0.0, 1.0 - (sigma / 10.0)))
            candidate_features["sigma"] = sigma

            # Calculate EV with proper fee/slippage estimation
            volume = float(outcome.get("volume", 0))
            from src.strategy.edge import gross_edge, net_ev, estimate_fee, estimate_slippage

            fee = estimate_fee(market_price, 10.0, self.engine.config)  # Estimate on $10 size
            slippage = estimate_slippage(outcome.get("orderbook", {}), 10.0)
            ev = net_ev(prob_model, market_price, fee, slippage)

            # Run filters (relaxed)
            filter_result = run_all_filters(
                outcome,
                candidate_features,
                outcome.get("orderbook"),
                ev,
                edge,
                self._filter_config_for_mode(),
            )

            if not filter_result["passed"]:
                # Log rejection
                self._log_rejection(
                    city_slug,
                    outcome.get("question", ""),
                    item["bucket"],
                    prob_model,
                    market_price,
                    edge,
                    filter_result.get("rejected_reason", "unknown")
                )
                continue

            # Run Signal Quality Layer validation
            signal = Signal.from_dict(city_slug, {
                "market_id": outcome["market_id"],
                "entry_price": market_price,
                "ev": ev,
                "p": prob_model,
                "spread": outcome.get("spread", 0.05),
                "best_ask": outcome.get("ask", market_price),
                "vwap_ask": outcome.get("vwap_ask", outcome.get("ask", market_price)),
                "ml": {
                    "confidence": candidate_features.get("confidence", 0.5),
                    "mae": candidate_features.get("mae", 1.5),
                },
                "features": candidate_features,
            })
            quality_result = self.engine.signal_quality.validate(signal)
            if not quality_result["accepted"]:
                self._log_rejection(
                    city_slug,
                    outcome.get("question", ""),
                    item["bucket"],
                    prob_model,
                    market_price,
                    edge,
                    f"quality_rejected:{quality_result.get('reason', 'unknown')}"
                )
                continue

            # Calculate position size using Kelly
            from src.strategy.sizing import kelly_fraction_binary, fractional_kelly, cap_position_size

            kelly_frac = kelly_fraction_binary(prob_model, market_price)
            frac_kelly = fractional_kelly(prob_model, market_price, self.engine.config.kelly_fraction)
            raw_size = frac_kelly * balance

            # Cap the size
            capped_size = cap_position_size(
                raw_size,
                balance,
                getattr(self.engine.config, "max_position_pct", 0.02),
                getattr(self.engine.config, "max_market_exposure_pct", 0.05)
            )
            final_size = max(0.0, capped_size)

            if final_size < 0.50:
                continue

            opportunities.append({
                "outcome": outcome,
                "prob": prob_model,
                "price": market_price,
                "edge": edge,
                "ev": ev,
                "size": final_size,
                "bucket": item["bucket"],
                "sigma": sigma,
                "features": candidate_features
            })

        return sorted(opportunities, key=lambda x: x["edge"], reverse=True)

    def _log_rejection(self, city, question, bucket, model_prob, market_price, edge, reason):
        """Log EVERY rejection for analysis."""
        import json
        from pathlib import Path
        from datetime import datetime

        log_path = Path("logs/rejections.jsonl")
        log_path.parent.mkdir(exist_ok=True)

        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "ts": datetime.utcnow().isoformat(),
                    "city": city,
                    "question": question,
                    "bucket": bucket,
                    "model_prob": round(model_prob, 4),
                    "market_price": round(market_price, 4),
                    "edge": round(edge, 4),
                    "reason": reason
                }) + "\n")
        except (Exception,) as e:
            logging.getLogger(__name__).warning(f"Failed to log rejection: {e}")

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

    def _review_signal_with_ai(self, loc, snap: dict, signal: dict, unit_sym: str) -> dict:
        """Optionally review a candidate with Groq after deterministic filters pass."""
        config = self.engine.config
        if not getattr(config, "ai_flow_enabled", False):
            return {"allowed": True, "reason": "ai_disabled", "review": None}

        max_reviews = int(getattr(config, "ai_max_reviews_per_scan", 5))
        if self._ai_reviews_used >= max_reviews:
            if getattr(config, "ai_force_blocking", False):
                return {"allowed": False, "reason": "ai_budget_exhausted", "review": None}
            return {"allowed": True, "reason": "ai_budget_exhausted_nonblocking", "review": None}

        self._ai_reviews_used += 1
        unit = "F" if unit_sym == "°F" else "C"
        review, is_anomaly = get_ai_trade_context(loc.name, snap, signal, unit=unit)
        if review is None:
            if getattr(config, "ai_force_blocking", False):
                return {"allowed": False, "reason": "ai_unavailable", "review": None}
            return {"allowed": True, "reason": "ai_unavailable_nonblocking", "review": None}

        if is_anomaly:
            return {"allowed": False, "reason": "ai_anomaly", "review": review}

        if not self._ai_filters_pass(signal, review):
            return {"allowed": False, "reason": "ai_filter_failed", "review": review}

        return {"allowed": True, "reason": "ai_ok", "review": review}

    def _paper_signal_context(self) -> dict:
        """Return current reconciled paper accounting for signal notifications."""
        if not hasattr(self.engine.paper_account, "get_state"):
            return {}
        state = self.engine.paper_account.get_state()
        return {
            "paper_status": "PAPER_ONLY",
            "paper_balance": getattr(state, "balance", 0.0),
            "paper_equity": getattr(state, "equity", getattr(state, "balance", 0.0)),
            "paper_cash_pnl": getattr(state, "cash_pnl", 0.0),
            "paper_total_pnl": getattr(state, "total_pnl", 0.0),
            "paper_total_gains": getattr(state, "total_gains", 0.0),
            "paper_total_losses": getattr(state, "total_losses", 0.0),
            "paper_open_exposure": getattr(state, "locked_in_positions", 0.0),
            "paper_closed_trades": getattr(state, "closed_trades", getattr(state, "wins", 0) + getattr(state, "losses", 0)),
            "paper_open_trades": getattr(state, "open_trades", 0),
        }

    def _signal_filters_pass(self, signal: dict, quality_result: dict) -> bool:
        """Check if signal passes Signal flow filters."""
        config = self.engine.config
        paper_training = self._paper_training_enabled()
        min_quality = (
            getattr(config, "paper_training_min_quality_score", 0.25)
            if paper_training
            else getattr(config, "signal_min_quality_score", 0.40)
        )
        # Align with SignalQualityLayer.MIN_CONFIDENCE (0.50)
        min_confidence = (
            getattr(config, "paper_training_min_confidence", 0.25)
            if paper_training
            else getattr(config, "signal_min_confidence", 0.50)
        )
        min_edge = (
            getattr(config, "paper_training_min_ev", 0.02)
            if paper_training
            else getattr(config, "signal_min_edge", 0.01)
        )

        # Quality score filter
        if quality_result.get("score", 0) < min_quality:
            return False

        # ML confidence filter (aligned with SignalQualityLayer)
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
                # SYNC: Update state.balance to match paper_account after trade
                paper_state = (
                    self.engine.paper_account.get_state()
                    if hasattr(self.engine.paper_account, "get_state")
                    else self.engine.paper_account
                )
                if hasattr(paper_state, "balance"):
                    state.balance = paper_state.balance
                    balance = state.balance  # Also update local variable
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
            trade_context.update(self._paper_signal_context())
            trade_context["ai_status"] = "VALIDÉ_GROQ" if signal.get("ai") else "NON_REQUIS"
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
                ai_note=format_ai_note(signal.get("ai")),
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
            live_open = bool(market.position and market.position.get("status") == "open")
            paper_open = bool(market.paper_position and market.paper_position.get("status") in ("open", "paper"))
            if market.status == "resolved" or not (live_open or paper_open):
                continue

            new_balance, won, pnl = self.engine.resolve_market(market, balance)
            if won is None or pnl is None:
                continue

            balance = new_balance
            result.resolved += 1
            if live_open:
                if won:
                    state.wins += 1
                else:
                    state.losses += 1

            unit_sym = "°F" if market.unit == "F" else "°C"
            resolved_pos = market.position if live_open else market.paper_position
            bucket = f"{resolved_pos['bucket_low']}-{resolved_pos['bucket_high']}{unit_sym}"
            temp_str = f"{market.actual_temp}{unit_sym}" if market.actual_temp is not None else "N/A"

            if won:
                self.engine.feedback.notify_trade_win(market.city_name, market.date, bucket, pnl, temp_str, balance)
            else:
                self.engine.feedback.notify_trade_loss(market.city_name, market.date, bucket, pnl, balance)

            self.engine.emit(f"[{'WIN' if won else 'LOSS'}] {market.city_name} {market.date} | {pnl:+.2f}")
            self.engine.storage.save_market(market)
            time.sleep(0.3)


# Audit: Includes fee and slippage awareness
