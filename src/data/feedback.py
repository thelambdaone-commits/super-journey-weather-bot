"""
Rich feedback logging for market events, decisions and resolutions.
"""
from __future__ import annotations

from datetime import datetime, timezone

from .schema import DatasetRow, SCHEMA_VERSION
from .storage import DatasetStorage


class FeedbackRecorder:
    """Append-only feedback loop for dataset generation."""

    def __init__(self, storage: DatasetStorage):
        self.storage = storage

    def record_decision(
        self,
        *,
        market,
        location,
        modes,
        snapshot: dict,
        features: dict,
        signal: dict | None,
        probability_estimate,
        edge_estimate,
        action: str,
        reason: str,
        horizon: str,
        outcome: dict | None = None,
    ) -> None:
        """Record a pre-resolution market decision snapshot."""
        target = outcome or {}
        row = DatasetRow(
            version=SCHEMA_VERSION,
            event_type="decision",
            action=action,
            city=market.city,
            date=market.date,
            timestamp=int(datetime.now(timezone.utc).timestamp()),
            market_id=target.get("market_id"),
            question=target.get("question"),
            forecast_source=snapshot.get("best_source"),
            forecast_horizon=horizon,
            ecmwf_max=snapshot.get("ecmwf"),
            hrrr_max=snapshot.get("hrrr"),
            gfs_max=snapshot.get("gfs"),
            ensemble_mean=features.get("ensemble_mean"),
            ensemble_std=features.get("ensemble_std"),
            forecast_spread=features.get("forecast_spread"),
            forecast_temp=None if probability_estimate is None else probability_estimate.adjusted_temp,
            raw_forecast_temp=snapshot.get("best"),
            market_price=target.get("ask") if target else features.get("market_price"),
            market_implied_prob=target.get("ask") if target else features.get("market_implied_prob"),
            liquidity=features.get("liquidity"),
            spread=target.get("spread") if target else features.get("spread"),
            top_market_price=features.get("top_market_price"),
            top_bucket=features.get("top_bucket"),
            orderbook_depth=features.get("liquidity"),
            raw_prob=None if signal is None else signal.get("raw_prob"),
            calibrated_prob=None if signal is None else signal.get("p"),
            confidence=None if probability_estimate is None else probability_estimate.confidence,
            adjusted_ev=None if edge_estimate is None else edge_estimate.adjusted_ev,
            raw_ev=None if edge_estimate is None else edge_estimate.raw_ev,
            kelly=None if signal is None else signal.get("kelly"),
            decision_size=None if signal is None else signal.get("cost"),
            decision_reason=reason,
            lat=location.lat,
            lon=location.lon,
            day_of_year=datetime.now(timezone.utc).timetuple().tm_yday,
            hours_to_resolution=features.get("hours_to_resolution"),
            actual_temp=None,
            bucket=None
            if signal is None
            else f"{signal.get('bucket_low')}-{signal.get('bucket_high')}{location.unit}",
            actual_bucket=None,
            resolution_outcome=None,
            live_mode=modes.live_trade,
            paper_mode=modes.paper_mode,
            signal_mode=modes.signal_mode,
        )
        self.storage.append(row)

    def record_resolution(
        self,
        *,
        market,
        location,
        modes,
        pos: dict,
        outcome: str,
    ) -> None:
        """Record a post-resolution dataset row."""
        features = pos.get("features", {})
        row = DatasetRow(
            version=SCHEMA_VERSION,
            event_type="resolution",
            action="RESOLVE",
            city=market.city,
            date=market.date,
            timestamp=int(datetime.now(timezone.utc).timestamp()),
            market_id=pos.get("market_id"),
            question=pos.get("question"),
            forecast_source=pos.get("forecast_src"),
            forecast_horizon=None,
            ecmwf_max=features.get("ecmwf_max"),
            hrrr_max=features.get("hrrr_max"),
            gfs_max=features.get("gfs_max"),
            ensemble_mean=features.get("ensemble_mean"),
            ensemble_std=features.get("ensemble_std"),
            forecast_spread=features.get("forecast_spread"),
            forecast_temp=pos.get("forecast_temp"),
            raw_forecast_temp=pos.get("raw_forecast_temp"),
            market_price=pos.get("entry_price"),
            market_implied_prob=pos.get("entry_price"),
            liquidity=features.get("liquidity"),
            spread=pos.get("spread"),
            top_market_price=features.get("top_market_price"),
            top_bucket=features.get("top_bucket"),
            orderbook_depth=features.get("liquidity"),
            raw_prob=pos.get("raw_prob"),
            calibrated_prob=pos.get("p"),
            confidence=pos.get("ml", {}).get("confidence"),
            adjusted_ev=pos.get("ev"),
            raw_ev=pos.get("raw_ev"),
            kelly=pos.get("kelly"),
            decision_size=pos.get("cost"),
            decision_reason=pos.get("close_reason"),
            lat=location.lat,
            lon=location.lon,
            day_of_year=datetime.now(timezone.utc).timetuple().tm_yday,
            hours_to_resolution=0.0,
            actual_temp=market.actual_temp,
            bucket=f"{pos.get('bucket_low')}-{pos.get('bucket_high')}{location.unit}",
            actual_bucket=f"{market.actual_temp}{location.unit}" if market.actual_temp is not None else None,
            resolution_outcome=outcome,
            live_mode=modes.live_trade,
            paper_mode=modes.paper_mode,
            signal_mode=modes.signal_mode,
        )
        self.storage.append(row)
