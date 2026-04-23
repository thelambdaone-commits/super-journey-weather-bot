"""
Versioned dataset schema for model and backtest inputs.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


SCHEMA_VERSION = "2.0"


@dataclass
class DatasetRow:
    """Immutable dataset row for one market event, decision or resolution."""

    version: str
    event_type: str
    action: str
    city: str
    date: str
    timestamp: int
    market_id: str | None
    question: str | None
    forecast_source: str | None
    forecast_horizon: str | None
    ecmwf_max: float | None
    hrrr_max: float | None
    gfs_max: float | None
    ensemble_mean: float | None
    ensemble_std: float | None
    forecast_spread: float | None
    forecast_temp: float | None
    raw_forecast_temp: float | None
    market_price: float | None
    market_implied_prob: float | None
    liquidity: float | None
    spread: float | None
    top_market_price: float | None
    top_bucket: str | None
    orderbook_depth: float | None
    raw_prob: float | None
    calibrated_prob: float | None
    confidence: float | None
    adjusted_ev: float | None
    raw_ev: float | None
    kelly: float | None
    decision_size: float | None
    decision_reason: str | None
    lat: float
    lon: float
    day_of_year: int
    hours_to_resolution: float | None
    actual_temp: float | None
    bucket: str | None
    actual_bucket: str | None
    resolution_outcome: str | None
    live_mode: bool
    paper_mode: bool
    signal_mode: bool

    def to_dict(self) -> dict[str, Any]:
        """Return a serializable row payload."""
        return asdict(self)
