"""
Version 3.0 schema for ML-ready temporal dataset.
Row per market per scan = temporal market intelligence system.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Optional


SCHEMA_VERSION_V3 = "3.0"
SCAN_MAX_DEPTH = 100

CITY_COORDS = {
    "nyc": (40.7772, -73.8726),
    "chicago": (41.9742, -87.9073),
    "miami": (25.7959, -80.2870),
    "dallas": (32.8471, -96.8518),
    "seattle": (47.4502, -122.3088),
    "atlanta": (33.6407, -84.4277),
    "london": (51.5048, 0.0495),
    "paris": (48.9962, 2.5979),
    "munich": (48.3537, 11.7750),
    "ankara": (40.1281, 32.9951),
    "buenos-aires": (-34.6083, -58.3732),
    "sao-paulo": (-23.5475, -46.6361),
    "singapore": (1.3521, 103.8198),
    "tokyo": (35.5494, 139.7794),
    "shanghai": (31.2304, 121.4737),
    "seoul": (37.5665, 126.9780),
    "wellington": (-41.2865, 174.7762),
    "lucknow": (26.8467, 80.9462),
    "toronto": (43.6532, -79.3832),
    "tel-aviv": (32.0853, 34.7818),
}


def get_city_coords(city: str) -> tuple[float, float]:
    """Get lat/lon for a city."""
    return CITY_COORDS.get(city, (0.0, 0.0))


@dataclass
class DatasetRowV3:
    """Immutable dataset row for temporal market analysis.
    
    Unit: one row = one market at one point in time (scan index).
    Enables learning of:
    - forecast convergence dynamics
    - edge decay over time
    - market reaction to weather information
    """

    event_type: str
    action: str
    city: str
    city_name: str
    date: str
    unit: str
    lat: float
    lon: float
    day_of_year: int

    market_id: Optional[str] = None
    question: Optional[str] = None
    station: Optional[str] = None

    scan_index: int = 0
    scan_sequence_id: str = ""

    forecast_source: Optional[str] = None
    forecast_horizon: Optional[str] = None
    forecast_temp: Optional[float] = None
    raw_forecast_temp: Optional[float] = None

    ecmwf_max: Optional[float] = None
    hrrr_max: Optional[float] = None
    gfs_max: Optional[float] = None
    dwd_max: Optional[float] = None
    nws_max: Optional[float] = None
    metar_max: Optional[float] = None

    ensemble_mean: Optional[float] = None
    ensemble_std: Optional[float] = None
    forecast_spread: Optional[float] = None

    previous_forecast_temp: Optional[float] = None
    forecast_delta: Optional[float] = None

    model_confidence_score: Optional[float] = None
    city_source_mae: Optional[float] = None
    city_source_bias: Optional[float] = None
    source_tier: str = "unknown"

    market_price: Optional[float] = None
    market_implied_prob: Optional[float] = None
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None
    liquidity: Optional[float] = None
    spread: Optional[float] = None
    volume: Optional[float] = None

    market_regime: str = "unknown"
    price_movement_since_discovery: Optional[float] = None
    top_bucket: Optional[str] = None
    top_bucket_price: Optional[float] = None
    top_bucket_evolution: list[str] = field(default_factory=list)

    raw_prob: Optional[float] = None
    calibrated_prob: Optional[float] = None
    forecast_market_gap: Optional[float] = None

    adjusted_ev: Optional[float] = None
    raw_ev: Optional[float] = None

    kelly: Optional[float] = None
    decision_size: Optional[float] = None
    decision_reason: Optional[str] = None

    bucket_low: Optional[float] = None
    bucket_high: Optional[float] = None
    bucket_width: Optional[float] = None
    bucket: Optional[str] = None

    hours_to_resolution: Optional[float] = None
    hours_at_discovery: Optional[float] = None

    timestamp: int = 0
    scan_ts: Optional[str] = None

    actual_temp: Optional[float] = None
    actual_bucket: Optional[str] = None

    resolution_outcome: Optional[str] = None
    resolution_confidence: Optional[float] = None

    realized_edge: Optional[float] = None
    realized_pnl: Optional[float] = None

    live_mode: bool = False
    paper_mode: bool = False
    signal_mode: bool = False

    entry_price: Optional[float] = None
    shares: Optional[float] = None
    cost: Optional[float] = None

    created_at: Optional[str] = None
    event_end_date: Optional[str] = None

    metadata: dict[str, Any] = field(default_factory=dict)
    version: str = SCHEMA_VERSION_V3

    def to_dict(self) -> dict[str, Any]:
        """Return serializable row payload."""
        return asdict(self)

    @classmethod
    def from_market_json(
        cls,
        market: dict[str, Any],
        scan_index: int,
        scan_snapshot: Optional[dict] = None,
        market_snapshot: Optional[dict] = None,
        ml_stats: Optional[dict] = None,
        previous_snapshot: Optional[dict] = None,
    ) -> DatasetRowV3:
        """Build a V3 row from market JSON at a specific scan point."""
        
        city = market.get("city", "")
        city_name = market.get("city_name", city)
        date = market.get("date", "")
        unit = market.get("unit", "C")
        station = market.get("station")
        market_id = market.get("position", {}).get("market_id") if market.get("position") else None
        question = market.get("position", {}).get("question") if market.get("position") else None

        snapshot = scan_snapshot or {}
        market_snap = market_snapshot or {}

        ecmwf = snapshot.get("ecmwf")
        hrrr = snapshot.get("hrrr")
        gfs = snapshot.get("gfs")
        dwd = snapshot.get("dwd")
        nws = snapshot.get("nws")
        metar = snapshot.get("metar")

        temps = [t for t in [ecmwf, hrrr, gfs, dwd, nws, metar] if t is not None]
        ensemble_mean = sum(temps) / len(temps) if temps else None
        ensemble_std = (
            (sum((t - ensemble_mean) ** 2 for t in temps) / len(temps)) ** 0.5
            if len(temps) > 1 else 0.0
        )
        forecast_spread = max(temps) - min(temps) if len(temps) > 1 else 0.0

        primary_source = snapshot.get("source", "ecmwf")
        forecast_temp = snapshot.get("temp", ensemble_mean)

        prev_temp = previous_snapshot.get("temp") if previous_snapshot else None
        forecast_delta = None
        if prev_temp is not None and forecast_temp is not None:
            forecast_delta = forecast_temp - prev_temp

        city_source_key = f"{city}:{primary_source}"
        source_stats = ml_stats.get("by_city_source", {}).get(city_source_key) if ml_stats else None
        if not source_stats:
            source_stats = ml_stats.get("by_source", {}).get(primary_source) if ml_stats else None
            source_tier = "source" if source_stats else "unknown"
        else:
            source_tier = "city_source"

        city_source_mae = source_stats.get("mae") if source_stats else None
        city_source_bias = source_stats.get("bias") if source_stats else None
        model_confidence_score = source_stats.get("confidence") if source_stats else None

        position = market.get("position") or {}
        entry_price = position.get("entry_price") if position else None
        market_price = market_snap.get("top_bucket_price") or entry_price
        calibrated_prob = position.get("p") if position else None

        bid = market_snap.get("bid", market.get("all_outcomes", [{}])[0].get("bid")) if market.get("all_outcomes") else None
        ask = market_snap.get("ask", market.get("all_outcomes", [{}])[0].get("ask")) if market.get("all_outcomes") else None
        spread_val = position.get("spread") if position else None
        volume = market.get("all_outcomes", [{}])[0].get("volume") if market.get("all_outcomes") else None

        bucket_low = position.get("bucket_low") if position else None
        bucket_high = position.get("bucket_high") if position else None
        bucket_width = (bucket_high - bucket_low) if (bucket_low and bucket_high) else None

        top_bucket = market_snap.get("top_bucket")

        price_movement = None
        if entry_price and market_price:
            price_movement = market_price - entry_price

        forecast_market_gap = None
        if calibrated_prob is not None and market_price is not None:
            forecast_market_gap = abs(calibrated_prob - market_price)

        hours_left = snapshot.get("hours_left", market.get("hours_at_discovery", 24))
        
        try:
            dt = datetime.fromisoformat(snapshot.get("ts", datetime.now().isoformat()))
            day_of_year = dt.timetuple().tm_yday
            timestamp = int(dt.timestamp())
            scan_ts = snapshot.get("ts")
        except (Exception,) as e:
            day_of_year = 0
            timestamp = 0
            scan_ts = None

        lat, lon = get_city_coords(city)

        return cls(
            event_type="market_scan",
            action="OBSERVE",
            city=city,
            city_name=city_name,
            date=date,
            unit=unit,
            lat=lat,
            lon=lon,
            day_of_year=day_of_year,
            station=station,
            market_id=market_id,
            question=question,
            scan_index=scan_index,
            forecast_source=primary_source,
            forecast_horizon=snapshot.get("horizon"),
            forecast_temp=forecast_temp,
            raw_forecast_temp=snapshot.get("temp"),
            ecmwf_max=ecmwf,
            hrrr_max=hrrr,
            gfs_max=gfs,
            dwd_max=dwd,
            nws_max=nws,
            metar_max=metar,
            ensemble_mean=ensemble_mean,
            ensemble_std=ensemble_std,
            forecast_spread=forecast_spread,
            previous_forecast_temp=prev_temp,
            forecast_delta=forecast_delta,
            model_confidence_score=model_confidence_score,
            city_source_mae=city_source_mae,
            city_source_bias=city_source_bias,
            source_tier=source_tier,
            market_price=market_price,
            market_implied_prob=market_price,
            bid_price=bid,
            ask_price=ask,
            liquidity=volume,
            spread=spread_val,
            volume=volume,
            market_regime="stable",
            price_movement_since_discovery=price_movement,
            top_bucket=top_bucket,
            top_bucket_price=market_snap.get("top_price"),
            raw_prob=calibrated_prob,
            calibrated_prob=calibrated_prob,
            forecast_market_gap=forecast_market_gap,
            adjusted_ev=position.get("ev") if position else None,
            raw_ev=position.get("ev") if position else None,
            kelly=position.get("kelly") if position else None,
            decision_size=position.get("cost") if position else None,
            bucket_low=bucket_low,
            bucket_high=bucket_high,
            bucket_width=bucket_width,
            bucket=f"{bucket_low}-{bucket_high}{unit}" if bucket_low and bucket_high else None,
            hours_to_resolution=hours_left,
            hours_at_discovery=market.get("hours_at_discovery"),
            timestamp=timestamp,
            scan_ts=scan_ts,
            actual_temp=market.get("actual_temp"),
            actual_bucket=f"{market.get('actual_temp')}{unit}" if market.get("actual_temp") else None,
            resolution_outcome=market.get("resolved_outcome"),
            entry_price=entry_price,
            shares=position.get("shares") if position else None,
            cost=position.get("cost") if position else None,
            live_mode=market.get("live_mode", False),
            paper_mode=market.get("paper_mode", False),
            signal_mode=market.get("signal_mode", False),
            created_at=market.get("created_at"),
            event_end_date=market.get("event_end_date"),
        )


def validate_row_v3(payload: dict) -> DatasetRowV3:
    """Validate and construct a V3 row from stored payload."""
    if payload.get("version") != SCHEMA_VERSION_V3:
        raise ValueError(f"expected schema {SCHEMA_VERSION_V3}, got {payload.get('version')}")
    return DatasetRowV3(**payload)


def load_rows_v3(path: str | Path) -> list[DatasetRowV3]:
    """Load V3 dataset rows from JSONL storage."""
    from pathlib import Path
    import json
    
    rows: list[DatasetRowV3] = []
    file_path = Path(path)
    if not file_path.exists():
        return rows
    for line in file_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(validate_row_v3(json.loads(line)))
    return rows