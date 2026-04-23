"""
Backfill dataset builder from stored market history.
"""
from __future__ import annotations

from datetime import datetime, timezone

from ...features.weather_features import build_weather_features
from ...weather.locations import LOCATIONS
from ..schema import DatasetRow, SCHEMA_VERSION
from ..storage import DatasetStorage
from .aligner import align_snapshots
from .fetch_forecasts import load_market_snapshots


def _day_of_year(date_str: str | None) -> int:
    if not date_str:
        return datetime.now(timezone.utc).timetuple().tm_yday
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").timetuple().tm_yday
    except Exception:
        return datetime.now(timezone.utc).timetuple().tm_yday


def _actual_bucket(market: dict) -> str | None:
    temp = market.get("actual_temp")
    unit = market.get("unit")
    if temp is None or not unit:
        return None
    return f"{temp}{unit}"


def build_historical_dataset(
    markets_dir: str = "data/markets",
    data_dir: str = "data",
    filename: str = "dataset_rows.jsonl",
) -> dict:
    """Rebuild append-only dataset rows from stored historical market files."""
    storage = DatasetStorage(data_dir=data_dir, filename=filename)
    rebuilt = 0
    skipped = 0
    resolved = 0

    storage.path.write_text("", encoding="utf-8")
    for market in load_market_snapshots(markets_dir):
        city = market.get("city")
        location = LOCATIONS.get(city)
        if not city or location is None:
            skipped += 1
            continue

        market_id = None
        position = market.get("position") or {}
        market_id = position.get("market_id")
        aligned_snapshots = align_snapshots(market)
        if not aligned_snapshots:
            skipped += 1
            continue

        decision_written = False
        resolution_written = False
        for snapshot in aligned_snapshots:
            weather = build_weather_features(snapshot)
            bucket = None
            if position.get("bucket_low") is not None and position.get("bucket_high") is not None:
                bucket = f"{position.get('bucket_low')}-{position.get('bucket_high')}{market.get('unit', '')}"

            row = DatasetRow(
                version=SCHEMA_VERSION,
                event_type="historical_snapshot",
                action="HOLD" if position else "OBSERVE",
                city=city,
                date=market.get("date", ""),
                timestamp=int(datetime.now(timezone.utc).timestamp()),
                market_id=market_id,
                question=position.get("question"),
                forecast_source=snapshot.get("source"),
                forecast_horizon=snapshot.get("horizon"),
                ecmwf_max=snapshot.get("ecmwf"),
                hrrr_max=snapshot.get("hrrr"),
                gfs_max=snapshot.get("dwd"),
                ensemble_mean=weather.get("ensemble_mean"),
                ensemble_std=weather.get("ensemble_std"),
                forecast_spread=weather.get("forecast_spread"),
                forecast_temp=snapshot.get("temp"),
                raw_forecast_temp=snapshot.get("temp"),
                market_price=position.get("entry_price"),
                market_implied_prob=position.get("entry_price"),
                liquidity=None,
                spread=position.get("spread"),
                top_market_price=None,
                top_bucket=None,
                orderbook_depth=None,
                raw_prob=position.get("raw_prob"),
                calibrated_prob=position.get("p"),
                confidence=(position.get("ml") or {}).get("confidence"),
                adjusted_ev=position.get("ev"),
                raw_ev=position.get("raw_ev"),
                kelly=position.get("kelly"),
                decision_size=position.get("cost"),
                decision_reason=position.get("close_reason"),
                lat=location.lat,
                lon=location.lon,
                day_of_year=_day_of_year(market.get("date")),
                hours_to_resolution=snapshot.get("hours_left"),
                actual_temp=market.get("actual_temp"),
                bucket=bucket,
                actual_bucket=_actual_bucket(market),
                resolution_outcome=market.get("resolved_outcome"),
                live_mode=False,
                paper_mode=False,
                signal_mode=False,
            )
            storage.append(row)
            rebuilt += 1

            if position and not decision_written:
                decision_row = DatasetRow(
                    version=SCHEMA_VERSION,
                    event_type="decision",
                    action="BUY" if position else "OBSERVE",
                    city=city,
                    date=market.get("date", ""),
                    timestamp=int(datetime.now(timezone.utc).timestamp()),
                    market_id=market_id,
                    question=position.get("question"),
                    forecast_source=position.get("forecast_src") or snapshot.get("source"),
                    forecast_horizon=snapshot.get("horizon"),
                    ecmwf_max=snapshot.get("ecmwf"),
                    hrrr_max=snapshot.get("hrrr"),
                    gfs_max=snapshot.get("dwd"),
                    ensemble_mean=weather.get("ensemble_mean"),
                    ensemble_std=weather.get("ensemble_std"),
                    forecast_spread=weather.get("forecast_spread"),
                    forecast_temp=position.get("forecast_temp"),
                    raw_forecast_temp=snapshot.get("temp"),
                    market_price=position.get("entry_price"),
                    market_implied_prob=position.get("entry_price"),
                    liquidity=position.get("features", {}).get("liquidity"),
                    spread=position.get("spread"),
                    top_market_price=position.get("features", {}).get("top_market_price"),
                    top_bucket=position.get("features", {}).get("top_bucket"),
                    orderbook_depth=position.get("features", {}).get("liquidity"),
                    raw_prob=position.get("raw_prob"),
                    calibrated_prob=position.get("p"),
                    confidence=(position.get("ml") or {}).get("confidence"),
                    adjusted_ev=position.get("ev"),
                    raw_ev=position.get("raw_ev"),
                    kelly=position.get("kelly"),
                    decision_size=position.get("cost"),
                    decision_reason="historical_backfill",
                    lat=location.lat,
                    lon=location.lon,
                    day_of_year=_day_of_year(market.get("date")),
                    hours_to_resolution=snapshot.get("hours_left"),
                    actual_temp=None,
                    bucket=bucket,
                    actual_bucket=None,
                    resolution_outcome=None,
                    live_mode=False,
                    paper_mode=False,
                    signal_mode=False,
                )
                storage.append(decision_row)
                rebuilt += 1
                decision_written = True

        if position and market.get("actual_temp") is not None and not resolution_written:
            resolution_snapshot = aligned_snapshots[-1]
            weather = build_weather_features(resolution_snapshot)
            bucket = None
            if position.get("bucket_low") is not None and position.get("bucket_high") is not None:
                bucket = f"{position.get('bucket_low')}-{position.get('bucket_high')}{market.get('unit', '')}"
            resolution_row = DatasetRow(
                version=SCHEMA_VERSION,
                event_type="resolution",
                action="RESOLVE",
                city=city,
                date=market.get("date", ""),
                timestamp=int(datetime.now(timezone.utc).timestamp()),
                market_id=market_id,
                question=position.get("question"),
                forecast_source=position.get("forecast_src") or resolution_snapshot.get("source"),
                forecast_horizon=resolution_snapshot.get("horizon"),
                ecmwf_max=resolution_snapshot.get("ecmwf"),
                hrrr_max=resolution_snapshot.get("hrrr"),
                gfs_max=resolution_snapshot.get("dwd"),
                ensemble_mean=weather.get("ensemble_mean"),
                ensemble_std=weather.get("ensemble_std"),
                forecast_spread=weather.get("forecast_spread"),
                forecast_temp=position.get("forecast_temp"),
                raw_forecast_temp=resolution_snapshot.get("temp"),
                market_price=position.get("entry_price"),
                market_implied_prob=position.get("entry_price"),
                liquidity=position.get("features", {}).get("liquidity"),
                spread=position.get("spread"),
                top_market_price=position.get("features", {}).get("top_market_price"),
                top_bucket=position.get("features", {}).get("top_bucket"),
                orderbook_depth=position.get("features", {}).get("liquidity"),
                raw_prob=position.get("raw_prob"),
                calibrated_prob=position.get("p"),
                confidence=(position.get("ml") or {}).get("confidence"),
                adjusted_ev=position.get("ev"),
                raw_ev=position.get("raw_ev"),
                kelly=position.get("kelly"),
                decision_size=position.get("cost"),
                decision_reason="historical_resolution",
                lat=location.lat,
                lon=location.lon,
                day_of_year=_day_of_year(market.get("date")),
                hours_to_resolution=0.0,
                actual_temp=market.get("actual_temp"),
                bucket=bucket,
                actual_bucket=_actual_bucket(market),
                resolution_outcome=market.get("resolved_outcome"),
                live_mode=False,
                paper_mode=False,
                signal_mode=False,
            )
            storage.append(resolution_row)
            rebuilt += 1
            resolution_written = True

        if market.get("actual_temp") is not None:
            resolved += 1

    return {
        "rebuilt_rows": rebuilt,
        "skipped_markets": skipped,
        "resolved_markets": resolved,
        "output": str(storage.path),
    }
