"""
Market Replay Engine: Reconstruct temporal market trajectories.
Generates synthetic scans from historical market data.
"""
from __future__ import annotations

import json
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional


def interpolate_temps(
    actual_temp: Optional[float],
    forecast_temps: list[float],
    target_scans: int,
    discovery_ts: datetime,
    resolution_ts: datetime,
) -> list[tuple[float, float, float]]:
    """Interpolate temperature trajectory with model predictions.
    
    Returns list of (forecast_temp, hours_remaining, confidence) tuples.
    """
    if not forecast_temps:
        forecast_temps = [actual_temp or 0]

    scan_interval = (resolution_ts - discovery_ts) / target_scans if target_scans > 0 else timedelta(hours=24)
    total_hours = (resolution_ts - discovery_ts).total_seconds() / 3600

    trajectory = []
    current_time = discovery_ts

    for i in range(target_scans):
        hours_remaining = total_hours - (i * scan_interval.total_seconds() / 3600)
        hours_remaining = max(0, hours_remaining)

        progress = i / max(target_scans - 1, 1)

        if actual_temp and forecast_temps:
            base_forecast = forecast_temps[min(i % len(forecast_temps), len(forecast_temps) - 1)]
            convergence = progress * 0.8
            interpolated = base_forecast * (1 - convergence) + (actual_temp or base_forecast) * convergence
            confidence = 0.3 + (1 - progress) * 0.5
        else:
            interpolated = forecast_temps[0] if forecast_temps else 0
            confidence = 0.4 - (progress * 0.2)

        trajectory.append((round(interpolated, 1), round(hours_remaining, 1), round(confidence, 2)))
        current_time += scan_interval

    return trajectory


def estimate_model_errors(
    forecast_temp: float,
    actual_temp: Optional[float],
    source: str,
    city: str,
    ml_stats: Optional[dict] = None,
) -> dict[str, float]:
    """Estimate model error based on historical MAE by source/city."""
    if not ml_stats:
        return {
            "mae": 2.0,
            "bias": 0.0,
            "confidence": 0.3,
        }

    city_key = f"{city}:{source}"
    stats = ml_stats.get("by_city_source", {}).get(city_key)

    if not stats:
        source_stats = ml_stats.get("by_source", {}).get(source)
        if source_stats:
            stats = source_stats
        else:
            default_maes = {
                "ecmwf": 2.0, "hrrr": 3.0, "gfs": 2.5,
                "dwd": 1.5, "nws": 2.0, "metar": 5.0,
            }
            mae = default_maes.get(source, 2.5)
            return {"mae": mae, "bias": 0.0, "confidence": 0.3}

    return {
        "mae": stats.get("mae", 2.0),
        "bias": stats.get("bias", 0.0),
        "confidence": stats.get("confidence", 0.4),
    }


def compute_market_regime_from_trajectory(
    price_history: list[float],
    entry_price: float,
) -> str:
    """Determine regime from price trajectory."""
    if not price_history:
        return "unknown"

    if len(price_history) < 2:
        return "stable"

    mean_price = sum(price_history) / len(price_history)
    variance = sum((p - mean_price) ** 2 for p in price_history) / len(price_history)
    std = math.sqrt(variance)

    if std > 0.15:
        return "volatile"
    elif abs(price_history[-1] - entry_price) > 0.1:
        return "trending"
    elif std > 0.05:
        return "mixed"
    else:
        return "stable"


def estimate_bucket_distribution(
    forecast_temp: float,
    mae: float,
    unit: str,
) -> dict[str, tuple[float, float, float]]:
    """Estimate probability distribution across buckets.
    
    Returns dict of bucket_name -> (low, high, prob)
    """
    bucket_width = 2 if unit == "F" else 1

    base_temp = round(forecast_temp / bucket_width) * bucket_width
    distribution = {}

    for offset in range(-3, 4):
        low = base_temp + offset * bucket_width
        high = low + bucket_width
        distance = abs(forecast_temp - (low + bucket_width / 2))

        prob = math.exp(-0.5 * (distance / mae) ** 2) if mae > 0 else 0.5
        distribution[f"{low}-{high}{unit}"] = (low, high, prob)

    total = sum(p for _, _, p in distribution.values())
    if total > 0:
        distribution = {k: (l, h, p / total) for k, (l, h, p) in distribution.items()}

    return distribution


class MarketReplayEngine:
    """Reconstruct temporal market trajectories for data scaling.
    
    Quality tags for integrity tracking:
    - source: "real" | "replay" | "synthetic" | "backfill"
    - replay_from: original market file (for replay)
    - synthetic_reasons: list of augmentation reasons
    - confidence: 0-1 quality score
    """

    REPLAY_TAG = "replay"
    SYNTHETIC_TAG = "synthetic"
    BACKFILL_TAG = "backfill"
    REAL_TAG = "real"

    def __init__(self, data_dir: str = "data", ml_stats: Optional[dict] = None):
        self.data_dir = Path(data_dir)
        self.markets_dir = self.data_dir / "markets"
        self.ml_stats = ml_stats or {}
        self._load_ml_stats()

    def _load_ml_stats(self) -> None:
        if not self.ml_stats:
            ml_path = self.data_dir / "ml_model.json"
            if ml_path.exists():
                try:
                    self.ml_stats = json.loads(ml_path.read_text(encoding="utf-8"))
                except Exception:
                    pass

    def replay_market(
        self,
        market: dict[str, Any],
        target_scans: int = 30,
        market_file_name: str = "",
    ) -> list[dict[str, Any]]:
        """Generate replayed scan history for a market.
        
        Args:
            market: Market JSON
            target_scans: Number of synthetic scans to generate
            market_file_name: Original market filename for tagging
        
        Returns list of replayed scan snapshots with metadata tags.
        """
        city = market.get("city", "")
        date = market.get("date", "")
        unit = market.get("unit", "C")
        actual_temp = market.get("actual_temp")
        station = market.get("station")

        position = market.get("position") or {}
        entry_price = position.get("entry_price", 0.5)
        market_id = position.get("market_id", "")

        existing_snapshots = market.get("forecast_snapshots", [])
        forecast_temps = [s.get("temp") for s in existing_snapshots if s.get("temp")]
        n_real_scans = len(existing_snapshots)

        created_str = market.get("created_at", datetime.now().isoformat())
        try:
            discovery_ts = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
        except Exception:
            discovery_ts = datetime.now()

        event_end_str = market.get("event_end_date")
        if event_end_str:
            try:
                resolution_ts = datetime.fromisoformat(event_end_str.replace("Z", "+00:00"))
            except Exception:
                resolution_ts = discovery_ts + timedelta(hours=24)
        else:
            resolution_ts = discovery_ts + timedelta(hours=24)

        trajectory = interpolate_temps(
            actual_temp, forecast_temps, target_scans, discovery_ts, resolution_ts
        )

        bucket_width = 2 if unit == "F" else 1
        all_outcomes = market.get("all_outcomes", [])
        top_bucket = None
        top_price = 0.5

        if all_outcomes:
            sorted_outcomes = sorted(all_outcomes, key=lambda x: x.get("volume", 0), reverse=True)
            if sorted_outcomes:
                top_bucket = sorted_outcomes[0].get("question", "").split(" be ")[-1].split(" on ")[0] if sorted_outcomes[0].get("question") else None
                top_price = sorted_outcomes[0].get("price", 0.5)

        position_bucket = None
        if position.get("bucket_low") and position.get("bucket_high"):
            position_bucket = f"{position.get('bucket_low')}-{position.get('bucket_high')}{unit}"

        replays = []
        price_evolution = []

        for i, (forecast_temp, hours_left, confidence) in enumerate(trajectory):
            if hours_left <= 0 and i < len(trajectory) - 1:
                continue

            progress = i / max(target_scans - 1, 1)

            source = "ecmwf"
            if i < n_real_scans:
                source = existing_snapshots[i].get("source", source)

            error_stats = estimate_model_errors(forecast_temp, actual_temp, source, city, self.ml_stats)

            bucket_low = round(forecast_temp / bucket_width) * bucket_width
            bucket_high = bucket_low + bucket_width
            bucket = f"{bucket_low}-{bucket_high}{unit}"

            price_trend = entry_price * (1 - 0.1 * progress)
            price_evolution.append(price_trend)

            gap = abs(1.0 - (forecast_temp / (actual_temp or forecast_temp + 1)))
            adjusted_price = price_trend + gap * 0.1 * (1 if forecast_temp > (actual_temp or forecast_temp) else -1)
            adjusted_price = max(0.01, min(0.99, adjusted_price))

            regime = compute_market_regime_from_trajectory(price_evolution, entry_price)

            bucket_dist = estimate_bucket_distribution(forecast_temp, error_stats["mae"], unit)

            is_synthetic = i >= n_real_scans
            scan_tag = self.REAL_TAG if not is_synthetic else self.REPLAY_TAG
            
            quality_confidence = 1.0 if not is_synthetic else max(0.6, 1.0 - (i - n_real_scans) * 0.02)
            
            replay = {
                "ts": (discovery_ts + timedelta(minutes=i * 60)).isoformat(),
                "horizon": _compute_horizon(hours_left),
                "hours_left": hours_left,
                "source": source,
                "temp": forecast_temp,
                "ecmwf": forecast_temp + (0.5 if source == "ecmwf" else 0),
                "hrrr": forecast_temp - (0.3 if source == "hrrr" else 0),
                "metar": actual_temp or forecast_temp - 2,
                "confidence": confidence,
                "mae": error_stats["mae"],
                "bias": error_stats["bias"],
                "forecast_market_gap": abs(adjusted_price - price_trend),
                "price_trend": price_trend,
                "adjusted_price": adjusted_price,
                "market_regime": regime,
                "bucket": bucket,
                "bucket_dist": bucket_dist,
                "metadata": {
                    "source": scan_tag,
                    "replay_from": market_file_name if is_synthetic else None,
                    "replay_scan_index": i if is_synthetic else None,
                    "real_scans_used": n_real_scans,
                    "quality_confidence": quality_confidence,
                    "is_synthetic": is_synthetic,
                },
            }
            replays.append(replay)

        return replays

    def replay_all_markets(self, target_scans: int = 30) -> list[dict[str, Any]]:
        """Replay all markets in data/markets/.
        
        Each replay is tagged with metadata for integrity tracking.
        """
        all_replays = []

        for market_file in self.markets_dir.glob("*.json"):
            try:
                market = json.loads(market_file.read_text(encoding="utf-8"))
                replays = self.replay_market(market, target_scans, market_file.name)
                city = market.get("city", "")
                date = market.get("date", "")
                position = market.get("position") or {}
                market_id = position.get("market_id") if position else ""
                unit = market.get("unit", "C")
                actual_temp = market.get("actual_temp")
                for i, replay in enumerate(replays):
                    replay["city"] = city
                    replay["date"] = date
                    replay["scan_index"] = i
                    replay["market_id"] = market_id
                    replay["unit"] = unit
                    replay["actual_temp"] = actual_temp
                    all_replays.append(replay)
            except Exception:
                continue

        return all_replays


def _compute_horizon(hours: float) -> str:
    if hours <= 6:
        return "D+0"
    elif hours <= 24:
        return "D+1"
    elif hours <= 48:
        return "D+2"
    else:
        return "D+3"


def get_replay_engine(data_dir: str = "data") -> MarketReplayEngine:
    """Factory function."""
    return MarketReplayEngine(data_dir=data_dir)