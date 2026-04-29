"""
Weather feature builders.
"""
from __future__ import annotations

import math


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _std(values: list[float]) -> float | None:
    if len(values) < 2:
        return 0.0 if values else None
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return math.sqrt(variance)


def build_weather_features(snapshot: dict) -> dict:
    """Build normalized weather features from a forecast snapshot."""
    values = [
        snapshot.get("ecmwf"), 
        snapshot.get("hrrr"), 
        snapshot.get("gfs"), 
        snapshot.get("dwd"), 
        snapshot.get("nws"),
        snapshot.get("metno"),
    ]
    forecasts = [float(value) for value in values if value is not None]
    ensemble_mean = _mean(forecasts)
    ensemble_std = _std(forecasts)
    if forecasts:
        spread = max(forecasts) - min(forecasts)
    else:
        spread = None

    return {
        "ecmwf_max": snapshot.get("ecmwf"),
        "hrrr_max": snapshot.get("hrrr"),
        "gfs_max": snapshot.get("gfs"),
        "dwd_max": snapshot.get("dwd"),
        "metno_max": snapshot.get("metno"),
        "optimal_max": snapshot.get("optimal"),
        "optimal_sigma": snapshot.get("optimal_sigma"),
        "optimal_confidence": snapshot.get("optimal_confidence"),
        "optimal_weights": snapshot.get("optimal_weights"),
        "ensemble_mean": round(ensemble_mean, 4) if ensemble_mean is not None else None,
        "ensemble_std": round(ensemble_std, 4) if ensemble_std is not None else None,
        "forecast_spread": round(spread, 4) if spread is not None else None,
    }
