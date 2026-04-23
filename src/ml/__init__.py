"""
Minimal ML module for forecast calibration and scoring.
"""
from __future__ import annotations

import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Optional


MODEL_FILENAME = "ml_model.json"


def default_sigma(unit: str) -> float:
    """Return fallback sigma by unit."""
    return 2.0 if unit == "F" else 1.2


def model_path(data_dir: str = "data") -> Path:
    """Return the model storage path."""
    return Path(data_dir) / MODEL_FILENAME


def _summarize(errors: list[float], unit: str) -> Dict[str, Any]:
    """Summarize forecast errors."""
    n = len(errors)
    mae = sum(abs(e) for e in errors) / n
    bias = sum(errors) / n
    rmse = math.sqrt(sum(e * e for e in errors) / n)
    sigma = max(default_sigma(unit) * 0.5, mae)
    scale = 6.0 if unit == "F" else 3.0
    confidence = max(0.1, min(0.95, (1 - min(mae / scale, 0.85)) * min(1.0, n / 8)))
    return {
        "n": n,
        "mae": round(mae, 4),
        "bias": round(bias, 4),
        "rmse": round(rmse, 4),
        "sigma": round(sigma, 4),
        "confidence": round(confidence, 4),
    }


def train_model(data_dir: str = "data") -> Dict[str, Any]:
    """Train a lightweight calibration model from market history."""
    data_path = Path(data_dir) / "markets"
    groups: dict[tuple[str, str], list[float]] = defaultdict(list)
    source_groups: dict[str, list[float]] = defaultdict(list)
    units: dict[tuple[str, str], str] = {}
    source_units: dict[str, str] = {}
    cities_seen = set()
    samples = 0

    for market_file in data_path.glob("*.json"):
        try:
            market = json.loads(market_file.read_text(encoding="utf-8"))
        except Exception:
            continue

        actual_temp = market.get("actual_temp")
        city = market.get("city")
        unit = market.get("unit", "C")
        if actual_temp is None or not city:
            continue

        cities_seen.add(city)
        for snap in market.get("forecast_snapshots", []):
            src = snap.get("source")
            temp = snap.get("temp")
            if src is None or temp is None:
                continue
            error = float(temp) - float(actual_temp)
            groups[(city, src)].append(error)
            source_groups[src].append(error)
            units[(city, src)] = unit
            source_units[src] = unit
            samples += 1

    by_city_source = {}
    for key, errors in groups.items():
        city, src = key
        by_city_source[f"{city}:{src}"] = _summarize(errors, units[key])

    by_source = {}
    for src, errors in source_groups.items():
        by_source[src] = _summarize(errors, source_units[src])

    model = {
        "version": 1,
        "samples": samples,
        "cities": len(cities_seen),
        "by_city_source": by_city_source,
        "by_source": by_source,
    }

    path = model_path(data_dir)
    path.write_text(json.dumps(model, indent=2, ensure_ascii=False), encoding="utf-8")
    return model


def load_model(data_dir: str = "data") -> Optional[Dict[str, Any]]:
    """Load a trained model if present."""
    path = model_path(data_dir)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def score_forecast(
    city: str,
    source: Optional[str],
    forecast_temp: float,
    unit: str,
    model: Optional[Dict[str, Any]] = None,
    data_dir: str = "data",
) -> Dict[str, Any]:
    """Score a forecast using trained calibration stats."""
    model = model or load_model(data_dir)
    if not model or not source:
        return {
            "adjusted_temp": forecast_temp,
            "sigma": default_sigma(unit),
            "confidence": 0.2,
            "bias": 0.0,
            "mae": default_sigma(unit),
            "n": 0,
            "tier": "default",
        }

    city_key = f"{city}:{source}"
    stats = model.get("by_city_source", {}).get(city_key)
    tier = "city_source"
    if not stats:
        stats = model.get("by_source", {}).get(source)
        tier = "source"

    if not stats:
        return {
            "adjusted_temp": forecast_temp,
            "sigma": default_sigma(unit),
            "confidence": 0.2,
            "bias": 0.0,
            "mae": default_sigma(unit),
            "n": 0,
            "tier": "default",
        }

    bias = float(stats.get("bias", 0.0))
    return {
        "adjusted_temp": round(float(forecast_temp) - bias, 2),
        "sigma": float(stats.get("sigma", default_sigma(unit))),
        "confidence": float(stats.get("confidence", 0.2)),
        "bias": bias,
        "mae": float(stats.get("mae", default_sigma(unit))),
        "n": int(stats.get("n", 0)),
        "tier": tier,
    }
