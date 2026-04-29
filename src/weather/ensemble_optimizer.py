"""Bias-aware weather ensemble optimizer."""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SOURCE_PRIORS = {
    "hrrr": 1.25,
    "ecmwf": 1.20,
    "nws": 1.10,
    "dwd": 1.05,
    "gfs": 0.95,
    "metno": 0.85,
    "metar": 0.45,
}


@dataclass
class OptimizedForecast:
    temp: float
    sigma: float
    confidence: float
    weights: dict[str, float]
    primary_source: str


class EnsembleOptimizer:
    """Combine available weather sources using historical error and dispersion."""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.model = self._load_model()

    def _load_model(self) -> dict[str, Any]:
        path = self.data_dir / "ml_model.json"
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}

    def _stats_for(self, city: str, source: str) -> dict[str, float]:
        city_stats = self.model.get("by_city_source", {}).get(f"{city}:{source}") or {}
        if int(city_stats.get("n", 0) or 0) >= 5:
            return city_stats

        source_stats = self.model.get("by_source", {}).get(source) or {}
        if int(source_stats.get("n", 0) or 0) >= 10:
            return source_stats

        return {}

    def _source_weight(self, city: str, source: str) -> tuple[float, float, float]:
        stats = self._stats_for(city, source)
        mae = max(float(stats.get("mae", 1.8)), 0.25)
        bias = float(stats.get("bias", 0.0))
        n = int(stats.get("n", 0) or 0)
        sample_weight = min(1.0, n / 30.0) if n else 0.35
        prior = SOURCE_PRIORS.get(source, 0.75)
        return prior * sample_weight / (mae * mae), bias, mae

    def optimize(self, city: str, unit: str, snapshot: dict) -> OptimizedForecast | None:
        values = {
            source: snapshot.get(source)
            for source in ("ecmwf", "hrrr", "gfs", "dwd", "nws", "metno")
            if snapshot.get(source) is not None
        }
        if not values:
            return None

        weighted_sum = 0.0
        total_weight = 0.0
        raw_weights: dict[str, float] = {}
        adjusted_values: dict[str, float] = {}
        maes: list[float] = []

        for source, temp in values.items():
            weight, bias, mae = self._source_weight(city, source)
            adjusted = float(temp) - bias
            raw_weights[source] = weight
            adjusted_values[source] = adjusted
            weighted_sum += adjusted * weight
            total_weight += weight
            maes.append(mae)

        if total_weight <= 0:
            return None

        optimized_temp = weighted_sum / total_weight
        weights = {source: round(weight / total_weight, 4) for source, weight in raw_weights.items()}
        primary_source = max(weights, key=weights.get)

        dispersion = 0.0
        if len(adjusted_values) > 1:
            dispersion = math.sqrt(
                sum(weight * ((adjusted_values[source] - optimized_temp) ** 2) for source, weight in raw_weights.items())
                / total_weight
            )

        base_sigma = 2.0 if unit == "F" else 1.2
        mae_floor = min(maes) if maes else base_sigma
        sigma = max(base_sigma * 0.75, mae_floor * 0.65, dispersion)
        agreement = 1.0 / (1.0 + dispersion)
        coverage = min(1.0, len(values) / 3.0)
        confidence = max(0.1, min(0.95, agreement * coverage))

        return OptimizedForecast(
            temp=round(optimized_temp, 1 if unit == "C" else 0),
            sigma=round(sigma, 4),
            confidence=round(confidence, 4),
            weights=weights,
            primary_source=primary_source,
        )
