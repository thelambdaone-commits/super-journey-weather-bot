"""
Model adapter around the current lightweight ML scorer.
"""
from __future__ import annotations

from ..ml import score_forecast


class ForecastModel:
    """Thin wrapper over the current calibration model."""

    def __init__(self, model: dict | None, data_dir: str):
        self.model = model
        self.data_dir = data_dir

    def score(self, city: str, source: str | None, forecast_temp: float, unit: str) -> dict:
        """Score one forecast snapshot."""
        return score_forecast(
            city,
            source,
            forecast_temp,
            unit,
            model=self.model,
            data_dir=self.data_dir,
        )

