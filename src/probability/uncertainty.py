"""
Uncertainty estimation helpers.
"""
from __future__ import annotations


def uncertainty_penalty(features: dict) -> float:
    """Estimate a simple uncertainty penalty from forecast disagreement."""
    ensemble_std = features.get("ensemble_std")
    forecast_spread = features.get("forecast_spread")
    penalty = 0.0
    if ensemble_std is not None:
        penalty += min(float(ensemble_std) * 0.02, 0.15)
    if forecast_spread is not None:
        penalty += min(float(forecast_spread) * 0.01, 0.10)
    return round(penalty, 4)

