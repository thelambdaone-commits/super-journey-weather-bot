"""
Probability engine.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..weather.math import bucket_prob
from .calibration import CalibrationEngine, CalibrationValidator
from .model import ForecastModel


@dataclass
class ProbabilityEstimate:
    """Probability output for one candidate market outcome."""

    adjusted_temp: float
    sigma: float
    confidence: float
    probability: float
    bias: float
    mae: float
    n: int
    tier: str


class ProbabilityEngine:
    """Compose model scoring and probability calibration."""

    def __init__(self, model: dict | None, data_dir: str):
        self.model = ForecastModel(model, data_dir)
        self.calibration_path = Path(data_dir) / "calibration.pkl"
        self.calibrator = CalibrationEngine(method="isotonic")
        self.calibrator.load(str(self.calibration_path))

    def fit_calibration(self, y_prob, y_true) -> bool:
        """Fit and persist calibration from labeled outcomes."""
        validator = CalibrationValidator()
        accepted_candidate = None
        for method in ("isotonic", "platt"):
            candidate = CalibrationEngine(method=method)
            report = validator.validate(candidate, y_prob, y_true)
            if report.accepted:
                candidate.fit(y_prob, y_true)
                if candidate.fitted:
                    accepted_candidate = candidate
                    break
        if accepted_candidate is None:
            return False
        self.calibrator = accepted_candidate
        self.calibrator.save(str(self.calibration_path))
        return True

    def calibration_report(self, y_prob, y_true) -> dict:
        """Return calibration metrics for monitoring."""
        return self.calibrator.evaluate(y_prob, y_true)

    def estimate_bucket(
        self,
        city: str,
        source: str | None,
        forecast_temp: float,
        unit: str,
        t_low: float,
        t_high: float,
    ) -> ProbabilityEstimate:
        """Estimate calibrated bucket probability."""
        score = self.model.score(city, source, forecast_temp, unit)
        raw_probability = bucket_prob(score["adjusted_temp"], t_low, t_high, score["sigma"])
        calibrated_probability = float(self.calibrator.transform([raw_probability], score["confidence"])[0])
        return ProbabilityEstimate(
            adjusted_temp=float(score["adjusted_temp"]),
            sigma=float(score["sigma"]),
            confidence=float(score["confidence"]),
            probability=calibrated_probability,
            bias=float(score["bias"]),
            mae=float(score["mae"]),
            n=int(score["n"]),
            tier=str(score["tier"]),
        )
