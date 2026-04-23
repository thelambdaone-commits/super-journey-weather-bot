"""
Regime Augmentation: Meteorological regime labeling for market patterns.
Creates latent structure for better ML generalization.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


@dataclass
class RegimeLabel:
    """Meteorological regime classification."""

    regime_type: str
    confidence: float
    features: dict[str, float]
    description: str


class RegimeClassifier:
    """Classify market states into meteorological regimes."""

    REGIME_TYPES = [
        "clear_warm",
        "clear_cool",
        "frontal_passage",
        "unstable",
        "stable_cold",
        "stable_warm",
        "coastal_influence",
        "continental",
        "mixed",
        "unknown",
    ]

    def __init__(self):
        self._regime_thresholds = {
            "forecast_spread": 3.0,
            "temp_gradient": 5.0,
            "variance_threshold": 2.0,
        }

    def classify_from_features(
        self,
        forecast_temp: float,
        ensemble_std: float,
        forecast_spread: float,
        model_confidence: float,
        ecmwf: Optional[float] = None,
        hrrr: Optional[float] = None,
        actual_temp: Optional[float] = None,
        unit: str = "C",
        day_of_year: int = 0,
    ) -> RegimeLabel:
        """Classify regime based on weather features."""

        temps = [t for t in [forecast_temp, ecmwf, hrrr, actual_temp] if t is not None]
        mean_temp = sum(temps) / len(temps) if temps else forecast_temp

        features = {
            "forecast_spread": forecast_spread,
            "ensemble_std": ensemble_std,
            "temp": mean_temp,
            "confidence": model_confidence,
            "source_disagreement": abs((ecmwf or mean_temp) - (hrrr or mean_temp)) if ecmwf and hrrr else 0,
        }

        regime_type = self._determine_regime(features, unit, day_of_year)

        confidence = self._compute_confidence(features, regime_type)

        return RegimeLabel(
            regime_type=regime_type,
            confidence=confidence,
            features=features,
            description=self._regime_description(regime_type),
        )

    def _determine_regime(
        self,
        features: dict[str, float],
        unit: str,
        day_of_year: int,
    ) -> str:
        spread = features.get("forecast_spread", 0)
        std = features.get("ensemble_std", 0)
        temp = features.get("temp", 20)
        disagreement = features.get("source_disagreement", 0)

        seasonal_factor = self._get_seasonal_factor(day_of_year)

        if disagreement > 3:
            return "frontal_passage"
        if spread > 4:
            return "unstable"
        if std < 1 and disagreement < 1:
            if temp > (20 if unit == "C" else 70):
                return "stable_warm"
            else:
                return "stable_cold"
        if disagreement > 2:
            return "coastal_influence"
        if spread > 2:
            return "mixed"
        if temp > (25 if unit == "C" else 77):
            return "clear_warm"
        if temp < (10 if unit == "C" else 50):
            return "clear_cool"
        return "mixed"

    def _compute_confidence(self, features: dict[str, float], regime_type: str) -> float:
        base_confidence = 0.5

        if features.get("source_disagreement", 0) < 2:
            base_confidence += 0.2
        if features.get("ensemble_std", 0) < 2:
            base_confidence += 0.15
        if features.get("confidence", 0) > 0.5:
            base_confidence += 0.15

        if regime_type == "unknown":
            base_confidence -= 0.2

        return max(0.1, min(0.95, base_confidence))

    def _get_seasonal_factor(self, day_of_year: int) -> str:
        if day_of_year == 0:
            return "unknown"
        if 60 <= day_of_year <= 90:
            return "spring_nh"
        elif 152 <= day_of_year <= 243:
            return "summer_nh"
        elif 244 <= day_of_year <= 334:
            return "fall_nh"
        else:
            return "winter_nh"

    def _regime_description(self, regime_type: str) -> str:
        descriptions = {
            "clear_warm": "Stable high pressure, warm conditions",
            "clear_cool": "Stable high pressure, cool conditions",
            "frontal_passage": "Active weather, temperature swings expected",
            "unstable": "High uncertainty, model disagreement",
            "stable_cold": "Persistent cold, low model variance",
            "stable_warm": "Persistent warm, low model variance",
            "coastal_influence": "Maritime effects, temperature moderation",
            "continental": "Inland conditions, high temperature range",
            "mixed": "Transition conditions, moderate uncertainty",
            "unknown": "Insufficient data for classification",
        }
        return descriptions.get(regime_type, "Unknown regime")


class RegimeAugmenter:
    """Augment dataset with regime labels."""

    def __init__(self, classifier: Optional[RegimeClassifier] = None):
        self.classifier = classifier or RegimeClassifier()

    def augment_rows(
        self,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Add regime labels to dataset rows."""
        augmented = []

        for row in rows:
            regime_label = self.classifier.classify_from_features(
                forecast_temp=row.get("forecast_temp", 20),
                ensemble_std=row.get("ensemble_std", 0),
                forecast_spread=row.get("forecast_spread", 0),
                model_confidence=row.get("model_confidence_score", 0.5),
                ecmwf=row.get("ecmwf_max"),
                hrrr=row.get("hrrr_max"),
                actual_temp=row.get("actual_temp"),
                unit=row.get("unit", "C"),
                day_of_year=row.get("day_of_year", 0),
            )

            augmented_row = dict(row)
            augmented_row["regime_type"] = regime_label.regime_type
            augmented_row["regime_confidence"] = regime_label.confidence
            augmented_row["regime_description"] = regime_label.description
            augmented_row["source_disagreement"] = regime_label.features.get("source_disagreement", 0)

            augmented.append(augmented_row)

        return augmented

    def augment_v3_file(
        self,
        input_path: Path,
        output_path: Optional[Path] = None,
    ) -> dict[str, Any]:
        """Augment a V3 dataset file with regime labels."""
        if output_path is None:
            output_path = input_path.parent / f"{input_path.stem}_regime_augmented.jsonl"

        rows = []
        for line in input_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))

        if not rows:
            return {"status": "error", "message": "empty_dataset"}

        augmented = self.augment_rows(rows)

        output_path.parent.mkdir(exist_ok=True)
        count = 0
        with output_path.open("w", encoding="utf-8") as f:
            for row in augmented:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
                count += 1

        regime_counts = self._count_regimes(augmented)

        return {
            "status": "success",
            "rows_augmented": count,
            "output_path": str(output_path),
            "regime_distribution": regime_counts,
        }

    def _count_regimes(self, rows: list[dict[str, Any]]) -> dict[str, int]:
        counts = {regime: 0 for regime in self.classifier.REGIME_TYPES}
        for row in rows:
            regime = row.get("regime_type", "unknown")
            counts[regime] = counts.get(regime, 0) + 1
        return counts


def get_regime_augmenter() -> RegimeAugmenter:
    """Factory function."""
    return RegimeAugmenter()