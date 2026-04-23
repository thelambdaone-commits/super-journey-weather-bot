"""
Dataset V4 Builder: Creates ML-ready dataset from real market + time-aligned features.
Format: (market_id, timestamp_t) → features → outcome
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from .time_aligned_features import TimeAlignedFeatureBuilder, TimeAlignedFeatures
from .resolution_validator import ResolutionValidator


ML_FEATURE_COLS = [
    "market_price",
    "spread",
    "volume",
    "liquidity",
    "price_momentum",
    "ecmwf_temp",
    "hrrr_temp",
    "ensemble_mean",
    "ensemble_std",
    "model_prob",
    "model_confidence",
    "mispricing",
    "forecast_market_gap",
    "disagreement_score",
    "hours_to_resolution",
    "regime_confidence",
    "latitude",
    "longitude",
    "day_of_year",
]


@dataclass
class DatasetV4Row:
    """ML-ready row for training."""

    market_id: str
    city: str
    timestamp: int
    datetime: str

    features: dict[str, float]
    target: Optional[float]
    realized_edge: Optional[float]

    is_resolved: bool
    resolution_outcome: Optional[str]
    confidence: float

    version: str = "4.0"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict."""
        return {
            "version": self.version,
            "market_id": self.market_id,
            "city": self.city,
            "timestamp": self.timestamp,
            "datetime": self.datetime,
            **self.features,
            "target": self.target,
            "realized_edge": self.realized_edge,
            "is_resolved": self.is_resolved,
            "resolution_outcome": self.resolution_outcome,
            "confidence": self.confidence,
        }


class DatasetV4Builder:
    """Builds ML-ready Dataset V4."""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.feature_builder = TimeAlignedFeatureBuilder(data_dir=data_dir)
        self.validator = ResolutionValidator(data_dir=data_dir)

    def build_row(self, features: TimeAlignedFeatures, validation: Optional[dict] = None) -> DatasetV4Row:
        """Build single ML row from features."""
        lat, lon = self._get_coords(features.city)

        day_of_year = 0
        if features.datetime:
            from datetime import datetime
            try:
                dt = datetime.fromisoformat(features.datetime)
                day_of_year = dt.timetuple().tm_yday
            except Exception:
                pass

        feat_dict = {
            "market_price": features.market_price,
            "spread": features.spread,
            "volume": features.volume,
            "liquidity": features.liquidity,
            "price_momentum": features.price_momentum,
            "ecmwf_temp": features.ecmwf_temp or 0,
            "hrrr_temp": features.hrrr_temp or 0,
            "ensemble_mean": features.ensemble_mean or 0,
            "ensemble_std": features.ensemble_std or 0,
            "model_prob": features.model_prob or 0.5,
            "model_confidence": features.model_confidence,
            "mispricing": features.mispricing,
            "forecast_market_gap": features.forecast_market_gap,
            "disagreement_score": features.disagreement_score,
            "hours_to_resolution": features.hours_to_resolution,
            "regime_confidence": features.regime_confidence,
            "latitude": lat,
            "longitude": lon,
            "day_of_year": day_of_year,
        }

        target = None
        realized_edge = None

        if features.is_resolved:
            raw_outcome = features.resolution_outcome
            if raw_outcome == "WIN":
                target = 1.0
            elif raw_outcome == "LOSS":
                target = 0.0
            elif raw_outcome == "YES":
                target = 1.0
            elif raw_outcome == "NO":
                target = 0.0

            if target is not None and features.market_price > 0:
                payout = 1.0 / features.market_price
                market_ev = features.market_price * payout
                actual_ev = target * payout
                realized_edge = actual_ev - market_ev

        confidence = 0.5
        if validation:
            confidence = validation.get("confidence", 0.5)

        return DatasetV4Row(
            market_id=features.market_id,
            city=features.city or "",
            timestamp=features.timestamp,
            datetime=features.datetime,
            features=feat_dict,
            target=target,
            realized_edge=realized_edge,
            is_resolved=features.is_resolved,
            resolution_outcome=features.resolution_outcome,
            confidence=confidence,
        )

    def _get_coords(self, city: str) -> tuple[float, float]:
        """Get coordinates for city."""
        coords = {
            "nyc": (40.7128, -74.0060),
            "chicago": (41.8781, -87.6298),
            "miami": (25.7617, -80.1918),
            "seattle": (47.6062, -122.3321),
            "atlanta": (33.7490, -84.3880),
            "dallas": (32.7767, -96.7970),
            "los-angeles": (34.0522, -118.2437),
            "boston": (42.3601, -71.0589),
            "denver": (39.7392, -104.9903),
            "phoenix": (33.4484, -112.0740),
            "london": (51.5074, -0.1278),
            "paris": (48.8566, 2.3522),
            "tokyo": (35.6762, 139.6503),
        }
        return coords.get(city, (0, 0))

    def build_dataset(self, min_confidence: float = 0.3) -> list[DatasetV4Row]:
        """Build full V4 dataset."""
        features_list = self.feature_builder.build_all()
        validations = {r.market_id: r for r in self.validator.validate_all()}

        rows = []
        for features in features_list:
            validation = validations.get(features.market_id)

            if validation and validation.confidence < min_confidence:
                continue

            row = self.build_row(features, validation.__dict__ if validation else None)
            rows.append(row)

        return rows

    def save_dataset(self, rows: list[DatasetV4Row], filename: str = "dataset_v4.jsonl") -> Path:
        """Save dataset to file."""
        output_path = self.data_dir / filename
        output_path.parent.mkdir(exist_ok=True)

        count = 0
        with output_path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row.to_dict(), ensure_ascii=False) + "\n")
                count += 1

        return output_path

    def run(self) -> dict[str, Any]:
        """Run full dataset build."""
        rows = self.build_dataset()

        if not rows:
            return {
                "status": "no_data",
                "rows": 0,
                "resolved": 0,
                "unresolved": 0,
            }

        output_path = self.save_dataset(rows)

        resolved = [r for r in rows if r.is_resolved]
        with_target = [r for r in rows if r.target is not None]
        cities = set(r.city for r in rows if r.city)

        return {
            "status": "success",
            "rows": len(rows),
            "resolved": len(resolved),
            "with_target": len(with_target),
            "unresolved": len(rows) - len(resolved),
            "cities": len(cities),
            "output_path": str(output_path),
        }


def build_dataset_v4(data_dir: str = "data") -> dict[str, Any]:
    """Convenience function."""
    builder = DatasetV4Builder(data_dir=data_dir)
    return builder.run()


def format_v4_report(result: dict[str, Any]) -> list[str]:
    """Format V4 build report."""
    lines = [
        f"\n{'='*50}",
        "DATASET V4 BUILD REPORT",
        f"{'='*50}",
        f"Status: {result.get('status')}",
        f"Total rows: {result.get('rows', 0)}",
        f"Resolved: {result.get('resolved', 0)}",
        f"With target: {result.get('with_target', 0)}",
        f"Unresolved: {result.get('unresolved', 0)}",
        f"Cities: {result.get('cities', 0)}",
        f"Output: {result.get('output_path', 'N/A')}",
        f"{'='*50}\n",
    ]
    return lines


def load_dataset_v4(path: str) -> list[dict]:
    """Load V4 dataset."""
    path = Path(path)
    if not path.exists():
        return []

    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows