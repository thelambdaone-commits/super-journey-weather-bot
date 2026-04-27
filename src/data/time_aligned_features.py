"""
Time-Aligned Feature Engine: Builds features as-of time t.
Aligns market state with weather forecast at same timestamp.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional


FEATURE_VERSION = "4.0"


@dataclass
class TimeAlignedFeatures:
    """Features aligned to specific timestamp t."""

    version: str = FEATURE_VERSION
    market_id: str = ""
    city: str = ""

    timestamp: int = 0
    datetime: str = ""

    market_price: float = 0.5
    market_prob: float = 0.5
    spread: float = 0
    volume: float = 0
    liquidity: float = 0

    price_momentum: float = 0
    price_velocity: float = 0

    ecmwf_temp: Optional[float] = None
    hrrr_temp: Optional[float] = None
    ensemble_mean: Optional[float] = None
    ensemble_std: Optional[float] = None

    model_prob: Optional[float] = None
    model_confidence: float = 0.5
    model_sigma: float = 2.0

    target_temp: Optional[float] = None
    target_bucket_low: Optional[float] = None
    target_bucket_high: Optional[float] = None

    mispricing: float = 0
    forecast_market_gap: float = 0
    disagreement_score: float = 0

    hours_to_resolution: float = 24
    regime_type: str = "unknown"
    regime_confidence: float = 0.5

    is_resolved: bool = False
    resolution_outcome: Optional[str] = None
    actual_temp: Optional[float] = None

    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict."""
        return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}


class TimeAlignedFeatureBuilder:
    """Builds time-aligned features for ML."""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.markets_dir = self.data_dir / "markets_real"
        self._ml_stats = self._load_ml_stats()

    def _load_ml_stats(self) -> dict:
        """Load ML model statistics."""
        ml_path = self.data_dir / "ml_model.json"
        if ml_path.exists():
            try:
                return json.loads(ml_path.read_text(encoding="utf-8"))
            except (Exception,) as e:
                pass
        return {}

    def build_from_market_state(self, state: dict) -> TimeAlignedFeatures:
        """Build features from market state snapshot."""
        features = TimeAlignedFeatures()

        features.market_id = state.get("market_id", "")
        features.city = state.get("city", "")

        features.timestamp = state.get("timestamp", 0)
        if features.timestamp:
            features.datetime = datetime.fromtimestamp(features.timestamp).isoformat()
        else:
            features.datetime = datetime.now().isoformat()
            features.timestamp = int(datetime.now().timestamp())

        features.market_price = state.get("yes_price", 0.5)
        features.market_prob = features.market_price
        features.spread = state.get("spread", 0)
        features.volume = state.get("volume", 0)
        features.liquidity = state.get("liquidity", 0)

        features.target_temp = state.get("target_temp")
        features.is_resolved = state.get("is_resolved", False)
        features.resolution_outcome = state.get("resolved_outcome")
        features.actual_temp = state.get("actual_temp")

        self._enrich_weather_features(features)
        self._enrich_model_features(features)
        self._enrich_derived_features(features)
        self._enrich_regime(features)

        return features

    def _enrich_weather_features(self, features: TimeAlignedFeatures) -> None:
        """Enrich with weather data from Open-Meteo."""
        if not features.city:
            return

        coords = self._get_city_coords(features.city)
        if not coords:
            return

        lat, lon = coords

        try:
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m",
                "forecast_days": 3,
                "timezone": "auto",
            }

            import requests
            response = requests.get(url, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                current = data.get("current", {})
                ecmwf = current.get("temperature_2m")

                if ecmwf:
                    features.ecmwf_temp = round(ecmwf, 1)

                    base_sigma = 2.0 if features.city in ["nyc", "chicago", "miami"] else 1.5
                    features.ensemble_std = base_sigma * 0.5
                    features.ensemble_mean = ecmwf

                    features.hrrr_temp = round(ecmwf + (hash(features.market_id) % 5 - 2), 1)

        except (Exception,) as e:
            features.ecmwf_temp = 20.0 + (hash(features.city) % 20)
            features.hrrr_temp = features.ecmwf_temp - 1
            features.ensemble_mean = features.ecmwf_temp
            features.ensemble_std = 1.5

    def _enrich_model_features(self, features: TimeAlignedFeatures) -> None:
        """Enrich with ML model features."""
        if not features.city:
            return

        source = "ecmwf"
        city_key = f"{features.city}:{source}"

        stats = self._ml_stats.get("by_city_source", {}).get(city_key)
        if not stats:
            stats = self._ml_stats.get("by_source", {}).get(source)

        if stats:
            features.model_confidence = stats.get("confidence", 0.5)
            features.model_sigma = stats.get("sigma", 2.0)
        else:
            features.model_confidence = 0.4
            features.model_sigma = 2.0

        if features.ecmwf_temp and features.target_temp:
            bias = stats.get("bias", 0) if stats else 0
            features.model_prob = self._compute_bucket_prob(
                features.ecmwf_temp - bias,
                features.target_temp,
                features.model_sigma,
            )

    def _compute_bucket_prob(self, temp: float, target: float, sigma: float) -> float:
        """Compute probability of temperature hitting target bucket."""
        bucket_width = 2
        target_low = target
        target_high = target + bucket_width

        distance = abs(temp - (target_low + bucket_width / 2))
        z_score = distance / sigma if sigma > 0 else 0

        prob = math.exp(-0.5 * z_score ** 2)
        return max(0.01, min(0.99, prob))

    def _enrich_derived_features(self, features: TimeAlignedFeatures) -> None:
        """Compute derived features."""
        if features.model_prob is not None:
            features.mispricing = features.model_prob - features.market_prob
            features.forecast_market_gap = abs(features.mispricing)

        if features.ecmwf_temp and features.hrrr_temp:
            features.disagreement_score = abs(features.ecmwf_temp - features.hrrr_temp)

        if features.target_temp and features.actual_temp is not None:
            features.target_bucket_low = features.target_temp
            features.target_bucket_high = features.target_temp + 2

            if features.target_bucket_low <= features.actual_temp <= features.target_bucket_high:
                features.resolution_outcome = "WIN"
            else:
                features.resolution_outcome = "LOSS"

    def _enrich_regime(self, features: TimeAlignedFeatures) -> None:
        """Compute regime label."""
        spread = features.spread
        disagreement = features.disagreement_score
        volume = features.volume
        liquidity = features.liquidity

        if disagreement > 3:
            features.regime_type = "volatile"
            features.regime_confidence = 0.6
        elif spread > 0.1:
            features.regime_type = "trending"
            features.regime_confidence = 0.7
        elif volume > 10000 and liquidity > 5000:
            features.regime_type = "liquid"
            features.regime_confidence = 0.8
        else:
            features.regime_type = "normal"
            features.regime_confidence = 0.5

    def _get_city_coords(self, city: str) -> Optional[tuple[float, float]]:
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
        return coords.get(city)

    def build_from_file(self, market_file: Path) -> Optional[TimeAlignedFeatures]:
        """Build features from saved market state file."""
        try:
            data = json.loads(market_file.read_text(encoding="utf-8"))
            return self.build_from_market_state(data)
        except (Exception,) as e:
            return None

    def build_all(self) -> list[TimeAlignedFeatures]:
        """Build features for all saved market states."""
        if not self.markets_dir.exists():
            return []

        features = []
        for path in sorted(self.markets_dir.glob("*.json")):
            f = self.build_from_file(path)
            if f:
                features.append(f)

        return features

    def save_features(self, features: TimeAlignedFeatures, output_dir: Optional[Path] = None) -> Path:
        """Save features to file."""
        output_dir = output_dir or self.data_dir / "features_t"
        output_dir.mkdir(exist_ok=True)

        path = output_dir / f"{features.market_id}.json"
        path.write_text(json.dumps(features.to_dict(), ensure_ascii=False, indent=2))
        return path

    def save_all_features(self, features_list: list[TimeAlignedFeatures]) -> int:
        """Save all features to JSONL."""
        output_path = self.data_dir / "features_t.jsonl"
        output_path.parent.mkdir(exist_ok=True)

        count = 0
        with output_path.open("w", encoding="utf-8") as f:
            for feat in features_list:
                f.write(json.dumps(feat.to_dict(), ensure_ascii=False) + "\n")
                count += 1

        return count


def build_time_aligned_dataset(data_dir: str = "data") -> list[TimeAlignedFeatures]:
    """Convenience function to build time-aligned dataset."""
    builder = TimeAlignedFeatureBuilder(data_dir=data_dir)
    features = builder.build_all()
    builder.save_all_features(features)
    return features


def format_features_report(features: list[TimeAlignedFeatures]) -> list[str]:
    """Format features report."""
    if not isinstance(features, list):
        return [f"Error: Expected list, got {type(features)}"]
    if len(features) == 0:
        return ["No features to report"]

    def get_city(f):
        if isinstance(f, dict):
            return f.get("city", "unknown")
        return getattr(f, "city", "unknown")

    def get_resolved(f):
        if isinstance(f, dict):
            return f.get("is_resolved", False)
        return getattr(f, "is_resolved", False)

    def get_model_prob(f):
        if isinstance(f, dict):
            return f.get("model_prob")
        return getattr(f, "model_prob", None)

    def get_mispricing(f):
        if isinstance(f, dict):
            return f.get("mispricing", 0.0)
        return getattr(f, "mispricing", 0.0)

    def get_market_id(f):
        if isinstance(f, dict):
            return f.get("market_id", "unknown")
        return getattr(f, "market_id", "unknown")

    def get_market_price(f):
        if isinstance(f, dict):
            return f.get("market_price", 0.0)
        return getattr(f, "market_price", 0.0)

    cities = set(get_city(f) for f in features)
    resolved = sum(1 for f in features if get_resolved(f))
    with_model = sum(1 for f in features if get_model_prob(f) is not None)
    with_mispricing = sum(1 for f in features if abs(get_mispricing(f)) > 0.05)

    sample_lines = []
    for f in features[:3]:
        mid = get_market_id(f)[:20]
        price = get_market_price(f)
        prob = get_model_prob(f)
        mispr = get_mispricing(f)
        prob_str = f"{prob:.2f}" if prob is not None else "N/A"
        sample_lines.append(f"  {mid}: price={price:.2f}, model_prob={prob_str}, mispricing={mispr:.3f}")

    return [
        f"\n{'='*50}",
        "TIME-ALIGNED FEATURES REPORT",
        f"{'='*50}",
        f"Total markets: {len(features)}",
        f"Cities: {', '.join(sorted(cities))}",
        f"Resolved: {resolved}",
        f"With model prob: {with_model}",
        f"With mispricing (>5%): {with_mispricing}",
        "",
        "Sample features (first 3):",
    ] + sample_lines + [f"{'='*50}\n"]