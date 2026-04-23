"""
Feature engine entrypoint.
"""
from __future__ import annotations

from .context_features import build_context_features
from .market_features import build_market_features
from .weather_features import build_weather_features


class FeatureEngine:
    """Compose weather, market and context features."""

    def build(
        self,
        location,
        snapshot: dict,
        outcomes: list[dict],
        hours_to_resolution: float | None,
        selected_outcome: dict | None = None,
    ) -> dict:
        """Build a flat feature payload."""
        features = {}
        features.update(build_weather_features(snapshot))
        features.update(build_market_features(outcomes, selected_outcome))
        features.update(build_context_features(location, hours_to_resolution))
        return features

