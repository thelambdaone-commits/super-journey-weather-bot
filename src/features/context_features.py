"""
Context feature builders.
"""
from __future__ import annotations

from datetime import datetime, timezone


def build_context_features(location, hours_to_resolution: float | None = None, now: datetime | None = None) -> dict:
    """Build contextual static features."""
    current = now or datetime.now(timezone.utc)
    return {
        "lat": location.lat,
        "lon": location.lon,
        "day_of_year": current.timetuple().tm_yday,
        "hours_to_resolution": hours_to_resolution,
        "region": location.region,
        "unit": location.unit,
    }

