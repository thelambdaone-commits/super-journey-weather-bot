"""
Temporal alignment helpers for leak-free historical dataset building.
"""
from __future__ import annotations

from datetime import datetime, timezone


def parse_ts(value: str | None) -> datetime | None:
    """Parse ISO timestamps safely."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def is_pre_resolution(snapshot_ts: str | None, event_end_date: str | None) -> bool:
    """Return True when a snapshot was captured before market resolution."""
    snap = parse_ts(snapshot_ts)
    end = parse_ts(event_end_date)
    if snap is None or end is None:
        return True
    return snap <= end


def align_snapshots(market: dict) -> list[dict]:
    """Return only leak-free forecast snapshots for a market."""
    event_end_date = market.get("event_end_date") or market.get("eventEndDate")
    aligned: list[dict] = []
    for snapshot in market.get("forecast_snapshots", []):
        if is_pre_resolution(snapshot.get("ts"), event_end_date):
            aligned.append(snapshot)
    return aligned

