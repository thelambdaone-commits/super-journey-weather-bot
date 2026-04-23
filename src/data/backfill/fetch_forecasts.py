"""
Historical forecast extraction from stored market snapshots.
"""
from __future__ import annotations

import json
from pathlib import Path


def load_market_snapshots(markets_dir: str = "data/markets") -> list[dict]:
    """Load all stored market payloads for historical backfill."""
    markets: list[dict] = []
    for path in sorted(Path(markets_dir).glob("*.json")):
        try:
            markets.append(json.loads(path.read_text(encoding="utf-8")))
        except Exception:
            continue
    return markets

