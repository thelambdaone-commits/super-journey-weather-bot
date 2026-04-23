"""
Fetch and persist missing actual temperatures for stored markets.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from ...weather.apis import get_actual_temp


def load_market(path: Path) -> dict | None:
    """Load one market JSON payload."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_market(path: Path, payload: dict) -> None:
    """Persist one market JSON payload."""
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def find_missing_actuals(markets_dir: str = "data/markets") -> list[dict]:
    """Return markets that still miss actual temperature labels."""
    missing: list[dict] = []
    for path in sorted(Path(markets_dir).glob("*.json")):
        market = load_market(path)
        if not market or market.get("actual_temp") is not None:
            continue
        missing.append(
            {
                "path": path,
                "city": market.get("city"),
                "city_name": market.get("city_name"),
                "date": market.get("date"),
                "status": market.get("status"),
            }
        )
    return missing


def backfill_actuals(markets_dir: str = "data/markets", vc_key: str = "", dry_run: bool = False) -> dict:
    """Backfill actual temperatures into stored market files."""
    missing = find_missing_actuals(markets_dir)
    updated = 0
    failed = 0

    for item in missing:
        city = item["city"]
        date_str = item["date"]
        temp = get_actual_temp(city, date_str, vc_key=vc_key) if city and date_str else None
        if temp is None:
            failed += 1
            continue

        if dry_run:
            updated += 1
            continue

        market = load_market(item["path"])
        if not market:
            failed += 1
            continue
        market["actual_temp"] = temp
        save_market(item["path"], market)
        updated += 1
        time.sleep(0.2)

    return {
        "missing": len(missing),
        "updated": updated,
        "failed": failed,
        "dry_run": dry_run,
    }

