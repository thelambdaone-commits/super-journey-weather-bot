#!/usr/bin/env python3
"""
backfill.py — Historical data backfill and dataset reconstruction.

Usage:
    python backfill.py --actuals
    python backfill.py --actuals --dry-run
    python backfill.py --dataset
    python backfill.py --all
"""
from __future__ import annotations

import sys

from src.data.backfill.builder import build_historical_dataset
from src.data.backfill.fetch_actuals import backfill_actuals
from src.weather.config import get_config


def main(args: list[str]) -> int:
    """Run backfill operations."""
    config = get_config()
    dry_run = "--dry-run" in args

    if "--help" in args or "-h" in args:
        print(__doc__)
        return 0

    run_actuals = "--actuals" in args or "--all" in args or not args
    run_dataset = "--dataset" in args or "--all" in args

    if run_actuals:
        summary = backfill_actuals(dry_run=dry_run)
        print("Actuals backfill:")
        for key, value in summary.items():
            print(f"  {key}: {value}")

    if run_dataset and not dry_run:
        summary = build_historical_dataset(data_dir=config.data_dir)
        print("Dataset rebuild:")
        for key, value in summary.items():
            print(f"  {key}: {value}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
