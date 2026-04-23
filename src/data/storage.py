"""
Append-only storage for model and evaluation datasets.
"""
from __future__ import annotations

import json
from pathlib import Path

from .schema import DatasetRow


class DatasetStorage:
    """Append-only JSONL storage."""

    def __init__(self, data_dir: str = "data", filename: str = "dataset_rows.jsonl"):
        self.path = Path(data_dir) / filename
        self.path.parent.mkdir(exist_ok=True)

    def append(self, row: DatasetRow) -> None:
        """Append one immutable dataset row."""
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row.to_dict(), ensure_ascii=False) + "\n")

