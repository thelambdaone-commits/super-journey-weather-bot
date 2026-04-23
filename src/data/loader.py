"""
Dataset loading and schema validation helpers.
"""
from __future__ import annotations

import json
from pathlib import Path

from .schema import DatasetRow, SCHEMA_VERSION


def validate_row(payload: dict) -> DatasetRow:
    """Validate and coerce a stored dataset row."""
    if payload.get("version") != SCHEMA_VERSION:
        raise ValueError(f"unsupported dataset schema version: {payload.get('version')}")
    return DatasetRow(**payload)


def load_rows(path: str | Path) -> list[DatasetRow]:
    """Load dataset rows from JSONL storage."""
    rows: list[DatasetRow] = []
    file_path = Path(path)
    if not file_path.exists():
        return rows
    for line in file_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(validate_row(json.loads(line)))
    return rows

