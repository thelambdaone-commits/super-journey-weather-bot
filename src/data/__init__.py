"""
Data layer for immutable dataset records and append-only storage.
"""
from .schema import DatasetRow, SCHEMA_VERSION
from .schema_v3 import DatasetRowV3, SCHEMA_VERSION_V3, SCAN_MAX_DEPTH
from .snapshot_engine import SnapshotEngine, get_snapshot_engine
from .target_layer import (
    compute_realized_edge,
    compute_ml_target,
    enrich_row_with_targets,
    validate_target_distribution,
    TargetLayer,
)

__all__ = [
    "DatasetRow",
    "DatasetRowV3",
    "SCHEMA_VERSION",
    "SCHEMA_VERSION_V3",
    "SCAN_MAX_DEPTH",
    "SnapshotEngine",
    "get_snapshot_engine",
    "compute_realized_edge",
    "compute_ml_target",
    "enrich_row_with_targets",
    "validate_target_distribution",
    "TargetLayer",
]

