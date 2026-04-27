"""
Dataset utilities for ML training and inference.
"""
from __future__ import annotations
import numpy as np
from pathlib import Path
from pathlib import Path
from ..data.loader import load_rows

FEATURE_COLS = [
    "ecmwf_max",
    "gfs_max",
    "hrrr_max",
    "ensemble_std",
    "forecast_spread",
    "market_price",
    "hours_to_resolution",
    "day_of_year",
    "lat",
    "lon"
]

def row_to_features(row) -> np.ndarray:
    """Transform a DatasetRow or dict into a feature vector."""
    def g(name, default=0.0):
        if isinstance(row, dict):
            return row.get(name, default)
        return getattr(row, name, default)

    # We use a set of 10 standard features for the WeatherBot ML
    features = [
        g("ecmwf_max") or 0.0,
        g("gfs_max") or 0.0,
        g("hrrr_max") or 0.0,
        g("ensemble_std", 1.0) or 1.0,
        g("forecast_spread", 1.0) or 1.0,
        g("market_price", 0.5) or 0.5,
        g("hours_to_resolution", 24.0) or 24.0,
        (g("day_of_year", 180) or 180) / 366.0,
        float(g("lat", 0.0) or 0.0) / 90.0,
        float(g("lon", 0.0) or 0.0) / 180.0
    ]
    return np.array(features)

def extract_features(rows: list) -> np.ndarray:
    """Batch extract features from rows."""
    if not rows:
        return np.array([]).reshape(0, len(FEATURE_COLS))
    return np.array([row_to_features(row) for row in rows])

def load_training_data(data_dir: str = "data") -> tuple[np.ndarray, np.ndarray]:
    """Load and process all RESOLVED rows for training."""
    path = Path(data_dir) / "dataset_rows.jsonl"
    rows = load_rows(path)
    
    X = []
    y = []
    
    for row in rows:
        # Only use resolved markets for training
        if row.actual_temp is not None and row.actual_bucket is not None:
            # Target: 1 if prediction was correct, 0 otherwise
            # Or Target: 1 if outcome was YES for the bucket, 0 otherwise
            # In this bot, we usually predict if a specific bucket wins.
            if row.bucket == row.actual_bucket:
                y.append(1)
            else:
                y.append(0)
            X.append(row_to_features(row))
            
    if not X:
        return np.array([]), np.array([])
        
    return np.array(X), np.array(y)
