"""
Weather calibration by city + horizon + source.
Provides adjusted forecasts with historical bias correction.
"""
from __future__ import annotations
from pathlib import Path
from typing import Dict, Optional
import json
import numpy as np


DEFAULT_SIGMA = {
    "C": 2.0,
    "F": 3.6,  # F ≈ C * 1.8
}


class WeatherCalibrator:
    """
    Calibrated weather model by city, horizon, and source.
    
    Usage:
        calibrator = WeatherCalibrator(data_dir)
        score = calibrator.score(city, source, horizon, forecast_temp, unit, bucket)
        # Returns bias-adjusted probability
    """
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.model_path = Path(data_dir) / "weather_calibration.json"
        self.model = self._load_model()
    
    def _load_model(self) -> dict:
        """Load calibration model."""
        if self.model_path.exists():
            return json.loads(self.model_path.read_text())
        return {"by_city_source_horizon": {}, "by_city_source": {}, "by_source": {}}
    
    def _save_model(self) -> None:
        """Persist calibration model."""
        self.model_path.parent.mkdir(parents=True, exist_ok=True)
        self.model_path.write_text(json.dumps(self.model, indent=2))
    
    def fit_from_dataset(self, dataset_path: str) -> dict:
        """Fit calibration from dataset with city+source+horizon."""
        from ..data.loader import load_rows
        
        rows = load_rows(dataset_path)
        
        # Group by city:source:horizon
        by_key: dict = {}
        
        for row in rows:
            if row.actual_temp is None or row.forecast_temp is None:
                continue
            
            city = row.city or "unknown"
            source = row.forecast_source or "unknown"
            horizon = row.forecast_horizon or "D+1"
            key = f"{city}:{source}:{horizon}"
            
            if key not in by_key:
                by_key[key] = []
            by_key[key].append(row.forecast_temp - row.actual_temp)
        
        # Calculate stats
        new_model = {"by_city_source_horizon": {}, "by_city_source": {}, "by_source": {}}
        
        for key, errors in by_key.items():
            if len(errors) < 3:
                continue
            
            arr = np.array(errors)
            new_model["by_city_source_horizon"][key] = {
                "mean": float(np.mean(arr)),
                "std": float(np.std(arr)),
                "mae": float(np.mean(np.abs(arr))),
                "bias": float(np.mean(arr)),
                "n": len(errors),
            }
        
        # Aggregate by city:source
        by_cs: dict = {}
        for key, errors in by_key.items():
            parts = key.split(":")
            if len(parts) >= 2:
                cs_key = f"{parts[0]}:{parts[1]}"
                if cs_key not in by_cs:
                    by_cs[cs_key] = []
                by_cs[cs_key].extend(errors)
        
        for key, errors in by_cs.items():
            if len(errors) < 3:
                continue
            arr = np.array(errors)
            new_model["by_city_source"][key] = {
                "mean": float(np.mean(arr)),
                "std": float(np.std(arr)),
                "mae": float(np.mean(np.abs(arr))),
                "bias": float(np.mean(arr)),
                "n": len(errors),
            }
        
        # Aggregate by source
        by_source: dict = {}
        for key, errors in by_key.items():
            parts = key.split(":")
            if len(parts) >= 1:
                source = parts[1] if len(parts) > 1 else "unknown"
                if source not in by_source:
                    by_source[source] = []
                by_source[source].extend(errors)
        
        for source, errors in by_source.items():
            if len(errors) < 3:
                continue
            arr = np.array(errors)
            new_model["by_source"][source] = {
                "mean": float(np.mean(arr)),
                "std": float(np.std(arr)),
                "mae": float(np.mean(np.abs(arr))),
                "bias": float(np.mean(arr)),
                "n": len(errors),
            }
        
        self.model = new_model
        self._save_model()
        return new_model
    
    def get_stats(self, city: str, source: str, horizon: str = "D+1") -> dict:
        """Get calibration stats for city:source:horizon."""
        # Try most specific first
        key = f"{city}:{source}:{horizon}"
        if key in self.model.get("by_city_source_horizon", {}):
            return self.model["by_city_source_horizon"][key]
        
        # Fall back to city:source
        cs_key = f"{city}:{source}"
        if cs_key in self.model.get("by_city_source", {}):
            return self.model["by_city_source"][cs_key]
        
        # Fall back to source only
        if source in self.model.get("by_source", {}):
            return self.model["by_source"][source]
        
        return {"mean": 0.0, "bias": 0.0, "n": 0}
    
    def score(
        self,
        city: str,
        source: str,
        horizon: str,
        forecast_temp: float,
        unit: str,
        bucket_low: float,
        bucket_high: float,
    ) -> dict:
        """Score a forecast with calibration."""
        from ..weather.math import bucket_prob
        
        stats = self.get_stats(city, source, horizon)
        
        bias = stats.get("bias", 0.0)
        sigma = stats.get("std", DEFAULT_SIGMA.get(unit, 2.0))
        n = stats.get("n", 0)
        
        # Apply bias correction
        adjusted_temp = forecast_temp - bias
        
        # Calculate probability
        probability = bucket_prob(adjusted_temp, bucket_low, bucket_high, sigma)
        
        confidence = min(1.0, n / 50.0) if n > 0 else 0.2
        
        return {
            "adjusted_temp": adjusted_temp,
            "raw_temp": forecast_temp,
            "bias": bias,
            "sigma": sigma,
            "confidence": confidence,
            "probability": probability,
            "n": n,
            "tier": "city_source_horizon" if n >= 10 else ("city_source" if stats.get("n", 0) >= 5 else "default"),
        }
    
    def get_performance_report(self) -> dict:
        """Get calibration performance by segment."""
        report = {}
        
        # By city:source:horizon
        for key, stats in self.model.get("by_city_source_horizon", {}).items():
            if stats.get("n", 0) >= 5:
                report[key] = stats
        
        # Summary
        total_n = sum(s.get("n", 0) for s in report.values())
        return {
            "segments": len(report),
            "total_samples": total_n,
            "segments_detail": report,
        }