"""
Target computation layer for V3 dataset.
Computes ML-ready targets from resolved market data.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Optional

from .schema_v3 import DatasetRowV3


def compute_payout(market_price: float) -> float:
    """Polymarket payout = 1 / market_price."""
    if market_price <= 0 or market_price >= 1:
        return 0.0
    return 1.0 / market_price


def compute_realized_edge(
    calibrated_prob: Optional[float],
    market_price: Optional[float],
    resolution_outcome: Optional[str] = None,
    actual_bucket: Optional[str] = None,
    target_bucket: Optional[str] = None,
) -> Optional[float]:
    """Compute realized edge = (calibrated_prob - market_price) * payout.
    
    This is the true signal: our model's EV vs market's implied EV.
    
    Args:
        calibrated_prob: Our calibrated probability
        market_price: Market implied probability (price)
        resolution_outcome: "win" or "loss"
        actual_bucket: Actual temperature bucket
        target_bucket: Bucket we traded on
    
    Returns:
        Realized edge if resolvable, None otherwise
    """
    if calibrated_prob is None or market_price is None:
        return None
    
    if market_price <= 0 or market_price >= 1:
        return None
    
    payout = compute_payout(market_price)
    
    if resolution_outcome is not None:
        actual_outcome = 1.0 if resolution_outcome == "win" else 0.0
    elif actual_bucket is not None and target_bucket is not None:
        actual_outcome = 1.0 if actual_bucket == target_bucket else 0.0
    else:
        return None
    
    realized_ev = actual_outcome * payout
    market_implied_ev = market_price * payout
    
    return realized_ev - market_implied_ev


def compute_raw_ev(
    market_price: Optional[float],
    actual_temp: Optional[float],
    bucket_low: Optional[float],
    bucket_high: Optional[float],
) -> Optional[float]:
    """Compute raw EV based on actual outcome.
    
    Simple: did the actual temp fall in our bucket?
    """
    if market_price is None or actual_temp is None:
        return None
    if bucket_low is None or bucket_high is None:
        return None
    
    payout = compute_payout(market_price)
    win = 1.0 if bucket_low <= actual_temp <= bucket_high else 0.0
    
    return win * payout - 1.0


def normalize_realized_edge(
    edge: float,
    market_price: Optional[float] = None,
    method: str = "simple",
) -> float:
    """Normalize realized edge for ML training.
    
    Methods:
    - simple: return as-is
    - price_adjusted: divide by (1 - market_price) to account for payout variance
    - log: log-transform to handle wide range
    """
    if edge is None or (isinstance(edge, float) and math.isnan(edge)):
        return 0.0
    
    if method == "simple":
        return edge
    
    if method == "price_adjusted":
        if market_price and 0 < market_price < 1:
            return edge / (1 - market_price)
        return edge
    
    if method == "log":
        return math.log1p(max(edge, 0)) - math.log1p(max(-edge, 0))
    
    return edge


def compute_resolution_confidence(
    actual_temp: Optional[float],
    bucket_low: Optional[float],
    bucket_high: Optional[float],
    sigma: Optional[float] = None,
) -> Optional[float]:
    """Compute confidence of resolution based on margin.
    
    If actual temp is near center of bucket, high confidence.
    If near edges, lower confidence (near miss).
    """
    if actual_temp is None or bucket_low is None or bucket_high is None:
        return None
    
    bucket_width = bucket_high - bucket_low
    if bucket_width <= 0:
        return None
    
    center = (bucket_low + bucket_high) / 2
    distance_from_center = abs(actual_temp - center)
    normalized_distance = distance_from_center / (bucket_width / 2)
    
    if normalized_distance <= 0.25:
        return 0.95
    elif normalized_distance <= 0.5:
        return 0.8
    elif normalized_distance <= 0.75:
        return 0.6
    else:
        return 0.4


def compute_ml_target(row: DatasetRowV3) -> dict[str, Any]:
    """Compute all ML targets for a V3 row.
    
    Returns a dict of target fields to add to the row.
    """
    targets: dict[str, Any] = {}
    
    realized_edge = compute_realized_edge(
        row.calibrated_prob,
        row.market_price,
        row.resolution_outcome,
        row.actual_bucket,
        row.bucket,
    )
    
    if realized_edge is not None:
        targets["realized_edge"] = realized_edge
        targets["realized_edge_normalized"] = normalize_realized_edge(
            realized_edge, row.market_price, method="price_adjusted"
        )
        
        payout = compute_payout(row.market_price) if row.market_price else None
        if payout and payout > 0:
            targets["realized_pnl"] = (
                (1.0 if row.resolution_outcome == "win" else 0.0) * payout - row.cost
                if row.cost else None
            )
    
    resolution_confidence = compute_resolution_confidence(
        row.actual_temp,
        row.bucket_low,
        row.bucket_high,
        row.city_source_mae,
    )
    if resolution_confidence is not None:
        targets["resolution_confidence"] = resolution_confidence
    
    return targets


def enrich_row_with_targets(row: DatasetRowV3) -> DatasetRowV3:
    """Enrich a V3 row with computed target fields."""
    targets = compute_ml_target(row)
    row_dict = row.to_dict()
    row_dict.update(targets)
    return DatasetRowV3(**row_dict)


def validate_target_distribution(rows: list[DatasetRowV3]) -> dict[str, Any]:
    """Validate that target distribution is suitable for ML.
    
    Returns a report with statistics and warnings.
    """
    resolved_rows = [r for r in rows if r.actual_temp is not None or r.resolution_outcome is not None]
    
    if not resolved_rows:
        return {
            "status": "no_resolved_rows",
            "resolved_count": 0,
            "warnings": ["No resolved rows to analyze"],
        }
    
    realized_edges = [r.realized_edge for r in resolved_rows if r.realized_edge is not None]
    
    stats: dict[str, Any] = {
        "resolved_count": len(resolved_rows),
        "total_count": len(rows),
        "resolution_rate": len(resolved_rows) / len(rows) if rows else 0,
    }
    
    if realized_edges:
        stats.update({
            "edge_mean": sum(realized_edges) / len(realized_edges),
            "edge_min": min(realized_edges),
            "edge_max": max(realized_edges),
            "edge_std": (
                math.sqrt(sum((e - sum(realized_edges) / len(realized_edges)) ** 2 for e in realized_edges) / len(realized_edges))
                if len(realized_edges) > 1 else 0
            ),
        })
        
        positive_edges = [e for e in realized_edges if e > 0]
        negative_edges = [e for e in realized_edges if e < 0]
        stats.update({
            "positive_edges": len(positive_edges),
            "negative_edges": len(negative_edges),
            "win_rate": len(positive_edges) / len(realized_edges) if realized_edges else 0,
        })
        
        warnings: list[str] = []
        
        if len(realized_edges) < 30:
            warnings.append("low_sample_size")
        
        edge_mean = stats.get("edge_mean", 0)
        if edge_mean < -0.2:
            warnings.append("significant_negative_bias")
        
        win_rate = stats.get("win_rate", 0)
        if win_rate < 0.3:
            warnings.append("low_win_rate")
        elif win_rate > 0.9:
            warnings.append("unusually_high_win_rate")
        
        stats["warnings"] = warnings
        stats["status"] = "ok" if not warnings else "review_recommended"
    
    return stats


class TargetLayer:
    """Target computation for V3 dataset pipeline."""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
    
    def process_v3_file(self, input_path: Optional[Path] = None, output_path: Optional[Path] = None) -> dict[str, Any]:
        """Read V3 dataset, enrich with targets, write back."""
        from .schema_v3 import load_rows_v3
        
        input_path = input_path or self.data_dir / "dataset_rows_v3.jsonl"
        output_path = output_path or self.data_dir / "dataset_rows_v3_enriched.jsonl"
        
        if not input_path.exists():
            return {"status": "input_not_found", "path": str(input_path)}
        
        rows = load_rows_v3(input_path)
        
        enriched_rows = [enrich_row_with_targets(row) for row in rows]
        
        count = 0
        output_path.parent.mkdir(exist_ok=True)
        with output_path.open("w", encoding="utf-8") as f:
            for row in enriched_rows:
                f.write(json.dumps(row.to_dict(), ensure_ascii=False) + "\n")
                count += 1
        
        validation = validate_target_distribution(enriched_rows)
        
        return {
            "status": "success",
            "rows_processed": count,
            "validation": validation,
            "output_path": str(output_path),
        }