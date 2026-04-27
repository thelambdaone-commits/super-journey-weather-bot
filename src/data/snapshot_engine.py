"""
Snapshot Engine V3: transforms market JSON → ML-ready temporal rows.
One row per market per scan = captures forecast/market dynamics.
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Optional

from .schema_v3 import DatasetRowV3, SCAN_MAX_DEPTH


class SnapshotEngine:
    """Transforms market snapshots into temporal ML-ready rows."""

    def __init__(self, data_dir: str = "data", ml_stats: Optional[dict] = None):
        self.data_dir = Path(data_dir)
        self.markets_dir = self.data_dir / "markets"
        self.ml_stats = ml_stats or {}
        self._cache: dict[str, dict] = {}

    def _load_ml_stats(self) -> dict:
        """Load ML model stats if not already loaded."""
        if not self.ml_stats:
            ml_path = self.data_dir / "ml_model.json"
            if ml_path.exists():
                try:
                    self.ml_stats = json.loads(ml_path.read_text(encoding="utf-8"))
                except (Exception,) as e:
                    self.ml_stats = {}
        return self.ml_stats

    def _detect_market_regime(
        self, 
        market_snapshots: list[dict],
        current_price: float,
        entry_price: Optional[float] = None
    ) -> str:
        """Detect market regime: stable, volatile, or trending."""
        if len(market_snapshots) < 2:
            return "stable"
        
        prices = [s.get("top_price", 0) for s in market_snapshots if s.get("top_price")]
        if len(prices) < 2:
            return "stable"
        
        mean_price = sum(prices) / len(prices)
        variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std = variance ** 0.5
        
        if std > 0.1:
            return "volatile"
        
        if entry_price and abs(current_price - entry_price) > 0.15:
            return "trending"
        
        if std > 0.05:
            return "volatile"
        
        return "stable"

    def _compute_price_movement(
        self,
        market_snapshots: list[dict],
        market: dict,
        scan_index: int
    ) -> Optional[float]:
        """Compute price movement from discovery to current scan."""
        entry_price = market.get("position", {}).get("entry_price") if market.get("position") else None
        if not entry_price:
            return None
        
        if scan_index == 0:
            return 0.0
        
        current_price = market_snapshots[min(scan_index, len(market_snapshots) - 1)].get("top_price") if market_snapshots else entry_price
        return (current_price or entry_price) - entry_price

    def _get_top_bucket_evolution(self, market_snapshots: list[dict]) -> list[str]:
        """Extract top bucket history."""
        return [s.get("top_bucket", "unknown") for s in market_snapshots if s.get("top_bucket")]

    def process_market(self, market: dict[str, Any]) -> list[DatasetRowV3]:
        """Convert a market JSON into one row per scan snapshot.
        
        Returns up to SCAN_MAX_DEPTH rows, one per forecast snapshot.
        """
        market_id = market.get("position", {}).get("market_id") if market.get("position") else market.get("market_id", "")
        sequence_id = f"{market.get('city', 'unknown')}_{market.get('date', 'unknown')}_{market_id}"
        
        forecast_snapshots = market.get("forecast_snapshots", [])
        market_snapshots = market.get("market_snapshots", [])
        
        ml_stats = self._load_ml_stats()
        rows: list[DatasetRowV3] = []
        
        max_scans = min(len(forecast_snapshots), SCAN_MAX_DEPTH)
        
        for scan_index in range(max_scans):
            scan_snap = forecast_snapshots[scan_index]
            market_snap = market_snapshots[scan_index] if scan_index < len(market_snapshots) else {}
            previous_snap = forecast_snapshots[scan_index - 1] if scan_index > 0 else None
            
            entry_price = market.get("position", {}).get("entry_price") if market.get("position") else None
            current_price = market_snap.get("top_price") or entry_price
            
            regime = self._detect_market_regime(
                market_snapshots[:scan_index + 1],
                current_price or 0,
                entry_price
            )
            
            row = DatasetRowV3.from_market_json(
                market=market,
                scan_index=scan_index,
                scan_snapshot=scan_snap,
                market_snapshot=market_snap,
                ml_stats=ml_stats,
                previous_snapshot=previous_snap,
            )
            
            row.scan_sequence_id = sequence_id
            row.market_regime = regime
            row.price_movement_since_discovery = self._compute_price_movement(
                market_snapshots, market, scan_index
            )
            row.top_bucket_evolution = self._get_top_bucket_evolution(
                market_snapshots[:scan_index + 1]
            )
            
            rows.append(row)
        
        return rows

    def process_market_file(self, market_path: Path) -> list[DatasetRowV3]:
        """Load and process a single market JSON file."""
        try:
            market = json.loads(market_path.read_text(encoding="utf-8"))
        except (Exception,) as e:
            return []
        return self.process_market(market)

    def process_all_markets(self, limit: Optional[int] = None) -> list[DatasetRowV3]:
        """Process all market files in data/markets/.
        
        Args:
            limit: Optional limit for testing. If None, processes all.
        """
        if not self.markets_dir.exists():
            return []
        
        all_rows: list[DatasetRowV3] = []
        
        market_files = sorted(self.markets_dir.glob("*.json"))
        if limit:
            market_files = market_files[:limit]
        
        for market_file in market_files:
            rows = self.process_market_file(market_file)
            all_rows.extend(rows)
        
        return all_rows

    def write_rows(self, rows: list[DatasetRowV3], output_path: Optional[Path] = None) -> int:
        """Append rows to V3 dataset file.
        
        Returns the number of rows written.
        """
        if not rows:
            return 0
        
        path = output_path or (self.data_dir / "dataset_rows_v3.jsonl")
        path.parent.mkdir(exist_ok=True)
        
        count = 0
        with path.open("a", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row.to_dict(), ensure_ascii=False) + "\n")
                count += 1
        
        return count

    def run_full_pipeline(self, limit: Optional[int] = None, output_path: Optional[Path] = None) -> dict[str, Any]:
        """Full pipeline: load markets → process → write V3 rows.
        
        Returns statistics about the run.
        """
        rows = self.process_all_markets(limit=limit)
        
        if not rows:
            return {
                "markets_processed": 0,
                "rows_generated": 0,
                "output_path": str(output_path or self.data_dir / "dataset_rows_v3.jsonl"),
            }
        
        count = self.write_rows(rows, output_path)
        
        resolved_rows = [r for r in rows if r.actual_temp is not None]
        decision_rows = [r for r in rows if r.decision_size is not None]
        
        return {
            "markets_processed": len(set(r.scan_sequence_id for r in rows)),
            "rows_generated": count,
            "resolved_rows": len(resolved_rows),
            "decision_rows": len(decision_rows),
            "output_path": str(output_path or self.data_dir / "dataset_rows_v3.jsonl"),
        }


def get_snapshot_engine(data_dir: str = "data") -> SnapshotEngine:
    """Factory function to get a configured SnapshotEngine."""
    return SnapshotEngine(data_dir=data_dir)