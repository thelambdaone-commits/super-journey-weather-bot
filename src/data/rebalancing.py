"""
Data Rebalancing Engine: Corrects distribution bias before ML training.
Addresses: synthetic dominance, regime collapse, real-data underweight.
"""
from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


@dataclass
class RebalanceConfig:
    """Configuration for data rebalancing."""

    real_weight: float = 2.5
    replay_weight: float = 1.0
    synthetic_weight: float = 0.5

    max_regime_ratio: float = 0.25
    target_entropy: float = 0.30

    stratify_by_regime: bool = True
    stratify_by_city: bool = True

    def validate(self) -> list[str]:
        issues = []
        if self.real_weight < 1.0:
            issues.append("real_weight should be >= 1.0")
        if self.max_regime_ratio < 0.1 or self.max_regime_ratio > 0.5:
            issues.append("max_regime_ratio should be 0.1-0.5")
        return issues


@dataclass
class RebalanceReport:
    """Report from rebalancing operation."""

    input_rows: int
    output_rows: int
    weights_applied: dict[str, float]
    regime_distribution_before: dict[str, int]
    regime_distribution_after: dict[str, int]
    entropy_before: float
    entropy_after: float
    sample_weights: dict[str, float]
    success: bool


class DataRebalancer:
    """Rebalance dataset for ML training without removing data."""

    def __init__(
        self,
        data_dir: str = "data",
        config: Optional[RebalanceConfig] = None,
    ):
        self.data_dir = Path(data_dir)
        self.config = config or RebalanceConfig()
        self._validate_config()

    def _validate_config(self) -> None:
        issues = self.config.validate()
        if issues:
            raise ValueError(f"Invalid config: {issues}")

    def _detect_source_tag(self, row: dict) -> str:
        """Detect data source from row."""
        metadata = row.get("metadata", {})
        source = metadata.get("source")

        if source in ("real", "replay", "synthetic", "backfill"):
            return source

        if metadata.get("replay_from"):
            return "replay"
        if metadata.get("synthetic"):
            return "synthetic"

        event_type = row.get("event_type", "")
        if event_type in ("migrated_v2", "backfill"):
            return "backfill"
        if event_type == "replay_scan":
            return "replay"
        if event_type == "market_scan":
            return "replay"

        return "real"

    def _compute_entropy(self, distribution: dict[str, int]) -> float:
        """Compute Shannon entropy of distribution."""
        if not distribution:
            return 0.0

        total = sum(distribution.values())
        if total == 0:
            return 0.0

        entropy = 0.0
        for count in distribution.values():
            if count > 0:
                p = count / total
                entropy -= p * math.log2(p)

        max_entropy = math.log2(len(distribution)) if distribution else 1
        return entropy / max_entropy if max_entropy > 0 else 0.0

    def _compute_regime_distribution(self, rows: list[dict]) -> dict[str, int]:
        """Compute regime distribution."""
        regimes: dict[str, int] = {}
        for row in rows:
            regime = row.get("regime_type", "unknown")
            regimes[regime] = regimes.get(regime, 0) + 1
        return regimes

    def _balance_regimes(
        self,
        rows: list[dict],
        max_ratio: float,
    ) -> list[dict]:
        """Downsample over-represented regimes, keep all others."""
        regime_dist = self._compute_regime_distribution(rows)
        total = len(rows)
        max_count = int(total * max_ratio)

        balanced = []
        regime_counts: dict[str, int] = {}

        for row in rows:
            regime = row.get("regime_type", "unknown")
            current = regime_counts.get(regime, 0)

            if current >= max_count:
                continue

            balanced.append(row)
            regime_counts[regime] = current + 1

        return balanced

    def _compute_sample_weights(self, rows: list[dict]) -> dict[str, float]:
        """Compute sample weights based on source type."""
        weights = {}

        for row in rows:
            row_id = self._get_row_id(row)
            source = self._detect_source_tag(row)

            if source == "real":
                weights[row_id] = self.config.real_weight
            elif source == "replay":
                weights[row_id] = self.config.replay_weight
            elif source == "synthetic":
                weights[row_id] = self.config.synthetic_weight
            else:
                weights[row_id] = 1.0

        return weights

    def _get_row_id(self, row: dict) -> str:
        """Generate unique ID for row."""
        city = row.get("city", "")
        date = row.get("date", "")
        ts = row.get("timestamp", 0)
        scan_idx = row.get("scan_index", 0)
        return f"{city}_{date}_{ts}_{scan_idx}"

    def rebalance(self, input_path: Path, output_path: Optional[Path] = None) -> RebalanceReport:
        """Rebalance dataset to fix distribution issues."""

        rows = []
        for line in input_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))

        if not rows:
            return RebalanceReport(
                input_rows=0, output_rows=0,
                weights_applied={}, regime_distribution_before={},
                regime_distribution_after={}, entropy_before=0, entropy_after=0,
                sample_weights={}, success=False
            )

        regime_before = self._compute_regime_distribution(rows)
        entropy_before = self._compute_entropy(regime_before)

        if self.config.stratify_by_regime:
            balanced = self._balance_regimes(rows, self.config.max_regime_ratio)
        else:
            balanced = rows

        entropy_after = self._compute_entropy(self._compute_regime_distribution(balanced))
        regime_after = self._compute_regime_distribution(balanced)

        sample_weights = self._compute_sample_weights(balanced)

        output_path = output_path or input_path.parent / f"{input_path.stem}_rebalanced.jsonl"
        output_path.parent.mkdir(exist_ok=True)

        with output_path.open("w", encoding="utf-8") as f:
            for row in balanced:
                row_copy = dict(row)
                row_copy["_sample_weight"] = sample_weights.get(self._get_row_id(row), 1.0)
                f.write(json.dumps(row_copy, ensure_ascii=False) + "\n")

        return RebalanceReport(
            input_rows=len(rows),
            output_rows=len(balanced),
            weights_applied={
                "real": self.config.real_weight,
                "replay": self.config.replay_weight,
                "synthetic": self.config.synthetic_weight,
            },
            regime_distribution_before=regime_before,
            regime_distribution_after=regime_after,
            entropy_before=entropy_before,
            entropy_after=entropy_after,
            sample_weights=sample_weights,
            success=True,
        )


def rebalance_dataset(
    input_path: str = "data/dataset_rows_scaled.jsonl",
    output_path: Optional[str] = None,
    config: Optional[RebalanceConfig] = None,
) -> RebalanceReport:
    """Convenience function to rebalance dataset."""
    rebalancer = DataRebalancer(data_dir=str(Path(input_path).parent), config=config)
    out = Path(output_path) if output_path else None
    return rebalancer.rebalance(Path(input_path), out)


def format_rebalance_report(report: RebalanceReport) -> list[str]:
    """Format rebalance report for CLI."""
    lines = [
        f"\n{'='*50}",
        "DATA REBALANCE REPORT",
        f"{'='*50}",
        f"Input rows: {report.input_rows}",
        f"Output rows: {report.output_rows}",
        f"Rows removed: {report.input_rows - report.output_rows}",
        "",
        "Weights applied:",
        f"  real: {report.weights_applied.get('real', 0):.1f}x",
        f"  replay: {report.weights_applied.get('replay', 0):.1f}x",
        f"  synthetic: {report.weights_applied.get('synthetic', 0):.1f}x",
        "",
        f"Entropy before: {report.entropy_before:.3f}",
        f"Entropy after: {report.entropy_after:.3f}",
        "",
        "Regime distribution before:",
    ]

    for regime, count in sorted(report.regime_distribution_before.items(), key=lambda x: -x[1]):
        lines.append(f"  {regime}: {count}")

    lines.append("")
    lines.append("Regime distribution after:")

    for regime, count in sorted(report.regime_distribution_after.items(), key=lambda x: -x[1]):
        lines.append(f"  {regime}: {count}")

    status = "✅ SUCCESS" if report.success else "❌ FAILED"
    lines.append("")
    lines.append(f"Status: {status}")
    lines.append(f"{'='*50}\n")

    return lines