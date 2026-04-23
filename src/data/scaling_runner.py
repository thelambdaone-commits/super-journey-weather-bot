"""
Scaling Runner: Orchestrates the complete data scaling pipeline.
Deterministic, reproducible dataset builds.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from .data_integrity import DataIntegrityChecker, ScalingRuleset
from .market_replay import MarketReplayEngine
from .regime_augment import RegimeAugmenter
from .snapshot_engine import SnapshotEngine


@dataclass
class ScalingConfig:
    """Configuration for data scaling pipeline."""

    target_rows: int = 2000
    scans_per_market: int = 30
    use_regime_augmentation: bool = True
    use_integrity_check: bool = True
    output_filename: str = "dataset_rows_scaled.jsonl"
    ruleset: Optional[ScalingRuleset] = None

    def __post_init__(self):
        if self.ruleset is None:
            self.ruleset = ScalingRuleset()


@dataclass
class ScalingReport:
    """Report from scaling pipeline execution."""

    config: dict[str, Any]
    markets_processed: int
    real_rows_added: int
    replay_rows_added: int
    synthetic_rows_added: int
    total_rows: int
    regime_distribution: dict[str, int]
    integrity_report: dict[str, Any]
    output_path: str
    duration_seconds: float
    success: bool
    warnings: list[str]
    errors: list[str]


class ScalingRunner:
    """Orchestrates the complete data scaling pipeline."""

    def __init__(
        self,
        data_dir: str = "data",
        config: Optional[ScalingConfig] = None,
    ):
        self.data_dir = Path(data_dir)
        self.config = config or ScalingConfig()
        self.ruleset = self.config.ruleset or ScalingRuleset()

        self.snapshot_engine = SnapshotEngine(data_dir=data_dir)
        self.replay_engine = MarketReplayEngine(data_dir=data_dir)
        self.regime_augmenter = RegimeAugmenter()
        self.integrity_checker = DataIntegrityChecker(data_dir=data_dir, ruleset=self.ruleset)

    def run(self) -> ScalingReport:
        """Execute the full scaling pipeline."""
        start_time = datetime.now()
        warnings: list[str] = []
        errors: list[str] = []

        try:
            existing_rows = self._load_existing_v3_rows()
            real_rows = self._generate_real_base(existing_rows)
            replay_rows = self._generate_replay_rows()
            combined_rows = real_rows + replay_rows

            if self.config.use_regime_augmentation:
                combined_rows = self.regime_augmenter.augment_rows(combined_rows)

            self._write_scaled_dataset(combined_rows)

            integrity_report = {}
            if self.config.use_integrity_check:
                integrity_check = self.integrity_checker.run_check()
                integrity_report = {
                    "integrity_score": integrity_check.integrity_score,
                    "integrity_grade": integrity_check.integrity_grade,
                    "real_ratio": integrity_check.real_ratio,
                    "diversity_index": integrity_check.diversity_index,
                    "regime_entropy": integrity_check.regime_entropy,
                    "warnings": integrity_check.warnings,
                    "recommendations": integrity_check.recommendations,
                }

            duration = (datetime.now() - start_time).total_seconds()
            total_rows = len(combined_rows)

            regime_dist = self._count_regimes(combined_rows)

            if integrity_report:
                if integrity_report.get("real_ratio", 1) < self.ruleset.synthetic_max_ratio:
                    warnings.append(f"real_ratio_low: {integrity_report.get('real_ratio', 0):.1%}")
                if integrity_report.get("diversity_index", 1) < 0.4:
                    warnings.append("diversity_index_low")

            return ScalingReport(
                config={
                    "target_rows": self.config.target_rows,
                    "scans_per_market": self.config.scans_per_market,
                    "use_regime_augmentation": self.config.use_regime_augmentation,
                },
                markets_processed=len(set(r.get("scan_sequence_id", "") for r in real_rows)),
                real_rows_added=len(real_rows),
                replay_rows_added=len(replay_rows),
                synthetic_rows_added=0,
                total_rows=total_rows,
                regime_distribution=regime_dist,
                integrity_report=integrity_report,
                output_path=str(self.data_dir / self.config.output_filename),
                duration_seconds=duration,
                success=True,
                warnings=warnings,
                errors=errors,
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            return ScalingReport(
                config={},
                markets_processed=0,
                real_rows_added=0,
                replay_rows_added=0,
                synthetic_rows_added=0,
                total_rows=0,
                regime_distribution={},
                integrity_report={},
                output_path="",
                duration_seconds=duration,
                success=False,
                warnings=warnings,
                errors=[str(e)],
            )

    def _load_existing_v3_rows(self) -> list[dict[str, Any]]:
        """Load existing V3 dataset if present."""
        v3_path = self.data_dir / "dataset_rows_v3.jsonl"
        if not v3_path.exists():
            return []

        rows = []
        for line in v3_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
        return rows

    def _generate_real_base(self, existing_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Generate base rows from existing V3 data, marked as real."""
        if existing_rows:
            for row in existing_rows:
                row["metadata"] = row.get("metadata", {})
                row["metadata"]["source"] = "real"
            return existing_rows

        rows = list(self.snapshot_engine.process_all_markets())
        result = []
        for row in rows:
            row_dict = row.to_dict()
            row_dict["metadata"] = {"source": "real"}
            result.append(row_dict)
        return result

    def _generate_replay_rows(self) -> list[dict[str, Any]]:
        """Generate replay rows with proper quality tags."""
        from src.data.market_replay import MarketReplayEngine
        
        temp_engine = MarketReplayEngine(data_dir=str(self.data_dir))
        replays = temp_engine.replay_all_markets(target_scans=self.config.scans_per_market)

        replay_rows = []
        for replay in replays:
            replay_row = dict(replay)
            replay_row["scan_sequence_id"] = f"{replay['city']}_{replay['date']}_{replay['market_id']}"
            replay_row["event_type"] = "replay_scan"
            replay_row["action"] = "OBSERVE"
            replay_row["city_name"] = replay["city"].replace("-", " ").title()
            replay_row["station"] = None
            replay_row["market_id"] = replay.get("market_id")
            replay_row["question"] = None
            replay_row["forecast_source"] = replay.get("source")
            replay_row["forecast_horizon"] = replay.get("horizon")
            replay_row["raw_forecast_temp"] = replay.get("temp")
            replay_row["ecmwf_max"] = replay.get("ecmwf")
            replay_row["hrrr_max"] = replay.get("hrrr")
            replay_row["ensemble_mean"] = replay.get("temp")
            replay_row["ensemble_std"] = replay.get("confidence", 0.5)
            replay_row["market_price"] = replay.get("adjusted_price")
            replay_row["market_implied_prob"] = replay.get("adjusted_price")
            replay_row["forecast_market_gap"] = replay.get("forecast_market_gap")
            replay_row["hours_to_resolution"] = replay.get("hours_left")
            replay_row["timestamp"] = int(datetime.fromisoformat(replay["ts"]).timestamp())
            replay_row["version"] = "3.0"
            
            metadata = replay.get("metadata", {})
            metadata["source"] = metadata.get("source", "replay")
            replay_row["metadata"] = metadata

            replay_rows.append(replay_row)

        return replay_rows

    def _write_scaled_dataset(self, rows: list[dict[str, Any]]) -> Path:
        """Write the scaled dataset to file."""
        output_path = self.data_dir / self.config.output_filename
        output_path.parent.mkdir(exist_ok=True)

        with output_path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

        return output_path

    def _count_regimes(self, rows: list[dict[str, Any]]) -> dict[str, int]:
        """Count regime distribution."""
        counts: dict[str, int] = {}
        for row in rows:
            regime = row.get("regime_type", "unknown")
            counts[regime] = counts.get(regime, 0) + 1
        return counts


def run_scaling_pipeline(
    data_dir: str = "data",
    target_rows: int = 2000,
    scans_per_market: int = 30,
    use_regime_augmentation: bool = True,
) -> ScalingReport:
    """Convenience function to run scaling pipeline."""
    config = ScalingConfig(
        target_rows=target_rows,
        scans_per_market=scans_per_market,
        use_regime_augmentation=use_regime_augmentation,
    )
    runner = ScalingRunner(data_dir=data_dir, config=config)
    return runner.run()


def format_scaling_report(report: ScalingReport) -> list[str]:
    """Format scaling report for CLI output."""
    lines = [
        f"\n{'='*50}",
        "DATA SCALING REPORT",
        f"{'='*50}",
        f"Target rows: {report.config.get('target_rows', 'N/A')}",
        f"Scans/market: {report.config.get('scans_per_market', 'N/A')}",
        f"Duration: {report.duration_seconds:.2f}s",
        "",
        f"Markets processed: {report.markets_processed}",
        f"Real rows: {report.real_rows_added}",
        f"Replay rows: {report.replay_rows_added}",
        f"Total rows: {report.total_rows}",
        "",
        "Regime distribution:",
    ]

    for regime, count in sorted(report.regime_distribution.items(), key=lambda x: -x[1]):
        lines.append(f"  {regime}: {count}")

    if report.integrity_report:
        lines.append("")
        lines.append(f"Integrity score: {report.integrity_report.get('integrity_score', 0):.1f}/100 ({report.integrity_report.get('integrity_grade', 'N/A')})")
        lines.append(f"Real ratio: {report.integrity_report.get('real_ratio', 0):.1%}")
        lines.append(f"Diversity index: {report.integrity_report.get('diversity_index', 0):.3f}")

    if report.warnings:
        lines.append("")
        lines.append("Warnings:")
        for w in report.warnings:
            lines.append(f"  ⚠️ {w}")

    if report.errors:
        lines.append("")
        lines.append("Errors:")
        for e in report.errors:
            lines.append(f"  ❌ {e}")

    status = "✅ SUCCESS" if report.success else "❌ FAILED"
    lines.append("")
    lines.append(f"Status: {status}")
    lines.append(f"Output: {report.output_path}")
    lines.append(f"{'='*50}\n")

    return lines