"""
Data Integrity Layer: Guards against synthetic overfitting and label leakage.
Ensures ML dataset quality for quantitative research.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


@dataclass
class DataIntegrityReport:
    """Report on dataset integrity and synthetic contamination risk."""

    total_rows: int
    real_rows: int
    replay_rows: int
    synthetic_rows: int
    backfill_rows: int

    real_ratio: float
    replay_ratio: float
    synthetic_ratio: float

    diversity_index: float
    regime_entropy: float
    source_disagreement_score: float

    leakage_detected: bool
    leakage_details: list[str]

    integrity_score: float
    integrity_grade: str

    warnings: list[str]
    recommendations: list[str]


@dataclass
class ScalingRuleset:
    """Ruleset for safe augmentation."""

    max_replay_scans_per_market: int = 30
    min_real_rows_for_replay: int = 50
    replay_max_deviation: float = 0.15
    synthetic_max_ratio: float = 0.4

    allow_interpolation: bool = True
    allow_backfill: bool = True
    allow_synthesis: bool = True

    train_split_requires_real: bool = True
    validation_mixed_allowed: bool = True
    test_split_requires_real: bool = True

    def validate(self) -> list[str]:
        """Validate ruleset consistency."""
        issues = []
        if self.max_replay_scans_per_market < 10:
            issues.append("max_replay_scans too low (< 10)")
        if self.replay_max_deviation > 0.3:
            issues.append("replay_max_deviation too high (> 0.3)")
        if self.synthetic_max_ratio > 0.6:
            issues.append("synthetic_max_ratio too high (> 0.6)")
        return issues


DEFAULT_RULESET = ScalingRuleset()


class DataIntegrityChecker:
    """Checks dataset for integrity issues."""

    def __init__(
        self,
        data_dir: str = "data",
        ruleset: Optional[ScalingRuleset] = None,
    ):
        self.data_dir = Path(data_dir)
        self.ruleset = ruleset or DEFAULT_RULESET
        self._validate_ruleset()

    def _validate_ruleset(self) -> None:
        issues = self.ruleset.validate()
        if issues:
            raise ValueError(f"Invalid ruleset: {issues}")

    def _detect_data_tag(self, row: dict) -> str:
        """Detect the data source tag of a row."""
        metadata = row.get("metadata", {})

        source = metadata.get("source")
        if source == "real":
            return "real"
        if source == "backfill":
            return "backfill"
        if source == "replay":
            return "replay"
        if source == "synthetic":
            return "synthetic"

        if metadata.get("replay_from"):
            return "replay"
        if metadata.get("synthetic"):
            return "synthetic"

        event_type = row.get("event_type", "")
        if event_type == "migrated_v2":
            return "backfill"
        if event_type == "replay_scan":
            return "replay"

        return "real"

    def compute_diversity_index(self, rows: list[dict]) -> float:
        """Compute diversity index based on feature variance.
        
        0.0 = all identical
        1.0 = maximum diversity
        """
        if not rows:
            return 0.0

        numeric_fields = ["forecast_temp", "market_price", "hours_to_resolution"]
        variances = []

        for field in numeric_fields:
            values = [r.get(field, 0) for r in rows if r.get(field) is not None]
            if len(values) < 2:
                continue

            mean = sum(values) / len(values)
            variance = sum((v - mean) ** 2 for v in values) / len(values)
            max_variance = (max(values) - min(values)) ** 2 / 4 if len(values) > 1 else 0

            if max_variance > 0:
                normalized = 1 - (variance / max_variance)
                variances.append(normalized)

        return sum(variances) / len(variances) if variances else 0.0

    def compute_regime_entropy(self, rows: list[dict]) -> float:
        """Compute entropy of regime distribution.
        
        High entropy = good diversity
        Low entropy = regime collapse
        """
        if not rows:
            return 0.0

        regimes = [r.get("market_regime", "unknown") for r in rows]
        regime_counts: dict[str, int] = {}

        for regime in regimes:
            regime_counts[regime] = regime_counts.get(regime, 0) + 1

        total = len(regimes)
        entropy = 0.0

        for count in regime_counts.values():
            p = count / total
            if p > 0:
                entropy -= p * math.log2(p)

        max_entropy = math.log2(len(regime_counts)) if regime_counts else 1
        return entropy / max_entropy if max_entropy > 0 else 0.0

    def compute_source_disagreement(self, rows: list[dict]) -> float:
        """Compute average disagreement between forecast sources.
        
        High disagreement = sources are independent
        Low disagreement = source correlation artificial
        """
        if not rows:
            return 0.0

        disagreements = []
        for row in rows:
            ecmwf = row.get("ecmwf_max")
            hrrr = row.get("hrrr_max")
            gfs = row.get("gfs_max")

            temps = [t for t in [ecmwf, hrrr, gfs] if t is not None]

            if len(temps) < 2:
                continue

            mean = sum(temps) / len(temps)
            variance = sum((t - mean) ** 2 for t in temps) / len(temps)
            std = variance ** 0.5

            if mean != 0:
                cv = std / abs(mean)
                disagreements.append(min(cv, 2.0))

        return sum(disagreements) / len(disagreements) if disagreements else 0.0

    def detect_leakage(self, rows: list[dict]) -> tuple[bool, list[str]]:
        """Detect potential label leakage."""
        leaks: list[str] = []

        future_temp_count = 0
        for row in rows:
            future_marker = row.get("metadata", {}).get("future_temp_used")
            if future_marker:
                future_temp_count += 1

        if future_temp_count > len(rows) * 0.1:
            leaks.append(f"future_temp_contamination: {future_temp_count} rows")

        actual_temp_early = 0
        for row in rows:
            hours = row.get("hours_to_resolution", 999)
            if hours > 12 and row.get("actual_temp") is not None:
                actual_temp_early += 1

        if actual_temp_early > len(rows) * 0.2:
            leaks.append("early_resolution_backfill: may contain leakage")

        return len(leaks) > 0, leaks

    def check_synthetic_ratio(self, tag_counts: dict[str, int]) -> list[str]:
        """Check if synthetic data ratio is within bounds."""
        warnings = []
        total = sum(tag_counts.values())
        if total == 0:
            return warnings

        for tag, count in tag_counts.items():
            ratio = count / total
            if tag == "synthetic" and ratio > self.ruleset.synthetic_max_ratio:
                warnings.append(
                    f"synthetic_ratio_exceeded: {ratio:.1%} > {self.ruleset.synthetic_max_ratio:.1%}"
                )

        return warnings

    def compute_integrity_score(self, report: DataIntegrityReport) -> float:
        """Compute overall integrity score 0-100."""
        score = 50.0

        score += report.real_ratio * 30

        score += report.diversity_index * 10

        score += report.regime_entropy * 10

        if report.leakage_detected:
            score -= 30

        warnings_penalty = len(report.warnings) * 5
        score -= warnings_penalty

        return max(0.0, min(100.0, score))

    def grade_from_score(self, score: float) -> str:
        """Convert score to letter grade."""
        if score >= 90:
            return "A+"
        elif score >= 80:
            return "A"
        elif score >= 70:
            return "B"
        elif score >= 60:
            return "C"
        elif score >= 50:
            return "D"
        else:
            return "F"

    def run_check(self, dataset_path: Optional[Path] = None) -> DataIntegrityReport:
        """Run full integrity check on dataset."""
        path = dataset_path or self.data_dir / "dataset_rows_v3.jsonl"

        if not path.exists():
            return DataIntegrityReport(
                total_rows=0, real_rows=0, replay_rows=0, synthetic_rows=0, backfill_rows=0,
                real_ratio=0, replay_ratio=0, synthetic_ratio=0,
                diversity_index=0, regime_entropy=0, source_disagreement_score=0,
                leakage_detected=False, leakage_details=[],
                integrity_score=0, integrity_grade="F",
                warnings=["dataset_not_found"], recommendations=["create_dataset"]
            )

        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rows.append(json.loads(line))

        if not rows:
            return DataIntegrityReport(
                total_rows=0, real_rows=0, replay_rows=0, synthetic_rows=0, backfill_rows=0,
                real_ratio=0, replay_ratio=0, synthetic_ratio=0,
                diversity_index=0, regime_entropy=0, source_disagreement_score=0,
                leakage_detected=False, leakage_details=[],
                integrity_score=0, integrity_grade="F",
                warnings=["empty_dataset"], recommendations=["collect_data"]
            )

        tag_counts: dict[str, int] = {"real": 0, "replay": 0, "synthetic": 0, "backfill": 0}

        for row in rows:
            tag = self._detect_data_tag(row)
            tag_counts[tag] = tag_counts.get(tag, 0) + 1

        total = len(rows)

        report = DataIntegrityReport(
            total_rows=total,
            real_rows=tag_counts["real"],
            replay_rows=tag_counts["replay"],
            synthetic_rows=tag_counts["synthetic"],
            backfill_rows=tag_counts["backfill"],
            real_ratio=tag_counts["real"] / total,
            replay_ratio=tag_counts["replay"] / total,
            synthetic_ratio=tag_counts["synthetic"] / total,
            diversity_index=self.compute_diversity_index(rows),
            regime_entropy=self.compute_regime_entropy(rows),
            source_disagreement_score=self.compute_source_disagreement(rows),
            leakage_detected=False,
            leakage_details=[],
            integrity_score=0,
            integrity_grade="F",
            warnings=[],
            recommendations=[],
        )

        leakage_found, leakage_details = self.detect_leakage(rows)
        report.leakage_detected = leakage_found
        report.leakage_details = leakage_details

        report.warnings = self.check_synthetic_ratio(tag_counts)

        if report.diversity_index < 0.3:
            report.warnings.append("low_feature_diversity")
        if report.regime_entropy < 0.3:
            report.warnings.append("regime_collapse")

        report.recommendations = self._generate_recommendations(report)

        report.integrity_score = self.compute_integrity_score(report)
        report.integrity_grade = self.grade_from_score(report.integrity_score)

        return report

    def _generate_recommendations(self, report: DataIntegrityReport) -> list[str]:
        """Generate recommendations based on report."""
        recs = []

        if report.real_ratio < 0.5:
            recs.append("increase_real_data_collection")

        if report.diversity_index < 0.5:
            recs.append("improve_feature_variance")

        if report.regime_entropy < 0.4:
            recs.append("add_regime_diversity")

        if report.synthetic_ratio > 0.3:
            recs.append("reduce_synthetic_overfitting_risk")

        if report.leakage_detected:
            recs.append("fix_leakage_before_ml_training")

        return recs


def run_integrity_check(
    data_dir: str = "data",
    dataset_path: Optional[str] = None,
) -> DataIntegrityReport:
    """Convenience function to run integrity check."""
    checker = DataIntegrityChecker(data_dir)
    path = Path(dataset_path) if dataset_path else None
    return checker.run_check(path)


def format_integrity_report(report: DataIntegrityReport) -> list[str]:
    """Format integrity report for CLI output."""
    lines = [
        f"\n{'='*50}",
        "DATA INTEGRITY REPORT",
        f"{'='*50}",
        f"Total rows: {report.total_rows}",
        f"  Real: {report.real_rows} ({report.real_ratio:.1%})",
        f"  Replay: {report.replay_rows} ({report.replay_ratio:.1%})",
        f"  Synthetic: {report.synthetic_rows} ({report.synthetic_ratio:.1%})",
        f"  Backfill: {report.backfill_rows}",
        "",
        f"Diversity index: {report.diversity_index:.3f}",
        f"Regime entropy: {report.regime_entropy:.3f}",
        f"Source disagreement: {report.source_disagreement_score:.3f}",
        "",
        f"Integrity score: {report.integrity_score:.1f}/100 ({report.integrity_grade})",
    ]

    if report.leakage_detected:
        lines.append("")
        lines.append("⚠️  LEAKAGE DETECTED:")
        for detail in report.leakage_details:
            lines.append(f"  - {detail}")

    if report.warnings:
        lines.append("")
        lines.append("Warnings:")
        for w in report.warnings:
            lines.append(f"  ⚠️ {w}")

    if report.recommendations:
        lines.append("")
        lines.append("Recommendations:")
        for r in report.recommendations:
            lines.append(f"  → {r}")

    lines.append(f"{'='*50}\n")
    return lines