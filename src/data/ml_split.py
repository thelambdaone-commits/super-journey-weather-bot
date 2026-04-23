"""
ML Split Safe: Strict train/valid/test split for ML training.
Ensures no leakage from synthetic data into validation/test sets.
"""
from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


@dataclass
class SplitConfig:
    """Configuration for ML split."""

    train_ratio: float = 0.7
    valid_ratio: float = 0.15
    test_ratio: float = 0.15

    stratify_by: list[str] = None

    def __post_init__(self):
        if self.stratify_by is None:
            self.stratify_by = ["city", "regime_type"]

        total = self.train_ratio + self.valid_ratio + self.test_ratio
        if abs(total - 1.0) > 0.01:
            raise ValueError(f"Split ratios must sum to 1.0, got {total}")


@dataclass
class SplitReport:
    """Report from ML split operation."""

    total_rows: int
    train_rows: int
    valid_rows: int
    test_rows: int

    train_real: int
    train_replay: int
    valid_real: int
    valid_replay: int
    test_real: int
    test_replay: int

    train_regimes: dict[str, int]
    valid_regimes: dict[str, int]
    test_regimes: dict[str, int]

    train_cities: int
    valid_cities: int
    test_cities: int

    success: bool


class MLSplitSafe:
    """Create ML-safe train/valid/test splits."""

    def __init__(
        self,
        data_dir: str = "data",
        config: Optional[SplitConfig] = None,
        seed: int = 42,
    ):
        self.data_dir = Path(data_dir)
        self.config = config or SplitConfig()
        self.seed = seed
        random.seed(seed)

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
        if event_type == "replay_scan":
            return "replay"

        return "real"

    def _stratify_groups(self, rows: list[dict]) -> dict[tuple, list[dict]]:
        """Group rows by stratification keys."""
        groups: dict[tuple, list[dict]] = {}

        for row in rows:
            key = tuple(row.get(k) for k in self.config.stratify_by)
            if key not in groups:
                groups[key] = []
            groups[key].append(row)

        return groups

    def _split_group(
        self,
        rows: list[dict],
        train_ratio: float,
        valid_ratio: float,
    ) -> tuple[list[dict], list[dict], list[dict]]:
        """Split a group of rows."""
        if len(rows) == 1:
            return [rows[0]], [], []
        if len(rows) == 2:
            return [rows[0]], [], [rows[1]]

        random.shuffle(rows)
        n = len(rows)

        n_train = max(1, int(n * train_ratio))
        n_valid = max(1, int(n * valid_ratio))

        train = rows[:n_train]
        valid = rows[n_train:n_train + n_valid]
        test = rows[n_train + n_valid:]

        return train, valid, test

    def split(
        self,
        input_path: Path,
        output_dir: Optional[Path] = None,
    ) -> SplitReport:
        """Create ML-safe train/valid/test split."""

        rows = []
        for line in input_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))

        if not rows:
            return SplitReport(
                total_rows=0, train_rows=0, valid_rows=0, test_rows=0,
                train_real=0, train_replay=0, valid_real=0, valid_replay=0,
                test_real=0, test_replay=0,
                train_regimes={}, valid_regimes={}, test_regimes={},
                train_cities=0, valid_cities=0, test_cities=0,
                success=False
            )

        groups = self._stratify_groups(rows)

        train_rows: list[dict] = []
        valid_rows: list[dict] = []
        test_rows: list[dict] = []

        for group_key, group_rows in groups.items():
            tr, vr, te = self._split_group(
                group_rows,
                self.config.train_ratio,
                self.config.valid_ratio,
            )
            train_rows.extend(tr)
            valid_rows.extend(vr)
            test_rows.extend(te)

        output_dir = output_dir or input_path.parent / "ml_splits"
        output_dir.mkdir(exist_ok=True)

        train_path = output_dir / "train.jsonl"
        valid_path = output_dir / "valid.jsonl"
        test_path = output_dir / "test.jsonl"

        for path, row_list in [(train_path, train_rows), (valid_path, valid_rows), (test_path, test_rows)]:
            with path.open("w", encoding="utf-8") as f:
                for row in row_list:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")

        def count_by_tag(row_list: list[dict], tag: str) -> int:
            return sum(1 for r in row_list if self._detect_source_tag(r) == tag)

        def count_regimes(row_list: list[dict]) -> dict[str, int]:
            regimes: dict[str, int] = {}
            for r in row_list:
                regime = r.get("regime_type", "unknown")
                regimes[regime] = regimes.get(regime, 0) + 1
            return regimes

        def count_cities(row_list: list[dict]) -> int:
            return len({r.get("city") for r in row_list if r.get("city")})

        return SplitReport(
            total_rows=len(rows),
            train_rows=len(train_rows),
            valid_rows=len(valid_rows),
            test_rows=len(test_rows),
            train_real=count_by_tag(train_rows, "real"),
            train_replay=count_by_tag(train_rows, "replay"),
            valid_real=count_by_tag(valid_rows, "real"),
            valid_replay=count_by_tag(valid_rows, "replay"),
            test_real=count_by_tag(test_rows, "real"),
            test_replay=count_by_tag(test_rows, "replay"),
            train_regimes=count_regimes(train_rows),
            valid_regimes=count_regimes(valid_rows),
            test_regimes=count_regimes(test_rows),
            train_cities=count_cities(train_rows),
            valid_cities=count_cities(valid_rows),
            test_cities=count_cities(test_rows),
            success=True,
        )


def split_for_ml(
    input_path: str = "data/dataset_rows_rebalanced.jsonl",
    output_dir: Optional[str] = None,
    train_ratio: float = 0.7,
    valid_ratio: float = 0.15,
    test_ratio: float = 0.15,
) -> SplitReport:
    """Convenience function to split dataset for ML."""
    splitter = MLSplitSafe(
        data_dir=str(Path(input_path).parent),
        config=SplitConfig(
            train_ratio=train_ratio,
            valid_ratio=valid_ratio,
            test_ratio=test_ratio,
        ),
    )
    out = Path(output_dir) if output_dir else None
    return splitter.split(Path(input_path), out)


def format_split_report(report: SplitReport) -> list[str]:
    """Format split report for CLI."""
    lines = [
        f"\n{'='*50}",
        "ML SPLIT REPORT",
        f"{'='*50}",
        f"Total rows: {report.total_rows}",
        "",
        f"Train: {report.train_rows} ({report.train_rows/report.total_rows:.1%})",
        f"  real: {report.train_real}, replay: {report.train_replay}",
        f"  cities: {report.train_cities}, regimes: {len(report.train_regimes)}",
        "",
        f"Valid: {report.valid_rows} ({report.valid_rows/report.total_rows:.1%})",
        f"  real: {report.valid_real}, replay: {report.valid_replay}",
        f"  cities: {report.valid_cities}, regimes: {len(report.valid_regimes)}",
        "",
        f"Test: {report.test_rows} ({report.test_rows/report.total_rows:.1%})",
        f"  real: {report.test_real}, replay: {report.test_replay}",
        f"  cities: {report.test_cities}, regimes: {len(report.test_regimes)}",
        "",
        "Regime distributions:",
    ]

    all_regimes = set(report.train_regimes) | set(report.valid_regimes) | set(report.test_regimes)
    for regime in sorted(all_regimes):
        lines.append(f"  {regime}: train={report.train_regimes.get(regime, 0)}, "
                    f"valid={report.valid_regimes.get(regime, 0)}, "
                    f"test={report.test_regimes.get(regime, 0)}")

    status = "✅ SUCCESS" if report.success else "❌ FAILED"
    lines.append("")
    lines.append(f"Status: {status}")
    lines.append(f"{'='*50}\n")

    return lines