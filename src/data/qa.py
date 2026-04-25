"""
Dataset QA and stability diagnostics.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import fields
from pathlib import Path
from statistics import mean
from typing import Iterable

from .loader import load_rows
from .schema import DatasetRow


NUMERIC_BOUNDS = {
    "ecmwf_max": (-80.0, 160.0),
    "hrrr_max": (-80.0, 160.0),
    "gfs_max": (-80.0, 160.0),
    "ensemble_mean": (-80.0, 160.0),
    "ensemble_std": (0.0, 80.0),
    "forecast_spread": (0.0, 120.0),
    "forecast_temp": (-80.0, 160.0),
    "raw_forecast_temp": (-80.0, 160.0),
    "market_price": (0.0, 1.0),
    "market_implied_prob": (0.0, 1.0),
    "liquidity": (0.0, 1_000_000_000.0),
    "spread": (0.0, 1.0),
    "top_market_price": (0.0, 1.0),
    "orderbook_depth": (0.0, 1_000_000_000.0),
    "raw_prob": (0.0, 1.0),
    "calibrated_prob": (0.0, 1.0),
    "confidence": (0.0, 1.0),
    "adjusted_ev": (-10.0, 10.0),
    "raw_ev": (-10.0, 10.0),
    "kelly": (0.0, 1.0),
    "decision_size": (0.0, 1_000_000.0),
    "lat": (-90.0, 90.0),
    "lon": (-180.0, 180.0),
    "day_of_year": (1, 366),
    "hours_to_resolution": (0.0, 365 * 24.0),
    "actual_temp": (-80.0, 160.0),
}


def _safe_mean(values: Iterable[float]) -> float | None:
    values = list(values)
    return round(mean(values), 4) if values else None


class DataQARunner:
    """Run QA checks on the append-only dataset."""

    def __init__(self, data_dir: str = "data", filename: str = "dataset_rows.jsonl"):
        self.path = Path(data_dir) / filename

    def run(self) -> dict:
        """Build a dataset QA report."""
        rows = load_rows(self.path)
        total = len(rows)
        report = {
            "path": str(self.path),
            "samples_count": total,
            "null_rates": self._null_rates(rows, total),
            "outlier_counts": self._outlier_counts(rows),
            "source_bias": self._source_bias(rows),
            "city_bias": self._city_bias(rows),
            "stability": self._stability_summary(rows),
            "validation": self._validation_summary(rows),
        }
        return report

    def _null_rates(self, rows: list[DatasetRow], total: int) -> dict:
        """Return null rates per schema field."""
        rates = {}
        for field in fields(DatasetRow):
            null_count = sum(1 for row in rows if getattr(row, field.name) is None)
            rates[field.name] = round((null_count / total), 4) if total else 0.0
        return rates

    def _outlier_counts(self, rows: list[DatasetRow]) -> dict:
        """Return simple bound-based outlier counts."""
        counts = {}
        for name, (low, high) in NUMERIC_BOUNDS.items():
            count = 0
            for row in rows:
                value = getattr(row, name)
                if value is None:
                    continue
                if value < low or value > high:
                    count += 1
            counts[name] = count
        return counts

    def _source_bias(self, rows: list[DatasetRow]) -> dict:
        """Mean signed error per forecast source against actual temp."""
        errors = defaultdict(list)
        for row in rows:
            if row.actual_temp is None:
                continue
            for source in ("ecmwf_max", "hrrr_max", "gfs_max", "ensemble_mean"):
                value = getattr(row, source)
                if value is None:
                    continue
                errors[source].append(float(value) - float(row.actual_temp))
        return {
            source: {
                "samples": len(values),
                "mean_error": _safe_mean(values),
                "mean_abs_error": _safe_mean(abs(value) for value in values),
            }
            for source, values in errors.items()
        }

    def _city_bias(self, rows: list[DatasetRow]) -> dict:
        """Mean signed error by city using ensemble mean when available."""
        errors = defaultdict(list)
        for row in rows:
            if row.actual_temp is None or row.ensemble_mean is None:
                continue
            errors[row.city].append(float(row.ensemble_mean) - float(row.actual_temp))
        return {
            city: {
                "samples": len(values),
                "mean_error": _safe_mean(values),
                "mean_abs_error": _safe_mean(abs(value) for value in values),
            }
            for city, values in sorted(errors.items())
        }

    def _stability_summary(self, rows: list[DatasetRow]) -> dict:
        """Return high-level stability signals."""
        total = len(rows)
        resolved = [row for row in rows if row.actual_temp is not None]
        decisions = [row for row in rows if row.event_type == "decision"]
        missing_actual = total - len(resolved)
        high_spread = sum(1 for row in rows if row.forecast_spread is not None and row.forecast_spread >= 8.0)
        low_liquidity = sum(1 for row in rows if row.liquidity is not None and row.liquidity < 500.0)
        return {
            "resolved_samples": len(resolved),
            "decision_samples": len(decisions),
            "missing_actual_temp": missing_actual,
            "high_forecast_spread_samples": high_spread,
            "low_liquidity_samples": low_liquidity,
        }

    def _validation_summary(self, rows: list[DatasetRow]) -> dict:
        """Return hard gates for live/statistical readiness."""
        decisions = [row for row in rows if row.event_type == "decision"]
        resolved = [row for row in rows if row.actual_temp is not None or row.resolution_outcome is not None]
        decision_cities = {row.city for row in decisions}
        resolution_cities = {row.city for row in resolved}
        issues = []
        if len(decisions) < 200:
            issues.append("need_at_least_200_decisions")
        if len(resolved) < 200:
            issues.append("need_at_least_200_resolved_rows")
        if len(decision_cities) < 10:
            issues.append("need_at_least_10_decision_cities")
        if len(resolution_cities) < 10:
            issues.append("need_at_least_10_resolution_cities")
        return {
            "ready_for_live": not issues,
            "issues": issues,
        }


def format_qa_report(report: dict) -> list[str]:
    """Render a compact CLI QA report."""
    lines = [f"\n{'='*50}", "DATA QA", f"{'='*50}"]
    lines.append(f"Path: {report['path']}")
    lines.append(f"Samples: {report['samples_count']}")

    null_rates = report["null_rates"]
    top_nulls = sorted(null_rates.items(), key=lambda item: item[1], reverse=True)[:8]
    lines.append("")
    lines.append("Top null rates:")
    for name, rate in top_nulls:
        lines.append(f" - {name}: {rate:.1%}")

    outliers = {name: count for name, count in report["outlier_counts"].items() if count > 0}
    lines.append("")
    lines.append("Outliers:")
    if outliers:
        for name, count in sorted(outliers.items(), key=lambda item: item[1], reverse=True):
            lines.append(f" - {name}: {count}")
    else:
        lines.append(" - none detected")

    lines.append("")
    lines.append("Source bias:")
    if report["source_bias"]:
        for name, payload in report["source_bias"].items():
            lines.append(
                f" - {name}: n={payload['samples']} mean_err={payload['mean_error']} "
                f"mae={payload['mean_abs_error']}"
            )
    else:
        lines.append(" - insufficient resolved samples")

    lines.append("")
    lines.append("City bias:")
    if report["city_bias"]:
        by_abs = sorted(
            report["city_bias"].items(),
            key=lambda item: abs(item[1]["mean_error"] or 0.0),
            reverse=True,
        )[:8]
        for city, payload in by_abs:
            lines.append(
                f" - {city}: n={payload['samples']} mean_err={payload['mean_error']} "
                f"mae={payload['mean_abs_error']}"
            )
    else:
        lines.append(" - insufficient resolved samples")

    stability = report["stability"]
    lines.append("")
    lines.append("Stability:")
    for key, value in stability.items():
        lines.append(f" - {key}: {value}")
    validation = report.get("validation", {})
    lines.append("")
    lines.append("Validation gate:")
    lines.append(f" - ready_for_live: {'yes' if validation.get('ready_for_live') else 'no'}")
    for issue in validation.get("issues", []):
        lines.append(f" - {issue}")
    lines.append(f"{'='*50}\n")
    return lines
