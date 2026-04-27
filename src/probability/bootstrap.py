"""
Bootstrap calibration fit from resolved historical dataset rows.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from ..data.loader import load_rows
from ..ml import train_model
from ..weather.locations import LOCATIONS
from ..weather.math import bucket_prob, in_bucket
from .calibration import CalibrationEngine, CalibrationValidationReport, CalibrationValidator
from .model import ForecastModel


@dataclass
class BootstrapCalibrationReport:
    """Calibration bootstrap summary."""

    samples_used: int
    sources: dict[str, int]
    before_brier: float
    before_mean_error: float
    after_brier: float
    after_mean_error: float
    fitted: bool
    output_path: str
    accepted: bool
    selected_method: str
    validation_reason: str
    holdout_before_brier: float
    holdout_after_brier: float
    holdout_train_samples: int
    holdout_test_samples: int
    holdout_has_perfect_predictions: bool
    holdout_variance_preserved: bool


def _parse_bucket(bucket: str) -> tuple[float, float] | None:
    if not bucket:
        return None
    value = bucket.strip()
    if value.endswith("C") or value.endswith("F"):
        value = value[:-1]
    parts = value.split("-")
    if len(parts) != 2:
        return None
    try:
        return float(parts[0]), float(parts[1])
    except (Exception,) as e:
        return None


def bootstrap_calibration_fit(data_dir: str = "data", dataset_file: str = "dataset_rows.jsonl") -> BootstrapCalibrationReport:
    """Fit calibration from current resolved historical dataset."""
    dataset_path = Path(data_dir) / dataset_file
    rows = load_rows(dataset_path)
    model = train_model(data_dir)
    scorer = ForecastModel(model, data_dir)
    validator = CalibrationValidator()

    y_prob: list[float] = []
    y_true: list[int] = []
    sources: Counter[str] = Counter()

    for row in rows:
        if row.actual_temp is None or not row.bucket or row.forecast_source is None:
            continue
        forecast_temp = row.raw_forecast_temp if row.raw_forecast_temp is not None else row.forecast_temp
        if forecast_temp is None:
            continue
        location = LOCATIONS.get(row.city)
        if location is None:
            continue
        bucket_range = _parse_bucket(row.bucket)
        if bucket_range is None:
            continue

        score = scorer.score(row.city, row.forecast_source, float(forecast_temp), location.unit)
        low, high = bucket_range
        raw_probability = bucket_prob(score["adjusted_temp"], low, high, score["sigma"])
        truth = 1 if in_bucket(float(row.actual_temp), low, high) else 0
        y_prob.append(float(raw_probability))
        y_true.append(truth)
        sources[row.forecast_source] += 1

    before_metrics = CalibrationEngine(method="isotonic").evaluate(y_prob, y_true)
    candidate_reports: list[tuple[CalibrationValidationReport, CalibrationEngine]] = []
    for method in ("isotonic", "platt"):
        calibrator = CalibrationEngine(method=method)
        report = validator.validate(calibrator, y_prob, y_true)
        if report.accepted:
            calibrator.fit(y_prob, y_true)
            if calibrator.fitted:
                candidate_reports.append((report, calibrator))

    chosen_report: CalibrationValidationReport | None = None
    chosen_calibrator: CalibrationEngine | None = None
    if candidate_reports:
        chosen_report, chosen_calibrator = min(candidate_reports, key=lambda item: item[0].after_brier)

    output_path = Path(data_dir) / "calibration.pkl"
    if chosen_calibrator is not None and chosen_calibrator.fitted:
        chosen_calibrator.save(str(output_path))
        after_metrics = chosen_calibrator.evaluate(y_prob, y_true)
    else:
        after_metrics = before_metrics
        fallback_method = "platt" if len(y_prob) >= 18 else "isotonic"
        chosen_report = validator.validate(CalibrationEngine(method=fallback_method), y_prob, y_true)
        CalibrationEngine(method=fallback_method).save(str(output_path))

    return BootstrapCalibrationReport(
        samples_used=len(y_true),
        sources=dict(sources),
        before_brier=before_metrics["brier_score"],
        before_mean_error=before_metrics["mean_error"],
        after_brier=after_metrics["brier_score"],
        after_mean_error=after_metrics["mean_error"],
        fitted=bool(chosen_calibrator and chosen_calibrator.fitted),
        output_path=str(output_path),
        accepted=bool(chosen_report and chosen_report.accepted),
        selected_method=chosen_report.method if chosen_report else "none",
        validation_reason=chosen_report.reason if chosen_report else "no_validation",
        holdout_before_brier=chosen_report.before_brier if chosen_report else 0.0,
        holdout_after_brier=chosen_report.after_brier if chosen_report else 0.0,
        holdout_train_samples=chosen_report.train_samples if chosen_report else 0,
        holdout_test_samples=chosen_report.test_samples if chosen_report else 0,
        holdout_has_perfect_predictions=bool(chosen_report and chosen_report.has_perfect_predictions),
        holdout_variance_preserved=bool(chosen_report and chosen_report.variance_preserved),
    )


def format_bootstrap_report(report: BootstrapCalibrationReport) -> list[str]:
    """Render a compact CLI bootstrap report."""
    lines = [f"\n{'='*50}", "CALIBRATION BOOTSTRAP", f"{'='*50}"]
    lines.append(f"Samples used: {report.samples_used}")
    lines.append(f"Fitted: {'yes' if report.fitted else 'no'}")
    lines.append(f"Accepted: {'yes' if report.accepted else 'no'}")
    lines.append(f"Method: {report.selected_method}")
    lines.append(f"Validation: {report.validation_reason}")
    lines.append(f"Output: {report.output_path}")
    lines.append("")
    lines.append("Sources:")
    if report.sources:
        for source, count in sorted(report.sources.items()):
            lines.append(f" - {source}: {count}")
    else:
        lines.append(" - no usable labeled rows")
    lines.append("")
    lines.append(f"Brier before: {report.before_brier:.6f}")
    lines.append(f"Mean error before: {report.before_mean_error:.6f}")
    lines.append(f"Brier after: {report.after_brier:.6f}")
    lines.append(f"Mean error after: {report.after_mean_error:.6f}")
    lines.append("")
    lines.append("Holdout:")
    lines.append(f" - train samples: {report.holdout_train_samples}")
    lines.append(f" - test samples: {report.holdout_test_samples}")
    lines.append(f" - brier before: {report.holdout_before_brier:.6f}")
    lines.append(f" - brier after: {report.holdout_after_brier:.6f}")
    lines.append(f" - perfect predictions: {'yes' if report.holdout_has_perfect_predictions else 'no'}")
    lines.append(f" - variance preserved: {'yes' if report.holdout_variance_preserved else 'no'}")
    if report.before_brier:
        improvement = (report.before_brier - report.after_brier) / report.before_brier
        lines.append(f"Relative Brier improvement: {improvement:.2%}")
    lines.append(f"{'='*50}\n")
    return lines
