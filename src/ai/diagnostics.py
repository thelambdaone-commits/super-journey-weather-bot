"""
AI and self-improvement diagnostics.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from . import analyze_forecast, check_anomaly, get_groq_client
from ..data.loader import load_rows
from ..data.learning import run_learning_validation
from ..ml import load_model
from ..probability.calibration import CalibrationEngine
from ..weather.locations import LOCATIONS


@dataclass
class AIDiagnosticsReport:
    """Compact diagnostics for Groq and the self-improvement loop."""

    groq_available: bool
    groq_probe_ok: bool
    groq_probe_reason: str
    dataset_rows: int
    resolved_rows: int
    decision_rows: int
    model_loaded: bool
    model_samples: int
    model_cities: int
    calibration_loaded: bool
    calibration_fitted: bool
    feedback_loop_active: bool
    autoimprovement_ready: bool
    probe_analysis: str | None
    probe_recommendation: str | None
    probe_anomaly: bool
    readiness_score: int
    readiness_label: str
    readiness_notes: list[str]


def _probe_groq() -> tuple[bool, str, dict | None]:
    """Try a small Groq probe using a known city context."""
    if get_groq_client() is None:
        return False, "groq_unavailable", None

    sample_city = "paris" if "paris" in LOCATIONS else next(iter(LOCATIONS))
    result = analyze_forecast(sample_city, 18.0, 19.0, 18.5, None)
    if result.get("error"):
        return False, str(result["error"]), result

    anomaly = check_anomaly(sample_city, 18.0, 0.55, 0.12, 0.78)
    result["anomaly"] = anomaly
    return True, "ok", result


def run_ai_diagnostics(data_dir: str = "data") -> AIDiagnosticsReport:
    """Run a compact status check for Groq and the learning loop."""
    dataset_path = Path(data_dir) / "dataset_rows.jsonl"
    rows = load_rows(dataset_path)
    model = load_model(data_dir)
    calibrator = CalibrationEngine(method="isotonic")
    calibration_loaded = calibrator.load(str(Path(data_dir) / "calibration.pkl"))

    groq_ok, groq_reason, groq_payload = _probe_groq()
    resolved_rows = sum(1 for row in rows if row.actual_temp is not None)
    decision_rows = sum(1 for row in rows if row.event_type == "decision")
    model_loaded = model is not None
    model_samples = int(model.get("samples", 0)) if model else 0
    model_cities = int(model.get("cities", 0)) if model else 0
    calibration_fitted = bool(calibrator.fitted)
    feedback_loop_active = bool(rows and model_loaded and resolved_rows > 0 and decision_rows > 0)
    learning = run_learning_validation(data_dir)
    autoimprovement_ready = bool(
        feedback_loop_active
        and calibration_loaded
        and calibration_fitted
        and learning.ready_for_scoring_fit
    )

    return AIDiagnosticsReport(
        groq_available=get_groq_client() is not None,
        groq_probe_ok=groq_ok,
        groq_probe_reason=groq_reason,
        dataset_rows=len(rows),
        resolved_rows=resolved_rows,
        decision_rows=decision_rows,
        model_loaded=model_loaded,
        model_samples=model_samples,
        model_cities=model_cities,
        calibration_loaded=calibration_loaded,
        calibration_fitted=calibration_fitted,
        feedback_loop_active=feedback_loop_active,
        autoimprovement_ready=autoimprovement_ready,
        probe_analysis=None if not groq_payload else groq_payload.get("analysis"),
        probe_recommendation=None if not groq_payload else groq_payload.get("recommendation"),
        probe_anomaly=bool(groq_payload and groq_payload.get("anomaly", {}).get("is_anomaly")),
        readiness_score=learning.learning_readiness_score,
        readiness_label=learning.readiness_label,
        readiness_notes=learning.notes,
    )


def format_ai_diagnostics(report: AIDiagnosticsReport) -> list[str]:
    """Render AI diagnostics for CLI output."""
    lines = [f"\n{'='*50}", "AI STATUS", f"{'='*50}"]
    lines.append(f"Groq available: {'yes' if report.groq_available else 'no'}")
    lines.append(f"Groq probe: {'ok' if report.groq_probe_ok else 'failed'} ({report.groq_probe_reason})")
    lines.append(f"Dataset rows: {report.dataset_rows}")
    lines.append(f"Resolved rows: {report.resolved_rows}")
    lines.append(f"Decision rows: {report.decision_rows}")
    lines.append(f"Model loaded: {'yes' if report.model_loaded else 'no'}")
    lines.append(f"Model samples: {report.model_samples}")
    lines.append(f"Model cities: {report.model_cities}")
    lines.append(f"Calibration loaded: {'yes' if report.calibration_loaded else 'no'}")
    lines.append(f"Calibration fitted: {'yes' if report.calibration_fitted else 'no'}")
    lines.append(f"Feedback loop active: {'yes' if report.feedback_loop_active else 'no'}")
    lines.append(f"Autoimprovement ready: {'yes' if report.autoimprovement_ready else 'no'}")
    lines.append(f"Learning readiness: {report.readiness_score}/100 ({report.readiness_label})")
    if report.readiness_notes:
        lines.append("Readiness notes: " + ", ".join(report.readiness_notes))
    if report.probe_analysis:
        lines.append(f"Groq analysis: {report.probe_analysis}")
    if report.probe_recommendation:
        lines.append(f"Groq recommendation: {report.probe_recommendation}")
    lines.append(f"Groq anomaly flag: {'yes' if report.probe_anomaly else 'no'}")
    lines.append(f"{'='*50}\n")
    return lines
