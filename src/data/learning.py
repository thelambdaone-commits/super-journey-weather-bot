"""
Learning readiness diagnostics.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from statistics import mean

from .loader import load_rows
from .schema_v3 import load_rows_v3 as load_rows_v3_func


@dataclass
class LearningValidationReportV3:
    """Extended readiness summary for V3 temporal dataset."""

    dataset_path: str
    schema_version: str
    total_rows: int
    scan_count: int
    unique_markets: int
    unique_cities: int
    resolved_rows: int
    decision_rows: int
    avg_scans_per_market: float
    edge_mean: float
    edge_std: float
    win_rate: float
    temporal_coverage_score: int
    learning_readiness_score: int
    readiness_label: str
    notes: list[str]


def run_learning_validation_v3(data_dir: str = "data", filename: str = "dataset_rows_v3.jsonl") -> LearningValidationReportV3:
    """Compute readiness score for V3 temporal dataset."""
    import math
    
    path = Path(data_dir) / filename
    rows = load_rows_v3_func(path)
    
    if not rows:
        return LearningValidationReportV3(
            dataset_path=str(path),
            schema_version="3.0",
            total_rows=0,
            scan_count=0,
            unique_markets=0,
            unique_cities=0,
            resolved_rows=0,
            decision_rows=0,
            avg_scans_per_market=0,
            edge_mean=0,
            edge_std=0,
            win_rate=0,
            temporal_coverage_score=0,
            learning_readiness_score=0,
            readiness_label="no_data",
            notes=["V3 dataset is empty"],
        )
    
    total_rows = len(rows)
    unique_sequences = {r.scan_sequence_id for r in rows}
    unique_markets = len(unique_sequences)
    unique_cities = len({r.city for r in rows})
    resolved_rows = len([r for r in rows if r.actual_temp is not None])
    decision_rows = len([r for r in rows if r.decision_size is not None])
    avg_scans = total_rows / unique_markets if unique_markets else 0
    
    realized_edges = [r.realized_edge for r in rows if r.realized_edge is not None and r.actual_temp is not None]
    
    edge_mean = sum(realized_edges) / len(realized_edges) if realized_edges else 0
    edge_std = (
        math.sqrt(sum((e - edge_mean) ** 2 for e in realized_edges) / len(realized_edges))
        if len(realized_edges) > 1 else 0
    )
    positive_edges = len([e for e in realized_edges if e > 0])
    win_rate = positive_edges / len(realized_edges) if realized_edges else 0
    
    temporal_score = min(round((avg_scans / 10) * 40 + (unique_markets / 50) * 30), 100) if avg_scans and unique_markets else 0
    edge_score = min(round(max(0, edge_mean + 0.5) * 60 + max(0, 0.5 - edge_std) * 40), 100) if realized_edges else 0
    readiness_score = int(min(temporal_score + edge_score, 100))
    
    notes: list[str] = []
    if total_rows < 100:
        notes.append("low_volume")
    if avg_scans < 2:
        notes.append("low_temporal_depth")
    if unique_markets < 10:
        notes.append("low_market_diversity")
    if not resolved_rows:
        notes.append("no_resolved_rows")
    
    return LearningValidationReportV3(
        dataset_path=str(path),
        schema_version="3.0",
        total_rows=total_rows,
        scan_count=total_rows,
        unique_markets=unique_markets,
        unique_cities=unique_cities,
        resolved_rows=resolved_rows,
        decision_rows=decision_rows,
        avg_scans_per_market=round(avg_scans, 2),
        edge_mean=round(edge_mean, 4),
        edge_std=round(edge_std, 4),
        win_rate=round(win_rate, 4),
        temporal_coverage_score=temporal_score,
        learning_readiness_score=readiness_score,
        readiness_label=_label_from_score(readiness_score),
        notes=notes,
    )


def format_learning_validation_v3(report: LearningValidationReportV3) -> list[str]:
    """Render V3 learning validation report."""
    lines = [f"\n{'='*50}", "V3 LEARNING VALIDATION", f"{'='*50}"]
    lines.append(f"Dataset: {report.dataset_path}")
    lines.append(f"Schema: {report.schema_version}")
    lines.append(f"Total rows: {report.total_rows}")
    lines.append(f"Unique markets: {report.unique_markets}")
    lines.append(f"Unique cities: {report.unique_cities}")
    lines.append(f"Avg scans/market: {report.avg_scans_per_market}")
    lines.append(f"Resolved rows: {report.resolved_rows}")
    lines.append(f"Decision rows: {report.decision_rows}")
    lines.append("")
    lines.append(f"Edge mean: {report.edge_mean:.4f}")
    lines.append(f"Edge std: {report.edge_std:.4f}")
    lines.append(f"Win rate: {report.win_rate:.1%}")
    lines.append("")
    lines.append(f"Temporal score: {report.temporal_coverage_score}/100")
    lines.append(f"Overall readiness: {report.learning_readiness_score}/100")
    lines.append(f"Readiness label: {report.readiness_label}")
    if report.notes:
        lines.append("")
        lines.append("Notes:")
        for note in report.notes:
            lines.append(f" - {note}")
    lines.append(f"{'='*50}\n")
    return lines


@dataclass
class LearningValidationReport:
    """Readiness summary for statistical learning."""

    dataset_path: str
    total_rows: int
    decision_rows: int
    resolution_rows: int
    resolved_rows: int
    decision_cities: int
    resolution_cities: int
    resolution_sources: int
    decision_ratio: float
    resolution_ratio: float
    avg_decisions_per_city: float
    avg_resolutions_per_city: float
    learning_readiness_score: int
    readiness_label: str
    ready_for_scoring_fit: bool
    ready_for_top_k_backtest: bool
    notes: list[str]


def _bounded_ratio(value: float, target: float) -> float:
    if target <= 0:
        return 0.0
    return min(value / target, 1.0)


def _label_from_score(score: int) -> str:
    if score >= 80:
        return "ready"
    if score >= 45:
        return "monitor"
    return "not_ready"


def run_learning_validation(data_dir: str = "data", filename: str = "dataset_rows.jsonl") -> LearningValidationReport:
    """Compute a readiness score for learning / calibration / backtest."""
    path = Path(data_dir) / filename
    rows = load_rows(path)
    total_rows = len(rows)
    decision_rows = [row for row in rows if row.event_type == "decision"]
    resolution_rows = [row for row in rows if row.event_type == "resolution"]
    resolved_rows = [row for row in rows if row.actual_temp is not None or row.resolution_outcome is not None]

    decision_cities = {row.city for row in decision_rows}
    resolution_cities = {row.city for row in resolution_rows}
    resolution_sources = {row.forecast_source for row in resolution_rows if row.forecast_source}

    decision_ratio = len(decision_rows) / total_rows if total_rows else 0.0
    resolution_ratio = len(resolved_rows) / total_rows if total_rows else 0.0
    avg_decisions_per_city = mean([sum(1 for row in decision_rows if row.city == city) for city in decision_cities]) if decision_cities else 0.0
    avg_resolutions_per_city = mean([sum(1 for row in resolved_rows if row.city == city) for city in resolution_cities]) if resolution_cities else 0.0

    volume_score = round(_bounded_ratio(total_rows, 1000) * 30)
    decision_score = round(_bounded_ratio(len(decision_rows), 200) * 25)
    resolution_score = round(_bounded_ratio(len(resolved_rows), 200) * 25)
    coverage_score = round(_bounded_ratio(len(decision_cities), 10) * 10 + _bounded_ratio(len(resolution_sources), 3) * 10)
    readiness_score = int(min(volume_score + decision_score + resolution_score + coverage_score, 100))

    notes: list[str] = []
    if total_rows < 200:
        notes.append("low_total_volume")
    if len(decision_rows) < 50:
        notes.append("decision_volume_low")
    if len(resolved_rows) < 50:
        notes.append("resolution_volume_low")
    if len(decision_cities) < 5:
        notes.append("city_coverage_low")
    if len(resolution_sources) < 2:
        notes.append("source_coverage_low")

    return LearningValidationReport(
        dataset_path=str(path),
        total_rows=total_rows,
        decision_rows=len(decision_rows),
        resolution_rows=len(resolution_rows),
        resolved_rows=len(resolved_rows),
        decision_cities=len(decision_cities),
        resolution_cities=len(resolution_cities),
        resolution_sources=len(resolution_sources),
        decision_ratio=round(decision_ratio, 4),
        resolution_ratio=round(resolution_ratio, 4),
        avg_decisions_per_city=round(avg_decisions_per_city, 2),
        avg_resolutions_per_city=round(avg_resolutions_per_city, 2),
        learning_readiness_score=readiness_score,
        readiness_label=_label_from_score(readiness_score),
        ready_for_scoring_fit=bool(len(decision_rows) >= 100 and len(resolved_rows) >= 100 and len(decision_cities) >= 8),
        ready_for_top_k_backtest=bool(len(decision_rows) >= 25 and len(resolved_rows) >= 25 and len(decision_cities) >= 5),
        notes=notes,
    )


def format_learning_validation(report: LearningValidationReport) -> list[str]:
    """Render a concise CLI report."""
    lines = [f"\n{'='*50}", "LEARNING VALIDATION", f"{'='*50}"]
    lines.append(f"Dataset: {report.dataset_path}")
    lines.append(f"Total rows: {report.total_rows}")
    lines.append(f"Decision rows: {report.decision_rows}")
    lines.append(f"Resolution rows: {report.resolution_rows}")
    lines.append(f"Resolved rows: {report.resolved_rows}")
    lines.append(f"Decision cities: {report.decision_cities}")
    lines.append(f"Resolution cities: {report.resolution_cities}")
    lines.append(f"Resolution sources: {report.resolution_sources}")
    lines.append("")
    lines.append(f"Decision ratio: {report.decision_ratio:.1%}")
    lines.append(f"Resolution ratio: {report.resolution_ratio:.1%}")
    lines.append(f"Avg decisions / city: {report.avg_decisions_per_city:.2f}")
    lines.append(f"Avg resolutions / city: {report.avg_resolutions_per_city:.2f}")
    lines.append(f"Readiness score: {report.learning_readiness_score}/100")
    lines.append(f"Readiness label: {report.readiness_label}")
    lines.append(f"Ready for scoring fit: {'yes' if report.ready_for_scoring_fit else 'no'}")
    lines.append(f"Ready for top-K backtest: {'yes' if report.ready_for_top_k_backtest else 'no'}")
    if report.notes:
        lines.append("")
        lines.append("Notes:")
        for note in report.notes:
            lines.append(f" - {note}")
    lines.append(f"{'='*50}\n")
    return lines
