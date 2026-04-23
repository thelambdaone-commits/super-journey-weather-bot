"""
Append-only snapshot log for scan-time opportunity competition.
"""
from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path

from .loader import load_rows
from ..strategy.scoring import ScoringEngine


@dataclass
class SnapshotEntry:
    """One scored candidate within a scan snapshot."""

    snapshot_id: str
    snapshot_ts: int
    city: str
    date: str
    market_id: str | None
    question: str | None
    forecast_horizon: str | None
    bucket: str | None
    action: str
    score: float
    rank: int
    selected: bool
    calibrated_prob: float | None
    market_prob: float | None
    edge: float | None
    confidence: float | None
    liquidity: float | None
    regime_label: str
    signal_type: str
    priority: str
    decision_reason: str | None
    resolved_outcome: str | None = None
    actual_temp: float | None = None

    def to_dict(self) -> dict:
        return asdict(self)


class SnapshotStorage:
    """Append-only JSONL storage for snapshot entries."""

    def __init__(self, data_dir: str = "data", filename: str = "snapshot_rows.jsonl"):
        self.path = Path(data_dir) / filename
        self.path.parent.mkdir(exist_ok=True)

    def append(self, entry: SnapshotEntry) -> None:
        """Append one snapshot entry."""
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry.to_dict(), ensure_ascii=False) + "\n")

    def load_all(self) -> list[SnapshotEntry]:
        """Load all snapshot entries."""
        entries: list[SnapshotEntry] = []
        if not self.path.exists():
            return entries
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            entries.append(SnapshotEntry(**payload))
        return entries


def _regime_label(row) -> str:
    spread = float(row.forecast_spread or 0.0)
    if spread >= 4.0:
        return "EXTREME"
    if spread >= 2.0:
        return "VOLATILE"
    return "STABLE"


def _signal_type(row) -> str:
    uncertainty = float(row.forecast_spread or 0.0)
    if uncertainty >= 6.0:
        return "high-uncertainty"
    if row.liquidity is not None and row.liquidity >= 5000:
        return "clean-orderbook"
    return "edge-opportunity"


def build_snapshot_log(
    data_dir: str = "data",
    dataset_file: str = "dataset_rows.jsonl",
    snapshot_file: str = "snapshot_rows.jsonl",
    top_k: int = 3,
) -> dict:
    """Rebuild snapshot competition rows from decision rows."""
    rows = load_rows(Path(data_dir) / dataset_file)
    decisions = [
        row
        for row in rows
        if row.event_type == "decision" and row.action == "BUY" and row.market_id is not None
    ]
    storage = SnapshotStorage(data_dir=data_dir, filename=snapshot_file)
    storage.path.write_text("", encoding="utf-8")

    grouped: dict[str, list] = defaultdict(list)
    for row in decisions:
        grouped[row.date].append(row)

    scorer = ScoringEngine()
    rebuilt = 0
    snapshots = 0
    for date, candidates in grouped.items():
        scored = []
        for row in candidates:
            score = scorer.score_row(row)
            scored.append((score, row))
        if not scored:
            continue
        scored.sort(key=lambda item: (item[0], item[1].adjusted_ev or 0.0, item[1].city), reverse=True)
        snapshots += 1
        for rank, (score, row) in enumerate(scored, start=1):
            entry = SnapshotEntry(
                snapshot_id=date,
                snapshot_ts=int(row.timestamp),
                city=row.city,
                date=row.date,
                market_id=row.market_id,
                question=row.question,
                forecast_horizon=row.forecast_horizon,
                bucket=row.bucket,
                action=row.action,
                score=round(score, 4),
                rank=rank,
                selected=rank <= top_k,
                calibrated_prob=row.calibrated_prob,
                market_prob=row.market_price,
                edge=row.adjusted_ev,
                confidence=row.confidence,
                liquidity=row.liquidity,
                regime_label=_regime_label(row),
                signal_type=_signal_type(row),
                priority="HIGH" if (row.adjusted_ev or 0.0) > 0.1 else "NORMAL",
                decision_reason=row.decision_reason,
                resolved_outcome=row.resolution_outcome,
                actual_temp=row.actual_temp,
            )
            storage.append(entry)
            rebuilt += 1

    return {
        "rebuilt_rows": rebuilt,
        "snapshots": snapshots,
        "output": str(storage.path),
        "top_k": top_k,
    }
