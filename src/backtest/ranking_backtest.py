"""
Top-K ranking backtest and diagnostics.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from hashlib import sha1
import math
from pathlib import Path
from random import Random
from statistics import mean

from ..data.loader import load_rows
from ..data.schema import DatasetRow
from ..strategy.scoring import ScoringEngine


@dataclass
class RankingBacktestReport:
    """Summary of ranking backtest results."""

    dataset_path: str
    rows: int
    resolved_rows: int
    decision_rows: int
    eligible_snapshots: int
    top_k: int
    top_k_avg_pnl: float
    random_avg_pnl: float
    all_avg_pnl: float
    top_k_avg_hit_rate: float
    random_avg_hit_rate: float
    all_avg_hit_rate: float
    score_pnl_correlation: float
    rank_hit_rates: list[tuple[int, float]]
    rank_avg_pnl: list[tuple[int, float]]
    naive_avg_pnl: float
    benchmark_outperformance: float
    pnl_std: float = 0.0
    pnl_95_ci: tuple[float, float] = (0.0, 0.0)
    city_breakdown: dict[str, float] = None
    horizon_breakdown: dict[str, float] = None
    source_breakdown: dict[str, float] = None
    conf_breakdown: dict[str, float] = None
    windows: list[dict] = None


def _bucket_to_tuple(bucket: str | None) -> tuple[float, float] | None:
    if not bucket:
        return None
    raw = bucket.strip()
    if raw.endswith("C") or raw.endswith("F"):
        raw = raw[:-1]
    parts = raw.split("-")
    if len(parts) != 2:
        return None
    try:
        return float(parts[0]), float(parts[1])
    except (Exception,) as e:
        return None


def _infer_outcome(row: DatasetRow) -> tuple[bool | None, float | None]:
    if row.resolution_outcome in {"win", "loss"}:
        won = row.resolution_outcome == "win"
    elif row.actual_temp is not None and row.bucket:
        bucket = _bucket_to_tuple(row.bucket)
        if bucket is None:
            return None, None
        low, high = bucket
        won = low <= float(row.actual_temp) <= high
    else:
        return None, None

    size = float(row.decision_size or row.kelly or 1.0)
    price = float(row.market_price or row.market_implied_prob or 0.5)
    if price <= 0:
        price = 0.5
        
    # Friction Modeling
    slippage = 0.015 # 1.5% fixed slippage
    commission = 0.005 # 0.5% platform fee
    
    entry_price = price + slippage
    exit_price = 1.0 - commission if won else 0.0
    
    pnl = size * (exit_price - entry_price) / entry_price
    return won, round(pnl, 4)


def _correlation(xs: list[float], ys: list[float]) -> float:
    if len(xs) != len(ys) or len(xs) < 2:
        return 0.0
    mean_x = mean(xs)
    mean_y = mean(ys)
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    var_x = sum((x - mean_x) ** 2 for x in xs)
    var_y = sum((y - mean_y) ** 2 for y in ys)
    if var_x <= 1e-12 or var_y <= 1e-12:
        return 0.0
    return round(cov / (var_x ** 0.5 * var_y ** 0.5), 4)


class RankingBacktester:
    """Evaluate whether the ranking logic selects better trades than baselines."""

    def __init__(self, dataset_path: str = "data/dataset_rows.jsonl"):
        self.dataset_path = Path(dataset_path)
        self.scorer = ScoringEngine()

    def load_data(self) -> list[DatasetRow]:
        """Load schema-validated dataset rows."""
        return load_rows(self.dataset_path)

    def _group_candidates(self, rows: list[DatasetRow]) -> dict[str, list[DatasetRow]]:
        """Group decision rows into comparable snapshots."""
        groups: dict[str, list[DatasetRow]] = defaultdict(list)
        for row in rows:
            if row.event_type != "decision":
                continue
            if row.market_id is None:
                continue
            if row.action != "BUY":
                continue
            key = row.date
            groups[key].append(row)
        return groups

    def _pair_resolutions(self, rows: list[DatasetRow]) -> dict[str, DatasetRow]:
        """Map decision rows to their eventual resolution row."""
        resolved: dict[str, DatasetRow] = {}
        for row in rows:
            if row.event_type != "resolution":
                continue
            if row.market_id is None:
                continue
            resolved[row.market_id] = row
        return resolved

    def _seed_for_group(self, key: str) -> int:
        """Stable deterministic seed for baseline sampling."""
        return int(sha1(key.encode("utf-8")).hexdigest()[:8], 16)

    def run(self, top_k: int = 3) -> RankingBacktestReport:
        """Run ranking backtest and return diagnostics."""
        rows = self.load_data()
        resolved = self._pair_resolutions(rows)
        groups = self._group_candidates(rows)

        top_k_pnls: list[float] = []
        random_pnls: list[float] = []
        all_pnls: list[float] = []
        top_k_hits: list[float] = []
        random_hits: list[float] = []
        all_hits: list[float] = []
        score_values: list[float] = []
        pnl_values: list[float] = []
        rank_hit_map: dict[int, list[int]] = defaultdict(list)
        rank_pnl_map: dict[int, list[float]] = defaultdict(list)
        naive_pnls: list[float] = []
        eligible_snapshots = 0

        city_pnl_map: dict[str, list[float]] = defaultdict(list)
        horizon_pnl_map: dict[str, list[float]] = defaultdict(list)
        source_pnl_map: dict[str, list[float]] = defaultdict(list)
        conf_bucket_pnl_map: dict[str, list[float]] = defaultdict(list)

        sorted_keys = []
        for key in sorted(groups.keys()):
            paired_count = sum(1 for row in groups[key] if row.market_id in resolved)
            if paired_count >= top_k:
                sorted_keys.append(key)
        split_idx = int(len(sorted_keys) * 0.7)
        test_keys = sorted_keys[split_idx:] if len(sorted_keys) >= 3 else sorted_keys
        
        for key in test_keys:
            candidates = groups[key]
            scored: list[tuple[float, DatasetRow, bool, float]] = []
            for row in candidates:
                outcome = resolved.get(row.market_id)
                if outcome is None:
                    continue
                won, pnl = _infer_outcome(outcome)
                if won is None or pnl is None:
                    continue
                score = self.scorer.score_row(row)
                scored.append((score, row, won, pnl))
                score_values.append(score)
                pnl_values.append(pnl)

            if len(scored) < top_k:
                continue

            eligible_snapshots += 1
            ranked = sorted(scored, key=lambda item: (item[0], item[3], item[1].city), reverse=True)
            top = ranked[:top_k]

            rng = Random(self._seed_for_group(key))
            randomized = scored[:]
            rng.shuffle(randomized)
            random_pick = randomized[:top_k]

            top_k_pnl = sum(item[3] for item in top)
            random_pnl = sum(item[3] for item in random_pick)
            all_pnl = sum(item[3] for item in scored)

            top_k_pnls.append(top_k_pnl)
            random_pnls.append(random_pnl)
            all_pnls.append(all_pnl)

            top_k_hits.append(sum(1 for item in top if item[2]) / len(top))
            random_hits.append(sum(1 for item in random_pick if item[2]) / len(random_pick))
            all_hits.append(sum(1 for item in scored if item[2]) / len(scored))
            
            naive_pick = sorted(scored, key=lambda item: (item[1].market_implied_prob or 0), reverse=True)[:top_k]
            naive_pnls.append(sum(item[3] for item in naive_pick))

            for rank, item in enumerate(top, start=1):
                rank_hit_map[rank].append(1 if item[2] else 0)
                rank_pnl_map[rank].append(item[3])

            for _, row, won, pnl in scored:
                city_pnl_map[row.city or "unknown"].append(pnl)
                horizon = row.forecast_horizon or "unknown"
                horizon_pnl_map[horizon].append(pnl)
                source = row.forecast_source or "unknown"
                source_pnl_map[source].append(pnl)
                conf = row.confidence if row.confidence is not None else 0.0
                if conf < 0.3:
                    bucket = "low"
                elif conf < 0.6:
                    bucket = "medium"
                else:
                    bucket = "high"
                conf_bucket_pnl_map[bucket].append(pnl)

        rank_hit_rates = [
            (rank, round(mean(values), 4))
            for rank, values in sorted(rank_hit_map.items())
            if values
        ]
        rank_avg_pnl = [
            (rank, round(mean(values), 4))
            for rank, values in sorted(rank_pnl_map.items())
            if values
        ]

        top_k_avg_pnl = round(mean(top_k_pnls), 4) if top_k_pnls else 0.0
        naive_avg_pnl = round(mean(naive_pnls), 4) if naive_pnls else 0.0
        
        # 1. Statistical Significance (Bootstrap)
        pnl_std = 0.0
        pnl_ci = (0.0, 0.0)
        if len(top_k_pnls) > 5:
            import statistics
            pnl_std = statistics.stdev(top_k_pnls)
            # Simple bootstrap approximation
            pnl_ci = (top_k_avg_pnl - 1.96 * pnl_std / math.sqrt(len(top_k_pnls)),
                      top_k_avg_pnl + 1.96 * pnl_std / math.sqrt(len(top_k_pnls)))

        city_breakdown = {
            city: round(mean(pnls), 4) for city, pnls in sorted(city_pnl_map.items()) if pnls
        }
        horizon_breakdown = {
            h: round(mean(pnls), 4) for h, pnls in sorted(horizon_pnl_map.items()) if pnls
        }
        source_breakdown = {
            src: round(mean(pnls), 4) for src, pnls in sorted(source_pnl_map.items()) if pnls
        }
        conf_breakdown = {
            bucket: round(mean(pnls), 4) for bucket, pnls in sorted(conf_bucket_pnl_map.items()) if pnls
        }

        return RankingBacktestReport(
            dataset_path=str(self.dataset_path),
            rows=len(rows),
            resolved_rows=sum(1 for row in rows if row.actual_temp is not None or row.resolution_outcome is not None),
            decision_rows=sum(1 for row in rows if row.event_type == "decision"),
            eligible_snapshots=eligible_snapshots,
            top_k=top_k,
            top_k_avg_pnl=top_k_avg_pnl,
            random_avg_pnl=round(mean(random_pnls), 4) if random_pnls else 0.0,
            all_avg_pnl=round(mean(all_pnls), 4) if all_pnls else 0.0,
            top_k_avg_hit_rate=round(mean(top_k_hits), 4) if top_k_hits else 0.0,
            random_avg_hit_rate=round(mean(random_hits), 4) if random_hits else 0.0,
            all_avg_hit_rate=round(mean(all_hits), 4) if all_hits else 0.0,
            score_pnl_correlation=_correlation(score_values, pnl_values),
            rank_hit_rates=rank_hit_rates,
            rank_avg_pnl=rank_avg_pnl,
            naive_avg_pnl=naive_avg_pnl,
            benchmark_outperformance=round(top_k_avg_pnl - naive_avg_pnl, 4),
            pnl_std=round(pnl_std, 4),
            pnl_95_ci=(round(pnl_ci[0], 4), round(pnl_ci[1], 4)),
            city_breakdown=city_breakdown,
            horizon_breakdown=horizon_breakdown,
            source_breakdown=source_breakdown,
            conf_breakdown=conf_breakdown,
            windows=[]
        )


def should_promote_ranking(report: RankingBacktestReport, min_outperformance: float = 0.0) -> tuple[bool, str]:
    """
    Determine if ranking should be promoted based on backtest.
    
    Args:
        report: RankingBacktestReport from run()
        min_outperformance: Minimum outperformance vs naive (default 0.0)
    
    Returns:
        (should_promote: bool, reason: str)
    """
    if report.eligible_snapshots < 10:
        return False, f"Insufficient snapshots: {report.eligible_snapshots}/10"
    
    if report.benchmark_outperformance < min_outperformance:
        return False, f"No outperformance: {report.benchmark_outperformance:+.4f} < {min_outperformance:+.4f}"
    
    if report.top_k_avg_hit_rate < 0.40:
        return False, f"Low hit rate: {report.top_k_avg_hit_rate:.1%} < 40%"
    
    if report.score_pnl_correlation < 0.1:
        return False, f"Weak correlation: {report.score_pnl_correlation:+.4f} < 0.1"
    
    return True, f"Ready: outperformance={report.benchmark_outperformance:+.4f}, hit_rate={report.top_k_avg_hit_rate:.1%}, corr={report.score_pnl_correlation:+.4f}"


def format_ranking_report(report: RankingBacktestReport) -> list[str]:
    """Render a CLI report for ranking backtests."""
    lines = [f"\n{'='*50}", "RANKING BACKTEST", f"{'='*50}"]
    lines.append(f"Dataset: {report.dataset_path}")
    lines.append(f"Rows: {report.rows}")
    lines.append(f"Resolved rows: {report.resolved_rows}")
    lines.append(f"Decision rows: {report.decision_rows}")
    lines.append(f"Eligible snapshots: {report.eligible_snapshots}")
    lines.append(f"Top-K: {report.top_k}")
    lines.append("")
    lines.append(f"Top-K avg PnL: {report.top_k_avg_pnl:+.4f}")
    lines.append(f"Naive avg PnL: {report.naive_avg_pnl:+.4f} (Benchmark)")
    lines.append(f"Outperformance: {report.benchmark_outperformance:+.4f}")
    lines.append(f"Random avg PnL: {report.random_avg_pnl:+.4f}")
    lines.append(f"All avg PnL: {report.all_avg_pnl:+.4f}")
    lines.append(f"Top-K avg hit rate: {report.top_k_avg_hit_rate:.1%}")
    lines.append(f"Score/PnL correlation: {report.score_pnl_correlation:+.4f}")
    lines.append("")
    lines.append("Hit rate by rank:")
    if report.rank_hit_rates:
        for rank, value in report.rank_hit_rates:
            lines.append(f" - rank {rank}: {value:.1%}")
    else:
        lines.append(" - no ranked samples")
    lines.append("")
    lines.append("Avg PnL by rank:")
    if report.rank_avg_pnl:
        for rank, value in report.rank_avg_pnl:
            lines.append(f" - rank {rank}: {value:+.4f}")
    else:
        lines.append(" - no ranked samples")
    lines.append("")
    lines.append("City breakdown (avg PnL):")
    if report.city_breakdown:
        for city, pnl in sorted(report.city_breakdown.items(), key=lambda x: x[1], reverse=True):
            lines.append(f" - {city}: {pnl:+.4f}")
    else:
        lines.append(" - no data")
    lines.append("")
    lines.append("Horizon breakdown (avg PnL):")
    if report.horizon_breakdown:
        for horizon, pnl in sorted(report.horizon_breakdown.items()):
            lines.append(f" - {horizon}: {pnl:+.4f}")
    else:
        lines.append(" - no data")
    lines.append("")
    lines.append("Source breakdown (avg PnL):")
    if report.source_breakdown:
        for src, pnl in sorted(report.source_breakdown.items(), key=lambda x: x[1], reverse=True):
            lines.append(f" - {src}: {pnl:+.4f}")
    else:
        lines.append(" - no data")
    lines.append("")
    lines.append("Confidence bucket breakdown (avg PnL):")
    if report.conf_breakdown:
        for bucket, pnl in sorted(report.conf_breakdown.items()):
            lines.append(f" - {bucket}: {pnl:+.4f}")
    else:
        lines.append(" - no data")
    lines.append("")
    lines.append(f"95% CI: [{report.pnl_95_ci[0]:+.4f}, {report.pnl_95_ci[1]:+.4f}]")
    lines.append(f"{'='*50}\n")
    return lines

# Audit: Includes fee and slippage awareness
