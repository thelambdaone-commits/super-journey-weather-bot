"""Detailed paper-trading performance report."""
from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..trading.paper_account import PaperAccount
from ..trading.resolver import TRADING_FEE_PERCENT

ENTRY_FRICTION_RATE = PaperAccount.FEE_RATE + PaperAccount.SLIPPAGE_RATE


@dataclass
class PaperRow:
    trade: dict[str, Any]
    actual_temp: float | None
    won: bool | None
    pnl: float | None

    @property
    def cost(self) -> float:
        return float(self.trade.get("cost") or 0.0)

    @property
    def ev(self) -> float:
        return float(self.trade.get("ev") or 0.0)

    @property
    def probability(self) -> float:
        return max(0.0, min(1.0, float(self.trade.get("p") or 0.0)))


def load_paper_trades(path: str | Path = "logs/paper_trades.json") -> list[dict[str, Any]]:
    """Load persisted paper trades."""
    file_path = Path(path)
    if not file_path.exists():
        return []
    try:
        data = json.loads(file_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []
    return data if isinstance(data, list) else []


def load_market_index(markets_dir: str | Path = "data/markets") -> dict[tuple[str, str], dict[str, Any]]:
    """Index market files by city/date."""
    root = Path(markets_dir)
    index: dict[tuple[str, str], dict[str, Any]] = {}
    if not root.exists():
        return index
    for path in root.glob("*.json"):
        try:
            market = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        city = str(market.get("city_name") or market.get("city") or "").lower()
        date = str(market.get("date") or "")
        if city and date:
            index[(city, date)] = market
            index[(str(market.get("city") or "").lower(), date)] = market
    return index


def build_rows(
    trades: list[dict[str, Any]],
    market_index: dict[tuple[str, str], dict[str, Any]],
) -> list[PaperRow]:
    """Attach outcomes and net PnL estimates to paper trades."""
    rows = []
    for trade in trades:
        city = str(trade.get("city") or "").lower()
        date = str(trade.get("date") or "")
        market = market_index.get((city, date), {})
        actual_temp = _as_float(market.get("actual_temp"))
        won = _won_trade(trade, actual_temp)
        pnl = _estimate_net_pnl(trade, won) if won is not None else None
        rows.append(PaperRow(trade=trade, actual_temp=actual_temp, won=won, pnl=pnl))
    return rows


def format_paper_report(
    *,
    trades_path: str | Path = "logs/paper_trades.json",
    markets_dir: str | Path = "data/markets",
    data_dir: str | Path = "data",
) -> str:
    """Return a detailed Markdown paper report."""
    trades = load_paper_trades(trades_path)
    rows = build_rows(trades, load_market_index(markets_dir))
    account = PaperAccount(str(data_dir)).get_state()

    settled = [row for row in rows if row.won is not None]
    clean_settled = [row for row in settled if not _row_quality_flags(row)]
    open_rows = [row for row in rows if row.won is None]
    wins = [row for row in settled if row.won]
    losses = [row for row in settled if row.won is False]
    total_staked = sum(row.cost for row in rows)
    settled_staked = sum(row.cost for row in settled)
    net_pnl = sum(float(row.pnl or 0.0) for row in settled)
    clean_staked = sum(row.cost for row in clean_settled)
    clean_pnl = sum(float(row.pnl or 0.0) for row in clean_settled)
    clean_roi = (clean_pnl / clean_staked * 100.0) if clean_staked else 0.0
    entry_friction = sum(row.cost * ENTRY_FRICTION_RATE for row in rows)
    expected_edge_usd = sum(row.ev * row.cost for row in rows)
    roi = (net_pnl / settled_staked * 100.0) if settled_staked else 0.0
    win_rate = (len(wins) / len(settled) * 100.0) if settled else 0.0
    avg_ev = (sum(row.ev for row in rows) / len(rows)) if rows else 0.0
    brier = _brier_score(settled)
    calibration = _calibration_summary(settled)

    lines = [
        "# Paper Trading Report",
        "",
        "## Executive Summary",
        f"- Trades logged: `{len(rows)}`",
        f"- Settled / open: `{len(settled)}` / `{len(open_rows)}`",
        f"- Win rate: `{win_rate:.1f}%`",
        f"- Net PnL on settled trades: `${net_pnl:+.2f}`",
        f"- ROI on settled stake: `{roi:+.2f}%`",
        f"- Quality-filtered settled trades: `{len(clean_settled)}`",
        f"- Quality-filtered net PnL: `${clean_pnl:+.2f}` (`{clean_roi:+.2f}%` ROI)",
        f"- Total stake logged: `${total_staked:.2f}`",
        f"- Expected edge logged: `${expected_edge_usd:+.2f}`",
        f"- Average EV per trade: `{avg_ev:+.4f}`",
        f"- Entry friction estimate paid/locked: `${entry_friction:.2f}`",
        f"- Paper account balance: `${account.balance:,.2f}`",
        f"- Paper account total PnL: `${account.total_pnl:+.2f}`",
        "",
        "## Calibration",
        f"- Brier score: `{brier:.4f}`" if brier is not None else "- Brier score: `N/A`",
        f"- Calibration buckets: {calibration}",
        "",
        "## Breakdown",
        _format_group("By city", settled, lambda row: str(row.trade.get("city") or "unknown")),
        _format_group("By source", settled, lambda row: str(row.trade.get("forecast_src") or "unknown")),
        _format_group("By horizon", settled, lambda row: str(row.trade.get("horizon") or "unknown")),
        "",
        "## Diagnostics",
    ]

    lines.extend(_diagnostics(rows, settled, clean_settled, expected_edge_usd, net_pnl))
    return "\n".join(lines)


def _won_trade(trade: dict[str, Any], actual_temp: float | None) -> bool | None:
    if actual_temp is None:
        return None
    low = _as_float(trade.get("bucket_low"))
    high = _as_float(trade.get("bucket_high"))
    if low is None or high is None:
        return None
    return low <= actual_temp <= high


def _estimate_net_pnl(trade: dict[str, Any], won: bool | None) -> float | None:
    if won is None:
        return None
    price = float(trade.get("entry_price") or 0.0)
    cost = float(trade.get("cost") or 0.0)
    shares = float(trade.get("shares") or 0.0)
    if price <= 0 or cost <= 0 or shares <= 0:
        return None
    fee = cost * TRADING_FEE_PERCENT
    return round(shares * (1.0 - price) - fee, 2) if won else round(-cost - fee, 2)


def _brier_score(rows: list[PaperRow]) -> float | None:
    if not rows:
        return None
    return sum((row.probability - (1.0 if row.won else 0.0)) ** 2 for row in rows) / len(rows)


def _calibration_summary(rows: list[PaperRow]) -> str:
    if not rows:
        return "`N/A`"
    buckets: dict[str, list[PaperRow]] = defaultdict(list)
    for row in rows:
        low = int(row.probability * 5) * 20
        high = min(low + 20, 100)
        buckets[f"{low}-{high}%"].append(row)

    parts = []
    for label in sorted(buckets, key=lambda item: int(item.split("-")[0])):
        group = buckets[label]
        avg_p = sum(row.probability for row in group) / len(group)
        hit = sum(1 for row in group if row.won) / len(group)
        parts.append(f"`{label}: n={len(group)}, p={avg_p:.2f}, hit={hit:.2f}`")
    return ", ".join(parts)


def _format_group(title: str, rows: list[PaperRow], key_fn) -> str:
    if not rows:
        return f"### {title}\nNo settled trades."
    groups: dict[str, list[PaperRow]] = defaultdict(list)
    for row in rows:
        groups[key_fn(row)].append(row)

    lines = [f"### {title}", "| Group | Trades | Win Rate | Net PnL | ROI | Avg EV |", "|---|---:|---:|---:|---:|---:|"]
    for key, group in sorted(groups.items()):
        stake = sum(row.cost for row in group)
        pnl = sum(float(row.pnl or 0.0) for row in group)
        wr = sum(1 for row in group if row.won) / len(group) * 100.0
        roi = pnl / stake * 100.0 if stake else 0.0
        avg_ev = sum(row.ev for row in group) / len(group)
        lines.append(f"| {key} | {len(group)} | {wr:.1f}% | ${pnl:+.2f} | {roi:+.1f}% | {avg_ev:+.4f} |")
    return "\n".join(lines)


def _diagnostics(
    rows: list[PaperRow],
    settled: list[PaperRow],
    clean_settled: list[PaperRow],
    expected_edge_usd: float,
    net_pnl: float,
) -> list[str]:
    diagnostics = []
    flagged = [(row, _row_quality_flags(row)) for row in rows if _row_quality_flags(row)]
    duplicate_keys = _duplicate_trade_keys(rows)
    if len(clean_settled) < 30:
        diagnostics.append(
            "- Quality-filtered sample too small for profit claims. Need at least `30` clean settled paper trades."
        )
    if not settled:
        diagnostics.append("- No settled paper trades yet, so real edge is still unproven.")
    if flagged:
        diagnostics.append(
            f"- Data quality warning: `{len(flagged)}` trades have impossible/extreme fields "
            "(for example EV > 100%, entry < 1c, or inconsistent shares). Treat raw PnL as contaminated."
        )
        diagnostics.append("- Raw PnL must not be used as proof of edge until contaminated historical trades are excluded.")
    if duplicate_keys:
        diagnostics.append(
            f"- Duplicate decision warning: `{len(duplicate_keys)}` market/date/bucket keys appear more than once."
        )
    if settled and not clean_settled:
        diagnostics.append("- No settled trades remain after quality filters; profit cannot be evaluated yet.")
    if settled and expected_edge_usd > 0 and net_pnl < 0:
        diagnostics.append("- Expected edge is positive but realized PnL is negative: inspect calibration and fill assumptions.")
    if rows and sum(1 for row in rows if row.probability >= 0.95) / len(rows) > 0.2:
        diagnostics.append("- Many trades have probability >= 95%; check overconfidence/calibration.")
    if not diagnostics:
        diagnostics.append("- No obvious report-level anomaly detected. Continue collecting resolved samples.")
    return diagnostics


def _row_quality_flags(row: PaperRow) -> list[str]:
    flags = []
    price = float(row.trade.get("entry_price") or 0.0)
    cost = row.cost
    shares = float(row.trade.get("shares") or 0.0)
    if price <= 0 or price >= 1:
        flags.append("invalid_price")
    if 0 < price < 0.01:
        flags.append("sub_cent_entry")
    if row.ev > 1.0 or row.ev < -1.0:
        flags.append("extreme_ev")
    if price > 0 and cost > 0 and shares > 0 and abs((shares * price) - cost) > max(0.05, cost * 0.02):
        flags.append("inconsistent_shares")
    return flags


def _duplicate_trade_keys(rows: list[PaperRow]) -> set[tuple[Any, ...]]:
    seen = set()
    duplicates = set()
    for row in rows:
        trade = row.trade
        key = (
            trade.get("market_id"),
            trade.get("date"),
            trade.get("bucket_low"),
            trade.get("bucket_high"),
        )
        if key in seen:
            duplicates.add(key)
        seen.add(key)
    return duplicates


def _as_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number):
        return None
    return number
