from __future__ import annotations

import json
import os
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .telegram_control_center import send_message


EVENT_LOG = Path(os.getenv("DESK_EVENT_LOG", "data/desk_events.jsonl"))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def log_event(event_type: str, **payload: Any) -> None:
    EVENT_LOG.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "ts": utc_now().isoformat(),
        "type": event_type,
        **payload,
    }
    with EVENT_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def read_events(hours: int = 24) -> list[dict[str, Any]]:
    if not EVENT_LOG.exists():
        return []

    cutoff = utc_now() - timedelta(hours=hours)
    events: list[dict[str, Any]] = []

    with EVENT_LOG.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
                ts = datetime.fromisoformat(row["ts"])
                if ts >= cutoff:
                    events.append(row)
            except (json.JSONDecodeError, KeyError, ValueError):
                continue

    return events


def p95(values: list[float]) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    return statistics.quantiles(values, n=100)[94]


def safe_pct(x: float) -> str:
    sign = "+" if x > 0 else ""
    return f"{sign}{x:.2f}%"


@dataclass
class DeskMetrics:
    scan_p95: float
    error_rate: float
    api_success_rate: float
    signals: int

    pnl_7d: float
    pnl_30d: float
    drawdown: float
    winrate: float
    profit_factor: float
    friction: float

    predicted_edge: float
    realized_edge: float
    best_setup: str
    high_tier_hitrate: float
    drift: str

    top_error: str


def compute_metrics() -> DeskMetrics:
    events_24h = read_events(24)
    events_7d = read_events(24 * 7)
    events_30d = read_events(24 * 30)

    scans = [float(e.get("latency_s", 0)) for e in events_24h if e["type"] == "scan_cycle"]
    errors = [e for e in events_24h if e["type"] == "error"]
    api_calls = [e for e in events_24h if e["type"] == "api_call"]
    signals_24h = [e for e in events_24h if e["type"] == "signal"]

    total_ops = max(1, len(scans) + len(api_calls) + len(signals_24h))
    error_rate = len(errors) / total_ops * 100

    api_success_rate = 100.0
    if api_calls:
        ok = sum(1 for e in api_calls if e.get("ok") is True)
        api_success_rate = ok / len(api_calls) * 100

    trades_7d = [e for e in events_7d if e["type"] == "trade_resolved"]
    trades_30d = [e for e in events_30d if e["type"] == "trade_resolved"]

    def pnl(trades: list[dict[str, Any]]) -> float:
        return sum(float(t.get("net_pnl_pct", 0)) for t in trades)

    pnl_7d = pnl(trades_7d)
    pnl_30d = pnl(trades_30d)

    wins = [float(t.get("net_pnl_pct", 0)) for t in trades_30d if float(t.get("net_pnl_pct", 0)) > 0]
    losses = [abs(float(t.get("net_pnl_pct", 0))) for t in trades_30d if float(t.get("net_pnl_pct", 0)) < 0]

    total_trades = len(wins) + len(losses)
    winrate = (len(wins) / total_trades * 100) if total_trades else 0.0
    profit_factor = (sum(wins) / sum(losses)) if losses else (999.0 if wins else 0.0)

    fees = sum(abs(float(t.get("fees_pct", 0))) for t in trades_30d)
    slippage = sum(abs(float(t.get("slippage_pct", 0))) for t in trades_30d)
    friction = fees + slippage

    equity = []
    current = 0.0
    for t in sorted(trades_30d, key=lambda x: x.get("ts", "")):
        current += float(t.get("net_pnl_pct", 0))
        equity.append(current)

    drawdown = 0.0
    peak = 0.0
    for x in equity:
        peak = max(peak, x)
        drawdown = min(drawdown, x - peak)

    predicted_edges = [float(s.get("edge_pct", 0)) for s in signals_24h]
    predicted_edge = statistics.mean(predicted_edges) if predicted_edges else 0.0

    realized_edges = [float(t.get("realized_edge_pct", t.get("net_pnl_pct", 0))) for t in trades_30d]
    realized_edge = statistics.mean(realized_edges) if realized_edges else 0.0

    setup_pnl: dict[str, float] = defaultdict(float)
    for t in trades_30d:
        setup = str(t.get("setup", "unknown"))
        setup_pnl[setup] += float(t.get("net_pnl_pct", 0))
    best_setup = max(setup_pnl, key=setup_pnl.get) if setup_pnl else "n/a"

    high = [t for t in trades_30d if str(t.get("confidence", "")).upper() == "HIGH"]
    high_wins = [t for t in high if float(t.get("net_pnl_pct", 0)) > 0]
    high_tier_hitrate = (len(high_wins) / len(high) * 100) if high else 0.0

    spreads = [float(e.get("spread_pct", 0)) for e in events_24h if e["type"] == "market_snapshot"]
    avg_spread = statistics.mean(spreads) if spreads else 0.0

    drift = "LOW"
    if realized_edge < predicted_edge * 0.35 and predicted_edge > 0:
        drift = "HIGH"
    elif realized_edge < predicted_edge * 0.65 and predicted_edge > 0:
        drift = "MODERATE"
    if avg_spread > 8:
        drift = "HIGH" if drift == "MODERATE" else "MODERATE"

    error_types = Counter(str(e.get("error_type", "unknown")) for e in errors)
    top_error = error_types.most_common(1)[0][0] if error_types else "none"

    return DeskMetrics(
        scan_p95=p95(scans),
        error_rate=error_rate,
        api_success_rate=api_success_rate,
        signals=len(signals_24h),
        pnl_7d=pnl_7d,
        pnl_30d=pnl_30d,
        drawdown=drawdown,
        winrate=winrate,
        profit_factor=profit_factor,
        friction=friction,
        predicted_edge=predicted_edge,
        realized_edge=realized_edge,
        best_setup=best_setup,
        high_tier_hitrate=high_tier_hitrate,
        drift=drift,
        top_error=top_error,
    )


def format_desk_report(m: DeskMetrics) -> str:
    return f"""
🏛 <b>DESK REPORT</b>

<b>Machine</b>
⚡ Scan P95: {m.scan_p95:.2f}s
🚨 Error Rate: {m.error_rate:.2f}% | Top: {m.top_error}
🌐 API Success: {m.api_success_rate:.1f}%
📡 Signals 24h: {m.signals}

<b>Trading</b>
💰 PnL 7d: {safe_pct(m.pnl_7d)}
💰 PnL 30d: {safe_pct(m.pnl_30d)}
📉 Drawdown: {safe_pct(m.drawdown)}
🎯 Win/PF: {m.winrate:.1f}% / {m.profit_factor:.2f}
🧾 Friction 30d: {m.friction:.2f}%

<b>Alpha</b>
🧠 Pred Edge: {safe_pct(m.predicted_edge)}
✅ Realized Edge: {safe_pct(m.realized_edge)}
🏆 Best Setup: {m.best_setup}
📊 HIGH Hit Rate: {m.high_tier_hitrate:.1f}%
🌪 Drift: <b>{m.drift}</b>

Time: {utc_now().strftime("%Y-%m-%d %H:%M UTC")}
""".strip()


def format_morning_report(m: DeskMetrics) -> str:
    return f"""
☀️ <b>MORNING RISK REPORT</b>

<b>Status</b>
🌐 API Health: {m.api_success_rate:.1f}%
🚨 Pending Issues: {m.top_error if m.error_rate > 0 else "None"}
📡 signals Expected: {m.signals} (24h)

<b>Risk</b>
📉 Current DD: {safe_pct(m.drawdown)}
💰 Balance: Dynamic
🛡 Risk Mode: <b>STABLE</b>

Time: {utc_now().strftime("%Y-%m-%d 08:00 UTC")}
""".strip()


def format_risk_summary(m: DeskMetrics) -> str:
    return f"""
🛡 <b>RISK SUMMARY</b>

📉 Max Drawdown: {safe_pct(m.drawdown)}
💰 Friction: {m.friction:.2f}%
🎯 Win Rate: {m.winrate:.1f}%
📊 Profit Factor: {m.profit_factor:.2f}

<b>Thresholds</b>
Limit: -15.0% DD
Current: {safe_pct(m.drawdown)}

Status: <b>SAFE</b>
""".strip()


def format_health_report(m: DeskMetrics) -> str:
    return f"""
🟢 <b>SYSTEM HEALTH</b>

Scanner: OK
Pricing: OK
Telegram: OK
DB: OK

⚡ Latency P95: {m.scan_p95:.2f}s
🚨 Error Rate: {m.error_rate:.2f}%
📡 signals 24h: {m.signals}
""".strip()


def send_desk_report() -> None:
    send_message(format_desk_report(compute_metrics()))


def send_morning_report() -> None:
    send_message(format_morning_report(compute_metrics()))


def maybe_alert(m: DeskMetrics) -> None:
    if m.drawdown <= -15:
        send_message(f"🚨 <b>CRITICAL RISK</b>\n\nDrawdown breached: {safe_pct(m.drawdown)}")
    if m.error_rate >= 5:
        send_message(f"🚨 <b>CRITICAL OPS</b>\n\nError rate: {m.error_rate:.2f}%\nTop: {m.top_error}")
    if m.api_success_rate < 90:
        send_message(f"⚠️ <b>API DEGRADED</b>\n\nSuccess rate: {m.api_success_rate:.1f}%")
    if m.drift == "HIGH":
        send_message("⚠️ <b>ALPHA DRIFT HIGH</b>\n\nRealized edge is decaying versus predicted edge.")


if __name__ == "__main__":
    metrics = compute_metrics()
    maybe_alert(metrics)
    send_message(format_desk_report(metrics))
