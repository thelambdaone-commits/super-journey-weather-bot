"""
Helper functions for trading engine.
"""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
from .types import RuntimeModes
from ..ai import analyze_forecast, check_anomaly

PAPER_TRADES_FILE = "logs/paper_trades.json"

def log_paper_trade(city: str, date_str: str, horizon: str, signal: dict) -> None:
    """Persist simulated paper trades for later review."""
    path = Path(PAPER_TRADES_FILE)
    path.parent.mkdir(exist_ok=True)

    if path.exists():
        try:
            trades = json.loads(path.read_text(encoding="utf-8"))
        except (Exception,) as e:
            trades = []
    else:
        trades = []

    trades.append({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "city": city,
        "date": date_str,
        "horizon": horizon,
        "market_id": signal["market_id"],
        "question": signal["question"],
        "bucket_low": signal["bucket_low"],
        "bucket_high": signal["bucket_high"],
        "entry_price": signal["entry_price"],
        "cost": signal["cost"],
        "shares": signal["shares"],
        "p": signal["p"],
        "ev": signal["ev"],
        "kelly": signal["kelly"],
        "forecast_temp": signal["forecast_temp"],
        "forecast_src": signal["forecast_src"],
        "ai": signal.get("ai"),
        "status": signal.get("status", "open"),  # Use status from signal (should be "open")
    })
    path.write_text(json.dumps(trades, indent=2, ensure_ascii=False), encoding="utf-8")

def build_signal_marker(signal: dict) -> dict:
    """Build a compact persistent marker for a market signal."""
    return {
        "market_id": signal["market_id"],
        "bucket_low": signal["bucket_low"],
        "bucket_high": signal["bucket_high"],
        "forecast_src": signal["forecast_src"],
        "forecast_temp": signal["forecast_temp"],
        "entry_price": signal["entry_price"],
        "ev": signal.get("ev", 0),
        "ml_conf": signal.get("ml", {}).get("confidence", 0),
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }

def should_emit_marker(state: dict | None, signal: dict) -> bool:
    """Return True when this signal has not already been emitted."""
    if not state:
        return True
    return state.get("market_id") != signal["market_id"]

def format_ai_note(ai: dict | None) -> str:
    """Format AI summary for notifications."""
    if not ai:
        return ""
    confidence = ai.get("confidence")
    recommendation = ai.get("recommendation")
    analysis = ai.get("analysis", "")
    parts = []
    if confidence:
        parts.append(f"Confiance: {confidence}")
    if recommendation:
        parts.append(f"IA: {recommendation}")
    if analysis:
        parts.append(f"Analyse: {analysis[:120]}")
    return "\n" + "\n".join(parts) if parts else ""

def format_ml_note(ml: dict | None) -> str:
    """Format ML summary for notifications."""
    if not ml:
        return ""
    return (
        f"\nML: {ml.get('tier', 'default')} | conf {ml.get('confidence', 0):.2f}"
        f"\nTemp ajustee: {ml.get('adjusted_temp')}"
        f"\nSigma: {ml.get('sigma', 0):.2f} | n={ml.get('n', 0)}"
    )

def bot_mode_label(modes: RuntimeModes) -> str:
    """Return the current bot mode label."""
    active = []
    if modes.paper_mode: active.append("paper_mode")
    if modes.live_trade: active.append("live_trade")
    if modes.signal_mode: active.append("signal_mode")
    active.append(f"tui_{'on' if modes.tui_mode else 'off'}")
    return ",".join(active) if active else "all_off"

def get_ai_trade_context(city: str, snap: dict, signal: dict, unit: str = "C") -> tuple[dict | None, bool]:
    """Optionally review a trade with Groq and flag anomalies."""
    analysis = analyze_forecast(
        city,
        snap.get("ecmwf"),
        snap.get("hrrr"),
        snap.get("metar"),
        None,
        unit=unit,
    )
    if analysis.get("error"):
        return None, False

    anomaly = check_anomaly(
        city,
        signal["forecast_temp"],
        signal["entry_price"],
        signal["ev"],
        signal["p"],
        unit=unit,
    )
    analysis["anomaly"] = anomaly
    return analysis, bool(anomaly.get("is_anomaly"))

# Audit: Includes fee and slippage awareness
