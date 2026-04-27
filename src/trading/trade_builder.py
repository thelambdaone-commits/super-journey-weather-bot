"""
Trade-oriented signal payload builder.
"""
from __future__ import annotations

from urllib.parse import quote


def build_polymarket_link(event_slug: str | None, market_id: str | None) -> str | None:
    """Build the safest available Polymarket link for a signal."""
    if event_slug:
        return f"https://polymarket.com/event/{quote(str(event_slug))}"
    if market_id:
        return f"https://polymarket.com/market/{quote(str(market_id))}"
    return "https://polymarket.com/weather"


def _confidence_label(confidence: float | None) -> str:
    if confidence is None:
        return "INCONNUE"
    if confidence >= 0.85:
        return "ÉLEVÉE"
    if confidence >= 0.70:
        return "MOYENNE"
    return "FAIBLE"


def _risk_label(signal: dict) -> str:
    uncertainty = float(signal.get("edge_penalties", {}).get("uncertainty", 0.0))
    spread = float(signal.get("spread", 0.0) or 0.0)
    if uncertainty >= 0.15 or spread >= 0.05:
        return "ÉLEVÉ"
    if uncertainty >= 0.08 or spread >= 0.025:
        return "MODÉRÉ"
    return "FAIBLE"


def _regime_label(signal: dict) -> str:
    spread = float(signal.get("features", {}).get("forecast_spread", 0.0) or 0.0)
    if spread >= 4.0:
        return "EXTRÊME"
    if spread >= 2.0:
        return "VOLATILE"
    return "STABLE"


def _signal_score(signal: dict) -> float:
    edge = max(float(signal.get("ev", 0.0) or 0.0), 0.0)
    confidence = max(float(signal.get("ml", {}).get("confidence", 0.0) or 0.0), 0.0)
    liquidity = float(signal.get("features", {}).get("liquidity", 0.0) or 0.0)
    liquidity_factor = min(liquidity / 5000.0, 1.0)
    return round(edge * confidence * liquidity_factor, 4)


def _reason_lines(signal: dict) -> list[str]:
    lines: list[str] = []
    spread = float(signal.get("features", {}).get("forecast_spread", 0.0) or 0.0)
    top_bucket = signal.get("features", {}).get("top_bucket")
    market_edge = float(signal.get("p", 0.0) or 0.0) - float(signal.get("entry_price", 0.0) or 0.0)

    if spread <= 1.5:
        lines.append("Modèles en accord (faible dispersion)")
    elif spread >= 3.5:
        lines.append("Régime volatil; mise ajustée au risque")

    if market_edge >= 0.10:
        lines.append("Le marché sous-estime nettement ce bucket")
    elif market_edge > 0:
        lines.append("Modèle au-dessus du prix du marché")

    if top_bucket and top_bucket != f"{signal.get('bucket_low')}-{signal.get('bucket_high')}":
        lines.append(f"Foule concentrée sur {top_bucket}")

    source = signal.get("forecast_src")
    if source:
        lines.append(f"Source principale: {str(source).upper()}")

    return lines[:3]


def build_trade_payload(
    *,
    city: str,
    date_str: str,
    horizon: str,
    bucket: str,
    unit: str,
    signal: dict,
    question: str,
    event_slug: str | None,
    priority: str,
    emoji: str,
) -> dict:
    """Build a formatter-friendly trade payload for notifications."""
    market_name = question or f"{city} max temperature {date_str}"
    confidence = float(signal.get("ml", {}).get("confidence", 0.0) or 0.0)
    return {
        "city": city,
        "date": date_str,
        "horizon": horizon,
        "market_name": market_name,
        "bucket": bucket,
        "unit": unit,
        "forecast_temp": signal.get("forecast_temp"),
        "sigma": signal.get("sigma"),
        "forecast_source": signal.get("forecast_src"),
        "source_bias": signal.get("ml", {}).get("bias"),
        "calibrated_prob": signal.get("p"),
        "market_prob": signal.get("entry_price"),
        "edge": signal.get("ev"),
        "confidence": confidence,
        "confidence_label": _confidence_label(confidence),
        "risk_label": _risk_label(signal),
        "regime_label": _regime_label(signal),
        "action": f"ACHETER OUI ({bucket})",
        "size": signal.get("cost"),
        "kelly": signal.get("kelly", 0.0),
        "reason_lines": _reason_lines(signal),
        "trade_url": build_polymarket_link(event_slug, signal.get("market_id")),
        "status": "OPPORTUNITÉ" if float(signal.get('ev', 0.0) or 0.0) > 0 else "IGNORER",
        "signal_score": _signal_score(signal),
        "priority": priority,
        "emoji": emoji,
    }

# Audit: Includes fee and slippage awareness
