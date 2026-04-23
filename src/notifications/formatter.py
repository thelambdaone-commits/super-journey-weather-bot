"""
Signal message formatting for Telegram and other notification channels.
"""
from __future__ import annotations


def _format_percent(value: float | None, digits: int = 1) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}%}"


def _format_temp(value: float | None, unit: str | None) -> str:
    if value is None:
        return "n/a"
    suffix = unit or ""
    return f"{value:.1f}{suffix}"


def _format_money(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"${value:.2f}"


def _format_reason_lines(reason_lines: list[str]) -> str:
    if not reason_lines:
        return "→ Aucun raisonnement disponible"
    return "\n".join(f"→ {line}" for line in reason_lines[:3])


def format_weather_signal(payload: dict) -> str:
    """Render a premium, trade-ready weather signal message in French with Markdown."""
    confidence_pct = _format_percent(payload.get("confidence"), 0)
    score = payload.get("signal_score")
    score_text = "n/a" if score is None else f"{score:.2f}"
    rank = payload.get("rank")
    rank_text = "n/a" if rank is None else f"#{int(rank)}"
    link = payload.get("trade_url") or "https://polymarket.com/weather"
    source_bias = payload.get("source_bias")
    source_bias_text = "n/a" if source_bias is None else f"{source_bias:+.2f}{payload.get('unit', '')}"

    return (
        f"{payload.get('emoji', '🌡️')} *SIGNAL MÉTÉO EDGE*\n\n"
        f"📍 *Ville:* {payload.get('city', 'n/a')}\n"
        f"📅 *Horizon:* {payload.get('horizon', 'n/a')}\n"
        f"🏦 *Marché:* {payload.get('market_name', 'n/a')}\n\n"
        f"──────────────\n"
        f"🌡️ *Prévisions (ML calibré)*\n"
        f"→ `{_format_temp(payload.get('forecast_temp'), payload.get('unit'))}`"
        f" (±{_format_temp(payload.get('sigma'), payload.get('unit'))})\n"
        f"→ Source: *{(payload.get('forecast_source') or 'n/a').upper()}* | biais `{source_bias_text}`\n\n"
        f"📊 *Marché vs Modèle*\n"
        f"→ Bucket: `{payload.get('bucket', 'n/a')}`\n"
        f"→ Modèle: *{_format_percent(payload.get('calibrated_prob'))}*\n"
        f"→ Marché: *{_format_percent(payload.get('market_prob'))}*\n\n"
        f"⚡ *Edge*\n"
        f"→ `{_format_percent(payload.get('edge'))}` (*{payload.get('priority', 'NORMAL')}*)\n"
        f"→ Signal Score: `{score_text}`\n"
        f"→ Rang: `{rank_text}`\n\n"
        f"📈 *Confiance*\n"
        f"→ {confidence_pct} (*{payload.get('confidence_label', 'n/a')}*)\n"
        f"→ Risque: *{payload.get('risk_label', 'n/a')}* | Régime: *{payload.get('regime_label', 'n/a')}*\n\n"
        f"💰 *Trade Recommandé*\n"
        f"→ `{payload.get('action', 'n/a')}`\n"
        f"→ Mise: *{_format_money(payload.get('size'))}* | Kelly: `{payload.get('kelly', 0.0):.2f}`\n\n"
        f"🧠 *Raisonnement*\n"
        f"{_format_reason_lines(payload.get('reason_lines', []))}\n\n"
        f"🔗 *Lien du Marché*\n"
        f"→ [Cliquez ici pour trader]({link})\n\n"
        f"🚨 *Statut:* {payload.get('status', 'OPPORTUNITÉ')}"
    )
