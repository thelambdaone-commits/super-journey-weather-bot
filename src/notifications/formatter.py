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
    ml = payload.get("ml", {})
    confidence_pct = _format_percent(ml.get("confidence", payload.get("confidence")), 2)
    score = payload.get("signal_score")
    score_text = "n/a" if score is None else f"{score:.2f}"
    rank = payload.get("rank")
    rank_text = "n/a" if rank is None else f"#{int(rank)}"
    link = payload.get("trade_url") or f"https://polymarket.com/event/will-it-be-above-{payload.get('city', 'weather')}-on-{payload.get('date', 'today')}"
    
    # Top 1% Audit Insights
    uncertainty = ml.get("bayesian_uncertainty")
    anomaly = ml.get("anomaly_error")
    sentiment = ml.get("sentiment_boost")
    portfolio = ml.get("portfolio_notes")
    
    audit_lines = []
    if uncertainty is not None:
        audit_lines.append(f"🧬 Incertitude Bayésienne: `{uncertainty:.3f}`")
    if anomaly is not None:
        audit_lines.append(f"🔍 Erreur Reconstruction (AE): `{anomaly:.3f}`")
    if sentiment and sentiment > 0:
        audit_lines.append(f"🔥 Sentiment Alerte: `+{sentiment*100:.0f}%`")
    if portfolio:
        audit_lines.append(f"💼 Note Portefeuille: _{portfolio}_")

    audit_section = "\n".join(audit_lines) if audit_lines else "✅ Audit Engine: *OK*"

    return (
        f"{payload.get('emoji', '🌡️')} *SIGNAL MÉTÉO PREMIUM*\n\n"
        f"📍 *Ville:* {payload.get('city', 'n/a').upper()}\n"
        f"📅 *Horizon:* {payload.get('horizon', 'n/a')}\n"
        f"🏦 *Marché:* {payload.get('market_name', 'n/a')}\n\n"
        f"──────────────\n"
        f"🌡️ *Prévisions (ML calibré)*\n"
        f"→ `{_format_temp(payload.get('forecast_temp'), payload.get('unit'))}`"
        f" (±{_format_temp(payload.get('sigma', ml.get('sigma')), payload.get('unit'))})\n"
        f"→ Source: *{(payload.get('forecast_source') or 'n/a').upper()}*\n\n"
        f"📊 *Analyse Probabiliste*\n"
        f"→ Modèle: *{_format_percent(payload.get('calibrated_prob'), 2)}*\n"
        f"→ Marché: *{_format_percent(payload.get('market_prob'), 2)}*\n"
        f"→ Edge (ROI): *{_format_percent(payload.get('edge'), 2)}*\n\n"
        f"🛡️ *Audit Engine v2.5.2*\n"
        f"{audit_section}\n\n"
        f"⚡ *Score & Classement*\n"
        f"→ Signal Score: `{score_text}/1.0`\n"
        f"→ Rang Global: `{rank_text}`\n"
        f"→ Confiance: `{confidence_pct}`\n\n"
        f"💰 *Exécution Recommandée*\n"
        f"→ Action: `{payload.get('action', 'ACHETER OUI')}`\n"
        f"→ Mise: *{_format_money(payload.get('size'))}* (Kelly fractionné)\n\n"
        f"🔗 *Accès Direct*\n"
        f"→ [Ouvrir sur Polymarket]({link})\n\n"
        f"🚨 *Statut:* {payload.get('status', 'OPPORTUNITÉ')}"
    )
