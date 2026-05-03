"""
Notification Formatter - Bridge to Premium V2 Telegram Templates.
"""
from __future__ import annotations
from . import telegram_control_center as tg


def _money(value, signed: bool = False) -> str:
    try:
        amount = float(value or 0.0)
    except (TypeError, ValueError):
        amount = 0.0
    if signed:
        return f"{amount:+,.2f}$"
    return f"{amount:,.2f}$"


def _pct(value) -> str:
    try:
        return f"{float(value or 0.0) * 100:.1f}%"
    except (TypeError, ValueError):
        return "N/A"


def format_weather_signal(payload: dict) -> str:
    """Wrapper for legacy calls."""
    ai_status = payload.get("ai_status") or "NON_REQUIS"
    paper_status = payload.get("paper_status") or "PAPER_ONLY"
    pnl_realized = payload.get("paper_total_pnl")
    total_gains = payload.get("paper_total_gains")
    total_losses = payload.get("paper_total_losses")

    return (
        f"{payload.get('emoji', '🌡️')} *SIGNAL WEATHER EDGE*\n"
        f"──────────────\n"
        f"📍 *Ville:* {payload.get('city', 'Unknown')}\n"
        f"📅 *Date:* `{payload.get('date', 'N/A')}` | Horizon: `{payload.get('horizon', 'N/A')}`\n"
        f"📦 *Bucket:* `{payload.get('bucket', 'N/A')}`\n"
        f"🧾 *Marché:* {payload.get('market_name', 'N/A')}\n"
        f"──────────────\n"
        f"🤖 *Décision IA:* `{ai_status}`\n"
        f"🧪 *Mode:* `{paper_status}`\n"
        f"→ Proba modèle: `{_pct(payload.get('calibrated_prob'))}` | Prix marché: `{_pct(payload.get('market_prob'))}`\n"
        f"→ Edge net: `{_pct(payload.get('edge'))}` | Qualité: `{_pct(payload.get('signal_score'))}`\n"
        f"→ Mise paper: `{_money(payload.get('size'))}`\n"
        f"──────────────\n"
        f"💰 *Paper PnL à jour*\n"
        f"→ Gains: `{_money(total_gains)}` | Pertes: `{_money(total_losses)}`\n"
        f"→ PnL réalisé: `{_money(pnl_realized, signed=True)}`\n"
        f"→ Cash PnL: `{_money(payload.get('paper_cash_pnl'), signed=True)}` | Expo ouverte: `{_money(payload.get('paper_open_exposure'))}`\n"
        f"→ Solde cash: `{_money(payload.get('paper_balance'))}` | Equity: `{_money(payload.get('paper_equity'))}`\n"
        f"→ Fermés: `{payload.get('paper_closed_trades', 0)}` | Ouverts: `{payload.get('paper_open_trades', 0)}`\n"
        f"──────────────\n"
        f"📡 *Source:* `{payload.get('forecast_source', 'N/A')}`\n"
        f"📝 {payload.get('note') or 'Validation complète avant paper trade.'}"
    )

def format_signal_for_telegram(signal_dict: dict) -> bool:
    """
    Bridge from scanner/engine to the Premium Signal template.
    """
    try:
        tg.send_signal(
            city=signal_dict.get("city", "Unknown"),
            market=f"{'ABOVE' if signal_dict.get('side') == 'ABOVE' else 'BELOW'} {signal_dict.get('threshold')}°C",
            fair_value=signal_dict.get("p", 0.5),
            market_odds=signal_dict.get("entry_price", 0.5),
            edge=signal_dict.get("ev", 0.0),
            confidence=signal_dict.get("ml", {}).get("tier", "LOW"),
            size_pct=signal_dict.get("kelly", 0.0) * 100,
            reason=signal_dict.get("reasons", None)
        )
        return True
    except (Exception):
        return False

def format_daily_recap(stats: dict) -> bool:
    """Bridge to Daily Recap."""
    return tg.send_daily_report(stats)

def format_weekly_recap(stats: dict) -> bool:
    """Bridge to Weekly Recap."""
    return tg.send_weekly_report(stats)

def format_trust_update(trade_result: dict) -> bool:
    """Bridge to Trust Update."""
    return tg.send_trust_update(
        city=trade_result.get("city", "Unknown"),
        market=trade_result.get("market", "Unknown"),
        result=trade_result.get("result", "LOST"),
        pnl=trade_result.get("pnl_pct", 0.0)
    )
