import os
import re
import requests
from typing import Optional, List
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

from ..weather.config import get_config
from .formatter import format_weather_signal


def escape_markdown(text: str) -> str:
    """Escape special chars for Telegram Markdown V1."""
    if not text:
        return ""
    # For V1, we mainly need to escape _, *, `, and [
    for char in ['_', '*', '`', '[']:
        text = text.replace(char, '\\' + char)
    return text


def _format_signal_msg(city: str, date: str, bucket: str, price: float, ev: float, 
                    cost: float, source: str, question: str, note: str = "") -> str:
    """Format signal message with escaped fields."""
    # Escape all user-supplied fields
    safe_city = escape_markdown(city)
    safe_question = escape_markdown(question[:50] if question else "")
    safe_note = escape_markdown(note[:100] if note else "")
    
    msg = (
        f"🌡️ *SIGNAL METEO*\n\n"
        f"📍 Ville: *{safe_city}*\n"
        f"📅 Date: {date}\n"
        f"📦 Bucket: `{bucket}`\n"
        f"💰 Prix: ${price:.3f}\n"
        f"⚡ EV: `+{ev:.2f}`\n"
        f"💵 Mise: ${cost:.2f}\n"
        f"📡 Source: {source.upper()}\n"
    )
    if safe_question:
        msg += f"\n🏛️ Question: _{safe_question}_"
    if safe_note:
        msg += f"\n📝 Note: _{safe_note}_"
    return msg


class TelegramNotifier:
    """Telegram notifications."""
    
    def __init__(self, token: str = "", chat_id: str = ""):
        config = get_config()
        # Try env vars first, then config
        self.token = token or os.environ.get("TELEGRAM_TOKEN", config.telegram_bot_token)
        self.chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID", config.telegram_chat_id)
        self.signal_chat_id = os.environ.get(
            "TELEGRAM_SIGNAL_CHAT_ID",
            config.telegram_signal_chat_id or self.chat_id,
        )

    def verify_token(self) -> bool:
        """Verify if the token is valid by calling getMe."""
        if not self.token:
            return False
        url = f"https://api.telegram.org/bot{self.token}/getMe"
        try:
            r = requests.get(url, timeout=5)
            return r.ok
        except Exception:
            return False
    
    def send(self, message: str, parse_mode: Optional[str] = None, chat_id: Optional[str] = None) -> bool:
        """Send message to Telegram. parse_mode=None for security (prevents injection)."""
        target_chat_id = chat_id or self.chat_id
        if not self.token or not target_chat_id:
            return False
        
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": target_chat_id,
            "text": message,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            if not response.ok:
                print(f"Telegram error {response.status_code}: {response.text}")
            response.raise_for_status()
            data = response.json()
            return bool(data.get("ok"))
        except Exception as exc:
            print(f"Telegram exception: {exc}")
            return False
    
    def notify_trade_open(self, city: str, date: str, bucket: str, price: float, 
                       ev: float, cost: float, source: str, ai_note: str = "") -> bool:
        """Notify trade opened."""
        msg = (
            f"🚀 *TRADE OUVERT*\n\n"
            f"📍 *Ville:* {city}\n"
            f"📅 *Date:* {date}\n"
            f"📦 *Bucket:* `{bucket}`\n"
            f"💰 *Prix:* `${price:.3f}`\n"
            f"⚡ *EV:* `+{ev:.2f}`\n"
            f"💵 *Mise:* `${cost:.2f}`\n"
            f"📡 *Source:* {source.upper()}"
        )
        if ai_note:
            msg += f"\n\n_{ai_note.strip()}_"
        return self.send(msg, parse_mode="Markdown")

    def notify_signal(self, city: str, date: str, bucket: str, price: float, 
                   ev: float, cost: float, source: str, horizon: str, 
                   question: str, market_id: str, note: str = "", 
                   ai_note: str = "", *,
                   calibrated_prob: float | None = None, market_prob: float | None = None,
                   uncertainty: float | None = None, signal_type: str | None = None,
                   quality: float | None = None, priority: str | None = None,
                   emoji: str | None = None, confidence_score: float | None = None,
                   source_bias: float | None = None, trade_context: dict | None = None) -> bool:
        """Send a manual trading signal."""
        payload = {
            "city": city,
            "date": date,
            "bucket": bucket,
            "forecast_source": source,
            "calibrated_prob": calibrated_prob,
            "market_prob": market_prob,
            "edge": ev,
            "size": cost,
            "horizon": horizon,
            "market_name": question,
            "confidence": confidence_score,
            "signal_score": quality,
            "priority": priority or "NORMAL",
            "emoji": emoji or "🌡️",
            "source_bias": source_bias,
            "status": "OPPORTUNITÉ" if ev > 0 else "IGNORER",
            "reason_lines": [],
            "note": note
        }
        if trade_context:
            payload.update(trade_context)
        
        message = format_weather_signal(payload)
        if ai_note:
            message = f"{message}\n\n_{ai_note.strip()}_"
            
        return self.send(message, parse_mode="Markdown", chat_id=self.signal_chat_id)

    def notify_bot_started(self, mode: str, cities: int, scan_minutes: int) -> bool:
        """Notify bot startup."""
        return self.send(
            f"✅ *BOT DÉMARRÉ*\n\n"
            f"⚙️ *Mode:* `{mode}`\n"
            f"🏙️ *Villes:* `{cities}`\n"
            f"⏱️ *Scan:* `{scan_minutes} min`\n"
            f"📡 *Statut:* `Surveillance active`",
            parse_mode="Markdown",
        )

    def notify_bot_stopped(self, reason: str) -> bool:
        """Notify bot shutdown."""
        return self.send(
            f"⚠️ *BOT ARRÊTÉ*\n\n"
            f"❓ *Raison:* `{reason}`\n"
            f"📡 *Statut:* `Inactif`",
            parse_mode="Markdown",
        )

    def notify_health(self, message: str) -> bool:
        """Send bot health information."""
        return self.send(
            f"🏥 *SANTÉ DU BOT*\n\n{message}",
            parse_mode="Markdown",
        )

    def notify_trade_win(self, city: str, date: str, bucket: str, pnl: float, temp: str, balance: float) -> bool:
        """Notify trade win."""
        return self.send(
            f"🎉 *TRADE GAGNÉ*\n\n"
            f"📍 *Ville:* {city}\n"
            f"📅 *Date:* {date}\n"
            f"📦 *Bucket:* `{bucket}`\n"
            f"🌡️ *Temp Réelle:* `{temp}`\n"
            f"💰 *Gain:* `+${pnl:.2f}`\n"
            f"🏦 *Solde:* `${balance:.2f}`",
            parse_mode="Markdown"
        )

    def notify_trade_loss(self, city: str, date: str, bucket: str, pnl: float, balance: float) -> bool:
        """Notify trade loss."""
        return self.send(
            f"❌ *TRADE PERDU*\n\n"
            f"📍 *Ville:* {city}\n"
            f"📅 *Date:* {date}\n"
            f"📦 *Bucket:* `{bucket}`\n"
            f"💸 *Perte:* `${pnl:.2f}`\n"
            f"🏦 *Solde:* `${balance:.2f}`",
            parse_mode="Markdown"
        )

    def notify_hourly_report(self, summary: dict, city_signals: list[dict]) -> bool:
        """Send a comprehensive hourly portfolio and market summary."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 1. Header & Portfolio Summary
        safe_api_status = escape_markdown(summary['api_status'])
        msg = (
            f"🕒 *RAPPORT HORAIRE - WEATHERBOT*\n"
            f"📅 {now}\n"
            f"──────────────\n"
            f"💰 *Portefeuille*\n"
            f"→ PnL Total: `{summary['pnl_total']:+.2f}$` ({summary['pnl_pct']:+.2f}%)\n"
            f"→ Expo: `{summary['exposure']:.2f}$` | Drawdown: `{summary['drawdown']:.1f}%`\n"
            f"→ Actifs: `{summary['active_signals']}` | Effective Bets: `{summary.get('hhi_div', 0)}` (HHI)\n"
            f"→ Drift: `{summary['drift'].upper()}`\n"
            f"──────────────\n"
            f"📡 *Statut Système*\n"
            f"→ Uptime: `{summary['uptime']}`\n"
            f"→ APIs: {safe_api_status}\n"
            f"──────────────\n"
        )
        
        # 2. City Signals Summary
        if city_signals:
            msg += "📍 *Signaux Détectés*\n"
            for s in city_signals[:10]: # Limit to 10 for readability
                safe_city = escape_markdown(s['city'])
                msg += (
                    f"• {safe_city.upper()} | {s['edge']:+.1f}% edge | {s['conf']}% conf\n"
                    f"  Prix: {s['price']:.2f}$ | Risk: {s['risk']}\n"
                )
            if len(city_signals) > 10:
                msg += f"  *(+ {len(city_signals)-10} autres villes)*\n"
        else:
            msg += "✅ *Surveillance active :* Aucun signal majeur détecté.\n"
            
        msg += "\n🚨 _Audit Engine v2.5.1 Active_"
        return self.send(msg, parse_mode="Markdown")

    def notify_gem_alert(self, signal: dict) -> bool:
        """Send an immediate high-priority alert for GEM signals."""
        now = datetime.now().strftime("%H:%M")
        safe_city = escape_markdown(signal['city'])
        safe_question = escape_markdown(signal['question'])
        safe_reason = escape_markdown(signal['reason'])
        
        msg = (
            f"💎 *ALERTE PRIORITAIRE : SIGNAL GEM* 💎\n"
            f"⏰ {now} | Confiance: {signal['conf']}%\n"
            f"──────────────\n"
            f"📍 *Ville:* {safe_city.upper()}\n"
            f"🏦 *Marché:* {safe_question}\n"
            f"──────────────\n"
            f"📊 *MÉTRIQUES CLÉS*\n"
            f"→ Edge Estimé: `{signal['edge']:+.2f}%` 🔥\n"
            f"→ Signal Score: `{signal['score']:.2f}/1.0`\n"
            f"→ Prix Marché: `{signal['price']:.2f}$`\n"
            f"→ Probabilité ML: `{signal['prob']*100:.1f}%`\n"
            f"──────────────\n"
            f"💰 *TRADE RECOMMANDÉ*\n"
            f"→ Action: `ACHETER OUI`\n"
            f"→ Mise: `${signal['sizing']:.2f}` (Kelly fractionné)\n"
            f"→ Horizon: {signal['horizon']} jours\n"
            f"──────────────\n"
            f"🧠 *RAISONNEMENT*\n"
            f"_{safe_reason}_\n\n"
            f"🔗 [Accéder au Marché]({signal['url']})\n"
            f"──────────────\n"
            f"⚠️ *Statut Risque:* {signal['risk_status'].upper()}"
        )
        return self.send(msg, parse_mode="Markdown", chat_id=self.signal_chat_id)


# Global notifier
_notifier: Optional[TelegramNotifier] = None


def get_notifier(token: str = "", chat_id: str = "") -> TelegramNotifier:
    """Get global notifier instance."""
    global _notifier
    if _notifier is None:
        _notifier = TelegramNotifier(token, chat_id)
    return _notifier
