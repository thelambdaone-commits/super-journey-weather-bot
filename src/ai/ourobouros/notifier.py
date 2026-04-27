"""
Ouroboros notifier - Telegram notifications for ouroboros events.
"""
import os
from typing import Optional


class OuroborosNotifier:
    """Sends Telegram notifications for Ouroboros events."""
    
    def __init__(self, enabled: bool = True):
        self.enabled = enabled and os.getenv("OUROBOROS_TELEGRAM_FEED", "false").lower() == "true"
        
        if self.enabled:
            # Reuse existing TelegramNotifier
            try:
                from src.notifications import get_notifier
                self._notifier = get_notifier()
            except ImportError:
                self._notifier = None
        else:
            self._notifier = None
    
    def send(self, message: str) -> bool:
        """Send message to Telegram with Markdown enabled."""
        if not self._notifier:
            return False
        try:
            # Explicitly pass parse_mode="Markdown"
            return bool(self._notifier.send(message, parse_mode="Markdown"))
        except (Exception,) as e:
            return False
    
    def notify_check(self, reason: str) -> bool:
        """Notify about check result (skip)."""
        return self.send(
            "🐍 *OUROBOROS CHECK*\n"
            "──────────────\n"
            f"⏸️ _ {reason} _"
        )
    
    def notify_start(self, new_resolutions: int, total_gems: int) -> bool:
        """Notify about retrain start."""
        return self.send(
            "🔥 *GEM FACTORY STARTED*\n"
            "──────────────\n"
            "⚙️ *OPÉRATION*\n"
            f"→ Résolutions: `{new_resolutions}`\n"
            f"→ GEMs: `{total_gems}` détectés\n"
            "→ Pipeline: `Train → Calibrate`\n"
            "──────────────\n"
            "🏗️ _ Optimisation du modèle en cours... _"
        )
    
    def notify_success(
        self,
        trained_rows: int,
        retrain_count: int,
        gems_gold: int = 0,
        gems_silver: int = 0,
        gems_bronze: int = 0,
    ) -> bool:
        """Notify about successful retrain."""
        gems_list = []
        if gems_gold > 0: gems_list.append(f"🥇 GOLD: `{gems_gold}`")
        if gems_silver > 0: gems_list.append(f"🥈 SILVER: `{gems_silver}`")
        if gems_bronze > 0: gems_list.append(f"🥉 BRONZE: `{gems_bronze}`")
        
        gems_display = "\n".join(gems_list) if gems_list else "`Aucun`"
        
        return self.send(
            "✅ *GEM FACTORY SUCCESS*\n"
            "──────────────\n"
            "📊 *RÉSULTATS*\n"
            f"→ Dataset: `{trained_rows}` échantillons\n"
            f"→ Retrains/24h: `{retrain_count}`\n"
            "──────────────\n"
            "💎 *GEMS PRODUITS*\n"
            f"{gems_display}\n"
            "──────────────\n"
            "✨ _ Modèle mis à jour avec succès _"
        )
    
    def notify_failed(self, error: str, restored: bool = True) -> bool:
        """Notify about failed retrain with rollback."""
        restore_msg = "🟢 _ Ancien modèle restauré _" if restored else "🔴 _ Échec de la restauration _"
        return self.send(
            "🚨 *OUROBOROS FAILED*\n"
            "──────────────\n"
            "❌ *ERREUR SYSTÈME*\n"
            "→ Type: `Critical Failure`\n"
            f"→ Log: `{error[:100]}`\n"
            "──────────────\n"
            "🛡️ *SÉCURITÉ*\n"
            f"{restore_msg}"
        )
    
    def notify_skip(self, reason: str) -> bool:
        """Notify about skipped retrain."""
        return self.send(
            "⏭️ *OUROBOROS SKIP*\n"
            f"_ {reason} _"
        )