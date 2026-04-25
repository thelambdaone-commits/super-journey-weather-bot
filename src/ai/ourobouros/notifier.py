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
        """Send message to Telegram."""
        if not self._notifier:
            return False
        try:
            return self._notifier.send(message)
        except Exception:
            return False
    
    def notify_check(self, reason: str) -> bool:
        """Notify about check result (skip)."""
        return self.send(f"🐍 *OUROBOROS CHECK*\n⏸️ _{reason}_")
    
    def notify_start(self, new_resolutions: int, total_gems: int) -> bool:
        """Notify about retrain start."""
        return self.send(
            f"🔥 *GEM FACTORY STARTED*\n"
            f"Nouvelles résolutions: `{new_resolutions}`\n"
            f"GEMs détectés: `{total_gems}`\n"
            f"Pipeline: `train → calibrate → ai-status`"
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
        gems_emoji = ""
        if gems_gold > 0:
            gems_emoji += f"🥇{gems_gold} "
        if gems_silver > 0:
            gems_emoji += f"🥈{gems_silver} "
        if gems_bronze > 0:
            gems_emoji += f"🥉{gems_bronze}"
        
        return self.send(
            f"✅ *GEM FACTORY SUCCESS*\n"
            f"Rows entraînées: `{trained_rows}`\n"
            f"Retrains aujourd'hui: `{retrain_count}`\n"
            f"GEMs: {gems_emoji or 'Aucun'}"
        )
    
    def notify_failed(self, error: str, restored: bool = True) -> bool:
        """Notify about failed retrain with rollback."""
        restore_msg = "♻️ _Ancien modèle restauré_" if restored else "⚠️ _Pas de backup à restaurer_"
        return self.send(
            f"🚨 *OUROBOROS FAILED*\n"
            f"Erreur: `{error[:100]}`\n"
            f"{restore_msg}"
        )
    
    def notify_skip(self, reason: str) -> bool:
        """Notify about skipped retrain."""
        return self.send(f"⏭️ *OUROBOROS SKIP*\n_{reason}_")