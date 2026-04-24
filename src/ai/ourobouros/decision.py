"""
Ouroboros decision engine - determines when to retrain.
"""
from datetime import date
from typing import Optional


class DecisionEngine:
    """Determines if retraining should occur."""
    
    def __init__(
        self,
        min_resolutions: int = 10,
        min_dataset_rows: int = 50,
        patience: int = 5,
    ):
        self.min_resolutions = min_resolutions
        self.min_dataset_rows = min_dataset_rows
        self.patience = patience
    
    def should_retrain(
        self,
        state: dict,
        gem_gold: int = 0,
        gem_silver: int = 0,
        gem_bronze: int = 0,
    ) -> tuple[bool, str]:
        """
        Determine if retrain should happen.
        
        Returns (should_retrain: bool, reason: str)
        """
        today = str(date.today())
        
        # Check daily limit
        if state.get("last_retrain_date") != today:
            state["retrain_count_today"] = 0
            state["last_retrain_date"] = today
        
        # Check if locked out today
        if state.get("retrain_count_today", 0) >= 2:
            return False, f"Limite journalière atteinte ({state['retrain_count_today']}/2)"
        
        # Load dataset stats
        dataset_rows = state.get("last_trained_rows", 0)
        resolved = state.get("last_trained_resolved", 0)
        
        # Update GEM counts
        total_gems = gem_gold + gem_silver + gem_bronze
        state["gems_gold"] = state.get("gems_gold", 0) + gem_gold
        state["gems_silver"] = state.get("gems_silver", 0) + gem_silver
        state["gems_bronze"] = state.get("gems_bronze", 0) + gem_bronze
        state["gems_detected"] = state.get("gems_detected", 0) + total_gems
        
        # Check patience - new resolutions since last train
        # This would be calculated by external caller
        new_resolutions = state.get("pending_resolutions", 0)
        
        if new_resolutions < self.patience:
            return False, f"Patience: {new_resolutions}/{self.patience} résolutions"
        
        # Check minimum resolutions
        if new_resolutions < self.min_resolutions:
            return False, f"Pas assez de nouvelles résolutions: {new_resolutions}/{self.min_resolutions}"
        
        # Check GEM quality degradation
        gem_ratio = total_gems / max(1, new_resolutions)
        if gem_ratio < 0.1 and new_resolutions >= self.min_resolutions:
            # Low GEM ratio but enough data - might need retrain
            return True, f"GEM ratio bas ({gem_ratio:.1%}) - retrain recommandé"
        
        return True, f"Prêt pour retrain: {new_resolutions} nouvelles résolutions"