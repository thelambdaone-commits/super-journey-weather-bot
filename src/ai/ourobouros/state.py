"""
Ouroboros state manager.
"""
import json
from pathlib import Path
from typing import Optional


class StateManager:
    """Manages Ouroboros persistent state."""
    
    def __init__(self, state_path: str = "data/ouroboros_state.json"):
        self.state_path = Path(state_path)
    
    def load(self) -> dict:
        """Load state from disk."""
        if self.state_path.exists():
            try:
                return json.loads(self.state_path.read_text())
            except (json.JSONDecodeError, IOError):
                pass
        return self._default_state()
    
    def _default_state(self) -> dict:
        """Default state."""
        return {
            "last_trained_rows": 0,
            "last_trained_resolved": 0,
            "retrain_count_today": 0,
            "last_retrain_date": None,
            "last_status": "cold_start",
            "last_brier_score": None,
            "last_log_loss": None,
            "resolved_count": 0,
            "gems_detected": 0,
            "gems_gold": 0,
            "gems_silver": 0,
            "gems_bronze": 0,
        }
    
    def save(self, state: dict) -> None:
        """Save state to disk."""
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(state, indent=2))
    
    def reset(self) -> dict:
        """Reset to default state."""
        state = self._default_state()
        self.save(state)
        return state