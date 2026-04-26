"""
Ouroboros configuration.
"""
from dataclasses import dataclass


@dataclass
class OuroborosConfig:
    """Configuration for Ouroboros auto-improvement loop."""
    
    # Thresholds
    min_resolutions: int = 10
    min_dataset_rows: int = 50
    patience: int = 5
    
    # Limits
    max_retrain_per_day: int = 2
    max_tuning_per_day: int = 1
    
    # Safety
    timeout: int = 300  # seconds
    tuning_timeout: int = 300
    tuning_min_improvement: float = 0.01
    
    # GEM thresholds
    gem_gold_threshold: float = 0.95
    gem_silver_threshold: float = 0.85
    gem_bronze_threshold: float = 0.75
    
    # Paths
    state_path: str = "data/ouroboros_state.json"
    lock_path: str = "data/ouroboros.lock"
    backup_dir: str = "data/backups"
    max_backups: int = 5