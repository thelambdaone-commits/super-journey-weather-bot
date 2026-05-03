"""
Ouroboros - Auto-improvement loop for GEM factory.

Usage:
    import run_ourobouros from this package when running auto-improvement tasks.

    result = run_ourobouros(
        min_resolutions=10,
        max_retrain_per_day=2,
        patience=5,
        timeout=300,
    )
"""
from .engine import OuroborosEngine, run_ourobouros
from .config import OuroborosConfig
from .state import StateManager
from .lock import LockManager
from .backup import BackupManager
from .decision import DecisionEngine
from .pipeline import PipelineRunner
from .notifier import OuroborosNotifier

__all__ = [
    "OuroborosEngine",
    "OuroborosConfig",
    "run_ourobouros",
    "StateManager",
    "LockManager",
    "BackupManager",
    "DecisionEngine",
    "PipelineRunner",
    "OuroborosNotifier",
]
