"""
Ouroboros engine - main orchestrator.
"""
from datetime import datetime
from pathlib import Path

from .config import OuroborosConfig
from .state import StateManager
from .lock import LockManager
from .backup import BackupManager
from .decision import DecisionEngine
from .pipeline import PipelineRunner
from .notifier import OuroborosNotifier


class OuroborosEngine:
    """
    Ouroboros Auto-Improvement Engine.
    
    Architecture:
        SCAN → SIGNAL → TRADE → RESOLUTION → FEEDBACK
                                      ↓
                            OUROBOROS LOOP
                                      ↓
                            retrain (si conditions réunies)
                                      ↓
                            nouveau modèle + calibration
    """
    
    def __init__(self, config: OuroborosConfig = None):
        self.config = config or OuroborosConfig()
        
        # Initialize managers
        self.state_manager = StateManager(self.config.state_path)
        self.lock_manager = LockManager(
            self.config.lock_path,
            self.config.timeout,
        )
        self.backup_manager = BackupManager(
            self.config.backup_dir,
            self.config.max_backups,
        )
        self.decision = DecisionEngine(
            self.config.min_resolutions,
            self.config.min_dataset_rows,
            self.config.patience,
            self.config.max_retrain_per_day,
        )
        self.pipeline = PipelineRunner(self.config.timeout)
        self.notifier = OuroborosNotifier()
        
        # Load state
        self.state = self.state_manager.load()
    
    def run(self) -> dict:
        """
        Run Ouroboros loop.
        
        Returns:
            dict with keys: success, action, reason, details
        """
        result = {
            "success": False,
            "action": "check",
            "reason": "",
            "details": {},
        }
        
        # 1. Check lock
        if not self.lock_manager.acquire():
            result["action"] = "locked"
            result["reason"] = "Parallèle en cours"
            return result
        
        try:
            # 2. Load current dataset stats
            dataset_rows = self._count_dataset_rows()
            resolved_rows = self._count_resolved_rows()
            new_resolutions = resolved_rows - self.state.get("last_trained_resolved", 0)
            
            # Update pending resolutions
            self.state["pending_resolutions"] = new_resolutions
            self.state["resolved_count"] = resolved_rows
            self.state["dataset_rows"] = dataset_rows
            
            # 3. Decision: should we retrain?
            should_train, reason = self.decision.should_retrain(
                self.state,
                gem_gold=0,
                gem_silver=0,
                gem_bronze=0,
            )
            
            if not should_train:
                result["action"] = "skip"
                result["reason"] = reason
                self.notifier.notify_skip(reason)
                return result
            
            # 4. Notify start
            total_gems = (
                self.state.get("gems_gold", 0) +
                self.state.get("gems_silver", 0) +
                self.state.get("gems_bronze", 0)
            )
            self.notifier.notify_start(new_resolutions, total_gems)
            
            # 5. Create backup
            self.backup_manager.create()
            
            # 6. Run pipeline
            success, details = self.pipeline.run_full_pipeline()
            
            if success:
                # 7. Update state
                self.state["last_trained_rows"] = dataset_rows
                self.state["last_trained_resolved"] = resolved_rows
                self.state["retrain_count_today"] = self.state.get("retrain_count_today", 0) + 1
                self.state["last_status"] = "success"
                self.state["last_run_at"] = datetime.utcnow().isoformat()
                
                result["success"] = True
                result["action"] = "retrain"
                result["reason"] = "Pipeline exécuté"
                result["details"] = details
                
                # 8. Notify success
                self.notifier.notify_success(
                    trained_rows=dataset_rows,
                    retrain_count=self.state["retrain_count_today"],
                )
            else:
                # 7. Rollback
                restored = self.backup_manager.restore()
                
                self.state["last_status"] = "failed"
                error_msg = str(details)[:200]
                result["reason"] = error_msg
                
                # 8. Notify failure
                self.notifier.notify_failed(error_msg, restored)
            
            # Save state
            self.state_manager.save(self.state)
            
        except (Exception,) as e:
            result["action"] = "error"
            result["reason"] = str(e)
            
            # Try rollback
            self.backup_manager.restore()
            self.notifier.notify_failed(str(e), True)
        
        finally:
            # Release lock
            self.lock_manager.release()
        
        return result
    
    def _count_dataset_rows(self) -> int:
        """Count total dataset rows."""
        path = Path("data/dataset_rows.jsonl")
        if not path.exists():
            return 0
        with open(path) as f:
            return sum(1 for _ in f)
    
    def _count_resolved_rows(self) -> int:
        """Count resolved dataset rows (with actual_temp not None)."""
        import json
        path = Path("data/dataset_rows.jsonl")
        if not path.exists():
            return 0
        count = 0
        with open(path) as f:
            for line in f:
                try:
                    row = json.loads(line)
                    if row.get("actual_temp") is not None or row.get("resolution_outcome") in {"win", "loss"}:
                        count += 1
                except (Exception,) as e:
                    pass
        return count
    
    def get_status(self) -> dict:
        """Get current Ouroboros status."""
        return {
            "locked": self.lock_manager.is_locked(),
            "state": self.state,
            "dataset_rows": self._count_dataset_rows(),
            "resolved_rows": self._count_resolved_rows(),
            "pending_resolutions": self.state.get("pending_resolutions", 0),
            "backups": self.backup_manager.list(),
        }


def run_ourobouros(
    min_resolutions: int = 10,
    max_retrain_per_day: int = 2,
    patience: int = 5,
    timeout: int = 300,
) -> dict:
    """
    Simple entry point for Ouroboros.
    
    Args:
        min_resolutions: Minimum new resolutions to trigger retrain
        max_retrain_per_day: Maximum retrains per day
        patience: Minimum resolutions between retrains
        timeout: Timeout for each pipeline step (seconds)
    
    Returns:
        dict with keys: success, action, reason, details
    """
    config = OuroborosConfig(
        min_resolutions=min_resolutions,
        max_retrain_per_day=max_retrain_per_day,
        patience=patience,
        timeout=timeout,
    )
    engine = OuroborosEngine(config)
    return engine.run()
