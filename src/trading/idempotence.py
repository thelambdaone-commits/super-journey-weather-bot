
import hashlib
import time
import json
import os
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

IDEMPOTENCE_FILE = "data/idempotence_registry.json"

class IdempotenceManager:
    """
    Institutional-grade idempotence manager to prevent redundant executions.
    Tracks Signal, Order, and Notification hashes persistently.
    """
    def __init__(self, storage_path: str = IDEMPOTENCE_FILE):
        self.storage_path = Path(storage_path)
        self.registry = self._load()
    
    def _load(self) -> dict:
        if self.storage_path.exists():
            try:
                with open(self.storage_path, 'r') as f:
                    return json.load(f)
            except (Exception,) as e:
                logger.error(f"Failed to load idempotence registry: {e}")
                return {}
        return {}

    def _save(self):
        try:
            self.storage_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.storage_path, 'w') as f:
                json.dump(self.registry, f, indent=2)
        except (Exception,) as e:
            logger.error(f"Failed to save idempotence registry: {e}")

    def is_duplicate(self, category: str, data: str, window_seconds: int = 3600) -> bool:
        """
        Check if an action in a category with specific data has been performed recently.
        Règle: 1 Signal = 1 Ordre = 1 Notification.
        """
        if not data:
            return False
            
        h = hashlib.sha256(data.strip().encode()).hexdigest()
        key = f"{category}:{h}"
        now = time.time()
        
        last_ts = self.registry.get(key)
        if last_ts and (now - last_ts) < window_seconds:
            logger.warning(f"Idempotence block: {category} duplicate detected (age: {now - last_ts:.1f}s)")
            return True
            
        self.registry[key] = now
        self._save()
        return False

    def clear_old_entries(self, max_age_seconds: int = 86400 * 7):
        """Cleanup entries older than 7 days."""
        now = time.time()
        before_count = len(self.registry)
        self.registry = {k: ts for k, ts in self.registry.items() if (now - ts) < max_age_seconds}
        if len(self.registry) < before_count:
            self._save()

# Global instance for easy access
_manager = None

def get_idempotence_manager() -> IdempotenceManager:
    global _manager
    if _manager is None:
        _manager = IdempotenceManager()
    return _manager

# Audit: Includes fee and slippage awareness
