"""
Ouroboros lock manager - prevents race conditions.
"""
import os
import time
from pathlib import Path


class LockManager:
    """Manages lock file to prevent concurrent runs."""
    
    def __init__(self, lock_path: str = "data/ouroboros.lock", timeout: int = 300):
        self.lock_path = Path(lock_path)
        self.timeout = timeout
    
    def acquire(self) -> bool:
        """Acquire lock. Returns True if acquired, False if already locked."""
        if self.lock_path.exists():
            pid = self.lock_path.read_text().strip()
            # Check if process is still alive
            if pid and self._is_process_alive(pid):
                return False
            # stale lock - remove it
            self.lock_path.unlink(missing_ok=True)
        
        self.lock_path.write_text(str(os.getpid()))
        return True
    
    def release(self) -> None:
        """Release lock."""
        self.lock_path.unlink(missing_ok=True)
    
    def is_locked(self) -> bool:
        """Check if locked by another process."""
        if not self.lock_path.exists():
            return False
        pid = self.lock_path.read_text().strip()
        return self._is_process_alive(pid)
    
    def _is_process_alive(self, pid: str) -> bool:
        """Check if process is still alive."""
        try:
            os.kill(int(pid), 0)
            return True
        except (OSError, ValueError):
            return False
    
    def wait_for_lock(self) -> bool:
        """Wait for lock to be released."""
        start = time.time()
        while time.time() - start < self.timeout:
            if self.acquire():
                return True
            time.sleep(5)
        return False