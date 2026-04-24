"""
Ouroboros backup manager.
"""
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional


class BackupManager:
    """Manages backups of ML model and calibration."""
    
    def __init__(self, backup_dir: str = "data/backups", max_backups: int = 5):
        self.backup_dir = Path(backup_dir)
        self.max_backups = max_backups
    
    def create(self) -> list[Path]:
        """Create backup of current model and calibration."""
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        created = []
        
        # Backup files
        for name in ["ml_model.json", "calibration.pkl"]:
            src = Path("data") / name
            if src.exists():
                dst = self.backup_dir / f"{name.replace('.', '_')}_{ts}{src.suffix}"
                shutil.copy2(src, dst)
                created.append(dst)
        
        # Cleanup old backups
        self._cleanup()
        
        return created
    
    def restore(self, timestamp: Optional[str] = None) -> bool:
        """Restore from backup. If timestamp is None, restored latest."""
        files = self._list_backups()
        
        # Find latest by timestamp in filename
        target = None
        for f in files:
            if timestamp and timestamp in f.name:
                target = f
                break
            elif not target or f.stat().st_mtime > target.stat().st_mtime:
                target = f
        
        if not target:
            return False
        
        # Restore ml_model
        if "ml_model" in target.name:
            dst = Path("data") / "ml_model.json"
            shutil.copy2(target, dst)
        
        # Restore calibration 
        elif "calibration" in target.name:
            dst = Path("data") / "calibration.pkl"
            shutil.copy2(target, dst)
        
        return True
    
    def list(self) -> list[dict]:
        """List all backups with metadata."""
        backups = []
        for f in self._list_backups():
            backups.append({
                "path": str(f),
                "name": f.name,
                "timestamp": f.stat().st_mtime,
                "size": f.stat().st_size,
            })
        return sorted(backups, key=lambda x: x["timestamp"], reverse=True)
    
    def _list_backups(self):
        """List backup files."""
        results = []
        for ext in ["json", "pkl"]:
            for f in self.backup_dir.glob(f"*.{ext}"):
                results.append(f)
        return sorted(results, key=lambda x: x.stat().st_mtime, reverse=True)
    
    def _cleanup(self) -> None:
        """Remove old backups beyond max_backups."""
        for ext in ["json", "pkl"]:
            files = sorted(
                self.backup_dir.glob(f"*.{ext}"),
                key=lambda x: x.stat().st_mtime,
                reverse=True,
            )
            for f in files[self.max_backups:]:
                f.unlink(missing_ok=True)