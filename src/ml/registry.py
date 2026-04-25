"""
Model Registry - Governance and versioning for ML models.
"""
import json
import shutil
from pathlib import Path
from datetime import datetime

class ModelRegistry:
    """
    Manages versioned models and their associated metadata.
    Ensures reproducibility and rollback capabilities.
    """
    def __init__(self, base_dir: str = "data"):
        self.models_dir = Path(base_dir) / "models"
        self.registry_path = self.models_dir / "registry.json"
        self.models_dir.mkdir(parents=True, exist_ok=True)
        
        if not self.registry_path.exists():
            self._save_registry({"current_version": None, "history": []})

    def _load_registry(self) -> dict:
        with open(self.registry_path, "r") as f:
            return json.load(f)

    def _save_registry(self, data: dict):
        with open(self.registry_path, "w") as f:
            json.dump(data, f, indent=2)

    def register_model(self, model_path: str, metrics: dict, data_hash: str) -> str:
        """Register a new model version."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        version = f"v_{timestamp}"
        
        version_dir = self.models_dir / version
        version_dir.mkdir(exist_ok=True)
        
        # Copy model file
        target_path = version_dir / "ml_model.json"
        shutil.copy(model_path, target_path)
        
        # Update registry
        registry = self._load_registry()
        entry = {
            "version": version,
            "timestamp": datetime.now().isoformat(),
            "metrics": metrics,
            "data_hash": data_hash,
            "path": str(target_path)
        }
        registry["history"].append(entry)
        registry["current_version"] = version
        self._save_registry(registry)
        
        return version

    def get_latest_model_path(self) -> str | None:
        registry = self._load_registry()
        version = registry.get("current_version")
        if not version: return None
        
        for entry in registry["history"]:
            if entry["version"] == version:
                return entry["path"]
        return None

    def get_version_history(self) -> list:
        return self._load_registry().get("history", [])
