"""
Ouroboros pipeline runner - executes train/calibrate/ai-status.
"""
import subprocess
import hashlib
from pathlib import Path
from typing import Optional


from ...ml.registry import ModelRegistry

class PipelineRunner:
    """Runs the ML pipeline commands and manages versioning."""
    
    def __init__(self, timeout: int = 300):
        self.timeout = timeout
        self.registry = ModelRegistry()
    
    def run(self, command: str) -> tuple[bool, str]:
        """
        Run a single pipeline command.
        """
        cmd_map = {
            "train": ["venv/bin/python3", "bot.py", "train"],
            "calibrate": ["venv/bin/python3", "bot.py", "calibrate"],
            "tune": ["venv/bin/python3", "bot.py", "tune"],
            "ai-status": ["venv/bin/python3", "bot.py", "ai-status"],
        }
        
        if command not in cmd_map:
            return False, f"Unknown command: {command}"
        
        cmd = list(cmd_map[command])
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                cwd=str(Path.cwd()),
            )
            
            output = result.stdout + result.stderr
            return (result.returncode == 0), output
            
        except subprocess.TimeoutExpired:
            return False, f"Timeout après {self.timeout}s"
        except (Exception,) as e:
            return False, str(e)

    def _stable_data_hash(self) -> str:
        """Hash model inputs/outputs deterministically for registry provenance."""
        digest = hashlib.sha256()
        for rel_path in ("data/dataset_rows.jsonl", "data/ml_model.json", "data/calibration.pkl"):
            path = Path(rel_path)
            digest.update(rel_path.encode("utf-8"))
            if path.exists():
                digest.update(path.read_bytes())
            else:
                digest.update(b"<missing>")
        return "sha256_" + digest.hexdigest()[:16]
    
    def run_full_pipeline(self, include_tuning: bool = False) -> tuple[bool, dict]:
        """
        Run full pipeline: [tune ->] train -> calibrate -> registry update.
        
        Args:
            include_tuning: If True, run tuning before train
        """
        results = {}
        
        # 0. Optional tuning
        if include_tuning:
            success, output = self.run("tune")
            results["tune"] = {"success": success, "output": output[:500]}
        
        # 1. Train
        # Detect dataset size to choose model
        try:
            with open("data/dataset_rows.jsonl", "r") as f:
                n_rows = sum(1 for _ in f)
        except:
            n_rows = 0
            
        model_type = "logistic" if n_rows < 300 else "xgboost"
        train_cmd = ["venv/bin/python3", "bot.py", "train", "--model", model_type, "--save"]
        
        try:
            result = subprocess.run(
                train_cmd,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                cwd=str(Path.cwd()),
            )
            success = (result.returncode == 0)
            output = result.stdout + result.stderr
        except (Exception,) as e:
            success = False
            output = str(e)

        results["train"] = {"success": success, "output": output[:500], "model_used": model_type}
        if not success:
            return False, results
        
        # 2. Calibrate
        success, output = self.run("calibrate")
        results["calibrate"] = {"success": success, "output": output[:500]}
        if not success:
            return False, results

        # 3. Governance: Register the new version
        try:
            metrics = {"status": "success", "source": "ouroboros_auto"}
            data_hash = self._stable_data_hash()
            
            version = self.registry.register_model(
                model_path="data/ml_model.json",
                metrics=metrics,
                data_hash=data_hash
            )
            results["registry"] = {"success": True, "version": version}
        except (Exception,) as e:
            results["registry"] = {"success": False, "error": str(e)}
            return False, results

        # 4. Optional diagnostics
        self.run("ai-status")
        
        return True, results
