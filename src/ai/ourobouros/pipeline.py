"""
Ouroboros pipeline runner - executes train/calibrate/ai-status.
"""
import subprocess
from pathlib import Path
from typing import Optional


from src.ml.registry import ModelRegistry

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
            "train": ["python", "bot.py", "train"],
            "calibrate": ["python", "bot.py", "calibrate"],
            "ai-status": ["python", "bot.py", "ai-status"],
        }
        
        if command not in cmd_map:
            return False, f"Unknown command: {command}"
        
        cmd = cmd_map[command]
        
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
        except Exception as e:
            return False, str(e)
    
    def run_full_pipeline(self) -> tuple[bool, dict]:
        """
        Run full pipeline: train -> calibrate -> registry update.
        """
        results = {}
        
        # 1. Train
        success, output = self.run("train")
        results["train"] = {"success": success, "output": output[:500]}
        if not success:
            return False, results
        
        # 2. Calibrate
        success, output = self.run("calibrate")
        results["calibrate"] = {"success": success, "output": output[:500]}
        if not success:
            return False, results

        # 3. Governance: Register the new version
        try:
            # Extract basic metrics from output if possible (placeholder)
            # In a real v3.0, bot.py train would return JSON metrics
            metrics = {"status": "success", "source": "ouroboros_auto"}
            data_hash = "sha256_" + str(hash(output))[:12] # Simple hash for demo
            
            version = self.registry.register_model(
                model_path="data/ml_model.json",
                metrics=metrics,
                data_hash=data_hash
            )
            results["registry"] = {"success": True, "version": version}
        except Exception as e:
            results["registry"] = {"success": False, "error": str(e)}

        # 4. Optional diagnostics
        self.run("ai-status")
        
        return True, results