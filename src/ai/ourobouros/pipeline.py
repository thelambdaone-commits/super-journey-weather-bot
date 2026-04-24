"""
Ouroboros pipeline runner - executes train/calibrate/ai-status.
"""
import subprocess
from pathlib import Path
from typing import Optional


class PipelineRunner:
    """Runs the ML pipeline commands."""
    
    def __init__(self, timeout: int = 300):
        self.timeout = timeout
    
    def run(self, command: str) -> tuple[bool, str]:
        """
        Run a single pipeline command.
        
        Args:
            command: "train", "calibrate", or "ai-status"
            
        Returns:
            (success: bool, output: str)
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
            
            if result.returncode == 0:
                return True, output
            else:
                return False, output
                
        except subprocess.TimeoutExpired:
            return False, f"Timeout après {self.timeout}s"
        except Exception as e:
            return False, str(e)
    
    def run_full_pipeline(self) -> tuple[bool, dict]:
        """
        Run full pipeline: train -> calibrate -> ai-status
        
        Returns:
            (success: bool, results: dict)
        """
        results = {}
        
        # Train
        success, output = self.run("train")
        results["train"] = {"success": success, "output": output[:500] if output else ""}
        if not success:
            return False, results
        
        # Calibrate
        success, output = self.run("calibrate")
        results["calibrate"] = {"success": success, "output": output[:500] if output else ""}
        if not success:
            return False, results
        
        # AI Status (optional check)
        success, output = self.run("ai-status")
        results["ai-status"] = {"success": success, "output": output[:500] if output else ""}
        
        return True, results