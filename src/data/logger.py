"""
Structured logging for auditability and replay.
"""
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone

class StructuredLogger:
    def __init__(self, name: str, log_dir: str = "logs"):
        self.log_path = Path(log_dir) / f"{name}_audit.jsonl"
        self.log_path.parent.mkdir(exist_ok=True)
        
        # Configure standard logging too
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.FileHandler(Path(log_dir) / f"{name}.log")
            handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(handler)

    def log_event(self, event_type: str, data: dict):
        """Log a structured event for audit trail."""
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "unix_ts": time.time(),
            "event": event_type,
            **data
        }
        
        # Write to JSONL
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
            
        # Standard log
        self.logger.info(f"{event_type}: {json.dumps(data)}")

# Global instance for trading events
trading_logger = StructuredLogger("trading")
