"""
Institutional-grade data validation and anti-leakage checks.
"""
from __future__ import annotations
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any

class AntiLeakageScanner:
    """
    Scans datasets for information leakage from future outcomes.
    """
    def __init__(self, dataset_path: str):
        self.dataset_path = Path(dataset_path)

    def scan(self) -> Dict[str, Any]:
        """Perform anti-leakage audit on the dataset."""
        if not self.dataset_path.exists():
            return {"status": "error", "message": "Dataset not found"}

        violations = []
        total_rows = 0
        
        with open(self.dataset_path, "r", encoding="utf-8") as f:
            for line in f:
                total_rows += 1
                row = json.loads(line)
                
                # Rule 1: No labels in decision rows
                if row.get("event_type") == "decision":
                    if row.get("actual_temp") is not None:
                        violations.append(f"Row {total_rows}: Decision row contains 'actual_temp' (label leakage)")
                    if row.get("resolution_outcome") is not None:
                        violations.append(f"Row {total_rows}: Decision row contains 'resolution_outcome' (label leakage)")
                
                # Rule 2: Timestamps consistency
                event_ts = row.get("timestamp")
                if event_ts:
                    if isinstance(event_ts, str):
                        event_dt = datetime.fromisoformat(event_ts.replace("Z", "+00:00"))
                    else:
                        event_dt = datetime.fromtimestamp(event_ts, tz=timezone.utc)
                    
                    # Check if any feature looks like a timestamp and is in the future
                    for k, v in row.items():
                        if "timestamp" in k.lower():
                            try:
                                if isinstance(v, str):
                                    feature_dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
                                else:
                                    feature_dt = datetime.fromtimestamp(v, tz=timezone.utc)
                                    
                                if feature_dt > event_dt:
                                    violations.append(f"Row {total_rows}: Feature '{k}' is from future ({v} > {event_ts})")
                            except (ValueError, TypeError, OSError):
                                continue

        return {
            "status": "success" if not violations else "violation",
            "total_rows": total_rows,
            "violations": violations[:10], # Show first 10
            "violation_count": len(violations)
        }

def run_leakage_audit(dataset_path: str = "data/dataset_v4.jsonl"):
    """Run the audit and print result."""
    scanner = AntiLeakageScanner(dataset_path)
    report = scanner.scan()
    print(f"🕵️ ANTI-LEAKAGE AUDIT: {report['status'].upper()}")
    print(f"Total rows scanned: {report['total_rows']}")
    if report['violations']:
        print(f"Found {report['violation_count']} violations!")
        for v in report['violations']:
            print(f"  - {v}")
    else:
        print("✅ No leakage detected in decision/label separation.")
