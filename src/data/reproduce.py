
import hashlib
import os
import json
from pathlib import Path

def get_code_hash() -> str:
    """Calculate a hash of the entire src directory for reproducibility."""
    hasher = hashlib.sha1()
    src_path = Path("src")
    if not src_path.exists():
        return "unknown"
    
    for path in sorted(src_path.rglob("*.py")):
        if "__pycache__" in str(path): continue
        hasher.update(path.read_bytes())
    return hasher.hexdigest()[:8]

def save_audit_artifact(report_text: str):
    """Save the audit report as a persistent artifact with reproducibility metadata."""
    report_id = f"audit_{int(os.path.getmtime('bot.py'))}_{get_code_hash()}"
    path = Path("logs/artifacts") / f"{report_id}.md"
    path.parent.mkdir(exist_ok=True)
    
    metadata = {
        "report_id": report_id,
        "code_hash": get_code_hash(),
        "timestamp": os.path.getmtime('bot.py'),
        "env_hash": hashlib.sha1(open(".env", "rb").read()).hexdigest()[:8] if Path(".env").exists() else "none"
    }
    
    content = f"# Audit Report {report_id}\n\n"
    content += f"## Reproducibility Metadata\n"
    content += f"```json\n{json.dumps(metadata, indent=2)}\n```\n\n"
    content += report_text
    
    path.write_text(content, encoding="utf-8")
    print(f"✅ Audit artifact saved to {path}")
