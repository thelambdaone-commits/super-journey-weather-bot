import os
import json
import subprocess
from pathlib import Path
from datetime import datetime

REPORT = {"timestamp": str(datetime.utcnow()), "sections": {}}

PASS_SCORE = 0
TOTAL_SCORE = 0


def grade(name, passed, details):
    global PASS_SCORE, TOTAL_SCORE
    TOTAL_SCORE += 1
    if passed:
        PASS_SCORE += 1
    REPORT["sections"][name] = {"passed": passed, "details": details}


def run(cmd):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=600)
        return r.returncode, r.stdout[-4000:], r.stderr[-4000:]
    except Exception as e:
        return 999, "", str(e)


# ==================================================
# 1. CODE HEALTH
# ==================================================

rc, out, err = run("pytest tests/ -q")
grade("unit_tests", rc == 0, err if rc else "pytest passed")

rc, out, err = run("python3 bot.py audit --city Paris")
grade("startup_audit", rc == 0, err if rc else out)

# ==================================================
# 2. BACKTEST ENGINE
# ==================================================

rc, out, err = run("python3 bot.py ranking-backtest --city Paris")
grade("backtest_runs", rc == 0, err if rc else out)

# ==================================================
# 3. LOOKAHEAD TEST
# ==================================================

files = list(Path(".").rglob("*.py"))
bad = []

for f in files:
    if "venv" in str(f) or ".gemini" in str(f) or "tests/audit" in str(f):
        continue
    txt = f.read_text(errors="ignore").lower()

    if "close[i]" in txt and "signal[i]" in txt:
        bad.append(str(f))

grade("lookahead_static_scan", len(bad) == 0, bad if bad else "No obvious lookahead patterns")

# ==================================================
# 4. EXCEPTION HYGIENE
# ==================================================

bad = []

for f in files:
    if "venv" in str(f) or ".gemini" in str(f) or "tests/" in str(f) or "run_full_audit" in str(f):
        continue
    txt = f.read_text(errors="ignore")
    if "except Exception" in txt:
        # Filter out some known acceptable ones if needed, but the audit is strict
        bad.append(str(f))

grade("broad_exceptions", len(bad) == 0, bad if bad else "No broad except Exception blocks")

# ==================================================
# 5. COST MODEL
# ==================================================

bad = []

for f in files:
    if "venv" in str(f) or ".gemini" in str(f):
        continue
    if "src/trading" not in str(f) and "src/strategy" not in str(f) and "src/backtest" not in str(f):
        continue
    txt = f.read_text(errors="ignore").lower()

    if "fee" not in txt and "slippage" not in txt:
        bad.append(str(f))

grade("transaction_cost_model", len(bad) < 5, bad if bad else "Fees/slippage references found in core trading logic")

# ==================================================
# 6. DATA QUALITY
# ==================================================

db_files = list(Path(".").rglob("*.db"))
grade("database_exists", len(db_files) > 0, [str(x) for x in db_files])

# ==================================================
# 7. PAPER LIVE MODE
# ==================================================

rc, out, err = run("python3 bot.py live --paper --signal-off --tui-off --limit-runs 1")
grade("paper_mode_boot", rc == 0, err if rc else "paper mode launches")

# ==================================================
# 8. PERFORMANCE FILES
# ==================================================

perf = list(Path(".").rglob("*performance*.py"))
grade("performance_engine", len(perf) > 0, [str(x) for x in perf])

# ==================================================
# FINAL SCORE
# ==================================================

REPORT["score"] = f"{PASS_SCORE}/{TOTAL_SCORE}"
REPORT["ratio"] = round(PASS_SCORE / TOTAL_SCORE, 2)

if REPORT["ratio"] >= 0.90:
    REPORT["verdict"] = "PRODUCTION READY"
elif REPORT["ratio"] >= 0.70:
    REPORT["verdict"] = "PROMISING BUT NEEDS HARDENING"
elif REPORT["ratio"] >= 0.50:
    REPORT["verdict"] = "RESEARCH PROJECT"
else:
    REPORT["verdict"] = "CHATEAU DE CARTES"

with open("audit_verdict.md", "w") as f:
    f.write("# Institutional Audit Verdict\n\n")
    f.write("```json\n")
    f.write(json.dumps(REPORT, indent=2))
    f.write("\n```")

print(json.dumps(REPORT, indent=2))
print("\nSaved -> audit_verdict.md")
