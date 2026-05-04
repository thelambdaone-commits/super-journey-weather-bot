#!/usr/bin/env python3
"""
Watchdog for WeatherBot.
Monitors data/state.json heartbeat and restarts the bot if it stops scanning.
"""
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

HEARTBEAT_TIMEOUT = 2 * 3600  # 2 hours without scan = stuck
STATE_FILE = Path("data/state.json")
BOT_PID_FILE = Path("bot.pid")
LOCK_FILE = Path("data/weatherbot.lock")


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def get_bot_pid() -> int | None:
    """Read PID from bot.pid or lock file."""
    for path in [BOT_PID_FILE, LOCK_FILE]:
        if path.exists():
            try:
                pid = int(path.read_text().strip())
                # Check if process is alive
                os.kill(pid, 0)
                return pid
            except (ValueError, ProcessLookupError, PermissionError):
                pass
    return None


def kill_bot(pid: int):
    """Gracefully kill the bot process."""
    log(f"Killing bot (PID={pid})...")
    try:
        os.kill(pid, signal.SIGTERM)
        time.sleep(5)
        os.kill(pid, 0)  # Check if still alive
        log(f"Bot still alive, sending SIGKILL")
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def start_bot():
    """Start the bot via the run script or systemd."""
    log("Starting bot...")
    # Try systemd first
    try:
        result = subprocess.run(["systemctl", "start", "weatherbot"], capture_output=True, text=True)
        if result.returncode == 0:
            log("Bot started via systemctl")
            return
    except FileNotFoundError:
        pass

    # Fallback: run script
    subprocess.Popen(
        ["/home/74h2hfpyj79x/weatherbot/run_bot.sh"],
        cwd="/home/74h2hfpyj79x/weatherbot",
    )
    log("Bot start script executed")


def check_heartbeat() -> bool:
    """Check if heartbeat is fresh."""
    if not STATE_FILE.exists():
        log("No state file found")
        return False

    try:
        data = json.loads(STATE_FILE.read_text())
        last_beat = data.get("last_heartbeat", 0)
        if last_beat == 0:
            log("Heartbeat never set")
            return False

        age = time.time() - last_beat
        if age > HEARTBEAT_TIMEOUT:
            log(f"Heartbeat too old: {age:.0f}s (timeout={HEARTBEAT_TIMEOUT}s)")
            return False

        log(f"Heartbeat OK (age={age:.0f}s)")
        return True
    except (json.JSONDecodeError, Exception) as e:
        log(f"Error reading state: {e}")
        return False


def main():
    log("=== WeatherBot Watchdog Started ===")
    log(f"Heartbeat timeout: {HEARTBEAT_TIMEOUT}s")
    log(f"State file: {STATE_FILE}")

    if not check_heartbeat():
        pid = get_bot_pid()
        if pid:
            kill_bot(pid)
            time.sleep(10)

        # Clean up stale lock
        if LOCK_FILE.exists():
            log("Removing stale lock file")
            LOCK_FILE.unlink(missing_ok=True)

        start_bot()
    else:
        log("Bot is healthy")


if __name__ == "__main__":
    main()
