"""
Process-level lock utilities.
"""
from __future__ import annotations

import errno
import fcntl
import os
from pathlib import Path
from typing import TextIO


class ProcessLockError(RuntimeError):
    """Raised when another process already holds the lock."""

    def __init__(self, lock_path: Path, holder_pid: str | None = None):
        self.lock_path = lock_path
        self.holder_pid = holder_pid
        detail = f"another instance is already running"
        if holder_pid:
            detail += f" (pid {holder_pid})"
        super().__init__(f"{detail}; lock={lock_path}")


class ProcessLock:
    """Non-blocking exclusive lock held for the lifetime of this object."""

    def __init__(self, lock_path: str | os.PathLike[str]):
        self.lock_path = Path(lock_path)
        self._file: TextIO | None = None

    def acquire(self) -> None:
        """Acquire the process lock or raise ProcessLockError."""
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_file = self.lock_path.open("a+", encoding="utf-8")

        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            holder_pid = self._read_holder_pid(lock_file)
            lock_file.close()
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                raise ProcessLockError(self.lock_path, holder_pid) from exc
            raise

        # Successfully locked - now replace the holder PID with our own.
        lock_file.seek(0)
        lock_file.truncate()
        lock_file.write(f"{os.getpid()}\n")
        lock_file.flush()
        os.fsync(lock_file.fileno())
        self._file = lock_file

    def release(self) -> None:
        """Release the lock if this process owns it."""
        if self._file is None:
            return
        try:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        finally:
            self._file.close()
            self._file = None
            try:
                self.lock_path.unlink(missing_ok=True)
            except OSError:
                pass

    def __enter__(self) -> "ProcessLock":
        self.acquire()
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        self.release()

    def _read_holder_pid(self, lock_file: TextIO) -> str | None:
        """Read PID from a lock file that we failed to lock."""
        try:
            lock_file.seek(0)
            return lock_file.read().strip() or None
        except OSError:
            return None
