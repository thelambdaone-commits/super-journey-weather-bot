import os

import pytest

from src.utils.process_lock import ProcessLock, ProcessLockError


def test_process_lock_blocks_second_instance(tmp_path):
    lock_path = tmp_path / "weatherbot.lock"
    first = ProcessLock(lock_path)
    second = ProcessLock(lock_path)

    first.acquire()
    try:
        with pytest.raises(ProcessLockError) as exc:
            second.acquire()
        assert exc.value.holder_pid == str(os.getpid())
    finally:
        first.release()
        second.release()


def test_process_lock_can_be_reacquired_after_release(tmp_path):
    lock_path = tmp_path / "weatherbot.lock"

    first = ProcessLock(lock_path)
    first.acquire()
    first.release()

    second = ProcessLock(lock_path)
    try:
        second.acquire()
    finally:
        second.release()
