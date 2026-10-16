"""Process lifecycle logging helpers."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable


DEFAULT_LOG_MAX_BYTES = 20 * 1024 * 1024
DEFAULT_LOG_BACKUP_COUNT = 5


@dataclass(frozen=True)
class CleanupFailure:
    owner: str
    error_type: str
    error: str
    exception: Exception = field(repr=False, compare=False)


class CleanupError(RuntimeError):
    def __init__(self, failures: Iterable[CleanupFailure]) -> None:
        self.failures = tuple(failures)
        if not self.failures:
            raise ValueError("cleanup error requires at least one failure")
        details = "; ".join(
            f"{failure.owner}: {failure.error_type}: {failure.error}"
            for failure in self.failures
        )
        super().__init__(f"resource cleanup failed: {details}")


@dataclass(frozen=True)
class CleanupStep:
    owner: str
    cleanup: Callable[[], None]
    release: Callable[[], None] = lambda: None

    def __post_init__(self) -> None:
        if not isinstance(self.owner, str) or not self.owner.strip():
            raise ValueError("cleanup owner must be a non-empty string")
        if not callable(self.cleanup) or not callable(self.release):
            raise ValueError("cleanup step callbacks must be callable")
        object.__setattr__(self, "owner", self.owner.strip())


def cleanup_owned_resources(steps: Iterable[CleanupStep]) -> None:
    """Attempt every cleanup and release ownership only after success."""

    failures: list[CleanupFailure] = []
    for step in steps:
        try:
            step.cleanup()
            step.release()
        except Exception as exc:
            failures.append(
                CleanupFailure(
                    owner=step.owner,
                    error_type=type(exc).__name__,
                    error=str(exc).strip() or type(exc).__name__,
                    exception=exc,
                )
            )
    if failures:
        raise CleanupError(failures)


class LifecycleLogger:
    def __init__(self, log_path: Path) -> None:
        self._log_path = log_path
        self._max_bytes = _read_positive_int_env("ORC_LOG_MAX_BYTES", DEFAULT_LOG_MAX_BYTES)
        self._backup_count = _read_non_negative_int_env("ORC_LOG_BACKUP_COUNT", DEFAULT_LOG_BACKUP_COUNT)
        self._log_path.parent.mkdir(parents=True, exist_ok=True)

    def event(self, event: str, **fields: object) -> None:
        payload = {
            "ts": time.time(),
            "event": event,
            **fields,
        }
        try:
            encoded = json.dumps(payload, ensure_ascii=True) + "\n"
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
            self._rotate_if_needed(len(encoded.encode("utf-8")))
            with self._log_path.open("a", encoding="utf-8") as f:
                f.write(encoded)
        except (FileNotFoundError, OSError):
            # Background daemon threads may emit late events during test teardown
            # after temporary directories are removed.
            return

    def _rotate_if_needed(self, next_bytes: int) -> None:
        if self._max_bytes <= 0 or not self._log_path.exists():
            return
        if self._log_path.stat().st_size + next_bytes <= self._max_bytes:
            return

        if self._backup_count <= 0:
            self._log_path.unlink(missing_ok=True)
            return

        oldest = self._rotated_path(self._backup_count)
        oldest.unlink(missing_ok=True)
        for index in range(self._backup_count - 1, 0, -1):
            src = self._rotated_path(index)
            dst = self._rotated_path(index + 1)
            if src.exists():
                src.replace(dst)
        self._log_path.replace(self._rotated_path(1))

    def _rotated_path(self, index: int) -> Path:
        return self._log_path.with_name(f"{self._log_path.name}.{index}")


def _read_positive_int_env(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _read_non_negative_int_env(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed >= 0 else default
