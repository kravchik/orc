"""Process lifecycle runtime for lead/worker app-server processes."""

from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Mapping, Optional


@dataclass(frozen=True)
class ProcessSpec:
    name: str
    command: list[str]
    cwd: Optional[str] = None
    env: Optional[Mapping[str, str]] = None


class LifecycleLogger:
    def __init__(self, log_path: Path) -> None:
        self._log_path = log_path
        self._log_path.parent.mkdir(parents=True, exist_ok=True)

    def event(self, event: str, **fields: object) -> None:
        payload = {
            "ts": time.time(),
            "event": event,
            **fields,
        }
        try:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
            with self._log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=True) + "\n")
        except (FileNotFoundError, OSError):
            # Background daemon threads may emit late events during test teardown
            # after temporary directories are removed.
            return


class ProcessRuntime:
    def __init__(
        self,
        specs: Dict[str, ProcessSpec],
        logger: LifecycleLogger,
        stop_timeout_sec: float = 2.0,
    ) -> None:
        self._specs = specs
        self._logger = logger
        self._stop_timeout_sec = stop_timeout_sec
        self._procs: Dict[str, subprocess.Popen[bytes]] = {}

    def start(self) -> None:
        if self._procs:
            raise RuntimeError("runtime already started")
        for role in ("lead", "worker"):
            spec = self._specs[role]
            proc = subprocess.Popen(
                spec.command,
                cwd=spec.cwd,
                env=self._build_env(spec.env),
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            self._procs[role] = proc
            self._logger.event(
                "process_started",
                role=role,
                pid=proc.pid,
                command=spec.command,
            )

    def stop(self) -> None:
        for role, proc in list(self._procs.items()):
            self._stop_one(role, proc)
        self._procs.clear()

    def status(self) -> Dict[str, str]:
        result: Dict[str, str] = {}
        for role, proc in self._procs.items():
            result[role] = "running" if proc.poll() is None else "stopped"
        return result

    def pids(self) -> Dict[str, int]:
        return {role: proc.pid for role, proc in self._procs.items()}

    def _build_env(self, extra: Optional[Mapping[str, str]]) -> Mapping[str, str]:
        env = os.environ.copy()
        if extra:
            env.update(extra)
        return env

    def _stop_one(self, role: str, proc: subprocess.Popen[bytes]) -> None:
        if proc.poll() is not None:
            self._logger.event("process_already_stopped", role=role, pid=proc.pid)
            if proc.stdin is not None and not proc.stdin.closed:
                proc.stdin.close()
            return

        proc.terminate()
        self._logger.event("process_terminate_sent", role=role, pid=proc.pid)
        try:
            proc.wait(timeout=self._stop_timeout_sec)
            self._logger.event(
                "process_stopped",
                role=role,
                pid=proc.pid,
                returncode=proc.returncode,
            )
            return
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=self._stop_timeout_sec)
            self._logger.event(
                "process_killed",
                role=role,
                pid=proc.pid,
                returncode=proc.returncode,
            )
        finally:
            if proc.stdin is not None and not proc.stdin.closed:
                proc.stdin.close()
