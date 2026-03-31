"""Minimal JSON-RPC over stdio client used by adapter and smoke tests."""

from __future__ import annotations

import itertools
import json
import os
import selectors
import subprocess
from typing import Callable, Mapping, Optional


MessageCallback = Callable[[str, dict], None]


class StdioJsonRpcClient:
    def __init__(
        self,
        command: list[str],
        cwd: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        on_message: Optional[MessageCallback] = None,
    ) -> None:
        self._command = command
        self._cwd = cwd
        self._env = env
        self._on_message = on_message
        self._proc: Optional[subprocess.Popen[bytes]] = None
        self._selector: Optional[selectors.BaseSelector] = None
        self._read_buffer = b""
        self._id_counter = itertools.count(1)

    @property
    def pid(self) -> Optional[int]:
        if self._proc is None:
            return None
        return self._proc.pid

    @property
    def returncode(self) -> Optional[int]:
        if self._proc is None:
            return None
        return self._proc.returncode

    def start(self) -> None:
        self._proc = subprocess.Popen(
            self._command,
            cwd=self._cwd,
            env=self._env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            bufsize=0,
        )
        self._selector = selectors.DefaultSelector()
        self._selector.register(self._proc.stdout, selectors.EVENT_READ)

    def stop(self) -> None:
        if self._selector is not None:
            self._selector.close()
            self._selector = None
        if self._proc is None:
            return
        if self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=2.0)
        if self._proc.stdin is not None and not self._proc.stdin.closed:
            self._proc.stdin.close()
        if self._proc.stdout is not None and not self._proc.stdout.closed:
            self._proc.stdout.close()
        self._proc = None
        self._read_buffer = b""

    def send(self, payload: dict) -> None:
        if self._proc is None or self._proc.stdin is None:
            raise RuntimeError("json-rpc process is not started")
        self._proc.stdin.write((json.dumps(payload, ensure_ascii=True) + "\n").encode("utf-8"))
        self._proc.stdin.flush()

    def send_request(self, method: str, params: dict) -> int:
        req_id = next(self._id_counter)
        self.send({"jsonrpc": "2.0", "id": req_id, "method": method, "params": params})
        return req_id

    def send_notification(self, method: str, params: dict) -> None:
        self.send({"jsonrpc": "2.0", "method": method, "params": params})

    def send_response(self, req_id: int, result: dict) -> None:
        self.send({"jsonrpc": "2.0", "id": req_id, "result": result})

    def send_error(
        self,
        req_id: int,
        *,
        code: int,
        message: str,
        data: dict | None = None,
    ) -> None:
        error_payload: dict = {"code": int(code), "message": str(message)}
        if data is not None:
            error_payload["data"] = data
        self.send({"jsonrpc": "2.0", "id": req_id, "error": error_payload})

    def read_message(self, timeout_sec: float) -> Optional[dict]:
        line = self._read_line(timeout_sec=timeout_sec)
        if line is None:
            return None
        msg = json.loads(line)
        if self._on_message is not None:
            self._on_message(line, msg)
        return msg

    def _read_line(self, timeout_sec: float) -> Optional[str]:
        if self._selector is None or self._proc is None or self._proc.stdout is None:
            raise RuntimeError("json-rpc process is not started")

        if b"\n" in self._read_buffer:
            line_bytes, self._read_buffer = self._read_buffer.split(b"\n", 1)
            return line_bytes.decode("utf-8")

        events = self._selector.select(timeout=timeout_sec)
        if not events:
            return None

        chunk = os.read(self._proc.stdout.fileno(), 4096)
        if chunk == b"":
            raise RuntimeError("json-rpc stdout closed")
        self._read_buffer += chunk
        if b"\n" not in self._read_buffer:
            return None
        line_bytes, self._read_buffer = self._read_buffer.split(b"\n", 1)
        return line_bytes.decode("utf-8")
