"""JSON-RPC over stdio with explicit transport progress."""

from __future__ import annotations

import itertools
import json
import os
import re
import selectors
import subprocess
import time
from dataclasses import dataclass
from typing import Callable, Literal, Mapping, Optional


MessageCallback = Callable[[str, dict], None]
StderrCallback = Callable[[str], None]
ReadKind = Literal["idle", "progressed", "message", "eof", "malformed"]

DEFAULT_READ_CHUNK_BYTES = 64 * 1024
DEFAULT_READ_BUDGET_BYTES = 256 * 1024
DEFAULT_READ_BUDGET_SEC = 0.005
STDERR_LINE_MAX_CHARS = 1000
STDERR_LINES_PER_WINDOW = 10
STDERR_WINDOW_SEC = 10.0


@dataclass(frozen=True)
class JsonRpcReadResult:
    kind: ReadKind
    message: dict | None = None
    bytes_read: int = 0
    wire_bytes: int = 0
    payload_bytes: int = 0
    buffered_bytes: int = 0
    error: str = ""

    @property
    def progressed(self) -> bool:
        return self.kind != "idle"

    @property
    def terminal(self) -> bool:
        return self.kind in {"eof", "malformed"}


class JsonlFrameDecoder:
    """Pure JSONL frame state machine used by the stdio transport."""

    def __init__(self) -> None:
        self._buffer = b""

    @property
    def buffered_bytes(self) -> int:
        return len(self._buffer)

    def reset(self) -> None:
        self._buffer = b""

    def consume(self, chunk: bytes | None) -> JsonRpcReadResult:
        if chunk == b"":
            return JsonRpcReadResult(
                kind="eof",
                buffered_bytes=len(self._buffer),
                error=("json-rpc stdout closed with a partial frame" if self._buffer else "json-rpc stdout closed"),
            )

        bytes_read = 0
        if chunk is not None:
            self._buffer += chunk
            bytes_read = len(chunk)

        if b"\n" not in self._buffer:
            return JsonRpcReadResult(
                kind="progressed" if bytes_read else "idle",
                bytes_read=bytes_read,
                buffered_bytes=len(self._buffer),
            )

        line_bytes, self._buffer = self._buffer.split(b"\n", 1)
        try:
            decoded = line_bytes.decode("utf-8")
            message = json.loads(decoded)
            if not isinstance(message, dict):
                raise ValueError("JSON-RPC frame must be an object")
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            return JsonRpcReadResult(
                kind="malformed",
                bytes_read=bytes_read,
                wire_bytes=len(line_bytes) + 1,
                payload_bytes=len(line_bytes),
                buffered_bytes=len(self._buffer),
                error=f"JSON frame decode failed: {exc}",
            )
        return JsonRpcReadResult(
            kind="message",
            message=message,
            bytes_read=bytes_read,
            wire_bytes=len(line_bytes) + 1,
            payload_bytes=len(line_bytes),
            buffered_bytes=len(self._buffer),
        )


def sanitize_stderr_text(text: str) -> str:
    sanitized = str(text).replace("\x00", "")
    patterns = (
        (re.compile(r"(?i)(authorization\s*:\s*bearer\s+)[^\s]+"), r"\1[REDACTED]"),
        (re.compile(r"(?i)((?:api[_-]?key|token|secret|password)\s*[:=]\s*)[^\s]+"), r"\1[REDACTED]"),
        (re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b"), "[REDACTED]"),
    )
    for pattern, replacement in patterns:
        sanitized = pattern.sub(replacement, sanitized)
    return sanitized[:STDERR_LINE_MAX_CHARS]


class _BoundedStderrReporter:
    def __init__(self, callback: StderrCallback | None) -> None:
        self._callback = callback
        self._buffer = b""
        self._window_started_at = time.monotonic()
        self._emitted = 0
        self._suppressed = 0

    def consume(self, chunk: bytes) -> None:
        if not chunk:
            return
        self._buffer = (self._buffer + chunk)[-64 * 1024 :]
        while b"\n" in self._buffer:
            raw_line, self._buffer = self._buffer.split(b"\n", 1)
            self._emit(raw_line.decode("utf-8", errors="replace"))

    def flush(self) -> None:
        if self._buffer:
            self._emit(self._buffer.decode("utf-8", errors="replace"))
            self._buffer = b""
        self._rotate_window(force=True)

    def _emit(self, line: str) -> None:
        self._rotate_window(force=False)
        if self._callback is None:
            return
        if self._emitted >= STDERR_LINES_PER_WINDOW:
            self._suppressed += 1
            return
        self._emitted += 1
        self._callback(sanitize_stderr_text(line))

    def _rotate_window(self, *, force: bool) -> None:
        now = time.monotonic()
        if not force and now - self._window_started_at < STDERR_WINDOW_SEC:
            return
        if self._callback is not None and self._suppressed:
            self._callback(f"[suppressed {self._suppressed} app-server stderr lines]")
        self._window_started_at = now
        self._emitted = 0
        self._suppressed = 0


class StdioJsonRpcClient:
    def __init__(
        self,
        command: list[str],
        cwd: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        on_message: Optional[MessageCallback] = None,
        on_stderr: Optional[StderrCallback] = None,
        read_budget_bytes: int = DEFAULT_READ_BUDGET_BYTES,
        read_budget_sec: float = DEFAULT_READ_BUDGET_SEC,
    ) -> None:
        self._command = command
        self._cwd = cwd
        self._env = env
        self._on_message = on_message
        self._on_stderr = on_stderr
        self._proc: Optional[subprocess.Popen[bytes]] = None
        self._selector: Optional[selectors.BaseSelector] = None
        self._decoder = JsonlFrameDecoder()
        self._stderr_reporter = _BoundedStderrReporter(on_stderr)
        self._read_budget_bytes = max(DEFAULT_READ_CHUNK_BYTES, int(read_budget_bytes))
        self._read_budget_sec = max(0.001, float(read_budget_sec))
        self._id_counter = itertools.count(1)
        self._last_stop_escalated_to_kill = False

    @property
    def pid(self) -> Optional[int]:
        if self._proc is None:
            return None
        return self._proc.pid

    @property
    def returncode(self) -> Optional[int]:
        if self._proc is None:
            return None
        return self._proc.poll()

    @property
    def last_stop_escalated_to_kill(self) -> bool:
        return self._last_stop_escalated_to_kill

    def start(self) -> None:
        self._proc = subprocess.Popen(
            self._command,
            cwd=self._cwd,
            env=self._env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        if self._proc.stdout is None or self._proc.stderr is None:
            raise RuntimeError("json-rpc process did not expose stdout/stderr")
        os.set_blocking(self._proc.stdout.fileno(), False)
        os.set_blocking(self._proc.stderr.fileno(), False)
        self._selector = selectors.DefaultSelector()
        self._selector.register(self._proc.stdout, selectors.EVENT_READ, data="stdout")
        self._selector.register(self._proc.stderr, selectors.EVENT_READ, data="stderr")

    def stop(self) -> None:
        self._last_stop_escalated_to_kill = False
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
                self._last_stop_escalated_to_kill = True
                self._proc.kill()
                self._proc.wait(timeout=2.0)
        self._stderr_reporter.flush()
        for stream in (self._proc.stdin, self._proc.stdout, self._proc.stderr):
            if stream is not None and not stream.closed:
                stream.close()
        self._proc = None
        self._decoder.reset()

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

    def poll_message(self, timeout_sec: float) -> JsonRpcReadResult:
        if self._selector is None or self._proc is None:
            raise RuntimeError("json-rpc process is not started")

        buffered = self._decoder.consume(None)
        if buffered.kind != "idle":
            return self._notify(buffered)

        events = self._selector.select(timeout=max(0.0, float(timeout_sec)))
        if not events:
            return buffered

        started_at = time.monotonic()
        total_read = 0
        progressed = False
        pending_events = list(events)
        while pending_events and total_read < self._read_budget_bytes:
            for key, _mask in pending_events:
                remaining = self._read_budget_bytes - total_read
                if remaining <= 0:
                    break
                try:
                    chunk = os.read(key.fileobj.fileno(), min(DEFAULT_READ_CHUNK_BYTES, remaining))
                except BlockingIOError:
                    continue
                if key.data == "stderr":
                    if chunk == b"":
                        self._selector.unregister(key.fileobj)
                        continue
                    total_read += len(chunk)
                    progressed = True
                    self._stderr_reporter.consume(chunk)
                    continue
                result = self._decoder.consume(chunk)
                total_read += len(chunk)
                progressed = result.progressed or progressed
                if result.kind in {"message", "eof", "malformed"}:
                    return self._notify(
                        JsonRpcReadResult(
                            kind=result.kind,
                            message=result.message,
                            bytes_read=total_read,
                            wire_bytes=result.wire_bytes,
                            payload_bytes=result.payload_bytes,
                            buffered_bytes=result.buffered_bytes,
                            error=result.error,
                        )
                    )
            if time.monotonic() - started_at >= self._read_budget_sec:
                break
            pending_events = list(self._selector.select(timeout=0.0))

        return JsonRpcReadResult(
            kind="progressed" if progressed else "idle",
            bytes_read=total_read,
            buffered_bytes=self._decoder.buffered_bytes,
        )

    def read_message(self, timeout_sec: float) -> Optional[dict]:
        result = self.poll_message(timeout_sec=timeout_sec)
        if result.kind == "message":
            return result.message
        if result.terminal:
            raise RuntimeError(result.error)
        return None

    def _notify(self, result: JsonRpcReadResult) -> JsonRpcReadResult:
        if result.kind == "message" and result.message is not None and self._on_message is not None:
            self._on_message("", result.message)
        return result
