from __future__ import annotations

import threading
from dataclasses import dataclass
from queue import Queue
from typing import Any


@dataclass(frozen=True)
class _Call:
    method: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    response: Queue[tuple[bool, Any]]


class SerializedCallLane:
    """Runs synchronous calls serially on one restartable edge thread."""

    def __init__(
        self,
        target: object,
        *,
        thread_name: str,
        join_timeout_sec: float = 2.0,
    ) -> None:
        self._target = target
        self._thread_name = thread_name
        self._join_timeout_sec = max(0.0, float(join_timeout_sec))
        self._queue: Queue[_Call | None] = Queue()
        self._thread: threading.Thread | None = None
        self._started = False
        self._stop_requested = False
        self._lifecycle_lock = threading.Lock()

    def start(self) -> None:
        with self._lifecycle_lock:
            if self._started:
                return
            self._stop_requested = False
            self._thread = threading.Thread(
                target=self._run,
                name=self._thread_name,
                daemon=True,
            )
            self._started = True
            self._thread.start()

    def stop(self) -> None:
        with self._lifecycle_lock:
            if not self._started:
                return
            thread = self._thread
            if thread is None:
                raise RuntimeError(f"{self._thread_name} has no worker thread")
            if not self._stop_requested and thread.is_alive():
                self._stop_requested = True
                self._queue.put(None)

        thread.join(timeout=self._join_timeout_sec)
        if thread.is_alive():
            raise RuntimeError(
                f"{self._thread_name} did not stop within {self._join_timeout_sec:.2f}s"
            )

        with self._lifecycle_lock:
            if self._thread is thread:
                self._thread = None
                self._started = False
                self._stop_requested = False

    def call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        with self._lifecycle_lock:
            if not self._started or self._stop_requested:
                raise RuntimeError(f"{self._thread_name} is not started")
            response: Queue[tuple[bool, Any]] = Queue(maxsize=1)
            self._queue.put(
                _Call(
                    method=method,
                    args=tuple(args),
                    kwargs=dict(kwargs),
                    response=response,
                )
            )

        ok, payload = response.get()
        if ok:
            return payload
        raise payload

    def _run(self) -> None:
        while True:
            call = self._queue.get()
            if call is None:
                return
            try:
                method = getattr(self._target, call.method)
                result = method(*call.args, **call.kwargs)
            except BaseException as exc:
                call.response.put((False, exc))
            else:
                call.response.put((True, result))
