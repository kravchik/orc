from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from queue import Queue
from typing import Any, Callable

from orchestrator.jsonrpc_stdio import StdioJsonRpcClient


@dataclass
class _JsonRpcClientCommand:
    method: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    response_queue: Queue[tuple[bool, Any]] | None


class JsonRpcClientThreadDriver:
    """Serializes direct JSON-RPC client calls through one dedicated edge thread."""

    def __init__(
        self,
        client: object,
        *,
        on_message: Callable[[str, dict], None] | None = None,
        thread_name: str = "jsonrpc-client-thread",
    ) -> None:
        self._client = client
        self._on_message = on_message
        self._thread_name = thread_name
        self._queue: Queue[_JsonRpcClientCommand | None] = Queue()
        self._thread: threading.Thread | None = None
        self._started = False

    @property
    def pid(self) -> int | None:
        raw = getattr(self._client, "pid", None)
        return raw if isinstance(raw, int) else None

    @property
    def returncode(self) -> int | None:
        raw = getattr(self._client, "returncode", None)
        return raw if isinstance(raw, int) else None

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._thread = threading.Thread(
            target=self._run,
            name=self._thread_name,
            daemon=True,
        )
        self._thread.start()
        self._call("start")

    def stop(self) -> None:
        if not self._started:
            return
        try:
            self._call("stop")
        finally:
            self._queue.put(None)
            thread = self._thread
            if thread is not None:
                thread.join(timeout=2.0)
            self._thread = None
            self._started = False

    def send_request(self, method: str, params: dict) -> int:
        result = self._call("send_request", method, params=params)
        if not isinstance(result, int):
            raise RuntimeError(f"json-rpc send_request returned non-int id: {result!r}")
        return result

    def send_notification(self, method: str, params: dict) -> None:
        self._call("send_notification", method, params=params)

    def send_response(self, req_id: int, result: dict) -> None:
        self._call("send_response", req_id, result=result)

    def send_error(
        self,
        req_id: int,
        *,
        code: int,
        message: str,
        data: dict | None = None,
    ) -> None:
        self._call(
            "send_error",
            req_id,
            code=code,
            message=message,
            data=data,
        )

    def read_message(self, timeout_sec: float) -> dict | None:
        result = self._call("read_message", timeout_sec=timeout_sec)
        if isinstance(result, dict) and self._on_message is not None:
            self._on_message(json.dumps(result, ensure_ascii=True), result)
        return result if isinstance(result, dict) or result is None else None

    def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        response_queue: Queue[tuple[bool, Any]] = Queue(maxsize=1)
        self._queue.put(
            _JsonRpcClientCommand(
                method=method,
                args=tuple(args),
                kwargs=dict(kwargs),
                response_queue=response_queue,
            )
        )
        ok, payload = response_queue.get()
        if ok:
            return payload
        raise payload

    def _run(self) -> None:
        while True:
            command = self._queue.get()
            if command is None:
                return
            response_queue = command.response_queue
            try:
                method = getattr(self._client, command.method)
                result = method(*command.args, **command.kwargs)
            except Exception as exc:
                if response_queue is not None:
                    response_queue.put((False, exc))
                continue
            if response_queue is not None:
                response_queue.put((True, result))


def build_threaded_jsonrpc_client_factory(
    *,
    base_factory: Callable[..., object] | None,
    thread_name: str,
) -> Callable[..., object]:
    def _factory(**kwargs: Any) -> object:
        base_ctor = base_factory or StdioJsonRpcClient
        on_message = kwargs.pop("on_message", None)
        base_client = base_ctor(**kwargs)
        return JsonRpcClientThreadDriver(
            base_client,
            on_message=on_message,
            thread_name=thread_name,
        )

    return _factory
