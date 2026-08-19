from __future__ import annotations

from typing import Any, Callable

from orchestrator.jsonrpc_stdio import JsonRpcReadResult, StdioJsonRpcClient
from orchestrator.serialized_call_lane import SerializedCallLane


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
        self._lane = SerializedCallLane(client, thread_name=thread_name)
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
        self._lane.start()
        try:
            self._call("start")
        except BaseException:
            self._lane.stop()
            self._started = False
            raise

    def stop(self) -> None:
        if not self._started:
            return
        try:
            self._call("stop")
        finally:
            try:
                self._lane.stop()
            except BaseException:
                raise
            else:
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
        result = self.poll_message(timeout_sec=timeout_sec)
        if result.kind == "message":
            return result.message
        if result.terminal:
            raise RuntimeError(result.error)
        return None

    def poll_message(self, timeout_sec: float) -> JsonRpcReadResult:
        poll = getattr(self._client, "poll_message", None)
        if callable(poll):
            result = self._call("poll_message", timeout_sec=timeout_sec)
            if not isinstance(result, JsonRpcReadResult):
                raise RuntimeError(f"json-rpc poll_message returned invalid result: {result!r}")
        else:
            message = self._call("read_message", timeout_sec=timeout_sec)
            result = JsonRpcReadResult(
                kind="message" if isinstance(message, dict) else "idle",
                message=message if isinstance(message, dict) else None,
            )
        if result.kind == "message" and result.message is not None and self._on_message is not None:
            # The session callback does not consume raw JSON; avoid serializing the
            # potentially huge response a second time on the edge thread.
            self._on_message("", result.message)
        return result

    def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        return self._lane.call(method, *args, **kwargs)


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
