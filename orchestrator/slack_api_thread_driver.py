"""Shared Slack API edge driver."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Any

from orchestrator.slack_smoke import SlackApi


@dataclass
class _SlackApiCommand:
    method: str
    kwargs: dict[str, Any]
    response_queue: Queue[tuple[bool, Any]] | None


class SlackApiThreadDriver:
    """Serializes all Slack API calls through one dedicated edge thread."""

    def __init__(self, client: SlackApi) -> None:
        self._client = client
        self._queue: Queue[_SlackApiCommand | None] = Queue()
        self._thread = threading.Thread(
            target=self._run,
            name="slack-api-thread",
            daemon=True,
        )
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._thread.start()

    def stop(self) -> None:
        if not self._started:
            return
        self._queue.put(None)
        self._thread.join(timeout=2.0)
        self._started = False

    def auth_test(self) -> dict:
        return self._call("auth_test")

    def conversations_history(
        self,
        *,
        channel_id: str,
        oldest: str | None = None,
        limit: int = 20,
    ) -> list[dict]:
        return self._call(
            "conversations_history",
            channel_id=channel_id,
            oldest=oldest,
            limit=limit,
        )

    def post_message(
        self,
        *,
        channel_id: str,
        text: str,
        thread_ts: str | None = None,
        blocks: list[dict] | None = None,
    ) -> dict:
        return self._call(
            "post_message",
            channel_id=channel_id,
            text=text,
            thread_ts=thread_ts,
            blocks=blocks,
        )

    def update_message(
        self,
        *,
        channel_id: str,
        ts: str,
        text: str,
        blocks: list[dict] | None = None,
    ) -> dict:
        return self._call(
            "update_message",
            channel_id=channel_id,
            ts=ts,
            text=text,
            blocks=blocks,
        )

    def delete_message(
        self,
        *,
        channel_id: str,
        ts: str,
    ) -> dict:
        return self._call(
            "delete_message",
            channel_id=channel_id,
            ts=ts,
        )

    def _call(self, method: str, **kwargs: Any) -> Any:
        reply_queue: Queue[tuple[bool, Any]] = Queue(maxsize=1)
        self._queue.put(_SlackApiCommand(method=method, kwargs=kwargs, response_queue=reply_queue))
        ok, payload = reply_queue.get()
        if ok:
            return payload
        raise payload

    def _run(self) -> None:
        while True:
            try:
                command = self._queue.get(timeout=0.1)
            except Empty:
                continue
            if command is None:
                break
            try:
                method = getattr(self._client, command.method)
                result = method(**command.kwargs)
            except Exception as exc:
                if command.response_queue is not None:
                    command.response_queue.put((False, exc))
            else:
                if command.response_queue is not None:
                    command.response_queue.put((True, result))
