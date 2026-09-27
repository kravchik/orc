"""Shared Slack API edge driver."""

from __future__ import annotations

from typing import Any

from orchestrator.serialized_call_lane import SerializedCallLane
from orchestrator.slack_smoke import SlackApi


class SlackApiThreadDriver:
    """Serializes all Slack API calls through one dedicated edge thread."""

    def __init__(self, client: SlackApi) -> None:
        self._lane = SerializedCallLane(
            client,
            thread_name="slack-api-thread",
        )

    def start(self) -> None:
        self._lane.start()

    def stop(self) -> None:
        self._lane.stop()

    def auth_test(self) -> dict:
        return self._call("auth_test")

    def download_file(self, url: str, *, max_bytes: int) -> bytes:
        result = self._call("download_file", url=url, max_bytes=max_bytes)
        if not isinstance(result, bytes):
            raise RuntimeError("slack download_file did not return bytes")
        return result

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
        mrkdwn: bool | None = None,
    ) -> dict:
        kwargs: dict[str, Any] = {
            "channel_id": channel_id,
            "text": text,
            "thread_ts": thread_ts,
            "blocks": blocks,
        }
        if mrkdwn is not None:
            kwargs["mrkdwn"] = mrkdwn
        return self._call(
            "post_message",
            **kwargs,
        )

    def update_message(
        self,
        *,
        channel_id: str,
        ts: str,
        text: str,
        blocks: list[dict] | None = None,
        parse: str | None = None,
    ) -> dict:
        kwargs: dict[str, Any] = {
            "channel_id": channel_id,
            "ts": ts,
            "text": text,
            "blocks": blocks,
        }
        if parse is not None:
            kwargs["parse"] = parse
        return self._call(
            "update_message",
            **kwargs,
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
        return self._lane.call(method, **kwargs)
