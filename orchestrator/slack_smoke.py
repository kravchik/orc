"""Minimal Slack polling smoke bot (no agent backend)."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable, Optional

from orchestrator.processes import LifecycleLogger
from orchestrator.slack_interactivity import SlackInboundSource, SlackSocketModeSource


Writer = Callable[[str], None]


class SlackApi:
    def __init__(self, token: str, api_base_url: str | None = None) -> None:
        self._token = token
        self._base = (api_base_url or "https://slack.com/api").rstrip("/")

    def auth_test(self) -> dict:
        return self._post_json("auth.test", {})

    def conversations_history(
        self,
        *,
        channel_id: str,
        oldest: str | None = None,
        limit: int = 20,
    ) -> list[dict]:
        payload: dict[str, object] = {"channel": channel_id, "limit": int(limit)}
        if oldest:
            payload["oldest"] = oldest
            payload["inclusive"] = False
        response = self._post_json("conversations.history", payload)
        messages = response.get("messages")
        if not isinstance(messages, list):
            return []
        return [msg for msg in messages if isinstance(msg, dict)]

    def post_message(
        self,
        *,
        channel_id: str,
        text: str,
        thread_ts: str | None = None,
        blocks: list[dict] | None = None,
    ) -> dict:
        payload: dict[str, object] = {"channel": channel_id, "text": text}
        if thread_ts:
            payload["thread_ts"] = thread_ts
        if isinstance(blocks, list) and blocks:
            payload["blocks"] = blocks
        response = self._post_json("chat.postMessage", payload)
        message = response.get("message")
        if isinstance(message, dict):
            return message
        return {}

    def update_message(
        self,
        *,
        channel_id: str,
        ts: str,
        text: str,
        blocks: list[dict] | None = None,
    ) -> dict:
        payload: dict[str, object] = {"channel": channel_id, "ts": ts, "text": text}
        if isinstance(blocks, list):
            payload["blocks"] = blocks
        response = self._post_json("chat.update", payload)
        message = response.get("message")
        if isinstance(message, dict):
            return message
        return {}

    def delete_message(
        self,
        *,
        channel_id: str,
        ts: str,
    ) -> dict:
        payload: dict[str, object] = {"channel": channel_id, "ts": ts}
        return self._post_json("chat.delete", payload)

    def _post_json(self, method: str, payload: dict[str, object]) -> dict:
        body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        req = urllib.request.Request(
            f"{self._base}/{method}",
            data=body,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json; charset=utf-8",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"slack {method} http error: {exc.code}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"slack {method} network error: {exc.reason}") from exc
        try:
            response = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"slack {method} invalid json response") from exc
        if response.get("ok") is not True:
            error_text = str(response.get("error", "unknown_error"))
            raise RuntimeError(f"slack {method} failed: {error_text}")
        return response


def run_slack_smoke(
    *,
    token: str,
    channel_id: str,
    log_path: str,
    poll_interval_sec: float = 2.0,
    api: Optional[SlackApi] = None,
    app_token: str | None = None,
    inbound_source: SlackInboundSource | None = None,
    idle_sleep_sec: float = 0.01,
    writer: Writer = print,
    max_poll_cycles: int | None = None,
) -> int:
    logger = LifecycleLogger(Path(log_path))
    client = api if api is not None else SlackApi(token)
    source = inbound_source
    if source is None and app_token:
        source = SlackSocketModeSource(app_token=app_token, logger=logger)
    bot_user_id: str | None = None
    try:
        auth = client.auth_test()
        raw_user = auth.get("user_id")
        if isinstance(raw_user, str) and raw_user.strip():
            bot_user_id = raw_user.strip()
    except Exception as exc:
        logger.event("slack_auth_test_failed", error=str(exc), error_type=type(exc).__name__)
    last_seen_ts: str | None = None
    poll_cycles = 0
    writer("slack-smoke started; send any text to get 'pong'")
    logger.event(
        "slack_smoke_started",
        channel_id=channel_id,
        socket_mode=source is not None,
        poll_interval_sec=float(poll_interval_sec),
        bot_user_id=bot_user_id,
    )
    try:
        if source is not None:
            source.start()
        while True:
            if source is not None:
                messages = source.poll_messages(limit=20)
            else:
                messages = client.conversations_history(
                    channel_id=channel_id,
                    oldest=last_seen_ts,
                    limit=20,
                )
            poll_cycles += 1
            pending: list[dict] = []
            for message in messages:
                ts = _normalized_ts(message.get("ts"))
                if ts is None:
                    continue
                if last_seen_ts is not None and _ts_to_float(ts) <= _ts_to_float(last_seen_ts):
                    continue
                if _is_bot_message(message=message, bot_user_id=bot_user_id):
                    if last_seen_ts is None or _ts_to_float(ts) > _ts_to_float(last_seen_ts):
                        last_seen_ts = ts
                    continue
                text = message.get("text")
                user = message.get("user")
                if not isinstance(text, str) or not isinstance(user, str):
                    if last_seen_ts is None or _ts_to_float(ts) > _ts_to_float(last_seen_ts):
                        last_seen_ts = ts
                    continue
                pending.append(message)
            pending.sort(key=lambda item: _ts_to_float(str(item.get("ts", "0"))))
            for message in pending:
                ts = _normalized_ts(message.get("ts"))
                if ts is None:
                    continue
                text = str(message.get("text", ""))
                user = str(message.get("user", ""))
                thread_ts_raw = message.get("thread_ts")
                thread_ts = thread_ts_raw if isinstance(thread_ts_raw, str) and thread_ts_raw.strip() else None
                reply = _build_reply(text=text)
                client.post_message(channel_id=channel_id, text=reply, thread_ts=thread_ts)
                logger.event(
                    "slack_response_sent",
                    channel_id=channel_id,
                    user=user,
                    ts=ts,
                    thread_ts=thread_ts,
                    text=reply,
                )
                writer(f"channel={channel_id} user={user} in={text!r} out={reply!r}")
                if last_seen_ts is None or _ts_to_float(ts) > _ts_to_float(last_seen_ts):
                    last_seen_ts = ts
            if max_poll_cycles is not None and poll_cycles >= max_poll_cycles:
                return 0
            if source is not None:
                time.sleep(max(0.0, float(idle_sleep_sec)))
            else:
                time.sleep(max(0.0, float(poll_interval_sec)))
    except KeyboardInterrupt:
        writer("^C")
        writer("slack-smoke stopping gracefully")
        logger.event("slack_smoke_sigint")
        return 130
    except Exception as exc:
        friendly = _humanize_slack_error(exc=exc, channel_id=channel_id)
        logger.event(
            "slack_error",
            error=str(exc),
            error_type=type(exc).__name__,
            friendly_error=friendly,
        )
        writer(f"slack-smoke error: {friendly}")
        raise RuntimeError(friendly) from exc
    finally:
        if source is not None:
            source.stop()
        logger.event("slack_smoke_stopped")


def _is_bot_message(*, message: dict, bot_user_id: str | None) -> bool:
    subtype = message.get("subtype")
    if isinstance(subtype, str) and subtype == "bot_message":
        return True
    bot_id = message.get("bot_id")
    if isinstance(bot_id, str) and bot_id.strip():
        return True
    if bot_user_id is None:
        return False
    user = message.get("user")
    return isinstance(user, str) and user == bot_user_id


def _normalized_ts(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    return text


def _ts_to_float(ts: str) -> float:
    try:
        return float(ts)
    except ValueError:
        return 0.0


def _build_reply(text: str) -> str:
    cmd = text.strip().lower()
    if cmd == "/start":
        return "ORC1 slack-smoke ready. Send any text to get pong. Commands: /start /status /stop"
    if cmd == "/status":
        return "status: running (mode=slack-smoke)"
    if cmd in ("/stop", "/quit"):
        return "stopped"
    return "pong"


def _humanize_slack_error(*, exc: Exception, channel_id: str) -> str:
    raw = str(exc)
    if "conversations.history" in raw and "not_in_channel" in raw:
        return (
            f"Slack bot is not in channel {channel_id}. "
            f"Invite it first (`/invite @botname`) and retry."
        )
    if "missing_scope" in raw:
        return (
            "Slack token is missing required scopes. "
            "Check app OAuth scopes and reinstall app to workspace."
        )
    return raw
