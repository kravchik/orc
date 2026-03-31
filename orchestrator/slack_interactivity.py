from __future__ import annotations

import hmac
import json
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Protocol
from urllib.parse import parse_qs

import websocket

from orchestrator.processes import LifecycleLogger


@dataclass(frozen=True)
class SlackInteractivityAction:
    channel_id: str
    thread_ts: str | None
    message_ts: str | None
    user_id: str | None
    action_id: str
    value: str
    raw_type: str


class SlackInboundSource(Protocol):
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def receives_messages(self) -> bool: ...
    def poll_messages(self, *, limit: int = 20) -> list[dict[str, Any]]: ...
    def poll_actions(self, *, limit: int = 20) -> list[SlackInteractivityAction]: ...


class SlackInteractivitySource(SlackInboundSource, Protocol):
    pass


class SlackInteractivityIngress:
    def __init__(
        self,
        *,
        signing_secret: str,
        path: str = "/slack/interactivity",
        now: Any = None,
        tolerance_sec: int = 300,
    ) -> None:
        self._signing_secret = str(signing_secret or "")
        self._path = path if str(path or "").startswith("/") else f"/{path}"
        self._now = now or time.time
        self._tolerance_sec = max(1, int(tolerance_sec))
        self._queue: Queue[SlackInteractivityAction] = Queue()

    @property
    def path(self) -> str:
        return self._path

    def receives_messages(self) -> bool:
        return False

    def poll_messages(self, *, limit: int = 20) -> list[dict[str, Any]]:
        _ = limit
        return []

    def poll_actions(self, *, limit: int = 20) -> list[SlackInteractivityAction]:
        out: list[SlackInteractivityAction] = []
        max_items = max(1, int(limit))
        while len(out) < max_items:
            try:
                out.append(self._queue.get_nowait())
            except Empty:
                break
        return out

    def handle_http_request(
        self,
        *,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes,
    ) -> tuple[int, bytes, str]:
        if method.upper() != "POST":
            return (405, b"method not allowed", "text/plain; charset=utf-8")
        if path != self._path:
            return (404, b"not found", "text/plain; charset=utf-8")
        if not self._verify_signature(headers=headers, body=body):
            return (401, b"invalid slack signature", "text/plain; charset=utf-8")
        payload = self._decode_payload(body)
        if payload is None:
            return (400, b"invalid payload", "text/plain; charset=utf-8")
        payload_type = str(payload.get("type") or "")
        if payload_type == "url_verification":
            challenge = str(payload.get("challenge") or "")
            return (200, challenge.encode("utf-8"), "text/plain; charset=utf-8")
        if payload_type != "block_actions":
            return (200, b"ok", "text/plain; charset=utf-8")
        self._enqueue_block_actions(payload)
        return (200, b"", "text/plain; charset=utf-8")

    def _verify_signature(self, *, headers: dict[str, str], body: bytes) -> bool:
        if not self._signing_secret:
            return False
        timestamp = str(headers.get("X-Slack-Request-Timestamp") or headers.get("x-slack-request-timestamp") or "")
        signature = str(headers.get("X-Slack-Signature") or headers.get("x-slack-signature") or "")
        if not timestamp or not signature:
            return False
        try:
            timestamp_int = int(timestamp)
        except ValueError:
            return False
        now_value = int(float(self._now()))
        if abs(now_value - timestamp_int) > self._tolerance_sec:
            return False
        basestring = b"v0:" + timestamp.encode("utf-8") + b":" + body
        expected = "v0=" + hmac.new(
            self._signing_secret.encode("utf-8"),
            basestring,
            sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, signature)

    def _decode_payload(self, body: bytes) -> dict[str, Any] | None:
        try:
            parsed = parse_qs(body.decode("utf-8"), keep_blank_values=True)
        except UnicodeDecodeError:
            return None
        raw_payload = parsed.get("payload")
        if not raw_payload:
            try:
                direct = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                return None
            return direct if isinstance(direct, dict) else None
        try:
            payload = json.loads(raw_payload[0])
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None

    def _enqueue_block_actions(self, payload: dict[str, Any]) -> None:
        for action in _block_actions_from_payload(payload):
            self._queue.put(action)


class SlackInteractivityHttpServer(SlackInteractivitySource):
    def __init__(
        self,
        *,
        signing_secret: str,
        host: str,
        port: int,
        path: str = "/slack/interactivity",
    ) -> None:
        self._ingress = SlackInteractivityIngress(signing_secret=signing_secret, path=path)
        self._host = str(host)
        self._port = int(port)
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def path(self) -> str:
        return self._ingress.path

    def start(self) -> None:
        if self._httpd is not None:
            return
        ingress = self._ingress

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("Content-Length") or "0")
                body = self.rfile.read(length)
                status, response_body, content_type = ingress.handle_http_request(
                    method="POST",
                    path=self.path,
                    headers={k: v for k, v in self.headers.items()},
                    body=body,
                )
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(response_body)))
                self.end_headers()
                if response_body:
                    self.wfile.write(response_body)

            def log_message(self, format: str, *args: object) -> None:  # noqa: A003
                _ = (format, args)
                return

        self._httpd = ThreadingHTTPServer((self._host, self._port), _Handler)
        self._thread = threading.Thread(
            target=self._httpd.serve_forever,
            name="slack-interactivity-http",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        httpd = self._httpd
        thread = self._thread
        self._httpd = None
        self._thread = None
        if httpd is None:
            return
        httpd.shutdown()
        httpd.server_close()
        if thread is not None:
            thread.join(timeout=2.0)

    def receives_messages(self) -> bool:
        return self._ingress.receives_messages()

    def poll_messages(self, *, limit: int = 20) -> list[dict[str, Any]]:
        return self._ingress.poll_messages(limit=limit)

    def poll_actions(self, *, limit: int = 20) -> list[SlackInteractivityAction]:
        return self._ingress.poll_actions(limit=limit)


class SlackSocketModeSource(SlackInboundSource):
    def __init__(
        self,
        *,
        app_token: str,
        api_base_url: str | None = None,
        reconnect_delay_sec: float = 1.0,
        open_timeout_sec: float = 30.0,
        logger: LifecycleLogger | None = None,
    ) -> None:
        self._app_token = str(app_token or "")
        self._api_base_url = (api_base_url or "https://slack.com/api").rstrip("/")
        self._reconnect_delay_sec = max(0.1, float(reconnect_delay_sec))
        self._open_timeout_sec = max(1.0, float(open_timeout_sec))
        self._logger = logger
        self._message_queue: Queue[dict[str, Any]] = Queue()
        self._action_queue: Queue[SlackInteractivityAction] = Queue()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._ws: websocket.WebSocket | None = None
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._stop.clear()
        self._event("slack_socket_mode_starting")
        self._thread = threading.Thread(target=self._run, name="slack-socket-mode", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        self._stop.set()
        self._event("slack_socket_mode_stopping")
        ws = self._ws
        self._ws = None
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        self._thread = None
        self._event("slack_socket_mode_stopped")

    def receives_messages(self) -> bool:
        return True

    def poll_messages(self, *, limit: int = 20) -> list[dict[str, Any]]:
        return _drain_queue(self._message_queue, limit=limit)

    def poll_actions(self, *, limit: int = 20) -> list[SlackInteractivityAction]:
        return _drain_queue(self._action_queue, limit=limit)

    def handle_socket_envelope(self, payload: dict[str, Any], *, ack: Any = None) -> None:
        envelope_id = payload.get("envelope_id")
        if isinstance(envelope_id, str) and envelope_id.strip() and ack is not None:
            ack(envelope_id.strip())
        envelope_type = str(payload.get("type") or "")
        self._event(
            "slack_socket_mode_envelope_received",
            envelope_type=envelope_type,
            envelope_id=(envelope_id.strip() if isinstance(envelope_id, str) else None),
        )
        if envelope_type == "interactive":
            inner = payload.get("payload")
            if isinstance(inner, dict):
                for action in _block_actions_from_payload(inner):
                    self._action_queue.put(action)
                    self._event(
                        "slack_socket_mode_action_received",
                        channel_id=action.channel_id,
                        thread_ts=action.thread_ts,
                        message_ts=action.message_ts,
                        action_id=action.action_id,
                        value=action.value,
                    )
            return
        if envelope_type == "events_api":
            inner = payload.get("payload")
            if not isinstance(inner, dict):
                return
            event = inner.get("event")
            message = _message_event_to_history_message(event)
            if message is not None:
                self._message_queue.put(message)
                self._event(
                    "slack_socket_mode_message_received",
                    channel_id=message.get("channel"),
                    thread_ts=message.get("thread_ts"),
                    message_ts=message.get("ts"),
                    user=message.get("user"),
                )
            return

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._event("slack_socket_mode_connecting")
                ws_url = self._open_socket_url()
                ws = websocket.create_connection(ws_url, timeout=self._open_timeout_sec)
                self._ws = ws
                self._event("slack_socket_mode_connected")
                while not self._stop.is_set():
                    raw = ws.recv()
                    if raw is None:
                        self._event("slack_socket_mode_connection_closed")
                        break
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8")
                    try:
                        payload = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(payload, dict):
                        continue
                    self.handle_socket_envelope(payload, ack=lambda eid: _ack_socket_envelope(ws, eid))
            except Exception as exc:
                self._event(
                    "slack_socket_mode_error",
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                if self._stop.is_set():
                    break
                self._event(
                    "slack_socket_mode_reconnect_scheduled",
                    reconnect_delay_sec=self._reconnect_delay_sec,
                )
                time.sleep(self._reconnect_delay_sec)
            finally:
                ws = self._ws
                self._ws = None
                if ws is not None:
                    try:
                        ws.close()
                    except Exception:
                        pass

    def _event(self, event: str, **fields: object) -> None:
        if self._logger is None:
            return
        self._logger.event(event, **fields)

    def _open_socket_url(self) -> str:
        if not self._app_token:
            raise RuntimeError("Slack app token is required for Socket Mode")
        body = b"{}"
        req = urllib.request.Request(
            f"{self._api_base_url}/apps.connections.open",
            data=body,
            headers={
                "Authorization": f"Bearer {self._app_token}",
                "Content-Type": "application/json; charset=utf-8",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self._open_timeout_sec) as resp:
                raw = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"slack apps.connections.open http error: {exc.code}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"slack apps.connections.open network error: {exc.reason}") from exc
        try:
            response = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError("slack apps.connections.open invalid json response") from exc
        if response.get("ok") is not True:
            raise RuntimeError(f"slack apps.connections.open failed: {response.get('error', 'unknown_error')}")
        url = response.get("url")
        if not isinstance(url, str) or not url.strip():
            raise RuntimeError("slack apps.connections.open missing socket url")
        return url.strip()


def _drain_queue(queue: Queue[Any], *, limit: int) -> list[Any]:
    out: list[Any] = []
    max_items = max(1, int(limit))
    while len(out) < max_items:
        try:
            out.append(queue.get_nowait())
        except Empty:
            break
    return out


def _ack_socket_envelope(ws: websocket.WebSocket, envelope_id: str) -> None:
    ws.send(json.dumps({"envelope_id": envelope_id}, ensure_ascii=True))


def _block_actions_from_payload(payload: dict[str, Any]) -> list[SlackInteractivityAction]:
    out: list[SlackInteractivityAction] = []
    channel = payload.get("channel")
    user = payload.get("user")
    message = payload.get("message")
    actions = payload.get("actions")
    if not isinstance(channel, dict) or not isinstance(actions, list):
        return out
    channel_id = str(channel.get("id") or "").strip()
    if not channel_id:
        return out
    user_id = None
    if isinstance(user, dict):
        raw_user_id = user.get("id")
        if isinstance(raw_user_id, str) and raw_user_id.strip():
            user_id = raw_user_id.strip()
    message_ts = None
    thread_ts = None
    if isinstance(message, dict):
        raw_message_ts = message.get("ts")
        if isinstance(raw_message_ts, str) and raw_message_ts.strip():
            message_ts = raw_message_ts.strip()
        raw_thread_ts = message.get("thread_ts")
        if isinstance(raw_thread_ts, str) and raw_thread_ts.strip():
            thread_ts = raw_thread_ts.strip()
        elif message_ts is not None:
            thread_ts = message_ts
    for raw_action in actions:
        if not isinstance(raw_action, dict):
            continue
        action_id = str(raw_action.get("action_id") or "").strip()
        value = str(raw_action.get("value") or "").strip()
        if not action_id or not value:
            continue
        out.append(
            SlackInteractivityAction(
                channel_id=channel_id,
                thread_ts=thread_ts,
                message_ts=message_ts,
                user_id=user_id,
                action_id=action_id,
                value=value,
                raw_type=str(payload.get("type") or ""),
            )
        )
    return out


def _message_event_to_history_message(event: Any) -> dict[str, Any] | None:
    if not isinstance(event, dict):
        return None
    if str(event.get("type") or "") != "message":
        return None
    subtype = event.get("subtype")
    if isinstance(subtype, str) and subtype.strip():
        return None
    channel = str(event.get("channel") or "").strip()
    ts = str(event.get("ts") or "").strip()
    text = event.get("text")
    user = event.get("user")
    if not channel or not ts or not isinstance(text, str) or not isinstance(user, str):
        return None
    thread_ts = event.get("thread_ts")
    payload: dict[str, Any] = {
        "channel": channel,
        "ts": ts,
        "text": text,
        "user": user,
    }
    if isinstance(thread_ts, str) and thread_ts.strip():
        payload["thread_ts"] = thread_ts.strip()
    return payload
