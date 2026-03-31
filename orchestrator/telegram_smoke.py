"""Minimal Telegram polling smoke bot (no agent backend)."""

from __future__ import annotations

import json
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Callable, Optional

from orchestrator.processes import LifecycleLogger


Writer = Callable[[str], None]
MINDWARP_CHAT_ID = 298525854
HARD_ALLOWED_CHAT_IDS = {MINDWARP_CHAT_ID}


class TelegramApi:
    def __init__(
        self,
        token: str,
        insecure_skip_verify: bool = False,
        api_base_url: str | None = None,
    ) -> None:
        self._token = token
        base_root = (api_base_url or "https://api.telegram.org").rstrip("/")
        self._base = f"{base_root}/bot{token}"
        self._ssl_context = (
            ssl._create_unverified_context() if insecure_skip_verify else ssl.create_default_context()
        )

    def get_updates(self, offset: Optional[int], timeout_sec: int) -> list[dict]:
        params = {"timeout": timeout_sec}
        if offset is not None:
            params["offset"] = offset
        payload = self._post_json("getUpdates", params)
        result = payload.get("result")
        if not isinstance(result, list):
            return []
        return [item for item in result if isinstance(item, dict)]

    def send_message(
        self,
        chat_id: int,
        text: str,
        message_thread_id: Optional[int] = None,
        reply_markup: Optional[dict] = None,
        parse_mode: Optional[str] = None,
        disable_notification: Optional[bool] = None,
    ) -> dict:
        payload: dict = {"chat_id": chat_id, "text": text}
        if message_thread_id is not None:
            payload["message_thread_id"] = message_thread_id
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        if parse_mode is not None:
            payload["parse_mode"] = parse_mode
        if disable_notification is not None:
            payload["disable_notification"] = disable_notification
        response = self._post_json("sendMessage", payload)
        result = response.get("result")
        return result if isinstance(result, dict) else {}

    def edit_message_text(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        parse_mode: Optional[str] = None,
    ) -> None:
        payload: dict = {"chat_id": chat_id, "message_id": message_id, "text": text}
        if parse_mode is not None:
            payload["parse_mode"] = parse_mode
        self._post_json("editMessageText", payload)

    def edit_message_reply_markup(
        self,
        chat_id: int,
        message_id: int,
        reply_markup: Optional[dict] = None,
    ) -> None:
        payload: dict = {"chat_id": chat_id, "message_id": message_id}
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        self._post_json("editMessageReplyMarkup", payload)

    def delete_message(self, chat_id: int, message_id: int) -> None:
        payload = {"chat_id": chat_id, "message_id": message_id}
        self._post_json("deleteMessage", payload)

    def answer_callback_query(self, callback_query_id: str, text: Optional[str] = None) -> None:
        payload: dict = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text
        self._post_json("answerCallbackQuery", payload)

    def _post_json(self, method: str, params: dict) -> dict:
        normalized: dict[str, str] = {}
        for key, value in params.items():
            if isinstance(value, (dict, list)):
                normalized[key] = json.dumps(value, ensure_ascii=True)
            else:
                normalized[key] = str(value)
        body = urllib.parse.urlencode(normalized).encode("utf-8")
        req = urllib.request.Request(
            f"{self._base}/{method}",
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=60, context=self._ssl_context) as resp:
                raw = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = ""
            try:
                body = exc.read().decode("utf-8")
            except Exception:
                body = ""
            if body:
                try:
                    payload = json.loads(body)
                    desc = payload.get("description")
                    if isinstance(desc, str) and desc.strip():
                        detail = f": {desc.strip()}"
                    else:
                        detail = f": {body[:240]}"
                except Exception:
                    detail = f": {body[:240]}"
            raise RuntimeError(f"telegram {method} http error: {exc.code}{detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"telegram {method} network error: {exc.reason}") from exc
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"telegram {method} invalid json response") from exc
        if payload.get("ok") is not True:
            raise RuntimeError(f"telegram {method} failed: {payload}")
        return payload


def run_telegram_smoke(
    token: str,
    log_path: str,
    poll_timeout_sec: int = 30,
    allowed_chat_ids: Optional[set[int]] = None,
    insecure_skip_verify: bool = False,
    telegram_api_base_url: str | None = None,
    api: Optional[TelegramApi] = None,
    writer: Writer = print,
    max_poll_cycles: Optional[int] = None,
) -> int:
    if not allowed_chat_ids:
        raise ValueError("allowed_chat_ids must be non-empty")
    logger = LifecycleLogger(Path(log_path))
    client = api if api is not None else TelegramApi(
        token=token,
        insecure_skip_verify=insecure_skip_verify,
        api_base_url=telegram_api_base_url,
    )
    offset: Optional[int] = None
    poll_cycles = 0
    writer("telegram-smoke started; send any text to get 'pong'")
    effective_allowed_chat_ids = (
        set(HARD_ALLOWED_CHAT_IDS)
        if allowed_chat_ids is None
        else set(HARD_ALLOWED_CHAT_IDS).intersection(allowed_chat_ids)
    )
    logger.event(
        "telegram_smoke_started",
        poll_timeout_sec=poll_timeout_sec,
        allowlist_enabled=True,
        effective_allowed_chat_ids=sorted(effective_allowed_chat_ids),
    )
    try:
        while True:
            updates = client.get_updates(offset=offset, timeout_sec=poll_timeout_sec)
            poll_cycles += 1
            for upd in updates:
                upd_id = upd.get("update_id")
                if isinstance(upd_id, int):
                    offset = upd_id + 1
                message = upd.get("message")
                if not isinstance(message, dict):
                    continue
                chat = message.get("chat")
                text = message.get("text")
                if not isinstance(chat, dict) or not isinstance(text, str):
                    continue
                chat_id = chat.get("id")
                if not isinstance(chat_id, int):
                    continue
                logger.event("telegram_update_received", update_id=upd_id, chat_id=chat_id, text=text)
                if chat_id not in effective_allowed_chat_ids:
                    logger.event("telegram_update_ignored", update_id=upd_id, chat_id=chat_id)
                    continue
                reply = _build_reply(text=text, allowlist_enabled=True)
                client.send_message(chat_id=chat_id, text=reply)
                logger.event("telegram_response_sent", chat_id=chat_id, text=reply)
                writer(f"chat_id={chat_id} in={text!r} out={reply!r}")
            if max_poll_cycles is not None and poll_cycles >= max_poll_cycles:
                return 0
    except KeyboardInterrupt:
        writer("^C")
        writer("telegram-smoke stopping gracefully")
        logger.event("telegram_smoke_sigint")
        return 130
    except Exception as exc:
        logger.event("telegram_error", error=str(exc), error_type=type(exc).__name__)
        writer(f"telegram-smoke error: {exc}")
        raise
    finally:
        logger.event("telegram_smoke_stopped")


def _build_reply(text: str, allowlist_enabled: bool) -> str:
    cmd = text.strip().lower()
    if cmd == "/start":
        return "ORC1 telegram-smoke ready. Send any text to get pong. Commands: /start /status /stop"
    if cmd == "/status":
        allowlist = "on" if allowlist_enabled else "off"
        return f"status: running (mode=telegram-smoke, allowlist={allowlist})"
    if cmd in ("/stop", "/quit"):
        return "stopped"
    return "pong"
