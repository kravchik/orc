"""Shared Telegram bridge helpers for TUI runtimes."""

from __future__ import annotations

from dataclasses import dataclass
import re
from threading import Lock
from typing import Callable

from orchestrator.processes import LifecycleLogger
from orchestrator.telegram_smoke import HARD_ALLOWED_CHAT_IDS, TelegramApi


def truncate_telegram_echo(text: str, limit: int = 3500) -> str:
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def resolve_allowed_chat_ids(allowed_chat_ids: set[int] | None) -> set[int]:
    if allowed_chat_ids is None:
        return set(HARD_ALLOWED_CHAT_IDS)
    return set(HARD_ALLOWED_CHAT_IDS).intersection(allowed_chat_ids)


class TelegramChatState:
    """Thread-safe holder for current Telegram chat id."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._chat_id: int | None = None

    def set_current(self, chat_id: int) -> None:
        with self._lock:
            self._chat_id = chat_id

    def get_current(self) -> int | None:
        with self._lock:
            return self._chat_id


@dataclass(frozen=True)
class TelegramTextUpdate:
    update_id: int | None
    chat_id: int
    message_id: int | None
    thread_id: int | None
    text: str


@dataclass(frozen=True)
class TelegramCallbackUpdate:
    update_id: int | None
    chat_id: int
    message_id: int | None
    thread_id: int | None
    callback_query_id: str | None
    data: str


def maybe_select_default_chat(
    *,
    logger: LifecycleLogger,
    chat_state: TelegramChatState,
    allowed_chat_ids: set[int],
    event_name: str,
    source: str,
) -> int | None:
    if len(allowed_chat_ids) != 1:
        return None
    chat_id = next(iter(allowed_chat_ids))
    chat_state.set_current(chat_id)
    logger.event(
        event_name,
        source=source,
        chat_id=chat_id,
        reason="single_allowed_chat",
    )
    return chat_id


def run_telegram_text_poll_loop(
    *,
    client: TelegramApi,
    logger: LifecycleLogger,
    source: str,
    allowed_chat_ids: set[int] | None,
    poll_timeout_sec: int,
    offset_ref: list[int | None],
    stop_when: Callable[[], bool],
    chat_state: TelegramChatState,
    on_text_update: Callable[[TelegramTextUpdate], None],
    on_callback_update: Callable[[TelegramCallbackUpdate], None] | None = None,
    on_poll_cycle: Callable[[], None] | None = None,
    on_poll_error: Callable[[Exception], bool] | None = None,
) -> None:
    timeout_sec = int(max(1, poll_timeout_sec))
    while not stop_when():
        if on_poll_cycle is not None:
            on_poll_cycle()
        try:
            updates = client.get_updates(offset=offset_ref[0], timeout_sec=timeout_sec)
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            http_code = extract_telegram_http_code(exc)
            if http_code == 409:
                logger.event(
                    "telegram_poll_conflict",
                    source=source,
                    http_code=http_code,
                    error=str(exc),
                )
                return
            if on_poll_error is not None and on_poll_error(exc):
                continue
            raise
        process_telegram_updates(
            updates=updates,
            logger=logger,
            source=source,
            allowed_chat_ids=allowed_chat_ids,
            offset_ref=offset_ref,
            chat_state=chat_state,
            on_text_update=on_text_update,
            on_callback_update=on_callback_update,
        )


def process_telegram_updates(
    *,
    updates: list[dict],
    logger: LifecycleLogger,
    source: str,
    allowed_chat_ids: set[int] | None,
    offset_ref: list[int | None] | None,
    chat_state: TelegramChatState,
    on_text_update: Callable[[TelegramTextUpdate], None],
    on_callback_update: Callable[[TelegramCallbackUpdate], None] | None = None,
) -> None:
    if not updates:
        return
    for upd in updates:
        upd_id = upd.get("update_id")
        if offset_ref is not None and isinstance(upd_id, int):
            offset_ref[0] = upd_id + 1
        callback = upd.get("callback_query")
        if isinstance(callback, dict):
            data = callback.get("data")
            msg = callback.get("message")
            cb_id_raw = callback.get("id")
            cb_id = cb_id_raw if isinstance(cb_id_raw, str) else None
            cb_chat_id: int | None = None
            cb_message_id: int | None = None
            cb_thread_id: int | None = None
            if isinstance(msg, dict):
                chat = msg.get("chat")
                if isinstance(chat, dict):
                    c = chat.get("id")
                    if isinstance(c, int):
                        cb_chat_id = c
                mid = msg.get("message_id")
                if isinstance(mid, int):
                    cb_message_id = mid
                tid = msg.get("message_thread_id")
                if isinstance(tid, int):
                    cb_thread_id = tid
            logger.event(
                "telegram_callback_received",
                source=source,
                update_id=upd_id,
                chat_id=cb_chat_id,
                message_id=cb_message_id,
                thread_id=cb_thread_id,
                data=data,
                callback_query_id=cb_id,
            )
            if not isinstance(cb_chat_id, int) or (
                allowed_chat_ids is not None and cb_chat_id not in allowed_chat_ids
            ):
                logger.event(
                    "telegram_callback_ignored",
                    source=source,
                    update_id=upd_id,
                    chat_id=cb_chat_id,
                    data=data,
                    reason="chat_not_allowed_or_missing",
                )
                continue
            chat_state.set_current(cb_chat_id)
            if on_callback_update is not None and isinstance(data, str):
                on_callback_update(
                    TelegramCallbackUpdate(
                        update_id=upd_id if isinstance(upd_id, int) else None,
                        chat_id=cb_chat_id,
                        message_id=cb_message_id,
                        thread_id=cb_thread_id,
                        callback_query_id=cb_id,
                        data=data,
                    )
                )
            continue
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
        message_id_raw = message.get("message_id")
        message_id = message_id_raw if isinstance(message_id_raw, int) else None
        thread_id_raw = message.get("message_thread_id")
        thread_id = thread_id_raw if isinstance(thread_id_raw, int) else None
        logger.event(
            "telegram_update_received",
            source=source,
            update_id=upd_id,
            chat_id=chat_id,
            message_id=message_id,
            text=text,
        )
        if allowed_chat_ids is not None and chat_id not in allowed_chat_ids:
            logger.event(
                "telegram_update_ignored",
                source=source,
                update_id=upd_id,
                chat_id=chat_id,
            )
            continue
        chat_state.set_current(chat_id)
        on_text_update(
            TelegramTextUpdate(
                update_id=upd_id if isinstance(upd_id, int) else None,
                chat_id=chat_id,
                message_id=message_id,
                thread_id=thread_id,
                text=text,
            )
        )


def extract_telegram_http_code(exc: Exception) -> int | None:
    match = re.search(r"http error:\s*(\d+)", str(exc))
    if match is None:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None
