from __future__ import annotations

import threading
import time
from queue import Empty, Queue
from typing import Any

from orchestrator.serialized_call_lane import SerializedCallLane
from orchestrator.telegram_smoke import TelegramApi


class TelegramApiThreadDriver:
    """Runs Telegram polling and Telegram mutations on separate edge threads."""

    def __init__(self, client: TelegramApi) -> None:
        self._client = client
        self._ops_lane = SerializedCallLane(
            client,
            thread_name="telegram-api-ops-thread",
        )
        self._updates: Queue[list[dict[str, Any]]] = Queue()
        self._poll_loop_thread: threading.Thread | None = None
        self._poll_loop_stop = threading.Event()
        self._poll_timeout_sec = 1
        self._poll_error_backoff_sec = 0.0
        self._offset: int | None = None
        self._poll_cycles = 0
        self._poll_errors: Queue[BaseException] = Queue()
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._ops_lane.start()

    def stop(self) -> None:
        if not self._started:
            return
        self.stop_polling()
        self._ops_lane.stop()
        self._started = False

    def send_message(self, chat_id: int, text: str, **kwargs: Any) -> dict:
        result = self._call("send_message", chat_id, text, **kwargs)
        return result if isinstance(result, dict) else {}

    def edit_message_text(self, chat_id: int, message_id: int, text: str, **kwargs: Any) -> None:
        self._call("edit_message_text", chat_id, message_id, text, **kwargs)

    def edit_message_reply_markup(
        self,
        chat_id: int,
        message_id: int,
        reply_markup: dict | None = None,
    ) -> None:
        self._call(
            "edit_message_reply_markup",
            chat_id,
            message_id,
            reply_markup=reply_markup,
        )

    def delete_message(self, chat_id: int, message_id: int) -> None:
        self._call("delete_message", chat_id, message_id)

    def answer_callback_query(self, callback_query_id: str, text: str | None = None) -> None:
        self._call("answer_callback_query", callback_query_id, text=text)

    @property
    def poll_cycles(self) -> int:
        return self._poll_cycles

    def start_polling(
        self,
        *,
        offset: int | None,
        poll_timeout_sec: int,
        poll_error_backoff_sec: float = 0.0,
    ) -> None:
        if self._poll_loop_thread is not None:
            return
        self._offset = offset
        self._poll_timeout_sec = max(1, int(poll_timeout_sec))
        self._poll_error_backoff_sec = max(0.0, float(poll_error_backoff_sec))
        self._poll_cycles = 0
        while not self._poll_errors.empty():
            try:
                self._poll_errors.get_nowait()
            except Empty:
                break
        self._poll_loop_stop.clear()
        self._poll_loop_thread = threading.Thread(
            target=self._poll_loop,
            name="telegram-api-longpoll-thread",
            daemon=True,
        )
        self._poll_loop_thread.start()

    def stop_polling(self) -> None:
        thread = self._poll_loop_thread
        if thread is None:
            return
        self._poll_loop_stop.set()
        thread.join(timeout=2.0)
        if thread.is_alive():
            raise RuntimeError("telegram-api-longpoll-thread did not stop within 2.00s")
        self._poll_loop_thread = None

    def drain_updates(self) -> list[dict[str, Any]]:
        drained: list[dict[str, Any]] = []
        while True:
            try:
                drained.extend(self._updates.get_nowait())
            except Empty:
                return drained

    def take_poll_error(self) -> BaseException | None:
        try:
            return self._poll_errors.get_nowait()
        except Empty:
            return None

    def has_pending_transport_events(self) -> bool:
        return (not self._updates.empty()) or (not self._poll_errors.empty())

    def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        return self._ops_lane.call(method, *args, **kwargs)

    def _poll_loop(self) -> None:
        timeout_sec = min(self._poll_timeout_sec, 1)
        while not self._poll_loop_stop.is_set():
            self._poll_cycles += 1
            try:
                updates = self._client.get_updates(offset=self._offset, timeout_sec=timeout_sec)
            except BaseException as exc:
                self._poll_errors.put(exc)
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    return
                if self._poll_error_backoff_sec > 0.0:
                    self._poll_loop_stop.wait(self._poll_error_backoff_sec)
                continue
            if not updates:
                continue
            fresh_updates: list[dict[str, Any]] = []
            for upd in updates:
                upd_id = upd.get("update_id")
                if isinstance(upd_id, int):
                    if self._offset is not None and upd_id < self._offset:
                        continue
                    self._offset = upd_id + 1
                fresh_updates.append(upd)
            if fresh_updates:
                self._updates.put(fresh_updates)
