"""Shared throttled Telegram status message lifecycle."""

from __future__ import annotations

import re
import threading
from collections import deque
from dataclasses import dataclass
from typing import Callable

from orchestrator import clock


@dataclass(frozen=True)
class TelegramStatusConfig:
    min_interval_sec: float = 1.0
    max_lines: int | None = None
    max_new_lines_per_flush: int = 5
    max_payload_chars: int = 3900


@dataclass(frozen=True)
class TelegramStatusClearPlan:
    status_key: str
    chat_id: int
    message_ids: tuple[int, ...]
    dropped_pending_lines: int


class TelegramStatusWindow:
    """Maintains a temporary Telegram status message with throttled updates."""

    def __init__(
        self,
        *,
        client,
        logger,
        source: str,
        status_key: str,
        chat_id_getter: Callable[[], int | None],
        target_fields_getter: Callable[[], dict] | None = None,
        send_kwargs_getter: Callable[[], dict] | None = None,
        eager_flush_when_due: bool = True,
        rate_limit_notifier: Callable[[int | None], None] | None = None,
        config: TelegramStatusConfig = TelegramStatusConfig(),
        monotonic_now: Callable[[], float] = clock.monotonic,
    ) -> None:
        self._client = client
        self._logger = logger
        self._source = source
        self._status_key = status_key
        self._chat_id_getter = chat_id_getter
        self._target_fields_getter = target_fields_getter or (lambda: {})
        self._send_kwargs_getter = send_kwargs_getter or (lambda: {})
        self._eager_flush_when_due = bool(eager_flush_when_due)
        self._rate_limit_notifier = rate_limit_notifier
        self._config = config
        self._now = monotonic_now
        self._lock = threading.Lock()
        self._message_ids: list[int] = []
        self._message_chat_id: int | None = None
        self._last_payload_chunks: list[str] = []
        self._window: list[str] = []
        self._pending: deque[str] = deque()
        self._pending_snapshot_lines: list[str] | None = None
        self._last_flush_ts: float | None = None

    def append(self, status_text: str) -> None:
        lines = _normalize_status_lines(status_text)
        if not lines:
            return
        with self._lock:
            self._pending.extend(lines)
            now = float(self._now())
            if self._eager_flush_when_due and self._should_flush(now):
                if self._pending_snapshot_lines is not None:
                    self._flush_pending_snapshot_locked(now=now, reason="append")
                else:
                    self._flush_locked(now=now, reason="append")
                return
            self._logger.event(
                "telegram_status_buffered",
                source=self._source,
                status_key=self._status_key,
                pending_lines=len(self._pending),
                window_lines=len(self._window),
                max_lines=self._config.max_lines,
                max_new_lines_per_flush=self._config.max_new_lines_per_flush,
                min_interval_sec=self._config.min_interval_sec,
                next_flush_in_sec=max(
                    0.0,
                    float(self._config.min_interval_sec) - max(0.0, now - float(self._last_flush_ts or now)),
                ),
                **self._target_fields_getter(),
            )

    def set_snapshot(self, status_text: str) -> None:
        lines = _normalize_status_lines(status_text)
        with self._lock:
            now = float(self._now())
            chat_id = self._chat_id_getter()
            if chat_id is None:
                self._logger.event(
                    "telegram_status_snapshot_skipped_no_chat",
                    source=self._source,
                    status_key=self._status_key,
                    **self._target_fields_getter(),
                )
                return
            if self._message_chat_id is not None and self._message_chat_id != chat_id:
                self._message_ids = []
                self._message_chat_id = None
                self._last_payload_chunks = []
            self._pending.clear()
            if lines:
                snapshot_lines = _trim_to_max_lines(lines, self._config.max_lines)
                payload = _render_status_payload(snapshot_lines)
                payload_chunks = _split_payload(payload, int(self._config.max_payload_chars))
                if payload_chunks == self._last_payload_chunks:
                    self._logger.event(
                        "telegram_status_snapshot_skipped_unchanged",
                        source=self._source,
                        status_key=self._status_key,
                        chat_id=chat_id,
                        message_id=self._message_ids[0] if self._message_ids else None,
                        **self._target_fields_getter(),
                    )
                    self._last_flush_ts = now
                    return
                if (not self._eager_flush_when_due) or (not self._should_flush(now)):
                    self._pending_snapshot_lines = snapshot_lines
                    self._pending.clear()
                    self._logger.event(
                        "telegram_status_snapshot_buffered",
                        source=self._source,
                        status_key=self._status_key,
                        chat_id=chat_id,
                        message_id=self._message_ids[0] if self._message_ids else None,
                        window_lines=len(snapshot_lines),
                        min_interval_sec=self._config.min_interval_sec,
                        next_flush_in_sec=max(
                            0.0,
                            float(self._config.min_interval_sec)
                            - max(0.0, now - float(self._last_flush_ts or now)),
                        ),
                        **self._target_fields_getter(),
                    )
                    return
                send_kwargs = dict(self._send_kwargs_getter() or {})
                try:
                    self._set_status_payload_chunks_locked(
                        chat_id=chat_id,
                        payload_chunks=payload_chunks,
                        send_kwargs=send_kwargs,
                    )
                    self._pending_snapshot_lines = None
                    self._window = snapshot_lines
                    self._last_flush_ts = now
                    self._logger.event(
                        "telegram_status_snapshot_set",
                        source=self._source,
                        status_key=self._status_key,
                        chat_id=chat_id,
                        message_id=self._message_ids[0] if self._message_ids else None,
                        message_count=len(self._message_ids),
                        window_lines=len(snapshot_lines),
                        **self._target_fields_getter(),
                    )
                except Exception as exc:
                    self._last_flush_ts = now
                    retry_after_sec = _extract_retry_after_sec(exc)
                    self._logger.event(
                        "telegram_status_snapshot_error",
                        source=self._source,
                        status_key=self._status_key,
                        chat_id=chat_id,
                        message_id=self._message_ids[0] if self._message_ids else None,
                        error=str(exc),
                        error_type=type(exc).__name__,
                        retry_after_sec=retry_after_sec,
                        **self._target_fields_getter(),
                    )
                    if self._rate_limit_notifier is not None:
                        self._rate_limit_notifier(retry_after_sec)
            else:
                self._window.clear()

    def clear(self) -> None:
        plan = self.take_clear_plan()
        if plan is None:
            return
        for message_id in plan.message_ids:
            try:
                self._client.delete_message(chat_id=plan.chat_id, message_id=message_id)
            except Exception as exc:
                retry_after_sec = _extract_retry_after_sec(exc)
                self._logger.event(
                    "telegram_status_delete_error",
                    source=self._source,
                    status_key=self._status_key,
                    chat_id=plan.chat_id,
                    message_id=message_id,
                    error=str(exc),
                    error_type=type(exc).__name__,
                    retry_after_sec=retry_after_sec,
                    **self._target_fields_getter(),
                )
                if self._rate_limit_notifier is not None:
                    self._rate_limit_notifier(retry_after_sec)
        self._logger.event(
            "telegram_status_cleared",
            source=self._source,
            status_key=self._status_key,
            chat_id=plan.chat_id,
            message_id=plan.message_ids[0],
            message_count=len(plan.message_ids),
            dropped_pending_lines=plan.dropped_pending_lines,
            **self._target_fields_getter(),
        )

    def take_clear_plan(self) -> TelegramStatusClearPlan | None:
        with self._lock:
            dropped_pending = len(self._pending)
            self._pending.clear()
            self._pending_snapshot_lines = None
            self._window.clear()
            self._last_flush_ts = None
            message_ids = list(self._message_ids)
            message_chat_id = self._message_chat_id
            self._message_ids = []
            self._message_chat_id = None
            self._last_payload_chunks = []
            if not message_ids or message_chat_id is None:
                return None
            return TelegramStatusClearPlan(
                status_key=self._status_key,
                chat_id=message_chat_id,
                message_ids=tuple(message_ids),
                dropped_pending_lines=dropped_pending,
            )

    def discard_pending(self) -> None:
        with self._lock:
            dropped_pending = len(self._pending)
            had_snapshot = self._pending_snapshot_lines is not None
            if dropped_pending == 0 and not had_snapshot:
                return
            self._pending.clear()
            self._pending_snapshot_lines = None
            self._logger.event(
                "telegram_status_pending_dropped",
                source=self._source,
                status_key=self._status_key,
                dropped_pending_lines=dropped_pending,
                dropped_snapshot=had_snapshot,
                chat_id=self._message_chat_id,
                message_id=self._message_ids[0] if self._message_ids else None,
                **self._target_fields_getter(),
            )

    def flush_pending(self, *, reason: str = "manual") -> None:
        with self._lock:
            now = float(self._now())
            if self._pending_snapshot_lines is not None:
                self._flush_pending_snapshot_locked(now=now, reason=reason)
                return
            if not self._pending:
                return
            self._flush_locked(now=now, reason=reason)

    def flush_due(self, *, reason: str = "due") -> bool:
        with self._lock:
            now = float(self._now())
            if not self._should_flush(now):
                return False
            if self._pending_snapshot_lines is not None:
                self._flush_pending_snapshot_locked(now=now, reason=reason)
                return True
            if not self._pending:
                return False
            self._flush_locked(now=now, reason=reason)
            return True

    def has_due_pending(self, *, now: float | None = None) -> bool:
        with self._lock:
            effective_now = float(self._now()) if now is None else float(now)
            if not self._should_flush(effective_now):
                return False
            return self._pending_snapshot_lines is not None or bool(self._pending)

    def scheduling_last_flush_ts(self) -> float | None:
        with self._lock:
            return self._last_flush_ts

    def pending_delivery_kind(self) -> str | None:
        with self._lock:
            if self._pending_snapshot_lines is None and not self._pending:
                return None
            return "edit" if self._message_ids else "send"

    def _should_flush(self, now: float) -> bool:
        last = self._last_flush_ts
        if last is None:
            return True
        return (now - last) >= float(self._config.min_interval_sec)

    def _flush_locked(self, *, now: float, reason: str) -> None:
        if self._pending_snapshot_lines is not None:
            self._flush_pending_snapshot_locked(now=now, reason=reason)
            return
        if not self._pending:
            return
        chat_id = self._chat_id_getter()
        if chat_id is None:
            self._logger.event(
                "telegram_status_skipped_no_chat",
                source=self._source,
                status_key=self._status_key,
                pending_lines=len(self._pending),
                reason=reason,
                **self._target_fields_getter(),
            )
            return
        if self._message_chat_id is not None and self._message_chat_id != chat_id:
            self._logger.event(
                "telegram_status_chat_switched",
                source=self._source,
                status_key=self._status_key,
                from_chat_id=self._message_chat_id,
                to_chat_id=chat_id,
                **self._target_fields_getter(),
            )
            self._message_ids = []
            self._message_chat_id = None
            self._last_payload_chunks = []
            self._window.clear()
            self._last_flush_ts = None

        # Flush all accumulated lines in one Telegram edit/send to avoid partial backlog drift.
        add_count = len(self._pending)
        if add_count <= 0:
            return
        to_add = list(self._pending)[:add_count]
        candidate = list(self._window)
        candidate.extend(to_add)
        candidate, trimmed = _trim_with_count(candidate, self._config.max_lines)
        payload = _render_status_payload(candidate)
        payload_chunks = _split_payload(payload, int(self._config.max_payload_chars))
        send_kwargs = dict(self._send_kwargs_getter() or {})
        try:
            if payload_chunks == self._last_payload_chunks:
                self._logger.event(
                    "telegram_status_update_skipped_unchanged",
                    source=self._source,
                    status_key=self._status_key,
                    chat_id=chat_id,
                    message_id=self._message_ids[0] if self._message_ids else None,
                    reason=reason,
                    **self._target_fields_getter(),
                )
                for _ in range(add_count):
                    self._pending.popleft()
                self._window = candidate
                self._last_flush_ts = now
                return
            self._set_status_payload_chunks_locked(
                chat_id=chat_id,
                payload_chunks=payload_chunks,
                send_kwargs=send_kwargs,
            )
            for _ in range(add_count):
                self._pending.popleft()
            self._window = candidate
            self._last_flush_ts = now
            self._logger.event(
                "telegram_status_updated",
                source=self._source,
                status_key=self._status_key,
                chat_id=chat_id,
                message_id=self._message_ids[0] if self._message_ids else None,
                message_count=len(self._message_ids),
                reason=reason,
                added_lines=add_count,
                pending_lines=len(self._pending),
                window_lines=len(self._window),
                trimmed_lines=trimmed,
                max_lines=self._config.max_lines,
                max_new_lines_per_flush=self._config.max_new_lines_per_flush,
                min_interval_sec=self._config.min_interval_sec,
                last_flush_ts=now,
                **self._target_fields_getter(),
            )
        except Exception as exc:
            self._last_flush_ts = now
            retry_after_sec = _extract_retry_after_sec(exc)
            self._logger.event(
                "telegram_status_update_error",
                source=self._source,
                status_key=self._status_key,
                chat_id=chat_id,
                message_id=self._message_ids[0] if self._message_ids else None,
                error=str(exc),
                error_type=type(exc).__name__,
                retry_after_sec=retry_after_sec,
                reason=reason,
                pending_lines=len(self._pending),
                **self._target_fields_getter(),
            )
            if self._rate_limit_notifier is not None:
                self._rate_limit_notifier(retry_after_sec)

    def _flush_pending_snapshot_locked(self, *, now: float, reason: str) -> None:
        lines = list(self._pending_snapshot_lines or [])
        if not lines:
            self._pending_snapshot_lines = None
            return
        chat_id = self._chat_id_getter()
        if chat_id is None:
            self._logger.event(
                "telegram_status_snapshot_skipped_no_chat",
                source=self._source,
                status_key=self._status_key,
                **self._target_fields_getter(),
            )
            return
        if self._message_chat_id is not None and self._message_chat_id != chat_id:
            self._logger.event(
                "telegram_status_chat_switched",
                source=self._source,
                status_key=self._status_key,
                from_chat_id=self._message_chat_id,
                to_chat_id=chat_id,
                **self._target_fields_getter(),
            )
            self._message_ids = []
            self._message_chat_id = None
            self._last_payload_chunks = []
            self._window.clear()
            self._last_flush_ts = None
        payload = _render_status_payload(lines)
        payload_chunks = _split_payload(payload, int(self._config.max_payload_chars))
        send_kwargs = dict(self._send_kwargs_getter() or {})
        try:
            if payload_chunks == self._last_payload_chunks:
                self._pending_snapshot_lines = None
                self._pending.clear()
                self._window = lines
                self._last_flush_ts = now
                self._logger.event(
                    "telegram_status_snapshot_skipped_unchanged",
                    source=self._source,
                    status_key=self._status_key,
                    chat_id=chat_id,
                    message_id=self._message_ids[0] if self._message_ids else None,
                    **self._target_fields_getter(),
                )
                return
            self._set_status_payload_chunks_locked(
                chat_id=chat_id,
                payload_chunks=payload_chunks,
                send_kwargs=send_kwargs,
            )
            self._pending_snapshot_lines = None
            self._pending.clear()
            self._window = lines
            self._last_flush_ts = now
            self._logger.event(
                "telegram_status_updated",
                source=self._source,
                status_key=self._status_key,
                chat_id=chat_id,
                message_id=self._message_ids[0] if self._message_ids else None,
                message_count=len(self._message_ids),
                reason=f"{reason}:snapshot",
                added_lines=0,
                pending_lines=len(self._pending),
                window_lines=len(self._window),
                trimmed_lines=0,
                max_lines=self._config.max_lines,
                max_new_lines_per_flush=self._config.max_new_lines_per_flush,
                min_interval_sec=self._config.min_interval_sec,
                last_flush_ts=now,
                **self._target_fields_getter(),
            )
        except Exception as exc:
            self._last_flush_ts = now
            retry_after_sec = _extract_retry_after_sec(exc)
            self._logger.event(
                "telegram_status_snapshot_error",
                source=self._source,
                status_key=self._status_key,
                chat_id=chat_id,
                message_id=self._message_ids[0] if self._message_ids else None,
                error=str(exc),
                error_type=type(exc).__name__,
                retry_after_sec=retry_after_sec,
                **self._target_fields_getter(),
            )
            if self._rate_limit_notifier is not None:
                self._rate_limit_notifier(retry_after_sec)

    def _set_status_payload_chunks_locked(
        self,
        *,
        chat_id: int,
        payload_chunks: list[str],
        send_kwargs: dict,
    ) -> None:
        existing_ids = list(self._message_ids)
        new_ids: list[int] = []
        for idx, chunk in enumerate(payload_chunks):
            if idx < len(existing_ids):
                message_id = existing_ids[idx]
                previous = self._last_payload_chunks[idx] if idx < len(self._last_payload_chunks) else None
                if previous != chunk:
                    self._client.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=chunk,
                    )
                new_ids.append(message_id)
                continue
            sent = self._client.send_message(chat_id=chat_id, text=chunk, **send_kwargs)
            message_id_raw = sent.get("message_id") if isinstance(sent, dict) else None
            if not isinstance(message_id_raw, int):
                raise RuntimeError("telegram status sendMessage did not return message_id")
            new_ids.append(message_id_raw)
        for message_id in existing_ids[len(payload_chunks):]:
            self._client.delete_message(chat_id=chat_id, message_id=message_id)
        self._message_ids = new_ids
        self._message_chat_id = chat_id
        self._last_payload_chunks = list(payload_chunks)


def _normalize_status_lines(status_text: str) -> list[str]:
    lines: list[str] = []
    for raw in str(status_text).splitlines():
        line = raw.strip()
        if line:
            lines.append(line)
    return lines


def _render_status_payload(lines: list[str]) -> str:
    if not lines:
        return "[status]"
    return "[status]\n" + "\n".join(lines)


def _trim_to_max_lines(lines: list[str], max_lines: int | None) -> list[str]:
    if max_lines is None:
        return list(lines)
    cap = max(1, int(max_lines))
    if len(lines) <= cap:
        return list(lines)
    return list(lines[-cap:])


def _trim_with_count(lines: list[str], max_lines: int | None) -> tuple[list[str], int]:
    if max_lines is None:
        return list(lines), 0
    cap = max(1, int(max_lines))
    trimmed = max(0, len(lines) - cap)
    if trimmed <= 0:
        return list(lines), 0
    return list(lines[trimmed:]), trimmed


def _split_payload(payload: str, limit: int) -> list[str]:
    if limit <= 0 or len(payload) <= limit:
        return [payload]
    parts: list[str] = []
    rest = payload
    while len(rest) > limit:
        cut = rest.rfind("\n", 0, limit + 1)
        if cut <= 0 or cut < (limit // 2):
            cut = limit
        chunk = rest[:cut]
        if chunk:
            parts.append(chunk)
        rest = rest[cut:]
        if rest.startswith("\n"):
            rest = rest[1:]
    if rest:
        parts.append(rest)
    return parts if parts else [payload]


def _extract_retry_after_sec(exc: Exception) -> int | None:
    match = re.search(r"retry after (\d+)", str(exc), flags=re.IGNORECASE)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None
