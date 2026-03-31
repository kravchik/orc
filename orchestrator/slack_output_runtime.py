"""Shared Slack status runtime: throttled status windows over Slack messages."""

from __future__ import annotations

import threading
from collections import deque
from typing import Callable

from orchestrator import clock
from orchestrator.protocol_status import format_item_status_lines
from orchestrator.telegram_status import TelegramStatusConfig


class SlackStatusWindow:
    def __init__(
        self,
        *,
        client,
        logger,
        source: str,
        channel_id_getter: Callable[[], str | None],
        thread_ts_getter: Callable[[], str | None] | None = None,
        config: TelegramStatusConfig = TelegramStatusConfig(),
        monotonic_now: Callable[[], float] = clock.monotonic,
    ) -> None:
        self._client = client
        self._logger = logger
        self._source = source
        self._channel_id_getter = channel_id_getter
        self._thread_ts_getter = thread_ts_getter or (lambda: None)
        self._config = config
        self._now = monotonic_now
        self._lock = threading.Lock()
        self._message_ts_list: list[str] = []
        self._message_channel_id: str | None = None
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
            if self._should_flush(now):
                if self._pending_snapshot_lines is not None:
                    self._flush_pending_snapshot_locked(now=now, reason="append")
                else:
                    self._flush_locked(now=now, reason="append")
                return
            self._logger.event(
                "slack_status_buffered",
                source=self._source,
                pending_lines=len(self._pending),
                window_lines=len(self._window),
                max_lines=self._config.max_lines,
                max_new_lines_per_flush=self._config.max_new_lines_per_flush,
                min_interval_sec=self._config.min_interval_sec,
            )

    def set_snapshot(self, status_text: str) -> None:
        lines = _normalize_status_lines(status_text)
        with self._lock:
            now = float(self._now())
            channel_id = self._channel_id_getter()
            if not channel_id:
                self._logger.event("slack_status_snapshot_skipped_no_channel", source=self._source)
                return
            if self._message_channel_id is not None and self._message_channel_id != channel_id:
                self._message_ts_list = []
                self._message_channel_id = None
                self._last_payload_chunks = []
            self._pending.clear()
            if lines:
                snapshot_lines = _trim_to_max_lines(lines, self._config.max_lines)
                payload_chunks = _split_payload(
                    _render_status_payload(snapshot_lines, source=self._source),
                    int(self._config.max_payload_chars),
                )
                if payload_chunks == self._last_payload_chunks:
                    self._last_flush_ts = now
                    self._logger.event(
                        "slack_status_snapshot_skipped_unchanged",
                        source=self._source,
                        channel_id=channel_id,
                        ts=self._message_ts_list[0] if self._message_ts_list else None,
                    )
                    return
                if not self._should_flush(now):
                    self._pending_snapshot_lines = snapshot_lines
                    self._logger.event(
                        "slack_status_snapshot_buffered",
                        source=self._source,
                        channel_id=channel_id,
                        ts=self._message_ts_list[0] if self._message_ts_list else None,
                    )
                    return
                self._set_status_payload_chunks_locked(
                    channel_id=channel_id,
                    thread_ts=self._thread_ts_getter(),
                    payload_chunks=payload_chunks,
                )
                self._pending_snapshot_lines = None
                self._window = snapshot_lines
                self._last_flush_ts = now
                self._logger.event(
                    "slack_status_snapshot_set",
                    source=self._source,
                    channel_id=channel_id,
                    ts=self._message_ts_list[0] if self._message_ts_list else None,
                    message_count=len(self._message_ts_list),
                    window_lines=len(snapshot_lines),
                )
            else:
                self._window.clear()

    def clear(self) -> None:
        with self._lock:
            dropped_pending = len(self._pending)
            self._pending.clear()
            self._pending_snapshot_lines = None
            self._window.clear()
            self._last_flush_ts = None
            message_ts_list = list(self._message_ts_list)
            channel_id = self._message_channel_id
            self._message_ts_list = []
            self._message_channel_id = None
            self._last_payload_chunks = []
            if not message_ts_list or not channel_id:
                return
            for ts in message_ts_list:
                try:
                    self._client.delete_message(channel_id=channel_id, ts=ts)
                except Exception as exc:
                    self._logger.event(
                        "slack_status_delete_error",
                        source=self._source,
                        channel_id=channel_id,
                        ts=ts,
                        error=str(exc),
                        error_type=type(exc).__name__,
                    )
            self._logger.event(
                "slack_status_cleared",
                source=self._source,
                channel_id=channel_id,
                ts=message_ts_list[0],
                message_count=len(message_ts_list),
                dropped_pending_lines=dropped_pending,
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

    def pending_delivery_kind(self) -> str | None:
        with self._lock:
            if self._pending_snapshot_lines is None and not self._pending:
                return None
            payload_chunks = self._pending_payload_chunks_locked()
            if not payload_chunks:
                return None
            if len(payload_chunks) > len(self._message_ts_list):
                return "send"
            return "edit"

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
        channel_id = self._channel_id_getter()
        if not channel_id:
            self._logger.event(
                "slack_status_skipped_no_channel",
                source=self._source,
                pending_lines=len(self._pending),
                reason=reason,
            )
            return
        if self._message_channel_id is not None and self._message_channel_id != channel_id:
            self._message_ts_list = []
            self._message_channel_id = None
            self._last_payload_chunks = []
            self._window.clear()
            self._last_flush_ts = None

        add_count = len(self._pending)
        if add_count <= 0:
            return
        candidate = list(self._window)
        candidate.extend(list(self._pending)[:add_count])
        candidate, trimmed = _trim_with_count(candidate, self._config.max_lines)
        payload_chunks = _split_payload(
            _render_status_payload(candidate, source=self._source),
            int(self._config.max_payload_chars),
        )
        if payload_chunks == self._last_payload_chunks:
            for _ in range(add_count):
                self._pending.popleft()
            self._window = candidate
            self._last_flush_ts = now
            self._logger.event(
                "slack_status_update_skipped_unchanged",
                source=self._source,
                channel_id=channel_id,
                ts=self._message_ts_list[0] if self._message_ts_list else None,
                reason=reason,
            )
            return
        self._set_status_payload_chunks_locked(
            channel_id=channel_id,
            thread_ts=self._thread_ts_getter(),
            payload_chunks=payload_chunks,
        )
        for _ in range(add_count):
            self._pending.popleft()
        self._window = candidate
        self._last_flush_ts = now
        self._logger.event(
            "slack_status_updated",
            source=self._source,
            channel_id=channel_id,
            ts=self._message_ts_list[0] if self._message_ts_list else None,
            message_count=len(self._message_ts_list),
            reason=reason,
            added_lines=add_count,
            pending_lines=len(self._pending),
            window_lines=len(self._window),
            trimmed_lines=trimmed,
        )

    def _pending_payload_chunks_locked(self) -> list[str]:
        if self._pending_snapshot_lines is not None:
            lines = list(self._pending_snapshot_lines)
            if not lines:
                return []
            return _split_payload(
                _render_status_payload(lines, source=self._source),
                int(self._config.max_payload_chars),
            )
        if not self._pending:
            return []
        candidate = list(self._window)
        candidate.extend(list(self._pending))
        candidate = _trim_to_max_lines(candidate, self._config.max_lines)
        return _split_payload(
            _render_status_payload(candidate, source=self._source),
            int(self._config.max_payload_chars),
        )

    def _flush_pending_snapshot_locked(self, *, now: float, reason: str) -> None:
        lines = list(self._pending_snapshot_lines or [])
        if not lines:
            self._pending_snapshot_lines = None
            return
        channel_id = self._channel_id_getter()
        if not channel_id:
            self._logger.event("slack_status_snapshot_skipped_no_channel", source=self._source)
            return
        payload_chunks = _split_payload(
            _render_status_payload(lines, source=self._source),
            int(self._config.max_payload_chars),
        )
        if payload_chunks == self._last_payload_chunks:
            self._pending_snapshot_lines = None
            self._pending.clear()
            self._window = lines
            self._last_flush_ts = now
            return
        self._set_status_payload_chunks_locked(
            channel_id=channel_id,
            thread_ts=self._thread_ts_getter(),
            payload_chunks=payload_chunks,
        )
        self._pending_snapshot_lines = None
        self._pending.clear()
        self._window = lines
        self._last_flush_ts = now
        self._logger.event(
            "slack_status_updated",
            source=self._source,
            channel_id=channel_id,
            ts=self._message_ts_list[0] if self._message_ts_list else None,
            message_count=len(self._message_ts_list),
            reason=f"{reason}:snapshot",
            added_lines=0,
            pending_lines=0,
            window_lines=len(self._window),
            trimmed_lines=0,
        )

    def _set_status_payload_chunks_locked(
        self,
        *,
        channel_id: str,
        thread_ts: str | None,
        payload_chunks: list[str],
    ) -> None:
        existing = list(self._message_ts_list)
        new_ts_list: list[str] = []
        for idx, chunk in enumerate(payload_chunks):
            if idx < len(existing):
                ts = existing[idx]
                previous = self._last_payload_chunks[idx] if idx < len(self._last_payload_chunks) else None
                if previous != chunk:
                    self._logger.event(
                        "slack_status_delivery_attempt",
                        source=self._source,
                        op="update",
                        channel_id=channel_id,
                        ts=ts,
                        thread_ts=thread_ts,
                        chunk_index=idx + 1,
                        chunks_total=len(payload_chunks),
                        text_len=len(chunk),
                        max_payload_chars=int(self._config.max_payload_chars),
                    )
                    self._client.update_message(channel_id=channel_id, ts=ts, text=chunk, blocks=None)
                new_ts_list.append(ts)
                continue
            self._logger.event(
                "slack_status_delivery_attempt",
                source=self._source,
                op="post",
                channel_id=channel_id,
                ts=None,
                thread_ts=thread_ts,
                chunk_index=idx + 1,
                chunks_total=len(payload_chunks),
                text_len=len(chunk),
                max_payload_chars=int(self._config.max_payload_chars),
            )
            sent = self._client.post_message(
                channel_id=channel_id,
                text=chunk,
                thread_ts=thread_ts,
                blocks=None,
            )
            ts_raw = sent.get("ts") if isinstance(sent, dict) else None
            if not isinstance(ts_raw, str) or not ts_raw.strip():
                raise RuntimeError("slack status post_message did not return ts")
            new_ts_list.append(ts_raw.strip())
        for ts in existing[len(payload_chunks):]:
            self._client.delete_message(channel_id=channel_id, ts=ts)
        self._message_ts_list = new_ts_list
        self._message_channel_id = channel_id
        self._last_payload_chunks = list(payload_chunks)


class SlackStructuredStatusThread:
    def __init__(
        self,
        *,
        client,
        logger,
        source: str,
        channel_id_getter: Callable[[], str | None],
        thread_ts_getter: Callable[[], str | None] | None = None,
        config: TelegramStatusConfig = TelegramStatusConfig(),
        monotonic_now: Callable[[], float] = clock.monotonic,
    ) -> None:
        self._client = client
        self._logger = logger
        self._source = source
        self._channel_id_getter = channel_id_getter
        self._thread_ts_getter = thread_ts_getter or (lambda: None)
        self._config = config
        self._now = monotonic_now
        self._lock = threading.Lock()
        self._root_ts: str | None = None
        self._root_channel_id: str | None = None
        self._last_root_text: str | None = None
        self._item_ts_by_id: dict[str, str] = {}
        self._last_item_text_by_id: dict[str, str] = {}
        self._pending_meta_lines: list[str] | None = None
        self._pending_snapshot: list[dict[str, str]] | None = None
        self._last_flush_ts: float | None = None

    def set_snapshot(
        self,
        *,
        meta_lines: list[str],
        snapshot: list[dict[str, str]],
    ) -> None:
        with self._lock:
            now = float(self._now())
            channel_id = self._channel_id_getter()
            if not channel_id:
                self._logger.event("slack_status_snapshot_skipped_no_channel", source=self._source)
                return
            self._ensure_root_channel_only(channel_id=channel_id)
            if self._root_channel_id is not None and self._root_channel_id != channel_id:
                self._root_ts = None
                self._root_channel_id = None
                self._last_root_text = None
                self._item_ts_by_id.clear()
                self._last_item_text_by_id.clear()
            self._pending_meta_lines = list(meta_lines)
            self._pending_snapshot = [dict(item) for item in snapshot]
            if not self._should_flush(now):
                self._logger.event(
                    "slack_status_snapshot_buffered",
                    source=self._source,
                    channel_id=channel_id,
                    ts=self._root_ts,
                )
                return
            self._flush_pending_locked(now=now, reason="snapshot")

    def clear(self) -> None:
        with self._lock:
            root_ts = self._root_ts
            channel_id = self._root_channel_id
            item_ts_list = list(self._item_ts_by_id.values())
            self._root_ts = None
            self._root_channel_id = None
            self._last_root_text = None
            self._item_ts_by_id.clear()
            self._last_item_text_by_id.clear()
            self._pending_meta_lines = None
            self._pending_snapshot = None
            self._last_flush_ts = None
            if not channel_id:
                return
            for ts in item_ts_list:
                self._client.delete_message(channel_id=channel_id, ts=ts)
            if root_ts:
                self._client.delete_message(channel_id=channel_id, ts=root_ts)

    def flush_pending(self, *, reason: str = "manual") -> None:
        with self._lock:
            now = float(self._now())
            self._flush_pending_locked(now=now, reason=reason)

    def flush_due(self, *, reason: str = "due") -> bool:
        with self._lock:
            now = float(self._now())
            if not self._should_flush(now):
                return False
            if self._pending_meta_lines is None or self._pending_snapshot is None:
                return False
            self._flush_pending_locked(now=now, reason=reason)
            return True

    def has_due_pending(self, *, now: float | None = None) -> bool:
        with self._lock:
            if self._pending_meta_lines is None or self._pending_snapshot is None:
                return False
            effective_now = float(self._now()) if now is None else float(now)
            return self._should_flush(effective_now)

    def pending_delivery_kind(self) -> str | None:
        with self._lock:
            if self._pending_meta_lines is None or self._pending_snapshot is None:
                return None
            desired_root = _render_structured_root_payload(
                meta_lines=self._pending_meta_lines,
                snapshot=self._pending_snapshot,
                source=self._source,
            )
            desired_items = {
                str(item.get("id") or ""): _render_structured_item_payload(item)
                for item in self._pending_snapshot
                if isinstance(item.get("id"), str) and str(item.get("id") or "").strip()
            }
            if self._root_ts is None:
                return "send"
            if self._last_root_text != desired_root:
                return "edit"
            for item_id, text in desired_items.items():
                if item_id not in self._item_ts_by_id:
                    return "send"
                if self._last_item_text_by_id.get(item_id) != text:
                    return "edit"
            return None

    def _should_flush(self, now: float) -> bool:
        last = self._last_flush_ts
        if last is None:
            return True
        return (now - last) >= float(self._config.min_interval_sec)

    def _flush_pending_locked(self, *, now: float, reason: str) -> None:
        meta_lines = list(self._pending_meta_lines or [])
        snapshot = [dict(item) for item in (self._pending_snapshot or [])]
        if self._pending_meta_lines is None or self._pending_snapshot is None:
            return
        channel_id = self._channel_id_getter()
        if not channel_id:
            return
        self._ensure_root_channel_only(channel_id=channel_id)
        root_text = _render_structured_root_payload(
            meta_lines=meta_lines,
            snapshot=snapshot,
            source=self._source,
        )
        item_pairs: list[tuple[str, str]] = []
        for item in snapshot:
            raw_item_id = item.get("id")
            if not isinstance(raw_item_id, str) or not raw_item_id.strip():
                continue
            item_pairs.append((raw_item_id.strip(), _render_structured_item_payload(item)))
        if self._root_ts is None:
            self._logger.event(
                "slack_status_delivery_attempt",
                source=self._source,
                op="post",
                channel_id=channel_id,
                ts=None,
                thread_ts=None,
                chunk_index=1,
                chunks_total=1,
                text_len=len(root_text),
                max_payload_chars=int(self._config.max_payload_chars),
                target="root",
            )
            sent = self._client.post_message(
                channel_id=channel_id,
                thread_ts=None,
                text=root_text,
                blocks=None,
            )
            root_ts_raw = sent.get("ts") if isinstance(sent, dict) else None
            if not isinstance(root_ts_raw, str) or not root_ts_raw.strip():
                raise RuntimeError("slack structured status root post_message did not return ts")
            self._root_ts = root_ts_raw.strip()
            self._root_channel_id = channel_id
            self._last_root_text = root_text
        elif self._last_root_text != root_text:
            self._logger.event(
                "slack_status_delivery_attempt",
                source=self._source,
                op="update",
                channel_id=channel_id,
                ts=self._root_ts,
                thread_ts=None,
                chunk_index=1,
                chunks_total=1,
                text_len=len(root_text),
                max_payload_chars=int(self._config.max_payload_chars),
                target="root",
            )
            self._client.update_message(
                channel_id=channel_id,
                ts=str(self._root_ts),
                text=root_text,
                blocks=None,
            )
            self._last_root_text = root_text
        if not self._root_ts:
            return
        for item_id, item_text in item_pairs:
            existing_ts = self._item_ts_by_id.get(item_id)
            if existing_ts is None:
                self._logger.event(
                    "slack_status_delivery_attempt",
                    source=self._source,
                    op="post",
                    channel_id=channel_id,
                    ts=None,
                    thread_ts=self._root_ts,
                    chunk_index=1,
                    chunks_total=1,
                    text_len=len(item_text),
                    max_payload_chars=int(self._config.max_payload_chars),
                    target="item",
                    item_id=item_id,
                )
                sent = self._client.post_message(
                    channel_id=channel_id,
                    thread_ts=self._root_ts,
                    text=item_text,
                    blocks=None,
                )
                item_ts_raw = sent.get("ts") if isinstance(sent, dict) else None
                if not isinstance(item_ts_raw, str) or not item_ts_raw.strip():
                    raise RuntimeError("slack structured status item post_message did not return ts")
                self._item_ts_by_id[item_id] = item_ts_raw.strip()
                self._last_item_text_by_id[item_id] = item_text
                continue
            if self._last_item_text_by_id.get(item_id) == item_text:
                continue
            self._logger.event(
                "slack_status_delivery_attempt",
                source=self._source,
                op="update",
                channel_id=channel_id,
                ts=existing_ts,
                thread_ts=self._root_ts,
                chunk_index=1,
                chunks_total=1,
                text_len=len(item_text),
                max_payload_chars=int(self._config.max_payload_chars),
                target="item",
                item_id=item_id,
            )
            self._client.update_message(
                channel_id=channel_id,
                ts=existing_ts,
                text=item_text,
                blocks=None,
            )
            self._last_item_text_by_id[item_id] = item_text
        self._pending_meta_lines = None
        self._pending_snapshot = None
        self._last_flush_ts = now
        self._logger.event(
            "slack_status_updated",
            source=self._source,
            channel_id=channel_id,
            ts=self._root_ts,
            message_count=1 + len(self._item_ts_by_id),
            reason=reason,
            added_lines=len(item_pairs),
            pending_lines=0,
            window_lines=len(item_pairs),
            trimmed_lines=0,
            mode="threaded",
        )

    def _ensure_root_channel_only(self, *, channel_id: str) -> None:
        thread_ts = self._thread_ts_getter()
        if not isinstance(thread_ts, str) or not thread_ts.strip():
            return
        raise RuntimeError(
            "Slack structured status threading supports only root-channel access points; "
            f"got channel_id={channel_id!r} thread_ts={thread_ts.strip()!r}"
        )


class SlackOutputRuntime:
    def __init__(
        self,
        *,
        client,
        logger,
        source: str,
        channel_id_getter: Callable[[], str | None],
        thread_ts_getter: Callable[[], str | None] | None = None,
        status_config: TelegramStatusConfig = TelegramStatusConfig(),
        status_monotonic_now: Callable[[], float] | None = None,
    ) -> None:
        self._client = client
        self._logger = logger
        self._source = source
        self._channel_id_getter = channel_id_getter
        self._thread_ts_getter = thread_ts_getter
        self._status_config = status_config
        self._status_monotonic_now = status_monotonic_now
        self._status_windows: dict[str, SlackStatusWindow] = {}
        self._structured_threads: dict[str, SlackStructuredStatusThread] = {}
        self._latest_status_key: str | None = None

    def append_status(
        self,
        status_text: str,
        *,
        status_key: str | None = None,
        force_new_if_not_latest: bool = False,
    ) -> None:
        _ = force_new_if_not_latest
        key = self._resolve_status_key(status_key=status_key)
        self._window_for_key(key).append(status_text)
        self._latest_status_key = key

    def set_status_snapshot(
        self,
        status_text: str,
        *,
        status_key: str | None = None,
    ) -> None:
        key = self._resolve_status_key(status_key=status_key)
        self._window_for_key(key).set_snapshot(status_text)
        self._latest_status_key = key

    def clear_status(self) -> None:
        for window in self._status_windows.values():
            window.clear()
        self._status_windows.clear()
        for thread in self._structured_threads.values():
            thread.clear()
        self._structured_threads.clear()
        self._latest_status_key = None

    def flush_status(self, *, status_key: str | None = None) -> None:
        key = status_key if status_key is not None else self._latest_status_key
        if key is None:
            return
        structured = self._structured_threads.get(key)
        if structured is not None:
            structured.flush_pending(reason="manual")
            return
        window = self._status_windows.get(key)
        if window is not None:
            window.flush_pending(reason="manual")

    def flush_due_statuses(self) -> bool:
        progressed = False
        for window in self._status_windows.values():
            if window.flush_due(reason="due"):
                progressed = True
        for thread in self._structured_threads.values():
            if thread.flush_due(reason="due"):
                progressed = True
        return progressed

    def has_due_statuses(self) -> bool:
        now = float((self._status_monotonic_now or clock.monotonic)())
        for window in self._status_windows.values():
            if window.has_due_pending(now=now):
                return True
        for thread in self._structured_threads.values():
            if thread.has_due_pending(now=now):
                return True
        return False

    def status_delivery_kind(self, *, status_key: str | None = None) -> str | None:
        key = status_key if status_key is not None else self._latest_status_key
        if key is None:
            return None
        structured = self._structured_threads.get(key)
        if structured is not None:
            return structured.pending_delivery_kind()
        window = self._status_windows.get(key)
        if window is None:
            return None
        return window.pending_delivery_kind()

    def set_status_snapshot_structured(
        self,
        *,
        status_key: str | None,
        meta_lines: list[str],
        snapshot: list[dict[str, str]],
    ) -> None:
        key = self._resolve_status_key(status_key=status_key)
        self._structured_thread_for_key(key).set_snapshot(meta_lines=meta_lines, snapshot=snapshot)
        self._latest_status_key = key

    def _window_for_key(self, key: str) -> SlackStatusWindow:
        window = self._status_windows.get(key)
        if window is not None:
            return window
        window = SlackStatusWindow(
            client=self._client,
            logger=self._logger,
            source=self._source,
            channel_id_getter=self._channel_id_getter,
            thread_ts_getter=self._thread_ts_getter,
            config=self._status_config,
            monotonic_now=self._status_monotonic_now or clock.monotonic,
        )
        self._status_windows[key] = window
        return window

    def _structured_thread_for_key(self, key: str) -> SlackStructuredStatusThread:
        thread = self._structured_threads.get(key)
        if thread is not None:
            return thread
        thread = SlackStructuredStatusThread(
            client=self._client,
            logger=self._logger,
            source=self._source,
            channel_id_getter=self._channel_id_getter,
            thread_ts_getter=self._thread_ts_getter,
            config=self._status_config,
            monotonic_now=self._status_monotonic_now or clock.monotonic,
        )
        self._structured_threads[key] = thread
        return thread

    def _resolve_status_key(self, *, status_key: str | None) -> str:
        if status_key is not None:
            return status_key
        if isinstance(self._latest_status_key, str):
            return self._latest_status_key
        return "default"


def _normalize_status_lines(status_text: str) -> list[str]:
    lines: list[str] = []
    for raw in str(status_text).splitlines():
        line = raw.strip()
        if line:
            lines.append(line)
    return lines


def _render_status_payload(lines: list[str], *, source: str) -> str:
    status_icon = _status_icon_for_source(source)
    status_header = f"{status_icon} [status]" if status_icon else "[status]"
    if not lines:
        return status_header
    rendered = [_render_status_line_text(line) for line in lines if line.strip()]
    if not rendered:
        return status_header
    return status_header + "\n" + "\n".join(rendered)


def _status_icon_for_source(source: str) -> str:
    if source in {"steward", "telegram_steward", "slack_steward"}:
        return "🧑‍✈️"
    if source in {"agent", "telegram_agent", "slack_agent"}:
        return "🚀"
    return ""


def _render_status_line_text(line: str) -> str:
    text = line.strip()
    marker = "agentCommentary: "
    if text.startswith(marker):
        return text[len(marker) :].strip()
    if text.startswith("commandExecution "):
        return f"> {text}"
    if text.startswith("webSearch ") or text.startswith("webFetch "):
        return f"🌐 {text}"
    return text


def _render_structured_root_payload(
    *,
    meta_lines: list[str],
    snapshot: list[dict[str, str]],
    source: str,
) -> str:
    status_icon = _status_icon_for_source(source)
    header = f"{status_icon} [status]" if status_icon else "[status]"
    summary = _structured_root_summary(meta_lines=meta_lines, snapshot=snapshot)
    return f"{header}\n{summary}" if summary else header


def _structured_root_summary(*, meta_lines: list[str], snapshot: list[dict[str, str]]) -> str:
    in_progress = [item for item in snapshot if str(item.get("status") or "") == "in_progress"]
    failed = [item for item in snapshot if str(item.get("status") or "") == "failed"]
    completed = [item for item in snapshot if str(item.get("status") or "") == "completed"]
    if failed:
        return f"{_humanize_item_type(str(failed[-1].get('type') or 'work'))} failed"
    if in_progress:
        return f"{_humanize_item_type(str(in_progress[-1].get('type') or 'work'))} in progress"
    if completed:
        return f"{_humanize_item_type(str(completed[-1].get('type') or 'work'))} completed"
    if meta_lines:
        last = str(meta_lines[-1] or "").strip()
        if last == "turn/started":
            return "Working"
        return _render_status_line_text(last)
    return "Working"


def _render_structured_item_payload(item: dict[str, str]) -> str:
    single_line = format_item_status_lines([item]).strip()
    if not single_line:
        return "status updated"
    return _render_status_line_text(single_line)


def _humanize_item_type(raw_type: str) -> str:
    text = str(raw_type or "").strip()
    if not text:
        return "Work"
    mapping = {
        "agentCommentary": "Reasoning",
        "reasoning": "Reasoning",
        "commandExecution": "Command",
        "webSearch": "Web search",
        "webFetch": "Web fetch",
        "applyPatch": "Patch",
    }
    if text in mapping:
        return mapping[text]
    return text[:1].upper() + text[1:]


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
    header, body = _extract_status_header(payload)
    if header is not None:
        if not body:
            return [payload]
        if limit <= len(header):
            return [payload]
        body_parts = _split_payload(body, limit - len(header))
        return [header + part for part in body_parts]
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


def _extract_status_header(payload: str) -> tuple[str | None, str]:
    if not payload:
        return None, payload
    first_line, sep, rest = payload.partition("\n")
    if first_line == "[status]" or first_line.endswith(" [status]"):
        return first_line + sep, rest
    return None, payload
