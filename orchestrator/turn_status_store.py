"""Shared turn-aware status routing for multiple access points."""

from __future__ import annotations

from typing import Any, Callable

from orchestrator.protocol_status import (
    ProtocolItemTracker,
    format_item_status_lines,
    format_protocol_status,
    should_emit_status_method,
)
from orchestrator.status_output_runtime import StatusOutputRuntime


class TurnStatusStore:
    """Routes status updates to turn-bound or auxiliary status windows."""

    def __init__(self, *, output_runtime: StatusOutputRuntime) -> None:
        self._output_runtime = output_runtime
        self._tracker = ProtocolItemTracker(max_tracked_turns=64)
        self._editable_turn_window = 5
        self._seen_turn_started = False
        self._active_turn_id: str | None = None
        self._active_aux_segment = 0
        self._active_segment_by_turn: dict[str, int] = {}
        self._item_segment_by_turn: dict[str, dict[str, int]] = {}
        self._meta_lines_by_status_key: dict[str, list[str]] = {}
        self._suppressed_segments_by_turn: dict[str, set[int]] = {}

    def clear(self, *, clear_output: bool = True) -> None:
        self._tracker.clear()
        self._seen_turn_started = False
        self._active_turn_id = None
        self._active_aux_segment = 0
        self._active_segment_by_turn.clear()
        self._item_segment_by_turn.clear()
        self._meta_lines_by_status_key.clear()
        self._suppressed_segments_by_turn.clear()
        if clear_output:
            self._output_runtime.clear_status()

    def split_current_turn(self) -> None:
        turn_id = self._current_turn_id()
        if not isinstance(turn_id, str) or not turn_id.strip():
            self._active_aux_segment += 1
            return
        normalized_turn_id = turn_id.strip()
        current_segment = int(self._active_segment_by_turn.get(normalized_turn_id, 0))
        self._active_segment_by_turn[normalized_turn_id] = current_segment + 1

    def flush_current_turn(self) -> None:
        status_key = self.current_turn_status_key()
        if status_key is None:
            return
        self._output_runtime.flush_status(status_key=status_key)

    def current_turn_status_key(self) -> str | None:
        turn_id = self._current_turn_id()
        if not isinstance(turn_id, str) or not turn_id.strip():
            return None
        normalized_turn_id = turn_id.strip()
        current_segment = int(self._active_segment_by_turn.get(normalized_turn_id, 0))
        if current_segment <= 0:
            return f"turn:{normalized_turn_id}"
        return f"turn:{normalized_turn_id}:segment:{current_segment}"

    def suppress_current_turn_segment(self) -> None:
        turn_id = self._current_turn_id()
        if not isinstance(turn_id, str) or not turn_id.strip():
            return
        normalized_turn_id = turn_id.strip()
        current_segment = int(self._active_segment_by_turn.get(normalized_turn_id, 0))
        self._suppressed_segments_by_turn.setdefault(normalized_turn_id, set()).add(current_segment)

    def apply_protocol_event(self, *, method: str, params: dict[str, Any] | None) -> None:
        normalized_params = params if isinstance(params, dict) else {}
        changed_item_id = self._tracker.apply(method=method, params=normalized_params)
        info = self._tracker.last_apply_info() or {}
        self._update_active_turn_from_info(info)
        self._remember_segment_assignment(info=info, changed_item_id=changed_item_id)
        if _is_turn_started_event(info=info, status_text=method):
            self._seen_turn_started = True
        elif _is_stale_pre_turn_item_event(
            info=info,
            status_text=method,
            seen_turn_started=self._seen_turn_started,
        ):
            return
        if _is_ignored_forgotten_turn_event(info=info):
            return
        turn_id = self._effective_turn_id(info=info, method=method, params=normalized_params)
        status_key = self._status_key_for_target(turn_id=turn_id, info=info)
        if turn_id:
            if _is_frozen_late_update(info=info, editable_turn_window=self._editable_turn_window):
                return
            if self._is_suppressed_segment(turn_id=turn_id, info=info):
                return
            snapshot = self._tracker.snapshot_for_turn(turn_id)
            if snapshot:
                self._set_snapshot_for_status_key(
                    status_key=status_key,
                    snapshot=self._filter_snapshot_for_status_key(snapshot, status_key=status_key),
                )
                return
        if not should_emit_status_method(method):
            return
        status_text = format_protocol_status(method=method, params=normalized_params)
        if turn_id:
            if _is_turn_started_event(info=info, status_text=method):
                self._record_meta_line(status_key=status_key, status_text=status_text)
                self._set_snapshot_for_status_key(
                    status_key=status_key,
                    fallback_lines=[status_text],
                )
                return
            self._record_meta_line(status_key=status_key, status_text=status_text)
            self._set_snapshot_for_status_key(
                status_key=status_key,
                fallback_lines=self._meta_lines_for_status_key(status_key),
            )
            return
        self._record_meta_line(status_key=status_key, status_text=status_text)
        self._set_snapshot_for_status_key(
            status_key=status_key,
            fallback_lines=self._meta_lines_for_status_key(status_key),
        )

    def apply_backend_status(
        self,
        *,
        status_text: str,
        snapshot_getter: Callable[[], list[dict[str, str]]],
        snapshot_for_turn_getter: Callable[[str], list[dict[str, str]]] | None = None,
        apply_info_getter: Callable[[], dict[str, Any] | None] | None = None,
    ) -> None:
        if not should_emit_status_method(status_text):
            return
        apply_info = apply_info_getter() if callable(apply_info_getter) else None
        info = apply_info if isinstance(apply_info, dict) else {}
        self._update_active_turn_from_info(info)
        changed_item_id_raw = info.get("item_id")
        changed_item_id = changed_item_id_raw.strip() if isinstance(changed_item_id_raw, str) else None
        self._remember_segment_assignment(info=info, changed_item_id=changed_item_id)
        if _is_turn_started_event(info=info, status_text=status_text):
            self._seen_turn_started = True
        elif _is_stale_pre_turn_item_event(
            info=info,
            status_text=status_text,
            seen_turn_started=self._seen_turn_started,
        ):
            return
        if _is_ignored_forgotten_turn_event(info=info):
            return
        turn_id = self._effective_turn_id(info=info, method=status_text, params=None)
        status_key = self._status_key_for_target(turn_id=turn_id, info=info)
        if turn_id:
            if _is_frozen_late_update(info=info, editable_turn_window=self._editable_turn_window):
                return
            if self._is_suppressed_segment(turn_id=turn_id, info=info):
                return
            if callable(snapshot_for_turn_getter):
                snapshot = list(snapshot_for_turn_getter(turn_id))
            else:
                snapshot = list(snapshot_getter())
            if snapshot:
                self._set_snapshot_for_status_key(
                    status_key=status_key,
                    snapshot=self._filter_snapshot_for_status_key(snapshot, status_key=status_key),
                )
                return
            if _is_turn_started_event(info=info, status_text=status_text):
                self._record_meta_line(status_key=status_key, status_text=status_text)
                self._set_snapshot_for_status_key(
                    status_key=status_key,
                    fallback_lines=[status_text],
                )
                return
            self._record_meta_line(status_key=status_key, status_text=status_text)
            self._set_snapshot_for_status_key(
                status_key=status_key,
                fallback_lines=self._meta_lines_for_status_key(status_key),
            )
            return
        self._record_meta_line(status_key=status_key, status_text=status_text)
        self._set_snapshot_for_status_key(
            status_key=status_key,
            fallback_lines=self._meta_lines_for_status_key(status_key),
        )

    def _current_turn_id(self) -> str | None:
        if isinstance(self._active_turn_id, str) and self._active_turn_id.strip():
            return self._active_turn_id
        return self._tracker.current_turn_id()

    def _aux_status_key(self) -> str:
        if self._active_aux_segment <= 0:
            return "aux"
        return f"aux:segment:{self._active_aux_segment}"

    def _effective_turn_id(
        self,
        *,
        info: dict[str, Any],
        method: str,
        params: dict[str, Any] | None,
    ) -> str | None:
        if _force_aux_status_event(method):
            return None
        resolved = _resolve_turn_id(info=info, status_text=method)
        if resolved:
            return resolved
        direct = _direct_turn_id(params=params)
        if direct:
            return direct
        current = self._current_turn_id()
        if current:
            return current
        return None

    def _status_key_for_target(self, *, turn_id: str | None, info: dict[str, Any]) -> str:
        resolved = self._status_key_for_turn(turn_id=turn_id, info=info)
        if resolved:
            return resolved
        return self._aux_status_key()

    def _update_active_turn_from_info(self, info: dict[str, Any]) -> None:
        kind = str(info.get("kind") or "").strip()
        turn_id_raw = info.get("turn_id")
        turn_id = turn_id_raw.strip() if isinstance(turn_id_raw, str) else ""
        if not turn_id:
            return
        if kind == "turn_started":
            self._active_turn_id = turn_id
            self._active_segment_by_turn.setdefault(turn_id, 0)
            self._item_segment_by_turn.setdefault(turn_id, {})
            return
        if kind == "turn_completed":
            if self._active_turn_id == turn_id:
                self._active_turn_id = None
            return
        if kind == "item_changed" and self._active_turn_id is None:
            turn_status = str(info.get("turn_status") or "").strip()
            if turn_status == "in_progress" or bool(info.get("has_active_turn")):
                self._active_turn_id = turn_id

    def _remember_segment_assignment(self, *, info: dict[str, Any], changed_item_id: str | None) -> None:
        turn_id_raw = info.get("turn_id")
        turn_id = turn_id_raw.strip() if isinstance(turn_id_raw, str) else ""
        if not turn_id:
            return
        kind = str(info.get("kind") or "").strip()
        if kind == "turn_started":
            self._active_segment_by_turn.setdefault(turn_id, 0)
            self._item_segment_by_turn.setdefault(turn_id, {})
            return
        if kind != "item_changed" or not isinstance(changed_item_id, str) or not changed_item_id.strip():
            return
        item_id = changed_item_id.strip()
        item_segments = self._item_segment_by_turn.setdefault(turn_id, {})
        item_segments.setdefault(item_id, int(self._active_segment_by_turn.get(turn_id, 0)))

    def _status_key_for_turn(self, *, turn_id: str | None, info: dict[str, Any]) -> str | None:
        if not turn_id:
            return None
        segment = self._segment_index_for_turn(turn_id=turn_id, info=info)
        if segment <= 0:
            return f"turn:{turn_id}"
        return f"turn:{turn_id}:segment:{segment}"

    def _segment_index_for_turn(self, *, turn_id: str, info: dict[str, Any]) -> int:
        kind = str(info.get("kind") or "").strip()
        if kind == "item_changed":
            item_id_raw = info.get("item_id")
            item_id = item_id_raw.strip() if isinstance(item_id_raw, str) else ""
            if item_id:
                item_segments = self._item_segment_by_turn.get(turn_id, {})
                if item_id in item_segments:
                    try:
                        return int(item_segments[item_id])
                    except (TypeError, ValueError):
                        return 0
        try:
            return int(self._active_segment_by_turn.get(turn_id, 0))
        except (TypeError, ValueError):
            return 0

    def _filter_snapshot_for_status_key(
        self,
        snapshot: list[dict[str, str]],
        *,
        status_key: str | None,
    ) -> list[dict[str, str]]:
        if not snapshot:
            return snapshot
        if not isinstance(status_key, str) or not status_key.startswith("turn:"):
            return snapshot
        suffix = status_key.split(":segment:", 1)
        turn_id = suffix[0].removeprefix("turn:")
        segment_idx = 0
        if len(suffix) == 2:
            try:
                segment_idx = int(suffix[1])
            except (TypeError, ValueError):
                return snapshot
        item_segments = self._item_segment_by_turn.get(turn_id)
        if not item_segments:
            return snapshot if segment_idx == 0 else []
        return [
            item
            for item in snapshot
            if int(item_segments.get(str(item.get("id") or ""), 0)) == segment_idx
        ]

    def _meta_lines_for_status_key(self, status_key: str | None) -> list[str]:
        if not isinstance(status_key, str) or not status_key:
            return []
        return list(self._meta_lines_by_status_key.get(status_key, []))

    def _record_meta_line(self, *, status_key: str, status_text: str) -> None:
        text = str(status_text or "").strip()
        if not text:
            return
        lines = self._meta_lines_by_status_key.setdefault(status_key, [])
        if lines and lines[-1] == text:
            return
        lines.append(text)

    def _set_snapshot_for_status_key(
        self,
        *,
        status_key: str | None,
        snapshot: list[dict[str, str]] | None = None,
        fallback_lines: list[str] | None = None,
    ) -> None:
        meta_lines = self._meta_lines_for_status_key(status_key)
        if snapshot:
            meta_lines = [line for line in meta_lines if line != "turn/started"]
        structured_setter = getattr(self._output_runtime, "set_status_snapshot_structured", None)
        if (
            callable(structured_setter)
            and isinstance(status_key, str)
            and status_key.startswith("turn:")
        ):
            structured_setter(
                status_key=status_key,
                meta_lines=list(meta_lines),
                snapshot=list(snapshot or []),
            )
            return
        lines: list[str] = []
        lines.extend(meta_lines)
        if snapshot:
            snapshot_text = format_item_status_lines(snapshot)
            if snapshot_text.strip():
                lines.extend([line for line in snapshot_text.splitlines() if line.strip()])
        elif fallback_lines:
            filtered_fallback = [
                line for line in fallback_lines if isinstance(line, str) and line.strip()
            ]
            if filtered_fallback != meta_lines:
                lines.extend(filtered_fallback)
        deduped: list[str] = []
        for line in lines:
            if deduped and deduped[-1] == line:
                continue
            deduped.append(line)
        self._output_runtime.set_status_snapshot(
            "\n".join(deduped),
            status_key=status_key,
        )

    def _is_suppressed_segment(self, *, turn_id: str, info: dict[str, Any]) -> bool:
        suppressed = self._suppressed_segments_by_turn.get(turn_id)
        if not suppressed:
            return False
        return self._segment_index_for_turn(turn_id=turn_id, info=info) in suppressed


def _resolve_turn_id(*, info: dict[str, Any], status_text: str) -> str | None:
    raw_turn_id = info.get("turn_id")
    if not isinstance(raw_turn_id, str) or not raw_turn_id.strip():
        return None
    turn_id = raw_turn_id.strip()
    kind = str(info.get("kind") or "").strip()
    if kind == "item_changed":
        return turn_id
    if kind in ("turn_started", "turn_completed") and status_text in ("turn/started", "turn/completed"):
        return turn_id
    return None


def _is_frozen_late_update(*, info: dict[str, Any], editable_turn_window: int) -> bool:
    kind = str(info.get("kind") or "").strip()
    if kind != "item_changed":
        return False
    late_distance = info.get("late_distance")
    if not isinstance(late_distance, int):
        return False
    return late_distance >= max(1, int(editable_turn_window))


def _is_turn_started_event(*, info: dict[str, Any], status_text: str) -> bool:
    kind = str(info.get("kind") or "").strip()
    return kind == "turn_started" and status_text == "turn/started"


def _is_stale_pre_turn_item_event(
    *,
    info: dict[str, Any],
    status_text: str,
    seen_turn_started: bool,
) -> bool:
    if seen_turn_started:
        return False
    if str(info.get("kind") or "").strip() != "item_changed":
        return False
    return status_text.startswith("item/")


def _is_ignored_forgotten_turn_event(*, info: dict[str, Any]) -> bool:
    return str(info.get("kind") or "").strip() == "item_ignored_forgotten_turn"


def _direct_turn_id(*, params: dict[str, Any] | None) -> str | None:
    if not isinstance(params, dict):
        return None
    direct = params.get("turnId")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()
    direct_snake = params.get("turn_id")
    if isinstance(direct_snake, str) and direct_snake.strip():
        return direct_snake.strip()
    msg = params.get("msg")
    if isinstance(msg, dict):
        msg_turn_id = msg.get("turnId")
        if isinstance(msg_turn_id, str) and msg_turn_id.strip():
            return msg_turn_id.strip()
        msg_turn_id_snake = msg.get("turn_id")
        if isinstance(msg_turn_id_snake, str) and msg_turn_id_snake.strip():
            return msg_turn_id_snake.strip()
    return None


def _force_aux_status_event(status_value: str) -> bool:
    normalized = str(status_value or "").strip()
    if normalized in {
        "codex/event/task_started",
        "codex/event/mcp_startup_update",
        "codex/event/mcp_startup_complete",
    }:
        return True
    return (
        normalized.startswith("task started ")
        or normalized.startswith("mcp startup:")
        or normalized.startswith("mcp startup complete")
    )
