"""Shared turn-aware status routing for multiple access points."""

from __future__ import annotations

from orchestrator.access_point_common import AccessPointId
from orchestrator.agent_identity import TurnRef, WorkStatusRef
from orchestrator.agent_inspection import AgentWorkSnapshot
from orchestrator.agent_session import WorkStatusKind
from orchestrator.interactive_driver_events import StatusStreamKey, StatusStreamKind
from orchestrator.protocol_status import format_item_status_lines
from orchestrator.status_output_runtime import StatusOutputRuntime


class TurnStatusStore:
    """Routes status updates to turn-bound or auxiliary status windows."""

    def __init__(
        self,
        *,
        output_runtime: StatusOutputRuntime,
        access_point: tuple[AccessPointId, AccessPointId | None],
        semantic_source: str,
        max_tracked_turns: int = 5,
    ) -> None:
        self._output_runtime = output_runtime
        self._access_point = access_point
        if not isinstance(semantic_source, str) or not semantic_source.strip():
            raise ValueError("turn status store requires semantic source")
        self._semantic_source = semantic_source.strip()
        self._max_tracked_turns = max(1, int(max_tracked_turns))
        self._tracked_turn_ids: list[str] = []
        self._active_turn_id: str | None = None
        self._active_aux_segment = 0
        self._active_segment_by_turn: dict[str, int] = {}
        self._item_segment_by_turn: dict[str, dict[str, int]] = {}
        self._meta_lines_by_status_key: dict[str, list[str]] = {}
        self._turn_started_meta_by_status_key: dict[str, str] = {}
        self._preserved_snapshot_by_stream: dict[
            StatusStreamKey,
            tuple[str, list[dict[str, str]], WorkStatusKind],
        ] = {}

    def clear(self, *, clear_output: bool = True) -> None:
        self._tracked_turn_ids.clear()
        self._active_turn_id = None
        self._active_aux_segment = 0
        self._active_segment_by_turn.clear()
        self._item_segment_by_turn.clear()
        self._meta_lines_by_status_key.clear()
        self._turn_started_meta_by_status_key.clear()
        self._preserved_snapshot_by_stream.clear()
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

    def apply_agent_status(
        self,
        *,
        stream_key: StatusStreamKey,
        status_text: str,
        kind: WorkStatusKind,
        status_text_kind: WorkStatusKind | None = None,
        work: AgentWorkSnapshot | None,
        item_ref: str = "",
        begins_turn: bool = False,
        preserve_work_snapshot: bool = False,
    ) -> None:
        if stream_key.access_point != self._access_point:
            raise ValueError("status stream access point does not match store")
        if stream_key.semantic_source != self._semantic_source:
            raise ValueError("status stream semantic source does not match store")
        display_kind = status_text_kind or kind
        turn_id = (
            _work_status_id(stream_key.status_ref)
            if stream_key.status_ref is not None
            else _turn_ref_status_id(stream_key.turn_ref)
        )
        self._remember_turn(turn_id)
        if kind is WorkStatusKind.TURN_COMPLETED and self._active_turn_id == turn_id:
            self.split_current_turn()
        self._update_active_turn(turn_id=turn_id, kind=kind, begins_turn=begins_turn)
        self._remember_segment_assignment(
            turn_id=turn_id,
            kind=kind,
            changed_item_id=item_ref or None,
        )
        status_key = (
            self._aux_status_key(turn_id)
            if stream_key.stream_kind is StatusStreamKind.AUXILIARY
            else self._status_key_for_turn(
                turn_id=turn_id,
                kind=kind,
                item_ref=item_ref,
            )
        )
        preserved = self._preserved_snapshot_by_stream.get(stream_key)
        if preserved is not None:
            status_key = preserved[0]
        raw_snapshot = _work_snapshot_rows(work)
        snapshot = (
            raw_snapshot
            if preserved is not None
            else self._filter_snapshot_for_status_key(
                raw_snapshot,
                status_key=status_key,
            )
        )
        if preserved is not None and snapshot:
            self._preserved_snapshot_by_stream[stream_key] = (
                status_key,
                [dict(item) for item in snapshot],
                display_kind,
            )
        elif preserve_work_snapshot and snapshot:
            self._preserved_snapshot_by_stream[stream_key] = (
                status_key,
                [dict(item) for item in snapshot],
                display_kind,
            )
        elif not snapshot and preserved is not None:
            status_key, snapshot, display_kind = preserved
            snapshot = [dict(item) for item in snapshot]
        if snapshot:
            self._set_snapshot_for_status_key(
                status_key=status_key,
                snapshot=snapshot,
                status_text_kind=display_kind,
            )
            if kind is WorkStatusKind.TURN_COMPLETED:
                self._preserved_snapshot_by_stream.pop(stream_key, None)
            return
        self._record_meta_line(status_key=status_key, status_text=status_text)
        if display_kind is WorkStatusKind.TURN_STARTED:
            normalized_status_text = str(status_text or "").strip()
            if normalized_status_text:
                self._turn_started_meta_by_status_key[status_key] = normalized_status_text
        self._set_snapshot_for_status_key(
            status_key=status_key,
            fallback_lines=self._meta_lines_for_status_key(status_key),
            status_text_kind=display_kind,
        )

    def _current_turn_id(self) -> str | None:
        if isinstance(self._active_turn_id, str) and self._active_turn_id.strip():
            return self._active_turn_id
        return None

    def _aux_status_key(self, turn_id: str) -> str:
        if self._active_aux_segment <= 0:
            return f"aux:{turn_id}"
        return f"aux:{turn_id}:segment:{self._active_aux_segment}"

    def _update_active_turn(
        self,
        *,
        turn_id: str,
        kind: WorkStatusKind,
        begins_turn: bool,
    ) -> None:
        if kind is WorkStatusKind.TURN_STARTED or begins_turn:
            self._active_turn_id = turn_id
            self._active_segment_by_turn.setdefault(turn_id, 0)
            self._item_segment_by_turn.setdefault(turn_id, {})
            if kind is WorkStatusKind.TURN_STARTED:
                return
        if kind is WorkStatusKind.TURN_COMPLETED:
            if self._active_turn_id == turn_id:
                self._active_turn_id = None
            return
        if kind is WorkStatusKind.ITEM_CHANGED and self._active_turn_id is None:
            self._active_turn_id = turn_id

    def _remember_turn(self, turn_id: str) -> None:
        if turn_id in self._tracked_turn_ids:
            return
        self._tracked_turn_ids.append(turn_id)
        while len(self._tracked_turn_ids) > self._max_tracked_turns:
            forgotten_turn = self._tracked_turn_ids.pop(0)
            self._active_segment_by_turn.pop(forgotten_turn, None)
            self._item_segment_by_turn.pop(forgotten_turn, None)
            if self._active_turn_id == forgotten_turn:
                self._active_turn_id = None
            prefixes = (f"turn:{forgotten_turn}", f"aux:{forgotten_turn}")
            stale_meta_keys = [
                key
                for key in self._meta_lines_by_status_key
                if any(
                    key == prefix or key.startswith(f"{prefix}:segment:")
                    for prefix in prefixes
                )
            ]
            for key in stale_meta_keys:
                self._meta_lines_by_status_key.pop(key, None)
                self._turn_started_meta_by_status_key.pop(key, None)
            stale_streams = [
                key
                for key in self._preserved_snapshot_by_stream
                if _status_stream_id(key) == forgotten_turn
            ]
            for key in stale_streams:
                self._preserved_snapshot_by_stream.pop(key, None)

    def _remember_segment_assignment(
        self,
        *,
        turn_id: str,
        kind: WorkStatusKind,
        changed_item_id: str | None,
    ) -> None:
        if kind is WorkStatusKind.TURN_STARTED:
            self._active_segment_by_turn.setdefault(turn_id, 0)
            self._item_segment_by_turn.setdefault(turn_id, {})
            return
        if (
            kind is not WorkStatusKind.ITEM_CHANGED
            or not isinstance(changed_item_id, str)
            or not changed_item_id.strip()
        ):
            return
        item_id = changed_item_id.strip()
        item_segments = self._item_segment_by_turn.setdefault(turn_id, {})
        item_segments.setdefault(item_id, int(self._active_segment_by_turn.get(turn_id, 0)))

    def _status_key_for_turn(
        self,
        *,
        turn_id: str,
        kind: WorkStatusKind,
        item_ref: str,
    ) -> str:
        segment = self._segment_index_for_turn(
            turn_id=turn_id,
            kind=kind,
            item_ref=item_ref,
        )
        if segment <= 0:
            return f"turn:{turn_id}"
        return f"turn:{turn_id}:segment:{segment}"

    def _segment_index_for_turn(
        self,
        *,
        turn_id: str,
        kind: WorkStatusKind,
        item_ref: str,
    ) -> int:
        if kind is WorkStatusKind.ITEM_CHANGED:
            normalized_item_ref = str(item_ref or "").strip()
            if normalized_item_ref:
                item_segments = self._item_segment_by_turn.get(turn_id, {})
                if normalized_item_ref in item_segments:
                    try:
                        return int(item_segments[normalized_item_ref])
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

    def _structured_meta_lines_for_status_key(self, status_key: str | None) -> list[str]:
        lines = self._meta_lines_for_status_key(status_key)
        if not isinstance(status_key, str):
            return lines
        turn_started = self._turn_started_meta_by_status_key.get(status_key)
        if turn_started is None:
            return lines
        try:
            lines.remove(turn_started)
        except ValueError:
            pass
        return lines

    def _record_meta_line(self, *, status_key: str, status_text: str) -> None:
        text = str(status_text or "").strip()
        if not text:
            return
        lines = self._meta_lines_by_status_key.setdefault(status_key, [])
        if lines and lines[-1] == text:
            return
        lines.append(text)
        while len(self._meta_lines_by_status_key) > self._max_tracked_turns:
            oldest_key = next(iter(self._meta_lines_by_status_key))
            self._meta_lines_by_status_key.pop(oldest_key, None)
            self._turn_started_meta_by_status_key.pop(oldest_key, None)

    def _set_snapshot_for_status_key(
        self,
        *,
        status_key: str | None,
        snapshot: list[dict[str, str]] | None = None,
        fallback_lines: list[str] | None = None,
        status_text_kind: WorkStatusKind,
    ) -> None:
        meta_lines = self._meta_lines_for_status_key(status_key)
        structured_setter = getattr(self._output_runtime, "set_status_snapshot_structured", None)
        if (
            callable(structured_setter)
            and isinstance(status_key, str)
            and status_key.startswith("turn:")
        ):
            structured_setter(
                status_key=status_key,
                meta_lines=(
                    []
                    if status_text_kind
                    in {WorkStatusKind.TURN_STARTED, WorkStatusKind.TURN_COMPLETED}
                    else self._structured_meta_lines_for_status_key(status_key)
                ),
                snapshot=list(snapshot or []),
            )
        else:
            lines: list[str] = []
            display_meta_lines = (
                self._structured_meta_lines_for_status_key(status_key)
                if snapshot
                else meta_lines
            )
            lines.extend(display_meta_lines)
            if snapshot:
                snapshot_text = format_item_status_lines(snapshot)
                if snapshot_text.strip():
                    lines.extend([line for line in snapshot_text.splitlines() if line.strip()])
            elif fallback_lines:
                filtered_fallback = [
                    line for line in fallback_lines if isinstance(line, str) and line.strip()
                ]
                if filtered_fallback != display_meta_lines:
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
        if snapshot is not None and isinstance(status_key, str) and status_key.startswith("turn:"):
            self._output_runtime.set_status_in_progress(
                status_key=status_key,
                in_progress=any(item.get("status") == "in_progress" for item in snapshot),
            )

def _turn_ref_status_id(turn_ref: TurnRef) -> str:
    return f"orc:{turn_ref.runtime_generation.value}:{turn_ref.sequence}"


def _work_status_id(status_ref: WorkStatusRef) -> str:
    return f"work:{status_ref.runtime_generation.value}:{status_ref.sequence}"


def _status_stream_id(stream_key: StatusStreamKey) -> str:
    if stream_key.status_ref is not None:
        return _work_status_id(stream_key.status_ref)
    return _turn_ref_status_id(stream_key.turn_ref)


def _work_snapshot_rows(work: AgentWorkSnapshot | None) -> list[dict[str, str]]:
    if work is None:
        return []
    return [
        {
            "id": item.item_ref,
            "type": item.kind,
            "status": item.state,
            "detail": "; ".join(item.details),
        }
        for item in work.items
    ]
