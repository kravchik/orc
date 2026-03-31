"""Helpers for protocol progress lines in console/TUI/telegram."""

from __future__ import annotations
from collections import OrderedDict
from pathlib import Path
from typing import Any

IGNORED_STATUS_METHODS = {
    # These are verbose codex/event deltas mirrored by item/* events.
    "codex/event/agent_message_content_delta",
    "codex/event/agent_message_delta",
    "codex/event/reasoning_content_delta",
    "codex/event/agent_reasoning_delta",
    # Prefer canonical item/* lifecycle events in status line.
    "codex/event/item_started",
    "codex/event/item_completed",
    # Noisy lifecycle/mirror events not useful for user-facing status line.
    "thread/started",
    "codex/event/user_message",
}


def should_emit_status_method(method: str) -> bool:
    return method not in IGNORED_STATUS_METHODS


class ProtocolItemTracker:
    """Tracks protocol item lifecycle from item/* events."""

    def __init__(self, *, max_tracked_turns: int = 5) -> None:
        self._turn_items: "OrderedDict[str, OrderedDict[str, dict[str, str]]]" = OrderedDict()
        self._turn_order: list[str] = []
        self._turn_status: dict[str, str] = {}
        self._current_turn_id: str | None = None
        self._last_completed_turn_id: str | None = None
        self._command_output_stats: dict[tuple[str, str], dict[str, Any]] = {}
        self._aux_turn_id = "__aux__"
        self._max_tracked_turns = max(1, int(max_tracked_turns))
        self._last_apply_info: dict[str, Any] | None = None
        self._forgotten_turn_ids: "OrderedDict[str, None]" = OrderedDict()

    def clear(self) -> None:
        self._turn_items.clear()
        self._turn_order.clear()
        self._turn_status.clear()
        self._current_turn_id = None
        self._last_completed_turn_id = None
        self._command_output_stats.clear()
        self._last_apply_info = None
        self._forgotten_turn_ids.clear()

    def current_turn_id(self) -> str | None:
        if not isinstance(self._current_turn_id, str) or not self._current_turn_id.strip():
            return None
        if self._turn_status.get(self._current_turn_id) != "in_progress":
            return None
        return self._current_turn_id

    def last_apply_info(self) -> dict[str, Any] | None:
        if self._last_apply_info is None:
            return None
        return dict(self._last_apply_info)

    def apply(self, method: str, params: dict[str, Any] | None) -> str | None:
        if method == "turn/started":
            turn_id = _extract_turn_id(method=method, params=params)
            if not turn_id:
                return None
            self._current_turn_id = turn_id
            self._turn_status[turn_id] = "in_progress"
            self._ensure_turn(turn_id)
            self._prune_turns()
            self._last_apply_info = {
                "kind": "turn_started",
                "turn_id": turn_id,
            }
            return None
        if method == "turn/completed":
            turn_id = _extract_turn_id(method=method, params=params)
            if not turn_id:
                return None
            self._turn_status[turn_id] = "completed"
            if self._current_turn_id == turn_id:
                self._current_turn_id = None
            self._last_completed_turn_id = turn_id
            self._ensure_turn(turn_id)
            self._prune_turns()
            self._last_apply_info = {
                "kind": "turn_completed",
                "turn_id": turn_id,
            }
            return None

        current_turn_id = self.current_turn_id()
        extracted_turn_id = _extract_turn_id(method=method, params=params)
        if extracted_turn_id and extracted_turn_id in self._forgotten_turn_ids:
            self._last_apply_info = {
                "kind": "item_ignored_forgotten_turn",
                "turn_id": extracted_turn_id,
                "method": method,
                "late_distance": self._max_tracked_turns,
                "is_late": True,
                "has_active_turn": current_turn_id is not None,
                "turn_status": "forgotten",
            }
            return None
        turn_id = extracted_turn_id or current_turn_id or self._aux_turn_id
        self._ensure_turn(turn_id)

        if method == "item/commandExecution/outputDelta":
            payload = self._extract_command_output_payload(params, turn_id=turn_id)
        else:
            payload = _extract_item_payload(method=method, params=params)
            if method == "item/completed" and isinstance(params, dict):
                item = params.get("item")
                if isinstance(item, dict):
                    raw_id = item.get("id")
                    if isinstance(raw_id, str) and raw_id:
                        self._command_output_stats.pop((turn_id, raw_id), None)
        if payload is None:
            return None
        item_id = payload["id"]
        bucket = self._turn_items[turn_id]
        current = bucket.get(item_id)
        if current == payload:
            return None
        bucket[item_id] = payload
        late_distance = self._late_distance(turn_id)
        self._last_apply_info = {
            "kind": "item_changed",
            "turn_id": turn_id,
            "item_id": item_id,
            "method": method,
            "late_distance": late_distance,
            "is_late": late_distance is not None and late_distance > 0,
            "has_active_turn": current_turn_id is not None,
            "turn_status": self._turn_status.get(turn_id),
        }
        self._prune_turns()
        return item_id

    def _extract_command_output_payload(self, params: dict[str, Any] | None, *, turn_id: str) -> dict[str, str] | None:
        if not isinstance(params, dict):
            return None
        raw_item_id = params.get("itemId")
        if not isinstance(raw_item_id, str) or not raw_item_id.strip():
            return None
        item_id = raw_item_id.strip()
        key = (turn_id, item_id)
        stats = self._command_output_stats.get(key)
        if stats is None:
            stats = {"chars": 0, "streams": set()}
            self._command_output_stats[key] = stats
        delta = params.get("delta")
        if isinstance(delta, str):
            stats["chars"] = int(stats.get("chars", 0)) + len(delta)
        stream = params.get("stream")
        if not isinstance(stream, str) or not stream.strip():
            stream = params.get("source")
        if isinstance(stream, str) and stream.strip():
            streams = stats.get("streams")
            if isinstance(streams, set):
                streams.add(stream.strip())
        chars = int(stats.get("chars", 0))
        detail_parts = [f"output_chars={chars}"]
        streams = stats.get("streams")
        if isinstance(streams, set) and streams:
            detail_parts.append(f"streams={','.join(sorted(str(v) for v in streams))}")
        detail = "; ".join(detail_parts)
        return {
            "id": item_id,
            "type": "commandExecution",
            "status": "in_progress",
            "detail": detail,
        }

    def snapshot(self) -> list[dict[str, str]]:
        selected_turn_id = self._current_turn_id
        if selected_turn_id is None or selected_turn_id not in self._turn_items:
            selected_turn_id = self._last_completed_turn_id
        if selected_turn_id is None or selected_turn_id not in self._turn_items:
            selected_turn_id = self._turn_order[-1] if self._turn_order else self._aux_turn_id
        return self.snapshot_for_turn(selected_turn_id)

    def snapshot_for_turn(self, turn_id: str | None) -> list[dict[str, str]]:
        selected_turn_id = turn_id or self._aux_turn_id
        bucket = self._turn_items.get(selected_turn_id)
        if not isinstance(bucket, OrderedDict):
            return []
        return [
            {
                "id": item_id,
                "type": item["type"],
                "status": item["status"],
                "detail": item.get("detail", ""),
            }
            for item_id, item in bucket.items()
        ]

    def _ensure_turn(self, turn_id: str) -> None:
        if turn_id not in self._turn_items:
            self._turn_items[turn_id] = OrderedDict()
            self._turn_order.append(turn_id)

    def _prune_turns(self) -> None:
        non_aux = [turn_id for turn_id in self._turn_order if turn_id != self._aux_turn_id]
        if len(non_aux) <= self._max_tracked_turns:
            return
        to_remove = non_aux[: len(non_aux) - self._max_tracked_turns]
        for turn_id in to_remove:
            self._turn_items.pop(turn_id, None)
            self._turn_status.pop(turn_id, None)
            self._turn_order = [value for value in self._turn_order if value != turn_id]
            if self._current_turn_id == turn_id:
                self._current_turn_id = None
            if self._last_completed_turn_id == turn_id:
                self._last_completed_turn_id = None
            self._remember_forgotten_turn(turn_id)
        keys = {key for key in self._command_output_stats.keys() if key[0] in to_remove}
        for key in keys:
            self._command_output_stats.pop(key, None)

    def _remember_forgotten_turn(self, turn_id: str) -> None:
        if not isinstance(turn_id, str) or not turn_id or turn_id == self._aux_turn_id:
            return
        self._forgotten_turn_ids.pop(turn_id, None)
        self._forgotten_turn_ids[turn_id] = None
        while len(self._forgotten_turn_ids) > (self._max_tracked_turns * 4):
            self._forgotten_turn_ids.popitem(last=False)

    def _late_distance(self, turn_id: str) -> int | None:
        if turn_id == self._aux_turn_id:
            return None
        current_turn_id = self.current_turn_id()
        if not current_turn_id:
            return None
        non_aux = [value for value in self._turn_order if value != self._aux_turn_id]
        if turn_id not in non_aux or current_turn_id not in non_aux:
            return None
        turn_idx = non_aux.index(turn_id)
        current_idx = non_aux.index(current_turn_id)
        if turn_idx >= current_idx:
            return 0
        return current_idx - turn_idx


def format_item_status_summary(snapshot: list[dict[str, str]], *, changed_item_id: str | None = None) -> str:
    if not snapshot:
        return "items: none"
    in_progress = sum(1 for item in snapshot if item.get("status") == "in_progress")
    completed = sum(1 for item in snapshot if item.get("status") == "completed")
    _ = changed_item_id
    return f"items: total={len(snapshot)}, in_progress={in_progress}, completed={completed}"


def format_item_status_lines(snapshot: list[dict[str, str]]) -> str:
    if not snapshot:
        return "items: none"
    lines: list[str] = []
    for item in snapshot:
        item_type = str(item.get("type", "item"))
        status = str(item.get("status", "unknown"))
        detail = str(item.get("detail", "")).strip()
        normalized_type = item_type[:1].lower() + item_type[1:] if item_type else item_type
        if normalized_type == "agentCommentary":
            if detail:
                lines.append(f"agentCommentary: {detail}")
            continue
        item_type = normalized_type
        if detail:
            lines.append(f"{item_type} {status}: {detail}")
        else:
            lines.append(f"{item_type} {status}")
    return "\n".join(lines)


def format_protocol_status(method: str, params: dict | None = None) -> str:
    task_started = _format_task_started_status(method=method, params=params)
    if task_started is not None:
        return task_started
    mcp_status = _format_mcp_startup_status(method=method, params=params)
    if mcp_status is not None:
        return mcp_status
    thread_status = _format_thread_status_changed(method=method, params=params)
    if thread_status is not None:
        return thread_status
    rate_limits = _format_rate_limits_status(method=method, params=params)
    if rate_limits is not None:
        return rate_limits
    usage = _format_context_usage_status(method=method, params=params)
    if usage is not None:
        return usage
    return method


def normalize_status_source(source: str) -> str:
    key = str(source or "").strip().lower()
    if key == "lead":
        return "LEAD"
    if key == "worker":
        return "WORKER"
    if key == "agent":
        return "AGENT"
    if key == "steward":
        return "STEWARD"
    if not key:
        return "UNKNOWN"
    return key.upper()


def format_status_with_source(source: str, text: str) -> str:
    return f"[{normalize_status_source(source)}] {text}"


def format_protocol_status_with_role(
    role: str,
    method: str,
    params: dict | None = None,
) -> str:
    return format_status_with_source(
        source=role,
        text=format_protocol_status(method=method, params=params),
    )


def _format_context_usage_status(method: str, params: dict | None) -> str | None:
    if method != "thread/tokenUsage/updated" or not isinstance(params, dict):
        return None
    token_usage = params.get("tokenUsage")
    if not isinstance(token_usage, dict):
        return None
    total_obj = token_usage.get("total")
    if not isinstance(total_obj, dict):
        return None
    total_tokens = total_obj.get("totalTokens")
    model_context = token_usage.get("modelContextWindow")
    if not isinstance(total_tokens, int) or not isinstance(model_context, int) or model_context <= 0:
        return None
    pct = max(0.0, min(100.0, (float(total_tokens) / float(model_context)) * 100.0))
    return f"thread/tokenUsage/updated ({pct:.1f}% ctx: {total_tokens}/{model_context})"


def _format_task_started_status(method: str, params: dict | None) -> str | None:
    if method != "codex/event/task_started":
        return None
    if not isinstance(params, dict):
        return "task started"
    msg = params.get("msg")
    if not isinstance(msg, dict):
        return "task started"
    parts: list[str] = []
    model_context_window = msg.get("model_context_window")
    if isinstance(model_context_window, int) and model_context_window > 0:
        parts.append(f"ctx window: {model_context_window}")
    collaboration_mode = msg.get("collaboration_mode_kind")
    if isinstance(collaboration_mode, str) and collaboration_mode.strip():
        parts.append(f"mode: {collaboration_mode.strip()}")
    if not parts:
        return "task started"
    return f"task started ({'; '.join(parts)})"


def _format_mcp_startup_status(method: str, params: dict | None) -> str | None:
    if not isinstance(params, dict):
        return None
    msg = params.get("msg")
    if not isinstance(msg, dict):
        return None
    if method == "codex/event/mcp_startup_update":
        server = str(msg.get("server") or "").strip()
        state = ""
        status_obj = msg.get("status")
        if isinstance(status_obj, dict):
            raw_state = status_obj.get("state")
            if isinstance(raw_state, str):
                state = raw_state.strip()
        if server and state:
            return f"mcp startup: {server} {state}"
        if server:
            return f"mcp startup: {server}"
        if state:
            return f"mcp startup: {state}"
        return "mcp startup"
    if method != "codex/event/mcp_startup_complete":
        return None
    parts: list[str] = []
    for key in ("ready", "failed", "cancelled"):
        raw_values = msg.get(key)
        if not isinstance(raw_values, list):
            continue
        values = [str(value).strip() for value in raw_values if isinstance(value, str) and str(value).strip()]
        if values:
            parts.append(f"{key}={','.join(values)}")
    if not parts:
        return "mcp startup complete"
    return f"mcp startup complete ({'; '.join(parts)})"


def _format_thread_status_changed(method: str, params: dict | None) -> str | None:
    if method != "thread/status/changed" or not isinstance(params, dict):
        return None
    raw_status = params.get("status")
    if isinstance(raw_status, str):
        text = raw_status.strip()
        if not text:
            return "thread status changed"
        return f"thread status: {text}"
    if not isinstance(raw_status, dict):
        return "thread status changed"
    status_type = str(raw_status.get("type") or "").strip()
    active_flags_raw = raw_status.get("activeFlags")
    active_flags: list[str] = []
    if isinstance(active_flags_raw, list):
        active_flags = [
            str(value).strip()
            for value in active_flags_raw
            if isinstance(value, str) and str(value).strip()
        ]
    if "waitingOnApproval" in active_flags:
        return "thread status: waiting on approval"
    if status_type and active_flags:
        return f"thread status: {status_type} ({', '.join(active_flags)})"
    if status_type:
        return f"thread status: {status_type}"
    if active_flags:
        return f"thread status: {', '.join(active_flags)}"
    return "thread status changed"


def _format_rate_limits_status(method: str, params: dict | None) -> str | None:
    if method != "account/rateLimits/updated" or not isinstance(params, dict):
        return None
    raw_limits = params.get("rateLimits")
    if not isinstance(raw_limits, dict):
        return "rate limits updated"
    parts: list[str] = []
    primary = raw_limits.get("primary")
    if isinstance(primary, dict):
        used = primary.get("usedPercent")
        if isinstance(used, int | float):
            parts.append(f"primary {int(used)}%")
    secondary = raw_limits.get("secondary")
    if isinstance(secondary, dict):
        used = secondary.get("usedPercent")
        if isinstance(used, int | float):
            parts.append(f"secondary {int(used)}%")
    if not parts:
        return "rate limits updated"
    return f"rate limits: {', '.join(parts)}"


def _extract_item_payload(method: str, params: dict[str, Any] | None) -> dict[str, str] | None:
    if not isinstance(params, dict):
        return None
    status: str | None = None
    item_obj: dict[str, Any] | None = None
    if method == "item/started":
        status = "in_progress"
        raw = params.get("item")
        item_obj = raw if isinstance(raw, dict) else None
    elif method == "item/completed":
        status = "completed"
        raw = params.get("item")
        item_obj = raw if isinstance(raw, dict) else None
    elif method == "codex/event/item_started":
        status = "in_progress"
        raw = params.get("msg")
        if isinstance(raw, dict):
            nested = raw.get("item")
            item_obj = nested if isinstance(nested, dict) else None
    elif method == "codex/event/item_completed":
        status = "completed"
        raw = params.get("msg")
        if isinstance(raw, dict):
            nested = raw.get("item")
            item_obj = nested if isinstance(nested, dict) else None
    if status is None or item_obj is None:
        return None
    raw_id = item_obj.get("id")
    if not isinstance(raw_id, str) or raw_id.strip() == "":
        return None
    raw_type = item_obj.get("type")
    item_type = str(raw_type).strip() if raw_type is not None else "item"
    if item_type == "":
        item_type = "item"
    item_type = _normalize_item_type(item_type)
    normalized_type = item_type[:1].lower() + item_type[1:] if item_type else item_type
    if normalized_type == "agentMessage":
        phase = _compact(item_obj.get("phase"))
        if phase == "commentary":
            item_type = "agentCommentary"
    if _is_hidden_item(item_type=item_type, item_obj=item_obj):
        return None
    detail = _build_item_detail(item_type=item_type, item_obj=item_obj)
    return {"id": raw_id, "type": item_type, "status": status, "detail": detail}


def is_hidden_item_status_event(method: str, params: dict[str, Any] | None) -> bool:
    item_obj = _extract_item_obj(method=method, params=params)
    if item_obj is None:
        return False
    raw_type = item_obj.get("type")
    item_type = str(raw_type).strip() if raw_type is not None else "item"
    if item_type == "":
        item_type = "item"
    return _is_hidden_item(item_type=item_type, item_obj=item_obj)


def _extract_item_obj(method: str, params: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(params, dict):
        return None
    if method in ("item/started", "item/completed"):
        raw = params.get("item")
        return raw if isinstance(raw, dict) else None
    if method in ("codex/event/item_started", "codex/event/item_completed"):
        raw = params.get("msg")
        if isinstance(raw, dict):
            nested = raw.get("item")
            return nested if isinstance(nested, dict) else None
    return None


def _is_hidden_item(*, item_type: str, item_obj: dict[str, Any]) -> bool:
    normalized_type = item_type[:1].lower() + item_type[1:] if item_type else item_type
    if normalized_type == "userMessage":
        return True
    if normalized_type == "agentCommentary":
        return False
    if normalized_type == "agentMessage":
        return True
    return False


def _build_item_detail(*, item_type: str, item_obj: dict[str, Any]) -> str:
    normalized_type = item_type[:1].lower() + item_type[1:] if item_type else item_type
    if normalized_type == "agentCommentary":
        return _compact(item_obj.get("text"), limit=None)
    if normalized_type == "commandExecution":
        command = _compact(item_obj.get("command"))
        parts: list[str] = []
        if command:
            parts.append(f"cmd={command}")
        exec_status = _compact(item_obj.get("status"))
        if exec_status:
            parts.append(f"status={exec_status}")
        exit_code = item_obj.get("exitCode")
        if isinstance(exit_code, int):
            parts.append(f"exit={exit_code}")
        duration_ms = item_obj.get("durationMs")
        if isinstance(duration_ms, int):
            parts.append(f"dur_ms={duration_ms}")
        return "; ".join(parts)
    if normalized_type == "fileChange":
        raw_changes = item_obj.get("changes")
        if isinstance(raw_changes, list) and raw_changes:
            first = raw_changes[0]
            if isinstance(first, dict):
                path_value = first.get("path")
                if isinstance(path_value, str) and path_value.strip():
                    return f"path={Path(path_value).name}"
        return ""
    if normalized_type == "webSearch":
        action_type = ""
        action = item_obj.get("action")
        if isinstance(action, dict):
            action_type = _compact(action.get("type"))
            action_query = _compact(action.get("query"))
            if action_query:
                query = action_query
            else:
                query = _compact(item_obj.get("query"))
        else:
            query = _compact(item_obj.get("query"))
        if not query:
            query = "<none>"
        if action_type:
            return f"action={action_type}; query={query}"
        return f"query={query}"
    if normalized_type == "agentMessage":
        phase = _compact(item_obj.get("phase"))
        text = _compact(item_obj.get("text"), limit=None)
        parts: list[str] = []
        if phase:
            parts.append(f"phase={phase}")
        if text:
            parts.append(f"text={text}")
        return "; ".join(parts)
    return ""


def _normalize_item_type(item_type: str) -> str:
    if not item_type:
        return item_type
    return item_type[:1].lower() + item_type[1:]


def _compact(value: Any, *, limit: int | None = 80) -> str:
    if not isinstance(value, str):
        return ""
    compact = " ".join(value.split())
    if not compact:
        return ""
    if limit is None:
        return compact
    if len(compact) <= limit:
        return compact
    return compact[: max(0, limit - 3)] + "..."


def _extract_turn_id(method: str, params: dict[str, Any] | None) -> str | None:
    if not isinstance(params, dict):
        return None
    direct = params.get("turnId")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()
    direct_snake = params.get("turn_id")
    if isinstance(direct_snake, str) and direct_snake.strip():
        return direct_snake.strip()
    if method in ("turn/started", "turn/completed"):
        turn = params.get("turn")
        if isinstance(turn, dict):
            raw_turn_id = turn.get("id")
            if isinstance(raw_turn_id, str) and raw_turn_id.strip():
                return raw_turn_id.strip()
    msg = params.get("msg")
    if isinstance(msg, dict):
        msg_turn_id = msg.get("turnId")
        if isinstance(msg_turn_id, str) and msg_turn_id.strip():
            return msg_turn_id.strip()
        msg_turn_id_snake = msg.get("turn_id")
        if isinstance(msg_turn_id_snake, str) and msg_turn_id_snake.strip():
            return msg_turn_id_snake.strip()
    return None
