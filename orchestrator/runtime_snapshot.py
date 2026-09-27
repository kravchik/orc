from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable

from orchestrator.access_point_common import AccessPointKey


ORC_RUNTIME_ID_LABEL = "ORC runtime id"


@dataclass(frozen=True)
class RuntimeNodeSnapshot:
    role: str
    agent_id: str
    state: str
    controllable: bool
    cwd: str = ""
    mode: str = ""
    thread_id: str = ""
    thread_name: str = ""
    configured_model: str = ""
    effective_model: str = ""
    approval_target: str = ""

    def as_dict(self) -> dict[str, Any]:
        row: dict[str, Any] = {
            "role": self.role,
            "agent_id": self.agent_id,
            "state": self.state,
            "controllable": self.controllable,
        }
        optional = {
            "cwd": self.cwd,
            "mode": self.mode,
            "thread_id": self.thread_id,
            "thread_name": self.thread_name,
        }
        row.update({key: value for key, value in optional.items() if value})
        if self.role in {"agent", "steward"}:
            row["configured_model"] = self.configured_model or "default"
            row["effective_model"] = self.effective_model or "pending"
        if self.approval_target and self.approval_target != "human":
            row["approval_target"] = self.approval_target
        return row


@dataclass(frozen=True)
class AccessPointRuntimeSnapshot:
    access_point: AccessPointKey
    state: str
    steward: RuntimeNodeSnapshot | None
    agent: RuntimeNodeSnapshot | None
    binding: RuntimeNodeSnapshot | None

    def nodes(self) -> tuple[RuntimeNodeSnapshot, ...]:
        return tuple(node for node in (self.steward, self.agent) if node is not None)

    def as_dict(self) -> dict[str, Any]:
        return {
            "access_point": {
                "type": self.access_point.type,
                "chat_id": self.access_point.chat_id,
                "thread_id": self.access_point.thread_id,
            },
            "state": self.state,
            "runtimes": [node.as_dict() for node in self.nodes()],
            "binding": self.binding.as_dict() if self.binding is not None else None,
        }

    def action_payload(self) -> dict[str, Any]:
        return {
            "runtimes": [node.as_dict() for node in self.nodes()],
            "binding": self.binding.as_dict() if self.binding is not None else None,
        }


def build_runtime_snapshot(
    *,
    access_point: AccessPointKey,
    state: str,
    steward_rows: Iterable[dict[str, Any]],
    agent_rows: Iterable[dict[str, Any]],
    binding: dict[str, Any] | None,
) -> AccessPointRuntimeSnapshot:
    normalized_state = str(state or "UNBOUND").strip().upper()
    steward_row = next(iter(steward_rows), None)
    agent_row = next(iter(agent_rows), None)

    steward = _node_from_row(steward_row, role="steward", default_controllable=False)
    binding_node = _node_from_row(
        binding or agent_row,
        role="agent",
        state=normalized_state,
        default_controllable=normalized_state in {"STARTING", "RUNNING"},
    )
    agent = (
        _node_from_row(
            agent_row or binding,
            role="agent",
            state=normalized_state,
            default_controllable=True,
        )
        if normalized_state in {"STARTING", "RUNNING"}
        else None
    )
    return AccessPointRuntimeSnapshot(
        access_point=access_point,
        state=normalized_state,
        steward=steward,
        agent=agent,
        binding=binding_node,
    )


def format_runtime_snapshot_context(snapshot: AccessPointRuntimeSnapshot) -> str:
    payload = json.dumps(snapshot.as_dict(), ensure_ascii=False, indent=2)
    return f"Authoritative access-point runtime snapshot:\n{payload}"


def format_status_text(
    snapshot: AccessPointRuntimeSnapshot,
    *,
    access_point_line: str,
) -> str:
    steward_state = snapshot.steward.state.lower() if snapshot.steward is not None else "not_started"
    binding = snapshot.binding
    agent_state = binding.state.lower() if binding is not None else "not_running"
    if binding is not None and binding.state == "BOUND_IDLE":
        agent_state = "stopped"
    lines = [
        f"state: {snapshot.state}",
        access_point_line,
        f"steward_node: {steward_state}",
        f"runtime_agent: {agent_state}",
    ]
    if binding is not None:
        lines.extend(
            [
                f"{ORC_RUNTIME_ID_LABEL}: {binding.agent_id}",
                *([f"session: {binding.thread_name}"] if binding.thread_name else []),
                f"cwd: {binding.cwd}",
                f"mode: {binding.mode}",
                f"configured_model: {binding.configured_model or 'default'}",
                f"effective_model: {binding.effective_model or 'pending'}",
                *(
                    [f"approver: {binding.approval_target}"]
                    if binding.approval_target and binding.approval_target != "human"
                    else []
                ),
            ]
        )
    return "\n".join(lines)


def _node_from_row(
    row: dict[str, Any] | None,
    *,
    role: str,
    state: str = "",
    default_controllable: bool,
) -> RuntimeNodeSnapshot | None:
    if row is None:
        return None
    resolved_state = str(state or row.get("state") or "RUNNING").strip().upper()
    configured_model = str(
        row.get("configured_model")
        or row.get("model")
        or ""
    ).strip()
    effective_model = str(row.get("effective_model") or "").strip()
    return RuntimeNodeSnapshot(
        role=str(row.get("role") or role).strip().lower(),
        agent_id=str(row.get("agent_id") or "").strip(),
        state=resolved_state,
        controllable=bool(row.get("controllable", default_controllable)),
        cwd=str(row.get("cwd") or "").strip(),
        mode=str(row.get("mode") or "").strip(),
        thread_id=str(row.get("thread_id") or "").strip(),
        thread_name=str(row.get("thread_name") or "").strip(),
        configured_model=configured_model,
        effective_model=effective_model,
        approval_target=str(row.get("approval_target") or "").strip(),
    )
