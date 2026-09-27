"""Typed Codex session catalog merged with persisted ORC bindings."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Mapping, Sequence

from orchestrator.access_point_common import (
    AccessPointKey,
    access_point_sort_key,
    format_access_point,
)
from orchestrator.codex_sessions import CodexCliSession
from orchestrator.steward_state import PersistedAccessPointState


@dataclass(frozen=True)
class AgentCatalogEntry:
    session_id: str
    thread_name: str
    cwd: str
    access_points: tuple[AccessPointKey, ...] = ()


def resolve_catalog_cwd(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    try:
        return str(Path(value).expanduser().resolve())
    except OSError:
        return value


def build_agent_catalog(
    *,
    sessions: Sequence[CodexCliSession],
    bindings: Mapping[AccessPointKey, PersistedAccessPointState],
    target_cwd: str = "",
) -> list[AgentCatalogEntry]:
    resolved_target = resolve_catalog_cwd(target_cwd)
    records_by_id: dict[str, AgentCatalogEntry] = {}
    for session in sessions:
        if session.session_id in records_by_id:
            continue
        records_by_id[session.session_id] = AgentCatalogEntry(
            session_id=session.session_id,
            thread_name=session.thread_name,
            cwd=session.cwd,
        )

    for access_point in sorted(bindings, key=access_point_sort_key):
        state = bindings[access_point]
        agent = state.agent if isinstance(state.agent, dict) else {}
        session_id = str(agent.get("thread_id") or "").strip()
        if not session_id:
            continue
        binding_cwd = resolve_catalog_cwd(str(state.project_cwd or agent.get("cwd") or ""))
        if resolved_target and binding_cwd != resolved_target:
            continue
        record = records_by_id.get(session_id)
        if record is None:
            record = AgentCatalogEntry(
                session_id=session_id,
                thread_name=str(agent.get("thread_name") or "").strip(),
                cwd=binding_cwd,
            )
        elif not record.thread_name:
            record = replace(
                record,
                thread_name=str(agent.get("thread_name") or "").strip(),
            )
        records_by_id[session_id] = replace(
            record,
            access_points=(*record.access_points, access_point),
        )

    return list(records_by_id.values())


def resolve_agent_catalog_entries(
    records: Sequence[AgentCatalogEntry],
    selector: str,
) -> list[AgentCatalogEntry]:
    session_id_matches = [record for record in records if record.session_id == selector]
    if session_id_matches:
        return session_id_matches
    return [record for record in records if record.thread_name == selector]


def format_agent_catalog_chunks(
    *,
    scope: str,
    records: Sequence[AgentCatalogEntry],
    message_char_limit: int,
) -> list[str]:
    if not records:
        return [f"agents ({scope}): none"]

    header = f"agents ({scope}): {len(records)}"
    record_blocks = [format_agent_catalog_entry(record) for record in records]
    body_limit = max(1, int(message_char_limit) - len(header) - 64)
    groups: list[list[str]] = []
    current: list[str] = []
    current_len = 0
    for block in record_blocks:
        added_len = len(block) + (2 if current else 0)
        if current and current_len + added_len > body_limit:
            groups.append(current)
            current = []
            current_len = 0
            added_len = len(block)
        current.append(block)
        current_len += added_len
    if current:
        groups.append(current)

    total = len(groups)
    chunks: list[str] = []
    for index, group in enumerate(groups, start=1):
        chunk_header = header if index == 1 else f"{header} (continued {index}/{total})"
        chunks.append(f"{chunk_header}\n\n" + "\n\n".join(group))
    return chunks


def format_agent_catalog_entry(record: AgentCatalogEntry) -> str:
    lines = [
        f"session: {record.thread_name or '[unnamed]'}",
        f"uuid: {record.session_id}",
        f"folder: {record.cwd}",
    ]
    if record.access_points:
        lines.extend(f"bound to: {format_access_point(item)}" for item in record.access_points)
    else:
        lines.append("bound to: none")
    return "\n".join(lines)
