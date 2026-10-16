"""Provider-namespaced session catalog merged with persisted ORC bindings."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Mapping, Sequence

from orchestrator.access_point_common import (
    AccessPointKey,
    access_point_sort_key,
    format_access_point,
)
from orchestrator.agent_identity import ProviderKind, ProviderProfileId, ProviderSessionRef
from orchestrator.agent_provider import (
    ProviderSessionCatalogEntry,
    ProviderSessionCatalogErrorKind,
)
from orchestrator.runtime_profile_catalog import ProviderSessionProjector
from orchestrator.provider_installation import ProviderInstallationStatusKind
from orchestrator.steward_state import PersistedAccessPointState


@dataclass(frozen=True)
class AgentCatalogEntry:
    session: ProviderSessionRef
    thread_name: str
    cwd: str
    adapter: str = ""
    adapter_version: str = ""
    access_points: tuple[AccessPointKey, ...] = ()

    @property
    def provider(self) -> ProviderKind:
        return self.session.provider

    @property
    def profile(self) -> ProviderProfileId:
        return self.session.profile


@dataclass(frozen=True)
class AgentCatalogFailure:
    provider: ProviderKind
    profile: ProviderProfileId
    kind: ProviderSessionCatalogErrorKind
    installation_reason: ProviderInstallationStatusKind | None = None

    def __post_init__(self) -> None:
        if self.kind is ProviderSessionCatalogErrorKind.INSTALLATION_UNAVAILABLE:
            if self.installation_reason not in {
                ProviderInstallationStatusKind.MISSING_COMMAND,
                ProviderInstallationStatusKind.MISSING_DEPENDENCY,
            }:
                raise ValueError(
                    "installation catalog failure requires unavailable reason"
                )
        elif self.installation_reason is not None:
            raise ValueError(
                "non-installation catalog failure must not include installation reason"
            )


@dataclass(frozen=True)
class AgentCatalogListing:
    entries: tuple[ProviderSessionCatalogEntry, ...]
    failures: tuple[AgentCatalogFailure, ...]

    def __post_init__(self) -> None:
        entries = tuple(self.entries)
        failures = tuple(self.failures)
        if any(not isinstance(item, ProviderSessionCatalogEntry) for item in entries):
            raise ValueError("agent catalog listing entries must be provider sessions")
        if any(not isinstance(item, AgentCatalogFailure) for item in failures):
            raise ValueError("agent catalog listing failures must be typed")
        object.__setattr__(self, "entries", entries)
        object.__setattr__(self, "failures", failures)


def agent_catalog_failure_payloads(
    failures: Sequence[AgentCatalogFailure],
) -> list[dict[str, str]]:
    return [
        {
            "provider": failure.provider.value,
            "profile": failure.profile.value,
            "kind": failure.kind.value,
            **(
                {"reason": failure.installation_reason.value}
                if failure.installation_reason is not None
                else {}
            ),
        }
        for failure in failures
    ]


def format_agent_catalog_failure(failure: AgentCatalogFailure) -> str:
    if failure.kind is ProviderSessionCatalogErrorKind.INSTALLATION_UNAVAILABLE:
        assert failure.installation_reason is not None
        return (
            f"{failure.provider.value}/{failure.profile.value} installation unavailable "
            f"({failure.installation_reason.value})"
        )
    return (
        f"{failure.provider.value}/{failure.profile.value} catalog unavailable "
        f"({failure.kind.value})"
    )


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
    sessions: Sequence[ProviderSessionCatalogEntry],
    bindings: Mapping[AccessPointKey, PersistedAccessPointState],
    target_cwd: str = "",
) -> list[AgentCatalogEntry]:
    resolved_target = resolve_catalog_cwd(target_cwd)
    records_by_id: dict[ProviderSessionRef, AgentCatalogEntry] = {}
    for session in sessions:
        session_ref = session.session_ref
        if session_ref in records_by_id:
            continue
        records_by_id[session_ref] = AgentCatalogEntry(
            session=session_ref,
            thread_name=session.name,
            cwd=session.cwd,
            adapter=session.adapter_name,
            adapter_version=session.adapter_version,
        )

    for access_point in sorted(bindings, key=access_point_sort_key):
        state = bindings[access_point]
        agent = state.agent
        if agent is None or agent.session is None:
            continue
        session_ref = agent.session
        binding_cwd = resolve_catalog_cwd(str(state.project_cwd or agent.configured.cwd or ""))
        if resolved_target and binding_cwd != resolved_target:
            continue
        record = records_by_id.get(session_ref)
        if record is None:
            record = AgentCatalogEntry(
                session=session_ref,
                thread_name=agent.session_name,
                cwd=binding_cwd,
                adapter=agent.adapter_name,
                adapter_version=agent.adapter_version,
            )
        else:
            record = replace(
                record,
                thread_name=record.thread_name or agent.session_name,
                adapter=record.adapter or agent.adapter_name,
                adapter_version=record.adapter_version or agent.adapter_version,
            )
        records_by_id[session_ref] = replace(
            record,
            access_points=(*record.access_points, access_point),
        )

    return list(records_by_id.values())


def resolve_agent_catalog_entries(
    records: Sequence[AgentCatalogEntry],
    selector: str,
    *,
    session_projector: ProviderSessionProjector,
) -> list[AgentCatalogEntry]:
    session_id_matches = [
        record
        for record in records
        if selector
        in {
            record.session.native_id,
            session_projector.namespaced_id(record.session),
        }
    ]
    if session_id_matches:
        return session_id_matches
    return [record for record in records if record.thread_name == selector]


def format_agent_catalog_chunks(
    *,
    scope: str,
    records: Sequence[AgentCatalogEntry],
    message_char_limit: int,
    session_projector: ProviderSessionProjector,
    reveal_named_session_ids: bool = False,
    record_limit: int | None = None,
) -> list[str]:
    if record_limit is not None and record_limit < 1:
        raise ValueError("agent catalog record limit must be positive")
    if not records:
        return [f"agents ({scope}): none"]

    header = f"agents ({scope}): {len(records)}"
    visible_records = records if record_limit is None else records[:record_limit]
    omitted_count = len(records) - len(visible_records)
    omission_notice = f"ещё {omitted_count} не показано" if omitted_count else ""
    record_blocks = [
        format_agent_catalog_entry(
            record,
            session_projector=session_projector,
            reveal_named_session_id=reveal_named_session_ids,
        )
        for record in visible_records
    ]
    body_limit = max(
        1,
        int(message_char_limit) - len(header) - len(omission_notice) - 66,
    )
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
    if omission_notice:
        chunks[-1] += f"\n\n{omission_notice}"
    return chunks


def format_agent_catalog_entry(
    record: AgentCatalogEntry,
    *,
    session_projector: ProviderSessionProjector,
    reveal_named_session_id: bool = False,
) -> str:
    lines = [
        f"session: {record.thread_name or '[unnamed]'}",
        f"provider: {record.provider.value}",
        f"profile: {record.profile.value}",
        f"adapter: {(record.adapter or 'unknown')}@"
        f"{(record.adapter_version or 'unknown')}",
        *(
            []
            if record.thread_name and not reveal_named_session_id
            else [
                "provider session id: "
                f"{session_projector.namespaced_id(record.session)}"
            ]
        ),
        f"folder: {record.cwd}",
    ]
    if record.access_points:
        lines.extend(f"bound to: {format_access_point(item)}" for item in record.access_points)
    else:
        lines.append("bound to: none")
    return "\n".join(lines)
