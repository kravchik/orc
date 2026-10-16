from __future__ import annotations

from orchestrator.agent_inspection import (
    AgentBackgroundSnapshot,
    AgentContextSnapshot,
    AgentSessionSnapshot,
    AgentWorkSnapshot,
    SnapshotAvailability,
)
from orchestrator.agent_identity import (
    ProviderKind,
    ProviderProfileId,
    ProviderSessionRef,
)
from orchestrator.runtime_profile_catalog import ProviderSessionProjector


def format_session_snapshot_lines(
    snapshot: AgentSessionSnapshot,
    *,
    session_projector: ProviderSessionProjector,
    thread_name_fallback: str = "",
    thread_id_fallback: str = "",
    session_provider: str = "",
    session_profile: str = "",
    model_fallback: str = "",
    include_model: bool = True,
) -> list[str]:
    lines: list[str] = []
    if snapshot.availability is not SnapshotAvailability.AVAILABLE:
        lines.append(f"session data: {_availability_text(snapshot.availability, snapshot.error)}")

    available = snapshot.availability is SnapshotAvailability.AVAILABLE
    name = (snapshot.name.strip() if available else "") or thread_name_fallback.strip()
    if name:
        lines.append(f"thread name: {name}")
    else:
        session_ref = snapshot.session_ref if available else None
        thread_id = session_ref.native_id.strip() if session_ref is not None else thread_id_fallback.strip()
        provider = (
            session_ref.provider.value
            if session_ref is not None
            else session_provider.strip()
        )
        if thread_id:
            session_id = thread_id
            if session_ref is not None:
                session_id = session_projector.namespaced_id(session_ref)
            elif provider:
                provider_kind = ProviderKind.parse(provider)
                profile = (
                    ProviderProfileId.parse(session_profile)
                    if session_profile.strip()
                    else session_projector.default_profile(provider_kind)
                )
                session_id = ProviderSessionRef(
                    provider=provider_kind,
                    profile=profile,
                    native_id=thread_id,
                )
                session_id = session_projector.namespaced_id(session_id)
            label = "provider session id" if provider else "thread id"
            lines.append(f"{label}: {session_id}")

    if available:
        if snapshot.status:
            lines.append(f"thread status: {snapshot.status}")
        if snapshot.active_flags:
            lines.append(f"active flags: {', '.join(snapshot.active_flags)}")
        if snapshot.loaded is not None:
            lines.append(f"loaded: {'yes' if snapshot.loaded else 'no'}")
        if snapshot.ephemeral is not None:
            lines.append(f"ephemeral: {'yes' if snapshot.ephemeral else 'no'}")
        if snapshot.backend:
            lines.append(f"model backend: {snapshot.backend}")
        if snapshot.created_at:
            lines.append(f"created at: {snapshot.created_at}")
        if snapshot.updated_at:
            lines.append(f"updated at: {snapshot.updated_at}")
        if snapshot.preview:
            lines.append(f"preview: {snapshot.preview}")
        if snapshot.auth_owner:
            lines.append(f"authentication owner: {snapshot.auth_owner}")
        if snapshot.auth_method_ids:
            lines.append(
                "authentication methods: " + ", ".join(snapshot.auth_method_ids)
            )

    if include_model:
        model = (snapshot.model.strip() if available else "") or model_fallback.strip()
        if model:
            lines.append(f"model: {model}")
    return lines


def format_context_snapshot(snapshot: AgentContextSnapshot) -> str:
    if snapshot.availability is SnapshotAvailability.UNSUPPORTED:
        return "model window remaining: unsupported"
    if snapshot.availability is SnapshotAvailability.UNKNOWN:
        return "model window remaining: unknown"
    if snapshot.availability is SnapshotAvailability.ERROR:
        return f"model window remaining: component error: {snapshot.error}"
    if snapshot.used_tokens is None or snapshot.model_context_window is None:
        return "model window remaining: unknown"
    remaining = max(0, snapshot.model_context_window - snapshot.used_tokens)
    percent = max(0.0, min(100.0, remaining * 100.0 / snapshot.model_context_window))
    return (
        f"model window remaining: {remaining}/{snapshot.model_context_window} tokens "
        f"({percent:.1f}%)"
    )


def append_work_snapshot_lines(lines: list[str], snapshot: AgentWorkSnapshot) -> None:
    if snapshot.availability is not SnapshotAvailability.AVAILABLE:
        lines.append(f"work data: {_availability_text(snapshot.availability, snapshot.error)}")
        return
    active_items = [item for item in snapshot.items if item.state.strip() == "in_progress"]
    lines.append(f"open items tracked by ORC: {len(active_items)}")
    if not active_items:
        lines.append("none")
        return
    for item in active_items:
        item_type = item.kind.strip() or "item"
        lines.append(f"- {item_type} {item.item_ref.strip()}".rstrip())
        if item.state.strip():
            lines.append(f"  status: {item.state.strip()}")
        for detail in item.details:
            normalized = _normalize_work_detail(detail.strip())
            if normalized:
                lines.append(f"  {normalized}")


def append_background_snapshot_lines(
    lines: list[str],
    snapshot: AgentBackgroundSnapshot,
) -> None:
    if snapshot.availability is not SnapshotAvailability.AVAILABLE:
        lines.append(
            f"background work: {_availability_text(snapshot.availability, snapshot.error)}"
        )
        return
    lines.append(f"background work: {len(snapshot.items)}")
    for item in snapshot.items:
        line = f"- {item.task_ref}: {item.state}"
        if item.summary:
            line += f" ({item.summary})"
        lines.append(line)


def _availability_text(state: SnapshotAvailability, error: str) -> str:
    if state is SnapshotAvailability.ERROR:
        return f"component error: {error}"
    return state.value


def _normalize_work_detail(detail: str) -> str:
    prefixes = (
        ("cmd=", "command: "),
        ("status=", "status: "),
        ("path=", "path: "),
        ("query=", "query: "),
        ("action=", "action: "),
        ("output_chars=", "output chars: "),
    )
    for prefix, replacement in prefixes:
        if detail.startswith(prefix):
            return f"{replacement}{detail[len(prefix):]}"
    return detail
