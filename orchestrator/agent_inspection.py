"""Provider-neutral local projection used by status and inspect."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum

from orchestrator.agent_capabilities import AgentCapabilitySnapshot
from orchestrator.agent_identity import ProviderSessionRef, TurnRef


class SnapshotAvailability(StrEnum):
    AVAILABLE = "available"
    UNSUPPORTED = "unsupported"
    UNKNOWN = "unknown"
    ERROR = "error"


def _validate_component(state: SnapshotAvailability, error: str) -> str:
    if not isinstance(state, SnapshotAvailability):
        raise ValueError("snapshot availability must be SnapshotAvailability")
    normalized = str(error or "").strip()
    if state is SnapshotAvailability.ERROR and not normalized:
        raise ValueError("error snapshot component requires an error")
    if state is not SnapshotAvailability.ERROR and normalized:
        raise ValueError(f"{state.value} snapshot component must not include an error")
    return normalized


def _require_string(label: str, value: object) -> None:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be a string")


def _require_typed_tuple(label: str, values: object, item_type: type) -> None:
    if not isinstance(values, tuple) or not all(isinstance(item, item_type) for item in values):
        raise ValueError(f"{label} must be a tuple of {item_type.__name__}")


@dataclass(frozen=True)
class AgentSessionSnapshot:
    availability: SnapshotAvailability
    session_ref: ProviderSessionRef | None = None
    name: str = ""
    status: str = ""
    active_flags: tuple[str, ...] = ()
    loaded: bool | None = None
    ephemeral: bool | None = None
    backend: str = ""
    model: str = ""
    created_at: str = ""
    updated_at: str = ""
    preview: str = ""
    auth_owner: str = ""
    auth_method_ids: tuple[str, ...] = ()
    error: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "error", _validate_component(self.availability, self.error))
        if self.session_ref is not None and not isinstance(self.session_ref, ProviderSessionRef):
            raise ValueError("session ref must be ProviderSessionRef")
        for label, value in (
            ("session name", self.name),
            ("session status", self.status),
            ("session backend", self.backend),
            ("session model", self.model),
            ("session created at", self.created_at),
            ("session updated at", self.updated_at),
            ("session preview", self.preview),
            ("session auth owner", self.auth_owner),
        ):
            _require_string(label, value)
        _require_typed_tuple("session active flags", self.active_flags, str)
        _require_typed_tuple("session auth method IDs", self.auth_method_ids, str)
        if any(not value or len(value) > 128 for value in self.auth_method_ids):
            raise ValueError("session auth method IDs must be 1..128 characters")
        if len(self.auth_method_ids) != len(set(self.auth_method_ids)):
            raise ValueError("session auth method IDs must be unique")
        for label, value in (("loaded", self.loaded), ("ephemeral", self.ephemeral)):
            if value is not None and not isinstance(value, bool):
                raise ValueError(f"{label} must be bool or None")


@dataclass(frozen=True)
class AgentWorkItemSnapshot:
    kind: str
    item_ref: str = ""
    state: str = ""
    details: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for label, value in (
            ("work item kind", self.kind),
            ("work item ref", self.item_ref),
            ("work item state", self.state),
        ):
            _require_string(label, value)
        _require_typed_tuple("work item details", self.details, str)


@dataclass(frozen=True)
class AgentWorkSnapshot:
    availability: SnapshotAvailability
    active_turn_ref: TurnRef | None = None
    items: tuple[AgentWorkItemSnapshot, ...] = ()
    error: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "error", _validate_component(self.availability, self.error))
        if self.active_turn_ref is not None and not isinstance(self.active_turn_ref, TurnRef):
            raise ValueError("active turn ref must be TurnRef")
        _require_typed_tuple("work items", self.items, AgentWorkItemSnapshot)


@dataclass(frozen=True)
class AgentContextSnapshot:
    availability: SnapshotAvailability
    used_tokens: int | None = None
    model_context_window: int | None = None
    error: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "error", _validate_component(self.availability, self.error))
        for label, value in (
            ("used tokens", self.used_tokens),
            ("model context window", self.model_context_window),
        ):
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
                raise ValueError(f"{label} must be a non-negative integer")
        if self.model_context_window == 0:
            raise ValueError("model context window must be positive")


@dataclass(frozen=True)
class AgentBackgroundItemSnapshot:
    task_ref: str
    state: str
    summary: str = ""

    def __post_init__(self) -> None:
        for label, value in (
            ("background task ref", self.task_ref),
            ("background task state", self.state),
            ("background task summary", self.summary),
        ):
            _require_string(label, value)


@dataclass(frozen=True)
class AgentBackgroundSnapshot:
    availability: SnapshotAvailability
    items: tuple[AgentBackgroundItemSnapshot, ...] = ()
    error: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "error", _validate_component(self.availability, self.error))
        _require_typed_tuple("background items", self.items, AgentBackgroundItemSnapshot)


@dataclass(frozen=True)
class AgentConfigurationSnapshot:
    availability: SnapshotAvailability
    models: tuple[str, ...] = ()
    current_model: str = ""
    effort_values: tuple[str, ...] = ()
    current_effort: str = ""
    error: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "error", _validate_component(self.availability, self.error))
        _require_typed_tuple("configuration models", self.models, str)
        _require_typed_tuple("configuration effort values", self.effort_values, str)
        _require_string("configuration current model", self.current_model)
        _require_string("configuration current effort", self.current_effort)
        if len(self.models) != len(set(self.models)):
            raise ValueError("configuration models must be unique")
        if len(self.effort_values) != len(set(self.effort_values)):
            raise ValueError("configuration effort values must be unique")
        if self.current_model and self.current_model not in self.models:
            raise ValueError("configuration current model must be available")
        if self.current_effort and self.current_effort not in self.effort_values:
            raise ValueError("configuration current effort must be available")


@dataclass(frozen=True)
class AgentRuntimeProjection:
    capabilities: AgentCapabilitySnapshot | None
    session: AgentSessionSnapshot
    work: AgentWorkSnapshot
    context: AgentContextSnapshot
    background: AgentBackgroundSnapshot
    configuration: AgentConfigurationSnapshot = AgentConfigurationSnapshot(
        SnapshotAvailability.UNSUPPORTED
    )

    def __post_init__(self) -> None:
        if self.capabilities is not None and not isinstance(
            self.capabilities,
            AgentCapabilitySnapshot,
        ):
            raise ValueError("projection capabilities must be AgentCapabilitySnapshot or None")
        for label, value, expected_type in (
            ("session", self.session, AgentSessionSnapshot),
            ("work", self.work, AgentWorkSnapshot),
            ("context", self.context, AgentContextSnapshot),
            ("background", self.background, AgentBackgroundSnapshot),
            ("configuration", self.configuration, AgentConfigurationSnapshot),
        ):
            if not isinstance(value, expected_type):
                raise ValueError(f"projection {label} must be {expected_type.__name__}")


def work_snapshot_rows(snapshot: AgentWorkSnapshot) -> list[dict[str, str]]:
    if snapshot.availability is not SnapshotAvailability.AVAILABLE:
        return []
    return [
        {
            "id": item.item_ref,
            "type": item.kind,
            "status": item.state,
            "detail": "; ".join(item.details),
        }
        for item in snapshot.items
    ]


def projection_with_session_error(
    projection: AgentRuntimeProjection,
    error: str,
) -> AgentRuntimeProjection:
    return replace(
        projection,
        session=replace(
            projection.session,
            availability=SnapshotAvailability.ERROR,
            error=str(error or "inspection refresh failed").strip(),
        ),
    )


def unavailable_projection(
    availability: SnapshotAvailability = SnapshotAvailability.UNKNOWN,
) -> AgentRuntimeProjection:
    return AgentRuntimeProjection(
        capabilities=None,
        session=AgentSessionSnapshot(availability),
        work=AgentWorkSnapshot(availability),
        context=AgentContextSnapshot(availability),
        background=AgentBackgroundSnapshot(availability),
        configuration=AgentConfigurationSnapshot(availability),
    )
