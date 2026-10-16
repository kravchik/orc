"""Transport-neutral events and delivery contracts for provider sessions."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Any, Callable, Generic, Protocol, TypeAlias, TypeVar

from orchestrator.access_point_common import AccessPointId, AccessPointKey
from orchestrator.agent_capabilities import AgentCapabilitySnapshot
from orchestrator.agent_identity import InspectionRef, TurnRef, WorkStatusRef
from orchestrator.agent_inspection import AgentRuntimeProjection, AgentWorkSnapshot
from orchestrator.agent_io import AgentInput, AgentOutput
from orchestrator.agent_session import (
    AgentApprovalRequest,
    AgentFormRequest,
    ApprovalDecisionResult,
    ElicitationAction,
    ElicitationDecisionResult,
    ElicitationValue,
    AgentEffectResult,
    RawProviderErrorPayload,
    WorkStatusKind,
)
from orchestrator.processes import LifecycleLogger


AccessPointT = TypeVar("AccessPointT")


@dataclass(frozen=True)
class AgentModelSelection(Generic[AccessPointT]):
    access_point: AccessPointT
    selected_model: str


@dataclass(frozen=True)
class AgentEffortSelection(Generic[AccessPointT]):
    access_point: AccessPointT
    selected_effort: str


class InteractiveAgentInputRequest(Protocol):
    agent_input: AgentInput

    @property
    def access_point(self) -> tuple[AccessPointId, AccessPointId | None]: ...


@dataclass
class InteractiveAgentRequest:
    chat_id: AccessPointId
    thread_id: AccessPointId | None
    agent_input: AgentInput

    @property
    def access_point(self) -> tuple[AccessPointId, AccessPointId | None]:
        return (self.chat_id, self.thread_id)


class InteractiveTurnOutcome(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class InteractiveAgentResult:
    chat_id: AccessPointId
    thread_id: AccessPointId | None
    agent_input: AgentInput
    output: AgentOutput | None
    turn_ref: TurnRef
    outcome: InteractiveTurnOutcome = InteractiveTurnOutcome.COMPLETED
    error: str = ""
    raw_provider_error: RawProviderErrorPayload | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.turn_ref, TurnRef):
            raise ValueError("interactive terminal result requires TurnRef")
        if not isinstance(self.outcome, InteractiveTurnOutcome):
            raise ValueError("interactive turn outcome must be InteractiveTurnOutcome")
        if not isinstance(self.error, str):
            raise ValueError("interactive turn error must be a string")
        self.error = self.error.strip()
        if self.outcome is InteractiveTurnOutcome.COMPLETED:
            if not isinstance(self.output, AgentOutput):
                raise ValueError("completed interactive turn requires AgentOutput")
            if self.error:
                raise ValueError("completed interactive turn must not include an error")
            if self.raw_provider_error is not None:
                raise ValueError("completed interactive turn must not include provider error")
            return
        if self.output is not None:
            raise ValueError("failed interactive turn must not include AgentOutput")
        if not self.error:
            raise ValueError("failed interactive turn requires an error")
        if self.raw_provider_error is not None and not isinstance(
            self.raw_provider_error, RawProviderErrorPayload
        ):
            raise ValueError("interactive raw provider error must be immutable")

    @property
    def failed(self) -> bool:
        return self.outcome is InteractiveTurnOutcome.FAILED

    def require_output(self) -> AgentOutput:
        if self.outcome is not InteractiveTurnOutcome.COMPLETED or self.output is None:
            raise RuntimeError("failed interactive turn has no AgentOutput")
        return self.output

    @property
    def rendered_error(self) -> str:
        return (
            self.raw_provider_error.render()
            if self.raw_provider_error is not None
            else self.error
        )

    @classmethod
    def failed_result(
        cls,
        *,
        chat_id: AccessPointId,
        thread_id: AccessPointId | None,
        agent_input: AgentInput,
        error: str,
        turn_ref: TurnRef,
        raw_provider_error: RawProviderErrorPayload | None = None,
    ) -> InteractiveAgentResult:
        return cls(
            chat_id=chat_id,
            thread_id=thread_id,
            agent_input=agent_input,
            output=None,
            turn_ref=turn_ref,
            outcome=InteractiveTurnOutcome.FAILED,
            error=error,
            raw_provider_error=raw_provider_error,
        )


@dataclass
class InteractiveApprovalPromptEvent:
    chat_id: AccessPointId
    thread_id: AccessPointId | None
    request: AgentApprovalRequest


@dataclass
class InteractiveElicitationPromptEvent:
    chat_id: AccessPointId
    thread_id: AccessPointId | None
    request: AgentFormRequest


@dataclass
class ApprovalDeliveryReceipt:
    request: AgentApprovalRequest
    decision: str
    result: ApprovalDecisionResult | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.request, AgentApprovalRequest):
            raise TypeError("approval delivery receipt requires AgentApprovalRequest")
        if not isinstance(self.decision, str) or not self.decision.strip():
            raise ValueError("approval delivery receipt requires a decision")
        self.decision = self.decision.strip()

    @property
    def completed(self) -> bool:
        return self.result is not None

    def complete(self, result: ApprovalDecisionResult) -> None:
        if not isinstance(result, ApprovalDecisionResult):
            raise TypeError("approval delivery receipt requires ApprovalDecisionResult")
        if self.result is not None:
            raise RuntimeError("approval delivery receipt is already completed")
        self.result = result


@dataclass
class ElicitationDeliveryReceipt:
    request: AgentFormRequest
    action: ElicitationAction
    content: tuple[tuple[str, ElicitationValue], ...] = ()
    result: ElicitationDecisionResult | None = None

    @property
    def completed(self) -> bool:
        return self.result is not None

    def complete(self, result: ElicitationDecisionResult) -> None:
        if self.result is not None:
            raise RuntimeError("elicitation delivery receipt is already completed")
        self.result = result


@dataclass
class InteractiveStatusEvent:
    chat_id: AccessPointId
    thread_id: AccessPointId | None
    status_text: str
    turn_ref: TurnRef
    status_ref: WorkStatusRef | None = None
    kind: WorkStatusKind = WorkStatusKind.NOTICE
    status_text_kind: WorkStatusKind | None = None
    work: AgentWorkSnapshot | None = None
    item_ref: str = ""
    suppress_status: bool = False
    preserve_work_snapshot: bool = False
    auxiliary: bool = False
    begins_turn: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.turn_ref, TurnRef):
            raise ValueError("interactive status event requires TurnRef")
        if not isinstance(self.kind, WorkStatusKind):
            raise ValueError("interactive status event requires WorkStatusKind")
        if self.status_text_kind is not None and not isinstance(
            self.status_text_kind,
            WorkStatusKind,
        ):
            raise ValueError("interactive status text kind must be WorkStatusKind")


class StatusStreamKind(StrEnum):
    TURN_BOUND = "turn_bound"
    AUXILIARY = "auxiliary"


@dataclass(frozen=True)
class StatusStreamKey:
    access_point: tuple[AccessPointId, AccessPointId | None]
    turn_ref: TurnRef
    status_ref: WorkStatusRef | None
    semantic_source: str
    stream_kind: StatusStreamKind

    def __post_init__(self) -> None:
        if not isinstance(self.turn_ref, TurnRef):
            raise ValueError("status stream key requires TurnRef")
        if self.status_ref is not None and not isinstance(self.status_ref, WorkStatusRef):
            raise ValueError("status stream key requires WorkStatusRef")
        if not isinstance(self.semantic_source, str) or not self.semantic_source.strip():
            raise ValueError("status stream key requires semantic source")
        object.__setattr__(self, "semantic_source", self.semantic_source.strip())
        if not isinstance(self.stream_kind, StatusStreamKind):
            raise ValueError("status stream key requires StatusStreamKind")


def status_stream_key(
    event: InteractiveStatusEvent,
    *,
    semantic_source: str,
    access_point: tuple[AccessPointId, AccessPointId | None] | None = None,
) -> StatusStreamKey:
    return StatusStreamKey(
        access_point=access_point or (event.chat_id, event.thread_id),
        turn_ref=event.turn_ref,
        status_ref=event.status_ref,
        semantic_source=semantic_source,
        stream_kind=(
            StatusStreamKind.AUXILIARY
            if event.auxiliary
            else StatusStreamKind.TURN_BOUND
        ),
    )


def coalesce_status_events(
    previous: InteractiveStatusEvent,
    current: InteractiveStatusEvent,
    *,
    semantic_source: str,
) -> InteractiveStatusEvent:
    """Apply last-write-wins without weakening snapshot preservation."""
    if status_stream_key(previous, semantic_source=semantic_source) != status_stream_key(
        current,
        semantic_source=semantic_source,
    ):
        raise ValueError("cannot coalesce different status streams")
    preserve_previous_snapshot = (
        previous.preserve_work_snapshot
        and _has_work_snapshot(previous.work)
        and not _has_work_snapshot(current.work)
    )
    return replace(
        current,
        status_text_kind=(
            previous.status_text_kind or previous.kind
            if preserve_previous_snapshot
            else current.status_text_kind
        ),
        work=previous.work if preserve_previous_snapshot else current.work,
        item_ref=previous.item_ref if preserve_previous_snapshot else current.item_ref,
        suppress_status=previous.suppress_status or current.suppress_status,
        preserve_work_snapshot=(
            previous.preserve_work_snapshot or current.preserve_work_snapshot
        ),
        begins_turn=(
            previous.begins_turn
            or current.begins_turn
            or previous.kind is WorkStatusKind.TURN_STARTED
        ),
    )


def _has_work_snapshot(work: AgentWorkSnapshot | None) -> bool:
    return work is not None and bool(work.items)


@dataclass
class InteractiveOutboundNote:
    chat_id: AccessPointId
    thread_id: AccessPointId | None
    text: str
    turn_ref: TurnRef | None = None


@dataclass
class InteractiveInterruptResult:
    chat_id: AccessPointId
    thread_id: AccessPointId | None
    result: str
    turn_ref: TurnRef
    error: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.turn_ref, TurnRef):
            raise ValueError("interactive interrupt result requires TurnRef")


@dataclass
class InteractiveModelApplyResult:
    chat_id: AccessPointId
    thread_id: AccessPointId | None
    requested_model: str
    actual_model: str
    result: str
    error: str


@dataclass
class InteractiveEffortApplyResult:
    chat_id: AccessPointId
    thread_id: AccessPointId | None
    requested_effort: str
    actual_effort: str
    result: str
    error: str


@dataclass
class InteractiveStartupResult:
    agent_id: str
    result: str
    state: str
    error: str
    cwd: str
    configured_model: str
    effective_model: str
    mode: str
    thread_id: str
    thread_name: str = ""
    provider: str = ""
    profile: str = ""
    adapter: str = ""
    adapter_version: str = ""
    runtime: str = ""
    runtime_version: str = ""
    configured_backend: str = ""
    effective_backend: str = ""


@dataclass
class InteractiveRuntimeFailure:
    agent_id: str
    error: str
    state: str = "BOUND_IDLE"


@dataclass(frozen=True)
class InteractiveInspectionResult:
    chat_id: AccessPointId
    thread_id: AccessPointId | None
    inspection_ref: InspectionRef
    projection: AgentRuntimeProjection


AgentDeliveryEvent: TypeAlias = (
    InteractiveAgentResult
    | InteractiveApprovalPromptEvent
    | InteractiveElicitationPromptEvent
    | InteractiveStatusEvent
    | InteractiveOutboundNote
    | InteractiveInterruptResult
    | InteractiveModelApplyResult
    | InteractiveEffortApplyResult
    | InteractiveStartupResult
    | InteractiveRuntimeFailure
    | InteractiveInspectionResult
)


class AgentDeliveryDriverPort(Protocol):
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def start_turn(
        self, request: InteractiveAgentInputRequest, turn_ref: TurnRef,
    ) -> AgentEffectResult: ...
    def steer_wait_reason(self, expected_turn_ref: TurnRef) -> str: ...
    def try_steer_turn(
        self, request: InteractiveAgentInputRequest, expected_turn_ref: TurnRef,
    ) -> AgentEffectResult: ...
    def submit_approval_decision(
        self,
        decision: str,
        *,
        expected_request: AgentApprovalRequest,
    ) -> ApprovalDeliveryReceipt: ...
    def submit_approval_decision_now(
        self,
        decision: str,
        *,
        expected_request: AgentApprovalRequest,
    ) -> ApprovalDecisionResult: ...
    def submit_elicitation_response(
        self,
        action: ElicitationAction,
        *,
        expected_request: AgentFormRequest,
        content: dict[str, object] | None = None,
    ) -> ElicitationDeliveryReceipt: ...
    def submit_model_selection(
        self,
        selection: AgentModelSelection[tuple[AccessPointId, AccessPointId | None]],
    ) -> None: ...
    def submit_effort_selection(
        self,
        selection: AgentEffortSelection[tuple[AccessPointId, AccessPointId | None]],
    ) -> None: ...
    def poll_once(self) -> list[AgentDeliveryEvent]: ...
    def acknowledge_event_handoff(self) -> None: ...
    def consume_poll_progress(self) -> bool: ...
    def is_ready(self) -> bool: ...
    def get_terminal_error(self) -> str: ...
    def get_capability_snapshot(self) -> AgentCapabilitySnapshot: ...
    def get_local_projection(self) -> AgentRuntimeProjection: ...
    def request_inspection(
        self,
        inspection_ref: InspectionRef,
        timeout_sec: float,
        access_point: tuple[AccessPointId, AccessPointId | None],
    ) -> AgentEffectResult: ...
    def has_active_turn(self) -> bool: ...
    def has_busy_work(self) -> bool: ...
    def interrupt_active_turn(self) -> None: ...

StewardDeliveryDriverFactory = Callable[
    [AccessPointKey, LifecycleLogger],
    AgentDeliveryDriverPort,
]
RuntimeDeliveryDriverFactory = Callable[
    [AccessPointKey, dict[str, Any], LifecycleLogger],
    AgentDeliveryDriverPort,
]
