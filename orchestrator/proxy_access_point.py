"""Typed access-point actions and shared AP contracts for proxy runtimes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Hashable, Protocol, TypeAlias, TypeVar

from orchestrator.access_point_common import (
    AccessPointApprovalDecision,
)
from orchestrator.agent_capabilities import (
    AgentCapability,
    AgentCapabilitySnapshot,
)
from orchestrator.agent_identity import InspectionRef
from orchestrator.agent_inspection import AgentRuntimeProjection
from orchestrator.agent_session import AgentApprovalRequest
from orchestrator.agent_io import AgentInput
from orchestrator.agent_identity import TurnRef
from orchestrator.agent_session import AgentEffectResult
from orchestrator.interactive_driver_events import (
    AgentEffortSelection,
    AgentModelSelection,
    ApprovalDeliveryReceipt,
    InteractiveAgentResult,
    InteractiveApprovalPromptEvent,
    InteractiveEffortApplyResult,
    InteractiveModelApplyResult,
    InteractiveOutboundNote,
    InteractiveStatusEvent,
)
from orchestrator.inspect_text import (
    append_background_snapshot_lines,
    append_work_snapshot_lines,
    format_context_snapshot,
    format_session_snapshot_lines,
)
from orchestrator.runtime_profile_catalog import ProviderSessionProjector
from orchestrator.proxy_model_choices import format_effort_inspect_lines
from orchestrator.turn_status_store import TurnStatusStore


AccessPointKeyT = TypeVar("AccessPointKeyT", bound=Hashable)
OutputRuntimeT = TypeVar("OutputRuntimeT")
PendingApprovalT = TypeVar("PendingApprovalT")


@dataclass(frozen=True)
class ProxySubmitPrompt(Generic[AccessPointKeyT]):
    access_point: AccessPointKeyT
    agent_input: AgentInput


@dataclass(frozen=True)
class ProxyApprovalDecision(AccessPointApprovalDecision[AccessPointKeyT]):
    expected_request: AgentApprovalRequest | None = None


@dataclass(frozen=True)
class ProxyLocalActionHandled:
    kind: str = "handled"


ProxyAction: TypeAlias = (
    ProxySubmitPrompt[AccessPointKeyT]
    | ProxyApprovalDecision[AccessPointKeyT]
    | AgentModelSelection[AccessPointKeyT]
    | AgentEffortSelection[AccessPointKeyT]
    | ProxyLocalActionHandled
)


class ProxyActionDriver(Protocol[AccessPointKeyT]):
    def start_turn(
        self, request: ProxySubmitPrompt[AccessPointKeyT], turn_ref: TurnRef,
    ) -> AgentEffectResult: ...
    def steer_wait_reason(self, expected_turn_ref: TurnRef) -> str: ...
    def try_steer_turn(
        self,
        request: ProxySubmitPrompt[AccessPointKeyT],
        expected_turn_ref: TurnRef,
    ) -> AgentEffectResult: ...

    def submit_approval_decision(
        self,
        decision: str,
        *,
        expected_request: AgentApprovalRequest,
    ) -> ApprovalDeliveryReceipt: ...

    def submit_model_selection(self, selection: AgentModelSelection[AccessPointKeyT]) -> None: ...
    def submit_effort_selection(self, selection: AgentEffortSelection[AccessPointKeyT]) -> None: ...

    def has_active_turn(self) -> bool: ...
    def has_busy_work(self) -> bool: ...
    def interrupt_active_turn(self) -> None: ...
    def get_capability_snapshot(self) -> AgentCapabilitySnapshot: ...
    def get_local_projection(self) -> AgentRuntimeProjection: ...
    def request_inspection(
        self,
        inspection_ref: InspectionRef,
        timeout_sec: float,
        access_point: AccessPointKeyT,
    ) -> AgentEffectResult: ...


class ProxyApprovalState(Protocol[AccessPointKeyT]):
    def split_status_after_approval(self, *, access_point: AccessPointKeyT) -> None: ...

    def clear_pending_approval(self) -> None: ...

    @property
    def pending_approval(self) -> object | None: ...


class ProxyAccessPointAdapter(
    Protocol[
        AccessPointKeyT,
        OutputRuntimeT,
        PendingApprovalT,
    ]
):
    def send_result(self, result: InteractiveAgentResult) -> None: ...

    def after_record_result(self, result: InteractiveAgentResult) -> None: ...

    def format_result_log(self, result: InteractiveAgentResult) -> str: ...

    def record_model_apply_result(self, result: InteractiveModelApplyResult) -> None: ...
    def record_effort_apply_result(self, result: InteractiveEffortApplyResult) -> None: ...

    def send_approval_prompt(self, event: InteractiveApprovalPromptEvent) -> PendingApprovalT: ...

    def send_outbound_note(self, note: InteractiveOutboundNote) -> None: ...

    def send_inspect(self, access_point: AccessPointKeyT, text: str) -> None: ...

    def flush_delivery_lane(self) -> bool: ...

    def access_point_for_status_event(self, event: InteractiveStatusEvent) -> AccessPointKeyT: ...

    def status_text_for_event(self, event: InteractiveStatusEvent) -> str: ...

    def build_status_runtime(
        self,
        access_point: AccessPointKeyT,
    ) -> tuple[OutputRuntimeT, TurnStatusStore]: ...


def dispatch_proxy_action(
    *,
    action: ProxyAction[AccessPointKeyT],
    driver: ProxyActionDriver[AccessPointKeyT],
    approval_state: ProxyApprovalState[AccessPointKeyT] | None = None,
) -> None:
    if isinstance(action, ProxySubmitPrompt):
        raise AssertionError("ProxySubmitPrompt must be queued by ProxyCore")
    if isinstance(action, AgentModelSelection):
        driver.submit_model_selection(action)
        return
    if isinstance(action, AgentEffortSelection):
        driver.submit_effort_selection(action)
        return
    if isinstance(action, ProxyLocalActionHandled):
        return
    if isinstance(action, ProxyApprovalDecision):
        if action.decision == "details":
            return
        expected_request = action.expected_request
        if not isinstance(expected_request, AgentApprovalRequest):
            raise RuntimeError("approval decision has no normalized pending request")
        driver.submit_approval_decision(
            action.decision,
            expected_request=expected_request,
        )
        if approval_state is not None:
            approval_state.split_status_after_approval(access_point=action.access_point)
            approval_state.clear_pending_approval()
        return
    raise AssertionError(f"unsupported proxy action: {action!r}")


def build_proxy_inspect_text(
    *,
    mode: str,
    projection: AgentRuntimeProjection,
    pending_approval: object | None,
    approval_command: str | None,
    approval_cwd: str | None,
    session_projector: ProviderSessionProjector,
) -> str:
    active_turn_ref = projection.work.active_turn_ref
    resolved_active_turn_id = (
        f"turn:{active_turn_ref.sequence}"
        if active_turn_ref is not None
        else (_approval_turn_id(pending_approval) or "none")
    )

    lines = [
        "inspect",
        f"mode: {mode}",
    ]
    lines.extend(
        format_session_snapshot_lines(
            projection.session,
            session_projector=session_projector,
        )
    )
    lines.append(format_context_snapshot(projection.context))
    if (
        projection.capabilities is not None
        and projection.capabilities.supports(AgentCapability.READ_EFFORT)
    ):
        lines.extend(format_effort_inspect_lines(projection))
    lines.append(f"active turn: {resolved_active_turn_id}")
    lines.append(f"pending approval: {'yes' if pending_approval is not None else 'no'}")
    if approval_command:
        lines.append(f"approval command: {approval_command}")
    if approval_cwd:
        lines.append(f"approval cwd: {approval_cwd}")
    lines.append("active work:")
    append_work_snapshot_lines(lines, projection.work)
    append_background_snapshot_lines(lines, projection.background)
    return "\n".join(lines)


def extract_proxy_pending_approval_command(pending_approval: object | None) -> str | None:
    request = getattr(pending_approval, "request", None)
    if request is None and isinstance(pending_approval, dict):
        request = pending_approval.get("request")
    if not isinstance(request, AgentApprovalRequest):
        return None
    normalized = request.command.strip()
    return normalized or None


def extract_proxy_pending_approval_cwd(pending_approval: object | None) -> str | None:
    request = getattr(pending_approval, "request", None)
    if request is None and isinstance(pending_approval, dict):
        request = pending_approval.get("request")
    if not isinstance(request, AgentApprovalRequest):
        return None
    normalized = request.cwd.strip()
    return normalized or None


def _approval_turn_id(pending_approval: object | None) -> str | None:
    request = getattr(pending_approval, "request", None)
    if request is None and isinstance(pending_approval, dict):
        request = pending_approval.get("request")
    if not isinstance(request, AgentApprovalRequest):
        return None
    return str(request.turn_ref.sequence)
