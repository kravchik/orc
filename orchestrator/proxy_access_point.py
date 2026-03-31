"""Typed access-point actions and shared AP contracts for proxy runtimes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Hashable, Protocol, TypeAlias, TypeVar

from orchestrator.access_point_common import (
    AccessPointApprovalDecision,
    AccessPointTextInput,
)
from orchestrator.turn_status_store import TurnStatusStore


AccessPointKeyT = TypeVar("AccessPointKeyT", bound=Hashable)
OutputRuntimeT = TypeVar("OutputRuntimeT")
ResultT = TypeVar("ResultT")
ApprovalEventT = TypeVar("ApprovalEventT")
OutboundNoteT = TypeVar("OutboundNoteT")
StatusEventT = TypeVar("StatusEventT")
PendingApprovalT = TypeVar("PendingApprovalT")


class ProxySubmitPrompt(AccessPointTextInput[AccessPointKeyT]):
    @property
    def prompt(self) -> str:
        return self.text


class ProxyApprovalDecision(AccessPointApprovalDecision[AccessPointKeyT]):
    pass


@dataclass(frozen=True)
class ProxyModelSelection(Generic[AccessPointKeyT]):
    access_point: AccessPointKeyT
    selected_model: str


@dataclass(frozen=True)
class ProxyLocalActionHandled:
    kind: str = "handled"


ProxyAction: TypeAlias = (
    ProxySubmitPrompt[AccessPointKeyT]
    | ProxyApprovalDecision[AccessPointKeyT]
    | ProxyModelSelection[AccessPointKeyT]
    | ProxyLocalActionHandled
)


class ProxyActionDriver(Protocol[AccessPointKeyT]):
    def submit_request(self, request: ProxySubmitPrompt[AccessPointKeyT]) -> None: ...

    def submit_approval_decision(self, decision: str) -> None: ...

    def submit_model_selection(self, selection: ProxyModelSelection[AccessPointKeyT]) -> None: ...


class ProxyApprovalState(Protocol[AccessPointKeyT]):
    def split_status_after_approval(self, *, access_point: AccessPointKeyT) -> None: ...

    def clear_pending_approval(self) -> None: ...


class ProxyAccessPointAdapter(
    Protocol[
        AccessPointKeyT,
        OutputRuntimeT,
        ResultT,
        ApprovalEventT,
        OutboundNoteT,
        StatusEventT,
        PendingApprovalT,
    ]
):
    def send_result(self, result: ResultT) -> None: ...

    def after_record_result(self, result: ResultT) -> None: ...

    def format_result_log(self, result: ResultT) -> str: ...

    def record_model_apply_result(self, result: object) -> None: ...

    def send_approval_prompt(self, event: ApprovalEventT) -> PendingApprovalT: ...

    def send_outbound_note(self, note: OutboundNoteT) -> None: ...

    def flush_delivery_lane(self) -> bool: ...

    def access_point_for_status_event(self, event: StatusEventT) -> AccessPointKeyT: ...

    def status_text_for_event(self, event: StatusEventT) -> str: ...

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
        driver.submit_request(action)
        return
    if isinstance(action, ProxyModelSelection):
        driver.submit_model_selection(action)
        return
    if isinstance(action, ProxyLocalActionHandled):
        return
    if isinstance(action, ProxyApprovalDecision):
        if action.decision == "details":
            return
        if approval_state is not None:
            approval_state.split_status_after_approval(access_point=action.access_point)
            approval_state.clear_pending_approval()
        driver.submit_approval_decision(action.decision)
        return
    raise AssertionError(f"unsupported proxy action: {action!r}")
