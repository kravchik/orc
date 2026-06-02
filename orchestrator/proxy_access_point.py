"""Typed access-point actions and shared AP contracts for proxy runtimes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Hashable, Protocol, TypeAlias, TypeVar

from orchestrator.access_point_common import (
    AccessPointApprovalDecision,
    AccessPointTextInput,
)
from orchestrator.inspect_text import append_active_work_lines, format_thread_metadata_lines
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

    def get_thread_id(self) -> str: ...

    def get_actual_thread_model(self) -> str: ...

    def has_active_turn(self) -> bool: ...

    def get_item_status_snapshot(self) -> list[dict[str, str]]: ...

    def get_last_item_apply_info(self) -> dict | None: ...


class ProxyApprovalState(Protocol[AccessPointKeyT]):
    def split_status_after_approval(self, *, access_point: AccessPointKeyT) -> None: ...

    def clear_pending_approval(self) -> None: ...

    @property
    def pending_approval(self) -> object | None: ...


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


def build_proxy_inspect_text(
    *,
    mode: str,
    thread_metadata: dict[str, object] | None,
    thread_id: str,
    model: str,
    has_active_turn: bool,
    last_apply_info: dict | None,
    pending_approval: object | None,
    approval_command: str | None,
    approval_cwd: str | None,
    rows: list[dict[str, str]],
) -> str:
    active_turn_id = "none"
    pending_turn_id = _approval_turn_id(pending_approval)
    if pending_turn_id:
        active_turn_id = pending_turn_id
    elif has_active_turn and isinstance(last_apply_info, dict):
        raw_turn_id = last_apply_info.get("turn_id")
        if isinstance(raw_turn_id, str) and raw_turn_id.strip():
            active_turn_id = raw_turn_id.strip()

    lines = [
        "inspect",
        f"mode: {mode}",
    ]
    lines.extend(
        format_thread_metadata_lines(
            dict(thread_metadata or {}),
            thread_id_fallback=str(thread_id or "").strip(),
            model_fallback=str(model or "").strip(),
        )
    )
    lines.append(f"active turn: {active_turn_id}")
    lines.append(f"pending approval: {'yes' if pending_approval is not None else 'no'}")
    if approval_command:
        lines.append(f"approval command: {approval_command}")
    if approval_cwd:
        lines.append(f"approval cwd: {approval_cwd}")
    append_active_work_lines(lines, rows)
    return "\n".join(lines)


def extract_proxy_pending_approval_command(pending_approval: object | None) -> str | None:
    request = getattr(pending_approval, "request", None)
    if request is None and isinstance(pending_approval, dict):
        request = pending_approval.get("request")
    params = getattr(request, "params", None)
    if not isinstance(params, dict):
        return None
    command = params.get("command")
    if not isinstance(command, str):
        return None
    normalized = command.strip()
    return normalized or None


def extract_proxy_pending_approval_cwd(pending_approval: object | None) -> str | None:
    request = getattr(pending_approval, "request", None)
    if request is None and isinstance(pending_approval, dict):
        request = pending_approval.get("request")
    params = getattr(request, "params", None)
    if not isinstance(params, dict):
        return None
    cwd = params.get("cwd")
    if not isinstance(cwd, str):
        return None
    normalized = cwd.strip()
    return normalized or None


def _approval_turn_id(pending_approval: object | None) -> str | None:
    request = getattr(pending_approval, "request", None)
    if request is None and isinstance(pending_approval, dict):
        request = pending_approval.get("request")
    params = getattr(request, "params", None)
    if not isinstance(params, dict):
        return None
    raw_turn_id = params.get("turnId")
    if not isinstance(raw_turn_id, str):
        return None
    normalized = raw_turn_id.strip()
    return normalized or None
