"""Transport-neutral events emitted by the interactive Codex driver."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol

from orchestrator.access_point_common import AccessPointId, AccessPointKey
from orchestrator.approval import ApprovalRequest
from orchestrator.processes import LifecycleLogger


class InteractivePrompt(Protocol):
    prompt: str

    @property
    def access_point(self) -> tuple[AccessPointId, AccessPointId | None]: ...


@dataclass
class InteractiveAgentRequest:
    chat_id: AccessPointId
    thread_id: AccessPointId | None
    prompt: str

    @property
    def access_point(self) -> tuple[AccessPointId, AccessPointId | None]:
        return (self.chat_id, self.thread_id)


@dataclass
class InteractiveAgentResult:
    chat_id: AccessPointId
    thread_id: AccessPointId | None
    prompt: str
    reply: str
    turn_id: str = ""


@dataclass
class InteractiveApprovalPromptEvent:
    chat_id: AccessPointId
    thread_id: AccessPointId | None
    request: ApprovalRequest


@dataclass
class InteractiveStatusEvent:
    chat_id: AccessPointId
    thread_id: AccessPointId | None
    status_text: str
    protocol_method: str = ""
    turn_id: str = ""
    suppress_status: bool = False
    status_snapshot: tuple[dict[str, str], ...] | None = None
    apply_info: dict[str, Any] | None = None


@dataclass
class InteractiveOutboundNote:
    chat_id: AccessPointId
    thread_id: AccessPointId | None
    text: str


@dataclass
class InteractiveInterruptResult:
    chat_id: AccessPointId
    thread_id: AccessPointId | None
    result: str
    error: str = ""


@dataclass
class InteractiveModelApplyResult:
    chat_id: AccessPointId
    thread_id: AccessPointId | None
    requested_model: str
    actual_model: str
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


@dataclass
class InteractiveRuntimeFailure:
    agent_id: str
    error: str
    state: str = "BOUND_IDLE"


class InteractiveCodexDriver(Protocol):
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def submit_request(self, request: InteractivePrompt) -> None: ...
    def submit_approval_decision(self, decision: str) -> None: ...
    def submit_approval_decision_now(self, decision: str) -> None: ...
    def poll_once(self) -> list[Any]: ...
    def consume_poll_progress(self) -> bool: ...
    def is_ready(self) -> bool: ...
    def get_terminal_error(self) -> str: ...
    def get_actual_thread_model(self) -> str: ...
    def get_thread_id(self) -> str: ...
    def get_thread_metadata(self) -> dict[str, object]: ...
    def get_item_status_snapshot(self) -> list[dict[str, str]]: ...
    def get_item_status_snapshot_for_turn(self, turn_id: str) -> list[dict[str, str]]: ...
    def get_last_item_apply_info(self) -> dict | None: ...
    def has_active_turn(self) -> bool: ...
    def pending_request_count(self) -> int: ...
    def interrupt_active_turn(self) -> None: ...


StewardDriverFactory = Callable[[AccessPointKey, LifecycleLogger], InteractiveCodexDriver]
RuntimeDriverFactory = Callable[
    [AccessPointKey, dict[str, Any], LifecycleLogger],
    InteractiveCodexDriver,
]
