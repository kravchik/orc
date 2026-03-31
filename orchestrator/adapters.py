"""Agent adapter abstractions for ORC1."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional, Protocol

from orchestrator import clock
from orchestrator.approval import (
    ApprovalDecisionProvider,
    ApprovalPolicy,
    ApprovalRequest,
    build_accept_settings,
)
from orchestrator.approval_runtime import (
    ApprovalServerRequest,
    _log_regex_allowlist_trace,
    build_approval_response_plan,
    parse_server_request,
)
from orchestrator.fsm import Fsm, State
from orchestrator.jsonrpc_stdio import StdioJsonRpcClient
from orchestrator.processes import LifecycleLogger
from orchestrator.protocol_status import (
    ProtocolItemTracker,
    is_hidden_item_status_event,
    should_emit_status_method,
)
from orchestrator.thread_resume import request_thread_start_or_resume


class AgentAdapter(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def start(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def ask_lead(self, prompt: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def ask_worker(self, prompt: str) -> str:
        raise NotImplementedError

    def supports_interactive(self) -> bool:
        return False

    def begin_turn(self, *, role: str, prompt: str) -> None:
        raise RuntimeError("interactive turns are not supported by this adapter")

    def poll_once(self) -> list["InteractiveAdapterEvent"]:
        return []

    def submit_approval_decision(self, decision: str) -> None:
        raise RuntimeError("interactive approval is not supported by this adapter")

    def is_turn_idle(self) -> bool:
        return True


class SessionLike(Protocol):
    def start(self, *, resume_thread_id: Optional[str] = None, start_params: Optional[dict] = None) -> None: ...

    def stop(self) -> None: ...

    def ask(
        self,
        prompt: str,
        *,
        approval_notify_callback: Optional[Callable[[str], None]] = None,
    ) -> str: ...

    def set_protocol_status_callback(
        self,
        callback: Optional[Callable[[str, str, dict], None]],
    ) -> None: ...


@dataclass
class InteractiveApprovalPending:
    server_request: ApprovalServerRequest
    approval_request: ApprovalRequest


@dataclass
class InteractiveTurnState:
    start_req_id: int
    turn_id: str | None
    prompt: str
    approval_notify_callback: Optional[Callable[[str], None]]
    deltas: list[str]
    final_deltas: list[str]
    final_answer_text: Optional[str]
    agent_message_phase_by_item_id: dict[str, str]
    deferred_pre_start_messages: list[dict] = field(default_factory=list)
    terminal_error: Optional[str] = None
    pending_approval: InteractiveApprovalPending | None = None
    pending_approval_emitted: bool = False


@dataclass(frozen=True)
class InteractiveTurnProgress:
    kind: str
    text: str | None = None
    approval_request: ApprovalRequest | None = None


@dataclass(frozen=True)
class InteractiveAdapterEvent:
    kind: str
    role: str
    text: str | None = None
    approval_request: ApprovalRequest | None = None


@dataclass
class InteractiveSessionStartState:
    initialize_req_id: int
    start_requests: list[tuple[str, dict]]
    requested_resume_thread_id: str | None = None
    active_start_req_id: int | None = None
    active_start_method: str | None = None
    started: bool = False
    deferred_messages: list[dict] = field(default_factory=list)


@dataclass(frozen=True)
class MockScenario:
    lead_messages: list[str]
    worker_messages: list[str]


class MockAgentAdapter(AgentAdapter):
    def __init__(self, logger: LifecycleLogger, scenario: MockScenario) -> None:
        self._logger = logger
        self._scenario = scenario
        self._started = False
        self._lead_idx = 0
        self._worker_idx = 0
        self._interactive_pending: tuple[str, str] | None = None

    @property
    def name(self) -> str:
        return "mock"

    def start(self) -> None:
        self._started = True
        self._logger.event("adapter_started", adapter=self.name)

    def stop(self) -> None:
        if self._started:
            self._logger.event("adapter_stopped", adapter=self.name)
        self._started = False

    def ask_lead(self, prompt: str) -> str:
        self._require_started()
        if self._lead_idx >= len(self._scenario.lead_messages):
            raise RuntimeError("mock lead message queue exhausted")
        msg = self._scenario.lead_messages[self._lead_idx]
        self._lead_idx += 1
        self._logger.event(
            "adapter_lead_message",
            adapter=self.name,
            prompt=prompt,
            response=msg,
        )
        return msg

    def ask_worker(self, prompt: str) -> str:
        self._require_started()
        if self._worker_idx >= len(self._scenario.worker_messages):
            raise RuntimeError("mock worker message queue exhausted")
        msg = self._scenario.worker_messages[self._worker_idx]
        self._worker_idx += 1
        self._logger.event(
            "adapter_worker_message",
            adapter=self.name,
            prompt=prompt,
            response=msg,
        )
        return msg

    def supports_interactive(self) -> bool:
        return True

    def begin_turn(self, *, role: str, prompt: str) -> None:
        self._require_started()
        if self._interactive_pending is not None:
            raise RuntimeError("mock adapter already has an active interactive turn")
        self._interactive_pending = (role, prompt)

    def poll_once(self) -> list[InteractiveAdapterEvent]:
        pending = self._interactive_pending
        if pending is None:
            return []
        role, prompt = pending
        self._interactive_pending = None
        if role == "lead":
            text = self.ask_lead(prompt)
        elif role == "worker":
            text = self.ask_worker(prompt)
        else:
            raise RuntimeError(f"unsupported interactive role: {role}")
        return [InteractiveAdapterEvent(kind="completed", role=role, text=text)]

    def is_turn_idle(self) -> bool:
        return self._interactive_pending is None

    def _require_started(self) -> None:
        if not self._started:
            raise RuntimeError("adapter is not started")


class CodexAppServerAdapter(AgentAdapter):
    """Real transport adapter for codex app-server (initialize/thread/turn)."""

    def __init__(
        self,
        logger: LifecycleLogger,
        lead_command: list[str],
        worker_command: list[str],
        cwd: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        request_timeout_sec: float = 30.0,
        rpc_timeout_sec: float = 5.0,
        rpc_retries: int = 3,
        approval_policy: Optional[ApprovalPolicy] = None,
        fsm: Optional[Fsm] = None,
        lead_approval_policy: str = "never",
        worker_approval_policy: str = "never",
        lead_sandbox: str = "workspace-write",
        worker_sandbox: str = "workspace-write",
        approval_decision_provider: Optional[ApprovalDecisionProvider] = None,
        approval_notify_callback: Optional[Callable[[str], None]] = None,
        protocol_status_callback: Optional[Callable[[str, str, dict], None]] = None,
        protocol_log_include_params: bool = True,
        session_factory: Optional[Callable[..., SessionLike]] = None,
    ) -> None:
        self._logger = logger
        self._lead_command = lead_command
        self._worker_command = worker_command
        self._cwd = cwd
        self._env = env
        self._request_timeout_sec = request_timeout_sec
        self._rpc_timeout_sec = rpc_timeout_sec
        self._rpc_retries = rpc_retries
        self._approval_policy = approval_policy or ApprovalPolicy.default()
        self._fsm = fsm
        self._lead_approval_policy = lead_approval_policy
        self._worker_approval_policy = worker_approval_policy
        self._lead_sandbox = lead_sandbox
        self._worker_sandbox = worker_sandbox
        self._approval_decision_provider = approval_decision_provider
        self._approval_notify_callback = approval_notify_callback
        self._protocol_status_callback = protocol_status_callback
        self._protocol_log_include_params = protocol_log_include_params
        self._session_factory: Callable[..., SessionLike] = session_factory or CodexJsonRpcSession
        self._lead: Optional[SessionLike] = None
        self._worker: Optional[SessionLike] = None
        self._started = False
        self._interactive_active_role: str | None = None
        self._interactive_active_state: InteractiveTurnState | None = None

    @property
    def name(self) -> str:
        return "codex-app-server"

    def start(self) -> None:
        if self._started:
            raise RuntimeError("adapter already started")
        self._lead = self._session_factory(
            role="lead",
            command=self._lead_command,
            logger=self._logger,
            cwd=self._cwd,
            env=self._env,
            request_timeout_sec=self._request_timeout_sec,
            rpc_timeout_sec=self._rpc_timeout_sec,
            rpc_retries=self._rpc_retries,
            approval_policy=self._approval_policy,
            fsm=self._fsm,
            thread_approval_policy=self._lead_approval_policy,
            thread_sandbox=self._lead_sandbox,
            approval_decision_provider=self._approval_decision_provider,
            approval_notify_callback=self._approval_notify_callback,
            protocol_status_callback=self._protocol_status_callback,
            protocol_log_include_params=self._protocol_log_include_params,
        )
        self._worker = self._session_factory(
            role="worker",
            command=self._worker_command,
            logger=self._logger,
            cwd=self._cwd,
            env=self._env,
            request_timeout_sec=self._request_timeout_sec,
            rpc_timeout_sec=self._rpc_timeout_sec,
            rpc_retries=self._rpc_retries,
            approval_policy=self._approval_policy,
            fsm=self._fsm,
            thread_approval_policy=self._worker_approval_policy,
            thread_sandbox=self._worker_sandbox,
            approval_decision_provider=self._approval_decision_provider,
            approval_notify_callback=self._approval_notify_callback,
            protocol_status_callback=self._protocol_status_callback,
            protocol_log_include_params=self._protocol_log_include_params,
        )
        self._lead.start()
        self._worker.start()
        self._started = True
        self._logger.event("adapter_started", adapter=self.name)

    def stop(self) -> None:
        if self._lead is not None:
            self._lead.stop()
        if self._worker is not None:
            self._worker.stop()
        if self._started:
            self._logger.event("adapter_stopped", adapter=self.name)
        self._started = False

    def bind_fsm(self, fsm: Fsm) -> None:
        self._fsm = fsm

    def ask_lead(self, prompt: str) -> str:
        self._require_started()
        return self._lead.ask(prompt)  # type: ignore[union-attr]

    def ask_worker(self, prompt: str) -> str:
        self._require_started()
        return self._worker.ask(prompt)  # type: ignore[union-attr]

    def supports_interactive(self) -> bool:
        return True

    def begin_turn(self, *, role: str, prompt: str) -> None:
        self._require_started()
        if self._interactive_active_state is not None:
            raise RuntimeError("interactive turn already in progress")
        session = self._session_for_role(role)
        self._interactive_active_role = role
        self._interactive_active_state = session.begin_interactive_turn(  # type: ignore[attr-defined]
            prompt,
            approval_notify_callback=self._approval_notify_callback,
        )

    def poll_once(self) -> list[InteractiveAdapterEvent]:
        state = self._interactive_active_state
        role = self._interactive_active_role
        if state is None or role is None:
            return []
        session = self._session_for_role(role)
        progress = session.poll_interactive_turn(state, timeout_sec=0.0)  # type: ignore[attr-defined]
        if progress.kind == "idle":
            return []
        if progress.kind == "approval_required":
            return [
                InteractiveAdapterEvent(
                    kind="approval_required",
                    role=role,
                    approval_request=progress.approval_request,
                )
            ]
        if progress.kind == "completed":
            self._interactive_active_role = None
            self._interactive_active_state = None
            return [InteractiveAdapterEvent(kind="completed", role=role, text=progress.text or "")]
        raise RuntimeError(f"unsupported interactive progress kind: {progress.kind}")

    def submit_approval_decision(self, decision: str) -> None:
        state = self._interactive_active_state
        role = self._interactive_active_role
        if state is None or role is None:
            raise RuntimeError("interactive approval has no active turn")
        session = self._session_for_role(role)
        session.submit_interactive_approval_decision(state, decision=decision)  # type: ignore[attr-defined]

    def is_turn_idle(self) -> bool:
        return self._interactive_active_state is None

    def _require_started(self) -> None:
        if not self._started:
            raise RuntimeError("adapter is not started")

    def _session_for_role(self, role: str) -> SessionLike:
        if role == "lead":
            if self._lead is None:
                raise RuntimeError("lead session is not initialized")
            return self._lead
        if role == "worker":
            if self._worker is None:
                raise RuntimeError("worker session is not initialized")
            return self._worker
        raise RuntimeError(f"unsupported role: {role}")

    def set_protocol_status_callback(
        self,
        callback: Optional[Callable[[str, str, dict], None]],
    ) -> None:
        self._protocol_status_callback = callback
        if self._lead is not None:
            self._lead.set_protocol_status_callback(callback)
        if self._worker is not None:
            self._worker.set_protocol_status_callback(callback)

    def set_protocol_item_callback(
        self,
        callback: Optional[Callable[[str, str], None]],
    ) -> None:
        if self._lead is not None:
            self._lead.set_protocol_item_callback(callback)
        if self._worker is not None:
            self._worker.set_protocol_item_callback(callback)


class CodexJsonRpcSession:
    _TERMINAL_INTERACTION_METHODS = {
        "item/commandExecution/terminalInteraction",
        "codex/event/terminal_interaction",
    }
    _USER_INPUT_REQUEST_METHODS = {
        "item/tool/requestUserInput",
        "tool/requestUserInput",
    }

    def __init__(
        self,
        role: str,
        command: list[str],
        logger: LifecycleLogger,
        cwd: Optional[str],
        env: Optional[Mapping[str, str]],
        request_timeout_sec: float,
        rpc_timeout_sec: float,
        rpc_retries: int,
        approval_policy: Optional[ApprovalPolicy],
        fsm: Optional[Fsm],
        thread_approval_policy: str,
        thread_sandbox: str,
        approval_decision_provider: Optional[ApprovalDecisionProvider],
        approval_notify_callback: Optional[Callable[[str], None]],
        protocol_status_callback: Optional[Callable[[str, str, dict], None]],
        protocol_log_include_params: bool,
        client_factory: Optional[Callable[..., Any]] = None,
    ) -> None:
        self._role = role
        self._command = command
        self._logger = logger
        self._cwd = cwd
        self._env = env
        self._request_timeout_sec = request_timeout_sec
        self._rpc_timeout_sec = max(0.1, float(rpc_timeout_sec))
        self._rpc_retries = max(0, int(rpc_retries))
        self._approval_policy = approval_policy
        self._fsm = fsm
        self._thread_approval_policy = thread_approval_policy
        self._thread_sandbox = thread_sandbox
        self._approval_decision_provider = approval_decision_provider
        self._approval_notify_callback = approval_notify_callback
        self._protocol_status_callback = protocol_status_callback
        self._protocol_log_include_params = protocol_log_include_params
        self._protocol_item_callback: Optional[Callable[[str, str], None]] = None
        client_ctor = client_factory or StdioJsonRpcClient
        self._client = client_ctor(
            command=self._command,
            cwd=self._cwd,
            env=self._env,
            on_message=self._on_protocol_in,
        )
        self._thread_id: Optional[str] = None
        self._thread_model: Optional[str] = None
        self._pending_messages: list[dict] = []
        self._always_allow_commands: set[str] = set()
        self._item_tracker = ProtocolItemTracker()

    @property
    def thread_id(self) -> Optional[str]:
        return self._thread_id

    @property
    def thread_model(self) -> Optional[str]:
        return self._thread_model

    @property
    def pid(self) -> Optional[int]:
        return self._client.pid

    @property
    def returncode(self) -> Optional[int]:
        return self._client.returncode

    def set_protocol_status_callback(
        self,
        callback: Optional[Callable[[str, str, dict], None]],
    ) -> None:
        self._protocol_status_callback = callback

    def set_protocol_item_callback(
        self,
        callback: Optional[Callable[[str, str], None]],
    ) -> None:
        self._protocol_item_callback = callback

    def get_item_status_snapshot(self, turn_id: str | None = None) -> list[dict[str, str]]:
        if turn_id is None:
            return self._item_tracker.snapshot()
        return self._item_tracker.snapshot_for_turn(turn_id)

    def get_last_item_apply_info(self) -> dict | None:
        return self._item_tracker.last_apply_info()

    def start(
        self,
        *,
        resume_thread_id: Optional[str] = None,
        start_params: Optional[dict] = None,
    ) -> None:
        state = self.begin_interactive_start(
            resume_thread_id=resume_thread_id,
            start_params=start_params,
        )
        deadline: float | None
        if self._request_timeout_sec > 0:
            deadline = clock.monotonic() + self._request_timeout_sec
        else:
            deadline = None
        while True:
            if self.poll_interactive_start(state, timeout_sec=0.05):
                return
            if deadline is not None and clock.monotonic() >= deadline:
                raise RuntimeError("timeout waiting for interactive session start")

    def stop(self) -> None:
        pid = self._client.pid
        self._client.stop()
        if pid is not None:
            self._logger.event(
                "process_stopped",
                role=self._role,
                pid=pid,
                returncode=self._client.returncode,
            )
        self._pending_messages = []

    def ask(
        self,
        prompt: str,
        *,
        approval_notify_callback: Optional[Callable[[str], None]] = None,
    ) -> str:
        if self._thread_id is None:
            raise RuntimeError("thread not initialized")
        req_id = self._send_request(
            method="turn/start",
            params={
                "threadId": self._thread_id,
                "input": [{"type": "text", "text": prompt, "text_elements": []}],
            },
        )
        response = self._wait_for_response(
            req_id=req_id,
            approval_notify_callback=approval_notify_callback,
        )
        turn_id = response.get("result", {}).get("turn", {}).get("id")
        if turn_id is None:
            raise RuntimeError("turn/start response did not contain turn id")

        self._logger.event(f"{self._role}_input", prompt=prompt, turn_id=turn_id)
        text = self._wait_for_turn_completion(
            turn_id=turn_id,
            approval_notify_callback=approval_notify_callback,
        )
        self._logger.event(f"{self._role}_output", response=text, turn_id=turn_id)
        return text

    def begin_interactive_turn(
        self,
        prompt: str,
        *,
        approval_notify_callback: Optional[Callable[[str], None]] = None,
    ) -> InteractiveTurnState:
        if self._thread_id is None:
            raise RuntimeError("thread not initialized")
        req_id = self._send_request(
            method="turn/start",
            params={
                "threadId": self._thread_id,
                "input": [{"type": "text", "text": prompt, "text_elements": []}],
            },
        )
        self._logger.event(
            "interactive_turn_start_requested",
            role=self._role,
            req_id=req_id,
            thread_id=self._thread_id,
            prompt_preview=str(prompt).strip()[:200],
        )
        return InteractiveTurnState(
            start_req_id=req_id,
            turn_id=None,
            prompt=prompt,
            approval_notify_callback=approval_notify_callback,
            deltas=[],
            final_deltas=[],
            final_answer_text=None,
            agent_message_phase_by_item_id={},
        )

    def begin_interactive_start(
        self,
        *,
        resume_thread_id: Optional[str] = None,
        start_params: Optional[dict] = None,
    ) -> InteractiveSessionStartState:
        self._item_tracker.clear()
        self._thread_id = None
        self._thread_model = None
        self._client.start()
        self._logger.event(
            "process_started",
            role=self._role,
            pid=self._client.pid,
            command=self._command,
        )
        req_id = self._send_request(
            method="initialize",
            params={"clientInfo": {"name": f"orc1-{self._role}", "version": "0.1"}},
        )
        params = start_params or {
            "approvalPolicy": self._thread_approval_policy,
            "sandbox": self._thread_sandbox,
        }
        if resume_thread_id:
            start_requests = [
                ("thread/resume", {"threadId": resume_thread_id}),
                ("thread/resume", {"id": resume_thread_id}),
            ]
        else:
            start_requests = [("thread/start", params)]
        return InteractiveSessionStartState(
            initialize_req_id=req_id,
            start_requests=start_requests,
            requested_resume_thread_id=resume_thread_id,
        )

    def poll_interactive_start(
        self,
        state: InteractiveSessionStartState,
        *,
        timeout_sec: float = 0.0,
    ) -> bool:
        deadline = clock.monotonic() + max(0.0, float(timeout_sec))
        deferred = state.deferred_messages
        while True:
            if state.started:
                self._pending_messages = deferred + self._pending_messages
                state.deferred_messages = []
                return True
            remaining = max(0.0, deadline - clock.monotonic())
            wait_timeout = 0.0 if timeout_sec <= 0 else max(0.0, remaining)
            if self._pending_messages:
                msg = self._pending_messages.pop(0)
            else:
                msg = self._read_message(timeout_sec=max(0.0, min(0.05, wait_timeout)) if timeout_sec > 0 else 0.0)
            if msg is None:
                state.deferred_messages = deferred
                return False
            if state.active_start_req_id is None:
                if msg.get("id") == state.initialize_req_id and "result" in msg:
                    self._send_notification(method="initialized", params={})
                    method, params = state.start_requests[0]
                    state.active_start_method = method
                    state.active_start_req_id = self._send_request(method=method, params=params)
                    if remaining <= 0.0:
                        state.deferred_messages = deferred
                        return False
                    continue
                if msg.get("id") == state.initialize_req_id and "error" in msg:
                    state.deferred_messages = deferred
                    raise RuntimeError(f"protocol error for request {state.initialize_req_id}: {msg['error']}")
            else:
                if msg.get("id") == state.active_start_req_id and "result" in msg:
                    result = msg.get("result") or {}
                    thread_id = str((result.get("thread") or {}).get("id") or "")
                    model = str(result.get("model") or "").strip()
                    if not thread_id:
                        state.deferred_messages = deferred
                        raise RuntimeError(f"{state.active_start_method} response did not contain thread id")
                    if not model:
                        state.deferred_messages = deferred
                        raise RuntimeError(f"{state.active_start_method} response did not contain model")
                    self._thread_id = thread_id
                    self._thread_model = model
                    state.started = True
                    if state.active_start_method == "thread/resume":
                        self._logger.event(
                            f"{self._role}_thread_resumed",
                            thread_id=thread_id,
                            requested_thread_id=state.requested_resume_thread_id,
                            model=model,
                        )
                    else:
                        self._logger.event(f"{self._role}_thread_started", thread_id=thread_id, model=model)
                    self._pending_messages = deferred + self._pending_messages
                    state.deferred_messages = []
                    return True
                if msg.get("id") == state.active_start_req_id and "error" in msg:
                    if state.active_start_method == "thread/resume" and len(state.start_requests) > 1:
                        state.start_requests.pop(0)
                        method, params = state.start_requests[0]
                        state.active_start_method = method
                        state.active_start_req_id = self._send_request(method=method, params=params)
                        if remaining <= 0.0:
                            state.deferred_messages = deferred
                            return False
                        continue
                    state.deferred_messages = deferred
                    raise RuntimeError(f"protocol error for request {state.active_start_req_id}: {msg['error']}")
            approval_started_at = clock.monotonic()
            if self._maybe_handle_server_request(msg):
                deadline += max(0.0, clock.monotonic() - approval_started_at)
                continue
            deferred.append(msg)
            if remaining <= 0.0:
                state.deferred_messages = deferred
                return False

    def poll_interactive_turn(
        self,
        state: InteractiveTurnState,
        *,
        timeout_sec: float = 0.0,
    ) -> InteractiveTurnProgress:
        if state.turn_id is None:
            started = self._poll_interactive_turn_start(
                state=state,
                timeout_sec=timeout_sec,
            )
            if not started:
                self._logger.event(
                    "interactive_turn_poll_idle_before_start",
                    role=self._role,
                    start_req_id=state.start_req_id,
                )
                return InteractiveTurnProgress(kind="idle")
        if state.pending_approval is not None:
            if state.pending_approval_emitted:
                return InteractiveTurnProgress(kind="idle")
            state.pending_approval_emitted = True
            return InteractiveTurnProgress(
                kind="approval_required",
                approval_request=state.pending_approval.approval_request,
            )
        deadline = clock.monotonic() + max(0.0, float(timeout_sec))
        while True:
            remaining = max(0.0, deadline - clock.monotonic())
            wait_timeout = 0.0 if timeout_sec <= 0 else max(0.0, remaining)
            if self._pending_messages:
                msg = self._pending_messages.pop(0)
            else:
                msg = self._read_message(timeout_sec=max(0.0, min(0.05, wait_timeout)) if timeout_sec > 0 else 0.0)
            if msg is None:
                self._logger.event(
                    "interactive_turn_poll_no_message",
                    role=self._role,
                    turn_id=state.turn_id,
                )
                return InteractiveTurnProgress(kind="idle")
            progress = self._consume_interactive_turn_message(state=state, msg=msg)
            if progress is not None:
                return progress
            if timeout_sec <= 0:
                return InteractiveTurnProgress(kind="idle")
            if remaining <= 0.0:
                return InteractiveTurnProgress(kind="idle")

    def _poll_interactive_turn_start(
        self,
        *,
        state: InteractiveTurnState,
        timeout_sec: float,
    ) -> bool:
        deadline = clock.monotonic() + max(0.0, float(timeout_sec))
        deferred = state.deferred_pre_start_messages
        while True:
            remaining = max(0.0, deadline - clock.monotonic())
            wait_timeout = 0.0 if timeout_sec <= 0 else max(0.0, remaining)
            if self._pending_messages:
                msg = self._pending_messages.pop(0)
            else:
                msg = self._read_message(timeout_sec=max(0.0, min(0.05, wait_timeout)) if timeout_sec > 0 else 0.0)
            if msg is None:
                self._logger.event(
                    "interactive_turn_start_poll_no_message",
                    role=self._role,
                    start_req_id=state.start_req_id,
                )
                state.deferred_pre_start_messages = deferred
                return False
            if msg.get("id") == state.start_req_id and "result" in msg:
                turn_id = msg.get("result", {}).get("turn", {}).get("id")
                if turn_id is None:
                    state.deferred_pre_start_messages = deferred
                    raise RuntimeError("turn/start response did not contain turn id")
                turn_id_str = str(turn_id)
                state.turn_id = turn_id_str
                self._logger.event(f"{self._role}_input", prompt=state.prompt, turn_id=turn_id_str)
                self._logger.event(
                    "interactive_turn_started",
                    role=self._role,
                    start_req_id=state.start_req_id,
                    turn_id=turn_id_str,
                )
                self._pending_messages = deferred + self._pending_messages
                state.deferred_pre_start_messages = []
                return True
            if msg.get("id") == state.start_req_id and "error" in msg:
                state.deferred_pre_start_messages = deferred
                raise RuntimeError(f"protocol error for request {state.start_req_id}: {msg['error']}")
            approval_started_at = clock.monotonic()
            if self._maybe_handle_server_request(
                msg,
                approval_notify_callback=state.approval_notify_callback,
            ):
                deadline += max(0.0, clock.monotonic() - approval_started_at)
                continue
            deferred.append(msg)
            if remaining <= 0.0:
                state.deferred_pre_start_messages = deferred
                return False

    def submit_interactive_approval_decision(
        self,
        state: InteractiveTurnState,
        *,
        decision: str,
    ) -> None:
        pending = state.pending_approval
        if pending is None:
            raise RuntimeError("interactive turn is not waiting for approval")
        normalized = ApprovalPolicy.normalize_human_decision_with_always(
            raw=decision,
            allow_always=ApprovalPolicy.is_approval_method(pending.server_request.method),
        )
        always_allow = normalized == "always_allow"
        resolved_decision = "accept" if always_allow else normalized
        self._logger.event(
            "approval_human_decision",
            role=self._role,
            id=pending.server_request.req_id,
            method=pending.server_request.method,
            decision=resolved_decision,
            always_allow=always_allow,
        )
        result: dict[str, Any] = {"decision": resolved_decision}
        if always_allow:
            command = pending.server_request.params.get("command")
            if isinstance(command, str) and command:
                self._always_allow_commands.add(command)
            accept_settings = build_accept_settings(params=pending.server_request.params)
            if accept_settings is not None:
                result["acceptSettings"] = accept_settings
                self._logger.event(
                    "approval_accept_settings_applied",
                    role=self._role,
                    id=pending.server_request.req_id,
                    accept_settings=accept_settings,
                )
            else:
                self._logger.event(
                    "approval_accept_settings_skipped",
                    role=self._role,
                    id=pending.server_request.req_id,
                    reason="missing proposedExecpolicyAmendment",
                )
        self._send_response(req_id=pending.server_request.req_id, result=result)
        if self._fsm is not None and self._fsm.state in (State.WORKER_WAITS_APPROVAL, State.HUMAN_THINKS):
            self._fsm.transition(State.WORKER_THINKS, reason="approval_decided")
        state.pending_approval = None
        state.pending_approval_emitted = False

    def _initialize(self) -> None:
        req_id = self._send_request(
            method="initialize",
            params={"clientInfo": {"name": f"orc1-{self._role}", "version": "0.1"}},
        )
        self._wait_for_response(req_id=req_id)
        self._send_notification(method="initialized", params={})

    def _thread_start(
        self,
        *,
        resume_thread_id: Optional[str] = None,
        start_params: Optional[dict] = None,
    ) -> None:
        params = start_params or {
            "approvalPolicy": self._thread_approval_policy,
            "sandbox": self._thread_sandbox,
        }
        if resume_thread_id is None:
            req_id = self._send_request(method="thread/start", params=params)
            response = self._wait_for_response(req_id=req_id)
            result = response.get("result", {}) if isinstance(response.get("result"), dict) else {}
            thread_id = result.get("thread", {}).get("id") if isinstance(result.get("thread"), dict) else None
            thread_model = str(result.get("model") or "").strip()
            if thread_id is None:
                raise RuntimeError("thread/start response did not contain thread id")
            if not thread_model:
                raise RuntimeError("thread/start response did not contain model")
            self._thread_id = str(thread_id)
            self._thread_model = thread_model
            return

        def _request_fn(method: str, req_params: dict) -> dict:
            req_id = self._send_request(method=method, params=req_params)
            return self._wait_for_response(req_id=req_id)

        start_result = request_thread_start_or_resume(
            request_fn=_request_fn,
            logger=self._logger,
            role_label=self._role,
            start_params=params,
            resume_thread_id=resume_thread_id,
        )
        self._thread_id = start_result.thread_id
        self._thread_model = start_result.model

    def _consume_interactive_turn_message(
        self,
        *,
        state: InteractiveTurnState,
        msg: dict,
    ) -> InteractiveTurnProgress | None:
        method = msg.get("method")
        params = msg.get("params", {})
        item_obj: dict | None = None
        if isinstance(params, dict):
            if method in ("item/started", "item/completed"):
                raw_item = params.get("item")
                item_obj = raw_item if isinstance(raw_item, dict) else None
            elif method in ("codex/event/item_started", "codex/event/item_completed"):
                raw_msg = params.get("msg")
                if isinstance(raw_msg, dict):
                    nested_item = raw_msg.get("item")
                    item_obj = nested_item if isinstance(nested_item, dict) else None
        if isinstance(item_obj, dict):
            item_type = str(item_obj.get("type") or "").strip()
            item_id = str(item_obj.get("id") or "").strip()
            if item_type and item_type.lower() == "agentmessage" and item_id:
                phase = str(item_obj.get("phase") or "").strip()
                if phase:
                    state.agent_message_phase_by_item_id[item_id] = phase
                if phase != "commentary":
                    text = item_obj.get("text")
                    if isinstance(text, str) and text.strip():
                        state.final_answer_text = text.strip()
        if method == "item/agentMessage/delta" and str(params.get("turnId")) == state.turn_id:
            delta = params.get("delta", "")
            if isinstance(delta, str):
                item_id = str(params.get("itemId") or "").strip()
                if item_id:
                    phase = state.agent_message_phase_by_item_id.get(item_id, "")
                    if phase == "commentary":
                        return None
                    if phase == "final_answer":
                        state.final_deltas.append(delta)
                        return None
                state.deltas.append(delta)
            return None
        if method == "codex/event/agent_message" and isinstance(params, dict):
            event_turn_id = str(params.get("id") or "").strip()
            if event_turn_id == state.turn_id:
                msg_payload = params.get("msg")
                if isinstance(msg_payload, dict):
                    phase = str(msg_payload.get("phase") or "").strip()
                    text = msg_payload.get("message")
                    if isinstance(text, str) and text.strip():
                        if phase in ("", "final_answer"):
                            state.final_answer_text = text.strip()
            return None
        if method == "turn/completed" and str(params.get("turn", {}).get("id")) == state.turn_id:
            self._emit_protocol_status(method=method, params=params)
            status = params.get("turn", {}).get("status")
            text = state.final_answer_text
            if not text:
                joined_final = "".join(state.final_deltas).strip()
                text = joined_final if joined_final else "".join(state.deltas).strip()
            if status == "failed":
                error_payload = params.get("turn", {}).get("error")
                raise RuntimeError(f"turn failed: {error_payload}")
            self._logger.event(f"{self._role}_output", response=text, turn_id=state.turn_id)
            return InteractiveTurnProgress(kind="completed", text=text)
        if method == "error" and str(params.get("turnId")) == state.turn_id:
            state.terminal_error = params.get("error", {}).get("message") or str(params)

        request = parse_server_request(msg)
        if request is not None:
            if request.method in self._USER_INPUT_REQUEST_METHODS:
                self._handle_user_input_request(
                    request=request,
                    approval_notify_callback=state.approval_notify_callback,
                )
                return None
            if not ApprovalPolicy.is_approval_method(request.method):
                self._logger.event(
                    "server_request_unsupported_method",
                    role=self._role,
                    id=request.req_id,
                    method=request.method,
                )
                self._send_error(
                    req_id=request.req_id,
                    code=-32601,
                    message=f"unsupported server request method: {request.method}",
                    data={"method": request.method},
                )
                return None
            progress = self._prepare_interactive_approval(
                state=state,
                request=request,
            )
            if progress is not None:
                return progress
            return None

        if isinstance(method, str):
            self._emit_protocol_status(method=method, params=params)
        return None

    def _prepare_interactive_approval(
        self,
        *,
        state: InteractiveTurnState,
        request: ApprovalServerRequest,
    ) -> InteractiveTurnProgress | None:
        transitioned = False
        if (
            self._role == "worker"
            and self._fsm is not None
            and self._fsm.state is State.WORKER_THINKS
        ):
            self._fsm.transition(State.WORKER_WAITS_APPROVAL, reason="approval_requested")
            transitioned = True

        def _before_human_required() -> None:
            if (
                transitioned
                and self._fsm is not None
                and self._fsm.state is State.WORKER_WAITS_APPROVAL
            ):
                self._fsm.transition(State.HUMAN_THINKS, reason="approval_human_required")

        decision_trace = self._approval_policy.decide_with_trace(method=request.method, params=request.params) if self._approval_policy is not None else None
        if decision_trace is not None:
            _log_regex_allowlist_trace(
                logger=self._logger,
                role=self._role,
                request=request,
                decision=decision_trace.decision,
                decision_source=decision_trace.source,
                command=decision_trace.command,
                regex_trace=decision_trace.regex_trace,
                approval_notify=state.approval_notify_callback or self._approval_notify_callback,
            )
        decision = decision_trace.decision if decision_trace is not None else "human"
        if decision != "human":
            plan = build_approval_response_plan(
                request=request,
                logger=self._logger,
                role=self._role,
                approval_policy=self._approval_policy,
                approval_decision_provider=None,
                approval_notify=state.approval_notify_callback or self._approval_notify_callback,
                always_allow_commands=self._always_allow_commands,
                before_human_required=_before_human_required,
                decide_human=None,
                log_approval_requested=True,
                log_human_required=True,
                log_human_decision=True,
                log_auto_decision=True,
            )
            self._send_response(req_id=plan.req_id, result=plan.result)
            if self._fsm is not None and self._fsm.state in (State.WORKER_WAITS_APPROVAL, State.HUMAN_THINKS):
                self._fsm.transition(State.WORKER_THINKS, reason="approval_decided")
            return None

        self._logger.event(
            "approval_requested",
            role=self._role,
            id=request.req_id,
            method=request.method,
            params=request.params,
        )
        _before_human_required()
        self._logger.event(
            "approval_human_required",
            role=self._role,
            id=request.req_id,
            method=request.method,
            source=decision_trace.source if decision_trace is not None else "interactive",
        )
        approval_request = ApprovalRequest(
            req_id=request.req_id,
            method=request.method,
            params=request.params,
            role=self._role or "agent",
        )
        state.pending_approval = InteractiveApprovalPending(
            server_request=request,
            approval_request=approval_request,
        )
        state.pending_approval_emitted = False
        return InteractiveTurnProgress(kind="approval_required", approval_request=approval_request)

    def _send_request(self, method: str, params: dict) -> int:
        req_id = self._client.send_request(method=method, params=params)
        self._logger.event(
            "protocol_out",
            role=self._role,
            id=req_id,
            method=method,
        )
        return req_id

    def _wait_for_response(
        self,
        req_id: int,
        *,
        approval_notify_callback: Optional[Callable[[str], None]] = None,
    ) -> dict:
        deferred: list[dict] = []
        attempts = self._rpc_retries + 1
        for attempt in range(1, attempts + 1):
            deadline = clock.monotonic() + self._rpc_timeout_sec
            while clock.monotonic() < deadline:
                if self._pending_messages:
                    msg = self._pending_messages.pop(0)
                else:
                    msg = self._read_message(timeout_sec=max(0.05, deadline - clock.monotonic()))
                if msg is None:
                    continue
                if msg.get("id") == req_id and "result" in msg:
                    self._pending_messages = deferred + self._pending_messages
                    return msg
                if msg.get("id") == req_id and "error" in msg:
                    self._pending_messages = deferred + self._pending_messages
                    raise RuntimeError(f"protocol error for request {req_id}: {msg['error']}")
                approval_started_at = clock.monotonic()
                if self._maybe_handle_server_request(
                    msg,
                    approval_notify_callback=approval_notify_callback,
                ):
                    deadline += max(0.0, clock.monotonic() - approval_started_at)
                    continue
                deferred.append(msg)
            if attempt < attempts:
                self._logger.event(
                    "protocol_rpc_timeout_retry",
                    role=self._role,
                    id=req_id,
                    attempt=attempt,
                    max_attempts=attempts,
                )
        self._pending_messages = deferred + self._pending_messages
        raise RuntimeError(f"timeout waiting for response id={req_id}")

    def _send_notification(self, method: str, params: dict) -> None:
        self._client.send_notification(method=method, params=params)
        self._logger.event(
            "protocol_out",
            role=self._role,
            method=method,
        )

    def _wait_for_turn_completion(
        self,
        turn_id: str,
        *,
        approval_notify_callback: Optional[Callable[[str], None]] = None,
    ) -> str:
        deadline: Optional[float]
        if self._request_timeout_sec > 0:
            deadline = clock.monotonic() + self._request_timeout_sec
        else:
            deadline = None
        deltas: list[str] = []
        final_deltas: list[str] = []
        final_answer_text: Optional[str] = None
        agent_message_phase_by_item_id: dict[str, str] = {}
        terminal_error: Optional[str] = None
        turn_id_str = str(turn_id)
        deferred: list[dict] = []
        while deadline is None or clock.monotonic() < deadline:
            if self._pending_messages:
                msg = self._pending_messages.pop(0)
            else:
                if deadline is None:
                    wait_timeout = 0.5
                else:
                    wait_timeout = max(0.05, deadline - clock.monotonic())
                msg = self._read_message(timeout_sec=wait_timeout)
            if msg is None:
                continue
            method = msg.get("method")
            params = msg.get("params", {})
            item_obj: dict | None = None
            if isinstance(params, dict):
                if method in ("item/started", "item/completed"):
                    raw_item = params.get("item")
                    item_obj = raw_item if isinstance(raw_item, dict) else None
                elif method in ("codex/event/item_started", "codex/event/item_completed"):
                    raw_msg = params.get("msg")
                    if isinstance(raw_msg, dict):
                        nested_item = raw_msg.get("item")
                        item_obj = nested_item if isinstance(nested_item, dict) else None
            if isinstance(item_obj, dict):
                item_type = str(item_obj.get("type") or "").strip()
                item_id = str(item_obj.get("id") or "").strip()
                if item_type and item_type.lower() == "agentmessage" and item_id:
                    phase = str(item_obj.get("phase") or "").strip()
                    if phase:
                        agent_message_phase_by_item_id[item_id] = phase
                    if phase != "commentary":
                        text = item_obj.get("text")
                        if isinstance(text, str) and text.strip():
                            final_answer_text = text.strip()
            if method == "item/agentMessage/delta" and str(params.get("turnId")) == turn_id_str:
                delta = params.get("delta", "")
                if isinstance(delta, str):
                    item_id = str(params.get("itemId") or "").strip()
                    if item_id:
                        phase = agent_message_phase_by_item_id.get(item_id, "")
                        if phase == "commentary":
                            continue
                        if phase == "final_answer":
                            final_deltas.append(delta)
                            continue
                    deltas.append(delta)
                continue
            if method == "codex/event/agent_message" and isinstance(params, dict):
                event_turn_id = str(params.get("id") or "").strip()
                if event_turn_id == turn_id_str:
                    msg_payload = params.get("msg")
                    if isinstance(msg_payload, dict):
                        phase = str(msg_payload.get("phase") or "").strip()
                        text = msg_payload.get("message")
                        if isinstance(text, str) and text.strip():
                            if phase in ("", "final_answer"):
                                final_answer_text = text.strip()
                continue
            if method == "turn/completed" and str(params.get("turn", {}).get("id")) == turn_id_str:
                self._emit_protocol_status(method=method, params=params)
                status = params.get("turn", {}).get("status")
                text = final_answer_text
                if not text:
                    joined_final = "".join(final_deltas).strip()
                    text = joined_final if joined_final else "".join(deltas).strip()
                if status == "failed":
                    error_payload = params.get("turn", {}).get("error")
                    self._pending_messages = deferred + self._pending_messages
                    raise RuntimeError(f"turn failed: {error_payload}")
                self._pending_messages = deferred + self._pending_messages
                return text
            if method == "error" and str(params.get("turnId")) == turn_id_str:
                terminal_error = params.get("error", {}).get("message") or str(params)
            approval_started_at = clock.monotonic()
            if self._maybe_handle_server_request(
                msg,
                approval_notify_callback=approval_notify_callback,
            ):
                elapsed = max(0.0, clock.monotonic() - approval_started_at)
                if deadline is not None:
                    deadline += elapsed
                continue
            if isinstance(method, str):
                self._emit_protocol_status(method=method, params=params)
        self._pending_messages = deferred + self._pending_messages
        if terminal_error:
            raise RuntimeError(f"turn failed: {terminal_error}")
        raise RuntimeError("timeout waiting for turn completion")

    def _read_message(self, timeout_sec: float) -> Optional[dict]:
        return self._client.read_message(timeout_sec=timeout_sec)

    def _maybe_handle_server_request(
        self,
        msg: dict,
        *,
        approval_notify_callback: Optional[Callable[[str], None]] = None,
    ) -> bool:
        request = parse_server_request(msg)
        if request is None:
            return False
        if request.method in self._USER_INPUT_REQUEST_METHODS:
            self._handle_user_input_request(
                request=request,
                approval_notify_callback=approval_notify_callback,
            )
            return True
        if not ApprovalPolicy.is_approval_method(request.method):
            self._logger.event(
                "server_request_unsupported_method",
                role=self._role,
                id=request.req_id,
                method=request.method,
            )
            self._send_error(
                req_id=request.req_id,
                code=-32601,
                message=f"unsupported server request method: {request.method}",
                data={"method": request.method},
            )
            return True

        transitioned = False
        if (
            self._role == "worker"
            and self._fsm is not None
            and self._fsm.state is State.WORKER_THINKS
        ):
            self._fsm.transition(State.WORKER_WAITS_APPROVAL, reason="approval_requested")
            transitioned = True

        def _before_human_required() -> None:
            if (
                transitioned
                and self._fsm is not None
                and self._fsm.state is State.WORKER_WAITS_APPROVAL
            ):
                self._fsm.transition(
                    State.HUMAN_THINKS,
                    reason="approval_human_required",
                )

        def _decide_human(req: ApprovalRequest) -> str:
            if self._approval_decision_provider is None:
                raise RuntimeError("approval requires human decision but no decision provider configured")
            return self._approval_decision_provider.decide(req)

        plan = build_approval_response_plan(
            request=request,
            logger=self._logger,
            role=self._role,
            approval_policy=self._approval_policy,
            approval_decision_provider=self._approval_decision_provider,
            approval_notify=approval_notify_callback or self._approval_notify_callback,
            always_allow_commands=self._always_allow_commands,
            before_human_required=_before_human_required,
            decide_human=_decide_human,
            log_approval_requested=True,
            log_human_required=True,
            log_human_decision=True,
            log_auto_decision=True,
        )

        self._send_response(req_id=plan.req_id, result=plan.result)
        if self._fsm is not None and self._fsm.state in (State.WORKER_WAITS_APPROVAL, State.HUMAN_THINKS):
            self._fsm.transition(State.WORKER_THINKS, reason="approval_decided")
        return True

    def _handle_user_input_request(
        self,
        *,
        request: ApprovalServerRequest,
        approval_notify_callback: Optional[Callable[[str], None]],
    ) -> None:
        preferred = "decline"
        policy = self._approval_policy
        if policy is not None and policy.default_decision in ("accept", "decline"):
            preferred = policy.default_decision
        if self._approval_decision_provider is not None:
            raw = self._approval_decision_provider.decide(
                ApprovalRequest(
                    req_id=request.req_id,
                    method=request.method,
                    params=request.params,
                    role=self._role,
                )
            )
            decision = raw.strip().lower()
            if decision == "always_allow":
                decision = "accept"
            if decision in ("accept", "decline", "cancel"):
                preferred = decision
            else:
                self._logger.event(
                    "user_input_request_invalid_decision",
                    role=self._role,
                    id=request.req_id,
                    method=request.method,
                    decision=decision,
                )
        available = self._extract_available_decisions(request.params)
        decision = self._choose_server_request_decision(
            preferred=preferred,
            available=available,
        )
        notify = approval_notify_callback or self._approval_notify_callback
        if notify is not None:
            notify(
                "interactive request received.\n"
                f"method={request.method}\n"
                f"decision={decision}"
            )
        self._logger.event(
            "user_input_request_decision",
            role=self._role,
            id=request.req_id,
            method=request.method,
            decision=decision,
            available_decisions=available,
        )
        self._send_response(req_id=request.req_id, result={"decision": decision})

    @staticmethod
    def _extract_available_decisions(params: dict) -> list[str]:
        raw = params.get("availableDecisions")
        if not isinstance(raw, list):
            return []
        out: list[str] = []
        for item in raw:
            if isinstance(item, str):
                out.append(item)
                continue
            if isinstance(item, dict) and item:
                key = next(iter(item.keys()))
                out.append(str(key))
        return out

    @staticmethod
    def _choose_server_request_decision(*, preferred: str, available: list[str]) -> str:
        if not available:
            return preferred
        if preferred in available:
            return preferred
        for candidate in ("decline", "cancel", "accept"):
            if candidate in available:
                return candidate
        return available[0]

    def _send_response(self, req_id: int, result: dict) -> None:
        self._client.send_response(req_id=req_id, result=result)
        self._logger.event(
            "protocol_out_response",
            role=self._role,
            id=req_id,
            result=result,
        )

    def _send_error(
        self,
        *,
        req_id: int,
        code: int,
        message: str,
        data: Optional[dict] = None,
    ) -> None:
        self._client.send_error(
            req_id=req_id,
            code=code,
            message=message,
            data=data,
        )
        payload = {"code": int(code), "message": str(message)}
        if data is not None:
            payload["data"] = data
        self._logger.event(
            "protocol_out_error_response",
            role=self._role,
            id=req_id,
            error=payload,
        )

    def _on_protocol_in(self, _raw_line: str, msg: dict) -> None:
        method = msg.get("method")
        params = msg.get("params")
        extra: dict = {}
        if self._protocol_log_include_params:
            # Full protocol payload for later fixture extraction/debugging.
            extra["message"] = msg
        if method in self._TERMINAL_INTERACTION_METHODS and isinstance(params, dict):
            extra = {
                "terminal_interaction": True,
                "params_preview": {
                    "threadId": params.get("threadId"),
                    "turnId": params.get("turnId"),
                    "itemId": params.get("itemId"),
                    "type": params.get("type"),
                    "source": params.get("source"),
                    "reason": params.get("reason"),
                },
            }
            if self._protocol_log_include_params:
                extra["message"] = msg
        self._logger.event(
            "protocol_in",
            role=self._role,
            method=method,
            id=msg.get("id"),
            **extra,
        )
        if method in ("item/started", "item/completed") and isinstance(params, dict):
            item = params.get("item")
            if isinstance(item, dict) and str(item.get("type") or "").strip() == "agentMessage":
                phase = item.get("phase")
                if phase is None:
                    self._logger.event(
                        "agent_message_missing_phase",
                        role=self._role,
                        method=method,
                        item_id=item.get("id"),
                        turn_id=params.get("turnId"),
                        text_preview=str(item.get("text") or "").strip()[:200],
                    )

    def _emit_protocol_status(self, method: str, params: dict) -> None:
        changed_item_id = self._item_tracker.apply(method=method, params=params)
        if changed_item_id is not None:
            apply_info = self._item_tracker.last_apply_info() or {}
            self._logger.event(
                "protocol_item_changed",
                role=self._role,
                item_id=changed_item_id,
                method=method,
                turn_id=apply_info.get("turn_id"),
                items_count=len(self._item_tracker.snapshot()),
            )
            late_distance = apply_info.get("late_distance")
            if isinstance(late_distance, int) and late_distance >= 2:
                self._logger.event(
                    "protocol_item_late_update",
                    role=self._role,
                    item_id=changed_item_id,
                    method=method,
                    turn_id=apply_info.get("turn_id"),
                    current_turn_distance=late_distance,
                )
        item_callback = self._protocol_item_callback
        if item_callback is not None and changed_item_id is not None:
            item_callback(self._role, changed_item_id)
            return
        callback = self._protocol_status_callback
        if callback is None:
            return
        if is_hidden_item_status_event(method=method, params=params):
            return
        if method in ("item/agentMessage/delta", "turn/completed"):
            return
        if not should_emit_status_method(method):
            return
        callback(self._role, method, params)
