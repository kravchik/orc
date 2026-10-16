"""Agent adapter abstractions for ORC1."""

from __future__ import annotations

from dataclasses import dataclass, field
import os
import time
from typing import Any, Callable, Mapping, Optional

from orchestrator import clock
from orchestrator.agent_capabilities import (
    AgentCapability,
    AgentCapabilitySnapshot,
    codex_capability_snapshot,
)
from orchestrator.agent_io import AgentInput, AgentOutput
from orchestrator.agent_session import RawProviderErrorPayload
from orchestrator.approval import ApprovalPolicy
from orchestrator.codex_approval_runtime import (
    ApprovalServerRequest,
    build_always_allow_cached_response_plan,
    log_session_approval_applied,
    parse_server_request,
)
from orchestrator.codex_approval import (
    COMMAND_APPROVAL_METHOD,
    NativeApprovalRequest,
    approval_response_decision_name,
    build_session_approval_response,
    is_codex_approval_method,
    supports_session_approval,
)
from orchestrator.context_window import compaction_matches_thread, parse_context_usage
from orchestrator.file_change_approval import enrich_file_change_approval_params_from_item
from orchestrator.jsonrpc_stdio import JsonRpcReadResult, StdioJsonRpcClient
from orchestrator.processes import LifecycleLogger
from orchestrator.protocol_status import (
    ProtocolItemTracker,
    is_hidden_item_status_event,
    should_emit_status_method,
)
from orchestrator.thread_resume import (
    codex_initialize_params,
    exclude_turns_unsupported_error,
    is_exclude_turns_unsupported,
    is_resume_parameter_compatibility_error,
    request_thread_start_or_resume,
    resume_param_candidates,
)


class JsonRpcRemoteError(RuntimeError):
    """A correlated JSON-RPC error proves that the remote rejected the request."""

    def __init__(self, req_id: int, error: object) -> None:
        self.req_id = int(req_id)
        self.error = dict(error) if isinstance(error, dict) else {"message": str(error)}
        raw_code = self.error.get("code")
        self.code = int(raw_code) if isinstance(raw_code, int) and not isinstance(raw_code, bool) else None
        super().__init__(f"protocol error for request {self.req_id}: {error}")


class JsonRpcDeliveryUncertain(RuntimeError):
    """The client cannot prove whether a sent request reached the provider."""


class JsonRpcTransportTerminal(RuntimeError):
    """The JSON-RPC transport reached a terminal read boundary."""


def protocol_raw_capture_enabled() -> bool:
    return os.getenv("ORC_PROTOCOL_LOG_RAW", "0").strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class InteractiveApprovalPending:
    server_request: ApprovalServerRequest
    approval_request: NativeApprovalRequest


@dataclass
class InteractiveTurnState:
    start_req_id: int
    turn_id: str | None
    agent_input: AgentInput
    approval_notify_callback: Optional[Callable[[str], None]]
    deltas: list[str]
    final_deltas: list[str]
    final_answer_text: Optional[str]
    agent_message_phase_by_item_id: dict[str, str]
    ordered_output_deltas: list[str] = field(default_factory=list)
    raw_item_by_item_id: dict[str, dict[str, Any]] = field(default_factory=dict)
    deferred_pre_start_messages: list[dict] = field(default_factory=list)
    terminal_error: Optional[str] = None
    pending_approval: InteractiveApprovalPending | None = None
    pending_approval_emitted: bool = False


@dataclass(frozen=True)
class InteractiveTurnProgress:
    kind: str
    output: AgentOutput | None = None
    approval_request: NativeApprovalRequest | None = None
    error: str | None = None
    raw_provider_error: RawProviderErrorPayload | None = None

    def require_completed_output(self) -> AgentOutput:
        if self.kind != "completed":
            raise ValueError(f"turn progress is not completed: {self.kind}")
        if self.output is None:
            raise RuntimeError("completed turn progress is missing AgentOutput")
        return self.output


@dataclass
class CodexInspectionState:
    thread_id: str
    metadata_req_id: int
    loaded_req_id: int | None = None
    metadata: dict[str, Any] | None = None


@dataclass(frozen=True)
class CodexInspectionProgress:
    completed: bool
    metadata: dict[str, Any] | None = None
    loaded: bool | None = None
    error: str = ""


@dataclass
class InteractiveSessionStartState:
    initialize_req_id: int
    start_requests: list[tuple[str, dict]]
    requested_resume_thread_id: str | None = None
    active_start_req_id: int | None = None
    active_start_method: str | None = None
    started: bool = False
    deferred_messages: list[dict] = field(default_factory=list)


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
        startup_timeout_sec: float,
        rpc_timeout_sec: float,
        rpc_retries: int,
        thread_approval_policy: str,
        thread_sandbox: str,
        protocol_status_callback: Optional[Callable[[str, str, dict], None]],
        protocol_log_include_params: bool,
        client_factory: Optional[Callable[..., Any]] = None,
    ) -> None:
        self._role = role
        self._command = command
        self._logger = logger
        self._cwd = cwd
        self._env = env
        self._startup_timeout_sec = float(startup_timeout_sec)
        self._rpc_timeout_sec = max(0.1, float(rpc_timeout_sec))
        self._rpc_retries = max(0, int(rpc_retries))
        self._thread_approval_policy = thread_approval_policy
        self._thread_sandbox = thread_sandbox
        self._protocol_status_callback = protocol_status_callback
        self._protocol_log_include_params = protocol_log_include_params
        self._protocol_item_callback: Optional[Callable[[str, str], None]] = None
        client_ctor = client_factory or StdioJsonRpcClient
        self._client = client_ctor(
            command=self._command,
            cwd=self._cwd,
            env=self._env,
            on_message=None,
            on_stderr=self._on_protocol_stderr,
        )
        self._thread_id: Optional[str] = None
        self._thread_model: Optional[str] = None
        self._thread_metadata: dict[str, Any] = {}
        self._context_usage: dict[str, object] = {}
        self._runtime_name = "codex-app-server"
        self._runtime_version = "unknown"
        self._pending_messages: list[dict] = []
        self._always_allow_commands: set[str] = set()
        self._item_tracker = ProtocolItemTracker()
        self._transport_progressed = False
        self._request_started: dict[int, tuple[str, float]] = {}
        self._ignored_response_ids: set[int] = set()
        self._inspection_response_ids: set[int] = set()
        self._startup_started_at: float | None = None
        self._startup_last_waiting_at: float | None = None

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

    def begin_thread_inspection(self) -> CodexInspectionState:
        thread_id = str(self._thread_id or "").strip()
        if not thread_id:
            raise RuntimeError("agent session has no provider session id")
        state = CodexInspectionState(
            thread_id=thread_id,
            metadata_req_id=self._send_request(
                method="thread/read",
                params={"threadId": thread_id},
            ),
        )
        self._inspection_response_ids.add(state.metadata_req_id)
        return state

    def poll_thread_inspection(
        self,
        state: CodexInspectionState,
    ) -> CodexInspectionProgress | None:
        if state.metadata is None:
            response = self._poll_correlated_response(state.metadata_req_id)
            if response is None:
                return None
            if "error" in response:
                return CodexInspectionProgress(
                    completed=True,
                    error=_jsonrpc_error_message(response.get("error")),
                )
            result = response.get("result") if isinstance(response.get("result"), dict) else {}
            raw_thread = result.get("thread")
            if isinstance(raw_thread, dict):
                state.metadata = dict(raw_thread)
            elif any(key in result for key in ("id", "status", "preview", "ephemeral")):
                state.metadata = dict(result)
            else:
                state.metadata = dict(self._thread_metadata)
            state.loaded_req_id = self._send_request(method="thread/loaded/list", params={})
            self._inspection_response_ids.add(state.loaded_req_id)
            return None

        if state.loaded_req_id is None:
            raise AssertionError("Codex inspection lost loaded-state request")
        response = self._poll_correlated_response(state.loaded_req_id)
        if response is None:
            return None
        loaded: bool | None = None
        if "error" not in response:
            result = response.get("result") if isinstance(response.get("result"), dict) else {}
            raw_items = result.get("data")
            if not isinstance(raw_items, list):
                raw_items = result.get("threads")
            if not isinstance(raw_items, list):
                raw_items = result.get("threadIds")
            if isinstance(raw_items, list):
                loaded = any(
                    (isinstance(item, str) and item.strip() == state.thread_id)
                    or (
                        isinstance(item, dict)
                        and str(item.get("id") or "").strip() == state.thread_id
                    )
                    for item in raw_items
                )
        metadata = dict(state.metadata)
        metadata.setdefault("id", state.thread_id)
        metadata.setdefault("model", str(self._thread_model or "").strip())
        if loaded is not None:
            metadata["loaded"] = loaded
        self._thread_metadata.update(metadata)
        return CodexInspectionProgress(
            completed=True,
            metadata=metadata,
            loaded=loaded,
        )

    def cancel_thread_inspection(self, state: CodexInspectionState) -> None:
        request_ids = {state.metadata_req_id}
        if state.loaded_req_id is not None:
            request_ids.add(state.loaded_req_id)
        outstanding_ids = request_ids.intersection(self._inspection_response_ids)
        self._ignored_response_ids.update(outstanding_ids)
        self._inspection_response_ids.difference_update(request_ids)
        self._pending_messages = [
            message
            for message in self._pending_messages
            if message.get("id") not in request_ids
        ]

    def _poll_correlated_response(self, req_id: int) -> dict[str, Any] | None:
        for index, message in enumerate(self._pending_messages):
            if message.get("id") == req_id and ("result" in message or "error" in message):
                self._inspection_response_ids.discard(req_id)
                return self._pending_messages.pop(index)
        message = self._read_message(timeout_sec=0.0)
        if message is None:
            return None
        response_id = message.get("id")
        if response_id == req_id and ("result" in message or "error" in message):
            self._inspection_response_ids.discard(req_id)
            return message
        self._pending_messages.append(message)
        return None

    def get_cached_thread_metadata(self) -> dict[str, Any]:
        return dict(self._thread_metadata)

    def get_cached_context_usage(self) -> dict[str, object]:
        return dict(self._context_usage)

    def get_capability_snapshot(self) -> AgentCapabilitySnapshot:
        return codex_capability_snapshot(
            runtime_name=self._runtime_name,
            runtime_version=self._runtime_version,
            backend_name=str(self._thread_metadata.get("modelProvider") or "unknown"),
        )

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
        deadline = clock.monotonic() + self._startup_timeout_sec
        while True:
            if self.poll_interactive_start(state, timeout_sec=0.05):
                return
            if clock.monotonic() >= deadline:
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

    def begin_interactive_turn(
        self,
        agent_input: AgentInput,
        *,
        approval_notify_callback: Optional[Callable[[str], None]] = None,
    ) -> InteractiveTurnState:
        if self._thread_id is None:
            raise RuntimeError("thread not initialized")
        req_id = self._send_request(
            method="turn/start",
            params={
                "threadId": self._thread_id,
                "input": [{"type": "text", "text": agent_input.text, "text_elements": []}],
            },
        )
        self._logger.event(
            "interactive_turn_start_requested",
            role=self._role,
            req_id=req_id,
            thread_id=self._thread_id,
            prompt_preview=agent_input.text.strip()[:200],
        )
        return InteractiveTurnState(
            start_req_id=req_id,
            turn_id=None,
            agent_input=agent_input,
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
        self._thread_metadata = {}
        self._context_usage = {}
        self._runtime_name = "codex-app-server"
        self._runtime_version = "unknown"
        self._client.start()
        self._startup_started_at = clock.monotonic()
        self._startup_last_waiting_at = self._startup_started_at
        self._logger.event(
            "process_started",
            role=self._role,
            pid=self._client.pid,
            command=self._command,
        )
        self._log_startup_timeline("process_spawn", pid=self._client.pid)
        req_id = self._send_request(
            method="initialize",
            params=codex_initialize_params(client_name=f"orc1-{self._role}"),
        )
        self._log_startup_timeline("initialize_sent", request_id=req_id)
        params = start_params or {
            "approvalPolicy": self._thread_approval_policy,
            "sandbox": self._thread_sandbox,
        }
        if resume_thread_id:
            start_requests = [
                ("thread/resume", resume_params)
                for resume_params in resume_param_candidates(resume_thread_id)
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
            msg = self._take_turn_message(
                timeout_sec=max(0.0, min(0.05, wait_timeout)) if timeout_sec > 0 else 0.0,
            )
            if msg is None:
                self._log_startup_waiting()
                state.deferred_messages = deferred
                return False
            if state.active_start_req_id is None:
                if msg.get("id") == state.initialize_req_id and "result" in msg:
                    self._capture_initialize_result(msg.get("result"))
                    self._log_startup_timeline(
                        "initialize_completed",
                        request_id=state.initialize_req_id,
                    )
                    self._send_notification(method="initialized", params={})
                    method, params = state.start_requests[0]
                    state.active_start_method = method
                    state.active_start_req_id = self._send_request(method=method, params=params)
                    self._log_startup_timeline(
                        "thread_resume_sent" if method == "thread/resume" else "thread_start_sent",
                        request_id=state.active_start_req_id,
                        requested_thread_id=state.requested_resume_thread_id,
                    )
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
                    self._context_usage = {}
                    thread_payload = result.get("thread")
                    self._thread_metadata = dict(thread_payload) if isinstance(thread_payload, dict) else {}
                    self._thread_metadata.setdefault("id", thread_id)
                    self._thread_metadata.setdefault("model", model)
                    state.started = True
                    self._log_startup_timeline(
                        "ready",
                        thread_id=thread_id,
                        model=model,
                    )
                    self._startup_last_waiting_at = None
                    if state.active_start_method == "thread/resume":
                        self._logger.event(
                            f"{self._role}_thread_resumed",
                            thread_id=thread_id,
                            requested_thread_id=state.requested_resume_thread_id,
                            model=model,
                        )
                    else:
                        self._logger.event(f"{self._role}_thread_started", thread_id=thread_id, model=model)
                    self._logger.event(
                        "agent_capability_snapshot",
                        role=self._role,
                        **self.get_capability_snapshot().to_log_fields(),
                    )
                    self._pending_messages = deferred + self._pending_messages
                    state.deferred_messages = []
                    return True
                if msg.get("id") == state.active_start_req_id and "error" in msg:
                    if (
                        state.active_start_method == "thread/resume"
                        and is_exclude_turns_unsupported(msg.get("error"))
                    ):
                        state.deferred_messages = deferred
                        raise exclude_turns_unsupported_error(msg.get("error"))
                    if (
                        state.active_start_method == "thread/resume"
                        and len(state.start_requests) > 1
                        and is_resume_parameter_compatibility_error(msg.get("error"))
                    ):
                        state.start_requests.pop(0)
                        method, params = state.start_requests[0]
                        state.active_start_method = method
                        state.active_start_req_id = self._send_request(method=method, params=params)
                        self._log_startup_timeline(
                            "thread_resume_retry_sent",
                            request_id=state.active_start_req_id,
                            requested_thread_id=state.requested_resume_thread_id,
                        )
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
                return self._poll_pending_approval_occurrence(state)
            state.pending_approval_emitted = True
            return InteractiveTurnProgress(
                kind="approval_required",
                approval_request=state.pending_approval.approval_request,
            )
        deadline = clock.monotonic() + max(0.0, float(timeout_sec))
        consumed_message = False
        while True:
            remaining = max(0.0, deadline - clock.monotonic())
            wait_timeout = 0.0 if timeout_sec <= 0 else max(0.0, remaining)
            msg = self._take_turn_message(
                timeout_sec=max(0.0, min(0.05, wait_timeout)) if timeout_sec > 0 else 0.0,
            )
            if msg is None:
                if timeout_sec > 0:
                    self._logger.event(
                        "interactive_turn_poll_no_message",
                        role=self._role,
                        turn_id=state.turn_id,
                    )
                return InteractiveTurnProgress(
                    kind="progressed" if consumed_message else "idle"
                )
            consumed_message = True
            progress = self._consume_interactive_turn_message(state=state, msg=msg)
            if progress is not None:
                return progress
            if timeout_sec <= 0:
                return InteractiveTurnProgress(kind="progressed")
            if remaining <= 0.0:
                return InteractiveTurnProgress(kind="progressed")

    def _poll_pending_approval_occurrence(
        self,
        state: InteractiveTurnState,
    ) -> InteractiveTurnProgress:
        occurrences = self.drain_pending_interactive_approval_occurrences(
            state,
            limit=1,
        )
        if not occurrences:
            return InteractiveTurnProgress(kind="idle")
        return InteractiveTurnProgress(
            kind="approval_required",
            approval_request=occurrences[0],
        )

    def drain_pending_interactive_approval_occurrences(
        self,
        state: InteractiveTurnState,
        *,
        limit: int = 64,
        fail_closed_on_overflow: bool = False,
    ) -> tuple[NativeApprovalRequest, ...]:
        if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
            raise ValueError("approval occurrence drain limit must be a positive integer")
        occurrences: list[NativeApprovalRequest] = []
        while len(occurrences) < limit:
            msg = self._take_turn_message(timeout_sec=0.0)
            if msg is None:
                break
            request = parse_server_request(msg)
            if request is None or not (
                is_codex_approval_method(request.method)
                or request.method in self._USER_INPUT_REQUEST_METHODS
            ):
                self._pending_messages.insert(0, msg)
                break
            request = self._enrich_interactive_approval_request_from_items(
                state=state,
                request=request,
            )
            occurrences.append(
                NativeApprovalRequest(
                    req_id=request.req_id,
                    method=request.method,
                    params=request.params,
                    role=self._role or "agent",
                )
            )
        if fail_closed_on_overflow and len(occurrences) == limit:
            overflow = self._take_turn_message(timeout_sec=0.0)
            if overflow is not None:
                overflow_request = parse_server_request(overflow)
                if overflow_request is None or not (
                    is_codex_approval_method(overflow_request.method)
                    or overflow_request.method in self._USER_INPUT_REQUEST_METHODS
                ):
                    self._pending_messages.insert(0, overflow)
                else:
                    self.reject_concurrent_interactive_approval_request(
                        request_token=overflow_request.req_id,
                        reason="approval occurrence buffer capacity exceeded",
                    )
                    self._logger.event(
                        "codex_approval_occurrence_capacity_exceeded",
                        role=self._role,
                        limit=limit,
                        id=overflow_request.req_id,
                    )
                    raise RuntimeError("approval occurrence buffer capacity exceeded")
        return tuple(occurrences)

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
                if timeout_sec > 0:
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
                self._logger.event(f"{self._role}_input", prompt=state.agent_input.text, turn_id=turn_id_str)
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
            if self._maybe_handle_server_request(msg):
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
        expected_request_token: int,
        decision: str,
        via: str = "human",
        decision_source: str = "interactive",
    ) -> None:
        pending = state.pending_approval
        if pending is None:
            raise RuntimeError("interactive turn is not waiting for approval")
        if pending.server_request.req_id != expected_request_token:
            raise RuntimeError("Codex approval request token no longer matches")
        if pending.server_request.method in self._USER_INPUT_REQUEST_METHODS:
            available = self._extract_available_decisions(pending.server_request.params)
            normalized = str(decision or "").strip().lower()
            resolved_decision = self._choose_server_request_decision(
                preferred=normalized,
                available=available,
            )
            self._logger.event(
                "user_input_request_decision",
                role=self._role,
                id=pending.server_request.req_id,
                method=pending.server_request.method,
                decision=resolved_decision,
                available_decisions=available,
            )
            self._send_response(
                req_id=pending.server_request.req_id,
                result={"decision": resolved_decision},
            )
            state.pending_approval = None
            state.pending_approval_emitted = False
            return
        normalized = ApprovalPolicy.normalize_human_decision_with_always(
            raw=decision,
            allow_always=(
                self.get_capability_snapshot().supports(AgentCapability.APPROVAL_ALWAYS)
                and supports_session_approval(
                    method=pending.server_request.method,
                    params=pending.server_request.params,
                )
            ),
        )
        always_allow = normalized == "always_allow"
        session_result = (
            build_session_approval_response(
                method=pending.server_request.method,
                params=pending.server_request.params,
            )
            if always_allow
            else None
        )
        if always_allow and session_result is None:
            raise RuntimeError("session approval is unavailable for this request")
        resolved_decision = (
            approval_response_decision_name(session_result)
            if session_result is not None
            else normalized
        )
        if via == "human":
            self._logger.event(
                "approval_human_decision",
                role=self._role,
                id=pending.server_request.req_id,
                method=pending.server_request.method,
                decision=resolved_decision,
                always_allow=always_allow,
            )
        elif via == "policy_auto":
            self._logger.event(
                "approval_auto_decision",
                role=self._role,
                id=pending.server_request.req_id,
                method=pending.server_request.method,
                decision=resolved_decision,
                source=decision_source,
                command=str(pending.server_request.params.get("command") or ""),
            )
        else:
            raise ValueError(f"unsupported approval decision source: {via}")
        result: dict[str, Any] = session_result or {"decision": resolved_decision}
        if always_allow:
            command = pending.server_request.params.get("command")
            if (
                pending.server_request.method == COMMAND_APPROVAL_METHOD
                and isinstance(command, str)
                and command
            ):
                self._always_allow_commands.add(command)
            log_session_approval_applied(
                logger=self._logger,
                role=self._role,
                req_id=pending.server_request.req_id,
                method=pending.server_request.method,
                result=result,
            )
        self._send_response(req_id=pending.server_request.req_id, result=result)
        state.pending_approval = None
        state.pending_approval_emitted = False

    def reject_interactive_approval_request(
        self,
        state: InteractiveTurnState,
        *,
        expected_request_token: int,
        reason: str,
    ) -> None:
        pending = state.pending_approval
        if pending is None:
            raise RuntimeError("interactive turn is not waiting for approval")
        if pending.server_request.req_id != expected_request_token:
            raise RuntimeError("Codex approval request token no longer matches")
        message = str(reason or "").strip() or "approval request is no longer active"
        self._send_error(
            req_id=expected_request_token,
            code=-32600,
            message=message,
            data={"reason": "approval_correlation_rejected"},
        )
        state.pending_approval = None
        state.pending_approval_emitted = False

    def reject_concurrent_interactive_approval_request(
        self,
        *,
        request_token: int,
        reason: str,
    ) -> None:
        message = str(reason or "").strip() or "approval request capacity exceeded"
        self._send_error(
            req_id=request_token,
            code=-32600,
            message=message,
            data={"reason": "approval_occurrence_capacity"},
        )

    def submit_interactive_turn_interrupt(self, state: InteractiveTurnState) -> None:
        if self._thread_id is None:
            raise RuntimeError("thread not initialized")
        if not isinstance(state.turn_id, str) or not state.turn_id.strip():
            raise RuntimeError("interactive turn has no active turn id")
        req_id = self._send_request(
            method="turn/interrupt",
            params={
                "threadId": self._thread_id,
                "turnId": state.turn_id.strip(),
            },
        )
        self._wait_for_response(req_id=req_id)

    def submit_interactive_turn_steer(self, state: InteractiveTurnState, agent_input: AgentInput) -> None:
        if self._thread_id is None:
            raise RuntimeError("thread not initialized")
        if not isinstance(state.turn_id, str) or not state.turn_id.strip():
            raise RuntimeError("interactive turn has no active turn id")
        steer_text = agent_input.text
        try:
            req_id = self._send_request(
                method="turn/steer",
                params={
                    "threadId": self._thread_id,
                    "expectedTurnId": state.turn_id.strip(),
                    "input": [{"type": "text", "text": steer_text, "text_elements": []}],
                },
            )
        except Exception as exc:
            raise JsonRpcDeliveryUncertain(str(exc) or type(exc).__name__) from exc
        self._logger.event(
            "interactive_turn_steer_requested",
            role=self._role,
            req_id=req_id,
            thread_id=self._thread_id,
            turn_id=state.turn_id.strip(),
            prompt_preview=steer_text.strip()[:200],
        )
        try:
            self._wait_for_response(req_id=req_id)
        except JsonRpcRemoteError:
            raise
        except Exception as exc:
            raise JsonRpcDeliveryUncertain(str(exc) or type(exc).__name__) from exc

    def poll_background_protocol_message(self, *, timeout_sec: float = 0.0) -> bool:
        if self._pending_messages:
            msg = self._pending_messages.pop(0)
        else:
            msg = self._read_message(
                timeout_sec=max(0.0, min(0.05, float(timeout_sec))) if timeout_sec > 0 else 0.0
            )
        if msg is None:
            return False
        if isinstance(msg.get("id"), int) and ("result" in msg or "error" in msg):
            self._pending_messages.insert(0, msg)
            return False
        approval_started_at = clock.monotonic()
        if self._maybe_handle_server_request(msg):
            _ = approval_started_at
            return True
        method = msg.get("method")
        params = msg.get("params", {})
        if isinstance(method, str) and isinstance(params, dict):
            self._emit_protocol_status(
                method=method,
                params=params,
                allow_terminal_status=True,
            )
            self._logger.event(
                "protocol_background_consumed",
                role=self._role,
                method=method,
            )
            return True
        return False

    def _initialize(self) -> None:
        req_id = self._send_request(
            method="initialize",
            params=codex_initialize_params(client_name=f"orc1-{self._role}"),
        )
        response = self._wait_for_response(req_id=req_id)
        self._capture_initialize_result(response.get("result"))
        self._send_notification(method="initialized", params={})

    def _capture_initialize_result(self, value: object) -> None:
        if not isinstance(value, dict):
            return
        raw_info = value.get("serverInfo")
        if not isinstance(raw_info, dict):
            raw_info = value.get("runtimeInfo")
        if not isinstance(raw_info, dict):
            raw_info = {}
        name = str(raw_info.get("name") or "").strip()
        version = str(
            raw_info.get("version")
            or value.get("serverVersion")
            or value.get("version")
            or ""
        ).strip()
        if name:
            self._runtime_name = name
        if version:
            self._runtime_version = version

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
            self._context_usage = {}
            thread_payload = result.get("thread")
            self._thread_metadata = dict(thread_payload) if isinstance(thread_payload, dict) else {}
            self._thread_metadata.setdefault("id", self._thread_id)
            self._thread_metadata.setdefault("model", self._thread_model)
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
        self._context_usage = {}
        self._thread_metadata = {
            "id": self._thread_id,
            "model": self._thread_model,
        }
        if start_result.backend:
            self._thread_metadata["modelProvider"] = start_result.backend

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
            if item_id:
                state.raw_item_by_item_id[item_id] = dict(item_obj)
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
                        state.ordered_output_deltas.append(delta)
                        state.final_deltas.append(delta)
                        return None
                state.ordered_output_deltas.append(delta)
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
                return InteractiveTurnProgress(
                    kind="failed",
                    error="Codex turn failed",
                    raw_provider_error=RawProviderErrorPayload.from_json_object(
                        error_payload
                    ),
                )
            if status == "interrupted":
                self._logger.event(f"{self._role}_interrupted", turn_id=state.turn_id)
                return InteractiveTurnProgress(kind="interrupted")
            self._logger.event(f"{self._role}_output", response=text, turn_id=state.turn_id)
            return InteractiveTurnProgress(kind="completed", output=AgentOutput(text=text))
        if method == "error" and str(params.get("turnId")) == state.turn_id:
            state.terminal_error = params.get("error", {}).get("message") or str(params)

        request = parse_server_request(msg)
        if request is not None:
            request = self._enrich_interactive_approval_request_from_items(
                state=state,
                request=request,
            )
            if request.method in self._USER_INPUT_REQUEST_METHODS:
                return self._prepare_interactive_user_input(
                    state=state,
                    request=request,
                )
            if not is_codex_approval_method(request.method):
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
            self._emit_protocol_status(
                method=method,
                params=params,
                allow_terminal_status=True,
            )
        return None

    def _take_turn_message(self, *, timeout_sec: float) -> dict[str, Any] | None:
        if self._pending_messages:
            response_id = self._pending_messages[0].get("id")
            if isinstance(response_id, int) and response_id in self._inspection_response_ids:
                return None
            return self._pending_messages.pop(0)
        message = self._read_message(timeout_sec=timeout_sec)
        if message is None:
            return None
        response_id = message.get("id")
        if isinstance(response_id, int) and response_id in self._inspection_response_ids:
            self._pending_messages.append(message)
            return None
        return message

    def _prepare_interactive_user_input(
        self,
        *,
        state: InteractiveTurnState,
        request: ApprovalServerRequest,
    ) -> InteractiveTurnProgress:
        available = self._extract_available_decisions(request.params)
        self._logger.event(
            "user_input_request_pending",
            role=self._role,
            id=request.req_id,
            method=request.method,
            available_decisions=available,
        )
        approval_request = NativeApprovalRequest(
            req_id=request.req_id,
            method=request.method,
            params=request.params,
            role=self._role or "agent",
        )
        state.pending_approval = InteractiveApprovalPending(
            server_request=request,
            approval_request=approval_request,
        )
        state.pending_approval_emitted = True
        return InteractiveTurnProgress(
            kind="approval_required",
            approval_request=approval_request,
        )

    def _prepare_interactive_approval(
        self,
        *,
        state: InteractiveTurnState,
        request: ApprovalServerRequest,
    ) -> InteractiveTurnProgress | None:
        cached_plan = (
            build_always_allow_cached_response_plan(
                request=request,
                logger=self._logger,
                role=self._role,
                always_allow_commands=self._always_allow_commands,
            )
            if self.get_capability_snapshot().supports(AgentCapability.APPROVAL_ALWAYS)
            else None
        )
        if cached_plan is not None:
            self._send_response(req_id=cached_plan.req_id, result=cached_plan.result)
            return None
        approval_request = NativeApprovalRequest(
            req_id=request.req_id,
            method=request.method,
            params=request.params,
            role=self._role or "agent",
        )
        state.pending_approval = InteractiveApprovalPending(
            server_request=request,
            approval_request=approval_request,
        )
        state.pending_approval_emitted = True
        return InteractiveTurnProgress(kind="approval_required", approval_request=approval_request)

    def _enrich_interactive_approval_request_from_items(
        self,
        *,
        state: InteractiveTurnState,
        request: ApprovalServerRequest,
    ) -> ApprovalServerRequest:
        if request.method != "item/fileChange/requestApproval":
            return request
        raw_item_id = request.params.get("itemId")
        if not isinstance(raw_item_id, str) or not raw_item_id.strip():
            return request
        item_id = raw_item_id.strip()
        item = state.raw_item_by_item_id.get(item_id)
        enriched_params = enrich_file_change_approval_params_from_item(
            params=request.params,
            item=item,
        )
        if enriched_params == request.params:
            return request
        self._logger.event(
            "approval_request_enriched_from_item",
            role=self._role,
            id=request.req_id,
            method=request.method,
            item_id=item_id,
            changes_count=len(enriched_params.get("changes") or []),
        )
        return ApprovalServerRequest(
            req_id=request.req_id,
            method=request.method,
            params=enriched_params,
        )

    def _send_request(self, method: str, params: dict) -> int:
        req_id = self._client.send_request(method=method, params=params)
        self._request_started[req_id] = (method, clock.monotonic())
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
                    raise JsonRpcRemoteError(req_id, msg["error"])
                approval_started_at = clock.monotonic()
                if self._maybe_handle_server_request(msg):
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

    def _read_message(self, timeout_sec: float) -> Optional[dict]:
        poll_message = getattr(self._client, "poll_message", None)
        if callable(poll_message):
            result = poll_message(timeout_sec=timeout_sec)
            if not isinstance(result, JsonRpcReadResult):
                raise RuntimeError(f"invalid json-rpc transport result: {result!r}")
            if result.progressed:
                self._transport_progressed = True
            if result.kind == "progressed":
                if result.buffered_bytes:
                    self._logger.event(
                        "jsonrpc_transport_progress",
                        role=self._role,
                        bytes_read=result.bytes_read,
                        buffered_bytes=result.buffered_bytes,
                        startup_elapsed_ms=self._startup_elapsed_ms(),
                    )
                return None
            if result.terminal:
                raise JsonRpcTransportTerminal(result.error)
            msg = result.message if result.kind == "message" else None
            if msg is not None:
                if self._discard_ignored_response(msg):
                    return None
                self._on_protocol_in(
                    "",
                    msg,
                    wire_bytes=result.wire_bytes,
                    payload_bytes=result.payload_bytes,
                )
            return msg

        msg = self._client.read_message(timeout_sec=timeout_sec)
        if isinstance(msg, dict):
            self._transport_progressed = True
            if self._discard_ignored_response(msg):
                return None
            self._on_protocol_in("", msg)
            return msg
        return None

    def _discard_ignored_response(self, message: dict[str, Any]) -> bool:
        response_id = message.get("id")
        if not isinstance(response_id, int) or response_id not in self._ignored_response_ids:
            return False
        self._ignored_response_ids.discard(response_id)
        self._request_started.pop(response_id, None)
        self._logger.event(
            "protocol_ignored_response_discarded",
            role=self._role,
            response_id=response_id,
        )
        return True

    def consume_transport_progress(self) -> bool:
        progressed = self._transport_progressed
        self._transport_progressed = False
        return progressed

    def _maybe_handle_server_request(self, msg: dict) -> bool:
        request = parse_server_request(msg)
        if request is None:
            return False
        if request.method in self._USER_INPUT_REQUEST_METHODS:
            available = self._extract_available_decisions(request.params)
            decision = (
                "cancel"
                if "decline" not in available and "cancel" in available
                else "decline"
            )
            self._logger.event(
                "server_request_rejected_outside_agent_session_port",
                role=self._role,
                id=request.req_id,
                method=request.method,
                decision=decision,
                available_decisions=available,
            )
            self._send_response(req_id=request.req_id, result={"decision": decision})
            return True
        if not is_codex_approval_method(request.method):
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
        self._logger.event(
            "server_request_rejected_outside_agent_session_port",
            role=self._role,
            id=request.req_id,
            method=request.method,
            decision="decline",
        )
        self._send_response(req_id=request.req_id, result={"decision": "decline"})
        return True

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
        try:
            self._client.send_response(req_id=req_id, result=result)
        except JsonRpcDeliveryUncertain:
            raise
        except Exception as exc:
            error = str(exc).strip() or type(exc).__name__
            raise JsonRpcDeliveryUncertain(error) from exc
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

    def _on_protocol_in(
        self,
        _raw_line: str,
        msg: dict,
        *,
        wire_bytes: int = 0,
        payload_bytes: int = 0,
    ) -> None:
        request_id = msg.get("id")
        raw_method = msg.get("method")
        is_response = not isinstance(raw_method, str) and ("result" in msg or "error" in msg)
        request_info = (
            self._request_started.pop(request_id, None)
            if is_response and isinstance(request_id, int)
            else None
        )
        method = raw_method
        if not isinstance(method, str) and request_info is not None:
            method = request_info[0]
        params = msg.get("params")
        extra: dict[str, Any] = {}
        if wire_bytes:
            extra["wire_bytes"] = wire_bytes
        if payload_bytes:
            extra["payload_bytes"] = payload_bytes
        if request_info is not None:
            extra["duration_ms"] = round(
                max(0.0, clock.monotonic() - request_info[1]) * 1000.0,
                3,
            )
            extra.update(_summarize_protocol_response(msg))
        if self._protocol_log_include_params:
            # Explicit fixture/debug mode only. Production callers keep this off.
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
            id=request_id,
            **extra,
        )
        if method in {
            "mcpServer/startupStatus/updated",
            "codex/event/mcp_startup_update",
        } and isinstance(params, dict):
            self._log_mcp_startup_status(params)
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

    def _on_protocol_stderr(self, line: str) -> None:
        self._logger.event(
            "app_server_stderr",
            role=self._role,
            line=str(line),
        )

    def _startup_elapsed_ms(self) -> float | None:
        if self._startup_started_at is None:
            return None
        return round(max(0.0, clock.monotonic() - self._startup_started_at) * 1000.0, 3)

    def _log_startup_timeline(self, phase: str, **fields: object) -> None:
        self._logger.event(
            "jsonrpc_startup_timeline",
            role=self._role,
            phase=phase,
            elapsed_ms=self._startup_elapsed_ms(),
            **fields,
        )

    def _log_startup_waiting(self) -> None:
        if self._startup_started_at is None:
            return
        now = clock.monotonic()
        last = self._startup_last_waiting_at
        if last is not None and now - last < 5.0:
            return
        self._startup_last_waiting_at = now
        self._logger.event(
            "startup_waiting",
            role=self._role,
            elapsed_ms=self._startup_elapsed_ms(),
        )

    def _log_mcp_startup_status(self, params: dict[str, Any]) -> None:
        legacy = params.get("msg") if isinstance(params.get("msg"), dict) else {}
        status_payload = params.get("status", legacy.get("status"))
        if isinstance(status_payload, dict):
            status = str(status_payload.get("state") or status_payload.get("status") or "").strip()
            error = str(
                status_payload.get("error")
                or status_payload.get("failureReason")
                or ""
            ).strip()
        else:
            status = str(status_payload or params.get("state") or legacy.get("state") or "").strip()
            error = ""
        error = str(
            error
            or params.get("error")
            or params.get("failureReason")
            or legacy.get("error")
            or legacy.get("failureReason")
            or ""
        ).strip()
        emitted_at_raw = params.get("emittedAtMs", legacy.get("emittedAtMs"))
        emitted_at_ms = float(emitted_at_raw) if isinstance(emitted_at_raw, (int, float)) else None
        lag_ms = (
            round(max(0.0, time.time() * 1000.0 - emitted_at_ms), 3)
            if emitted_at_ms is not None
            else None
        )
        self._logger.event(
            "mcp_startup_status",
            role=self._role,
            mcp_name=str(
                params.get("name")
                or params.get("server")
                or legacy.get("name")
                or legacy.get("server")
                or ""
            ).strip(),
            status=status,
            error=error,
            emitted_at_ms=emitted_at_ms,
            handling_lag_ms=lag_ms,
            startup_elapsed_ms=self._startup_elapsed_ms(),
        )

    def _emit_protocol_status(
        self,
        method: str,
        params: dict,
        *,
        allow_terminal_status: bool = False,
    ) -> None:
        self._update_context_usage(method=method, params=params)
        active_turn_before = self._item_tracker.current_turn_id()
        changed_item_id = self._item_tracker.apply(method=method, params=params)
        apply_info = self._item_tracker.last_apply_info() or {}
        forgotten_turn_reasons = apply_info.get("forgotten_turn_reasons")
        if isinstance(forgotten_turn_reasons, dict):
            for turn_id, reason in forgotten_turn_reasons.items():
                self._logger.event(
                    "protocol_turn_evicted",
                    role=self._role,
                    turn_id=turn_id,
                    reason=reason,
                )
        if apply_info.get("kind") == "item_ignored_forgotten_turn":
            self._logger.event(
                "protocol_item_forgotten_turn_update_ignored",
                role=self._role,
                method=method,
                turn_id=apply_info.get("turn_id"),
                current_turn_distance=apply_info.get("late_distance"),
            )
        if changed_item_id is not None:
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
        terminal_turn_id = str(apply_info.get("turn_id") or "").strip()
        is_historical_terminal = (
            allow_terminal_status
            and method == "turn/completed"
            and active_turn_before is not None
            and terminal_turn_id != active_turn_before
        )
        if method == "item/agentMessage/delta" or (
            method == "turn/completed" and not is_historical_terminal
        ):
            return
        if not should_emit_status_method(method):
            return
        callback(self._role, method, params)

    def _update_context_usage(self, *, method: str, params: dict[str, Any]) -> None:
        thread_id = str(self._thread_id or "").strip()
        if not thread_id:
            self._context_usage = {}
            return
        if compaction_matches_thread(method, params, expected_thread_id=thread_id):
            self._context_usage = {}
            return
        if method != "thread/tokenUsage/updated":
            return
        usage = parse_context_usage(params, expected_thread_id=thread_id)
        if usage is not None:
            self._context_usage = usage


def _summarize_protocol_response(msg: dict[str, Any]) -> dict[str, Any]:
    result = msg.get("result") if isinstance(msg.get("result"), dict) else {}
    thread = result.get("thread") if isinstance(result.get("thread"), dict) else {}
    summary: dict[str, Any] = {}
    thread_id = str(thread.get("id") or result.get("threadId") or "").strip()
    model = str(result.get("model") or thread.get("model") or "").strip()
    if thread_id:
        summary["thread_id"] = thread_id
    if model:
        summary["model"] = model
    counts: dict[str, int] = {}
    for source in (result, thread):
        for key in ("data", "threads", "turns", "items", "history"):
            value = source.get(key)
            if isinstance(value, list):
                counts[key] = len(value)
    if counts:
        summary["counts"] = counts
    error = msg.get("error")
    if isinstance(error, dict):
        summary["error_code"] = error.get("code")
        summary["error_message"] = str(error.get("message") or "")[:300]
    return summary


def _jsonrpc_error_message(value: object) -> str:
    if isinstance(value, dict):
        message = str(value.get("message") or "").strip()
        if message:
            return message
    normalized = str(value or "").strip()
    return normalized or "provider metadata request failed"
