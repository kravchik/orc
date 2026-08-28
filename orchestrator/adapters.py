"""Agent adapter abstractions for ORC1."""

from __future__ import annotations

from dataclasses import dataclass, field
import os
import time
from typing import Any, Callable, Mapping, Optional

from orchestrator import clock
from orchestrator.approval import (
    ApprovalDecisionProvider,
    ApprovalPolicy,
    ApprovalRequest,
    COMMAND_APPROVAL_METHOD,
    build_session_approval_response,
    supports_session_approval,
)
from orchestrator.approval_runtime import (
    ApprovalServerRequest,
    _log_regex_allowlist_trace,
    build_approval_response_plan,
    parse_server_request,
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


def protocol_raw_capture_enabled() -> bool:
    return os.getenv("ORC_PROTOCOL_LOG_RAW", "0").strip().lower() in {"1", "true", "yes", "on"}


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
    raw_item_by_item_id: dict[str, dict[str, Any]] = field(default_factory=dict)
    deferred_pre_start_messages: list[dict] = field(default_factory=list)
    terminal_error: Optional[str] = None
    pending_approval: InteractiveApprovalPending | None = None
    pending_approval_emitted: bool = False


@dataclass(frozen=True)
class InteractiveTurnProgress:
    kind: str
    text: str | None = None
    approval_request: ApprovalRequest | None = None
    error: str | None = None


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
        request_timeout_sec: float,
        rpc_timeout_sec: float,
        rpc_retries: int,
        approval_policy: Optional[ApprovalPolicy],
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
            on_message=None,
            on_stderr=self._on_protocol_stderr,
        )
        self._thread_id: Optional[str] = None
        self._thread_model: Optional[str] = None
        self._thread_metadata: dict[str, Any] = {}
        self._context_usage: dict[str, object] = {}
        self._pending_messages: list[dict] = []
        self._always_allow_commands: set[str] = set()
        self._item_tracker = ProtocolItemTracker()
        self._transport_progressed = False
        self._request_started: dict[int, tuple[str, float]] = {}
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

    def request(self, method: str, params: dict) -> dict:
        req_id = self._send_request(method=method, params=params)
        return self._wait_for_response(req_id=req_id)

    def get_thread_metadata(self) -> dict[str, Any]:
        thread_id = str(self._thread_id or "").strip()
        if not thread_id:
            return {}
        metadata: dict[str, Any] = dict(self._thread_metadata)
        read_payload = self._request_thread_read(thread_id)
        if read_payload:
            metadata.update(read_payload)
        if "loaded" not in metadata:
            loaded = self._request_loaded_state(thread_id)
            if loaded is not None:
                metadata["loaded"] = loaded
        if "model" not in metadata and isinstance(self._thread_model, str) and self._thread_model.strip():
            metadata["model"] = self._thread_model.strip()
        return metadata

    def get_context_usage(self) -> dict[str, object]:
        return dict(self._context_usage)

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
        self._thread_metadata = {}
        self._context_usage = {}
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
            if self._pending_messages:
                msg = self._pending_messages.pop(0)
            else:
                msg = self._read_message(timeout_sec=max(0.0, min(0.05, wait_timeout)) if timeout_sec > 0 else 0.0)
            if msg is None:
                self._log_startup_waiting()
                state.deferred_messages = deferred
                return False
            if state.active_start_req_id is None:
                if msg.get("id") == state.initialize_req_id and "result" in msg:
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
                return InteractiveTurnProgress(kind="idle")
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
            if self._pending_messages:
                msg = self._pending_messages.pop(0)
            else:
                msg = self._read_message(timeout_sec=max(0.0, min(0.05, wait_timeout)) if timeout_sec > 0 else 0.0)
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
            allow_always=supports_session_approval(
                method=pending.server_request.method,
                params=pending.server_request.params,
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
            str(session_result["decision"])
            if session_result is not None
            else normalized
        )
        self._logger.event(
            "approval_human_decision",
            role=self._role,
            id=pending.server_request.req_id,
            method=pending.server_request.method,
            decision=resolved_decision,
            always_allow=always_allow,
        )
        result: dict[str, Any] = session_result or {"decision": resolved_decision}
        if always_allow:
            command = pending.server_request.params.get("command")
            if (
                pending.server_request.method == COMMAND_APPROVAL_METHOD
                and isinstance(command, str)
                and command
            ):
                self._always_allow_commands.add(command)
            accept_settings = result.get("acceptSettings")
            if isinstance(accept_settings, dict):
                self._logger.event(
                    "approval_accept_settings_applied",
                    role=self._role,
                    id=pending.server_request.req_id,
                    accept_settings=accept_settings,
                )
            elif result.get("decision") == "acceptForSession":
                self._logger.event(
                    "approval_session_accept_applied",
                    role=self._role,
                    id=pending.server_request.req_id,
                    method=pending.server_request.method,
                )
        self._send_response(req_id=pending.server_request.req_id, result=result)
        state.pending_approval = None
        state.pending_approval_emitted = False

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

    def submit_interactive_turn_steer(self, state: InteractiveTurnState, text: str) -> None:
        if self._thread_id is None:
            raise RuntimeError("thread not initialized")
        if not isinstance(state.turn_id, str) or not state.turn_id.strip():
            raise RuntimeError("interactive turn has no active turn id")
        steer_text = str(text)
        req_id = self._send_request(
            method="turn/steer",
            params={
                "threadId": self._thread_id,
                "expectedTurnId": state.turn_id.strip(),
                "input": [{"type": "text", "text": steer_text, "text_elements": []}],
            },
        )
        self._logger.event(
            "interactive_turn_steer_requested",
            role=self._role,
            req_id=req_id,
            thread_id=self._thread_id,
            turn_id=state.turn_id.strip(),
            prompt_preview=steer_text.strip()[:200],
        )
        self._wait_for_response(req_id=req_id)

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
            self._emit_protocol_status(method=method, params=params)
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
        self._thread_metadata = {"id": self._thread_id, "model": self._thread_model}

    def _request_thread_read(self, thread_id: str) -> dict[str, Any]:
        last_error: Exception | None = None
        for params in ({"threadId": thread_id}, {"id": thread_id}):
            try:
                response = self.request("thread/read", params)
            except Exception as exc:
                last_error = exc
                continue
            result = response.get("result") if isinstance(response.get("result"), dict) else {}
            thread_payload = result.get("thread")
            if isinstance(thread_payload, dict):
                return dict(thread_payload)
            if isinstance(result, dict) and any(key in result for key in ("id", "status", "preview", "ephemeral")):
                return dict(result)
        if last_error is not None:
            self._logger.event(
                "protocol_thread_read_failed",
                role=self._role,
                thread_id=thread_id,
                error=str(last_error),
                error_type=type(last_error).__name__,
            )
        return {}

    def _request_loaded_state(self, thread_id: str) -> bool | None:
        try:
            response = self.request("thread/loaded/list", {})
        except Exception as exc:
            self._logger.event(
                "protocol_thread_loaded_list_failed",
                role=self._role,
                thread_id=thread_id,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return None
        result = response.get("result") if isinstance(response.get("result"), dict) else {}
        raw_items = None
        if isinstance(result.get("data"), list):
            raw_items = result.get("data")
        elif isinstance(result.get("threads"), list):
            raw_items = result.get("threads")
        elif isinstance(result.get("threadIds"), list):
            raw_items = result.get("threadIds")
        if not isinstance(raw_items, list):
            return None
        for item in raw_items:
            if isinstance(item, str) and item.strip() == thread_id:
                return True
            if isinstance(item, dict):
                item_id = item.get("id")
                if isinstance(item_id, str) and item_id.strip() == thread_id:
                    return True
        return False

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
            if status == "interrupted":
                self._logger.event(f"{self._role}_interrupted", turn_id=state.turn_id)
                return InteractiveTurnProgress(kind="interrupted")
            self._logger.event(f"{self._role}_output", response=text, turn_id=state.turn_id)
            return InteractiveTurnProgress(kind="completed", text=text)
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
        decision_trace = self._approval_policy.decide_with_trace(method=request.method, params=request.params) if self._approval_policy is not None else None
        decision = decision_trace.decision if decision_trace is not None else "human"
        if decision == "human" and decision_trace is not None:
            _log_regex_allowlist_trace(
                logger=self._logger,
                role=self._role,
                request=request,
                decision=decision_trace.decision,
                decision_source=decision_trace.source,
                command=decision_trace.command,
                affected_paths=decision_trace.affected_paths,
                regex_trace=decision_trace.regex_trace,
                approval_notify=state.approval_notify_callback or self._approval_notify_callback,
            )
        if decision != "human":
            plan = build_approval_response_plan(
                request=request,
                logger=self._logger,
                role=self._role,
                approval_policy=self._approval_policy,
                approval_decision_provider=None,
                approval_notify=state.approval_notify_callback or self._approval_notify_callback,
                always_allow_commands=self._always_allow_commands,
                before_human_required=None,
                decide_human=None,
                log_approval_requested=True,
                log_human_required=True,
                log_human_decision=True,
                log_auto_decision=True,
                decision_trace=decision_trace,
            )
            self._send_response(req_id=plan.req_id, result=plan.result)
            return None

        self._logger.event(
            "approval_requested",
            role=self._role,
            id=request.req_id,
            method=request.method,
            params=request.params,
        )
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
                if status == "interrupted":
                    self._pending_messages = deferred + self._pending_messages
                    return ""
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
                raise RuntimeError(result.error)
            msg = result.message if result.kind == "message" else None
            if msg is not None:
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
            self._on_protocol_in("", msg)
            return msg
        return None

    def consume_transport_progress(self) -> bool:
        progressed = self._transport_progressed
        self._transport_progressed = False
        return progressed

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
            before_human_required=None,
            decide_human=_decide_human,
            log_approval_requested=True,
            log_human_required=True,
            log_human_decision=True,
            log_auto_decision=True,
        )

        self._send_response(req_id=plan.req_id, result=plan.result)
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

    def _emit_protocol_status(self, method: str, params: dict) -> None:
        self._update_context_usage(method=method, params=params)
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
