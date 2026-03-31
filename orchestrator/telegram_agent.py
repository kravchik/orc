"""Telegram transport that proxies messages to a local codex app-server agent."""

from __future__ import annotations

import time
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional, Protocol

from orchestrator.adapters import (
    CodexJsonRpcSession,
    InteractiveSessionStartState,
    InteractiveTurnState,
)
from orchestrator.approval import (
    ApprovalPolicy,
    ApprovalRequest,
    build_accept_settings,
)
from orchestrator.codex_single_agent_backend import CodexSingleAgentBackend
from orchestrator.processes import LifecycleLogger
from orchestrator.proxy_access_point import ProxyApprovalDecision, ProxyModelSelection, ProxySubmitPrompt
from orchestrator.proxy_core import ProxyCore
from orchestrator.proxy_model_choices import SUPPORTED_MODEL_CHOICES
from orchestrator.proxy_runner import build_proxy_runtime, run_proxy_runtime
from orchestrator.proxy_runtime import ProxyTransportAdapter, StopProxyRuntime
from orchestrator.protocol_status import (
    format_item_status_summary,
    format_protocol_status,
)
from orchestrator.telegram_output_runtime import TelegramKind, TelegramOutputRuntime
from orchestrator.telegram_status import TelegramStatusConfig
from orchestrator.turn_status_store import TurnStatusStore
from orchestrator.telegram_bridge import (
    TelegramCallbackUpdate,
    TelegramChatState,
    TelegramTextUpdate,
    extract_telegram_http_code,
    process_telegram_updates,
    resolve_allowed_chat_ids,
)
from orchestrator.jsonrpc_client_thread_driver import build_threaded_jsonrpc_client_factory
from orchestrator.telegram_approval_helper import (
    TelegramApprovalDecisionProvider,
    _format_approval_details,
    _format_approval_prompt,
)
from orchestrator.telegram_api_thread_driver import TelegramApiThreadDriver
from orchestrator.telegram_proxy_access_point import TelegramAccessPointAdapter
from orchestrator.thread_resume import resolve_resume_thread_id
from orchestrator.telegram_smoke import TelegramApi


Writer = Callable[[str], None]


@dataclass
class _TelegramAgentRequest:
    chat_id: int
    thread_id: int | None
    prompt: str

    @property
    def access_point(self) -> tuple[int, int | None]:
        return (self.chat_id, self.thread_id)


@dataclass
class _TelegramAgentResult:
    chat_id: int
    thread_id: int | None
    prompt: str
    reply: str


@dataclass
class _TelegramApprovalPromptEvent:
    chat_id: int
    thread_id: int | None
    request: ApprovalRequest


@dataclass
class _TelegramStatusEvent:
    chat_id: int
    thread_id: int | None
    status_text: str


@dataclass
class _TelegramOutboundNote:
    chat_id: int
    thread_id: int | None
    text: str


@dataclass
class _TelegramModelSelection:
    chat_id: int
    thread_id: int | None
    selected_model: str

    @property
    def access_point(self) -> tuple[int, int | None]:
        return (self.chat_id, self.thread_id)


@dataclass
class _TelegramModelApplyResult:
    chat_id: int
    thread_id: int | None
    requested_model: str
    actual_model: str
    result: str
    error: str


class TelegramAgentDriver(Protocol):
    def start(self) -> None: ...

    def stop(self) -> None: ...

    def submit_request(self, request: ProxySubmitPrompt[tuple[int, int | None]]) -> None: ...

    def submit_approval_decision(self, decision: str) -> None: ...

    def submit_model_selection(self, selection: ProxyModelSelection[tuple[int, int | None]]) -> None: ...

    def poll_once(self) -> list[Any]: ...

    def is_ready(self) -> bool: ...

    def get_actual_thread_model(self) -> str: ...

    def get_item_status_snapshot(self) -> list[dict[str, str]]: ...

    def get_item_status_snapshot_for_turn(self, turn_id: str) -> list[dict[str, str]]: ...

    def get_last_item_apply_info(self) -> dict | None: ...


class TelegramInteractiveCodexDriver:
    """Single-threaded interactive turn engine over a threaded JSON-RPC client edge."""

    def __init__(
        self,
        *,
        command: list[str],
        logger: LifecycleLogger,
        request_timeout_sec: float,
        rpc_timeout_sec: float,
        rpc_retries: int,
        approval_policy: ApprovalPolicy,
        thread_approval_policy: str,
        thread_sandbox: str,
        resume_thread_id: str | None,
        working_dir: str | None = None,
        initial_model: str | None = None,
        client_factory: Optional[Callable[..., object]],
        edge_thread_name: str = "telegram-jsonrpc-client-thread",
    ) -> None:
        self._logger = logger
        self._request_timeout_sec = request_timeout_sec
        self._thread_approval_policy = thread_approval_policy
        self._thread_sandbox = thread_sandbox
        self._resume_thread_id = resume_thread_id
        self._requested_model: str | None = (
            str(initial_model).strip() if isinstance(initial_model, str) and str(initial_model).strip() else None
        )
        self._actual_thread_model: str | None = None
        self._session = CodexJsonRpcSession(
            role="agent",
            command=command,
            logger=logger,
            cwd=working_dir,
            env=None,
            request_timeout_sec=request_timeout_sec,
            rpc_timeout_sec=rpc_timeout_sec,
            rpc_retries=rpc_retries,
            approval_policy=approval_policy,
            fsm=None,
            thread_approval_policy=thread_approval_policy,
            thread_sandbox=thread_sandbox,
            approval_decision_provider=None,
            approval_notify_callback=None,
            protocol_status_callback=None,
            protocol_log_include_params=True,
            client_factory=build_threaded_jsonrpc_client_factory(
                base_factory=client_factory,
                thread_name=edge_thread_name,
            ),
        )
        self._startup_state: InteractiveSessionStartState | None = None
        self._startup_ready = False
        self._backend_started_logged = False
        self._pending_requests: list[ProxySubmitPrompt[tuple[int, int | None]]] = []
        self._pending_decisions: list[str] = []
        self._pending_model_selections: list[ProxyModelSelection[tuple[int, int | None]]] = []
        self._event_queue: list[Any] = []
        self._active_request: ProxySubmitPrompt[tuple[int, int | None]] | None = None
        self._active_turn: InteractiveTurnState | None = None
        self._model_apply_selection: ProxyModelSelection[tuple[int, int | None]] | None = None

        self._session.set_protocol_item_callback(self._on_protocol_item)
        self._session.set_protocol_status_callback(self._on_protocol_status)

    def start(self) -> None:
        self._startup_state = self._begin_interactive_start(model=self._requested_model)

    def stop(self) -> None:
        pid = self._session.pid
        self._session.stop()
        self._logger.event("agent_backend_stopped", pid=pid, returncode=self._session.returncode)

    def submit_request(self, request: ProxySubmitPrompt[tuple[int, int | None]]) -> None:
        self._pending_requests.append(request)

    def submit_approval_decision(self, decision: str) -> None:
        self._pending_decisions.append(decision)

    def submit_model_selection(self, selection: ProxyModelSelection[tuple[int, int | None]]) -> None:
        self._pending_model_selections.append(selection)

    def poll_once(self) -> list[Any]:
        if not self._startup_ready:
            self._maybe_finish_startup()
        elif self._model_apply_selection is not None:
            self._maybe_finish_model_apply()
        elif self._pending_model_selections:
            self._maybe_begin_model_apply()
        elif self._active_request is None or self._active_turn is None:
            self._maybe_begin_turn()
        elif self._active_turn.pending_approval is not None:
            self._maybe_submit_approval_decision()
        else:
            self._maybe_poll_turn()
        out = list(self._event_queue)
        self._event_queue.clear()
        return out

    def get_actual_thread_model(self) -> str:
        return str(self._actual_thread_model or "")

    def get_thread_id(self) -> str:
        return str(self._session.thread_id or "")

    def is_ready(self) -> bool:
        return self._startup_ready

    def get_item_status_snapshot(self) -> list[dict[str, str]]:
        return self._session.get_item_status_snapshot()

    def get_item_status_snapshot_for_turn(self, turn_id: str) -> list[dict[str, str]]:
        return self._session.get_item_status_snapshot(turn_id=turn_id)

    def get_last_item_apply_info(self) -> dict | None:
        return self._session.get_last_item_apply_info()

    def _begin_interactive_start(self, *, model: str | None) -> InteractiveSessionStartState:
        start_params = {
            "approvalPolicy": self._thread_approval_policy,
            "sandbox": self._thread_sandbox,
        }
        if isinstance(model, str) and model.strip():
            start_params["model"] = model.strip()
        return self._session.begin_interactive_start(
            resume_thread_id=self._resume_thread_id,
            start_params=start_params,
        )

    def _maybe_finish_startup(self) -> None:
        if self._startup_state is None:
            return
        if self._session.poll_interactive_start(self._startup_state, timeout_sec=0.0):
            self._startup_ready = True
            self._actual_thread_model = str(self._session.thread_model or "").strip()
            if not self._backend_started_logged:
                self._logger.event("agent_backend_started", pid=self._session.pid)
                self._backend_started_logged = True

    def _maybe_begin_model_apply(self) -> None:
        if not self._pending_model_selections:
            return
        selection = self._pending_model_selections.pop(0)
        requested_model = str(selection.selected_model or "").strip()
        chat_id, thread_id = selection.access_point
        if not requested_model:
            self._event_queue.append(
                _TelegramModelApplyResult(
                    chat_id=chat_id,
                    thread_id=thread_id,
                    requested_model=selection.selected_model,
                    actual_model="",
                    result="rejected",
                    error="model must be a non-empty string",
                )
            )
            return
        self._logger.event("model_apply_requested", requested_model=requested_model)
        self._requested_model = requested_model
        self._resume_thread_id = None
        self._session.stop()
        self._startup_ready = False
        self._backend_started_logged = False
        self._startup_state = self._begin_interactive_start(model=requested_model)
        self._model_apply_selection = selection

    def _maybe_finish_model_apply(self) -> None:
        selection = self._model_apply_selection
        if selection is None:
            return
        chat_id, thread_id = selection.access_point
        try:
            self._maybe_finish_startup()
        except Exception as exc:
            self._event_queue.append(
                _TelegramModelApplyResult(
                    chat_id=chat_id,
                    thread_id=thread_id,
                    requested_model=selection.selected_model,
                    actual_model="",
                    result="rejected",
                    error=str(exc),
                )
            )
            self._model_apply_selection = None
            return
        if not self._startup_ready:
            return
        actual = self.get_actual_thread_model()
        self._logger.event(
            "model_apply_completed",
            requested_model=selection.selected_model,
            actual_model=actual,
        )
        self._event_queue.append(
            _TelegramModelApplyResult(
                chat_id=chat_id,
                thread_id=thread_id,
                requested_model=selection.selected_model,
                actual_model=actual,
                result="completed",
                error="",
            )
        )
        self._model_apply_selection = None

    def _maybe_begin_turn(self) -> None:
        if not self._pending_requests:
            return
        request = self._pending_requests.pop(0)
        self._active_request = request
        chat_id, thread_id = request.access_point
        self._logger.event(
            "interactive_driver_begin_turn",
            chat_id=chat_id,
            thread_id=thread_id,
            prompt_preview=request.prompt.strip()[:200],
        )
        try:
            self._active_turn = self._session.begin_interactive_turn(
                request.prompt,
                approval_notify_callback=lambda note, req=request: self._event_queue.append(
                    _TelegramOutboundNote(
                        chat_id=req.access_point[0],
                        thread_id=req.access_point[1],
                        text=note,
                    )
                ),
            )
        except Exception as exc:
            self._logger.event("telegram_agent_error", error=str(exc), error_type=type(exc).__name__)
            self._event_queue.append(
                _TelegramAgentResult(
                    chat_id=chat_id,
                    thread_id=thread_id,
                    prompt=request.prompt,
                    reply=f"agent error: {exc}",
                )
            )
            self._active_request = None
            self._active_turn = None

    def _maybe_submit_approval_decision(self) -> None:
        if self._active_turn is None or self._active_request is None or not self._pending_decisions:
            return
        decision = self._pending_decisions.pop(0)
        self._logger.event(
            "interactive_driver_submit_approval_decision",
            chat_id=self._active_request.access_point[0],
            thread_id=self._active_request.access_point[1],
            decision=decision,
        )
        try:
            self._session.submit_interactive_approval_decision(self._active_turn, decision=decision)
        except Exception as exc:
            chat_id, thread_id = self._active_request.access_point
            self._logger.event("telegram_agent_error", error=str(exc), error_type=type(exc).__name__)
            self._event_queue.append(
                _TelegramAgentResult(
                    chat_id=chat_id,
                    thread_id=thread_id,
                    prompt=self._active_request.prompt,
                    reply=f"agent error: {exc}",
                )
            )
            self._active_request = None
            self._active_turn = None

    def _maybe_poll_turn(self) -> None:
        if self._active_turn is None or self._active_request is None:
            return
        try:
            progress = self._session.poll_interactive_turn(self._active_turn, timeout_sec=0.0)
        except Exception as exc:
            chat_id, thread_id = self._active_request.access_point
            self._logger.event("telegram_agent_error", error=str(exc), error_type=type(exc).__name__)
            self._event_queue.append(
                _TelegramAgentResult(
                    chat_id=chat_id,
                    thread_id=thread_id,
                    prompt=self._active_request.prompt,
                    reply=f"agent error: {exc}",
                )
            )
            self._active_request = None
            self._active_turn = None
            return
        self._logger.event(
            "interactive_driver_turn_progress",
            chat_id=self._active_request.access_point[0],
            thread_id=self._active_request.access_point[1],
            kind=progress.kind,
            turn_id=self._active_turn.turn_id,
        )
        if progress.kind == "approval_required" and progress.approval_request is not None:
            chat_id, thread_id = self._active_request.access_point
            self._event_queue.append(
                _TelegramApprovalPromptEvent(
                    chat_id=chat_id,
                    thread_id=thread_id,
                    request=progress.approval_request,
                )
            )
            return
        if progress.kind == "completed":
            chat_id, thread_id = self._active_request.access_point
            self._logger.event(
                "interactive_driver_turn_completed",
                chat_id=chat_id,
                thread_id=thread_id,
                turn_id=self._active_turn.turn_id,
                reply_preview=(progress.text or "").strip()[:200],
            )
            self._event_queue.append(
                _TelegramAgentResult(
                    chat_id=chat_id,
                    thread_id=thread_id,
                    prompt=self._active_request.prompt,
                    reply=progress.text or "",
                )
            )
            self._active_request = None
            self._active_turn = None

    def _on_protocol_item(self, _role: str, changed_item_id: str) -> None:
        request = self._active_request
        if request is None:
            return
        chat_id, thread_id = request.access_point
        self._queue_event(
            _TelegramStatusEvent(
                chat_id=chat_id,
                thread_id=thread_id,
                status_text=format_item_status_summary(
                    self._session.get_item_status_snapshot(),
                    changed_item_id=changed_item_id,
                ),
            )
        )

    def _on_protocol_status(self, _role: str, method: str, params: dict) -> None:
        request = self._active_request
        if request is None:
            return
        chat_id, thread_id = request.access_point
        self._queue_event(
            _TelegramStatusEvent(
                chat_id=chat_id,
                thread_id=thread_id,
                status_text=_format_protocol_status_for_telegram(method=method, params=params),
            )
        )

    def _queue_event(self, event: object) -> None:
        if isinstance(event, _TelegramStatusEvent):
            if self._event_queue:
                previous = self._event_queue[-1]
                if (
                    isinstance(previous, _TelegramStatusEvent)
                    and previous.chat_id == event.chat_id
                    and previous.thread_id == event.thread_id
                ):
                    self._event_queue[-1] = event
                    return
        self._event_queue.append(event)


class TelegramAgentCore(
    ProxyCore[
        tuple[int, int | None],
        TelegramOutputRuntime,
        TelegramAgentDriver,
        _TelegramAgentResult,
        _TelegramApprovalPromptEvent,
        _TelegramOutboundNote,
        _TelegramStatusEvent,
        dict[str, Any],
    ]
):
    """Single-threaded Telegram runtime state and routing logic."""

    def __init__(
        self,
        *,
        client: TelegramApiThreadDriver,
        logger: LifecycleLogger,
        writer: Writer,
        status_config: TelegramStatusConfig,
        driver: TelegramAgentDriver,
    ) -> None:
        super().__init__(logger=logger, source="telegram", driver=driver, writer=writer)
        self._client = client
        self._status_config = status_config

    def record_model_apply_result(self, result: _TelegramModelApplyResult) -> None:
        self._require_access_point().record_model_apply_result(result)

    def _command_runtime(self, chat_id: int, thread_id: int | None) -> TelegramOutputRuntime:
        output_runtime, _status_store = self._status_runtime_for((chat_id, thread_id))
        return output_runtime

class _TelegramProxyTransport(ProxyTransportAdapter):
    """Telegram polling transport adapter for the shared proxy runtime."""

    def __init__(
        self,
        *,
        client: TelegramApiThreadDriver,
        logger: LifecycleLogger,
        allowed_chat_ids: set[int],
        poll_timeout_sec: int,
        backend_driver: TelegramAgentDriver,
        access_point: TelegramAccessPointAdapter,
        dispatch_action: Callable[[object], None],
    ) -> None:
        self._client = client
        self._logger = logger
        self._allowed_chat_ids = allowed_chat_ids
        self._poll_timeout_sec = poll_timeout_sec
        self._backend_driver = backend_driver
        self._access_point = access_point
        self._dispatch_action = dispatch_action
        self._chat_state = TelegramChatState()
        self._offset_ref: list[int | None] = [None]

    def start(self) -> None:
        self._client.start_polling(
            offset=self._offset_ref[0],
            poll_timeout_sec=self._poll_timeout_sec,
        )

    def stop(self) -> None:
        self._client.stop_polling()

    def ready(self) -> bool:
        return bool(self._backend_driver.is_ready())

    def consume_once(self) -> bool:
        error = self._client.take_poll_error()
        if error is not None:
            http_code = extract_telegram_http_code(error)
            if http_code == 409:
                self._logger.event(
                    "telegram_poll_conflict",
                    source="telegram_agent",
                    http_code=http_code,
                    error=str(error),
                )
                raise StopProxyRuntime(0)
            raise error
        updates = self._client.drain_updates()
        if not updates:
            return False
        process_telegram_updates(
            updates=updates,
            logger=self._logger,
            source="telegram_agent",
            allowed_chat_ids=self._allowed_chat_ids,
            offset_ref=self._offset_ref,
            chat_state=self._chat_state,
            on_text_update=self._on_text_update,
            on_callback_update=self._on_callback_update,
        )
        return True

    def _on_text_update(self, update: TelegramTextUpdate) -> None:
        action = self._access_point.handle_text_update(update)
        if action is not None:
            self._dispatch_action(action)

    def _on_callback_update(self, update: TelegramCallbackUpdate) -> None:
        action = self._access_point.handle_callback_update(update)
        if action is not None:
            self._dispatch_action(action)


def _shorten(value: str, limit: int = 140) -> str:
    compact = " ".join(value.split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."

def run_telegram_agent(
    token: str,
    log_path: str,
    agent_command: list[str],
    request_timeout_sec: float = 0.0,
    rpc_timeout_sec: float = 5.0,
    rpc_retries: int = 3,
    poll_timeout_sec: int = 30,
    allowed_chat_ids: Optional[set[int]] = None,
    insecure_skip_verify: bool = False,
    telegram_api_base_url: str | None = None,
    api: Optional[TelegramApi] = None,
    writer: Writer = print,
    max_poll_cycles: Optional[int] = None,
    driver_factory: Optional[Callable[[LifecycleLogger], TelegramAgentDriver]] = None,
    approval_policy: Optional[ApprovalPolicy] = None,
    thread_approval_policy: str = "on-request",
    thread_sandbox: str = "workspace-write",
    resume: bool = False,
    resume_sessions_root: str | None = None,
    telegram_status_config: TelegramStatusConfig | None = None,
    client_factory: Callable[..., Any] | None = None,
) -> int:
    if not allowed_chat_ids:
        raise ValueError("allowed_chat_ids must be non-empty")
    logger = LifecycleLogger(Path(log_path))
    raw_client = api if api is not None else TelegramApi(
        token=token,
        insecure_skip_verify=insecure_skip_verify,
        api_base_url=telegram_api_base_url,
    )
    client = TelegramApiThreadDriver(raw_client)
    client.start()

    effective_allowed_chat_ids = resolve_allowed_chat_ids(allowed_chat_ids)
    writer("telegram-agent started; send text to proxy to local codex app-server")
    logger.event(
        "telegram_agent_started",
        poll_timeout_sec=poll_timeout_sec,
        request_timeout_sec=request_timeout_sec,
        effective_allowed_chat_ids=sorted(effective_allowed_chat_ids),
    )
    resume_thread_id: str | None = None
    try:
        resume_thread_id = resolve_resume_thread_id(
            resume=resume,
            project_cwd=str(Path.cwd().resolve()),
            sessions_root=resume_sessions_root,
        )
    except RuntimeError as exc:
        logger.event("resume_failed", runtime_kind="proxy", runtime_mode="telegram-agent", error=str(exc))
        writer(f"resume error: {exc}")
        client.stop()
        return 1
    if resume_thread_id:
        logger.event("agent_resume_candidate_found", requested_thread_id=resume_thread_id)

    status_config = telegram_status_config or TelegramStatusConfig()
    driver: TelegramAgentDriver
    if driver_factory is None:
        driver = TelegramInteractiveCodexDriver(
            command=agent_command,
            logger=logger,
            request_timeout_sec=request_timeout_sec,
            rpc_timeout_sec=rpc_timeout_sec,
            rpc_retries=rpc_retries,
            approval_policy=approval_policy or ApprovalPolicy.default(),
            thread_approval_policy=thread_approval_policy,
            thread_sandbox=thread_sandbox,
            resume_thread_id=resume_thread_id,
            client_factory=client_factory,
        )
    else:
        driver = driver_factory(logger)

    core = TelegramAgentCore(
        client=client,
        logger=logger,
        writer=writer,
        status_config=status_config,
        driver=driver,
    )
    access_point = TelegramAccessPointAdapter(core=core)
    core.bind_access_point(access_point)

    def _handle_backend_event(event: object) -> None:
        if isinstance(event, _TelegramAgentResult):
            core.record_result(event)
            return
        if isinstance(event, _TelegramModelApplyResult):
            core.record_model_apply_result(event)
            return
        if isinstance(event, _TelegramApprovalPromptEvent):
            core.register_approval_request(event)
            return
        if isinstance(event, _TelegramOutboundNote):
            core.emit_outbound_note(event)
            return
        raise AssertionError(f"unsupported telegram-agent driver event: {event!r}")

    runtime = build_proxy_runtime(
        core=core,
        access_point=access_point,
        backend_driver=driver,
        transport_factory=lambda dispatch_action: _TelegramProxyTransport(
            client=client,
            logger=logger,
            allowed_chat_ids=effective_allowed_chat_ids,
            poll_timeout_sec=poll_timeout_sec,
            backend_driver=driver,
            access_point=access_point,
            dispatch_action=dispatch_action,
        ),
        status_event_type=_TelegramStatusEvent,
        handle_backend_event=_handle_backend_event,
    )
    def _handle_error(exc: Exception) -> int:
        logger.event("telegram_error", error=str(exc), error_type=type(exc).__name__)
        writer(f"telegram-agent error: {exc}")
        raise
    return run_proxy_runtime(
        runtime=runtime,
        backend_driver=driver,
        request_timeout_sec=request_timeout_sec,
        max_poll_cycles=max_poll_cycles,
        sleep_sec=0.01,
        sleep_after_progress=False,
        writer=writer,
        logger=logger,
        stopping_message="telegram-agent stopping gracefully",
        sigint_event_name="telegram_agent_sigint",
        stopped_event_name="telegram_agent_stopped",
        stop_resources=client.stop,
        handle_error=_handle_error,
    )


def _format_protocol_status_for_telegram(method: str, params: dict | None) -> str:
    return format_protocol_status(method=method, params=params)
