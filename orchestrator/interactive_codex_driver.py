"""Transport-neutral interactive Codex JSON-RPC driver."""

from __future__ import annotations

from typing import Any, Callable, Optional

from orchestrator import clock
from orchestrator.access_point_common import AccessPointId
from orchestrator.adapters import (
    CodexJsonRpcSession,
    InteractiveSessionStartState,
    InteractiveTurnState,
)
from orchestrator.approval import ApprovalPolicy
from orchestrator.interactive_driver_events import (
    InteractiveAgentResult,
    InteractiveApprovalPromptEvent,
    InteractiveInterruptResult,
    InteractiveModelApplyResult,
    InteractiveOutboundNote,
    InteractivePrompt,
    InteractiveStatusEvent,
)
from orchestrator.jsonrpc_client_thread_driver import build_threaded_jsonrpc_client_factory
from orchestrator.processes import LifecycleLogger
from orchestrator.protocol_status import (
    format_item_status_summary,
    format_protocol_status,
    is_completed_agent_commentary,
)
from orchestrator.proxy_access_point import ProxyModelSelection
from orchestrator.turn_status_store import should_suppress_status_apply_info


class CodexInteractiveDriver:
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
        edge_thread_name: str = "codex-jsonrpc-client-thread",
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
            thread_approval_policy=thread_approval_policy,
            thread_sandbox=thread_sandbox,
            approval_decision_provider=None,
            approval_notify_callback=None,
            protocol_status_callback=None,
            protocol_log_include_params=False,
            client_factory=build_threaded_jsonrpc_client_factory(
                base_factory=client_factory,
                thread_name=edge_thread_name,
            ),
        )
        self._startup_state: InteractiveSessionStartState | None = None
        self._startup_ready = False
        self._backend_started_logged = False
        self._pending_requests: list[InteractivePrompt] = []
        self._pending_decisions: list[str] = []
        self._pending_model_selections: list[
            ProxyModelSelection[tuple[AccessPointId, AccessPointId | None]]
        ] = []
        self._event_queue: list[Any] = []
        self._active_request: InteractivePrompt | None = None
        self._active_turn: InteractiveTurnState | None = None
        self._active_turn_started_at: float | None = None
        self._model_apply_selection: ProxyModelSelection[
            tuple[AccessPointId, AccessPointId | None]
        ] | None = None
        self._status_access_point_fallback: tuple[AccessPointId, AccessPointId | None] | None = None
        self._interrupt_in_flight = False
        self._steer_blocked_turn_id: str | None = None
        self._pending_request_seq = 0
        self._last_pending_wait_log_key: tuple[object, ...] | None = None
        self._poll_progressed = False
        self._terminal_error = ""

        self._session.set_protocol_item_callback(self._on_protocol_item)
        self._session.set_protocol_status_callback(self._on_protocol_status)

    def start(self) -> None:
        self._startup_state = self._begin_interactive_start(model=self._requested_model)

    def stop(self) -> None:
        pid = self._session.pid
        self._logger.event(
            "interactive_driver_stop_requested",
            pid=pid,
            queue_size=len(self._pending_requests),
            has_active_request=self._active_request is not None,
            has_active_turn=self._active_turn is not None,
            active_turn_id=self._active_turn.turn_id if self._active_turn is not None else None,
            active_pending_approval=(
                self._active_turn.pending_approval is not None if self._active_turn is not None else False
            ),
            startup_ready=self._startup_ready,
            model_apply_in_progress=self._model_apply_selection is not None,
            pending_model_selections=len(self._pending_model_selections),
            interrupt_in_flight=self._interrupt_in_flight,
        )
        self._session.stop()
        self._logger.event("agent_backend_stopped", pid=pid, returncode=self._session.returncode)

    def submit_request(self, request: InteractivePrompt) -> None:
        self._pending_requests.append(request)
        self._pending_request_seq += 1
        self._last_pending_wait_log_key = None
        chat_id, thread_id = request.access_point
        self._logger.event(
            "interactive_driver_request_queued",
            chat_id=chat_id,
            thread_id=thread_id,
            queue_size=len(self._pending_requests),
            prompt_preview=request.prompt.strip()[:200],
            startup_ready=self._startup_ready,
            has_active_request=self._active_request is not None,
            has_active_turn=self._active_turn is not None,
            active_turn_id=self._active_turn.turn_id if self._active_turn is not None else None,
            active_pending_approval=(
                self._active_turn.pending_approval is not None if self._active_turn is not None else False
            ),
            interrupt_in_flight=self._interrupt_in_flight,
            model_apply_in_progress=self._model_apply_selection is not None,
            pending_model_selections=len(self._pending_model_selections),
        )

    def submit_approval_decision(self, decision: str) -> None:
        self._pending_decisions.append(decision)

    def submit_approval_decision_now(self, decision: str) -> None:
        if self._active_turn is None:
            raise RuntimeError("no active turn is waiting for approval")
        self._session.submit_interactive_approval_decision(self._active_turn, decision=decision)

    def submit_model_selection(
        self,
        selection: ProxyModelSelection[tuple[AccessPointId, AccessPointId | None]],
    ) -> None:
        self._pending_model_selections.append(selection)

    def has_active_turn(self) -> bool:
        return self._active_request is not None and self._active_turn is not None

    def pending_request_count(self) -> int:
        return len(self._pending_requests)

    def get_active_turn_id(self) -> str:
        if self._active_turn is None:
            return ""
        return str(self._active_turn.turn_id or "").strip()

    def has_busy_work(self) -> bool:
        return (
            self._active_request is not None
            or self._active_turn is not None
            or self._model_apply_selection is not None
            or bool(self._pending_requests)
            or bool(self._pending_decisions)
            or bool(self._pending_model_selections)
        )

    def can_steer_active_turn(self) -> bool:
        if self._active_request is None or self._active_turn is None:
            return False
        if self._interrupt_in_flight or self._active_turn.pending_approval is not None:
            return False
        turn_id = str(self._active_turn.turn_id or "").strip()
        if not turn_id:
            return False
        return turn_id != str(self._steer_blocked_turn_id or "")

    def interrupt_active_turn(self) -> None:
        if self._active_turn is None or self._active_request is None:
            raise RuntimeError("no active turn to interrupt")
        self._logger.event(
            "interactive_driver_interrupt_requested",
            chat_id=self._active_request.access_point[0],
            thread_id=self._active_request.access_point[1],
            turn_id=self._active_turn.turn_id,
        )
        self._session.submit_interactive_turn_interrupt(self._active_turn)
        self._active_turn.pending_approval = None
        self._active_turn.pending_approval_emitted = False
        self._interrupt_in_flight = True

    def poll_once(self) -> list[Any]:
        self._poll_progressed = False
        was_ready = self._startup_ready
        if not self._startup_ready:
            self._log_pending_request_waiting("startup_not_ready")
            self._maybe_finish_startup()
            returncode = self._session.returncode
            if not self._startup_ready and returncode is not None:
                raise RuntimeError(f"app-server exited during startup (returncode={returncode})")
        elif self._model_apply_selection is not None:
            self._log_pending_request_waiting("model_apply_in_progress")
            self._maybe_finish_model_apply()
        elif self._pending_model_selections:
            self._log_pending_request_waiting("model_selection_pending")
            self._maybe_begin_model_apply()
        elif self._active_request is None or self._active_turn is None:
            drained_late = False
            if not self._pending_requests:
                drained_late = self._session.poll_background_protocol_message(timeout_sec=0.0)
            if drained_late:
                pass
            else:
                self._maybe_begin_turn()
        elif self._active_turn.pending_approval is not None and not self._interrupt_in_flight:
            self._log_pending_request_waiting("active_turn_waiting_on_approval")
            self._maybe_submit_approval_decision()
        elif self._maybe_submit_pending_steer():
            pass
        else:
            self._log_pending_request_waiting(self._pending_request_wait_reason())
            self._maybe_poll_turn()
        consume_transport_progress = getattr(self._session, "consume_transport_progress", None)
        if callable(consume_transport_progress) and consume_transport_progress():
            self._poll_progressed = True
        if self._startup_ready != was_ready:
            self._poll_progressed = True
        out = list(self._event_queue)
        self._event_queue.clear()
        if out:
            self._poll_progressed = True
        return out

    def consume_poll_progress(self) -> bool:
        progressed = self._poll_progressed
        self._poll_progressed = False
        return progressed

    def get_actual_thread_model(self) -> str:
        return str(self._actual_thread_model or "")

    def get_terminal_error(self) -> str:
        if self._terminal_error:
            return self._terminal_error
        returncode = self._session.returncode
        if returncode is None:
            return ""
        return f"app-server exited unexpectedly (returncode={returncode})"

    def get_thread_id(self) -> str:
        return str(self._session.thread_id or "")

    def get_thread_metadata(self) -> dict[str, object]:
        return self._session.get_thread_metadata()

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
                InteractiveModelApplyResult(
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
                InteractiveModelApplyResult(
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
            InteractiveModelApplyResult(
                chat_id=chat_id,
                thread_id=thread_id,
                requested_model=selection.selected_model,
                actual_model=actual,
                result="completed",
                error="",
            )
        )
        self._model_apply_selection = None

    def _pending_request_wait_reason(self) -> str:
        if not self._pending_requests:
            return "none"
        if self._active_request is None or self._active_turn is None:
            return "no_active_turn"
        if self._interrupt_in_flight:
            return "interrupt_in_flight"
        if self._active_turn.pending_approval is not None:
            return "active_turn_waiting_on_approval"
        turn_id = str(self._active_turn.turn_id or "").strip()
        if not turn_id:
            return "active_turn_id_missing"
        if turn_id == str(self._steer_blocked_turn_id or ""):
            return "steer_blocked_for_turn"
        if self._pending_requests[0].access_point != self._active_request.access_point:
            return "active_turn_different_access_point"
        return "active_turn_busy"

    def _log_pending_request_waiting(self, reason: str) -> None:
        if not self._pending_requests:
            return
        request = self._pending_requests[0]
        chat_id, thread_id = request.access_point
        active_turn_id = self._active_turn.turn_id if self._active_turn is not None else None
        active_pending_approval = (
            self._active_turn.pending_approval is not None if self._active_turn is not None else False
        )
        key = (
            self._pending_request_seq,
            reason,
            len(self._pending_requests),
            chat_id,
            thread_id,
            active_turn_id,
            active_pending_approval,
            self._interrupt_in_flight,
            self._model_apply_selection is not None,
            len(self._pending_model_selections),
            self._startup_ready,
        )
        if key == self._last_pending_wait_log_key:
            return
        self._last_pending_wait_log_key = key
        self._logger.event(
            "interactive_driver_request_waiting",
            reason=reason,
            chat_id=chat_id,
            thread_id=thread_id,
            queue_size=len(self._pending_requests),
            prompt_preview=request.prompt.strip()[:200],
            startup_ready=self._startup_ready,
            has_active_request=self._active_request is not None,
            has_active_turn=self._active_turn is not None,
            active_turn_id=active_turn_id,
            active_pending_approval=active_pending_approval,
            interrupt_in_flight=self._interrupt_in_flight,
            steer_blocked_turn_id=self._steer_blocked_turn_id,
            model_apply_in_progress=self._model_apply_selection is not None,
            pending_model_selections=len(self._pending_model_selections),
        )

    def _maybe_begin_turn(self) -> None:
        if not self._pending_requests:
            return
        request = self._pending_requests.pop(0)
        self._last_pending_wait_log_key = None
        self._active_request = request
        self._status_access_point_fallback = request.access_point
        self._interrupt_in_flight = False
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
                    InteractiveOutboundNote(
                        chat_id=req.access_point[0],
                        thread_id=req.access_point[1],
                        text=note,
                    )
                ),
            )
        except Exception as exc:
            self._logger.event("interactive_driver_error", error=str(exc), error_type=type(exc).__name__)
            self._event_queue.append(
                InteractiveAgentResult(
                    chat_id=chat_id,
                    thread_id=thread_id,
                    prompt=request.prompt,
                    reply=f"agent error: {exc}",
                )
            )
            self._active_request = None
            self._active_turn = None
            self._active_turn_started_at = None
            self._interrupt_in_flight = False
            self._steer_blocked_turn_id = None
            return
        self._steer_blocked_turn_id = None
        self._active_turn_started_at = clock.monotonic()
        self._logger.event(
            "interactive_driver_turn_start_submitted",
            chat_id=chat_id,
            thread_id=thread_id,
            turn_id=self._active_turn.turn_id,
            queue_size=len(self._pending_requests),
            prompt_preview=request.prompt.strip()[:200],
        )

    def _maybe_submit_pending_steer(self) -> bool:
        if not self.can_steer_active_turn() or not self._pending_requests:
            return False
        if self._active_request is None or self._active_turn is None:
            return False
        request = self._pending_requests[0]
        if request.access_point != self._active_request.access_point:
            return False
        turn_id = str(self._active_turn.turn_id or "").strip()
        try:
            self._session.submit_interactive_turn_steer(self._active_turn, request.prompt)
        except Exception as exc:
            chat_id, thread_id = request.access_point
            self._logger.event(
                "interactive_driver_steer_failed",
                chat_id=chat_id,
                thread_id=thread_id,
                turn_id=turn_id,
                prompt_preview=request.prompt.strip()[:200],
                error=str(exc),
                error_type=type(exc).__name__,
                fallback="queued",
            )
            self._steer_blocked_turn_id = turn_id or None
            return False
        self._pending_requests.pop(0)
        self._last_pending_wait_log_key = None
        chat_id, thread_id = request.access_point
        self._logger.event(
            "interactive_driver_steer_submitted",
            chat_id=chat_id,
            thread_id=thread_id,
            turn_id=turn_id,
            prompt_preview=request.prompt.strip()[:200],
        )
        return True

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
            self._logger.event("interactive_driver_error", error=str(exc), error_type=type(exc).__name__)
            self._event_queue.append(
                InteractiveAgentResult(
                    chat_id=chat_id,
                    thread_id=thread_id,
                    prompt=self._active_request.prompt,
                    reply=f"agent error: {exc}",
                    turn_id=str(self._active_turn.turn_id or ""),
                )
            )
            self._active_request = None
            self._active_turn = None
            self._active_turn_started_at = None
            self._interrupt_in_flight = False
            self._steer_blocked_turn_id = None

    def _maybe_poll_turn(self) -> None:
        if self._active_turn is None or self._active_request is None:
            return
        if (
            self._request_timeout_sec > 0.0
            and self._active_turn_started_at is not None
            and (clock.monotonic() - self._active_turn_started_at) >= self._request_timeout_sec
        ):
            chat_id, thread_id = self._active_request.access_point
            turn_id = str(self._active_turn.turn_id or "")
            self._logger.event(
                "interactive_driver_turn_timed_out",
                chat_id=chat_id,
                thread_id=thread_id,
                turn_id=turn_id,
                timeout_sec=self._request_timeout_sec,
            )
            self._event_queue.append(
                InteractiveAgentResult(
                    chat_id=chat_id,
                    thread_id=thread_id,
                    prompt=self._active_request.prompt,
                    reply=f"agent error: turn timed out after {self._request_timeout_sec:.3f}s",
                    turn_id=turn_id,
                )
            )
            self._active_request = None
            self._active_turn = None
            self._active_turn_started_at = None
            self._interrupt_in_flight = False
            self._steer_blocked_turn_id = None
            return
        while True:
            try:
                progress = self._session.poll_interactive_turn(self._active_turn, timeout_sec=0.0)
            except Exception as exc:
                chat_id, thread_id = self._active_request.access_point
                self._logger.event("interactive_driver_error", error=str(exc), error_type=type(exc).__name__)
                self._terminal_error = str(exc)
                self._event_queue.append(
                    InteractiveAgentResult(
                        chat_id=chat_id,
                        thread_id=thread_id,
                        prompt=self._active_request.prompt,
                        reply=f"agent error: {exc}",
                        turn_id=str(self._active_turn.turn_id or ""),
                    )
                )
                self._active_request = None
                self._active_turn = None
                self._active_turn_started_at = None
                self._interrupt_in_flight = False
                self._steer_blocked_turn_id = None
                return
            if progress.kind == "idle":
                return
            self._logger.event(
                "interactive_driver_turn_progress",
                chat_id=self._active_request.access_point[0],
                thread_id=self._active_request.access_point[1],
                kind=progress.kind,
                turn_id=self._active_turn.turn_id,
            )
            if progress.kind == "progressed":
                if self._event_queue:
                    return
                continue
            if progress.kind == "approval_required" and progress.approval_request is not None:
                chat_id, thread_id = self._active_request.access_point
                self._active_turn.pending_approval_emitted = True
                self._event_queue.append(
                    InteractiveApprovalPromptEvent(
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
                    InteractiveAgentResult(
                        chat_id=chat_id,
                        thread_id=thread_id,
                        prompt=self._active_request.prompt,
                        reply=progress.text or "",
                        turn_id=str(self._active_turn.turn_id or ""),
                    )
                )
                self._active_request = None
                self._active_turn = None
                self._active_turn_started_at = None
                self._interrupt_in_flight = False
                self._steer_blocked_turn_id = None
                return
            if progress.kind == "interrupted":
                chat_id, thread_id = self._active_request.access_point
                self._logger.event(
                    "interactive_driver_turn_interrupted",
                    chat_id=chat_id,
                    thread_id=thread_id,
                    turn_id=self._active_turn.turn_id,
                )
                self._event_queue.append(
                    InteractiveInterruptResult(
                        chat_id=chat_id,
                        thread_id=thread_id,
                        result="interrupted",
                    )
                )
                self._active_request = None
                self._active_turn = None
                self._active_turn_started_at = None
                self._interrupt_in_flight = False
                self._steer_blocked_turn_id = None
                return

    def _on_protocol_item(self, _role: str, changed_item_id: str) -> None:
        request = self._active_request
        access_point = request.access_point if request is not None else self._status_access_point_fallback
        if access_point is None:
            return
        chat_id, thread_id = access_point
        apply_info = self._session.get_last_item_apply_info() or {}
        snapshot = self._session.get_item_status_snapshot()
        capture_snapshot = is_completed_agent_commentary(
            snapshot,
            changed_item_id=changed_item_id,
        )
        self._queue_event(
            InteractiveStatusEvent(
                chat_id=chat_id,
                thread_id=thread_id,
                status_text=format_item_status_summary(
                    snapshot,
                    changed_item_id=changed_item_id,
                ),
                suppress_status=should_suppress_status_apply_info(apply_info),
                status_snapshot=(
                    tuple(dict(item) for item in snapshot)
                    if capture_snapshot
                    else None
                ),
                apply_info=dict(apply_info) if capture_snapshot else None,
            )
        )

    def _on_protocol_status(self, _role: str, method: str, params: dict) -> None:
        apply_info = self._session.get_last_item_apply_info() or {}
        request = self._active_request
        access_point = request.access_point if request is not None else self._status_access_point_fallback
        if access_point is None:
            return
        chat_id, thread_id = access_point
        turn = params.get("turn")
        turn_id = str(turn.get("id") or "").strip() if isinstance(turn, dict) else ""
        if not turn_id:
            turn_id = str(params.get("turnId") or "").strip()
        self._queue_event(
            InteractiveStatusEvent(
                chat_id=chat_id,
                thread_id=thread_id,
                status_text=format_protocol_status(method=method, params=params),
                protocol_method=method,
                turn_id=turn_id,
                suppress_status=should_suppress_status_apply_info(apply_info),
            )
        )

    def _queue_event(self, event: object) -> None:
        if isinstance(event, InteractiveStatusEvent):
            if self._event_queue:
                previous = self._event_queue[-1]
                if (
                    isinstance(previous, InteractiveStatusEvent)
                    and previous.chat_id == event.chat_id
                    and previous.thread_id == event.thread_id
                ):
                    if previous.status_snapshot is not None:
                        self._event_queue.append(event)
                        return
                    if previous.protocol_method == "turn/started" and not event.protocol_method:
                        event.protocol_method = previous.protocol_method
                        event.turn_id = previous.turn_id
                    event.suppress_status = previous.suppress_status or event.suppress_status
                    self._event_queue[-1] = event
                    return
        self._event_queue.append(event)
