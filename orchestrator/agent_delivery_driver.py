"""Provider-neutral delivery driver used by the canonical surfaces."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, replace
from enum import StrEnum
import time
from orchestrator.access_point_common import AccessPointId
from orchestrator.agent_capabilities import AgentCapabilitySnapshot
from orchestrator.agent_identity import (
    ApprovalRef,
    ElicitationRef,
    InspectionRef,
    ProviderSessionRef,
    TurnRef,
)
from orchestrator.agent_inspection import (
    AgentRuntimeProjection,
    projection_with_session_error,
    unavailable_projection,
)
from orchestrator.agent_io import AgentOutput
from orchestrator.agent_provider import (
    AgentSessionConfig,
    OpenSessionCorrelationId,
    OpenSessionOperation,
    ResumeSessionRequest,
    OpenFailed,
    Opened,
    ProviderHost,
    ProviderHostLifetime,
    StartSessionRequest,
    StopHost,
    validate_timeout_seconds,
)
from orchestrator.agent_session import (
    AgentApprovalRequest,
    AgentApprovalKind,
    AgentFormRequest,
    BoundedIdentityHistory,
    AgentSessionPort,
    AgentSessionEventDispatch,
    AgentSessionEventDispatchDisposition,
    ApprovalDecisionOrigin,
    ApprovalDecisionResult,
    AgentEffectOutcome,
    AgentEffectResult,
    ApprovalRequested,
    CloseSession,
    ElicitationAction,
    ElicitationRequested,
    EffortSelectionAccepted,
    EffortSelectionFailed,
    EffortSelectionUncertain,
    InspectionCompleted,
    InterruptTurn,
    ModelSelectionAccepted,
    ModelSelectionFailed,
    ModelSelectionUncertain,
    NonTerminalEventDisposition,
    OutputDelta,
    ProviderNotice,
    RawProviderErrorPayload,
    ResolveApproval,
    ResolveElicitation,
    RefreshInspection,
    SessionFailed,
    SessionStarted,
    SetEffort,
    SetModel,
    StartTurn,
    TrySteerTurn,
    TurnCompleted,
    TurnFailed,
    TurnInterrupted,
    TurnStarted,
    WorkStatusChanged,
    WorkStatusKind,
    approval_requested_disposition,
    approval_decision_preflight,
    dispatch_agent_session_event,
    turn_started_disposition,
    raw_provider_error_lifecycle_fields,
)
from orchestrator.approval import ApprovalPolicy, log_approval_policy_trace
from orchestrator.interactive_driver_events import (
    AgentDeliveryEvent,
    AgentEffortSelection,
    ApprovalDeliveryReceipt,
    ElicitationDeliveryReceipt,
    AgentModelSelection,
    InteractiveAgentResult,
    InteractiveApprovalPromptEvent,
    InteractiveElicitationPromptEvent,
    InteractiveEffortApplyResult,
    InteractiveInterruptResult,
    InteractiveInspectionResult,
    InteractiveModelApplyResult,
    InteractiveOutboundNote,
    InteractiveAgentInputRequest,
    InteractiveStatusEvent,
    InteractiveTurnOutcome,
    coalesce_status_events,
    status_stream_key,
)
from orchestrator.processes import LifecycleLogger


_MAX_TURN_ACCESS_POINT_OWNERS = 64
_MAX_COMPLETED_APPROVAL_REFS_PER_TURN = 64


class DeliveryTerminalSignal(StrEnum):
    TURN_TERMINAL = "turn_terminal"
    SESSION_FAILED = "session_failed"


class DeliveryTerminalDisposition(StrEnum):
    ACCEPTED = "accepted"
    SYNTHESIZED = "synthesized"
    NO_ACTIVE_TURN = "no_active_turn"
    STALE = "stale"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class DeliveryTerminalState:
    active_turn_ref: TurnRef | None
    session_available: bool


@dataclass(frozen=True)
class DeliveryTerminalTransition:
    state: DeliveryTerminalState
    disposition: DeliveryTerminalDisposition
    terminal_turn_ref: TurnRef | None


@dataclass(frozen=True)
class _DeferredSessionFailure:
    error: str


def transition_delivery_terminal(
    state: DeliveryTerminalState,
    *,
    signal: DeliveryTerminalSignal,
    event_turn_ref: TurnRef | None = None,
) -> DeliveryTerminalTransition:
    """Classify terminal ordering without owning mutable delivery state."""
    if not isinstance(state, DeliveryTerminalState):
        raise TypeError("delivery terminal transition requires DeliveryTerminalState")
    if not isinstance(signal, DeliveryTerminalSignal):
        raise TypeError("delivery terminal transition requires DeliveryTerminalSignal")
    if not state.session_available:
        return DeliveryTerminalTransition(
            state=DeliveryTerminalState(None, False),
            disposition=DeliveryTerminalDisposition.UNAVAILABLE,
            terminal_turn_ref=None,
        )
    if signal is DeliveryTerminalSignal.SESSION_FAILED:
        if event_turn_ref is not None:
            raise ValueError("session failure must not include a turn ref")
        return DeliveryTerminalTransition(
            state=DeliveryTerminalState(None, False),
            disposition=(
                DeliveryTerminalDisposition.SYNTHESIZED
                if state.active_turn_ref is not None
                else DeliveryTerminalDisposition.NO_ACTIVE_TURN
            ),
            terminal_turn_ref=state.active_turn_ref,
        )
    if not isinstance(event_turn_ref, TurnRef):
        raise ValueError("turn terminal requires a TurnRef")
    if event_turn_ref != state.active_turn_ref:
        return DeliveryTerminalTransition(
            state=state,
            disposition=DeliveryTerminalDisposition.STALE,
            terminal_turn_ref=None,
        )
    return DeliveryTerminalTransition(
        state=DeliveryTerminalState(None, True),
        disposition=DeliveryTerminalDisposition.ACCEPTED,
        terminal_turn_ref=event_turn_ref,
    )


class TurnAccessPointOwners:
    """Bounded lifecycle ownership for status events from current and recent turns."""

    def __init__(self, *, capacity: int = _MAX_TURN_ACCESS_POINT_OWNERS) -> None:
        if not isinstance(capacity, int) or isinstance(capacity, bool) or capacity <= 0:
            raise ValueError("turn access-point owner capacity must be a positive integer")
        self._capacity = capacity
        self._owners: OrderedDict[
            TurnRef,
            tuple[AccessPointId, AccessPointId | None],
        ] = OrderedDict()
        self._evicted: OrderedDict[TurnRef, None] = OrderedDict()

    def remember(
        self,
        turn_ref: TurnRef,
        access_point: tuple[AccessPointId, AccessPointId | None],
    ) -> None:
        existing = self._owners.get(turn_ref)
        if existing is not None:
            if existing != access_point:
                raise RuntimeError("turn access-point owner cannot change")
            return
        self._evicted.pop(turn_ref, None)
        self._owners[turn_ref] = access_point
        while len(self._owners) > self._capacity:
            evicted_ref, _ = self._owners.popitem(last=False)
            self._evicted[evicted_ref] = None
        while len(self._evicted) > self._capacity:
            self._evicted.popitem(last=False)

    def resolve(
        self,
        turn_ref: TurnRef,
    ) -> tuple[
        tuple[AccessPointId, AccessPointId | None] | None,
        NonTerminalEventDisposition,
    ]:
        owner = self._owners.get(turn_ref)
        if owner is not None:
            return owner, NonTerminalEventDisposition.ACCEPTED
        if turn_ref in self._evicted:
            return None, NonTerminalEventDisposition.EVICTED
        return None, NonTerminalEventDisposition.UNKNOWN

    def clear(self) -> None:
        self._owners.clear()
        self._evicted.clear()


class AgentDeliveryDriver:
    """Translate interaction effects and provider events without owning scheduling."""

    def __init__(
        self,
        *,
        provider_host: ProviderHost,
        logger: LifecycleLogger,
        session_config: AgentSessionConfig,
        resume_session_ref: ProviderSessionRef | None,
        initial_capabilities: AgentCapabilitySnapshot,
        status_source: str,
        approval_policy: ApprovalPolicy | None = None,
        drain_status_batch: bool = False,
        host_lifetime: ProviderHostLifetime = ProviderHostLifetime.DEDICATED,
        session_close_timeout_sec: float = 20.0,
    ) -> None:
        if (
            resume_session_ref is not None
            and (
                resume_session_ref.provider != provider_host.provider
                or resume_session_ref.profile != provider_host.profile
            )
        ):
            raise ValueError("resume session provider profile must match provider host")
        self._logger = logger
        self._host = provider_host
        if not isinstance(host_lifetime, ProviderHostLifetime):
            raise ValueError("delivery driver requires ProviderHostLifetime")
        self._host_lifetime = host_lifetime
        self._session_close_timeout_sec = validate_timeout_seconds(
            session_close_timeout_sec,
            field_name="provider session close timeout",
        )
        self._session_config = session_config
        self._resume_session_ref = resume_session_ref
        if not isinstance(status_source, str) or not status_source.strip():
            raise ValueError("delivery driver status_source must not be empty")
        self._status_source = status_source.strip()
        self._drain_status_batch = bool(drain_status_batch)
        self._requested_model: str | None = session_config.model or None
        self._capabilities = initial_capabilities
        self._approval_policy = approval_policy
        self._session_port = None
        self._startup_state: OpenSessionOperation | None = None
        self._startup_ready = False
        self._admission_closed = False
        self._deferred_session_failure: _DeferredSessionFailure | None = None
        self._pending_decisions: list[ApprovalDeliveryReceipt] = []
        self._pending_model_selections: list[
            AgentModelSelection[tuple[AccessPointId, AccessPointId | None]]
        ] = []
        self._pending_effort_selections: list[
            AgentEffortSelection[tuple[AccessPointId, AccessPointId | None]]
        ] = []
        self._event_queue: list[AgentDeliveryEvent] = []
        self._active_request: InteractiveAgentInputRequest | None = None
        self._active_turn_ref: TurnRef | None = None
        self._active_approval_ref: ApprovalRef | None = None
        self._active_elicitation_ref: ElicitationRef | None = None
        self._active_elicitation_request: AgentFormRequest | None = None
        self._completed_approval_refs = BoundedIdentityHistory[ApprovalRef](
            _MAX_COMPLETED_APPROVAL_REFS_PER_TURN
        )
        self._turn_access_point_owners = TurnAccessPointOwners()
        self._model_apply_selection: AgentModelSelection[
            tuple[AccessPointId, AccessPointId | None]
        ] | None = None
        self._effort_apply_selection: AgentEffortSelection[
            tuple[AccessPointId, AccessPointId | None]
        ] | None = None
        self._notice_access_point_fallback: tuple[AccessPointId, AccessPointId | None] | None = None
        self._interrupt_in_flight = False
        self._turn_started = False
        self._poll_progressed = False
        self._terminal_error = ""
        self._fallback_projection = replace(
            unavailable_projection(),
            capabilities=self._capabilities,
        )
        self._inspection_access_points: dict[
            InspectionRef,
            tuple[AccessPointId, AccessPointId | None],
        ] = {}

    def start(self) -> None:
        self._startup_state = self._begin_interactive_start(model=self._requested_model)

    def stop(self) -> None:
        self._logger.event(
            "interactive_driver_stop_requested",
            queue_size=0,
            has_active_request=self._active_request is not None,
            has_active_turn=self._active_turn_ref is not None,
            active_turn_ref=_turn_ref_text(self._active_turn_ref) or None,
            active_pending_approval=self._active_approval_ref is not None,
            startup_ready=self._startup_ready,
            model_apply_in_progress=self._model_apply_selection is not None,
            pending_model_selections=len(self._pending_model_selections),
            effort_apply_in_progress=self._effort_apply_selection is not None,
            pending_effort_selections=len(self._pending_effort_selections),
            interrupt_in_flight=self._interrupt_in_flight,
        )
        self._admission_closed = True
        self._release_provider_resources()
        self._clear_active_turn()
        self._session_port = None
        self._startup_ready = False
        self._deferred_session_failure = None
        self._inspection_access_points.clear()
        self._turn_access_point_owners.clear()
        self._pending_model_selections.clear()
        self._pending_effort_selections.clear()
        self._model_apply_selection = None
        self._effort_apply_selection = None

    def _release_provider_resources(self) -> None:
        startup = self._startup_state
        if startup is not None and not startup.completed:
            startup.cancel("agent session stopped during startup")
        port = self._session_port
        if port is not None:
            self._wait_for_session_close(port)
        if self._host_lifetime is ProviderHostLifetime.DEDICATED:
            stopped = self._host.stop_host(StopHost())
            if not stopped.stopped:
                raise RuntimeError(stopped.error)

    def _wait_for_session_close(self, port: AgentSessionPort) -> None:
        deadline = time.monotonic() + self._session_close_timeout_sec
        while True:
            result = port.close_session(CloseSession())
            if result.outcome is AgentEffectOutcome.COMPLETED:
                return
            if result.outcome is not AgentEffectOutcome.ACCEPTED:
                raise RuntimeError(result.error or "provider session close failed")
            self._host.consume_poll_progress()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("provider session close timed out")
            time.sleep(min(0.005, remaining))

    def start_turn(
        self,
        request: InteractiveAgentInputRequest,
        turn_ref: TurnRef,
    ) -> AgentEffectResult:
        if self._admission_closed or not self._startup_ready or self._session_port is None:
            return AgentEffectResult.rejected("agent session is not ready")
        if self._active_request is not None or self._active_turn_ref is not None:
            return AgentEffectResult.rejected("agent session is busy")
        result = self._session_port.start_turn(StartTurn(turn_ref, request.agent_input))
        if result.outcome is not AgentEffectOutcome.ACCEPTED:
            return result
        self._active_request = request
        self._active_turn_ref = turn_ref
        self._active_approval_ref = None
        self._active_elicitation_ref = None
        self._active_elicitation_request = None
        self._completed_approval_refs.clear()
        self._turn_access_point_owners.remember(turn_ref, request.access_point)
        self._notice_access_point_fallback = request.access_point
        self._interrupt_in_flight = False
        chat_id, thread_id = request.access_point
        self._logger.event(
            "interactive_driver_turn_start_submitted",
            chat_id=chat_id,
            thread_id=thread_id,
            turn_ref_sequence=turn_ref.sequence,
            queue_size=0,
            prompt_preview=request.agent_input.text.strip()[:200],
        )
        return result

    def submit_approval_decision(
        self,
        decision: str,
        *,
        expected_request: AgentApprovalRequest,
    ) -> ApprovalDeliveryReceipt:
        receipt = ApprovalDeliveryReceipt(
            request=expected_request,
            decision=decision,
        )
        preflight = self._approval_preflight(decision, expected_request)
        if preflight.outcome is not AgentEffectOutcome.ACCEPTED:
            receipt.complete(ApprovalDecisionResult.preflight(preflight))
            return receipt
        self._pending_decisions.append(receipt)
        return receipt

    def submit_approval_decision_now(
        self,
        decision: str,
        *,
        expected_request: AgentApprovalRequest,
    ) -> ApprovalDecisionResult:
        if self._admission_closed or self._session_port is None:
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected("no active turn is waiting for approval")
            )
        preflight = self._approval_preflight(decision, expected_request)
        if preflight.outcome is not AgentEffectOutcome.ACCEPTED:
            return ApprovalDecisionResult.preflight(preflight)
        resolution = self._resolve_and_handle_approval(
            decision=decision,
            origin=ApprovalDecisionOrigin.HUMAN,
            decision_source="interactive",
        )
        return resolution

    def submit_elicitation_response(
        self,
        action: ElicitationAction,
        *,
        expected_request: AgentFormRequest,
        content: dict[str, object] | None = None,
    ) -> ElicitationDeliveryReceipt:
        normalized_content = ()
        if action is ElicitationAction.ACCEPT:
            try:
                normalized_content = expected_request.validate_content(content or {})
            except ValueError as exc:
                receipt = ElicitationDeliveryReceipt(expected_request, action)
                receipt.complete(
                    ApprovalDecisionResult.preflight(
                        AgentEffectResult.rejected(str(exc))
                    )
                )
                return receipt
        receipt = ElicitationDeliveryReceipt(
            expected_request,
            action,
            normalized_content,
        )
        if (
            self._admission_closed
            or self._session_port is None
            or self._active_turn_ref != expected_request.turn_ref
            or self._active_elicitation_ref != expected_request.elicitation_ref
            or self._active_elicitation_request != expected_request
        ):
            receipt.complete(
                ApprovalDecisionResult.preflight(
                    AgentEffectResult.rejected("elicitation request is stale")
                )
            )
            return receipt
        try:
            result = self._session_port.resolve_elicitation(
                ResolveElicitation(
                    expected_request.turn_ref,
                    expected_request.elicitation_ref,
                    action,
                    normalized_content,
                )
            )
        except Exception as exc:
            result = ApprovalDecisionResult.provider_call_exception(exc)
        receipt.complete(result)
        if result.delivery_attempted and result.succeeded:
            self._active_elicitation_ref = None
            self._active_elicitation_request = None
        elif result.fatal_delivery_failure:
            self._fail_closed_active_turn(
                "elicitation response delivery failed: "
                f"{result.result.error or result.result.outcome.value}"
            )
        return receipt

    def _approval_preflight(
        self,
        decision: str,
        expected_request: AgentApprovalRequest,
    ) -> AgentEffectResult:
        return approval_decision_preflight(
            expected_request,
            active_turn_ref=self._active_turn_ref,
            active_approval_ref=self._active_approval_ref,
            decision=decision,
        )

    def submit_model_selection(
        self,
        selection: AgentModelSelection[tuple[AccessPointId, AccessPointId | None]],
    ) -> None:
        if self._admission_closed or not self._startup_ready or self._session_port is None:
            self._event_queue.append(
                InteractiveModelApplyResult(
                    chat_id=selection.access_point[0],
                    thread_id=selection.access_point[1],
                    requested_model=selection.selected_model,
                    actual_model="",
                    result="rejected",
                    error=self._terminal_error or "agent session is not ready",
                )
            )
            return
        self._pending_model_selections.append(selection)

    def submit_effort_selection(
        self,
        selection: AgentEffortSelection[tuple[AccessPointId, AccessPointId | None]],
    ) -> None:
        if self._admission_closed or not self._startup_ready or self._session_port is None:
            self._event_queue.append(
                InteractiveEffortApplyResult(
                    chat_id=selection.access_point[0],
                    thread_id=selection.access_point[1],
                    requested_effort=selection.selected_effort,
                    actual_effort="",
                    result="rejected",
                    error=self._terminal_error or "agent session is not ready",
                )
            )
            return
        self._pending_effort_selections.append(selection)

    def has_active_turn(self) -> bool:
        return self._active_request is not None and self._active_turn_ref is not None

    def has_busy_work(self) -> bool:
        return (
            self._active_request is not None
            or self._active_turn_ref is not None
            or self._model_apply_selection is not None
            or self._effort_apply_selection is not None
            or bool(self._pending_decisions)
            or bool(self._pending_model_selections)
            or bool(self._pending_effort_selections)
            or self._deferred_session_failure is not None
        )

    def steer_wait_reason(self, expected_turn_ref: TurnRef) -> str:
        if self._active_turn_ref == expected_turn_ref and not self._turn_started:
            return "provider_turn_pending"
        return ""

    def try_steer_turn(
        self,
        request: InteractiveAgentInputRequest,
        expected_turn_ref: TurnRef,
    ) -> AgentEffectResult:
        if self._admission_closed or not self._startup_ready or self._session_port is None:
            return AgentEffectResult.rejected("agent session is not ready")
        if self._active_request is None or self._active_turn_ref is None:
            return AgentEffectResult.rejected("no active turn")
        if self._active_turn_ref != expected_turn_ref:
            return AgentEffectResult.rejected("active turn no longer matches")
        if self._interrupt_in_flight:
            return AgentEffectResult.rejected("active turn is being interrupted")
        if self._active_approval_ref is not None:
            return AgentEffectResult.rejected("active turn is waiting for approval")
        result = self._session_port.try_steer_turn(
            TrySteerTurn(expected_turn_ref=expected_turn_ref, agent_input=request.agent_input)
        )
        chat_id, thread_id = request.access_point
        self._logger.event(
            "interactive_driver_steer_completed",
            chat_id=chat_id,
            thread_id=thread_id,
            turn_ref=_turn_ref_text(expected_turn_ref),
            turn_ref_sequence=expected_turn_ref.sequence,
            prompt_preview=request.agent_input.text.strip()[:200],
            outcome=result.outcome.value,
            error=result.error or None,
        )
        return result

    def interrupt_active_turn(self) -> None:
        if (
            self._admission_closed
            or self._session_port is None
            or self._active_turn_ref is None
            or self._active_request is None
        ):
            raise RuntimeError("no active turn to interrupt")
        if self._interrupt_in_flight:
            raise RuntimeError("interrupt already requested")
        self._logger.event(
            "interactive_driver_interrupt_requested",
            chat_id=self._active_request.access_point[0],
            thread_id=self._active_request.access_point[1],
            turn_ref=_turn_ref_text(self._active_turn_ref),
        )
        result = self._session_port.interrupt_turn(InterruptTurn(self._active_turn_ref))
        if result.outcome not in {AgentEffectOutcome.ACCEPTED, AgentEffectOutcome.COMPLETED}:
            raise RuntimeError(result.error)
        self._active_approval_ref = None
        self._interrupt_in_flight = True

    def poll_once(self) -> list[AgentDeliveryEvent]:
        return self._poll_ready_events(drain_status_batch=self._drain_status_batch)

    def _poll_ready_events(self, *, drain_status_batch: bool) -> list[AgentDeliveryEvent]:
        self._poll_progressed = False
        if self._active_turn_ref is None:
            self._cancel_pending_approval_deliveries(
                "approval decision cancelled because there is no active turn"
            )
        was_ready = self._startup_ready
        if not self._startup_ready:
            self._maybe_finish_startup()
        else:
            config_apply_started = False
            if (
                self._active_turn_ref is None
                and self._model_apply_selection is None
                and self._effort_apply_selection is None
                and self._pending_model_selections
            ):
                self._maybe_begin_model_apply()
                config_apply_started = self._model_apply_selection is not None
            elif (
                self._active_turn_ref is None
                and self._model_apply_selection is None
                and self._effort_apply_selection is None
                and self._pending_effort_selections
            ):
                self._maybe_begin_effort_apply()
                config_apply_started = self._effort_apply_selection is not None
            if config_apply_started:
                out = list(self._event_queue)
                self._event_queue.clear()
                self._poll_progressed = True
                return out
            if self._active_approval_ref is not None and not self._interrupt_in_flight:
                self._maybe_submit_approval_decision()
            self._poll_session_events()
            while drain_status_batch and self._event_queue and all(
                isinstance(event, InteractiveStatusEvent) for event in self._event_queue
            ):
                if not self._poll_session_events():
                    break
        if self._host.consume_poll_progress():
            self._poll_progressed = True
        if self._startup_ready != was_ready:
            self._poll_progressed = True
        out = list(self._event_queue)
        self._event_queue.clear()
        if out:
            self._poll_progressed = True
        return out

    def acknowledge_event_handoff(self) -> None:
        """Apply destructive session failure only after the event batch has an owner."""
        self._apply_deferred_session_failure()

    def consume_poll_progress(self) -> bool:
        progressed = self._poll_progressed
        self._poll_progressed = False
        return progressed

    def get_terminal_error(self) -> str:
        if self._deferred_session_failure is not None:
            return ""
        if self._terminal_error:
            return self._terminal_error
        return self._host.consume_terminal_error()

    def get_capability_snapshot(self) -> AgentCapabilitySnapshot:
        return self._capabilities

    def get_local_projection(self) -> AgentRuntimeProjection:
        port = self._session_port
        if port is not None:
            return port.get_local_projection()
        return self._fallback_projection

    def request_inspection(
        self,
        inspection_ref: InspectionRef,
        timeout_sec: float,
        access_point: tuple[AccessPointId, AccessPointId | None],
    ) -> AgentEffectResult:
        port = self._session_port
        if self._admission_closed or not self._startup_ready or port is None:
            return AgentEffectResult.rejected("agent session is not ready")
        receipt = port.refresh_inspection(RefreshInspection(inspection_ref, timeout_sec))
        if receipt.outcome is AgentEffectOutcome.ACCEPTED:
            self._inspection_access_points[inspection_ref] = access_point
        return receipt

    def is_ready(self) -> bool:
        return self._startup_ready and not self._admission_closed

    def _begin_interactive_start(self, *, model: str | None) -> OpenSessionOperation:
        config = replace(self._session_config, model=str(model or ""))
        correlation_id = OpenSessionCorrelationId.new()
        if self._resume_session_ref is not None:
            request = ResumeSessionRequest(
                correlation_id=correlation_id,
                config=config,
                session_ref=self._resume_session_ref,
            )
        else:
            request = StartSessionRequest(correlation_id=correlation_id, config=config)
        return self._host.open_session(request)

    def _maybe_finish_startup(self) -> None:
        if self._startup_state is None:
            return
        events = self._startup_state.poll_events()
        if events:
            self._poll_progressed = True
        for event in events:
            if isinstance(event, OpenFailed):
                self._terminal_error = event.error.message
                self._startup_state = None
                self._fallback_projection = projection_with_session_error(
                    self._fallback_projection,
                    event.error.message,
                )
                raise RuntimeError(event.error.message)
            if not isinstance(event, Opened):
                continue
            self._session_port = event.session
            self._startup_ready = True
            self._admission_closed = False
            self._deferred_session_failure = None
            self._capabilities = event.capabilities
            self._fallback_projection = event.session.get_local_projection()
            self._startup_state = None

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
        port = self._session_port
        if port is None:
            result = AgentEffectResult.rejected("agent session is not ready")
        else:
            result = port.set_model(SetModel(requested_model))
        if result.outcome is AgentEffectOutcome.ACCEPTED:
            self._resume_session_ref = None
            self._model_apply_selection = selection
            return
        chat_id, thread_id = selection.access_point
        self._event_queue.append(
            InteractiveModelApplyResult(
                chat_id=chat_id,
                thread_id=thread_id,
                requested_model=selection.selected_model,
                actual_model="",
                result="rejected",
                error=result.error or result.outcome.value,
            )
        )

    def _maybe_begin_effort_apply(self) -> None:
        if not self._pending_effort_selections:
            return
        selection = self._pending_effort_selections.pop(0)
        requested_effort = str(selection.selected_effort or "").strip()
        chat_id, thread_id = selection.access_point
        if not requested_effort:
            self._event_queue.append(
                InteractiveEffortApplyResult(
                    chat_id=chat_id,
                    thread_id=thread_id,
                    requested_effort=selection.selected_effort,
                    actual_effort="",
                    result="rejected",
                    error="effort must be a non-empty string",
                )
            )
            return
        self._logger.event("effort_apply_requested", requested_effort=requested_effort)
        port = self._session_port
        result = (
            port.set_effort(SetEffort(requested_effort))
            if port is not None
            else AgentEffectResult.rejected("agent session is not ready")
        )
        if result.outcome is AgentEffectOutcome.ACCEPTED:
            self._effort_apply_selection = selection
            return
        self._event_queue.append(
            InteractiveEffortApplyResult(
                chat_id=chat_id,
                thread_id=thread_id,
                requested_effort=selection.selected_effort,
                actual_effort="",
                result="rejected",
                error=result.error or result.outcome.value,
            )
        )

    def _maybe_submit_approval_decision(self) -> None:
        if (
            self._session_port is None
            or self._active_request is None
            or self._active_approval_ref is None
            or not self._pending_decisions
        ):
            return
        receipt = self._pending_decisions.pop(0)
        decision = receipt.decision
        expected_request = receipt.request
        self._logger.event(
            "interactive_driver_submit_approval_decision",
            chat_id=self._active_request.access_point[0],
            thread_id=self._active_request.access_point[1],
            decision=decision,
        )
        preflight = approval_decision_preflight(
            expected_request,
            active_turn_ref=self._active_turn_ref,
            active_approval_ref=self._active_approval_ref,
            decision=decision,
        )
        if preflight.outcome is not AgentEffectOutcome.ACCEPTED:
            receipt.complete(ApprovalDecisionResult.preflight(preflight))
            self._logger.event(
                "interactive_driver_stale_approval_decision_ignored",
                reason=preflight.error,
            )
            return
        resolution = self._resolve_and_handle_approval(
            decision=decision,
            origin=ApprovalDecisionOrigin.HUMAN,
            decision_source="interactive",
        )
        if not resolution.delivery_attempted:
            receipt.complete(resolution)
            self._logger.event(
                "interactive_driver_approval_preflight_rejected",
                outcome=resolution.result.outcome.value,
                error=resolution.result.error,
            )
            return
        receipt.complete(resolution)

    def _resolve_and_handle_approval(
        self,
        *,
        decision: str,
        origin: ApprovalDecisionOrigin,
        decision_source: str,
    ) -> ApprovalDecisionResult:
        try:
            resolution = self._resolve_approval(
                decision=decision,
                origin=origin,
                decision_source=decision_source,
            )
            if resolution.delivery_attempted:
                self._handle_approval_resolution(resolution)
            return resolution
        except Exception as exc:
            resolution = ApprovalDecisionResult.provider_call_exception(exc)
            if (
                self._active_request is not None
                and self._active_turn_ref is not None
                and self._session_port is not None
            ):
                self._fail_closed_approval_delivery(resolution)
            return resolution

    def _resolve_approval(
        self,
        *,
        decision: str,
        origin: ApprovalDecisionOrigin,
        decision_source: str,
    ) -> ApprovalDecisionResult:
        port = self._session_port
        turn_ref = self._active_turn_ref
        approval_ref = self._active_approval_ref
        if port is None or turn_ref is None or approval_ref is None:
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected("no active turn is waiting for approval")
            )
        try:
            return port.resolve_approval(
                ResolveApproval(
                    turn_ref,
                    approval_ref,
                    decision,
                    origin=origin,
                    decision_source=decision_source,
                )
            )
        except Exception as exc:
            return ApprovalDecisionResult.provider_call_exception(exc)

    def _handle_approval_resolution(self, resolution: ApprovalDecisionResult) -> None:
        if not resolution.delivery_attempted:
            raise RuntimeError(resolution.result.error)
        if resolution.succeeded:
            approval_ref = self._active_approval_ref
            if approval_ref is None:
                raise AssertionError("successful approval resolution lost active identity")
            evicted = self._completed_approval_refs.remember(approval_ref)
            if evicted is not None:
                self._logger.event(
                    "interactive_driver_approval_dedup_history_evicted",
                    turn_ref_sequence=approval_ref.turn.sequence,
                    evicted_approval_ref=evicted.value,
                )
            self._active_approval_ref = None
            return
        self._fail_closed_approval_delivery(resolution)

    def _fail_closed_approval_delivery(self, resolution: ApprovalDecisionResult) -> None:
        if not resolution.fatal_delivery_failure:
            raise AssertionError("approval fail-close requires a failed provider call")
        result = resolution.result
        error = (
            f"approval decision delivery {result.outcome.value}: "
            f"{result.error or 'provider did not confirm the decision'}"
        )
        turn_ref = self._active_turn_ref
        if turn_ref is None:
            raise AssertionError("approval delivery failure has no active correlated turn")
        self._logger.event(
            "interactive_driver_approval_delivery_failed",
            outcome=result.outcome.value,
            error=result.error or None,
            turn_ref=_turn_ref_text(turn_ref),
        )
        self._fail_closed_active_turn(error)

    def _fail_closed_active_turn(self, error: str) -> None:
        turn_ref = self._active_turn_ref
        port = self._session_port
        if self._active_request is None or turn_ref is None or port is None:
            raise AssertionError("fatal turn failure has no active correlated turn")
        transition = self._apply_terminal_transition(
            signal=DeliveryTerminalSignal.TURN_TERMINAL,
            event_turn_ref=turn_ref,
            outcome=InteractiveTurnOutcome.FAILED,
            error=error,
        )
        if transition.disposition is not DeliveryTerminalDisposition.ACCEPTED:
            raise AssertionError("fatal turn failure was not accepted by terminal owner")
        self._fallback_projection = projection_with_session_error(
            port.get_local_projection(),
            error,
        )
        cleanup_error = ""
        try:
            self._release_provider_resources()
        except Exception as exc:
            cleanup_error = _exception_error(exc)
        if cleanup_error:
            error = f"{error}; cleanup failed: {cleanup_error}"
            self._fallback_projection = projection_with_session_error(
                self._fallback_projection,
                error,
            )
        self._terminal_error = error
        self._session_port = None
        self._startup_state = None
        self._startup_ready = False
        self._admission_closed = True
        self._deferred_session_failure = None
        self._inspection_access_points.clear()
        self._turn_access_point_owners.clear()
        self._pending_model_selections.clear()
        self._pending_effort_selections.clear()
        self._model_apply_selection = None
        self._effort_apply_selection = None
        self._clear_active_turn()

    def _poll_session_events(self) -> bool:
        port = self._session_port
        if port is None:
            return False
        events = port.poll_events()
        for event in events:
            dispatch = dispatch_agent_session_event(event)
            if (
                dispatch.disposition
                is AgentSessionEventDispatchDisposition.DROP_CONTRACT_VIOLATION
            ):
                self._log_agent_session_contract_violation(dispatch)
                continue
            request = self._active_request
            if isinstance(event, SessionStarted):
                self._capabilities = event.capabilities
                self._startup_ready = True
                self._admission_closed = False
                self._terminal_error = ""
                self._fallback_projection = port.get_local_projection()
                selection = self._model_apply_selection
                if selection is not None:
                    self._logger.event(
                        "model_apply_completed",
                        requested_model=selection.selected_model,
                        actual_model=event.effective_model,
                    )
                    self._event_queue.append(
                        InteractiveModelApplyResult(
                            chat_id=selection.access_point[0],
                            thread_id=selection.access_point[1],
                            requested_model=selection.selected_model,
                            actual_model=event.effective_model,
                            result="completed",
                            error="",
                        )
                    )
                    self._model_apply_selection = None
                continue
            if isinstance(event, SessionFailed):
                self._apply_terminal_transition(
                    signal=DeliveryTerminalSignal.SESSION_FAILED,
                    outcome=InteractiveTurnOutcome.FAILED,
                    error=event.error,
                )
                continue
            if isinstance(
                event,
                (ModelSelectionAccepted, ModelSelectionFailed, ModelSelectionUncertain),
            ):
                selection = self._model_apply_selection
                if selection is None or selection.selected_model != event.requested_model:
                    self._logger.event(
                        "stale_model_selection_result_ignored",
                        requested_model=event.requested_model,
                        result_type=type(event).__name__,
                    )
                    continue
                if isinstance(event, ModelSelectionAccepted):
                    self._requested_model = event.requested_model
                    self._logger.event(
                        "model_apply_requested_configuration_accepted",
                        requested_model=event.requested_model,
                    )
                    self._event_queue.append(
                        InteractiveModelApplyResult(
                            chat_id=selection.access_point[0],
                            thread_id=selection.access_point[1],
                            requested_model=event.requested_model,
                            actual_model=port.get_local_projection().session.model,
                            result="requested",
                            error="",
                        )
                    )
                else:
                    self._event_queue.append(
                        InteractiveModelApplyResult(
                            chat_id=selection.access_point[0],
                            thread_id=selection.access_point[1],
                            requested_model=event.requested_model,
                            actual_model=port.get_local_projection().session.model,
                            result=(
                                "uncertain"
                                if isinstance(event, ModelSelectionUncertain)
                                else "rejected"
                            ),
                            error=event.error,
                        )
                    )
                self._model_apply_selection = None
                continue
            if isinstance(
                event,
                (EffortSelectionAccepted, EffortSelectionFailed, EffortSelectionUncertain),
            ):
                selection = self._effort_apply_selection
                if selection is None or selection.selected_effort != event.requested_effort:
                    self._logger.event(
                        "stale_effort_selection_result_ignored",
                        requested_effort=event.requested_effort,
                        result_type=type(event).__name__,
                    )
                    continue
                projection = port.get_local_projection().configuration
                self._event_queue.append(
                    InteractiveEffortApplyResult(
                        chat_id=selection.access_point[0],
                        thread_id=selection.access_point[1],
                        requested_effort=event.requested_effort,
                        actual_effort=projection.current_effort,
                        result=(
                            "requested"
                            if isinstance(event, EffortSelectionAccepted)
                            else "uncertain"
                            if isinstance(event, EffortSelectionUncertain)
                            else "rejected"
                        ),
                        error=(
                            ""
                            if isinstance(event, EffortSelectionAccepted)
                            else event.error
                        ),
                    )
                )
                self._effort_apply_selection = None
                continue
            if isinstance(event, InspectionCompleted):
                access_point = self._inspection_access_points.pop(event.inspection_ref, None)
                if access_point is None:
                    self._logger.event(
                        "stale_inspection_completion_ignored",
                        inspection_ref=event.inspection_ref.value,
                    )
                    continue
                self._event_queue.append(
                    InteractiveInspectionResult(
                        chat_id=access_point[0],
                        thread_id=access_point[1],
                        inspection_ref=event.inspection_ref,
                        projection=event.projection,
                    )
                )
                continue
            if isinstance(event, TurnStarted):
                disposition = turn_started_disposition(
                    active_turn_ref=self._active_turn_ref,
                    event_turn_ref=event.turn_ref,
                    already_started=self._turn_started,
                )
                if disposition is not NonTerminalEventDisposition.ACCEPTED:
                    self._log_nonterminal_event_ignored(
                        event_kind="turn_started",
                        disposition=disposition,
                        turn_ref=event.turn_ref,
                    )
                    continue
                self._turn_started = True
                self._logger.event(
                    "interactive_driver_turn_started",
                    turn_ref_sequence=event.turn_ref.sequence,
                )
                continue
            if isinstance(event, OutputDelta):
                continue
            if isinstance(event, WorkStatusChanged):
                access_point, disposition = self._turn_access_point_owners.resolve(
                    event.turn_ref
                )
                if access_point is None:
                    self._log_nonterminal_event_ignored(
                        event_kind="work_status_changed",
                        disposition=disposition,
                        turn_ref=event.turn_ref,
                    )
                    continue
                delivery_kind = event.kind
                if (
                    event.kind is WorkStatusKind.TURN_STARTED
                    and event.turn_ref != self._active_turn_ref
                ):
                    delivery_kind = WorkStatusKind.NOTICE
                self._queue_event(
                    InteractiveStatusEvent(
                        chat_id=access_point[0],
                        thread_id=access_point[1],
                        status_text=event.status_text,
                        status_ref=event.status_ref,
                        kind=delivery_kind,
                        status_text_kind=event.kind,
                        work=event.work,
                        item_ref=event.item_ref,
                        suppress_status=event.suppress_status,
                        preserve_work_snapshot=event.preserve_work_snapshot,
                        auxiliary=event.auxiliary,
                        turn_ref=event.turn_ref,
                    )
                )
                continue
            if isinstance(event, ProviderNotice):
                access_point = request.access_point if request is not None else self._notice_access_point_fallback
                if access_point is not None:
                    self._event_queue.append(
                        InteractiveOutboundNote(
                            chat_id=access_point[0],
                            thread_id=access_point[1],
                            text=event.text,
                            turn_ref=event.turn_ref,
                        )
                    )
                continue
            if isinstance(event, ApprovalRequested):
                if (
                    event.turn_ref != event.request.turn_ref
                    or event.approval_ref != event.request.approval_ref
                ):
                    raise RuntimeError("agent approval event correlation was lost")
                disposition = approval_requested_disposition(
                    active_turn_ref=self._active_turn_ref,
                    active_approval_ref=self._active_approval_ref,
                    completed_approval_refs=self._completed_approval_refs,
                    event_turn_ref=event.turn_ref,
                    event_approval_ref=event.approval_ref,
                )
                if disposition is not NonTerminalEventDisposition.ACCEPTED:
                    self._log_nonterminal_event_ignored(
                        event_kind="approval_requested",
                        disposition=disposition,
                        turn_ref=event.turn_ref,
                        approval_ref=event.approval_ref,
                    )
                    continue
                if request is None:
                    raise RuntimeError("accepted approval event has no active request")
                self._active_approval_ref = event.approval_ref
                trace = (
                    self._approval_policy.decide_with_trace(event.request)
                    if self._approval_policy is not None
                    and event.request.kind in {
                        AgentApprovalKind.COMMAND,
                        AgentApprovalKind.FILE_CHANGE,
                    }
                    else None
                )
                if trace is not None:
                    log_approval_policy_trace(
                        logger=self._logger,
                        role=None,
                        approval_ref=event.approval_ref.value,
                        approval_kind=event.request.kind,
                        decision=trace.decision,
                        decision_source=trace.source,
                        command=trace.command,
                        affected_paths=trace.affected_paths,
                        regex_trace=trace.regex_trace,
                        approval_notify=lambda text, ap=request.access_point, tr=event.turn_ref: self._event_queue.append(
                            InteractiveOutboundNote(
                                chat_id=ap[0],
                                thread_id=ap[1],
                                text=text,
                                turn_ref=tr,
                            )
                        ),
                    )
                if trace is not None and trace.decision != "human":
                    resolution = self._resolve_and_handle_approval(
                        decision=trace.decision,
                        origin=ApprovalDecisionOrigin.POLICY_AUTO,
                        decision_source=trace.source,
                    )
                    if resolution.delivery_attempted:
                        continue
                    self._logger.event(
                        "interactive_driver_approval_preflight_rejected",
                        outcome=resolution.result.outcome.value,
                        error=resolution.result.error,
                        source=trace.source,
                    )
                self._logger.event(
                    "approval_human_required",
                    approval_ref=event.approval_ref.value,
                    turn_ref_sequence=event.turn_ref.sequence,
                    source=trace.source if trace is not None else "interactive",
                )
                self._event_queue.append(
                    InteractiveApprovalPromptEvent(
                        chat_id=request.access_point[0],
                        thread_id=request.access_point[1],
                        request=event.request,
                    )
                )
                continue
            if isinstance(event, ElicitationRequested):
                if (
                    event.turn_ref != event.request.turn_ref
                    or event.elicitation_ref != event.request.elicitation_ref
                ):
                    raise RuntimeError("agent elicitation event correlation was lost")
                if (
                    event.turn_ref != self._active_turn_ref
                    or self._active_approval_ref is not None
                    or self._active_elicitation_ref is not None
                ):
                    self._log_nonterminal_event_ignored(
                        event_kind="elicitation_requested",
                        disposition=NonTerminalEventDisposition.CONFLICT,
                        turn_ref=event.turn_ref,
                    )
                    self._fail_closed_active_turn(
                        "elicitation responder ownership conflict"
                    )
                    continue
                if request is None:
                    raise RuntimeError("accepted elicitation event has no active request")
                self._active_elicitation_ref = event.elicitation_ref
                self._active_elicitation_request = event.request
                self._event_queue.append(
                    InteractiveElicitationPromptEvent(
                        chat_id=request.access_point[0],
                        thread_id=request.access_point[1],
                        request=event.request,
                    )
                )
                continue
            if isinstance(event, TurnCompleted):
                self._apply_terminal_transition(
                    signal=DeliveryTerminalSignal.TURN_TERMINAL,
                    event_turn_ref=event.turn_ref,
                    outcome=InteractiveTurnOutcome.COMPLETED,
                    output=event.output,
                )
                continue
            if isinstance(event, TurnFailed):
                self._apply_terminal_transition(
                    signal=DeliveryTerminalSignal.TURN_TERMINAL,
                    event_turn_ref=event.turn_ref,
                    outcome=InteractiveTurnOutcome.FAILED,
                    error=event.error,
                    raw_provider_error=event.raw_provider_error,
                )
                continue
            if isinstance(event, TurnInterrupted):
                self._apply_terminal_transition(
                    signal=DeliveryTerminalSignal.TURN_TERMINAL,
                    event_turn_ref=event.turn_ref,
                    interrupted=True,
                )
                continue
            self._log_agent_session_contract_violation(
                dispatch.as_drop_contract_violation(
                    "classified agent session event has no delivery handler"
                )
            )
        return bool(events)

    def _queue_terminal_result(
        self,
        request: InteractiveAgentInputRequest,
        turn_ref: TurnRef,
        *,
        outcome: InteractiveTurnOutcome,
        output: AgentOutput | None = None,
        error: str = "",
        raw_provider_error: RawProviderErrorPayload | None = None,
    ) -> None:
        result = (
            InteractiveAgentResult.failed_result(
                chat_id=request.access_point[0],
                thread_id=request.access_point[1],
                agent_input=request.agent_input,
                error=error,
                turn_ref=turn_ref,
                raw_provider_error=raw_provider_error,
            )
            if outcome is InteractiveTurnOutcome.FAILED
            else InteractiveAgentResult(
                chat_id=request.access_point[0],
                thread_id=request.access_point[1],
                agent_input=request.agent_input,
                output=output,
                turn_ref=turn_ref,
            )
        )
        self._refresh_pending_status_snapshot(request.access_point, turn_ref)
        chat_id, thread_id = request.access_point
        self._logger.event(
            "interactive_driver_turn_failed" if result.failed else "interactive_driver_turn_completed",
            chat_id=chat_id,
            thread_id=thread_id,
            turn_ref_sequence=turn_ref.sequence,
            reply_preview=(result.error if result.failed else result.require_output().text).strip()[:200],
            **raw_provider_error_lifecycle_fields(result.raw_provider_error),
        )
        self._event_queue.append(result)

    def _apply_terminal_transition(
        self,
        *,
        signal: DeliveryTerminalSignal,
        event_turn_ref: TurnRef | None = None,
        outcome: InteractiveTurnOutcome | None = None,
        output: AgentOutput | None = None,
        error: str = "",
        raw_provider_error: RawProviderErrorPayload | None = None,
        interrupted: bool = False,
    ) -> DeliveryTerminalTransition:
        request = self._active_request
        active_turn_ref = self._active_turn_ref
        transition = transition_delivery_terminal(
            self._delivery_terminal_state(),
            signal=signal,
            event_turn_ref=event_turn_ref,
        )
        self._active_turn_ref = transition.state.active_turn_ref
        self._admission_closed = not transition.state.session_available

        accepted = transition.disposition in {
            DeliveryTerminalDisposition.ACCEPTED,
            DeliveryTerminalDisposition.SYNTHESIZED,
        }
        terminal_turn_ref = transition.terminal_turn_ref
        if accepted and terminal_turn_ref is not None and request is not None:
            if interrupted:
                self._event_queue.append(
                    InteractiveInterruptResult(
                        chat_id=request.access_point[0],
                        thread_id=request.access_point[1],
                        result="interrupted",
                        turn_ref=terminal_turn_ref,
                    )
                )
            else:
                if outcome is None:
                    raise AssertionError("terminal result requires an outcome")
                self._queue_terminal_result(
                    request,
                    terminal_turn_ref,
                    outcome=outcome,
                    output=output,
                    error=error,
                    raw_provider_error=raw_provider_error,
                )
            self._clear_active_turn_payload()
        elif signal is DeliveryTerminalSignal.TURN_TERMINAL:
            self._log_terminal_ignored(
                event_turn_ref,
                active_turn_ref=active_turn_ref,
                disposition=transition.disposition,
                terminal_kind=(
                    "interrupted"
                    if interrupted
                    else (outcome.value if outcome else "unknown")
                ),
            )

        if signal is DeliveryTerminalSignal.SESSION_FAILED:
            self._clear_active_turn_payload()
            self._inspection_access_points.clear()
            self._turn_access_point_owners.clear()
            self._notice_access_point_fallback = None
            if self._deferred_session_failure is None:
                # Logical admission is closed above; the port stays attached for this handoff.
                self._deferred_session_failure = _DeferredSessionFailure(
                    str(error).strip() or "agent session failed"
                )
        return transition

    def _log_terminal_ignored(
        self,
        turn_ref: TurnRef | None,
        *,
        active_turn_ref: TurnRef | None,
        disposition: DeliveryTerminalDisposition,
        terminal_kind: str,
    ) -> None:
        if turn_ref is None:
            raise AssertionError("ignored turn terminal requires a TurnRef")
        fields: dict[str, object] = {
            "boundary": "agent_delivery_driver",
            "reason": (
                "unknown_owner"
                if disposition is DeliveryTerminalDisposition.ACCEPTED
                else disposition.value
            ),
            "terminal_kind": terminal_kind,
            "runtime_generation": turn_ref.runtime_generation.value,
            "turn_ref_sequence": turn_ref.sequence,
        }
        if active_turn_ref is not None:
            fields["active_runtime_generation"] = active_turn_ref.runtime_generation.value
            fields["active_turn_ref_sequence"] = active_turn_ref.sequence
        self._logger.event("turn_terminal_ignored", **fields)

    def _delivery_terminal_state(self) -> DeliveryTerminalState:
        return DeliveryTerminalState(
            active_turn_ref=self._active_turn_ref,
            session_available=(
                self._session_port is not None and not self._admission_closed
            ),
        )

    def _apply_deferred_session_failure(self) -> None:
        deferred = self._deferred_session_failure
        if deferred is None:
            return
        port = self._session_port
        projection = (
            port.get_local_projection()
            if port is not None
            else self._fallback_projection
        )
        self._terminal_error = deferred.error
        self._fallback_projection = projection_with_session_error(projection, deferred.error)
        self._session_port = None
        self._startup_state = None
        self._startup_ready = False
        self._deferred_session_failure = None
        selection = self._model_apply_selection
        if selection is not None:
            self._event_queue.append(
                InteractiveModelApplyResult(
                    chat_id=selection.access_point[0],
                    thread_id=selection.access_point[1],
                    requested_model=selection.selected_model,
                    actual_model="",
                    result="rejected",
                    error=deferred.error,
                )
            )
            self._model_apply_selection = None
        effort_selection = self._effort_apply_selection
        if effort_selection is not None:
            self._event_queue.append(
                InteractiveEffortApplyResult(
                    chat_id=effort_selection.access_point[0],
                    thread_id=effort_selection.access_point[1],
                    requested_effort=effort_selection.selected_effort,
                    actual_effort="",
                    result="uncertain",
                    error=deferred.error,
                )
            )
            self._effort_apply_selection = None
        self._poll_progressed = True

    def _refresh_pending_status_snapshot(
        self,
        access_point: tuple[AccessPointId, AccessPointId | None],
        turn_ref: TurnRef,
    ) -> None:
        for event in reversed(self._event_queue):
            if not isinstance(event, InteractiveStatusEvent):
                break
            if (event.chat_id, event.thread_id) != access_point:
                continue
            if event.turn_ref != turn_ref or event.preserve_work_snapshot:
                return
            if event.status_text_kind is None:
                event.status_text_kind = event.kind
            event.kind = WorkStatusKind.TURN_COMPLETED
            event.work = None
            event.item_ref = ""
            return

    def _clear_active_turn(self) -> None:
        self._active_turn_ref = None
        self._clear_active_turn_payload()

    def _clear_active_turn_payload(self) -> None:
        self._active_request = None
        self._active_approval_ref = None
        self._active_elicitation_ref = None
        self._active_elicitation_request = None
        self._completed_approval_refs.clear()
        self._cancel_pending_approval_deliveries(
            "approval decision cancelled because the turn ended"
        )
        self._interrupt_in_flight = False
        self._turn_started = False

    def _cancel_pending_approval_deliveries(self, error: str) -> None:
        while self._pending_decisions:
            receipt = self._pending_decisions.pop(0)
            receipt.complete(
                ApprovalDecisionResult.preflight(
                    AgentEffectResult.rejected(error)
                )
            )

    def _queue_event(self, event: AgentDeliveryEvent) -> None:
        if isinstance(event, InteractiveStatusEvent):
            event_key = status_stream_key(event, semantic_source=self._status_source)
            for index in range(len(self._event_queue) - 1, -1, -1):
                previous = self._event_queue[index]
                if (
                    isinstance(previous, InteractiveStatusEvent)
                    and status_stream_key(
                        previous,
                        semantic_source=self._status_source,
                    ) == event_key
                ):
                    self._event_queue[index] = coalesce_status_events(
                        previous,
                        event,
                        semantic_source=self._status_source,
                    )
                    return
        self._event_queue.append(event)

    def _log_nonterminal_event_ignored(
        self,
        *,
        event_kind: str,
        disposition: NonTerminalEventDisposition,
        turn_ref: TurnRef,
        approval_ref: ApprovalRef | None = None,
    ) -> None:
        fields: dict[str, object] = {
            "boundary": "agent_delivery_driver",
            "event_kind": event_kind,
            "disposition": disposition.value,
            "runtime_generation": turn_ref.runtime_generation.value,
            "turn_ref_sequence": turn_ref.sequence,
        }
        if approval_ref is not None:
            fields["approval_ref"] = approval_ref.value
        active_turn_ref = self._active_turn_ref
        if active_turn_ref is not None:
            fields["active_runtime_generation"] = active_turn_ref.runtime_generation.value
            fields["active_turn_ref_sequence"] = active_turn_ref.sequence
        self._logger.event("agent_nonterminal_event_ignored", **fields)

    def _log_agent_session_contract_violation(
        self,
        dispatch: AgentSessionEventDispatch,
    ) -> None:
        if (
            dispatch.disposition
            is not AgentSessionEventDispatchDisposition.DROP_CONTRACT_VIOLATION
        ):
            raise ValueError("contract violation logger requires a dropped dispatch")
        self._logger.event(
            "contract_violation",
            boundary="agent_session_event_dispatch",
            disposition=dispatch.disposition.value,
            event_kind=dispatch.kind.value,
            event_type=dispatch.event_type,
            reason=dispatch.reason,
        )


def _turn_ref_text(turn_ref: TurnRef | None) -> str:
    if turn_ref is None:
        return ""
    return f"orc:{turn_ref.runtime_generation.value}:{turn_ref.sequence}"


def _exception_error(exc: Exception) -> str:
    return str(exc).strip() or type(exc).__name__
