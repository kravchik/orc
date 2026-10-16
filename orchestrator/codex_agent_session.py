"""Codex implementation of the provider-neutral agent session port."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import replace
from datetime import datetime, timezone
from typing import Callable

from orchestrator import clock
from orchestrator.adapters import (
    CodexInspectionState,
    CodexJsonRpcSession,
    InteractiveTurnState,
    JsonRpcDeliveryUncertain,
    JsonRpcRemoteError,
    JsonRpcTransportTerminal,
)
from orchestrator.agent_capabilities import AgentCapability, AgentCapabilitySnapshot
from orchestrator.agent_identity import (
    InspectionRef,
    ProviderKind,
    ProviderProfileId,
    ProviderSessionRef,
    TurnRef,
    WorkStatusRef,
)
from orchestrator.agent_inspection import (
    AgentBackgroundSnapshot,
    AgentContextSnapshot,
    AgentRuntimeProjection,
    AgentSessionSnapshot,
    AgentWorkItemSnapshot,
    AgentWorkSnapshot,
    SnapshotAvailability,
)
from orchestrator.agent_provider import Opened, session_event_from_open_result
from orchestrator.agent_session import (
    ApprovalDecisionOrigin,
    ApprovalDecisionResult,
    AgentEffectResult,
    AgentSessionEffect,
    AgentSessionEvent,
    ApprovalRequested,
    CloseSession,
    InterruptTurn,
    InspectionCompleted,
    OutputDelta,
    ProviderNotice,
    RefreshInspection,
    ResolveApproval,
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
    unsupported_effect_result,
)
from orchestrator.codex_approval import (
    CodexApprovalCorrelations,
    CodexApprovalOccurrenceDisposition,
    normalize_codex_approval_request,
)
from orchestrator.processes import LifecycleLogger
from orchestrator.protocol_status import (
    format_item_status_summary,
    format_protocol_status,
    is_completed_agent_commentary,
)


_CODEX_ALWAYS_AUXILIARY_STATUS_METHODS = frozenset(
    {
        "codex/event/task_started",
        "codex/event/mcp_startup_update",
        "codex/event/mcp_startup_complete",
    }
)
_CODEX_TRANSPORT_TERMINATED = "Codex transport terminated"


class CodexWorkRefMap:
    """Bound native item IDs while exposing only ORC-owned work references."""

    def __init__(self, *, max_entries: int = 512) -> None:
        if not isinstance(max_entries, int) or isinstance(max_entries, bool) or max_entries <= 0:
            raise ValueError("work ref map max entries must be a positive integer")
        self._max_entries = max_entries
        self._refs: OrderedDict[str, str] = OrderedDict()
        self._next_sequence = 1

    def resolve(self, native_item_id: str) -> str:
        normalized = str(native_item_id or "").strip()
        if not normalized:
            raise ValueError("native item id must not be empty")
        existing = self._refs.pop(normalized, None)
        if existing is not None:
            self._refs[normalized] = existing
            return existing
        work_ref = f"work:{self._next_sequence}"
        self._next_sequence += 1
        self._refs[normalized] = work_ref
        while len(self._refs) > self._max_entries:
            self._refs.popitem(last=False)
        return work_ref


class CodexAgentSessionAdapter:
    """Maps an already-opened Codex session to typed effects and events."""

    def __init__(
        self,
        *,
        session: CodexJsonRpcSession,
        begin_model_change: Callable[[str], object],
        poll_model_change: Callable[
            [object],
            tuple[CodexJsonRpcSession, Opened] | None,
        ],
        close_handler: Callable[[], None],
        consume_host_failure: Callable[[], str],
        logger: LifecycleLogger,
        role: str,
    ) -> None:
        if not str(session.thread_id or "").strip():
            raise ValueError("CodexAgentSessionAdapter requires an opened session")
        self._session = session
        self._begin_model_change = begin_model_change
        self._poll_model_change = poll_model_change
        self._close_handler = close_handler
        self._consume_host_failure = consume_host_failure
        self._logger = logger
        self._role = role
        self._active_effect: StartTurn | None = None
        self._active_turn: InteractiveTurnState | None = None
        self._turn_started_emitted = False
        self._active_status_turn_ref: TurnRef | None = None
        self._active_work_status_ref: WorkStatusRef | None = None
        self._work_status_refs: dict[str, WorkStatusRef] = {}
        self._next_work_status_sequence = 1
        self._work_refs = CodexWorkRefMap()
        self._emitted_output_delta_count = 0
        self._approval_correlations = CodexApprovalCorrelations()
        self._model_change: object | None = None
        self._events: list[AgentSessionEvent] = []
        self._native_turn_refs: dict[str, TurnRef] = {}
        self._inspections: dict[InspectionRef, tuple[CodexInspectionState, float]] = {}
        self._projection = self._build_projection()
        self._activated = False
        self._closed = False

    def get_capability_snapshot(self) -> AgentCapabilitySnapshot:
        return self._session.get_capability_snapshot()

    def get_local_projection(self) -> AgentRuntimeProjection:
        self._projection = self._build_projection(
            session=self._projection.session,
        )
        return self._projection

    def refresh_inspection(self, effect: RefreshInspection) -> AgentEffectResult:
        unsupported = self._unsupported(effect)
        if unsupported is not None:
            return unsupported
        self._activate()
        if self._closed:
            return AgentEffectResult.rejected("agent session is closed")
        if effect.inspection_ref in self._inspections:
            return AgentEffectResult.rejected("inspection correlation is already active")
        try:
            state = self._session.begin_thread_inspection()
        except Exception as exc:
            return AgentEffectResult.rejected(_error_text(exc))
        self._inspections[effect.inspection_ref] = (
            state,
            clock.monotonic() + effect.timeout_sec,
        )
        return AgentEffectResult.accepted()

    def start_turn(self, effect: StartTurn) -> AgentEffectResult:
        unsupported = self._unsupported(effect)
        if unsupported is not None:
            return unsupported
        self._activate()
        if self._closed:
            return AgentEffectResult.rejected("agent session is closed")
        if self._active_turn is not None or self._model_change is not None:
            return AgentEffectResult.rejected("agent session is busy")
        try:
            turn = self._session.begin_interactive_turn(
                effect.agent_input,
                approval_notify_callback=lambda text: self._events.append(
                    ProviderNotice(text=str(text), turn_ref=effect.turn_ref)
                ),
            )
        except Exception as exc:
            return AgentEffectResult.rejected(_error_text(exc))
        self._active_effect = effect
        self._active_turn = turn
        native_turn_id = str(turn.turn_id or "").strip()
        if native_turn_id:
            self._remember_native_turn(native_turn_id, effect.turn_ref)
        self._turn_started_emitted = False
        self._active_status_turn_ref = None
        self._active_work_status_ref = None
        self._emitted_output_delta_count = 0
        self._approval_correlations.clear()
        return AgentEffectResult.accepted()

    def try_steer_turn(self, effect: TrySteerTurn) -> AgentEffectResult:
        unsupported = self._unsupported(effect)
        if unsupported is not None:
            return unsupported
        active_effect = self._active_effect
        active_turn = self._active_turn
        if active_effect is None or active_turn is None:
            return AgentEffectResult.rejected("no active turn")
        if active_effect.turn_ref != effect.expected_turn_ref:
            return AgentEffectResult.rejected("active turn no longer matches")
        if active_turn.pending_approval is not None:
            return AgentEffectResult.rejected("active turn is waiting for approval")
        try:
            self._session.submit_interactive_turn_steer(active_turn, effect.agent_input)
        except JsonRpcRemoteError as exc:
            if exc.code == -32601:
                return AgentEffectResult.unsupported(AgentCapability.STEER_ACTIVE_TURN)
            return AgentEffectResult.rejected(_error_text(exc))
        except JsonRpcDeliveryUncertain as exc:
            return AgentEffectResult.uncertain(_error_text(exc))
        return AgentEffectResult.accepted()

    def interrupt_turn(self, effect: InterruptTurn) -> AgentEffectResult:
        unsupported = self._unsupported(effect)
        if unsupported is not None:
            return unsupported
        active_effect = self._active_effect
        active_turn = self._active_turn
        if active_effect is None or active_turn is None:
            return AgentEffectResult.rejected("no active turn")
        if active_effect.turn_ref != effect.turn_ref:
            return AgentEffectResult.rejected("active turn no longer matches")
        try:
            self._session.submit_interactive_turn_interrupt(active_turn)
        except Exception as exc:
            return AgentEffectResult.rejected(_error_text(exc))
        active_turn.pending_approval = None
        active_turn.pending_approval_emitted = False
        self._discard_active_approval()
        return AgentEffectResult.accepted()

    def resolve_approval(self, effect: ResolveApproval) -> ApprovalDecisionResult:
        unsupported = self._unsupported(effect)
        if unsupported is not None:
            return ApprovalDecisionResult.preflight(unsupported)
        active_effect = self._active_effect
        active_turn = self._active_turn
        if active_effect is None or active_turn is None:
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected("no active turn")
            )
        if effect.turn_ref != active_effect.turn_ref:
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected("approval turn no longer matches")
            )
        correlation = self._approval_correlations.resolve(
            turn_ref=effect.turn_ref,
            approval_ref=effect.approval_ref,
        )
        if correlation is None:
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected("approval request no longer matches")
            )
        if not correlation.request.supports_decision(effect.decision):
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected(
                    f"approval decision is not supported: {effect.decision}"
                )
            )
        try:
            pending_occurrences = (
                self._session.drain_pending_interactive_approval_occurrences(
                    active_turn,
                    fail_closed_on_overflow=True,
                )
            )
            for native_request in pending_occurrences:
                occurrence = self._approval_correlations.approval_ref_for(
                    turn_ref=effect.turn_ref,
                    native_request_token=native_request.req_id,
                )
                if occurrence.disposition is CodexApprovalOccurrenceDisposition.DUPLICATE:
                    approval_ref = occurrence.approval_ref
                    if approval_ref is None:
                        raise AssertionError(
                            "duplicate Codex approval occurrence requires ApprovalRef"
                        )
                    duplicate_request = normalize_codex_approval_request(
                        native_request,
                        turn_ref=effect.turn_ref,
                        approval_ref=approval_ref,
                        capabilities=self.get_capability_snapshot(),
                    )
                    self._events.append(
                        ApprovalRequested(
                            turn_ref=effect.turn_ref,
                            approval_ref=approval_ref,
                            request=duplicate_request,
                        )
                    )
                    continue
                self._session.reject_concurrent_interactive_approval_request(
                    request_token=native_request.req_id,
                    reason="another approval occurrence is already pending",
                )
                self._logger.event(
                    "codex_approval_request_ignored",
                    role=self._role,
                    disposition="capacity",
                    reason="pending_approval_occurrence",
                    id=native_request.req_id,
                    active_turn_ref_sequence=effect.turn_ref.sequence,
                )
            self._session.submit_interactive_approval_decision(
                active_turn,
                expected_request_token=correlation.native_request.req_id,
                decision=effect.decision,
                via=(
                    "policy_auto"
                    if effect.origin is ApprovalDecisionOrigin.POLICY_AUTO
                    else "human"
                ),
                decision_source=effect.decision_source,
            )
        except Exception as exc:
            return ApprovalDecisionResult.provider_call_exception(exc)
        self._approval_correlations.discard(effect.approval_ref)
        return ApprovalDecisionResult.provider_call(AgentEffectResult.accepted())

    def set_model(self, effect: SetModel) -> AgentEffectResult:
        unsupported = self._unsupported(effect)
        if unsupported is not None:
            return unsupported
        self._activate()
        if self._closed:
            return AgentEffectResult.rejected("agent session is closed")
        if self._active_turn is not None or self._model_change is not None:
            return AgentEffectResult.rejected("agent session is busy")
        try:
            state = self._begin_model_change(effect.model)
        except Exception as exc:
            return AgentEffectResult.rejected(_error_text(exc))
        self._complete_inspections_with_error("agent session reopened for model change")
        self._model_change = state
        return AgentEffectResult.accepted()

    def set_effort(self, effect: SetEffort) -> AgentEffectResult:
        result = self._unsupported(effect)
        if result is None:
            raise AssertionError("Codex adapter unexpectedly supports SetEffort")
        return result

    def close_session(self, effect: CloseSession) -> AgentEffectResult:
        unsupported = self._unsupported(effect)
        if unsupported is not None:
            return unsupported
        if self._closed:
            return AgentEffectResult.completed()
        try:
            self._close_handler()
        except Exception as exc:
            return AgentEffectResult.rejected(_error_text(exc))
        self._closed = True
        if self._active_effect is not None:
            self._events.append(TurnInterrupted(turn_ref=self._active_effect.turn_ref))
        if self._model_change is not None:
            self._events.append(SessionFailed(error="agent session closed during model change"))
        self._complete_inspections_with_error("agent session closed")
        self._clear_turn()
        self._model_change = None
        return AgentEffectResult.completed()

    def poll_events(self) -> list[AgentSessionEvent]:
        if not self._closed:
            self._activate()
            failure = self._consume_host_failure()
            if failure:
                if self._active_effect is not None:
                    self._events.append(
                        TurnFailed(turn_ref=self._active_effect.turn_ref, error=failure)
                    )
                self._fail_session(failure)
            else:
                self._poll_inspections()
            if not self._closed and self._model_change is not None:
                self._poll_pending_model_change()
            elif not self._closed and self._active_turn is not None and self._active_effect is not None:
                self._poll_active_turn()
            elif not self._closed:
                try:
                    self._session.poll_background_protocol_message(timeout_sec=0.0)
                except JsonRpcTransportTerminal:
                    self._fail_session(
                        self._consume_host_failure() or _CODEX_TRANSPORT_TERMINATED
                    )
        out = list(self._events)
        self._events.clear()
        return out

    def _unsupported(self, effect: AgentSessionEffect) -> AgentEffectResult | None:
        return unsupported_effect_result(effect, self.get_capability_snapshot())

    def _activate(self) -> None:
        if self._activated:
            return
        self._session.set_protocol_item_callback(self._on_protocol_item)
        self._session.set_protocol_status_callback(self._on_protocol_status)
        self._activated = True

    def _poll_pending_model_change(self) -> None:
        state = self._model_change
        if state is None:
            return
        try:
            replacement = self._poll_model_change(state)
        except Exception as exc:
            self._fail_session(_error_text(exc))
            return
        if replacement is None:
            return
        replacement_transport, opened = replacement
        self._session = replacement_transport
        self._native_turn_refs.clear()
        self._work_status_refs.clear()
        self._active_status_turn_ref = None
        self._active_work_status_ref = None
        self._activated = False
        self._activate()
        self._model_change = None
        startup_event = session_event_from_open_result(opened)
        if not isinstance(startup_event, SessionStarted):
            raise AssertionError("opened model change must normalize to SessionStarted")
        self._events.append(startup_event)

    def _fail_session(self, error: str) -> None:
        failure = str(error).strip() or "agent session failed"
        try:
            self._close_handler()
        except Exception as exc:
            failure = f"{failure}; cleanup failed: {_error_text(exc)}"
        self._clear_turn()
        self._complete_inspections_with_error(failure)
        self._model_change = None
        self._closed = True
        self._events.append(SessionFailed(error=failure))

    def _poll_inspections(self) -> None:
        now = clock.monotonic()
        for inspection_ref, (state, deadline) in list(self._inspections.items()):
            if now >= deadline:
                self._session.cancel_thread_inspection(state)
                self._finish_inspection(
                    inspection_ref,
                    self._build_projection(session_error="timeout"),
                )
                continue
            try:
                progress = self._session.poll_thread_inspection(state)
            except Exception as exc:
                self._finish_inspection(
                    inspection_ref,
                    self._build_projection(session_error=_error_text(exc)),
                )
                continue
            if progress is None or not progress.completed:
                continue
            if progress.error:
                projection = self._build_projection(session_error=progress.error)
            else:
                projection = self._build_projection(metadata=progress.metadata)
            self._finish_inspection(inspection_ref, projection)

    def _finish_inspection(
        self,
        inspection_ref: InspectionRef,
        projection: AgentRuntimeProjection,
    ) -> None:
        if self._inspections.pop(inspection_ref, None) is None:
            return
        self._projection = projection
        self._events.append(InspectionCompleted(inspection_ref, projection))

    def _complete_inspections_with_error(self, error: str) -> None:
        for inspection_ref, (state, _deadline) in list(self._inspections.items()):
            self._session.cancel_thread_inspection(state)
            self._finish_inspection(
                inspection_ref,
                self._build_projection(session_error=error),
            )

    def _build_projection(
        self,
        *,
        metadata: dict[str, object] | None = None,
        session: AgentSessionSnapshot | None = None,
        session_error: str = "",
    ) -> AgentRuntimeProjection:
        capabilities = self.get_capability_snapshot()
        raw_metadata = (
            dict(metadata)
            if isinstance(metadata, dict)
            else self._session.get_cached_thread_metadata()
        )
        session_ref = ProviderSessionRef(
            ProviderKind.CODEX,
            str(self._session.thread_id or "").strip(),
            ProviderProfileId.CODEX_NATIVE,
        )
        if session_error:
            session_snapshot = replace(
                session or _codex_session_snapshot(raw_metadata, session_ref),
                availability=SnapshotAvailability.ERROR,
                error=session_error,
            )
        else:
            session_snapshot = _codex_session_snapshot(raw_metadata, session_ref)
        if session_snapshot.availability is SnapshotAvailability.AVAILABLE:
            session_snapshot = replace(
                session_snapshot,
                model=session_snapshot.model or str(self._session.thread_model or "").strip(),
                backend=session_snapshot.backend or capabilities.backend.name,
            )

        work_items = tuple(
            AgentWorkItemSnapshot(
                kind=str(row.get("type") or "item").strip() or "item",
                item_ref=self._work_ref(row.get("id")),
                state=str(row.get("status") or "").strip(),
                details=tuple(
                    part.strip()
                    for part in str(row.get("detail") or "").split(";")
                    if part.strip()
                ),
            )
            for row in self._session.get_item_status_snapshot()
        )
        context_raw = self._session.get_cached_context_usage()
        if not capabilities.supports(AgentCapability.CONTEXT_USAGE):
            context = AgentContextSnapshot(SnapshotAvailability.UNSUPPORTED)
        elif context_raw:
            context = AgentContextSnapshot(
                SnapshotAvailability.AVAILABLE,
                used_tokens=_optional_int(context_raw.get("used_tokens")),
                model_context_window=_optional_int(context_raw.get("model_context_window")),
            )
        else:
            context = AgentContextSnapshot(SnapshotAvailability.UNKNOWN)
        background_available = capabilities.supports(
            AgentCapability.BACKGROUND_TASK_INVENTORY
        ) or capabilities.supports(AgentCapability.SHELL_PROCESS_INVENTORY)
        return AgentRuntimeProjection(
            capabilities=capabilities,
            session=session_snapshot,
            work=AgentWorkSnapshot(
                SnapshotAvailability.AVAILABLE,
                active_turn_ref=self._active_effect.turn_ref if self._active_effect else None,
                items=work_items,
            ),
            context=context,
            background=AgentBackgroundSnapshot(
                SnapshotAvailability.AVAILABLE
                if background_available
                else SnapshotAvailability.UNSUPPORTED
            ),
        )

    def _poll_active_turn(self) -> None:
        effect = self._active_effect
        turn = self._active_turn
        if effect is None or turn is None:
            return
        while True:
            try:
                progress = self._session.poll_interactive_turn(turn, timeout_sec=0.0)
            except Exception as exc:
                self._events.append(
                    TurnFailed(turn_ref=effect.turn_ref, error=_error_text(exc))
                )
                self._clear_turn()
                return
            if turn.turn_id and not self._turn_started_emitted:
                self._remember_native_turn(str(turn.turn_id), effect.turn_ref)
                self._events.append(TurnStarted(turn_ref=effect.turn_ref))
                self._turn_started_emitted = True
            self._emit_output_deltas(effect, turn)
            if progress.kind == "idle":
                return
            if progress.kind == "progressed":
                if self._events:
                    return
                continue
            if progress.kind == "approval_required" and progress.approval_request is not None:
                native_request = progress.approval_request
                native_turn_id = str(native_request.params.get("turnId") or "").strip()
                approval_turn_ref = self._approval_turn_ref(
                    effect=effect,
                    turn=turn,
                    native_turn_id=native_turn_id,
                )
                if approval_turn_ref is None:
                    self._session.reject_interactive_approval_request(
                        turn,
                        expected_request_token=native_request.req_id,
                        reason="approval request references an unknown turn",
                    )
                    self._logger.event(
                        "codex_approval_request_ignored",
                        role=self._role,
                        disposition="stale",
                        reason="unknown_native_turn",
                        id=native_request.req_id,
                        native_turn_id=native_turn_id,
                        active_turn_ref_sequence=effect.turn_ref.sequence,
                    )
                    return
                occurrence = self._approval_correlations.approval_ref_for(
                    turn_ref=approval_turn_ref,
                    native_request_token=native_request.req_id,
                )
                if occurrence.disposition is CodexApprovalOccurrenceDisposition.CAPACITY:
                    self._session.reject_concurrent_interactive_approval_request(
                        request_token=native_request.req_id,
                        reason="another approval occurrence is already pending",
                    )
                    self._logger.event(
                        "codex_approval_request_ignored",
                        role=self._role,
                        disposition="capacity",
                        reason="pending_approval_occurrence",
                        id=native_request.req_id,
                        active_turn_ref_sequence=effect.turn_ref.sequence,
                    )
                    return
                approval_ref = occurrence.approval_ref
                if approval_ref is None:
                    raise AssertionError("accepted Codex approval occurrence requires ApprovalRef")
                request = normalize_codex_approval_request(
                    native_request,
                    turn_ref=approval_turn_ref,
                    approval_ref=approval_ref,
                    capabilities=self.get_capability_snapshot(),
                )
                duplicate = (
                    occurrence.disposition
                    is CodexApprovalOccurrenceDisposition.DUPLICATE
                )
                if approval_turn_ref != effect.turn_ref or duplicate:
                    disposition = "stale" if approval_turn_ref != effect.turn_ref else "duplicate"
                    if not duplicate:
                        self._session.reject_interactive_approval_request(
                            turn,
                            expected_request_token=native_request.req_id,
                            reason=f"{disposition} approval request",
                        )
                    self._events.append(
                        ApprovalRequested(
                            turn_ref=approval_turn_ref,
                            approval_ref=approval_ref,
                            request=request,
                        )
                    )
                    return
                self._logger.event(
                    "approval_requested",
                    role=self._role,
                    id=native_request.req_id,
                    method=native_request.method,
                    approval_ref=approval_ref.value,
                    turn_ref_sequence=approval_turn_ref.sequence,
                    kind=request.kind.value,
                    command=request.command,
                    cwd=request.cwd,
                    affected_paths=list(request.affected_paths),
                    diagnostics=dict(request.diagnostics),
                )
                self._approval_correlations.register(
                    native_request=native_request,
                    request=request,
                )
                self._events.append(
                    ApprovalRequested(
                        turn_ref=approval_turn_ref,
                        approval_ref=approval_ref,
                        request=request,
                    )
                )
                return
            if progress.kind == "completed":
                try:
                    output = progress.require_completed_output()
                except Exception as exc:
                    self._events.append(
                        TurnFailed(turn_ref=effect.turn_ref, error=_error_text(exc))
                    )
                    self._clear_turn()
                    return
                self._events.append(
                    TurnCompleted(
                        turn_ref=effect.turn_ref,
                        output=output,
                    )
                )
                self._clear_turn()
                return
            if progress.kind == "interrupted":
                self._events.append(TurnInterrupted(turn_ref=effect.turn_ref))
                self._clear_turn()
                return
            if progress.kind == "failed":
                self._events.append(
                    TurnFailed(
                        turn_ref=effect.turn_ref,
                        error=progress.error or "Codex turn failed",
                        raw_provider_error=progress.raw_provider_error,
                    )
                )
                self._clear_turn()
                return
            self._events.append(
                TurnFailed(
                    turn_ref=effect.turn_ref,
                    error=f"unknown Codex turn progress: {progress.kind!r}",
                )
            )
            self._clear_turn()
            return

    def _on_protocol_item(self, _role: str, changed_item_id: str) -> None:
        info = self._session.get_last_item_apply_info() or {}
        if str(info.get("kind") or "") == "item_ignored_forgotten_turn":
            return
        native_turn_id = str(info.get("turn_id") or "").strip()
        turn_ref, status_ref = self._status_target(native_turn_id)
        if turn_ref is None:
            return
        snapshot = self._session.get_item_status_snapshot(
            turn_id=native_turn_id or None,
        )
        effect = self._active_effect
        self._events.append(
            WorkStatusChanged(
                turn_ref=turn_ref,
                status_text=format_item_status_summary(
                    snapshot,
                    changed_item_id=changed_item_id,
                ),
                status_ref=status_ref,
                kind=WorkStatusKind.ITEM_CHANGED,
                work=_codex_work_snapshot(snapshot, turn_ref, self._work_ref),
                item_ref=self._work_ref(changed_item_id),
                suppress_status=_suppress_codex_status(info),
                preserve_work_snapshot=is_completed_agent_commentary(
                    snapshot,
                    changed_item_id=changed_item_id,
                ),
                origin=(
                    effect.agent_input.origin
                    if effect is not None and effect.turn_ref == turn_ref
                    else None
                ),
                runtime="codex-app-server",
                model_backend=self.get_capability_snapshot().backend.name,
            )
        )

    def _work_ref(self, native_item_id: object) -> str:
        normalized = str(native_item_id or "").strip()
        if not normalized:
            return ""
        return self._work_refs.resolve(normalized)

    def _emit_output_deltas(
        self,
        effect: StartTurn,
        turn: InteractiveTurnState,
    ) -> None:
        new_deltas = turn.ordered_output_deltas[self._emitted_output_delta_count :]
        self._emitted_output_delta_count = len(turn.ordered_output_deltas)
        self._events.extend(
            OutputDelta(turn_ref=effect.turn_ref, text=delta)
            for delta in new_deltas
            if delta
        )

    def _on_protocol_status(self, _role: str, method: str, params: dict) -> None:
        info = self._session.get_last_item_apply_info() or {}
        if str(info.get("kind") or "") == "item_ignored_forgotten_turn":
            native_turn_id = str(info.get("turn_id") or "").strip()
            turn_ref, status_ref = self._status_target(native_turn_id)
            if turn_ref is not None:
                effect = self._active_effect
                self._events.append(
                    WorkStatusChanged(
                        turn_ref=turn_ref,
                        status_text=format_protocol_status(method=method, params=params),
                        status_ref=status_ref,
                        kind=WorkStatusKind.NOTICE,
                        suppress_status=True,
                        origin=(
                            effect.agent_input.origin
                            if effect is not None and effect.turn_ref == turn_ref
                            else None
                        ),
                        runtime="codex-app-server",
                        model_backend=self.get_capability_snapshot().backend.name,
                    )
                )
            return
        native_turn_id = _codex_native_turn_id(method, params)
        had_active_status = (
            self._active_status_turn_ref is not None
            or self._active_work_status_ref is not None
        )
        turn_ref, status_ref = self._status_target(
            native_turn_id,
            activate=method == "turn/started",
        )
        if turn_ref is None:
            return
        effect = self._active_effect
        if method == "turn/started" and status_ref is None:
            self._events.append(TurnStarted(turn_ref=turn_ref))
            if effect is not None and effect.turn_ref == turn_ref:
                self._turn_started_emitted = True
        kind = {
            "turn/started": WorkStatusKind.TURN_STARTED,
            "turn/completed": WorkStatusKind.TURN_COMPLETED,
        }.get(method, WorkStatusKind.NOTICE)
        self._events.append(
            WorkStatusChanged(
                turn_ref=turn_ref,
                status_text=format_protocol_status(method=method, params=params),
                status_ref=status_ref,
                kind=kind,
                auxiliary=(
                    method in _CODEX_ALWAYS_AUXILIARY_STATUS_METHODS
                    or (not native_turn_id and not had_active_status)
                    or (effect is None and method == "codex/event/agent_message")
                ),
                origin=(
                    effect.agent_input.origin
                    if effect is not None and effect.turn_ref == turn_ref
                    else None
                ),
                runtime="codex-app-server",
                model_backend=self.get_capability_snapshot().backend.name,
            )
        )

        if method == "turn/completed":
            self._clear_active_status_target(turn_ref, status_ref)

    def _status_target(
        self,
        native_turn_id: str,
        *,
        activate: bool = False,
    ) -> tuple[TurnRef | None, WorkStatusRef | None]:
        normalized = str(native_turn_id or "").strip()
        effect = self._active_effect
        if not normalized:
            if self._active_status_turn_ref is not None:
                return self._active_status_turn_ref, None
            if self._active_work_status_ref is not None and effect is not None:
                return effect.turn_ref, self._active_work_status_ref
            return (effect.turn_ref, None) if effect is not None else (None, None)

        active_turn = self._active_turn
        if effect is not None and active_turn is not None:
            active_native_turn_id = str(active_turn.turn_id or "").strip()
            if active_native_turn_id and active_native_turn_id == normalized:
                self._remember_native_turn(normalized, effect.turn_ref)
        mapped = self._native_turn_refs.get(normalized)
        if mapped is not None:
            turn_ref, status_ref = mapped, None
        elif effect is not None:
            status_ref = self._work_status_refs.get(normalized)
            if status_ref is None:
                status_ref = WorkStatusRef(
                    effect.turn_ref.runtime_generation,
                    self._next_work_status_sequence,
                )
                self._next_work_status_sequence += 1
                self._work_status_refs[normalized] = status_ref
                while len(self._work_status_refs) > 8:
                    oldest = next(iter(self._work_status_refs))
                    self._work_status_refs.pop(oldest, None)
            turn_ref = effect.turn_ref
        else:
            return None, None

        activate_current_turn = (
            activate
            and effect is not None
            and turn_ref == effect.turn_ref
        )
        if activate_current_turn or (
            not activate
            and self._active_status_turn_ref is None
            and self._active_work_status_ref is None
        ):
            self._active_status_turn_ref = turn_ref if status_ref is None else None
            self._active_work_status_ref = status_ref
        return turn_ref, status_ref

    def _clear_active_status_target(
        self,
        turn_ref: TurnRef,
        status_ref: WorkStatusRef | None,
    ) -> None:
        if status_ref is not None:
            if self._active_work_status_ref == status_ref:
                self._active_work_status_ref = None
                self._active_status_turn_ref = None
            return
        if self._active_status_turn_ref == turn_ref:
            self._active_status_turn_ref = None
            self._active_work_status_ref = None

    def _remember_native_turn(self, native_turn_id: str, turn_ref: TurnRef) -> None:
        normalized = str(native_turn_id or "").strip()
        if not normalized:
            return
        self._native_turn_refs.pop(normalized, None)
        self._native_turn_refs[normalized] = turn_ref
        while len(self._native_turn_refs) > 8:
            oldest = next(iter(self._native_turn_refs))
            self._native_turn_refs.pop(oldest, None)

    def _approval_turn_ref(
        self,
        *,
        effect: StartTurn,
        turn: InteractiveTurnState,
        native_turn_id: str,
    ) -> TurnRef | None:
        if not native_turn_id:
            return effect.turn_ref
        if str(turn.turn_id or "").strip() == native_turn_id:
            self._remember_native_turn(native_turn_id, effect.turn_ref)
            return effect.turn_ref
        return self._native_turn_refs.get(native_turn_id)

    def _discard_active_approval(self) -> None:
        self._approval_correlations.clear()

    def _clear_turn(self) -> None:
        self._discard_active_approval()
        self._active_effect = None
        self._active_turn = None
        self._turn_started_emitted = False
        self._active_status_turn_ref = None
        self._active_work_status_ref = None
        self._emitted_output_delta_count = 0


def _error_text(exc: Exception) -> str:
    return str(exc).strip() or type(exc).__name__


def _codex_work_snapshot(
    rows: list[dict[str, str]],
    turn_ref: TurnRef,
    work_ref: Callable[[object], str],
) -> AgentWorkSnapshot:
    return AgentWorkSnapshot(
        SnapshotAvailability.AVAILABLE,
        active_turn_ref=turn_ref,
        items=tuple(
            AgentWorkItemSnapshot(
                kind=str(row.get("type") or "item").strip() or "item",
                item_ref=work_ref(row.get("id")),
                state=str(row.get("status") or "").strip(),
                details=tuple(
                    part.strip()
                    for part in str(row.get("detail") or "").split(";")
                    if part.strip()
                ),
            )
            for row in rows
        ),
    )


def _suppress_codex_status(info: dict[str, object]) -> bool:
    if str(info.get("kind") or "").strip() == "item_ignored_forgotten_turn":
        return True
    late_distance = info.get("late_distance")
    return (
        str(info.get("kind") or "").strip() == "item_changed"
        and isinstance(late_distance, int)
        and late_distance >= 5
    )


def _codex_native_turn_id(method: str, params: dict | None) -> str:
    if not isinstance(params, dict):
        return ""
    if method == "codex/event/agent_message":
        native_turn_id = str(params.get("id") or "").strip()
        if native_turn_id:
            return native_turn_id
    turn = params.get("turn")
    if isinstance(turn, dict):
        native_turn_id = str(turn.get("id") or "").strip()
        if native_turn_id:
            return native_turn_id
    for key in ("turnId", "turn_id"):
        native_turn_id = str(params.get(key) or "").strip()
        if native_turn_id:
            return native_turn_id
    message = params.get("msg")
    if isinstance(message, dict):
        for key in ("turnId", "turn_id"):
            native_turn_id = str(message.get(key) or "").strip()
            if native_turn_id:
                return native_turn_id
    return ""


def _codex_session_snapshot(
    metadata: dict[str, object],
    session_ref: ProviderSessionRef,
) -> AgentSessionSnapshot:
    raw_status = metadata.get("status")
    status = ""
    active_flags: tuple[str, ...] = ()
    if isinstance(raw_status, str):
        status = raw_status.strip()
    elif isinstance(raw_status, dict):
        status = str(raw_status.get("type") or "").strip()
        raw_flags = raw_status.get("activeFlags")
        if isinstance(raw_flags, list):
            active_flags = tuple(
                item.strip()
                for item in raw_flags
                if isinstance(item, str) and item.strip()
            )
    return AgentSessionSnapshot(
        SnapshotAvailability.AVAILABLE,
        session_ref=session_ref,
        name=str(metadata.get("name") or "").strip(),
        status=status,
        active_flags=active_flags,
        loaded=metadata.get("loaded") if isinstance(metadata.get("loaded"), bool) else None,
        ephemeral=(
            metadata.get("ephemeral")
            if isinstance(metadata.get("ephemeral"), bool)
            else None
        ),
        backend=str(metadata.get("modelProvider") or metadata.get("provider") or "").strip(),
        model=str(metadata.get("model") or "").strip(),
        created_at=_timestamp_text(metadata.get("createdAt")),
        updated_at=_timestamp_text(metadata.get("updatedAt")),
        preview=str(metadata.get("preview") or "").strip(),
    )


def _timestamp_text(value: object) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat().replace(
                "+00:00", "Z"
            )
        except (OverflowError, OSError, ValueError):
            return ""
    return ""


def _optional_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value
