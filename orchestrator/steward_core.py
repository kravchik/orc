from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Protocol

from orchestrator import clock
from orchestrator.access_point_common import (
    AccessPointKey,
    access_point_sort_key,
)
from orchestrator.approval import ApprovalRequest
from orchestrator.approval_delegation import (
    build_approval_delegation_prompt,
    format_approval_request_copy,
    parse_approval_delegation_decision,
)
from orchestrator.approval_target import HUMAN_APPROVAL_TARGET
from orchestrator.codex_sessions import find_codex_cli_thread_name, list_codex_cli_sessions
from orchestrator.delivery_errors import extract_http_code
from orchestrator.inspect_text import append_active_work_lines, format_thread_metadata_lines
from orchestrator.interactive_driver_events import (
    InteractiveAgentResult,
    InteractiveApprovalPromptEvent,
    InteractiveInterruptResult,
    InteractiveOutboundNote,
    InteractiveRuntimeFailure,
    InteractiveStartupResult,
    InteractiveStatusEvent,
)
from orchestrator.local_command_journal import (
    LocalCommandOutcome,
    format_local_command_journal_context,
)
from orchestrator.processes import LifecycleLogger
from orchestrator.routing_decision import (
    RoutingDeliverHuman,
    RoutingDispatch,
    RoutingReject,
    decide_routing_response,
)
from orchestrator.runtime_snapshot import (
    AccessPointRuntimeSnapshot,
    build_runtime_snapshot,
    format_runtime_snapshot_context,
)
from orchestrator.routing_envelope import (
    HUMAN_ADDRESS,
    RoutingEnvelope,
    RoutingEnvelopeError,
    format_human_routing_envelope,
    format_routing_envelope,
)
from orchestrator.steward_actions import (
    build_action_loop_terminal_prompt,
    build_action_result_prompt,
    build_steward_action_fingerprint,
    execute_approval_target_action,
    execute_steward_actions,
    parse_steward_response,
)
from orchestrator.steward_commands import (
    ApproverCommand,
    RoutingIdentityCommand,
    extract_fallback_command,
    is_local_immediate_command,
    parse_approver_command,
    parse_grunt_command,
    parse_shaman_command,
)
from orchestrator.binding_address import (
    ROUTING_MODE_GRUNT,
    ROUTING_MODE_SHAMAN,
    ResolvedRoutingIdentity,
)
from orchestrator.steward_inbound import (
    StewardApprovalDecision,
    StewardApprovalDetailsRequest,
    StewardInboundText,
)
from orchestrator.steward_state import PersistedAccessPointState
from orchestrator.steward_runtime_support import (
    AgentDeliveryLane,
    PendingApprovalDelegation,
    PendingRoutedRequest,
    PendingStewardOperation,
    RoutedHandoff,
    RoutedReplyTo,
    RoutingFailure,
    StewardDeliveryLane,
    StewardDriverEvent,
)


class StewardAccessPointAdapter(Protocol):
    def build_help_text(self, *, state: str) -> str: ...
    def build_status_text(
        self,
        *,
        snapshot: AccessPointRuntimeSnapshot,
    ) -> str: ...
    def register_approval(self, *, access_point: AccessPointKey, source: str, request: ApprovalRequest) -> Any: ...
    def preprocess_inbound(
        self,
        *,
        inbound: StewardInboundText,
        pending_approval: Any | None,
        handle_approval_action: Callable[[StewardApprovalDecision | StewardApprovalDetailsRequest], bool],
        handle_invalid_pending_approval_input: Callable[[], bool],
    ) -> bool: ...
    def send_approval_details(self, pending_approval: Any) -> None: ...
    def send_invalid_approval_reply(self, pending_approval: Any) -> None: ...
    def cancel_pending_approval_prompt(self, pending_approval: Any) -> None: ...
    def drop_pending_status_updates(
        self,
        *,
        access_point: AccessPointKey,
        include_steward: bool = False,
    ) -> None: ...
    def queue_text_reply(
        self,
        *,
        access_point: AccessPointKey,
        text: str,
        source: str,
        kind: Any,
        on_sent: Callable[[], None],
        on_failed: Callable[[Exception], None],
    ) -> None: ...
    def queue_routing_copy(
        self,
        *,
        access_point: AccessPointKey,
        text: str,
        kind: Any,
        on_sent: Callable[[], None],
        on_failed: Callable[[Exception], None],
    ) -> None: ...
    def queue_text_reply_with_result(
        self,
        *,
        access_point: AccessPointKey,
        text: str,
        source: str,
        kind: Any,
        on_sent: Callable[[int | None], None],
        on_failed: Callable[[Exception], None],
    ) -> None: ...
    def queue_text_reply_edit(
        self,
        *,
        access_point: AccessPointKey,
        message_id: int,
        text: str,
        source: str,
        kind: Any,
        on_sent: Callable[[], None],
        on_failed: Callable[[Exception], None],
    ) -> None: ...
    def send_outbound_note(
        self,
        *,
        access_point: AccessPointKey,
        source: str,
        text: str,
        kind: Any | None = None,
    ) -> None: ...
    def decorate_reply(self, *, text: str, source: str) -> str: ...
    def status_runtime_for(self, *, access_point: AccessPointKey, source: str) -> tuple[Any, Any]: ...


@dataclass(frozen=True)
class StewardCoreKinds:
    reply: Any
    command: Any
    session: Any
    warning: Any
    restore: Any
    steward_status_source: str
    agent_status_source: str


@dataclass(frozen=True)
class StewardCoreHooks:
    approval_human_response_event: str
    response_sent_event: str
    turn_error_event: str
    access_point_fields: Callable[[AccessPointKey], dict[str, Any]]
    write_output_line: Callable[[AccessPointKey, str], str]
    on_pre_approval_decision: Callable[[Any], None] | None = None
    on_fallback_command: Callable[[AccessPointKey, str, bool], None] | None = None
    on_bound_route: Callable[[AccessPointKey, str], None] | None = None


@dataclass
class PendingInterruptRequest:
    access_point: AccessPointKey
    source: str
    ack_message_id: int | None = None
    final_text: str | None = None
    outcome: LocalCommandOutcome | None = None
    delivery_failed: bool = False


@dataclass
class PendingAgentStartupNotice:
    access_point: AccessPointKey
    operation: str
    thread_name: str = ""
    runtime_thread_id: str = ""
    agent_id: str = ""
    cwd: str = ""
    ack_message_id: int | None = None
    final_text: str | None = None
    outcome: LocalCommandOutcome | None = None
    startup_result: InteractiveStartupResult | None = None
    startup_persisted: bool = True
    delivery_failed: bool = False
    edit_queued: bool = False


class StewardCore:
    _MAX_ROUTING_REPAIR_ATTEMPTS = 1
    _DEFAULT_ROUTING_QUEUE_CAPACITY = 256
    _AGENT_METADATA_PERSIST_RETRY_SEC = 5.0

    @staticmethod
    def _persisted_mutation_result(
        *,
        code: str,
        persisted: bool,
        **fields: Any,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "ok": persisted,
            "code": code if persisted else f"{code}_not_persisted",
            "persisted": persisted,
            **fields,
        }
        if not persisted:
            result["applied_in_memory"] = True
        return result

    def __init__(
        self,
        *,
        logger: LifecycleLogger,
        writer: Callable[[str], None],
        runtime: StewardDeliveryLane,
        agent_runtime: AgentDeliveryLane,
        access_point_adapter: StewardAccessPointAdapter,
        sessions_root: str | Path | None,
        persisted_state_by_access_point: dict[AccessPointKey, PersistedAccessPointState],
        pending_restore_greeting: set[AccessPointKey],
        persist_registry: Callable[[str, AccessPointKey | None], bool],
        drop_persisted_state: Callable[[AccessPointKey], bool],
        clear_status_runtimes: Callable[[AccessPointKey, bool], None],
        split_status_after_approval: Callable[[AccessPointKey, str], None],
        kinds: StewardCoreKinds,
        hooks: StewardCoreHooks,
        routing_queue_capacity: int = _DEFAULT_ROUTING_QUEUE_CAPACITY,
    ) -> None:
        self._logger = logger
        self._writer = writer
        self._runtime = runtime
        self._agent_runtime = agent_runtime
        self._access_point_adapter = access_point_adapter
        self._sessions_root = sessions_root
        self._persisted_state_by_access_point = persisted_state_by_access_point
        self._pending_restore_greeting = pending_restore_greeting
        self._persist_registry = persist_registry
        self._drop_persisted_state = drop_persisted_state
        self._clear_status_runtimes = clear_status_runtimes
        self._split_status_after_approval = split_status_after_approval
        self._kinds = kinds
        self._hooks = hooks
        self._pending_text_updates: list[StewardInboundText] = []
        self._active_operations: dict[AccessPointKey, PendingStewardOperation] = {}
        self._pending_approvals: dict[AccessPointKey, Any] = {}
        self._approval_delegations_by_target: dict[AccessPointKey, PendingApprovalDelegation] = {}
        self._pending_startup_notice_context: dict[AccessPointKey, str] = {}
        self._pending_interrupts: dict[AccessPointKey, PendingInterruptRequest] = {}
        self._pending_agent_startups: dict[AccessPointKey, PendingAgentStartupNotice] = {}
        self._pending_local_command_deliveries: dict[AccessPointKey, int] = {}
        self._agent_metadata_persist_retry_at: dict[AccessPointKey, float] = {}
        self._thread_name_by_id: dict[str, str] = {}
        self._routing_queue: list[PendingRoutedRequest] = []
        self._routing_queue_capacity = max(1, int(routing_queue_capacity))
        self._next_routing_sequence = 1

    def _queue_local_reply(
        self,
        *,
        access_point: AccessPointKey,
        text: str,
        source: str,
        kind: Any,
    ) -> None:
        self._access_point_adapter.queue_text_reply(
            access_point=access_point,
            text=text,
            source=source,
            kind=kind,
            on_sent=lambda: None,
            on_failed=lambda exc, ap=access_point: self._logger.event(
                "steward_local_reply_delivery_failed",
                **self._hooks.access_point_fields(ap),
                source=source,
                kind=str(kind),
                error=str(exc),
                error_type=type(exc).__name__,
                http_code=extract_http_code(exc),
            ),
        )

    def _queue_local_command_reply(
        self,
        *,
        access_point: AccessPointKey,
        outcome: LocalCommandOutcome,
        text: str,
        kind: Any,
    ) -> None:
        self._pending_local_command_deliveries[access_point] = (
            self._pending_local_command_deliveries.get(access_point, 0) + 1
        )

        def _record(user_delivery: str) -> None:
            try:
                self._complete_local_command(
                    access_point=access_point,
                    outcome=outcome,
                    user_delivery=user_delivery,
                )
            finally:
                self._finish_local_command_delivery(access_point)

        def _on_failed(exc: Exception) -> None:
            self._logger.event(
                "steward_local_reply_delivery_failed",
                **self._hooks.access_point_fields(access_point),
                source="steward",
                kind=str(kind),
                error=str(exc),
                error_type=type(exc).__name__,
                http_code=extract_http_code(exc),
            )
            _record("failed")

        try:
            self._access_point_adapter.queue_text_reply(
                access_point=access_point,
                text=text,
                source="steward",
                kind=kind,
                on_sent=lambda: _record("sent"),
                on_failed=_on_failed,
            )
        except Exception:
            self._finish_local_command_delivery(access_point)
            raise

    def _finish_local_command_delivery(self, access_point: AccessPointKey) -> None:
        remaining = self._pending_local_command_deliveries.get(access_point, 0) - 1
        if remaining > 0:
            self._pending_local_command_deliveries[access_point] = remaining
        else:
            self._pending_local_command_deliveries.pop(access_point, None)

    def _complete_local_command(
        self,
        *,
        access_point: AccessPointKey,
        outcome: LocalCommandOutcome,
        user_delivery: str,
    ) -> None:
        state = self._persisted_state_by_access_point.get(access_point)
        if state is None:
            state = PersistedAccessPointState(project_cwd="", agent=None)
            self._persisted_state_by_access_point[access_point] = state
        overflow_before = state.local_command_journal.overflow_dropped_count
        event = state.local_command_journal.append(
            outcome=outcome,
            user_delivery=user_delivery,
        )
        overflow_added = state.local_command_journal.overflow_dropped_count - overflow_before
        if overflow_added > 0:
            self._logger.event(
                "local_command_journal_overflow",
                **self._hooks.access_point_fields(access_point),
                dropped_count=overflow_added,
                pending_dropped_count=state.local_command_journal.overflow_dropped_count,
                dropped_through_event_id=(
                    state.local_command_journal.overflow_dropped_through_event_id
                ),
            )
        persisted = self._persist_registry("local_command_journal_append", access_point)
        self._logger.event(
            "local_command_journal_appended",
            **self._hooks.access_point_fields(access_point),
            event_id=event["event_id"],
            command=event["command"],
            result_ok=bool(outcome.result.get("ok")),
            user_delivery=user_delivery,
            persisted=persisted,
        )

    def _queue_routing_access_point_copy(
        self,
        *,
        access_point: AccessPointKey,
        text: str,
        kind: Any,
        sender_address: str,
        target_address: str,
        turn_id: str,
        repair_attempt: int,
        copy_role: str = "sender",
    ) -> None:
        correlation = {
            **self._routing_access_point_fields(access_point, prefix=copy_role),
            "sender_address": sender_address,
            "target_address": target_address,
            "turn_id": turn_id,
            "repair_attempt": repair_attempt,
            "copy_role": copy_role,
        }
        self._logger.event("routing_access_point_copy_queued", **correlation)

        def _on_sent() -> None:
            decorated = self._access_point_adapter.decorate_reply(text=text, source="agent")
            self._logger.event(
                self._hooks.response_sent_event,
                **self._hooks.access_point_fields(access_point),
                text=decorated,
            )
            self._writer(self._hooks.write_output_line(access_point, text))
            self._logger.event("routing_access_point_copy_delivered", **correlation)

        def _on_failed(exc: Exception) -> None:
            self._logger.event(
                "routing_access_point_copy_failed",
                **correlation,
                error=str(exc),
                error_type=type(exc).__name__,
                http_code=extract_http_code(exc),
            )

        try:
            self._access_point_adapter.queue_routing_copy(
                access_point=access_point,
                text=text,
                kind=kind,
                on_sent=_on_sent,
                on_failed=_on_failed,
            )
        except Exception as exc:
            _on_failed(exc)

    def _queue_interrupt_reply(
        self,
        *,
        access_point: AccessPointKey,
        text: str,
        on_sent: Callable[[int | None], None],
        on_failed: Callable[[Exception], None],
    ) -> None:
        self._access_point_adapter.queue_text_reply_with_result(
            access_point=access_point,
            text=text,
            source="steward",
            kind=self._kinds.command,
            on_sent=on_sent,
            on_failed=on_failed,
        )

    def _queue_interrupt_reply_edit(
        self,
        *,
        access_point: AccessPointKey,
        message_id: int,
        text: str,
        on_sent: Callable[[], None],
        on_failed: Callable[[Exception], None],
    ) -> None:
        self._access_point_adapter.queue_text_reply_edit(
            access_point=access_point,
            message_id=message_id,
            text=text,
            source="steward",
            kind=self._kinds.command,
            on_sent=on_sent,
            on_failed=on_failed,
        )

    def _emit_internal_action_notes(
        self,
        *,
        access_point: AccessPointKey,
        action_results: list[dict[str, Any]],
    ) -> None:
        for item in action_results:
            if not bool(item.get("ok")):
                continue
            action_type = str(item.get("type") or "").strip().upper()
            if action_type == "LIST_RESUMABLE":
                cwd = str(item.get("cwd") or "").strip()
                count = int(item.get("count") or 0)
                suffix = f" in {cwd}" if cwd else ""
                text = f"🧑‍✈️ Completed LIST_RESUMABLE{suffix}: {count} found"
            else:
                continue
            self._access_point_adapter.send_outbound_note(
                access_point=access_point,
                source="steward",
                text=text,
            )

    @property
    def pending_approval(self) -> Any | None:
        if len(self._pending_approvals) == 1:
            return next(iter(self._pending_approvals.values()))
        return None

    def pending_approval_for(self, access_point: AccessPointKey) -> Any | None:
        return self._pending_approvals.get(access_point)

    def iter_pending_approvals(self) -> list[Any]:
        return list(self._pending_approvals.values())

    def active_operation_for(self, access_point: AccessPointKey) -> PendingStewardOperation | None:
        return self._active_operations.get(access_point)

    @property
    def pending_queue_size(self) -> int:
        return len(self._pending_text_updates)

    def is_idle(self) -> bool:
        return (
            not self._active_operations
            and not self._pending_text_updates
            and not self._pending_approvals
            and not self._routing_queue
            and not self._pending_local_command_deliveries
            and not self._agent_runtime.has_pending_startup()
        )

    def enqueue_inbound(self, inbound: StewardInboundText) -> None:
        self._pending_text_updates.append(inbound)

    def _load_restore_sessions(
        self,
        access_point: AccessPointKey,
    ) -> tuple[str, str, list[Any], Any | None, list[Any]]:
        restored = self._persisted_state_by_access_point.get(access_point)
        if restored is None:
            return "", "", [], None, []
        agent = restored.agent if isinstance(restored.agent, dict) else {}
        thread_id = str(agent.get("thread_id") or "")
        cwd = str(restored.project_cwd or agent.get("cwd") or "").strip()
        if not cwd:
            return "", "", [], None, []
        sessions = list_codex_cli_sessions(
            project_cwd=cwd,
            sessions_root=self._sessions_root,
            limit=20,
        )
        self._remember_sessions(sessions)
        current = next((item for item in sessions if item.session_id == thread_id), None) if thread_id else None
        others = [item for item in sessions if item.session_id != thread_id][:5]
        return cwd, thread_id, sessions, current, others

    def build_startup_restore_notice(self, access_point: AccessPointKey) -> str | None:
        cwd, thread_id, _sessions, current, others = self._load_restore_sessions(access_point)
        if not cwd:
            return None
        lines = [
            "Context restored after restart.",
            f"Folder: {cwd}",
            f"Current session: {_format_restore_session_label_with_id(current, fallback_session_id=thread_id)}",
            "Other recent sessions in this folder:",
        ]
        if others:
            lines.extend(f"- {_format_restore_session_label_with_id(item)}" for item in others)
        else:
            lines.append("- none")
        lines.append(
            "Next: tell me what to do — continue the latest session, pick another one, "
            "switch folders, create a new session, make a checkout, and so on."
        )
        return "\n".join(lines)

    def emit_startup_restore_notices(self) -> None:
        pending = sorted(self._pending_restore_greeting, key=access_point_sort_key)
        self._pending_restore_greeting.clear()
        for access_point in pending:
            notice = self.build_startup_restore_notice(access_point)
            if not notice:
                continue
            self._logger.event(
                "access_point_restore_notice_enqueued",
                **self._hooks.access_point_fields(access_point),
                restored_runtime_state=self._agent_runtime.runtime_state(access_point),
            )
            self._access_point_adapter.send_outbound_note(
                access_point=access_point,
                source="steward",
                text=notice,
                kind=self._kinds.restore,
            )
            self._pending_startup_notice_context[access_point] = notice

    def _consume_startup_notice_context(self, access_point: AccessPointKey) -> str | None:
        return self._pending_startup_notice_context.pop(access_point, None)

    def handle_approval_action(self, action: StewardApprovalDecision | StewardApprovalDetailsRequest) -> bool:
        if isinstance(action, StewardApprovalDetailsRequest):
            pending = self._pending_approvals.get(action.access_point)
            if pending is not None:
                self._logger.event(
                    "steward_approval_details_requested",
                    **self._hooks.access_point_fields(pending.access_point),
                    route_target=pending.source,
                )
                self._access_point_adapter.send_approval_details(pending)
            return True
        if not isinstance(action, StewardApprovalDecision):
            raise AssertionError(f"unsupported steward approval action: {action!r}")
        pending = self._pending_approvals.get(action.access_point)
        if pending is None:
            return True
        if self._hooks.on_pre_approval_decision is not None:
            self._hooks.on_pre_approval_decision(pending)
        self._split_status_after_approval(pending.access_point, action.decision)
        fields = self._hooks.access_point_fields(pending.access_point)
        self._logger.event(
            self._hooks.approval_human_response_event,
            **fields,
            id=pending.request.req_id,
            response=action.decision,
            via=action.via,
            route_target=pending.source,
        )
        if hasattr(self._access_point_adapter, "cancel_pending_approval_prompt"):
            self._access_point_adapter.cancel_pending_approval_prompt(pending)
        if pending.source == "agent":
            self._agent_runtime.submit_approval_decision(pending.access_point, action.decision)
        else:
            self._runtime.submit_approval_decision(pending.access_point, action.decision)
        self._pending_approvals.pop(action.access_point, None)
        return True

    def handle_invalid_pending_approval_input(self, access_point: AccessPointKey) -> bool:
        pending = self._pending_approvals.get(access_point)
        if pending is None:
            return False
        self._logger.event(
            "steward_invalid_approval_input",
            **self._hooks.access_point_fields(pending.access_point),
            route_target=pending.source,
        )
        self._access_point_adapter.send_invalid_approval_reply(pending)
        return True

    def _drop_pending_inputs_for_access_point(self, access_point: AccessPointKey) -> int:
        before = len(self._pending_text_updates)
        self._pending_text_updates = [
            inbound for inbound in self._pending_text_updates if inbound.access_point != access_point
        ]
        return before - len(self._pending_text_updates)

    def _interrupt_final_text(self, *, success: bool) -> str:
        if success:
            return "interrupted."
        return "interrupt failed: turn already finished or request was rejected."

    def _maybe_finish_pending_interrupt_notice(self, access_point: AccessPointKey) -> None:
        pending = self._pending_interrupts.get(access_point)
        if pending is None:
            return
        if pending.final_text is None or pending.outcome is None:
            return
        if pending.delivery_failed:
            self._finalize_pending_interrupt(access_point, "failed")
            return
        if pending.ack_message_id is None:
            return
        message_id = int(pending.ack_message_id)
        final_text = str(pending.final_text)
        self._queue_interrupt_reply_edit(
            access_point=access_point,
            message_id=message_id,
            text=final_text,
            on_sent=lambda ap=access_point: self._finalize_pending_interrupt(ap, "sent"),
            on_failed=lambda exc, ap=access_point: self._fail_pending_interrupt_edit(ap, exc),
        )

    def _finalize_pending_interrupt(self, access_point: AccessPointKey, user_delivery: str) -> None:
        pending = self._pending_interrupts.pop(access_point, None)
        if pending is not None and pending.outcome is not None:
            self._complete_local_command(
                access_point=access_point,
                outcome=pending.outcome,
                user_delivery=user_delivery,
            )

    def _fail_pending_interrupt_edit(self, access_point: AccessPointKey, exc: Exception) -> None:
        self._logger.event(
            "steward_interrupt_edit_failed",
            **self._hooks.access_point_fields(access_point),
            error=str(exc),
            error_type=type(exc).__name__,
            http_code=extract_http_code(exc),
        )
        self._finalize_pending_interrupt(access_point, "failed")

    def _fail_pending_interrupt_notice(self, access_point: AccessPointKey, exc: Exception) -> None:
        pending = self._pending_interrupts.get(access_point)
        if pending is None:
            return
        self._logger.event(
            "steward_interrupt_notice_send_failed",
            **self._hooks.access_point_fields(access_point),
            error=str(exc),
            error_type=type(exc).__name__,
            http_code=extract_http_code(exc),
        )
        pending.delivery_failed = True
        self._maybe_finish_pending_interrupt_notice(access_point)

    def _handle_interrupt_command(self, access_point: AccessPointKey) -> bool:
        target_source: str | None = None
        interrupt_lane: StewardDeliveryLane | AgentDeliveryLane | None = None
        if self._agent_runtime.has_active_turn(access_point):
            target_source = "agent"
            interrupt_lane = self._agent_runtime
        elif self._runtime.has_active_turn(access_point):
            target_source = "steward"
            interrupt_lane = self._runtime
        if target_source is None or interrupt_lane is None:
            self._queue_local_command_reply(
                access_point=access_point,
                outcome=LocalCommandOutcome(
                    command="/interrupt",
                    result={"ok": False, "code": "no_active_turn"},
                ),
                text="no active turn to interrupt.",
                kind=self._kinds.command,
            )
            return True
        dropped = self._drop_pending_inputs_for_access_point(access_point)
        if dropped > 0:
            self._logger.event(
                "steward_interrupt_pending_inputs_dropped",
                **self._hooks.access_point_fields(access_point),
                dropped=dropped,
                route_target=target_source,
            )
        pending = PendingInterruptRequest(
            access_point=access_point,
            source=target_source,
        )
        self._pending_interrupts[access_point] = pending
        self._queue_interrupt_reply(
            access_point=access_point,
            text="interrupt requested.",
            on_sent=lambda message_id, ap=access_point: self._on_interrupt_notice_sent(ap, message_id),
            on_failed=lambda exc, ap=access_point: self._fail_pending_interrupt_notice(ap, exc),
        )
        try:
            interrupt_lane.interrupt_active_turn(access_point)
        except Exception as exc:
            self._logger.event(
                "steward_interrupt_request_failed",
                **self._hooks.access_point_fields(access_point),
                route_target=target_source,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            pending.final_text = self._interrupt_final_text(success=False)
            pending.outcome = LocalCommandOutcome(
                command="/interrupt",
                result={
                    "ok": False,
                    "code": "request_failed",
                    "target": target_source,
                },
            )
            self._maybe_finish_pending_interrupt_notice(access_point)
        return True

    def _on_interrupt_notice_sent(self, access_point: AccessPointKey, message_id: int | None) -> None:
        pending = self._pending_interrupts.get(access_point)
        if pending is None:
            return
        pending.ack_message_id = int(message_id) if isinstance(message_id, int) else None
        self._maybe_finish_pending_interrupt_notice(access_point)

    def _begin_agent_startup_notice(
        self,
        access_point: AccessPointKey,
        *,
        operation: str,
        initial_text: str = "runtime agent starting...",
        thread_name: str = "",
        runtime_thread_id: str = "",
        agent_id: str = "",
        cwd: str = "",
    ) -> None:
        pending = PendingAgentStartupNotice(
            access_point=access_point,
            operation=operation,
            thread_name=thread_name,
            runtime_thread_id=runtime_thread_id,
            agent_id=agent_id,
            cwd=cwd,
        )
        self._pending_agent_startups[access_point] = pending
        self._access_point_adapter.queue_text_reply_with_result(
            access_point=access_point,
            text=initial_text,
            source="steward",
            kind=self._kinds.session,
            on_sent=lambda message_id, notice=pending: self._on_agent_startup_notice_sent(
                notice, message_id
            ),
            on_failed=lambda exc, notice=pending: self._fail_agent_startup_notice_send(notice, exc),
        )

    def _on_agent_startup_notice_sent(
        self,
        pending: PendingAgentStartupNotice,
        message_id: int | None,
    ) -> None:
        if not isinstance(message_id, int):
            exc = RuntimeError("startup lifecycle notice did not return a delivery receipt")
            self._log_agent_startup_notice_failure(pending, exc, phase="send")
            pending.delivery_failed = True
            self._maybe_finish_agent_startup_notice(pending)
            return
        pending.ack_message_id = message_id
        self._logger.event(
            "agent_startup_lifecycle_notice_sent",
            **self._hooks.access_point_fields(pending.access_point),
            operation=pending.operation,
            thread_name=pending.thread_name,
            runtime_thread_id=pending.runtime_thread_id,
            delivery_id=pending.ack_message_id,
            delivery_outcome="sent",
        )
        self._maybe_finish_agent_startup_notice(pending)

    def _maybe_finish_agent_startup_notice(self, pending: PendingAgentStartupNotice) -> None:
        if pending.final_text is None:
            return
        if pending.delivery_failed:
            self._finish_agent_startup_notice(pending, user_delivery="failed")
            return
        if pending.ack_message_id is None:
            return
        if pending.edit_queued:
            return
        pending.edit_queued = True
        self._access_point_adapter.queue_text_reply_edit(
            access_point=pending.access_point,
            message_id=pending.ack_message_id,
            text=pending.final_text,
            source="steward",
            kind=self._kinds.session,
            on_sent=lambda notice=pending: self._on_agent_startup_notice_edited(notice),
            on_failed=lambda exc, notice=pending: self._fail_agent_startup_notice_edit(notice, exc),
        )

    def _on_agent_startup_notice_edited(self, pending: PendingAgentStartupNotice) -> None:
        self._logger.event(
            "agent_startup_lifecycle_notice_edited",
            **self._hooks.access_point_fields(pending.access_point),
            operation=pending.operation,
            thread_name=pending.thread_name,
            runtime_thread_id=pending.runtime_thread_id,
            delivery_id=pending.ack_message_id,
            delivery_outcome="sent",
        )
        self._finish_agent_startup_notice(pending, user_delivery="sent")

    def _finish_agent_startup_notice(
        self,
        pending: PendingAgentStartupNotice,
        *,
        user_delivery: str,
    ) -> None:
        if self._pending_agent_startups.get(pending.access_point) is pending:
            self._pending_agent_startups.pop(pending.access_point, None)
        if pending.outcome is not None:
            self._complete_local_command(
                access_point=pending.access_point,
                outcome=pending.outcome,
                user_delivery=user_delivery,
            )
        if pending.startup_result is not None:
            self._finish_agent_startup_action(
                access_point=pending.access_point,
                result=pending.startup_result,
                persisted=pending.startup_persisted,
            )

    def _log_agent_startup_notice_failure(
        self,
        pending: PendingAgentStartupNotice,
        exc: Exception,
        *,
        phase: str,
    ) -> None:
        self._logger.event(
            "agent_startup_notice_delivery_failed",
            **self._hooks.access_point_fields(pending.access_point),
            phase=phase,
            operation=pending.operation,
            thread_name=pending.thread_name,
            runtime_thread_id=pending.runtime_thread_id,
            delivery_id=pending.ack_message_id,
            delivery_outcome="failed",
            error=str(exc),
            error_type=type(exc).__name__,
            http_code=extract_http_code(exc),
        )

    def _fail_agent_startup_notice_send(
        self,
        pending: PendingAgentStartupNotice,
        exc: Exception,
    ) -> None:
        self._log_agent_startup_notice_failure(pending, exc, phase="send")
        pending.delivery_failed = True
        self._maybe_finish_agent_startup_notice(pending)

    def _fail_agent_startup_notice_edit(
        self,
        pending: PendingAgentStartupNotice,
        exc: Exception,
    ) -> None:
        self._log_agent_startup_notice_failure(pending, exc, phase="edit")
        self._finish_agent_startup_notice(pending, user_delivery="failed")

    @staticmethod
    def _resume_lifecycle_label(*, thread_name: str, thread_id: str) -> str:
        id_label = f"[{thread_id}]" if thread_id else "[unknown thread]"
        return thread_name or id_label

    @classmethod
    def _agent_startup_action_initial_text(cls, pending: PendingAgentStartupNotice) -> str:
        if pending.operation == "RESUME_AGENT":
            label = cls._resume_lifecycle_label(
                thread_name=pending.thread_name,
                thread_id=pending.runtime_thread_id,
            )
            text = f"Starting RESUME_AGENT: {label}"
        else:
            text = "Starting START_AGENT"
            if pending.agent_id:
                text += f": {pending.agent_id}"
        if pending.cwd:
            text += f" in {pending.cwd}"
        return text

    @classmethod
    def _agent_startup_action_result_text(
        cls,
        result: InteractiveStartupResult,
        *,
        operation: str,
        thread_name: str,
        runtime_thread_id: str,
    ) -> str:
        if operation == "RESUME_AGENT":
            label = cls._resume_lifecycle_label(
                thread_name=thread_name,
                thread_id=runtime_thread_id,
            )
            if result.result == "completed":
                return f"Started RESUME_AGENT: {label}\nstate: {result.state}"
            return "\n".join(
                [
                    f"Resume failed: {label}",
                    f"error: {result.error or 'startup failed'}",
                    "state: BOUND_IDLE",
                ]
            )
        label = result.agent_id or "unknown agent"
        if result.result == "completed":
            return f"Started START_AGENT: {label}\nstate: {result.state}"
        return "\n".join(
            [
                f"Start failed: {label}",
                f"error: {result.error or 'startup failed'}",
                "state: BOUND_IDLE",
            ]
        )

    def _begin_agent_startup_action_notice(
        self,
        access_point: AccessPointKey,
        action_results: list[dict[str, Any]],
    ) -> None:
        item = next(
            (
                candidate
                for candidate in action_results
                if str(candidate.get("type") or "").strip().upper()
                in {"START_AGENT", "RESUME_AGENT"}
                and bool(candidate.get("ok"))
            ),
            None,
        )
        if item is None:
            return
        operation = str(item.get("type") or "").strip().upper()
        pending = PendingAgentStartupNotice(
            access_point=access_point,
            operation=operation,
            thread_name=str(item.get("thread_name") or "").strip(),
            runtime_thread_id=str(item.get("thread_id") or "").strip(),
            agent_id=str(item.get("agent_id") or "").strip(),
            cwd=str(item.get("cwd") or "").strip(),
        )
        self._begin_agent_startup_notice(
            access_point,
            operation=pending.operation,
            initial_text=self._agent_startup_action_initial_text(pending),
            thread_name=pending.thread_name,
            runtime_thread_id=pending.runtime_thread_id,
            agent_id=pending.agent_id,
            cwd=pending.cwd,
        )

    @staticmethod
    def _agent_startup_result_text(
        result: InteractiveStartupResult,
        *,
        operation: str,
        persisted: bool,
    ) -> str:
        if result.result != "completed":
            return "\n".join(
                [
                    "runtime agent start failed.",
                    f"error: {result.error or 'startup failed'}",
                    "state: BOUND_IDLE",
                ]
            )
        if operation == "bind":
            headline = (
                "runtime agent started and bound."
                if persisted
                else "runtime agent started and bound in memory, but registry persistence failed. "
                "The binding will be lost on restart."
            )
        else:
            headline = (
                "runtime agent started."
                if persisted
                else "runtime agent started in memory, but registry persistence failed. "
                "Updated runtime metadata will be lost on restart."
            )
        lines = [
            headline,
            f"agent_id: {result.agent_id}",
            f"cwd: {result.cwd}",
            f"mode: {result.mode}",
        ]
        if operation != "bind" and result.effective_model:
            lines.append(f"model: {result.effective_model}")
        return "\n".join(lines)

    def _handle_agent_startup_result(
        self,
        access_point: AccessPointKey,
        result: InteractiveStartupResult,
    ) -> None:
        pending = self._pending_agent_startups.get(access_point)
        operation = pending.operation if pending is not None else "action"
        persist_reason = {
            "bind": "bind_command",
            "start": "start_command",
        }.get(operation, "start_agent_action")
        persisted = self._persist_registry(
            persist_reason,
            access_point,
        )
        if persisted:
            self._agent_runtime.acknowledge_metadata_change(access_point)
        if result.result != "completed":
            self._pending_startup_notice_context[access_point] = (
                f"Runtime startup failed: {result.error or 'startup failed'}. State: BOUND_IDLE."
            )
        if pending is None:
            self._finish_agent_startup_action(
                access_point=access_point,
                result=result,
                persisted=persisted,
            )
            return
        pending.thread_name = result.thread_name or pending.thread_name
        pending.runtime_thread_id = result.thread_id or pending.runtime_thread_id
        pending.agent_id = result.agent_id or pending.agent_id
        pending.cwd = result.cwd or pending.cwd
        if pending.operation in {"START_AGENT", "RESUME_AGENT"}:
            pending.startup_result = result
            pending.startup_persisted = persisted
            pending.final_text = self._agent_startup_action_result_text(
                result,
                operation=pending.operation,
                thread_name=pending.thread_name,
                runtime_thread_id=pending.runtime_thread_id,
            )
            self._maybe_finish_agent_startup_notice(pending)
            return
        if result.result == "completed":
            pending.outcome = LocalCommandOutcome(
                command=f"/{operation}",
                result=self._persisted_mutation_result(
                    code="bound" if operation == "bind" else "started",
                    persisted=persisted,
                    state=result.state,
                ),
            )
        else:
            pending.outcome = LocalCommandOutcome(
                command=f"/{operation}",
                result={
                    "ok": False,
                    "code": "startup_failed",
                    "state": result.state,
                    "persisted": persisted,
                },
            )
        pending.final_text = self._agent_startup_result_text(
            result,
            operation=operation,
            persisted=persisted,
        )
        self._maybe_finish_agent_startup_notice(pending)

    def _finish_agent_startup_action(
        self,
        *,
        access_point: AccessPointKey,
        result: InteractiveStartupResult,
        persisted: bool,
    ) -> None:
        operation = self._active_operations.get(access_point)
        if (
            operation is None
            or operation.source != "steward"
            or operation.phase != "waiting_agent_startup"
            or operation.pending_action_results is None
        ):
            return
        for item in operation.pending_action_results:
            action_type = str(item.get("type") or "").strip().upper()
            if action_type not in {"START_AGENT", "RESUME_AGENT"} or not bool(item.get("ok")):
                continue
            item.update(
                {
                    "agent_id": result.agent_id,
                    "state": result.state,
                    "thread_id": result.thread_id,
                    "thread_name": result.thread_name,
                    "configured_model": result.configured_model,
                    "effective_model": result.effective_model,
                }
            )
            if result.result != "completed":
                item["ok"] = False
                item["error"] = result.error or "startup failed"
            elif not persisted:
                item["ok"] = False
                item["applied_in_memory"] = True
                item["error"] = (
                    "registry persistence failed; the running agent is active only in memory "
                    "and will be lost on restart"
                )
        action_results = operation.pending_action_results
        operation.pending_action_results = None
        self._submit_steward_action_followup(
            access_point=access_point,
            operation=operation,
            action_results=action_results,
        )

    def drain_pending_inputs(
        self,
        *,
        preprocess_inbound: Callable[[StewardInboundText], bool],
    ) -> bool:
        progressed = False
        idx = 0
        while idx < len(self._pending_text_updates):
            inbound = self._pending_text_updates[idx]
            access_point = inbound.access_point
            pending = self._pending_approvals.get(access_point)
            active = self._active_operations.get(access_point)
            fallback_cmd = extract_fallback_command(inbound.text)
            local_immediate = is_local_immediate_command(fallback_cmd)
            is_slash_command = str(inbound.text or "").strip().startswith("/")
            has_earlier_same_access_point = any(
                pending_inbound.access_point == access_point
                for pending_inbound in self._pending_text_updates[:idx]
            )
            has_later_interrupt_same_access_point = any(
                pending_inbound.access_point == access_point
                and extract_fallback_command(pending_inbound.text) == "interrupt"
                for pending_inbound in self._pending_text_updates[idx + 1 :]
            )
            if self._pending_local_command_deliveries.get(access_point, 0) > 0:
                idx += 1
                continue
            if active is not None and pending is None and not local_immediate:
                if (
                    fallback_cmd is None
                    and not is_slash_command
                    and not has_earlier_same_access_point
                    and not has_later_interrupt_same_access_point
                    and self._submit_busy_followup_to_active_lane(inbound, active)
                ):
                    self._pending_text_updates.pop(idx)
                    progressed = True
                    continue
                idx += 1
                continue
            self._pending_text_updates.pop(idx)
            if not local_immediate and preprocess_inbound(inbound):
                progressed = True
                continue
            pending = self._pending_approvals.get(access_point)
            active = self._active_operations.get(access_point)
            if (
                pending is not None
                and active is not None
                and not local_immediate
            ):
                self._pending_text_updates.insert(idx, inbound)
                idx += 1
                continue
            self.handle_text_update(inbound)
            progressed = True
        return progressed

    def _submit_busy_followup_to_active_lane(
        self,
        inbound: StewardInboundText,
        active: PendingStewardOperation,
    ) -> bool:
        access_point = inbound.access_point
        try:
            if active.phase != "initial" or not active.allow_steer:
                return False
            if active.source == "agent":
                prompt = self._format_human_agent_input(access_point, inbound.text)
                self._agent_runtime.submit_request(access_point, prompt)
            elif active.source == "steward":
                self._submit_steward_user_request(access_point, inbound.text)
            else:
                return False
        except Exception as exc:
            self._logger.event(
                "steward_busy_followup_submit_failed",
                **self._hooks.access_point_fields(access_point),
                route_target=active.source,
                text_preview=inbound.text.strip()[:200],
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return False
        self._logger.event(
            "steward_busy_followup_submitted",
            **self._hooks.access_point_fields(access_point),
            route_target=active.source,
            text_preview=inbound.text.strip()[:200],
        )
        return True

    def _format_human_agent_input(self, access_point: AccessPointKey, text: str) -> str:
        address = self._shaman_address(access_point)
        if not address:
            return text
        envelope = format_human_routing_envelope(address, text)
        self._logger.event(
            "routing_envelope_created",
            **self._routing_access_point_fields(access_point, prefix="sender"),
            sender_address=HUMAN_ADDRESS,
            target_address=address,
        )
        return envelope

    def _runtime_snapshot(self, access_point: AccessPointKey) -> AccessPointRuntimeSnapshot:
        return build_runtime_snapshot(
            access_point=access_point,
            state=self._agent_runtime.runtime_state(access_point),
            steward_rows=self._runtime.show_running(access_point),
            agent_rows=self._agent_runtime.show_running(access_point),
            binding=self._agent_runtime.get_binding_info(access_point),
        )

    def _start_agent_action(
        self,
        access_point: AccessPointKey,
        spec: dict[str, Any],
    ) -> dict[str, Any]:
        resolved_spec = dict(spec)
        runtime_thread_id = str(resolved_spec.get("thread_id") or "").strip()
        if runtime_thread_id:
            resolved_name = find_codex_cli_thread_name(
                runtime_thread_id,
                sessions_root=self._sessions_root,
            )
            resolved_spec["thread_name"] = resolved_name or self._thread_name_by_id.get(
                runtime_thread_id,
                "",
            )
        return self._agent_runtime.start_agent(access_point, resolved_spec)

    def _remember_sessions(self, sessions: list[Any]) -> None:
        for item in sessions:
            thread_id = str(getattr(item, "session_id", "") or "").strip()
            thread_name = str(getattr(item, "thread_name", "") or "").strip()
            if thread_id and thread_name:
                self._thread_name_by_id[thread_id] = thread_name

    def _remember_resumable_action_results(self, action_results: list[dict[str, Any]]) -> None:
        for result in action_results:
            if str(result.get("type") or "").strip().upper() != "LIST_RESUMABLE":
                continue
            items = result.get("items")
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                thread_id = str(item.get("id") or "").strip()
                thread_name = str(item.get("thread_name") or "").strip()
                if thread_id and thread_name:
                    self._thread_name_by_id[thread_id] = thread_name

    def _stop_agent_action(self, access_point: AccessPointKey, requested_agent_id: str) -> dict[str, Any]:
        before = self._runtime_snapshot(access_point)
        steward = before.steward
        binding = before.binding
        if steward is not None and requested_agent_id == steward.agent_id:
            return self._log_stop_agent_action(
                access_point,
                requested_agent_id=requested_agent_id,
                result={
                    "ok": False,
                    "code": "steward_not_controllable",
                    "agent_id": requested_agent_id,
                    "state": before.state,
                    "controllable": False,
                    "error": "Steward runtime is not controllable",
                },
            )
        if binding is None or requested_agent_id != binding.agent_id:
            result: dict[str, Any] = {
                "ok": False,
                "code": "target_not_current_binding",
                "agent_id": requested_agent_id,
                "state": before.state,
                "error": "agent_id is not the current access point binding",
            }
            if binding is not None:
                result["current_agent_id"] = binding.agent_id
            return self._log_stop_agent_action(
                access_point,
                requested_agent_id=requested_agent_id,
                result=result,
            )
        if before.state == "BOUND_IDLE":
            return self._log_stop_agent_action(
                access_point,
                requested_agent_id=requested_agent_id,
                result={
                    "ok": True,
                    "code": "already_stopped",
                    "agent_id": binding.agent_id,
                    "state": "BOUND_IDLE",
                    "already_stopped": True,
                },
            )
        if before.state not in {"STARTING", "RUNNING"}:
            return self._log_stop_agent_action(
                access_point,
                requested_agent_id=requested_agent_id,
                result={
                    "ok": False,
                    "code": "invalid_state",
                    "agent_id": binding.agent_id,
                    "state": before.state,
                    "error": f"current binding cannot be stopped from state {before.state}",
                },
            )

        stopped = self._agent_runtime.stop_agent(access_point)
        after = self._runtime_snapshot(access_point)
        actual_agent_id = after.binding.agent_id if after.binding is not None else binding.agent_id
        if not stopped:
            return self._log_stop_agent_action(
                access_point,
                requested_agent_id=requested_agent_id,
                result={
                    "ok": False,
                    "code": "stop_failed",
                    "agent_id": actual_agent_id,
                    "state": after.state,
                    "error": "runtime agent stop failed",
                },
            )

        self._clear_status_runtimes(access_point, False)
        persisted = self._persist_registry("stop_agent_action", access_point)
        result = {
            "ok": persisted,
            "code": "stopped" if persisted else "stopped_not_persisted",
            "agent_id": actual_agent_id,
            "state": after.state,
            "already_stopped": False,
            "persisted": persisted,
        }
        if not persisted:
            result.update(
                {
                    "applied_in_memory": True,
                    "error": "registry persistence failed; stopped state is active only in memory",
                }
            )
        return self._log_stop_agent_action(
            access_point,
            requested_agent_id=requested_agent_id,
            result=result,
        )

    def _log_stop_agent_action(
        self,
        access_point: AccessPointKey,
        *,
        requested_agent_id: str,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        self._logger.event(
            "stop_agent_action_result",
            **self._hooks.access_point_fields(access_point),
            requested_agent_id=requested_agent_id,
            agent_id=str(result.get("agent_id") or ""),
            ok=bool(result.get("ok")),
            code=str(result.get("code") or ""),
            state=str(result.get("state") or ""),
            already_stopped=bool(result.get("already_stopped")),
        )
        return result

    def _submit_steward_user_request(self, access_point: AccessPointKey, text: str) -> None:
        ensure_started = getattr(self._runtime, "ensure_started", None)
        if callable(ensure_started):
            ensure_started(access_point)
        context_parts = [format_runtime_snapshot_context(self._runtime_snapshot(access_point))]
        persisted_state = self._persisted_state_by_access_point.get(access_point)
        pending_command_payload = (
            persisted_state.local_command_journal.pending_payload()
            if persisted_state is not None
            else {"events": []}
        )
        pending_command_events = pending_command_payload["events"]
        if pending_command_events:
            context_parts.append(format_local_command_journal_context(pending_command_payload))
        startup_notice = self._consume_startup_notice_context(access_point)
        if startup_notice:
            context_parts.append(f"Recent runtime notice shown to user:\n{startup_notice}")
        self._runtime.submit_request(
            access_point,
            text,
            context_note="\n\n".join(context_parts),
        )
        if pending_command_events and persisted_state is not None:
            last_event_id = int(pending_command_events[-1]["event_id"])
            persisted_state.local_command_journal.acknowledge(last_event_id)
            persisted = self._persist_registry("local_command_journal_delivered", access_point)
            self._logger.event(
                "local_command_journal_delivered",
                **self._hooks.access_point_fields(access_point),
                through_event_id=last_event_id,
                event_count=len(pending_command_events),
                persisted=persisted,
            )

    def build_status_text(self, *, snapshot: AccessPointRuntimeSnapshot) -> str:
        return self._access_point_adapter.build_status_text(snapshot=snapshot)

    def _build_inspect_text(
        self,
        *,
        access_point: AccessPointKey,
        snapshot: AccessPointRuntimeSnapshot,
    ) -> str:
        binding = snapshot.binding
        pending = self._pending_approvals.get(access_point)
        source_delegation = self._approval_delegation_for_source(access_point)
        target_delegation = self._approval_delegations_by_target.get(access_point)
        delegation = source_delegation or target_delegation
        delegation_role = "source" if source_delegation is not None else "target"
        approval_request = pending.request if pending is not None else (
            delegation.request if delegation is not None else None
        )
        rows: list[dict[str, str]]
        last_apply_info: dict | None
        has_active_turn: bool
        thread_metadata: dict[str, Any]
        if binding is not None:
            rows = self._agent_runtime.get_item_status_snapshot(access_point)
            last_apply_info = self._agent_runtime.get_last_item_apply_info(access_point)
            has_active_turn = self._agent_runtime.has_active_turn(access_point)
            getter = getattr(self._agent_runtime, "get_thread_metadata", None)
            thread_metadata = getter(access_point) if callable(getter) else {}
        else:
            rows = self._runtime.get_item_status_snapshot(access_point)
            last_apply_info = self._runtime.get_last_item_apply_info(access_point)
            has_active_turn = self._runtime.has_active_turn(access_point)
            getter = getattr(self._runtime, "get_thread_metadata", None)
            thread_metadata = getter(access_point) if callable(getter) else {}

        active_turn_id = "none"
        if pending is not None or source_delegation is not None:
            raw_turn_id = approval_request.params.get("turnId")
            if isinstance(raw_turn_id, str) and raw_turn_id.strip():
                active_turn_id = raw_turn_id.strip()
        elif has_active_turn and isinstance(last_apply_info, dict):
            raw_turn_id = last_apply_info.get("turn_id")
            if isinstance(raw_turn_id, str) and raw_turn_id.strip():
                active_turn_id = raw_turn_id.strip()

        lines = [
            "inspect",
            f"access point state: {snapshot.state}",
        ]
        steward = snapshot.steward
        lines.extend(
            [
                "steward runtime:",
                "role: steward",
                f"id: {steward.agent_id if steward is not None else 'none'}",
                f"state: {steward.state if steward is not None else 'NOT_STARTED'}",
                f"controllable: {'yes' if steward is not None and steward.controllable else 'no'}",
                "bound agent:",
                "role: agent",
                f"id: {binding.agent_id if binding is not None else 'none'}",
                f"state: {binding.state if binding is not None else 'UNBOUND'}",
                f"controllable: {'yes' if binding is not None and binding.controllable else 'no'}",
            ]
        )
        if binding is not None:
            lines.extend(
                [
                    f"cwd: {binding.cwd}",
                    f"mode: {binding.mode}",
                    f"configured model: {binding.configured_model or 'default'}",
                    f"effective model: {binding.effective_model or 'pending'}",
                ]
            )
            lines.extend(
                format_thread_metadata_lines(
                    thread_metadata,
                    thread_id_fallback=binding.thread_id,
                    thread_name_fallback=binding.thread_name,
                    model_fallback=binding.effective_model,
                )
            )
        lines.append(f"active turn: {active_turn_id}")
        lines.append(
            f"pending approval: {'yes' if pending is not None or source_delegation is not None else 'no'}"
        )
        if delegation is not None:
            lines.extend(
                [
                    "approval delegation: active",
                    f"approval role: {delegation_role}",
                    f"approval from: {delegation.source_address or 'unaddressed agent'}",
                    f"approval to: {delegation.target_address}",
                    f"approval method: {delegation.request.method}",
                ]
            )
        if approval_request is not None:
            command = approval_request.params.get("command")
            cwd = approval_request.params.get("cwd")
            if isinstance(command, str) and command.strip():
                lines.append(f"approval command: {command.strip()}")
            if isinstance(cwd, str) and cwd.strip():
                lines.append(f"approval cwd: {cwd.strip()}")
        append_active_work_lines(lines, rows)
        return "\n".join(lines)

    def register_approval(self, event: StewardDriverEvent, request: ApprovalRequest) -> None:
        delegation = self._approval_delegations_by_target.get(event.access_point)
        if event.source == "agent" and delegation is not None:
            try:
                self._agent_runtime.submit_approval_decision(event.access_point, "decline")
            except Exception as exc:
                self._logger.event(
                    "approval_delegation_target_decline_failed",
                    approval_id=delegation.approval_id,
                    source_address=delegation.source_address,
                    target_address=delegation.target_address,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
            if delegation.source_cancelled:
                self._logger.event(
                    "approval_delegation_cancelled_target_approval_declined",
                    approval_id=delegation.approval_id,
                    source_address=delegation.source_address,
                    target_address=delegation.target_address,
                )
                self._clear_approval_delegation(delegation)
                return
            self._fallback_approval_delegation(delegation, reason="target_waiting_approval")
            return
        if event.source == "agent" and self._try_delegate_approval(event.access_point, request):
            return
        self._register_human_approval(event.access_point, event.source, request)

    def _register_human_approval(
        self,
        access_point: AccessPointKey,
        source: str,
        request: ApprovalRequest,
    ) -> None:
        self._pending_approvals[access_point] = self._access_point_adapter.register_approval(
            access_point=access_point,
            source=source,
            request=request,
        )

    def _try_delegate_approval(self, source_access_point: AccessPointKey, request: ApprovalRequest) -> bool:
        try:
            target_address = self._agent_runtime.approval_target(source_access_point)
        except Exception as exc:
            self._queue_local_reply(
                access_point=source_access_point,
                text="approval delegation unavailable (invalid_configuration); asking human.",
                source="agent",
                kind=self._kinds.reply,
            )
            self._logger.event(
                "approval_delegation_fallback",
                source_address=self._identity_address(source_access_point),
                target_address="",
                reason="invalid_configuration",
                error=str(exc),
                fallback_target=HUMAN_APPROVAL_TARGET,
            )
            return False
        if target_address == HUMAN_APPROVAL_TARGET:
            return False
        source_address = self._identity_address(source_access_point)
        source_label = source_address or "unaddressed agent"
        target_identity = self._agent_runtime.resolve_routing_identity(target_address)
        target_access_point = target_identity.binding if target_identity is not None else None
        if target_identity is not None:
            target_address = target_identity.address
        fallback_reason = ""
        if target_access_point is None:
            fallback_reason = "target_unresolved"
        elif target_access_point == source_access_point:
            fallback_reason = "self_target"
        elif target_access_point in self._approval_delegations_by_target:
            fallback_reason = "target_delegation_busy"
        elif target_access_point in self._pending_approvals:
            fallback_reason = "target_waiting_approval"
        elif self._agent_runtime.runtime_state(target_access_point) != "RUNNING":
            fallback_reason = "target_not_running"
        elif self._agent_runtime.has_active_turn(target_access_point):
            fallback_reason = "target_busy"
        if fallback_reason:
            self._queue_local_reply(
                access_point=source_access_point,
                text=(
                    f"approval delegation to {target_address} unavailable "
                    f"({fallback_reason}); asking human."
                ),
                source="agent",
                kind=self._kinds.reply,
            )
            self._logger.event(
                "approval_delegation_fallback",
                source_address=source_address,
                target_address=target_address,
                reason=fallback_reason,
                fallback_target=HUMAN_APPROVAL_TARGET,
            )
            self._register_human_approval(source_access_point, "agent", request)
            return True

        thread_identity = (
            str(source_access_point.thread_id)
            if source_access_point.thread_id is not None
            else "main"
        )
        approval_id = (
            f"approval:{source_access_point.type}:{source_access_point.chat_id}:"
            f"{thread_identity}:{request.req_id}"
        )
        delegation = PendingApprovalDelegation(
            approval_id=approval_id,
            source_access_point=source_access_point,
            target_access_point=target_access_point,
            source_address=source_address,
            target_address=target_address,
            request=request,
        )
        prompt = build_approval_delegation_prompt(
            approval_id=approval_id,
            source_label=source_label,
            target_address=target_address,
            request=request,
        )
        try:
            self._agent_runtime.submit_request(target_access_point, prompt)
        except Exception as exc:
            self._logger.event(
                "approval_delegation_submit_failed",
                source_label=source_label,
                target_address=target_address,
                approval_id=approval_id,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            self._queue_local_reply(
                access_point=source_access_point,
                text=(
                    f"approval delegation to {target_address} unavailable "
                    "(submit_failed); asking human."
                ),
                source="agent",
                kind=self._kinds.reply,
            )
            self._register_human_approval(source_access_point, "agent", request)
            return True
        self._approval_delegations_by_target[target_access_point] = delegation
        self._active_operations[target_access_point] = PendingStewardOperation(
            access_point=target_access_point,
            source="agent",
            output_kind=self._kinds.reply,
            phase="approval_delegation",
        )
        self._queue_local_reply(
            access_point=source_access_point,
            text=f"approval delegated to {target_address}.",
            source="agent",
            kind=self._kinds.reply,
        )
        self._queue_local_reply(
            access_point=target_access_point,
            text=format_approval_request_copy(
                source_label=source_label,
                target_address=target_address,
                request=request,
            ),
            source="agent",
            kind=self._kinds.reply,
        )
        self._logger.event(
            "approval_delegation_started",
            approval_id=approval_id,
            source_address=source_address,
            target_address=target_address,
            method=request.method,
            req_id=request.req_id,
        )
        return True

    def _handle_approval_delegation_result(
        self,
        delegation: PendingApprovalDelegation,
        reply_text: str,
    ) -> None:
        if delegation.source_cancelled:
            self._logger.event(
                "approval_delegation_cancelled_target_result_ignored",
                approval_id=delegation.approval_id,
                source_address=delegation.source_address,
                target_address=delegation.target_address,
            )
            self._clear_approval_delegation(delegation)
            return
        try:
            parsed = parse_approval_delegation_decision(
                reply_text,
                expected_approval_id=delegation.approval_id,
            )
        except ValueError as exc:
            self._logger.event(
                "approval_delegation_invalid_response",
                approval_id=delegation.approval_id,
                source_address=delegation.source_address,
                target_address=delegation.target_address,
                error=str(exc),
            )
            self._fallback_approval_delegation(delegation, reason="invalid_target_response")
            return
        self._logger.event(
            "approval_delegation_decided",
            approval_id=delegation.approval_id,
            source_address=delegation.source_address,
            target_address=delegation.target_address,
            decision=parsed.decision,
        )
        if not self._submit_delegated_source_decision(delegation, parsed.decision):
            return
        self._queue_local_reply(
            access_point=delegation.target_access_point,
            text=(
                "approval decision\n"
                f"from: {delegation.target_address}\n"
                f"to: {delegation.source_address or 'unaddressed agent'}\n"
                f"decision: {parsed.decision}"
            ),
            source="agent",
            kind=self._kinds.reply,
        )
        decision_word = "accepted" if parsed.decision == "accept" else "declined"
        self._queue_local_reply(
            access_point=delegation.source_access_point,
            text=f"approval {decision_word} by {delegation.target_address}.",
            source="agent",
            kind=self._kinds.reply,
        )

    def _submit_delegated_source_decision(
        self,
        delegation: PendingApprovalDelegation,
        decision: str,
    ) -> bool:
        try:
            submit_now = getattr(self._agent_runtime, "submit_approval_decision_now", None)
            if callable(submit_now):
                submit_now(delegation.source_access_point, decision)
            else:
                self._agent_runtime.submit_approval_decision(delegation.source_access_point, decision)
        except Exception as exc:
            self._logger.event(
                "approval_source_decision_submit_failed",
                approval_id=delegation.approval_id,
                decision=decision,
                source_address=delegation.source_address,
                target_address=delegation.target_address,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            self._queue_local_reply(
                access_point=delegation.target_access_point,
                text=(
                    "approval decision delivery to "
                    f"{delegation.source_address or 'unaddressed agent'} failed; "
                    "source is asking human."
                ),
                source="agent",
                kind=self._kinds.reply,
            )
            self._queue_local_reply(
                access_point=delegation.source_access_point,
                text="approval decision could not be submitted to the source turn; asking human.",
                source="agent",
                kind=self._kinds.reply,
            )
            self._clear_approval_delegation(delegation)
            self._register_human_approval(
                delegation.source_access_point,
                "agent",
                delegation.request,
            )
            return False
        self._logger.event(
            "approval_source_decision_submitted",
            approval_id=delegation.approval_id,
            decision=decision,
            source_address=delegation.source_address,
            target_address=delegation.target_address,
        )
        self._clear_approval_delegation(delegation)
        return True

    def _fallback_approval_delegation(
        self,
        delegation: PendingApprovalDelegation,
        *,
        reason: str,
    ) -> None:
        self._logger.event(
            "approval_delegation_fallback",
            approval_id=delegation.approval_id,
            source_address=delegation.source_address,
            target_address=delegation.target_address,
            reason=reason,
            fallback_target=HUMAN_APPROVAL_TARGET,
        )
        self._queue_local_reply(
            access_point=delegation.target_access_point,
            text=(
                "approval delegation from "
                f"{delegation.source_address or 'unaddressed agent'} cancelled "
                f"({reason}); source is asking human."
            ),
            source="agent",
            kind=self._kinds.reply,
        )
        self._queue_local_reply(
            access_point=delegation.source_access_point,
            text=(
                f"approval delegation to {delegation.target_address} unavailable "
                f"({reason}); asking human."
            ),
            source="agent",
            kind=self._kinds.reply,
        )
        self._clear_approval_delegation(delegation)
        self._register_human_approval(
            delegation.source_access_point,
            "agent",
            delegation.request,
        )

    def _clear_approval_delegation(self, delegation: PendingApprovalDelegation) -> None:
        self._approval_delegations_by_target.pop(delegation.target_access_point, None)
        self._active_operations.pop(delegation.target_access_point, None)

    def _approval_delegation_for_source(
        self,
        source_access_point: AccessPointKey,
    ) -> PendingApprovalDelegation | None:
        for delegation in self._approval_delegations_by_target.values():
            if (
                delegation.source_access_point == source_access_point
                and not delegation.source_cancelled
            ):
                return delegation
        return None

    def _cancel_approval_delegation(
        self,
        delegation: PendingApprovalDelegation,
        *,
        reason: str,
    ) -> None:
        if delegation.source_cancelled:
            return
        delegation.source_cancelled = True
        self._logger.event(
            "approval_delegation_cancelled",
            approval_id=delegation.approval_id,
            source_address=delegation.source_address,
            target_address=delegation.target_address,
            reason=reason,
        )
        self._queue_local_reply(
            access_point=delegation.target_access_point,
            text=(
                "approval delegation from "
                f"{delegation.source_address or 'unaddressed agent'} cancelled ({reason})."
            ),
            source="agent",
            kind=self._kinds.reply,
        )
        if not self._agent_runtime.has_active_turn(delegation.target_access_point):
            self._clear_approval_delegation(delegation)
            return
        try:
            self._agent_runtime.interrupt_active_turn(delegation.target_access_point)
        except Exception as exc:
            self._logger.event(
                "approval_delegation_target_interrupt_failed",
                approval_id=delegation.approval_id,
                source_address=delegation.source_address,
                target_address=delegation.target_address,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return
        self._logger.event(
            "approval_delegation_target_interrupt_requested",
            approval_id=delegation.approval_id,
            source_address=delegation.source_address,
            target_address=delegation.target_address,
        )

    def handle_approval_prompt_delivery_failed(self, access_point: AccessPointKey, exc: Exception) -> None:
        pending = self._pending_approvals.get(access_point)
        if pending is None:
            return
        self._logger.event(
            "steward_approval_prompt_delivery_failed",
            **self._hooks.access_point_fields(access_point),
            route_target=pending.source,
            error=str(exc),
            error_type=type(exc).__name__,
            fallback_decision="decline",
        )
        self._pending_approvals.pop(access_point, None)
        if pending.source == "agent":
            self._agent_runtime.submit_approval_decision(access_point, "decline")
        else:
            self._runtime.submit_approval_decision(access_point, "decline")

    def process_steward_result(self, access_point: AccessPointKey, result: InteractiveAgentResult) -> None:
        operation = self._active_operations.get(access_point)
        if operation is None or operation.source != "steward":
            return
        initial_reply = result.reply
        plain_reply, actions = parse_steward_response(initial_reply)
        if operation.phase == "terminal_followup":
            if actions:
                self._logger.event(
                    "steward_action_loop_protocol_violation",
                    **self._hooks.access_point_fields(access_point),
                    reason="action_after_terminal",
                    completed_rounds=operation.action_round,
                    action_limit=operation.action_limit,
                    action_count=len(actions),
                    action_types=self._steward_action_types(actions),
                )
                final_reply = self._format_steward_action_loop_fallback(operation)
                finish_reason = "terminal_protocol_violation"
            else:
                final_reply = plain_reply
                finish_reason = "terminal_reply"
            self._finish_steward_action_loop(
                access_point=access_point,
                operation=operation,
                reason=finish_reason,
            )
            self._queue_steward_reply(
                access_point=access_point,
                operation=operation,
                text=final_reply or self._format_steward_action_loop_fallback(operation),
            )
            return
        if not actions:
            final_reply = plain_reply or operation.fallback_reply or "Control-plane actions completed."
            if operation.action_round:
                self._finish_steward_action_loop(
                    access_point=access_point,
                    operation=operation,
                    reason="final_reply",
                )
            self._queue_steward_reply(
                access_point=access_point,
                operation=operation,
                text=final_reply,
            )
            return
        if plain_reply:
            operation.fallback_reply = plain_reply
        action_types = self._steward_action_types(actions)
        next_round = operation.action_round + 1
        self._logger.event(
            "steward_actions_detected",
            **self._hooks.access_point_fields(access_point),
            action_round=next_round,
            action_limit=operation.action_limit,
            action_count=len(actions),
            action_types=action_types,
        )
        if operation.action_round >= operation.action_limit:
            self._terminate_steward_action_loop(
                access_point=access_point,
                operation=operation,
                reason="round_limit",
                pending_actions=actions,
            )
            return
        action_fingerprint = build_steward_action_fingerprint(actions)
        if operation.last_action_fingerprint == action_fingerprint:
            self._terminate_steward_action_loop(
                access_point=access_point,
                operation=operation,
                reason="duplicate_action",
                pending_actions=actions,
            )
            return
        operation.action_round = next_round
        operation.last_action_fingerprint = action_fingerprint
        action_results = execute_steward_actions(
            actions,
            sessions_root=self._sessions_root,
            show_running_provider=lambda: self._runtime_snapshot(access_point).action_payload(),
            show_running_access_point={
                "type": access_point.type,
                "chat_id": access_point.chat_id,
                "thread_id": access_point.thread_id,
            },
            stop_agent_provider=lambda agent_id: self._stop_agent_action(access_point, agent_id),
            start_agent_provider=lambda spec: self._start_agent_action(access_point, spec),
            start_agent_access_point={
                "type": access_point.type,
                "chat_id": access_point.chat_id,
                "thread_id": access_point.thread_id,
            },
            shaman_assign_provider=lambda address: self._agent_runtime.assign_address(access_point, address),
            shaman_show_provider=lambda: self._agent_runtime.show_address(access_point),
            shaman_rename_provider=lambda address: self._agent_runtime.rename_address(access_point, address),
            shaman_remove_provider=lambda: self._agent_runtime.remove_address(access_point),
            grunt_assign_provider=lambda address: self._agent_runtime.assign_address(
                access_point,
                address,
                routing_mode=ROUTING_MODE_GRUNT,
            ),
            grunt_show_provider=lambda: self._agent_runtime.show_address(
                access_point,
                routing_mode=ROUTING_MODE_GRUNT,
            ),
            grunt_rename_provider=lambda address: self._agent_runtime.rename_address(
                access_point,
                address,
                routing_mode=ROUTING_MODE_GRUNT,
            ),
            grunt_remove_provider=lambda: self._agent_runtime.remove_address(
                access_point,
                routing_mode=ROUTING_MODE_GRUNT,
            ),
            approval_target_assign_provider=lambda target: self._agent_runtime.assign_approval_target(
                access_point, target
            ),
            approval_target_show_provider=lambda: self._agent_runtime.show_approval_target(access_point),
            approval_target_change_provider=lambda target: self._agent_runtime.change_approval_target(
                access_point, target
            ),
            approval_target_clear_provider=lambda: self._agent_runtime.clear_approval_target(access_point),
        )
        self._remember_resumable_action_results(action_results)
        startup_action_pending = any(
            str(item.get("type") or "").strip().upper() in {"START_AGENT", "RESUME_AGENT"} and bool(item.get("ok"))
            for item in action_results
        )
        if startup_action_pending:
            self._clear_status_runtimes(access_point, False)
        if any(
            str(item.get("type") or "").strip().upper()
            in {
                "SHAMAN_ASSIGN",
                "SHAMAN_RENAME",
                "SHAMAN_REMOVE",
                "GRUNT_ASSIGN",
                "GRUNT_RENAME",
                "GRUNT_REMOVE",
                "APPROVAL_TARGET_ASSIGN",
                "APPROVAL_TARGET_CHANGE",
                "APPROVAL_TARGET_CLEAR",
            }
            and bool(item.get("ok"))
            for item in action_results
        ):
            if not self._persist_registry("address_action", access_point):
                self._mark_registry_persistence_failed(
                    action_results,
                    action_types={
                        "SHAMAN_ASSIGN",
                        "SHAMAN_RENAME",
                        "SHAMAN_REMOVE",
                        "GRUNT_ASSIGN",
                        "GRUNT_RENAME",
                        "GRUNT_REMOVE",
                        "APPROVAL_TARGET_ASSIGN",
                        "APPROVAL_TARGET_CHANGE",
                        "APPROVAL_TARGET_CLEAR",
                    },
                )
        self._emit_internal_action_notes(
            access_point=access_point,
            action_results=action_results,
        )
        if startup_action_pending:
            operation.phase = "waiting_agent_startup"
            operation.pending_action_results = action_results
            self._begin_agent_startup_action_notice(access_point, action_results)
            return
        self._submit_steward_action_followup(
            access_point=access_point,
            operation=operation,
            action_results=action_results,
        )

    def _submit_steward_action_followup(
        self,
        *,
        access_point: AccessPointKey,
        operation: PendingStewardOperation,
        action_results: list[dict[str, Any]],
    ) -> None:
        action_types = self._steward_action_types(action_results)
        ok_count = sum(1 for item in action_results if bool(item.get("ok")))
        failed_count = len(action_results) - ok_count
        progress_actions: list[dict[str, Any]] = []
        result_codes: list[str] = []
        for item in action_results:
            summary: dict[str, Any] = {
                "type": str(item.get("type") or "UNKNOWN").strip().upper(),
                "ok": bool(item.get("ok")),
            }
            code = str(item.get("code") or "").strip()
            if code:
                summary["code"] = code
                result_codes.append(code)
            progress_actions.append(summary)
        operation.action_progress.append(
            {
                "round": operation.action_round,
                "actions": progress_actions,
            }
        )
        self._logger.event(
            "steward_actions_executed",
            **self._hooks.access_point_fields(access_point),
            action_round=operation.action_round,
            action_limit=operation.action_limit,
            action_count=len(action_results),
            action_types=action_types,
            ok_count=ok_count,
            failed_count=failed_count,
            result_codes=result_codes,
        )
        operation.phase = "action_followup"
        self._runtime.submit_request(
            access_point,
            build_action_result_prompt(action_results),
        )

    def _terminate_steward_action_loop(
        self,
        *,
        access_point: AccessPointKey,
        operation: PendingStewardOperation,
        reason: str,
        pending_actions: list[dict[str, Any]],
    ) -> None:
        operation.phase = "terminal_followup"
        operation.action_terminal_reason = reason
        action_types = self._steward_action_types(pending_actions)
        rejection_code = (
            "action_loop_round_limit"
            if reason == "round_limit"
            else "action_loop_duplicate_action"
        )
        action_rejections = [
            {
                "type": action_type,
                "ok": False,
                "code": rejection_code,
                "error": "action was not executed because the Steward action loop stopped",
            }
            for action_type in action_types
        ]
        self._logger.event(
            "steward_action_loop_terminated",
            **self._hooks.access_point_fields(access_point),
            reason=reason,
            completed_rounds=operation.action_round,
            action_limit=operation.action_limit,
            pending_action_count=len(pending_actions),
            pending_action_types=action_types,
        )
        self._runtime.submit_request(
            access_point,
            build_action_loop_terminal_prompt(
                reason=reason,
                limit=operation.action_limit,
                completed_rounds=operation.action_round,
                progress=operation.action_progress,
                pending_actions=pending_actions,
                action_rejections=action_rejections,
            ),
        )

    def _finish_steward_action_loop(
        self,
        *,
        access_point: AccessPointKey,
        operation: PendingStewardOperation,
        reason: str,
    ) -> None:
        self._logger.event(
            "steward_action_loop_finished",
            **self._hooks.access_point_fields(access_point),
            reason=reason,
            completed_rounds=operation.action_round,
            action_limit=operation.action_limit,
            terminal_reason=operation.action_terminal_reason or None,
        )

    def _queue_steward_reply(
        self,
        *,
        access_point: AccessPointKey,
        operation: PendingStewardOperation,
        text: str,
    ) -> None:
        operation.phase = "waiting_reply_delivery"
        self._access_point_adapter.queue_text_reply(
            access_point=access_point,
            text=text,
            source="steward",
            kind=operation.output_kind,
            on_sent=lambda ap=access_point, body=text: self._complete_reply_delivery(
                access_point=ap,
                source="steward",
                text=body,
            ),
            on_failed=lambda exc, ap=access_point: self._fail_reply_delivery(
                access_point=ap,
                exc=exc,
            ),
        )

    @staticmethod
    def _steward_action_types(actions: list[dict[str, Any]]) -> list[str]:
        return [str(item.get("type") or "UNKNOWN").strip().upper() for item in actions]

    @staticmethod
    def _format_steward_action_loop_fallback(operation: PendingStewardOperation) -> str:
        progress_parts: list[str] = []
        for round_summary in operation.action_progress:
            round_number = int(round_summary.get("round") or 0)
            actions = round_summary.get("actions")
            if not isinstance(actions, list):
                continue
            labels = [
                f"{str(item.get('type') or 'UNKNOWN')} ({'ok' if bool(item.get('ok')) else 'failed'})"
                for item in actions
                if isinstance(item, dict)
            ]
            if labels:
                progress_parts.append(f"{round_number}: {', '.join(labels)}")
        lines = [
            f"Control-plane action loop stopped ({operation.action_terminal_reason or 'terminal guard'}).",
            f"Completed rounds: {operation.action_round}/{operation.action_limit}.",
        ]
        if progress_parts:
            lines.append(f"Progress: {'; '.join(progress_parts)}.")
        lines.extend(
            [
                "Pending actions were not executed.",
                "Continue?",
            ]
        )
        return "\n".join(lines)

    @staticmethod
    def _mark_registry_persistence_failed(
        action_results: list[dict[str, Any]],
        *,
        action_types: set[str],
    ) -> None:
        for item in action_results:
            if not bool(item.get("ok")):
                continue
            if str(item.get("type") or "").strip().upper() not in action_types:
                continue
            item["ok"] = False
            item["applied_in_memory"] = True
            item["error"] = (
                "registry persistence failed; the change is active only in memory "
                "and will be lost on restart"
            )

    def handle_driver_event(self, event: StewardDriverEvent) -> None:
        payload = event.event
        self._logger.event(
            "steward_driver_event_handling",
            **self._hooks.access_point_fields(event.access_point),
            route_target=event.source,
            event_type=type(payload).__name__,
        )
        if isinstance(payload, InteractiveStatusEvent):
            if payload.suppress_status:
                return
            if event.source == "agent":
                operation = self._active_operations.get(event.access_point)
                handoff = operation.routed_handoff if operation is not None else None
                if (
                    handoff is not None
                    and payload.protocol_method == "turn/started"
                    and not handoff.target_turn_started
                ):
                    handoff.target_turn_started = True
                    self._logger.event(
                        "routing_target_turn_started",
                        **self._routing_access_point_fields(event.access_point, prefix="target"),
                        sender_address=handoff.sender_address,
                        target_address=handoff.target_address,
                        source_turn_id=handoff.source_turn_id,
                        target_turn_id=payload.turn_id,
                    )
            _output_runtime, status_store = self._access_point_adapter.status_runtime_for(
                access_point=event.access_point,
                source=self._kinds.agent_status_source if event.source == "agent" else self._kinds.steward_status_source,
            )
            status_store.apply_backend_status(
                status_text=payload.status_text,
                snapshot_getter=(
                    (lambda ap=event.access_point: self._agent_runtime.get_item_status_snapshot(ap))
                    if event.source == "agent"
                    else (lambda ap=event.access_point: self._runtime.get_item_status_snapshot(ap))
                ),
                snapshot_for_turn_getter=(
                    (lambda turn_id, ap=event.access_point: self._agent_runtime.get_item_status_snapshot_for_turn(ap, turn_id))
                    if event.source == "agent"
                    else (lambda turn_id, ap=event.access_point: self._runtime.get_item_status_snapshot_for_turn(ap, turn_id))
                ),
                apply_info_getter=(
                    (lambda ap=event.access_point: self._agent_runtime.get_last_item_apply_info(ap))
                    if event.source == "agent"
                    else (lambda ap=event.access_point: self._runtime.get_last_item_apply_info(ap))
                ),
            )
            return
        if isinstance(payload, InteractiveApprovalPromptEvent):
            self._logger.event(
                "steward_driver_approval_registered",
                **self._hooks.access_point_fields(event.access_point),
                route_target=event.source,
                req_id=payload.request.req_id,
            )
            self.register_approval(event, payload.request)
            return
        if isinstance(payload, InteractiveOutboundNote):
            self._access_point_adapter.send_outbound_note(
                access_point=event.access_point,
                source=event.source,
                text=payload.text,
            )
            return
        if isinstance(payload, InteractiveStartupResult):
            if payload.result != "completed":
                self._fail_routing_requests_for_target(
                    event.access_point,
                    error_code="target_startup_failed",
                    details=payload.error or "target runtime startup failed",
                    target_state=payload.state or "BOUND_IDLE",
                )
            self._handle_agent_startup_result(event.access_point, payload)
            return
        if isinstance(payload, InteractiveRuntimeFailure):
            self._fail_routing_requests_for_target(
                event.access_point,
                error_code="target_backend_failure",
                details=payload.error or "target runtime stopped unexpectedly",
                target_state="BOUND_IDLE",
            )
            persisted = self._persist_registry("agent_runtime_failure", event.access_point)
            if persisted:
                self._agent_runtime.acknowledge_metadata_change(event.access_point)
            self._clear_status_runtimes(event.access_point, False)
            pending_approval = self._pending_approvals.get(event.access_point)
            if pending_approval is not None and getattr(pending_approval, "source", None) == "agent":
                self._pending_approvals.pop(event.access_point, None)
                self._access_point_adapter.clear_approval_markup(pending_approval)
            operation = self._active_operations.get(event.access_point)
            if operation is None or operation.source != "agent":
                self._queue_local_reply(
                    access_point=event.access_point,
                    text="\n".join(
                        [
                            "runtime agent stopped unexpectedly.",
                            f"error: {payload.error}",
                            "state: BOUND_IDLE",
                        ]
                    ),
                    source="steward",
                    kind=self._kinds.warning,
                )
            return
        if isinstance(payload, InteractiveInterruptResult):
            source_delegation = self._approval_delegation_for_source(event.access_point)
            if source_delegation is not None and event.source == "agent":
                self._cancel_approval_delegation(
                    source_delegation,
                    reason="source_interrupted",
                )
            delegation = self._approval_delegations_by_target.get(event.access_point)
            if delegation is not None and event.source == "agent":
                if delegation.source_cancelled:
                    self._logger.event(
                        "approval_delegation_cancelled_target_interrupted",
                        approval_id=delegation.approval_id,
                        source_address=delegation.source_address,
                        target_address=delegation.target_address,
                    )
                    self._clear_approval_delegation(delegation)
                else:
                    self._fallback_approval_delegation(delegation, reason="target_interrupted")
            pending_interrupt = self._pending_interrupts.get(event.access_point)
            if pending_interrupt is not None and pending_interrupt.source == event.source:
                pending_interrupt.final_text = self._interrupt_final_text(success=True)
                pending_interrupt.outcome = LocalCommandOutcome(
                    command="/interrupt",
                    result={
                        "ok": True,
                        "code": "interrupted",
                        "target": event.source,
                    },
                )
                self._maybe_finish_pending_interrupt_notice(event.access_point)
            pending_approval = self._pending_approvals.pop(event.access_point, None)
            if pending_approval is not None and getattr(pending_approval, "source", None) == event.source:
                self._access_point_adapter.clear_approval_markup(pending_approval)
            operation = self._active_operations.pop(event.access_point, None)
            handoff = operation.routed_handoff if operation is not None else None
            if handoff is not None:
                self._logger.event(
                    "routing_target_interrupted",
                    **self._routing_access_point_fields(event.access_point, prefix="target"),
                    sender_address=handoff.sender_address,
                    target_address=handoff.target_address,
                    source_turn_id=handoff.source_turn_id,
                )
            return
        if isinstance(payload, InteractiveAgentResult):
            reply_text = str(payload.reply or "")
            reply_preview = reply_text.strip()[:200]
            pending_interrupt = self._pending_interrupts.get(event.access_point)
            if pending_interrupt is not None and pending_interrupt.source == event.source:
                cancelled_delegation = self._approval_delegations_by_target.get(event.access_point)
                if cancelled_delegation is not None and cancelled_delegation.source_cancelled:
                    self._clear_approval_delegation(cancelled_delegation)
                self._logger.event(
                    "steward_interrupt_reply_suppressed",
                    **self._hooks.access_point_fields(event.access_point),
                    route_target=event.source,
                    reply_preview=reply_preview,
                )
                if pending_interrupt.final_text is None:
                    pending_interrupt.final_text = self._interrupt_final_text(success=False)
                    self._maybe_finish_pending_interrupt_notice(event.access_point)
                self._active_operations.pop(event.access_point, None)
                return
            self._logger.event(
                "steward_driver_result_received",
                **self._hooks.access_point_fields(event.access_point),
                route_target=event.source,
                reply_preview=reply_preview,
            )
            if event.source == "agent":
                delegation = self._approval_delegations_by_target.get(event.access_point)
                if delegation is not None:
                    self._handle_approval_delegation_result(delegation, reply_text)
                    return
                operation = self._active_operations.get(event.access_point)
                if operation is not None and operation.source == "agent":
                    if reply_text.startswith("agent error: "):
                        self._logger.event(
                            self._hooks.turn_error_event,
                            **self._hooks.access_point_fields(event.access_point),
                            route_target="agent",
                            error=reply_text[len("agent error: "):],
                            error_type="RuntimeError",
                        )
                        handoff = operation.routed_handoff
                        sender_address = (
                            handoff.sender_address
                            if handoff is not None
                            else self._shaman_address(event.access_point)
                        )
                        if sender_address:
                            if handoff is not None:
                                self._logger.event(
                                    "routing_target_turn_failed",
                                    **self._routing_access_point_fields(
                                        event.access_point,
                                        prefix="target",
                                    ),
                                    sender_address=handoff.sender_address,
                                    target_address=handoff.target_address,
                                    source_turn_id=handoff.source_turn_id,
                                    error=reply_text[len("agent error: ") :],
                                )
                            self._escalate_routing_failure(
                                sender_access_point=event.access_point,
                                sender_address=sender_address,
                                target_address=(
                                    handoff.target_address if handoff is not None else "unknown"
                                ),
                                error_code="agent_backend_failure",
                                details=reply_text[len("agent error: ") :],
                                output_kind=operation.output_kind,
                                turn_id=(
                                    handoff.source_turn_id
                                    if handoff is not None
                                    else str(payload.turn_id or "")
                                ),
                            )
                            return
                    if self._shaman_address(event.access_point):
                        self._handle_shaman_agent_result(
                            access_point=event.access_point,
                            reply_text=reply_text,
                            operation=operation,
                            turn_id=str(payload.turn_id or ""),
                        )
                        return
                    if self._grunt_address(event.access_point):
                        self._handle_grunt_agent_result(
                            access_point=event.access_point,
                            reply_text=reply_text,
                            operation=operation,
                            turn_id=str(payload.turn_id or ""),
                        )
                        return
                    if not reply_preview:
                        self._active_operations.pop(event.access_point, None)
                        return
                    operation.phase = "waiting_reply_delivery"
                    self._access_point_adapter.queue_text_reply(
                        access_point=event.access_point,
                        text=reply_text,
                        source="agent",
                        kind=operation.output_kind,
                        on_sent=lambda ap=event.access_point, body=reply_text: self._complete_reply_delivery(
                            access_point=ap,
                            source="agent",
                            text=body,
                        ),
                        on_failed=lambda exc, ap=event.access_point: self._fail_reply_delivery(
                            access_point=ap,
                            exc=exc,
                        ),
                    )
                return
            self.process_steward_result(event.access_point, payload)
            return
        raise AssertionError(f"unsupported steward driver event: {payload!r}")

    def _handle_shaman_agent_result(
        self,
        *,
        access_point: AccessPointKey,
        reply_text: str,
        operation: PendingStewardOperation,
        turn_id: str,
    ) -> None:
        sender_address = self._shaman_address(access_point)
        if not sender_address:
            return
        operation.routing.turn_id = turn_id
        decision = decide_routing_response(
            sender_address=sender_address,
            reply_text=reply_text,
            resolve_target=self._agent_runtime.resolve_routing_identity,
        )
        envelope = decision.envelope
        if envelope is not None:
            self._logger.event(
                "routing_envelope_parsed",
                **self._routing_access_point_fields(access_point, prefix="sender"),
                sender_address=sender_address,
                envelope_from=envelope.sender,
                target_address=envelope.target,
                turn_id=operation.routing.turn_id,
                repair_attempt=operation.routing.repair_attempt,
            )
        if isinstance(decision, RoutingReject):
            self._reject_routing_response(
                access_point=access_point,
                sender_address=sender_address,
                target_address=decision.target_address,
                error=decision.error,
                operation=operation,
            )
            return

        if isinstance(decision, RoutingDeliverHuman):
            self._deliver_routing_final_to_human(
                source_access_point=access_point,
                delivery_access_point=access_point,
                sender_address=sender_address,
                text=reply_text,
                operation=operation,
            )
            return

        if not isinstance(decision, RoutingDispatch):
            raise AssertionError(f"unsupported routing decision: {decision!r}")
        self._dispatch_routing_response(
            sender_access_point=access_point,
            sender_address=sender_address,
            reply_text=reply_text,
            decision=decision,
            operation=operation,
        )

    def _handle_grunt_agent_result(
        self,
        *,
        access_point: AccessPointKey,
        reply_text: str,
        operation: PendingStewardOperation,
        turn_id: str,
    ) -> None:
        sender_address = self._grunt_address(access_point)
        reply_to = operation.routed_reply_to
        if not sender_address or reply_to is None:
            self._logger.event(
                "routing_grunt_reply_target_missing",
                **self._routing_access_point_fields(access_point, prefix="sender"),
                sender_address=sender_address,
                turn_id=turn_id,
            )
            self._escalate_routing_failure(
                sender_access_point=access_point,
                sender_address=sender_address or "unknown",
                target_address="unknown",
                error_code="reply_target_missing",
                details="the Grunt operation has no one-hop reply target",
                output_kind=operation.output_kind,
                turn_id=turn_id,
            )
            return
        if not reply_text.strip():
            self._active_operations.pop(access_point, None)
            return

        operation.routing.turn_id = turn_id
        if reply_to.address == HUMAN_ADDRESS:
            self._deliver_routing_final_to_human(
                source_access_point=access_point,
                delivery_access_point=reply_to.access_point,
                sender_address=sender_address,
                text=reply_text,
                operation=operation,
            )
            return

        caller_address = self._shaman_address(reply_to.access_point)
        if not caller_address:
            self._escalate_routing_failure(
                sender_access_point=access_point,
                sender_address=sender_address,
                target_address=reply_to.address,
                error_code="reply_target_not_shaman",
                details="the original caller no longer has a Shaman routing identity",
                output_kind=operation.output_kind,
                target_access_point=reply_to.access_point,
                target_state=self._agent_runtime.runtime_state(reply_to.access_point),
                turn_id=turn_id,
            )
            return

        envelope = RoutingEnvelope(
            sender=sender_address,
            target=caller_address,
            body=reply_text,
        )
        synthetic_reply = format_routing_envelope(
            envelope.sender,
            envelope.target,
            envelope.body,
        )
        self._logger.event(
            "routing_grunt_reply_envelope_created",
            **self._routing_access_point_fields(access_point, prefix="sender"),
            **self._routing_access_point_fields(reply_to.access_point, prefix="target"),
            sender_address=sender_address,
            target_address=caller_address,
            original_target_address=reply_to.address,
            turn_id=turn_id,
        )
        self._dispatch_routing_response(
            sender_access_point=access_point,
            sender_address=sender_address,
            reply_text=synthetic_reply,
            decision=RoutingDispatch(
                envelope=envelope,
                target=ResolvedRoutingIdentity(
                    binding=reply_to.access_point,
                    address=caller_address,
                    mode=ROUTING_MODE_SHAMAN,
                ),
            ),
            operation=operation,
        )

    def _deliver_routing_final_to_human(
        self,
        *,
        source_access_point: AccessPointKey,
        delivery_access_point: AccessPointKey,
        sender_address: str,
        text: str,
        operation: PendingStewardOperation,
    ) -> None:
        operation.phase = "waiting_reply_delivery"
        operation.routing.sender_address = sender_address
        operation.routing.target_address = HUMAN_ADDRESS
        self._logger.event(
            "routing_response_completed",
            **self._routing_access_point_fields(source_access_point, prefix="sender"),
            sender_address=sender_address,
            target_address=HUMAN_ADDRESS,
            turn_id=operation.routing.turn_id,
            repair_attempt=operation.routing.repair_attempt,
        )
        self._access_point_adapter.queue_text_reply(
            access_point=delivery_access_point,
            text=text,
            source="agent",
            kind=operation.output_kind,
            on_sent=lambda ap=source_access_point, body=text: self._complete_reply_delivery(
                access_point=ap,
                source="agent",
                text=body,
            ),
            on_failed=lambda exc, ap=source_access_point: self._fail_reply_delivery(
                access_point=ap,
                exc=exc,
            ),
        )

    def _dispatch_routing_response(
        self,
        *,
        sender_access_point: AccessPointKey,
        sender_address: str,
        reply_text: str,
        decision: RoutingDispatch,
        operation: PendingStewardOperation,
    ) -> None:
        target_identity = decision.target
        target_access_point = target_identity.binding
        envelope = decision.envelope
        output_kind = operation.output_kind
        target_is_grunt = target_identity.mode == ROUTING_MODE_GRUNT
        sequence = self._next_routing_sequence
        self._next_routing_sequence += 1
        sender_is_grunt = bool(self._grunt_address(sender_access_point))
        sender_copy_text = (
            self._format_grunt_routing_copy(reply_text)
            if sender_is_grunt
            else reply_text
        )
        self._queue_routing_access_point_copy(
            access_point=sender_access_point,
            text=sender_copy_text,
            kind=output_kind,
            sender_address=sender_address,
            target_address=envelope.target,
            turn_id=operation.routing.turn_id,
            repair_attempt=operation.routing.repair_attempt,
            copy_role="sender",
        )
        self._active_operations.pop(sender_access_point, None)
        live_queue_size = self._live_routing_queue_size()
        if live_queue_size >= self._routing_queue_capacity:
            target_state = self._agent_runtime.runtime_state(target_access_point)
            self._logger.event(
                "routing_request_failed",
                **self._routing_access_point_fields(sender_access_point, prefix="sender"),
                **self._routing_access_point_fields(target_access_point, prefix="target"),
                queue_sequence=sequence,
                queue_capacity=self._routing_queue_capacity,
                sender_address=sender_address,
                target_address=envelope.target,
                source_turn_id=operation.routing.turn_id,
                error_code="routing_queue_full",
                target_state=target_state,
            )
            self._escalate_routing_failure(
                sender_access_point=sender_access_point,
                sender_address=sender_address,
                target_address=envelope.target,
                error_code="routing_queue_full",
                details=f"routing queue reached capacity {self._routing_queue_capacity}",
                output_kind=output_kind,
                target_access_point=target_access_point,
                target_state=target_state,
                turn_id=operation.routing.turn_id,
            )
            return
        request = PendingRoutedRequest(
            sequence=sequence,
            sender_access_point=sender_access_point,
            sender_address=sender_address,
            sender_mode=(ROUTING_MODE_GRUNT if sender_is_grunt else ROUTING_MODE_SHAMAN),
            target_access_point=target_access_point,
            target_address=envelope.target,
            target_mode=target_identity.mode,
            target_input=envelope.body if target_is_grunt else reply_text,
            output_kind=output_kind,
            source_turn_id=operation.routing.turn_id,
            repair_attempt=operation.routing.repair_attempt,
            routed_reply_to=(
                RoutedReplyTo(
                    access_point=sender_access_point,
                    address=sender_address,
                )
                if target_is_grunt
                else None
            ),
        )
        self._routing_queue.append(request)
        if target_access_point != sender_access_point:
            self._queue_routing_access_point_copy(
                access_point=target_access_point,
                text=reply_text,
                kind=output_kind,
                sender_address=sender_address,
                target_address=envelope.target,
                turn_id=operation.routing.turn_id,
                repair_attempt=operation.routing.repair_attempt,
                copy_role="target",
            )
        self._logger.event(
            "routing_request_enqueued",
            **self._routing_request_event_fields(request),
        )
        self._logger.event(
            "routing_response_completed",
            **self._routing_access_point_fields(sender_access_point, prefix="sender"),
            sender_address=sender_address,
            target_address=envelope.target,
            turn_id=operation.routing.turn_id,
            repair_attempt=operation.routing.repair_attempt,
        )

    @staticmethod
    def _format_grunt_routing_copy(envelope_text: str) -> str:
        lines = str(envelope_text or "").splitlines()
        if len(lines) < 4 or not lines[0].startswith("FROM: ") or not lines[1].startswith("TO: "):
            return envelope_text
        return "\n".join((f"> {lines[0]}", f"> {lines[1]}", *lines[2:]))

    def _live_routing_queue_size(self) -> int:
        return sum(request.failure is None for request in self._routing_queue)

    def _queued_sender_identity_failure(
        self,
        request: PendingRoutedRequest,
    ) -> RoutingFailure | None:
        target_state = self._agent_runtime.runtime_state(request.target_access_point)
        identity = self._agent_runtime.resolve_routing_identity(request.sender_address)
        if identity is None:
            return RoutingFailure(
                code="sender_unresolved",
                details="sender routing address is no longer assigned",
                target_state=target_state,
            )
        if identity.binding != request.sender_access_point:
            return RoutingFailure(
                code="sender_reassigned",
                details="sender routing address now belongs to another binding",
                target_state=target_state,
            )
        if identity.mode != request.sender_mode:
            return RoutingFailure(
                code="sender_mode_changed",
                details="sender routing mode changed while the request was queued",
                target_state=target_state,
            )
        return None

    def _queued_target_identity_failure(
        self,
        request: PendingRoutedRequest,
    ) -> RoutingFailure | None:
        identity = self._agent_runtime.resolve_routing_identity(request.target_address)
        if identity is None:
            return RoutingFailure(
                code="target_unresolved",
                details="target routing address is no longer assigned",
                target_state="UNRESOLVED",
            )
        if identity.binding != request.target_access_point:
            return RoutingFailure(
                code="target_reassigned",
                details="target routing address now belongs to another binding",
                target_state=self._agent_runtime.runtime_state(identity.binding),
            )
        if identity.mode != request.target_mode:
            return RoutingFailure(
                code="target_mode_changed",
                details="target routing mode changed while the request was queued",
                target_state=self._agent_runtime.runtime_state(request.target_access_point),
            )
        return None

    def _routing_request_event_fields(self, request: PendingRoutedRequest) -> dict[str, Any]:
        return {
            **self._routing_access_point_fields(request.sender_access_point, prefix="sender"),
            **self._routing_access_point_fields(request.target_access_point, prefix="target"),
            "queue_sequence": request.sequence,
            "queue_size": self._live_routing_queue_size(),
            "queue_capacity": self._routing_queue_capacity,
            "sender_address": request.sender_address,
            "target_address": request.target_address,
            "source_turn_id": request.source_turn_id,
            "repair_attempt": request.repair_attempt,
        }

    def drain_routing_queue(self) -> bool:
        if not self._routing_queue:
            return False
        progressed = False
        blocked_targets: set[AccessPointKey] = set()
        for request in tuple(self._routing_queue):
            if request not in self._routing_queue:
                continue
            if request.failure is not None:
                progressed = self._try_escalate_failed_routing_request(request) or progressed
                continue

            sender_failure = self._queued_sender_identity_failure(request)
            if sender_failure is not None:
                self._fail_routing_request(request, sender_failure)
                progressed = self._try_escalate_failed_routing_request(request) or progressed
                continue

            target_access_point = request.target_access_point
            if target_access_point in blocked_targets:
                self._defer_routing_request(request, reason="target_fifo_wait")
                continue

            target_failure = self._queued_target_identity_failure(request)
            if target_failure is not None:
                self._fail_routing_request(request, target_failure)
                progressed = self._try_escalate_failed_routing_request(request) or progressed
                continue

            target_state = self._agent_runtime.runtime_state(target_access_point)
            reason = ""
            if target_state != "RUNNING":
                reason = "target_not_running"
            elif target_access_point in self._pending_approvals:
                reason = "target_approval_wait"
            elif target_access_point in self._approval_delegations_by_target:
                reason = "target_delegation_busy"
            elif (
                target_access_point in self._active_operations
                or self._agent_runtime.has_active_turn(target_access_point)
            ):
                reason = "target_busy"
            if reason:
                blocked_targets.add(target_access_point)
                self._defer_routing_request(request, reason=reason, target_state=target_state)
                continue

            self._agent_runtime.submit_request(target_access_point, request.target_input)

            self._remove_routing_request(request)
            blocked_targets.add(target_access_point)
            self._active_operations[target_access_point] = PendingStewardOperation(
                access_point=target_access_point,
                source="agent",
                output_kind=self._kinds.reply,
                allow_steer=True,
                routed_handoff=RoutedHandoff(
                    sender_address=request.sender_address,
                    target_address=request.target_address,
                    source_turn_id=request.source_turn_id,
                ),
                routed_reply_to=request.routed_reply_to,
            )
            event_fields = self._routing_request_event_fields(request)
            self._logger.event("routing_request_dispatched", **event_fields)
            progressed = True
        return progressed

    def _defer_routing_request(
        self,
        request: PendingRoutedRequest,
        *,
        reason: str,
        target_state: str = "",
    ) -> None:
        if request.last_deferred_reason != reason:
            request.last_deferred_reason = reason
            self._logger.event(
                "routing_request_deferred",
                **self._routing_request_event_fields(request),
                reason=reason,
                target_state=target_state,
            )
        if request.deferred_notice_sent:
            return
        request.deferred_notice_sent = True
        self._queue_local_reply(
            access_point=request.sender_access_point,
            text=(
                f"routing to {request.target_address} queued ({reason}). "
                "Pending routing is kept in memory and will not survive an ORC restart."
            ),
            source="agent",
            kind=request.output_kind,
        )

    def _fail_routing_request(
        self,
        request: PendingRoutedRequest,
        failure: RoutingFailure,
    ) -> None:
        if request.failure is not None:
            return
        request.failure = failure
        self._logger.event(
            "routing_request_failed",
            **self._routing_request_event_fields(request),
            error_code=failure.code,
            error=failure.details,
            target_state=failure.target_state,
        )

    def _try_escalate_failed_routing_request(self, request: PendingRoutedRequest) -> bool:
        failure = request.failure
        if failure is None:
            return False
        sender = request.sender_access_point
        if (
            sender in self._active_operations
            or sender in self._pending_approvals
            or self._agent_runtime.has_active_turn(sender)
            or self._runtime.has_active_turn(sender)
        ):
            return False
        self._remove_routing_request(request)
        self._escalate_routing_failure(
            sender_access_point=sender,
            sender_address=request.sender_address,
            target_address=request.target_address,
            error_code=failure.code,
            details=failure.details,
            output_kind=request.output_kind,
            target_access_point=request.target_access_point,
            target_state=failure.target_state,
            turn_id=request.source_turn_id,
        )
        return True

    def _remove_routing_request(self, request: PendingRoutedRequest) -> None:
        self._routing_queue = [item for item in self._routing_queue if item is not request]

    def _fail_routing_requests_for_target(
        self,
        target_access_point: AccessPointKey,
        *,
        error_code: str,
        details: str,
        target_state: str,
    ) -> None:
        for request in self._routing_queue:
            if request.target_access_point != target_access_point or request.failure is not None:
                continue
            self._fail_routing_request(
                request,
                RoutingFailure(
                    code=error_code,
                    details=details,
                    target_state=target_state,
                ),
            )

    def _reject_routing_response(
        self,
        *,
        access_point: AccessPointKey,
        sender_address: str,
        target_address: str,
        error: RoutingEnvelopeError,
        operation: PendingStewardOperation,
    ) -> None:
        self._logger.event(
            "routing_envelope_rejected",
            **self._routing_access_point_fields(access_point, prefix="sender"),
            sender_address=sender_address,
            target_address=target_address,
            error_code=error.code,
            error=str(error),
            turn_id=operation.routing.turn_id,
            repair_attempt=operation.routing.repair_attempt,
        )
        if operation.routing.repair_attempt < self._MAX_ROUTING_REPAIR_ATTEMPTS:
            operation.routing.repair_attempt += 1
            operation.phase = "routing_repair"
            prompt = self._build_routing_repair_prompt(sender_address, error)
            try:
                self._agent_runtime.submit_request(access_point, prompt)
            except Exception as exc:
                self._active_operations.pop(access_point, None)
                self._escalate_routing_failure(
                    sender_access_point=access_point,
                    sender_address=sender_address,
                    target_address=target_address,
                    error_code="repair_submit_failed",
                    details=str(exc),
                    output_kind=operation.output_kind,
                    turn_id=operation.routing.turn_id,
                )
                return
            self._queue_local_reply(
                access_point=access_point,
                text=(
                    f"Routing response from {sender_address} was malformed. "
                    "Requested a corrected response."
                ),
                source="steward",
                kind=operation.output_kind,
            )
            self._logger.event(
                "routing_repair_notice_queued",
                **self._routing_access_point_fields(access_point, prefix="sender"),
                sender_address=sender_address,
                target_address=target_address,
                error_code=error.code,
                turn_id=operation.routing.turn_id,
                repair_attempt=operation.routing.repair_attempt,
            )
            self._logger.event(
                "routing_repair_requested",
                **self._routing_access_point_fields(access_point, prefix="sender"),
                sender_address=sender_address,
                target_address=target_address,
                error_code=error.code,
                turn_id=operation.routing.turn_id,
                repair_attempt=operation.routing.repair_attempt,
            )
            return

        self._active_operations.pop(access_point, None)
        self._logger.event(
            "routing_repair_exhausted",
            **self._routing_access_point_fields(access_point, prefix="sender"),
            sender_address=sender_address,
            target_address=target_address,
            error_code=error.code,
            turn_id=operation.routing.turn_id,
            repair_attempt=operation.routing.repair_attempt,
        )
        self._escalate_routing_failure(
            sender_access_point=access_point,
            sender_address=sender_address,
            target_address=target_address,
            error_code="repair_exhausted",
            details=self._format_routing_error(error),
            output_kind=operation.output_kind,
            turn_id=operation.routing.turn_id,
        )

    @staticmethod
    def _build_routing_repair_prompt(sender_address: str, error: RoutingEnvelopeError) -> str:
        error_detail = StewardCore._format_routing_error(error)
        return (
            "Routing validation failed for your previous final response.\n"
            f"error: {error_detail}\n"
            f"expected FROM: {sender_address}\n"
            "Return the complete corrected final response in exactly this format:\n"
            f"FROM: {sender_address}\n"
            "TO: human-or-known-agent-address\n\n"
            "<non-empty message body>"
        )

    @staticmethod
    def _format_routing_error(error: RoutingEnvelopeError) -> str:
        detail = str(error).strip()
        if not detail or detail == error.code:
            return error.code
        if detail.startswith(f"{error.code}:"):
            return detail
        return f"{error.code}: {detail}"

    def _escalate_routing_failure(
        self,
        *,
        sender_access_point: AccessPointKey,
        sender_address: str,
        target_address: str,
        error_code: str,
        details: str,
        output_kind: Any,
        target_access_point: AccessPointKey | None = None,
        target_state: str = "",
        turn_id: str = "",
    ) -> None:
        self._active_operations.pop(sender_access_point, None)
        prompt_lines = [
            "A routed agent response could not be completed safely. Explain the failure to the user and recommend a safe next action.",
            f"routing_error: {error_code}",
            f"sender_address: {sender_address}",
            f"requested_target: {target_address}",
            "sender_access_point: " + self._format_access_point(sender_access_point),
        ]
        if target_access_point is not None:
            prompt_lines.append("target_access_point: " + self._format_access_point(target_access_point))
        if target_state:
            prompt_lines.append(f"target_state: {target_state}")
        prompt_lines.append(f"details: {details}")
        prompt = "\n".join(prompt_lines)
        self._logger.event(
            "routing_exception_escalated",
            **self._routing_access_point_fields(sender_access_point, prefix="sender"),
            **(
                self._routing_access_point_fields(target_access_point, prefix="target")
                if target_access_point is not None
                else {}
            ),
            sender_address=sender_address,
            target_address=target_address,
            error_code=error_code,
            error=details,
            turn_id=turn_id,
        )
        try:
            self._runtime.submit_request(sender_access_point, prompt)
        except Exception as exc:
            self._logger.event(
                "routing_exception_escalation_failed",
                **self._routing_access_point_fields(sender_access_point, prefix="sender"),
                sender_address=sender_address,
                target_address=target_address,
                error_code=error_code,
                error=str(exc),
                error_type=type(exc).__name__,
                turn_id=turn_id,
            )
            self._queue_local_reply(
                access_point=sender_access_point,
                text=(
                    "routing failed and Steward escalation was unavailable.\n"
                    f"error: {error_code}\n"
                    f"target: {target_address}"
                ),
                source="steward",
                kind=self._kinds.warning,
            )
            return
        self._active_operations[sender_access_point] = PendingStewardOperation(
            access_point=sender_access_point,
            source="steward",
            output_kind=output_kind,
            allow_steer=False,
        )

    def _shaman_address(self, access_point: AccessPointKey) -> str:
        binding = self._agent_runtime.get_binding_info(access_point)
        if str((binding or {}).get("routing_mode") or ROUTING_MODE_SHAMAN) != ROUTING_MODE_SHAMAN:
            return ""
        return str((binding or {}).get("address") or "").strip()

    def _grunt_address(self, access_point: AccessPointKey) -> str:
        binding = self._agent_runtime.get_binding_info(access_point)
        if str((binding or {}).get("routing_mode") or ROUTING_MODE_SHAMAN) != ROUTING_MODE_GRUNT:
            return ""
        return str((binding or {}).get("address") or "").strip()

    def _identity_address(self, access_point: AccessPointKey) -> str:
        binding = self._agent_runtime.get_binding_info(access_point)
        return str((binding or {}).get("address") or "").strip()

    def _routing_access_point_fields(
        self,
        access_point: AccessPointKey,
        *,
        prefix: str,
    ) -> dict[str, Any]:
        binding = self._agent_runtime.get_binding_info(access_point) or {}
        return {
            f"{prefix}_access_point_type": access_point.type,
            f"{prefix}_chat_id": access_point.chat_id,
            f"{prefix}_thread_id": access_point.thread_id,
            f"{prefix}_agent_id": str(binding.get("agent_id") or ""),
            f"{prefix}_runtime_thread_id": str(binding.get("thread_id") or ""),
        }

    @staticmethod
    def _format_access_point(access_point: AccessPointKey) -> str:
        thread_id = access_point.thread_id if access_point.thread_id is not None else "main"
        return f"{access_point.type} chat_id={access_point.chat_id} thread_id={thread_id}"

    def _complete_reply_delivery(self, *, access_point: AccessPointKey, source: str, text: str) -> None:
        operation = self._active_operations.get(access_point)
        decorated = self._access_point_adapter.decorate_reply(text=text, source=source)
        self._logger.event(
            self._hooks.response_sent_event,
            **self._hooks.access_point_fields(access_point),
            text=decorated,
        )
        self._writer(self._hooks.write_output_line(access_point, text))
        if operation is not None and operation.routing.target_address:
            self._logger.event(
                "routing_message_delivered",
                **self._routing_access_point_fields(access_point, prefix="sender"),
                sender_address=operation.routing.sender_address,
                target_address=operation.routing.target_address,
                turn_id=operation.routing.turn_id,
                repair_attempt=operation.routing.repair_attempt,
            )
        self._active_operations.pop(access_point, None)

    def _fail_reply_delivery(self, *, access_point: AccessPointKey, exc: Exception) -> None:
        self._logger.event(
            "access_point_send_error",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            error=str(exc),
            error_type=type(exc).__name__,
            http_code=extract_http_code(exc),
        )
        self._writer(
            f"{access_point.type}-steward send error chat_id={access_point.chat_id} "
            f"thread_id={access_point.thread_id}: {exc}"
        )
        self._active_operations.pop(access_point, None)

    def drain_driver_events(self) -> bool:
        progressed = False
        pending_statuses: dict[tuple[AccessPointKey, str], StewardDriverEvent] = {}

        def _flush_pending_statuses() -> None:
            nonlocal pending_statuses
            if not pending_statuses:
                return
            for key in sorted(
                pending_statuses.keys(),
                key=lambda value: (*access_point_sort_key(value[0]), value[1]),
            ):
                self.handle_driver_event(pending_statuses[key])
            pending_statuses = {}

        def _drain(events: list[StewardDriverEvent]) -> None:
            nonlocal progressed
            for event in events:
                progressed = True
                if isinstance(event.event, InteractiveStatusEvent):
                    pending_statuses[(event.access_point, event.source)] = event
                    continue
                _flush_pending_statuses()
                self.handle_driver_event(event)

        _drain(self._runtime.poll_once())
        consume_steward_progress = getattr(self._runtime, "consume_poll_progress", None)
        if callable(consume_steward_progress) and consume_steward_progress():
            progressed = True
        _drain(self._agent_runtime.poll_once())
        consume_agent_progress = getattr(self._agent_runtime, "consume_poll_progress", None)
        if callable(consume_agent_progress) and consume_agent_progress():
            progressed = True
        now = clock.monotonic()
        for access_point in self._agent_runtime.pending_metadata_changes():
            retry_at = self._agent_metadata_persist_retry_at.get(access_point, 0.0)
            if now < retry_at:
                continue
            if self._persist_registry("agent_binding_metadata", access_point):
                self._agent_runtime.acknowledge_metadata_change(access_point)
                self._agent_metadata_persist_retry_at.pop(access_point, None)
                progressed = True
                continue
            self._agent_metadata_persist_retry_at[access_point] = (
                now + self._AGENT_METADATA_PERSIST_RETRY_SEC
            )
            self._logger.event(
                "agent_binding_metadata_persist_retry_scheduled",
                **self._hooks.access_point_fields(access_point),
                retry_delay_sec=self._AGENT_METADATA_PERSIST_RETRY_SEC,
            )
        _flush_pending_statuses()
        return progressed

    def _execute_routing_identity_command(
        self,
        *,
        access_point: AccessPointKey,
        command: RoutingIdentityCommand,
        is_bound: bool,
        routing_mode: str,
    ) -> tuple[str, dict[str, Any]]:
        label = routing_mode
        if not is_bound:
            return (
                f"{label} unavailable: no bound agent. Use /bind to create one.",
                {"ok": False, "code": "no_binding"},
            )
        operation = command.operation
        try:
            if operation == "show":
                result = self._agent_runtime.show_address(
                    access_point,
                    routing_mode=routing_mode,
                )
                address = str(result.get("address") or "")
                return (
                    f"{label}: {address}" if address else f"{label}: not assigned",
                    {"ok": True, "code": "shown", "address": address},
                )
            if operation == "assign":
                result = self._agent_runtime.assign_address(
                    access_point,
                    command.address,
                    routing_mode=routing_mode,
                )
                address = str(result.get("address") or "")
                success_text = f"{label} assigned: {address}"
                memory_text = f"{label} assigned in memory: {address}"
            elif operation == "rename":
                result = self._agent_runtime.rename_address(
                    access_point,
                    command.address,
                    routing_mode=routing_mode,
                )
                previous = str(result.get("previous_address") or "")
                address = str(result.get("address") or "")
                if previous == address:
                    return (
                        f"{label} unchanged: {address}",
                        {"ok": True, "code": "unchanged", "address": address},
                    )
                success_text = f"{label} renamed: {previous} -> {address}"
                memory_text = f"{label} renamed in memory: {previous} -> {address}"
            else:
                result = self._agent_runtime.remove_address(
                    access_point,
                    routing_mode=routing_mode,
                )
                removed = str(result.get("removed_address") or "")
                success_text = f"{label} removed: {removed}"
                memory_text = f"{label} removed in memory: {removed}"
        except Exception as exc:
            self._logger.event(
                f"{label}_command_failed",
                **self._hooks.access_point_fields(access_point),
                operation=operation,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return (
                f"{label} {operation} failed: {exc}",
                {"ok": False, "code": f"{operation}_failed", "error_type": type(exc).__name__},
            )

        updated_targets = int(result.get("updated_approval_targets") or 0)
        persisted = self._persist_registry(f"{label}_command", access_point)
        lines = [success_text if persisted else memory_text]
        if updated_targets:
            lines.append(f"updated approval targets: {updated_targets}")
        if not persisted:
            lines.append("registry persistence failed; the change will be lost on restart.")
        self._logger.event(
            f"{label}_command_executed",
            **self._hooks.access_point_fields(access_point),
            operation=operation,
            persisted=persisted,
        )
        command_result = self._persisted_mutation_result(
            code=operation,
            persisted=persisted,
        )
        if operation in {"assign", "rename"}:
            command_result["address"] = str(result.get("address") or "")
        if operation == "remove":
            command_result["removed_address"] = str(result.get("removed_address") or "")
        if updated_targets:
            command_result["updated_approval_targets"] = updated_targets
        return "\n".join(lines), command_result

    def _execute_approver_command(
        self,
        *,
        access_point: AccessPointKey,
        command: ApproverCommand,
    ) -> tuple[str, dict[str, Any]]:
        operation = command.operation
        action_type = f"APPROVAL_TARGET_{operation.upper()}"
        action: dict[str, Any] = {"type": action_type}
        if operation in {"assign", "change"}:
            action["approval_target"] = command.address
        result = execute_approval_target_action(
            action,
            assign_provider=lambda target: self._agent_runtime.assign_approval_target(
                access_point, target
            ),
            show_provider=lambda: self._agent_runtime.show_approval_target(access_point),
            change_provider=lambda target: self._agent_runtime.change_approval_target(
                access_point, target
            ),
            clear_provider=lambda: self._agent_runtime.clear_approval_target(access_point),
        )
        if not bool(result.get("ok")):
            error = str(result.get("error") or "unknown error")
            self._logger.event(
                "approver_command_failed",
                **self._hooks.access_point_fields(access_point),
                operation=operation,
                error=error,
            )
            if operation == "show" and error == "no bound agent for this access point":
                return (
                    "no bound agent. Use /bind to create one.",
                    {"ok": False, "code": "no_binding"},
                )
            return (
                f"approver {operation} failed: {error}",
                {"ok": False, "code": f"{operation}_failed", "error": error},
            )

        approval_target = str(result.get("approval_target") or HUMAN_APPROVAL_TARGET)
        if operation == "show":
            return (
                f"approver: {approval_target}",
                {"ok": True, "code": "shown", "approval_target": approval_target},
            )

        previous_target = str(
            result.get("previous_approval_target")
            or HUMAN_APPROVAL_TARGET
        )
        persisted = self._persist_registry("approver_command", access_point)
        verb = {"assign": "assigned", "change": "changed", "clear": "cleared"}[operation]
        qualifier = "" if persisted else " in memory"
        lines = [
            f"approver {verb}{qualifier}: {previous_target} -> {approval_target}",
            f"persisted: {'yes' if persisted else 'no'}",
        ]
        if not persisted:
            lines.append("registry persistence failed; the change will be lost on restart.")
        self._logger.event(
            "approver_command_executed",
            **self._hooks.access_point_fields(access_point),
            operation=operation,
            previous_approval_target=previous_target,
            approval_target=approval_target,
            persisted=persisted,
        )
        command_result = self._persisted_mutation_result(
            code=operation,
            persisted=persisted,
            previous_approval_target=previous_target,
            approval_target=approval_target,
        )
        return "\n".join(lines), command_result

    def handle_text_update(self, inbound: StewardInboundText) -> None:
        access_point = inbound.access_point
        self._logger.event(
            "steward_inbound_text_handling",
            **self._hooks.access_point_fields(access_point),
            text_preview=inbound.text.strip()[:200],
        )
        if hasattr(self._access_point_adapter, "drop_pending_status_updates"):
            self._access_point_adapter.drop_pending_status_updates(
                access_point=access_point,
                include_steward=True,
            )
        snapshot = self._runtime_snapshot(access_point)
        agent_state = snapshot.state
        is_bound = agent_state != "UNBOUND"
        is_running_bound = agent_state == "RUNNING"
        fallback_cmd = extract_fallback_command(inbound.text)
        self._logger.event(
            "steward_inbound_state_snapshot",
            **self._hooks.access_point_fields(access_point),
            runtime_state=agent_state,
            is_bound=is_bound,
            is_running_bound=is_running_bound,
            fallback_cmd=fallback_cmd,
        )
        if fallback_cmd == "help":
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            self._queue_local_command_reply(
                access_point=access_point,
                outcome=LocalCommandOutcome(
                    command="/help",
                    result={"ok": True, "code": "shown", "state": agent_state},
                ),
                text=self._access_point_adapter.build_help_text(state=agent_state),
                kind=self._kinds.command,
            )
            return
        if fallback_cmd == "approver":
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            command = parse_approver_command(inbound.text)
            if command.usage_error:
                reply = command.usage_error
                command_result = {"ok": False, "code": "usage_error"}
                kind = self._kinds.warning
            else:
                reply, command_result = self._execute_approver_command(
                    access_point=access_point,
                    command=command,
                )
                kind = self._kinds.command
            self._queue_local_command_reply(
                access_point=access_point,
                outcome=LocalCommandOutcome(
                    command=str(inbound.text or "").strip(),
                    result=command_result,
                ),
                text=reply,
                kind=kind,
            )
            return
        if fallback_cmd in {"shaman", "grunt"}:
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            command = (
                parse_shaman_command(inbound.text)
                if fallback_cmd == "shaman"
                else parse_grunt_command(inbound.text)
            )
            if command.usage_error:
                reply = command.usage_error
                command_result = {"ok": False, "code": "usage_error"}
                kind = self._kinds.warning
            else:
                reply, command_result = self._execute_routing_identity_command(
                    access_point=access_point,
                    command=command,
                    is_bound=is_bound,
                    routing_mode=(
                        ROUTING_MODE_SHAMAN
                        if fallback_cmd == "shaman"
                        else ROUTING_MODE_GRUNT
                    ),
                )
                kind = self._kinds.command
            self._queue_local_command_reply(
                access_point=access_point,
                outcome=LocalCommandOutcome(
                    command=str(inbound.text or "").strip(),
                    result=command_result,
                ),
                text=reply,
                kind=kind,
            )
            return
        if fallback_cmd == "status":
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            self._queue_local_command_reply(
                access_point=access_point,
                outcome=LocalCommandOutcome(
                    command="/status",
                    result={"ok": True, "code": "shown", "state": agent_state},
                ),
                text=self.build_status_text(snapshot=snapshot),
                kind=self._kinds.command,
            )
            return
        if fallback_cmd == "inspect":
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            self._queue_local_command_reply(
                access_point=access_point,
                outcome=LocalCommandOutcome(
                    command="/inspect",
                    result={"ok": True, "code": "shown", "state": agent_state},
                ),
                text=self._build_inspect_text(access_point=access_point, snapshot=snapshot),
                kind=self._kinds.command,
            )
            return
        if fallback_cmd == "interrupt":
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            if self._handle_interrupt_command(access_point):
                return
        if fallback_cmd == "bind":
            reply: str | None = None
            command_result: dict[str, Any] | None = None
            if is_running_bound:
                reply = "runtime agent is already running for this access point."
                command_result = {"ok": True, "code": "already_running", "state": "RUNNING"}
            elif is_bound:
                reply = "access point is already bound (state: BOUND_IDLE). Use /start or /reset."
                command_result = {"ok": True, "code": "already_bound", "state": "BOUND_IDLE"}
            else:
                self._begin_agent_startup_notice(access_point, operation="bind")
                try:
                    self._agent_runtime.start_agent(
                        access_point,
                        {
                            "cwd": str(Path.cwd().resolve()),
                            "mode": "proxy",
                        },
                    )
                    self._clear_status_runtimes(access_point, False)
                except Exception as exc:
                    self._handle_agent_startup_result(
                        access_point,
                        self._agent_runtime.startup_failure_result(
                            access_point,
                            error=str(exc),
                            default_cwd=str(Path.cwd().resolve()),
                        ),
                    )
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            if reply is not None:
                self._queue_local_command_reply(
                    access_point=access_point,
                    outcome=LocalCommandOutcome(
                        command="/bind",
                        result=command_result or {"ok": False, "code": "bind_failed"},
                    ),
                    text=reply,
                    kind=self._kinds.command,
                )
            return
        if fallback_cmd == "stop":
            if agent_state in {"STARTING", "RUNNING"}:
                stopped = self._agent_runtime.stop_agent(access_point)
                reply = "runtime agent stopped. state: BOUND_IDLE." if stopped else "runtime agent stop failed."
                persisted = not stopped or self._persist_registry("stop_command", access_point)
                if stopped and not persisted:
                    reply += " Registry persistence failed; this state may be lost on restart."
                command_result = (
                    self._persisted_mutation_result(
                        code="stopped",
                        persisted=persisted,
                        state="BOUND_IDLE",
                    )
                    if stopped
                    else {
                        "ok": False,
                        "code": "stop_failed",
                        "state": agent_state,
                        "persisted": persisted,
                    }
                )
            elif is_bound:
                reply = "runtime agent is already stopped (state: BOUND_IDLE)."
                command_result = {"ok": True, "code": "already_stopped", "state": "BOUND_IDLE"}
            else:
                reply = "no bound runtime agent. Use /bind to create one."
                command_result = {"ok": False, "code": "no_binding", "state": "UNBOUND"}
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            pending_startup = self._pending_agent_startups.get(access_point)
            if pending_startup is not None and agent_state == "STARTING":
                pending_startup.operation = "stop"
                pending_startup.outcome = LocalCommandOutcome(
                    command="/stop",
                    result=command_result,
                )
                pending_startup.final_text = reply
                self._maybe_finish_agent_startup_notice(pending_startup)
            else:
                self._queue_local_command_reply(
                    access_point=access_point,
                    outcome=LocalCommandOutcome(command="/stop", result=command_result),
                    text=reply,
                    kind=self._kinds.command,
                )
            return
        if fallback_cmd == "start":
            reply: str | None = None
            command_result: dict[str, Any] | None = None
            if is_running_bound:
                reply = "runtime agent is already running for this access point."
                command_result = {"ok": True, "code": "already_running", "state": "RUNNING"}
            elif agent_state == "STARTING":
                if access_point not in self._pending_agent_startups:
                    reply = "runtime agent is already starting for this access point."
                    command_result = {"ok": True, "code": "already_starting", "state": "STARTING"}
            elif is_bound:
                self._begin_agent_startup_notice(access_point, operation="start")
                try:
                    self._agent_runtime.start_bound_agent(access_point)
                    self._clear_status_runtimes(access_point, False)
                except Exception as exc:
                    self._handle_agent_startup_result(
                        access_point,
                        self._agent_runtime.startup_failure_result(
                            access_point,
                            error=str(exc),
                        ),
                    )
            else:
                reply = "no bound runtime agent. Use /bind to create one."
                command_result = {"ok": False, "code": "no_binding", "state": "UNBOUND"}
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            if reply is not None:
                self._queue_local_command_reply(
                    access_point=access_point,
                    outcome=LocalCommandOutcome(
                        command="/start",
                        result=command_result or {"ok": False, "code": "start_failed"},
                    ),
                    text=reply,
                    kind=self._kinds.command,
                )
            return
        if fallback_cmd == "reset":
            try:
                runtime_reset = self._agent_runtime.reset(access_point)
            except ValueError as exc:
                self._logger.event(
                    "access_point_reset_rejected",
                    **self._hooks.access_point_fields(access_point),
                    reason=str(exc),
                )
                self._queue_local_command_reply(
                    access_point=access_point,
                    outcome=LocalCommandOutcome(
                        command="/reset",
                        result={
                            "ok": False,
                            "code": "rejected",
                            "error_type": type(exc).__name__,
                        },
                    ),
                    text=f"reset rejected: {exc}",
                    kind=self._kinds.command,
                )
                return
            steward_reset = self._runtime.reset(access_point)
            self._agent_metadata_persist_retry_at.pop(access_point, None)
            self._clear_status_runtimes(access_point, True)
            self._pending_startup_notice_context.pop(access_point, None)
            if runtime_reset or steward_reset:
                reply = (
                    "access point reset completed.\n"
                    f"- runtime_agent: {'reset' if runtime_reset else 'no_binding'}\n"
                    f"- steward_node: {'reset' if steward_reset else 'not_started'}\n"
                    "- state: UNBOUND"
                )
            else:
                reply = "nothing to reset for this access point."
            command_result = {
                "ok": True,
                "code": "reset" if runtime_reset or steward_reset else "nothing_to_reset",
                "state": "UNBOUND",
                "runtime_reset": runtime_reset,
                "steward_reset": steward_reset,
            }
            if runtime_reset or steward_reset:
                reset_persisted = self._drop_persisted_state(access_point)
                command_result = self._persisted_mutation_result(
                    code="reset",
                    persisted=reset_persisted,
                    state="UNBOUND",
                    runtime_reset=runtime_reset,
                    steward_reset=steward_reset,
                )
                if not reset_persisted:
                    reply += (
                        "\nRegistry persistence failed; reset is active only in memory "
                        "and the binding may return after restart."
                    )
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            pending_startup = self._pending_agent_startups.get(access_point)
            if pending_startup is not None:
                pending_startup.operation = "reset"
                pending_startup.outcome = LocalCommandOutcome(
                    command="/reset",
                    result=command_result,
                )
                pending_startup.final_text = reply
                self._maybe_finish_agent_startup_notice(pending_startup)
            else:
                self._queue_local_command_reply(
                    access_point=access_point,
                    outcome=LocalCommandOutcome(command="/reset", result=command_result),
                    text=reply,
                    kind=self._kinds.command,
                )
            return
        if is_bound:
            steward_once_payload = extract_steward_once_payload(inbound.text)
            if steward_once_payload is not None:
                if not steward_once_payload:
                    self._queue_local_reply(
                        access_point=access_point,
                        text="usage: /steward <message>",
                        source="steward",
                        kind=self._kinds.warning,
                    )
                    return
                if self._hooks.on_bound_route is not None:
                    self._hooks.on_bound_route(access_point, "steward_once")
                self._logger.event(
                    "steward_route_selected",
                    **self._hooks.access_point_fields(access_point),
                    route_target="steward_once",
                )
                self._submit_steward_user_request(access_point, steward_once_payload)
                self._active_operations[access_point] = PendingStewardOperation(
                    access_point=access_point,
                    source="steward",
                    output_kind=self._kinds.reply,
                    allow_steer=False,
                )
                return
            if is_running_bound:
                if self._hooks.on_bound_route is not None:
                    self._hooks.on_bound_route(access_point, "runtime_agent")
                self._logger.event(
                    "steward_route_selected",
                    **self._hooks.access_point_fields(access_point),
                    route_target="runtime_agent",
                )
                prompt = self._format_human_agent_input(access_point, inbound.text)
                self._agent_runtime.submit_request(access_point, prompt)
                self._active_operations[access_point] = PendingStewardOperation(
                    access_point=access_point,
                    source="agent",
                    output_kind=self._kinds.reply,
                    allow_steer=True,
                    routed_reply_to=(
                        RoutedReplyTo(
                            access_point=access_point,
                            address=HUMAN_ADDRESS,
                        )
                        if self._grunt_address(access_point)
                        else None
                    ),
                )
                return
            if self._hooks.on_bound_route is not None:
                self._hooks.on_bound_route(access_point, "steward_while_bound_idle")
            self._logger.event(
                "steward_route_selected",
                **self._hooks.access_point_fields(access_point),
                route_target="steward_while_bound_idle",
            )
        else:
            self._logger.event(
                "steward_route_selected",
                **self._hooks.access_point_fields(access_point),
                route_target="steward_unbound",
            )
        self._submit_steward_user_request(access_point, inbound.text)
        self._active_operations[access_point] = PendingStewardOperation(
            access_point=access_point,
            source="steward",
            output_kind=self._kinds.reply,
            allow_steer=is_bound,
        )


def extract_steward_once_payload(raw_text: str) -> str | None:
    stripped = str(raw_text or "").strip()
    if not stripped.startswith("/"):
        return None
    head, _, tail = stripped.partition(" ")
    cmd = head.strip().lower()
    if cmd == "/steward" or cmd.startswith("/steward@"):
        return tail.strip()
    return None


def _format_restore_session_label(item: Any | None, *, fallback_session_id: str = "") -> str:
    if item is None:
        fallback = fallback_session_id.strip()
        return fallback or "not selected"
    session_id = str(getattr(item, "session_id", "") or "").strip()
    thread_name = str(getattr(item, "thread_name", "") or "").strip()
    timestamp = str(getattr(item, "last_activity_iso", "") or "").strip()
    label = thread_name or session_id or fallback_session_id.strip() or "not selected"
    if timestamp:
        return f"{label} — {timestamp}"
    return label


def _format_restore_session_label_with_id(item: Any | None, *, fallback_session_id: str = "") -> str:
    label = _format_restore_session_label(item, fallback_session_id=fallback_session_id)
    if label == "not selected":
        return label
    if item is not None and str(getattr(item, "thread_name", "") or "").strip():
        return label
    if item is None:
        session_id = fallback_session_id.strip()
    else:
        session_id = str(getattr(item, "session_id", "") or "").strip()
    if not session_id:
        return label
    return f"{label} [{session_id}]"
