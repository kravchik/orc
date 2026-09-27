from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, Protocol

from orchestrator import clock
from orchestrator.access_point_common import (
    AccessPointKey,
    AccessPointMessageRef,
    access_point_sort_key,
    format_access_point,
)
from orchestrator.access_point_work import AccessPointWorkSnapshot
from orchestrator.agent_catalog import (
    AgentCatalogEntry,
    build_agent_catalog,
    format_agent_catalog_chunks,
    format_agent_catalog_entry,
    resolve_agent_catalog_entries,
    resolve_catalog_cwd,
)
from orchestrator.approval import ApprovalRequest
from orchestrator.approval_delegation import format_approval_request_copy
from orchestrator.approval_interactions import ApprovalInteraction
from orchestrator.approval_target import HUMAN_APPROVAL_TARGET
from orchestrator.agent_interactions import AgentConversation, RoutedHandoff
from orchestrator.codex_sessions import find_codex_cli_thread_name, list_codex_cli_sessions
from orchestrator.context_window import format_context_window_remaining
from orchestrator.delivery_errors import extract_http_code
from orchestrator.inspect_text import append_active_work_lines, format_thread_metadata_lines
from orchestrator.interaction_queue import InteractionQueue, InteractionTickContext
from orchestrator.interactive_driver_events import (
    InteractiveAgentResult,
    InteractiveApprovalPromptEvent,
    InteractiveInterruptResult,
    InteractiveOutboundNote,
    InteractiveRuntimeFailure,
    InteractiveStartupResult,
    InteractiveSteerFallbackActivated,
    InteractiveSteerSubmitted,
    InteractiveStatusEvent,
)
from orchestrator.local_command_journal import (
    LocalCommandOutcome,
    format_local_command_journal_context,
)
from orchestrator.local_lifecycle_interactions import LocalLifecycleOperation
from orchestrator.processes import LifecycleLogger
from orchestrator.routing_decision import RoutingDispatch
from orchestrator.runtime_snapshot import (
    ORC_RUNTIME_ID_LABEL,
    AccessPointRuntimeSnapshot,
    build_runtime_snapshot,
    format_runtime_snapshot_context,
)
from orchestrator.routing_envelope import (
    HUMAN_ADDRESS,
    format_human_routing_envelope,
    format_routing_envelope,
)
from orchestrator.routing_interactions import (
    RoutedConversation,
    RoutedReplyTo,
    RoutingFailure,
)
from orchestrator.steward_actions import (
    execute_approval_target_action,
    execute_steward_actions,
)
from orchestrator.steward_interactions import StewardConversation
from orchestrator.steward_commands import (
    AgentsCommand,
    ApproverCommand,
    ResumeCommand,
    RoutingIdentityCommand,
    StewardHelpContext,
    extract_fallback_command,
    is_local_immediate_command,
    parse_agents_command,
    parse_approver_command,
    parse_grunt_command,
    parse_inspect_command,
    parse_resume_command,
    parse_shaman_command,
    parse_steward_user_command,
)
from orchestrator.user_commands import UNKNOWN_USER_COMMAND_TEXT
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
from orchestrator.steward_state import (
    RUNTIME_INTENT_RUNNING,
    PersistedAccessPointState,
)
from orchestrator.steward_runtime_support import (
    AgentDeliveryLane,
    AgentRuntimeStartDecision,
    PendingInterruptRequest,
    StewardDeliveryLane,
    StewardDriverEvent,
)
from orchestrator.uploads import (
    PendingUpload,
    format_upload_prompt,
    save_upload as save_upload_to_project,
    validate_pending_capacity,
)


class StewardAccessPointAdapter(Protocol):
    def build_help_text(self, *, state: str, startup: StewardHelpContext) -> str: ...
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
        on_sent: Callable[[AccessPointMessageRef], None],
        on_failed: Callable[[Exception], None],
    ) -> int: ...
    def supersede_text_reply(
        self,
        *,
        queue_token: int,
        text: str,
        source: str,
        kind: Any,
        reason: str,
    ) -> bool: ...
    def queue_text_reply_edit(
        self,
        *,
        access_point: AccessPointKey,
        message_ref: AccessPointMessageRef,
        text: str,
        source: str,
        kind: Any,
        on_sent: Callable[[], None],
        on_failed: Callable[[Exception], None],
    ) -> int | None: ...
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
class _PendingTextUpdate:
    inbound: StewardInboundText
    runtime_start_decision: AgentRuntimeStartDecision | None = None


class StewardCore:
    _DEFAULT_ROUTING_QUEUE_CAPACITY = 256
    _AGENT_METADATA_PERSIST_RETRY_SEC = 5.0
    _AGENTS_MESSAGE_CHAR_LIMIT = 3000

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
        persist_registry: Callable[[str, AccessPointKey | None], bool],
        drop_persisted_state: Callable[[AccessPointKey], bool],
        clear_status_runtimes: Callable[[AccessPointKey, bool], None],
        split_status: Callable[[AccessPointKey], None],
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
        self._persist_registry = persist_registry
        self._drop_persisted_state = drop_persisted_state
        self._clear_status_runtimes = clear_status_runtimes
        self._split_status = split_status
        self._kinds = kinds
        self._hooks = hooks
        self._pending_text_updates: list[_PendingTextUpdate] = []
        self._pending_uploads_by_access_point: dict[AccessPointKey, list[PendingUpload]] = {
            access_point: list(state.pending_uploads)
            for access_point, state in persisted_state_by_access_point.items()
            if state.pending_uploads
        }
        self._pending_startup_notice_context: dict[AccessPointKey, str] = {}
        self._pending_local_command_deliveries: dict[AccessPointKey, int] = {}
        self._agent_metadata_persist_retry_at: dict[AccessPointKey, float] = {}
        self._thread_name_by_id: dict[str, str] = {}
        self._interaction_queue: InteractionQueue[
            RoutedConversation | AgentConversation | StewardConversation | ApprovalInteraction
            | LocalLifecycleOperation | PendingInterruptRequest
        ] = InteractionQueue()
        self._routing_queue_capacity = max(1, int(routing_queue_capacity))

    def _reset_status_runtimes(
        self,
        access_point: AccessPointKey,
        include_steward: bool,
        *,
        reason: str,
    ) -> None:
        self._clear_status_runtimes(access_point, include_steward)
        self._logger.event(
            "steward_status_runtime_reset",
            **self._hooks.access_point_fields(access_point),
            reason=reason,
            include_steward=include_steward,
        )

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
        self._queue_local_command_reply_chunks(
            access_point=access_point,
            outcome=outcome,
            texts=[text],
            kind=kind,
        )

    def _queue_local_command_reply_chunks(
        self,
        *,
        access_point: AccessPointKey,
        outcome: LocalCommandOutcome,
        texts: list[str],
        kind: Any,
    ) -> None:
        chunks = [str(text) for text in texts if str(text)]
        if not chunks:
            raise ValueError("local command reply requires at least one non-empty chunk")
        self._pending_local_command_deliveries[access_point] = (
            self._pending_local_command_deliveries.get(access_point, 0) + 1
        )
        remaining = len(chunks)
        delivery_failed = False
        completed = False

        def _record_if_complete() -> None:
            nonlocal completed
            if completed or remaining > 0:
                return
            completed = True
            try:
                self._complete_local_command(
                    access_point=access_point,
                    outcome=outcome,
                    user_delivery="failed" if delivery_failed else "sent",
                )
            finally:
                self._finish_local_command_delivery(access_point)

        def _on_sent() -> None:
            nonlocal remaining
            if completed:
                return
            remaining -= 1
            _record_if_complete()

        def _on_failed(exc: Exception) -> None:
            nonlocal delivery_failed, remaining
            if completed:
                return
            self._logger.event(
                "steward_local_reply_delivery_failed",
                **self._hooks.access_point_fields(access_point),
                source="steward",
                kind=str(kind),
                error=str(exc),
                error_type=type(exc).__name__,
                http_code=extract_http_code(exc),
            )
            delivery_failed = True
            remaining -= 1
            _record_if_complete()

        try:
            for text in chunks:
                self._access_point_adapter.queue_text_reply(
                    access_point=access_point,
                    text=text,
                    source="steward",
                    kind=kind,
                    on_sent=_on_sent,
                    on_failed=_on_failed,
                )
        except Exception:
            if not completed:
                completed = True
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
        on_sent: Callable[[AccessPointMessageRef], None],
        on_failed: Callable[[Exception], None],
    ) -> int:
        return self._access_point_adapter.queue_text_reply_with_result(
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
        message_ref: AccessPointMessageRef,
        text: str,
        on_sent: Callable[[], None],
        on_failed: Callable[[Exception], None],
    ) -> None:
        self._access_point_adapter.queue_text_reply_edit(
            access_point=access_point,
            message_ref=message_ref,
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
        pending = self.iter_pending_approvals()
        if len(pending) == 1:
            return pending[0]
        return None

    def pending_approval_for(self, access_point: AccessPointKey) -> Any | None:
        child = self._human_approval_for(access_point)
        return child.human_prompt if child is not None else None

    def iter_pending_approvals(self) -> list[Any]:
        return [child.human_prompt for child in self._approval_children() if child.human_prompt is not None]

    def _approval_children(self) -> list[ApprovalInteraction]:
        return [item for item in self._interaction_queue.snapshot() if isinstance(item, ApprovalInteraction)]

    def _human_approval_for(self, access_point: AccessPointKey) -> ApprovalInteraction | None:
        return next((child for child in reversed(self._approval_children())
                     if child.source_access_point == access_point and child.phase == "human"), None)

    def _approval_for_source(self, access_point: AccessPointKey) -> ApprovalInteraction | None:
        return next(
            (
                child
                for child in reversed(self._approval_children())
                if child.source_access_point == access_point and child.pending
            ),
            None,
        )

    def _agent_approval_for_source(
        self,
        access_point: AccessPointKey,
    ) -> ApprovalInteraction | None:
        return next(
            (
                child
                for child in reversed(self._approval_children())
                if child.source_access_point == access_point
                and child.phase in {"target_starting", "target_waiting", "delegated"}
                and not child.source_cancelled
            ),
            None,
        )

    def _delegation_for_target(self, access_point: AccessPointKey) -> ApprovalInteraction | None:
        return next((child for child in reversed(self._approval_children())
                     if child.target_access_point == access_point and child.phase == "delegated"), None)

    def _access_point_work_snapshot(
        self,
        access_point: AccessPointKey,
        *,
        runtime_state: str | None = None,
    ) -> AccessPointWorkSnapshot:
        agent_conversation_active = False
        steward_conversation_active = False
        human_approval_pending = False
        approval_delegation_active = False
        routing_conversation_active = False
        local_lifecycle_active = False
        interrupt_active = False
        for item in self._interaction_queue.snapshot():
            if isinstance(item, AgentConversation):
                agent_conversation_active |= item.access_point == access_point and not item.finished
            elif isinstance(item, StewardConversation):
                steward_conversation_active |= item.access_point == access_point and not item.finished
            elif isinstance(item, ApprovalInteraction):
                human_approval_pending |= (
                    item.source_access_point == access_point
                    and item.phase == "human"
                )
                approval_delegation_active |= (
                    item.target_access_point == access_point and item.phase == "delegated"
                )
            elif isinstance(item, RoutedConversation):
                routing_conversation_active |= access_point in {
                    item.sender_access_point,
                    item.target_access_point,
                }
            elif isinstance(item, LocalLifecycleOperation):
                local_lifecycle_active |= item.access_point == access_point and not item.finished
            elif isinstance(item, PendingInterruptRequest):
                interrupt_active |= item.access_point == access_point and not item.finished
        return AccessPointWorkSnapshot(
            runtime_state=(
                runtime_state
                if runtime_state is not None
                else self._agent_runtime.runtime_state(access_point)
            ),
            agent_conversation_active=agent_conversation_active,
            steward_conversation_active=steward_conversation_active,
            agent_turn_active=self._agent_runtime.has_active_turn(access_point),
            steward_turn_active=self._runtime.has_active_turn(access_point),
            human_approval_pending=human_approval_pending,
            approval_delegation_active=approval_delegation_active,
            startup_input_pending=any(
                pending.runtime_start_decision is AgentRuntimeStartDecision.DEFERRED
                and pending.inbound.access_point == access_point
                for pending in self._pending_text_updates
            ),
            routing_conversation_active=routing_conversation_active,
            local_lifecycle_active=local_lifecycle_active,
            interrupt_active=interrupt_active,
        )

    def active_conversation_for(
        self, access_point: AccessPointKey,
    ) -> AgentConversation | StewardConversation | None:
        return self._steward_conversation_for(access_point) or self._agent_conversation_for(access_point)

    def _steward_conversation_for(
        self, access_point: AccessPointKey, *, include_finished: bool = False,
    ) -> StewardConversation | None:
        for item in reversed(self._interaction_queue.snapshot()):
            if isinstance(item, StewardConversation) and item.access_point == access_point:
                if include_finished or not item.finished:
                    return item
        return None

    def _agent_conversation_for(
        self, access_point: AccessPointKey, *, include_finished: bool = False
    ) -> AgentConversation | None:
        for item in reversed(self._interaction_queue.snapshot()):
            if isinstance(item, AgentConversation) and item.access_point == access_point:
                if include_finished or not item.finished:
                    return item
        return None

    def _new_human_agent_conversation(
        self, access_point: AccessPointKey,
    ) -> AgentConversation:
        return AgentConversation(
            access_point=access_point,
            routed_reply_to=(
                RoutedReplyTo(
                    access_point=access_point,
                    address=HUMAN_ADDRESS,
                )
                if self._grunt_address(access_point)
                else None
            ),
        )

    def _finish_conversation(
        self, access_point: AccessPointKey, *, source: str | None = None,
    ) -> AgentConversation | StewardConversation | None:
        conversation = (
            self._agent_conversation_for(access_point) if source == "agent"
            else self._steward_conversation_for(access_point)
        )
        if conversation is None and source is None:
            conversation = self._agent_conversation_for(access_point)
        if conversation is not None:
            conversation.finish()
        return conversation

    def _pending_interrupt_for(self, access_point: AccessPointKey) -> PendingInterruptRequest | None:
        for item in reversed(self._interaction_queue.snapshot()):
            if isinstance(item, (AgentConversation, StewardConversation)) and item.access_point == access_point:
                if item.interrupt_notice is not None:
                    return item.interrupt_notice
        for item in reversed(self._interaction_queue.snapshot()):
            if isinstance(item, PendingInterruptRequest) and item.orphan and not item.finished:
                if item.access_point == access_point:
                    return item
        return None

    @property
    def pending_queue_size(self) -> int:
        return len(self._pending_text_updates)

    def is_idle(self) -> bool:
        return (
            not self._pending_text_updates
            and not any(child.pending for child in self._approval_children())
            and not self._interaction_queue
            and not self._pending_local_command_deliveries
            and not self._agent_runtime.has_pending_startup()
        )

    def enqueue_inbound(self, inbound: StewardInboundText) -> None:
        parsed_command = parse_steward_user_command(inbound.text)
        if (
            not inbound.uploads
            and self.pending_approval_for(inbound.access_point) is None
            and self._delegation_for_target(inbound.access_point) is None
            and (not parsed_command.is_command or parsed_command.name == "steward")
        ):
            inbound = self._reserve_pending_uploads(inbound)
        self._pending_text_updates.append(_PendingTextUpdate(inbound=inbound))

    def upload_project_cwd(self, access_point: AccessPointKey) -> str:
        binding = self._agent_runtime.get_binding_info(access_point)
        cwd = str((binding or {}).get("cwd") or "").strip()
        if not cwd:
            raise ValueError("this access point has no bound working directory")
        return cwd

    def validate_incoming_upload(
        self,
        access_point: AccessPointKey,
        *,
        declared_size: int | None,
    ) -> str:
        cwd = self.upload_project_cwd(access_point)
        validate_pending_capacity(
            self._pending_uploads_by_access_point.get(access_point, []),
            declared_size=declared_size,
        )
        return cwd

    def save_incoming_upload(
        self,
        access_point: AccessPointKey,
        *,
        preferred_name: str,
        content: bytes,
    ) -> tuple[int, PendingUpload, bool]:
        cwd = self.validate_incoming_upload(access_point, declared_size=len(content))
        saved = save_upload_to_project(
            project_cwd=cwd,
            preferred_name=preferred_name,
            content=content,
        )
        pending = self._pending_uploads_by_access_point.setdefault(access_point, [])
        pending.append(saved.pending)
        persisted = self._persist_pending_uploads(access_point, reason="upload_saved")
        self._logger.event(
            "access_point_upload_saved",
            **self._hooks.access_point_fields(access_point),
            ordinal=len(pending),
            relative_path=saved.pending.relative_path,
            size_bytes=saved.pending.size_bytes,
            persisted=persisted,
        )
        return len(pending), saved.pending, persisted

    def _persist_pending_uploads(self, access_point: AccessPointKey, *, reason: str) -> bool:
        binding = self._agent_runtime.get_binding_info(access_point) or {}
        previous = self._persisted_state_by_access_point.get(access_point)
        if previous is None:
            previous = PersistedAccessPointState(
                project_cwd=str(binding.get("cwd") or ""),
                agent=None,
            )
        self._persisted_state_by_access_point[access_point] = replace(
            previous,
            pending_uploads=tuple(self._pending_uploads_by_access_point.get(access_point, [])),
        )
        return self._persist_registry(reason, access_point)

    def _reserve_pending_uploads(self, inbound: StewardInboundText) -> StewardInboundText:
        if inbound.uploads:
            return inbound
        pending = self._pending_uploads_by_access_point.get(inbound.access_point)
        if not pending:
            return inbound
        reserved = tuple(pending)
        self._pending_uploads_by_access_point.pop(inbound.access_point, None)
        self._persist_pending_uploads(inbound.access_point, reason="uploads_reserved_for_input")
        self._logger.event(
            "access_point_uploads_reserved",
            **self._hooks.access_point_fields(inbound.access_point),
            count=len(reserved),
        )
        return replace(inbound, uploads=reserved)

    @staticmethod
    def _input_with_uploads(inbound: StewardInboundText, text: str | None = None) -> str:
        return format_upload_prompt(
            uploads=inbound.uploads,
            text=inbound.text if text is None else text,
        )

    def _consume_startup_notice_context(self, access_point: AccessPointKey) -> str | None:
        return self._pending_startup_notice_context.pop(access_point, None)

    def handle_approval_action(self, action: StewardApprovalDecision | StewardApprovalDetailsRequest) -> bool:
        if isinstance(action, StewardApprovalDetailsRequest):
            pending = self.pending_approval_for(action.access_point)
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
        child = self._human_approval_for(action.access_point)
        if child is None:
            return True
        if not self._approval_parent_active(child):
            self._cancel_pending_approval(action.access_point, source=child.source)
            return True
        child.on_human_decision(self, action.decision, action.via)
        return True

    def approval_human_decision(self, child: ApprovalInteraction, decision: str, via: str) -> None:
        pending = child.human_prompt
        if self._hooks.on_pre_approval_decision is not None:
            self._hooks.on_pre_approval_decision(pending)
        self._split_status(pending.access_point)
        fields = self._hooks.access_point_fields(pending.access_point)
        self._logger.event(
            self._hooks.approval_human_response_event,
            **fields,
            id=pending.request.req_id,
            response=decision,
            via=via,
            route_target=pending.source,
        )
        self._access_point_adapter.cancel_pending_approval_prompt(pending)
        self._submit_approval_for_child(child, decision)

    def handle_invalid_pending_approval_input(self, access_point: AccessPointKey) -> bool:
        child = self._human_approval_for(access_point)
        if child is None:
            return False
        child.on_invalid_human_input(self)
        return True

    def approval_invalid_human_input(self, child: ApprovalInteraction) -> None:
        pending = child.human_prompt
        self._logger.event(
            "steward_invalid_approval_input",
            **self._hooks.access_point_fields(pending.access_point),
            route_target=pending.source,
        )
        self._access_point_adapter.send_invalid_approval_reply(pending)

    def _drop_pending_inputs_for_access_point(self, access_point: AccessPointKey) -> int:
        before = len(self._pending_text_updates)
        self._pending_text_updates = [
            pending for pending in self._pending_text_updates
            if pending.inbound.access_point != access_point
        ]
        return before - len(self._pending_text_updates)

    def _cancel_pending_approval(
        self,
        access_point: AccessPointKey,
        *,
        source: str,
    ) -> None:
        child = self._human_approval_for(access_point)
        if child is None or child.source != source:
            return
        pending = child.human_prompt
        child.finish()
        self._access_point_adapter.cancel_pending_approval_prompt(pending)
        self._access_point_adapter.clear_approval_markup(pending)

    def _approval_parent_active(self, child: ApprovalInteraction) -> bool:
        parent = child.parent
        if parent.finished or child.phase == "done" or child.source_cancelled:
            return False
        items = self._interaction_queue.snapshot()
        return (any(item is child for item in items)
                and any(item is parent for item in items)
                and child.belongs_to(parent, parent.pending_approval_request))

    def _submit_approval_for_child(self, child: ApprovalInteraction, decision: str, *, now: bool = False) -> None:
        if not self._approval_parent_active(child):
            raise RuntimeError("approval parent is no longer active")
        lane = self._agent_runtime if child.source == "agent" else self._runtime
        if now:
            self._agent_runtime.submit_approval_decision_now(
                child.source_access_point, decision, expected_request=child.request,
            )
        else:
            lane.submit_approval_decision(
                child.source_access_point, decision, expected_request=child.request,
            )

    def _interrupt_final_text(self, *, success: bool) -> str:
        if success:
            return "interrupted."
        return "interrupt failed: turn already finished or request was rejected."

    def interrupt_queue_initial(self, notice: PendingInterruptRequest) -> int | None:
        return self._queue_interrupt_reply(
            access_point=notice.access_point,
            text="interrupt requested.",
            on_sent=lambda message_ref: notice.notice_sent(self, message_ref),
            on_failed=lambda exc: notice.notice_failed(self, exc),
        )

    def interrupt_supersede(self, notice: PendingInterruptRequest) -> bool:
        assert notice.queue_token is not None and notice.final_text is not None
        return self._access_point_adapter.supersede_text_reply(
            queue_token=notice.queue_token,
            text=notice.final_text,
            source="steward",
            kind=self._kinds.command,
            reason="lifecycle_state_advanced",
        )

    def interrupt_queue_edit(self, notice: PendingInterruptRequest, text: str) -> None:
        assert notice.ack_message_ref is not None
        self._queue_interrupt_reply_edit(
            access_point=notice.access_point,
            message_ref=notice.ack_message_ref,
            text=text,
            on_sent=lambda: notice.notice_edited(self, text),
            on_failed=lambda exc: notice.edit_failed(self, exc, text),
        )

    def interrupt_log_final_sent(self, notice: PendingInterruptRequest) -> None:
        assert notice.ack_message_ref is not None
        self._logger.event(
            "interrupt_lifecycle_notice_final_sent",
            **self._hooks.access_point_fields(notice.access_point),
            delivery_id=notice.ack_message_ref.transport_id,
            delivery_outcome="sent",
        )

    def interrupt_complete(self, notice: PendingInterruptRequest, *, user_delivery: str) -> None:
        access_point = notice.access_point
        for item in self._interaction_queue.snapshot():
            if isinstance(item, (AgentConversation, StewardConversation)) and item.interrupt_notice is notice:
                item.interrupt_notice = None
                break
        else:
            if not any(item is notice for item in self._interaction_queue.snapshot()):
                return
        if notice.outcome is not None:
            self._complete_local_command(
                access_point=access_point,
                outcome=notice.outcome,
                user_delivery=user_delivery,
            )

    def interrupt_log_failure(self, notice: PendingInterruptRequest, exc: Exception, *, phase: str) -> None:
        self._logger.event(
            "steward_interrupt_notice_send_failed" if phase == "send" else "steward_interrupt_edit_failed",
            **self._hooks.access_point_fields(notice.access_point),
            error=str(exc),
            error_type=type(exc).__name__,
            http_code=extract_http_code(exc),
        )

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
        pending_approval = self.pending_approval_for(access_point)
        if (
            pending_approval is not None
            and getattr(pending_approval, "source", None) == target_source
        ):
            self._access_point_adapter.cancel_pending_approval_prompt(pending_approval)
        pending = PendingInterruptRequest(
            access_point=access_point,
            source=target_source,
        )
        conversation = (
            self._agent_conversation_for(access_point) if target_source == "agent"
            else self._steward_conversation_for(access_point)
        )
        if conversation is not None:
            conversation.interrupt_notice = pending
        else:
            pending.orphan = True
            self._interaction_queue.append(pending)
        pending.begin(self)
        try:
            if conversation is not None and target_source == "agent":
                conversation.interrupt(interrupt_lane.interrupt_active_turn)
            else:
                interrupt_lane.interrupt_active_turn(access_point)
        except Exception as exc:
            self._logger.event(
                "steward_interrupt_request_failed",
                **self._hooks.access_point_fields(access_point),
                route_target=target_source,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            pending.set_result(
                self,
                text=self._interrupt_final_text(success=False),
                outcome=LocalCommandOutcome(
                    command="/interrupt",
                    result={"ok": False, "code": "request_failed", "target": target_source},
                ),
            )
        return True

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
        local_command: str = "",
        notify_steward: bool = False,
        resume_existing: bool = False,
        show_session_identity: bool = False,
    ) -> LocalLifecycleOperation:
        pending = LocalLifecycleOperation(
            access_point=access_point,
            operation=operation,
            thread_name=thread_name,
            runtime_thread_id=runtime_thread_id,
            agent_id=agent_id,
            cwd=cwd,
            local_command=local_command,
            notify_steward=notify_steward,
            resume_existing=resume_existing,
            show_session_identity=show_session_identity,
        )
        self._interaction_queue.append(pending)
        pending.begin(self, initial_text)
        return pending

    def _begin_bound_agent_startup_notice(
        self,
        access_point: AccessPointKey,
    ) -> LocalLifecycleOperation:
        binding_info = self._agent_runtime.get_binding_info(access_point) or {}
        runtime_thread_id = str(binding_info.get("thread_id") or "").strip()
        thread_name = str(binding_info.get("thread_name") or "").strip()
        return self._begin_agent_startup_notice(
            access_point,
            operation="start",
            initial_text=self._bound_startup_initial_text(
                thread_name=thread_name,
                runtime_thread_id=runtime_thread_id,
            ),
            thread_name=thread_name,
            runtime_thread_id=runtime_thread_id,
            resume_existing=bool(runtime_thread_id),
            show_session_identity=True,
        )

    def _lifecycle_operation_for(self, access_point: AccessPointKey) -> LocalLifecycleOperation | None:
        return next(
            (item for item in reversed(self._interaction_queue.snapshot())
             if isinstance(item, LocalLifecycleOperation)
             and item.access_point == access_point and not item.finished),
            None,
        )

    def _request_agent_runtime_start(
        self,
        access_point: AccessPointKey,
        *,
        trigger_kind: str,
    ) -> AgentRuntimeStartDecision:
        decision = self._agent_runtime.start_decision(access_point)
        if decision not in {
            AgentRuntimeStartDecision.DEFERRED,
            AgentRuntimeStartDecision.STARTING,
        }:
            return decision
        self._logger.event(
            "agent_runtime_start_requested",
            **self._hooks.access_point_fields(access_point),
            trigger_kind=trigger_kind,
            decision=decision.value,
            reused=decision is AgentRuntimeStartDecision.STARTING,
        )
        if decision is AgentRuntimeStartDecision.STARTING:
            return decision
        self._begin_agent_startup_notice(access_point, operation="start")
        try:
            self._agent_runtime.start_bound_agent(access_point)
            self._persist_registry("lazy_starting", access_point)
            self._reset_status_runtimes(access_point, False, reason="lazy_start")
        except Exception as exc:
            self._handle_agent_startup_result(
                access_point,
                self._agent_runtime.startup_failure_result(
                    access_point,
                    error=str(exc),
                ),
            )
        return decision

    def lifecycle_queue_initial(self, operation: LocalLifecycleOperation, text: str) -> int | None:
        return self._access_point_adapter.queue_text_reply_with_result(
            access_point=operation.access_point,
            text=text,
            source="steward",
            kind=self._kinds.session,
            on_sent=lambda message_ref: operation.notice_sent(self, message_ref),
            on_failed=lambda exc: operation.notice_failed(self, exc),
        )

    def lifecycle_supersede(self, operation: LocalLifecycleOperation) -> bool:
        assert operation.final_text is not None
        assert operation.queue_token is not None
        return self._access_point_adapter.supersede_text_reply(
            queue_token=operation.queue_token,
            text=operation.final_text,
            source="steward",
            kind=self._kinds.session,
            reason="lifecycle_state_advanced",
        )

    def lifecycle_queue_edit(self, operation: LocalLifecycleOperation) -> int | None:
        assert operation.ack_message_ref is not None and operation.final_text is not None
        text = operation.final_text
        return self._access_point_adapter.queue_text_reply_edit(
            access_point=operation.access_point,
            message_ref=operation.ack_message_ref,
            text=text,
            source="steward",
            kind=self._kinds.session,
            on_sent=lambda: operation.notice_edited(self, operation.edit_text or text),
            on_failed=lambda exc: operation.edit_failed(self, exc, operation.edit_text or text),
        )

    def lifecycle_supersede_edit(self, operation: LocalLifecycleOperation) -> bool:
        assert operation.edit_queue_token is not None and operation.final_text is not None
        return self._access_point_adapter.supersede_text_reply(
            queue_token=operation.edit_queue_token,
            text=operation.final_text,
            source="steward",
            kind=self._kinds.session,
            reason="lifecycle_state_advanced",
        )

    def lifecycle_log_sent(self, pending: LocalLifecycleOperation, *, final: bool) -> None:
        self._logger.event(
            "agent_startup_lifecycle_notice_final_sent" if final and pending.final_sent_as_initial
            else "agent_startup_lifecycle_notice_edited" if final
            else "agent_startup_lifecycle_notice_sent",
            **self._hooks.access_point_fields(pending.access_point),
            operation=pending.operation.upper() if final and pending.final_sent_as_initial else pending.operation,
            thread_name=pending.thread_name,
            runtime_thread_id=pending.runtime_thread_id,
            delivery_id=(pending.ack_message_ref.transport_id
                         if pending.ack_message_ref is not None else None),
            delivery_outcome="sent",
        )

    def lifecycle_complete(self, pending: LocalLifecycleOperation, *, user_delivery: str) -> None:
        if pending.outcome is not None:
            self._complete_local_command(
                access_point=pending.access_point,
                outcome=pending.outcome,
                user_delivery=user_delivery,
            )
        if pending.startup_result is not None and pending.notify_steward:
            self._finish_agent_startup_action(
                access_point=pending.access_point,
                result=pending.startup_result,
                persisted=pending.startup_persisted,
            )

    def _log_agent_startup_notice_failure(
        self,
        pending: LocalLifecycleOperation,
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
            delivery_id=(
                pending.ack_message_ref.transport_id
                if pending.ack_message_ref is not None
                else None
            ),
            delivery_outcome="failed",
            error=str(exc),
            error_type=type(exc).__name__,
            http_code=extract_http_code(exc),
        )

    def lifecycle_log_failed(self, pending: LocalLifecycleOperation, exc: Exception, *, phase: str) -> None:
        self._log_agent_startup_notice_failure(pending, exc, phase=phase)

    @staticmethod
    def _resume_lifecycle_label(*, thread_name: str, thread_id: str) -> str:
        id_label = f"[{thread_id}]" if thread_id else "[unknown thread]"
        return thread_name or id_label

    @classmethod
    def _bound_startup_initial_text(
        cls,
        *,
        thread_name: str,
        runtime_thread_id: str,
    ) -> str:
        if not runtime_thread_id:
            return "runtime agent starting new session..."
        label = cls._resume_lifecycle_label(
            thread_name=thread_name,
            thread_id=runtime_thread_id,
        )
        return f"runtime agent resuming...\nsession: {label}"

    @classmethod
    def _agent_startup_action_initial_text(cls, pending: LocalLifecycleOperation) -> str:
        if pending.operation == "RESUME_AGENT":
            label = cls._resume_lifecycle_label(
                thread_name=pending.thread_name,
                thread_id=pending.runtime_thread_id,
            )
            text = f"Starting RESUME_AGENT: {label}"
            if pending.cwd:
                text += f" in {pending.cwd}"
            return text

        lines = ["Starting START_AGENT"]
        if pending.agent_id:
            lines.append(f"{ORC_RUNTIME_ID_LABEL}: {pending.agent_id}")
        if pending.cwd:
            lines.append(f"cwd: {pending.cwd}")
        return "\n".join(lines)

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
        label = result.agent_id or "unknown"
        if result.result == "completed":
            return "\n".join(
                [
                    "Started START_AGENT",
                    f"{ORC_RUNTIME_ID_LABEL}: {label}",
                    f"state: {result.state}",
                ]
            )
        return "\n".join(
            [
                "Start failed",
                f"{ORC_RUNTIME_ID_LABEL}: {label}",
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
        pending = LocalLifecycleOperation(
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
            notify_steward=True,
        )

    @classmethod
    def _agent_startup_result_text(
        cls,
        result: InteractiveStartupResult,
        *,
        operation: str,
        persisted: bool,
        resume_existing: bool = False,
        show_session_identity: bool = False,
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
        elif show_session_identity and resume_existing:
            headline = (
                "runtime agent resumed."
                if persisted
                else "runtime agent resumed in memory, but registry persistence failed. "
                "Updated runtime metadata will be lost on restart."
            )
        else:
            headline = (
                "runtime agent started."
                if persisted
                else "runtime agent started in memory, but registry persistence failed. "
                "Updated runtime metadata will be lost on restart."
            )
        lines = [headline]
        if operation == "bind" or not show_session_identity:
            lines.append(f"{ORC_RUNTIME_ID_LABEL}: {result.agent_id}")
        else:
            lines.append(
                "session: "
                + cls._resume_lifecycle_label(
                    thread_name=result.thread_name,
                    thread_id=result.thread_id,
                )
            )
        lines.extend((f"cwd: {result.cwd}", f"mode: {result.mode}"))
        if operation != "bind" and result.effective_model:
            lines.append(f"model: {result.effective_model}")
        return "\n".join(lines)

    def _handle_agent_startup_result(
        self,
        access_point: AccessPointKey,
        result: InteractiveStartupResult,
    ) -> None:
        pending = self._lifecycle_operation_for(access_point)
        operation = pending.operation if pending is not None else "action"
        persist_reason = {
            "bind": "bind_command",
            "start": "start_command",
            "RESUME_AGENT": (
                "resume_command"
                if pending is not None and pending.local_command
                else "start_agent_action"
            ),
        }.get(operation, "start_agent_action")
        persisted = self._persist_registry(
            persist_reason,
            access_point,
        )
        if persisted:
            self._agent_runtime.acknowledge_metadata_change(access_point)
        if result.result != "completed":
            self._reset_status_runtimes(
                access_point,
                False,
                reason="startup_failure",
            )
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
        if pending.operation in {"START_AGENT", "RESUME_AGENT"}:
            outcome = None
            if pending.local_command:
                if result.result == "completed":
                    command_result = self._persisted_mutation_result(
                        code="resumed",
                        persisted=persisted,
                        state=result.state,
                        thread_id=result.thread_id or pending.runtime_thread_id,
                    )
                else:
                    command_result = {
                        "ok": False,
                        "code": "startup_failed",
                        "state": result.state,
                        "persisted": persisted,
                        "thread_id": pending.runtime_thread_id,
                    }
                outcome = LocalCommandOutcome(
                    command=pending.local_command,
                    result=command_result,
                )
            pending.apply_startup_result(
                self, result, persisted=persisted, outcome=outcome,
                final_text=self._agent_startup_action_result_text(
                    result,
                    operation=pending.operation,
                    thread_name=(
                        pending.thread_name
                        if pending.local_command
                        else result.thread_name or pending.thread_name
                    ),
                    runtime_thread_id=(
                        pending.runtime_thread_id
                        if pending.local_command
                        else result.thread_id or pending.runtime_thread_id
                    ),
                ),
            )
            return
        if result.result == "completed":
            outcome = LocalCommandOutcome(
                command=f"/{operation}",
                result=self._persisted_mutation_result(
                    code="bound" if operation == "bind" else "started",
                    persisted=persisted,
                    state=result.state,
                ),
            )
        else:
            outcome = LocalCommandOutcome(
                command=f"/{operation}",
                result={
                    "ok": False,
                    "code": "startup_failed",
                    "state": result.state,
                    "persisted": persisted,
                },
            )
        pending.apply_startup_result(
            self, result, persisted=persisted, outcome=outcome,
            final_text=self._agent_startup_result_text(
                result,
                operation=operation,
                persisted=persisted,
                resume_existing=pending.resume_existing,
                show_session_identity=pending.show_session_identity,
            ),
        )

    def _finish_agent_startup_action(
        self,
        *,
        access_point: AccessPointKey,
        result: InteractiveStartupResult,
        persisted: bool,
    ) -> None:
        conversation = self._steward_conversation_for(access_point)
        if conversation is not None:
            conversation.complete_startup(self, result, persisted)

    def drain_pending_inputs(
        self,
        *,
        preprocess_inbound: Callable[[StewardInboundText], bool],
    ) -> bool:
        progressed = False
        idx = 0
        while idx < len(self._pending_text_updates):
            pending_update = self._pending_text_updates[idx]
            inbound = pending_update.inbound
            access_point = inbound.access_point
            pending = self.pending_approval_for(access_point)
            active = self.active_conversation_for(access_point)
            parsed_command = parse_steward_user_command(inbound.text)
            fallback_cmd = extract_fallback_command(inbound.text)
            local_immediate = parsed_command.is_unknown or is_local_immediate_command(fallback_cmd)
            is_slash_command = parsed_command.is_command
            has_earlier_same_access_point = any(
                pending.inbound.access_point == access_point
                for pending in self._pending_text_updates[:idx]
            )
            has_later_interrupt_same_access_point = any(
                pending.inbound.access_point == access_point
                and extract_fallback_command(pending.inbound.text) == "interrupt"
                for pending in self._pending_text_updates[idx + 1 :]
            )
            if self._pending_local_command_deliveries.get(access_point, 0) > 0:
                idx += 1
                continue
            if not is_slash_command and pending_update.runtime_start_decision is None:
                start_decision = self._request_agent_runtime_start(
                    access_point,
                    trigger_kind="human_input",
                )
                if start_decision in {
                    AgentRuntimeStartDecision.DEFERRED,
                    AgentRuntimeStartDecision.STARTING,
                }:
                    pending_update.runtime_start_decision = start_decision
                if start_decision is AgentRuntimeStartDecision.DEFERRED:
                    progressed = True
                    idx += 1
                    continue
            lifecycle = self._lifecycle_operation_for(access_point)
            if lifecycle is not None and not is_slash_command:
                idx += 1
                continue
            if (active is not None or self._delegation_for_target(access_point) is not None) and not local_immediate:
                if pending is not None:
                    if is_slash_command:
                        idx += 1
                        continue
                else:
                    if (
                        fallback_cmd is None
                        and not is_slash_command
                        and not has_earlier_same_access_point
                        and not has_later_interrupt_same_access_point
                        and active is not None
                        and self._submit_busy_followup_to_active_lane(inbound, active)
                    ):
                        self._pending_text_updates.pop(idx)
                        progressed = True
                        continue
                    idx += 1
                    continue
            self._pending_text_updates.pop(idx)
            if not is_slash_command and preprocess_inbound(inbound):
                progressed = True
                continue
            pending = self.pending_approval_for(access_point)
            active = self.active_conversation_for(access_point)
            if (
                pending is not None
                and (active is not None or self._delegation_for_target(access_point) is not None)
                and not local_immediate
            ):
                self._pending_text_updates.insert(idx, pending_update)
                idx += 1
                continue
            self.handle_text_update(inbound)
            progressed = True
        return progressed

    def _submit_busy_followup_to_active_lane(
        self,
        inbound: StewardInboundText,
        active: AgentConversation | StewardConversation,
    ) -> bool:
        access_point = inbound.access_point
        try:
            if active.phase != "initial":
                return False
            if isinstance(active, AgentConversation):
                conversation = self._agent_conversation_for(access_point)
                if conversation is None:
                    return False
                source_text = self._input_with_uploads(inbound)
                prompt = self._format_human_agent_input(
                    access_point,
                    source_text,
                )
                if not conversation.steer(
                    prompt,
                    lambda ap, text: self._agent_runtime.submit_request(
                        ap, text, as_steer=True,
                    ),
                    source_text=source_text,
                ):
                    return False
            elif isinstance(active, StewardConversation):
                if not active.allow_steer:
                    return False
                conversation = self._steward_conversation_for(access_point)
                if conversation is None or not conversation.steer(
                    self._input_with_uploads(inbound),
                    lambda ap, text: self._submit_steward_user_request(
                        ap, text, as_steer=True,
                    ),
                ):
                    return False
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
        self._split_status(access_point)
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

    def _help_startup_context(
        self,
        *,
        access_point: AccessPointKey,
        snapshot: AccessPointRuntimeSnapshot,
    ) -> StewardHelpContext:
        persisted = self._persisted_state_by_access_point.get(access_point)
        runtime_state = self._agent_runtime.snapshot_persisted().get(access_point)
        state = runtime_state or persisted
        project_cwd = str(state.project_cwd if state is not None else "").strip()
        has_binding = snapshot.binding is not None or bool(
            state is not None and isinstance(state.agent, dict) and state.agent
        )
        return StewardHelpContext(
            project_cwd=project_cwd,
            has_binding=has_binding,
            auto_start=bool(
                snapshot.state == "BOUND_IDLE"
                and has_binding
                and state is not None
                and state.runtime_intent == RUNTIME_INTENT_RUNNING
            ),
        )

    def _inspect_targets(self, source: AccessPointKey) -> list[AccessPointKey]:
        targets = set(self._persisted_state_by_access_point)
        targets.update(self._agent_runtime.snapshot_persisted())
        targets.add(source)
        return sorted(
            (target for target in targets if target.type == source.type),
            key=access_point_sort_key,
        )

    def _broadcast_inspect(self, source: AccessPointKey) -> None:
        targets = self._inspect_targets(source)
        for target in targets:
            try:
                text = self._build_inspect_text(
                    access_point=target,
                    snapshot=self._runtime_snapshot(target),
                )
                if target == source:
                    self._queue_local_command_reply(
                        access_point=target,
                        outcome=LocalCommandOutcome(
                            command="/inspect all",
                            result={
                                "ok": True,
                                "code": "broadcast",
                                "access_point_count": len(targets),
                            },
                        ),
                        text=text,
                        kind=self._kinds.command,
                    )
                else:
                    self._queue_local_reply(
                        access_point=target,
                        text=text,
                        source="steward",
                        kind=self._kinds.command,
                    )
            except Exception as exc:
                self._logger.event(
                    "inspect_all_target_failed",
                    **self._hooks.access_point_fields(target),
                    source_access_point_type=source.type,
                    source_chat_id=source.chat_id,
                    source_thread_id=source.thread_id,
                    error=str(exc),
                    error_type=type(exc).__name__,
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
        result = self._agent_runtime.start_agent(access_point, resolved_spec)
        self._persist_registry("start_agent_action_starting", access_point)
        return result

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

        self._cancel_approval_on_agent_stop(access_point)
        self._reset_status_runtimes(access_point, False, reason="stop_agent_action")
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

    def _submit_steward_user_request(
        self,
        access_point: AccessPointKey,
        text: str,
        *,
        as_steer: bool = False,
    ) -> None:
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
            as_steer=as_steer,
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
        source_approval = self._approval_for_source(access_point)
        source_delegation = (
            source_approval if source_approval is not None and source_approval.phase == "delegated" else None
        )
        target_delegation = self._delegation_for_target(access_point)
        delegation = source_delegation or target_delegation
        delegation_role = "source" if source_delegation is not None else "target"
        approval_request = (
            source_approval.request
            if source_approval is not None
            else (target_delegation.request if target_delegation is not None else None)
        )
        rows: list[dict[str, str]]
        last_apply_info: dict | None
        has_active_turn: bool
        thread_metadata: dict[str, Any]
        steward_context_usage: dict[str, object] = {}
        agent_context_usage: dict[str, object] = {}
        steward = snapshot.steward
        if steward is not None:
            raw_usage = self._runtime.get_context_usage(access_point)
            if isinstance(raw_usage, dict):
                steward_context_usage = dict(raw_usage)
        if binding is not None:
            raw_usage = self._agent_runtime.get_context_usage(access_point)
            if isinstance(raw_usage, dict):
                agent_context_usage = dict(raw_usage)
        if binding is not None:
            rows = self._agent_runtime.get_item_status_snapshot(access_point)
            last_apply_info = self._agent_runtime.get_last_item_apply_info(access_point)
            has_active_turn = self._agent_runtime.has_active_turn(access_point)
            thread_metadata = self._agent_runtime.get_thread_metadata(access_point)
        else:
            rows = self._runtime.get_item_status_snapshot(access_point)
            last_apply_info = self._runtime.get_last_item_apply_info(access_point)
            has_active_turn = self._runtime.has_active_turn(access_point)
            thread_metadata = self._runtime.get_thread_metadata(access_point)

        active_turn_id = "none"
        if source_approval is not None:
            raw_turn_id = approval_request.params.get("turnId")
            if isinstance(raw_turn_id, str) and raw_turn_id.strip():
                active_turn_id = raw_turn_id.strip()
        elif has_active_turn and isinstance(last_apply_info, dict):
            raw_turn_id = last_apply_info.get("turn_id")
            if isinstance(raw_turn_id, str) and raw_turn_id.strip():
                active_turn_id = raw_turn_id.strip()

        lines = [
            "inspect",
            "access point:",
            format_access_point(snapshot.access_point),
            f"state: {snapshot.state}",
        ]
        lines.extend(
            [
                "",
                "steward runtime:",
                "role: steward",
                f"{ORC_RUNTIME_ID_LABEL}: "
                f"{steward.agent_id if steward is not None else 'none'}",
                f"state: {steward.state if steward is not None else 'NOT_STARTED'}",
                f"controllable: {'yes' if steward is not None and steward.controllable else 'no'}",
            ]
        )
        if steward is not None:
            lines.extend(
                [
                    f"configured model: {steward.configured_model or 'default'}",
                    f"effective model: {steward.effective_model or 'pending'}",
                    format_context_window_remaining(steward_context_usage),
                ]
            )
        lines.extend(
            [
                "",
                "bound agent:",
                "role: agent",
                f"{ORC_RUNTIME_ID_LABEL}: "
                f"{binding.agent_id if binding is not None else 'none'}",
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
                    format_context_window_remaining(agent_context_usage),
                ]
            )
            metadata_lines = format_thread_metadata_lines(
                thread_metadata,
                thread_id_fallback=binding.thread_id,
                thread_name_fallback=binding.thread_name,
                include_model=False,
            )
            if metadata_lines:
                lines.extend(["", "session:", *metadata_lines])
        lines.extend(["", "current turn:"])
        lines.append(f"active turn: {active_turn_id}")
        lines.append(
            f"pending approval: {'yes' if source_approval is not None else 'no'}"
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
        lines.extend(["", "active work:"])
        append_active_work_lines(lines, rows)
        return "\n".join(lines)

    def register_approval(self, event: StewardDriverEvent, request: ApprovalRequest) -> None:
        delegation = self._delegation_for_target(event.access_point)
        if event.source == "agent" and delegation is not None:
            delegation.on_target_approval(self, request)
            return
        parent = (
            self._agent_conversation_for(event.access_point) if event.source == "agent"
            else self._steward_conversation_for(event.access_point)
        )
        if parent is None:
            raise RuntimeError("approval request has no active parent conversation")
        parent.pending_approval_request = request
        child = ApprovalInteraction(parent=parent, request=request)
        self._interaction_queue.append(child)
        child.start(self)

    def approval_register_human(self, child: ApprovalInteraction) -> None:
        child.human_prompt = self._access_point_adapter.register_approval(
            access_point=child.source_access_point, source=child.source, request=child.request,
        )

    def approval_target(self, child: ApprovalInteraction) -> str:
        return self._agent_runtime.approval_target(child.source_access_point)

    def approval_source_address(self, child: ApprovalInteraction) -> str:
        return self._identity_address(child.source_access_point)

    def approval_resolve_target(self, address: str) -> ResolvedRoutingIdentity | None:
        return self._agent_runtime.resolve_routing_identity(address)

    def approval_target_wait_reason(self, target: AccessPointKey) -> str:
        return self._access_point_work_snapshot(target).approval_target_wait_reason()

    def approval_target_runtime_state(self, target: AccessPointKey) -> str:
        return self._agent_runtime.runtime_state(target)

    def approval_request_runtime_start(self, child: ApprovalInteraction) -> str:
        return self._request_agent_runtime_start(
            child.target_access_point,
            trigger_kind="approval",
        ).value

    def approval_submit_target(self, child: ApprovalInteraction, prompt: str) -> None:
        self._agent_runtime.submit_request(child.target_access_point, prompt)

    def approval_unavailable(self, child: ApprovalInteraction, reason: str, error: str = "") -> None:
        if reason == "invalid_configuration":
            message = "approval delegation unavailable (invalid_configuration); asking human."
        else:
            message = (
                f"approval delegation to {child.target_address} unavailable ({reason}); asking human."
            )
        self.approval_notify(child.source_access_point, message)
        self._logger.event(
            "approval_delegation_fallback", source_address=child.source_address,
            target_address=child.target_address, reason=reason,
            **({"error": error} if error else {}), fallback_target=HUMAN_APPROVAL_TARGET,
        )

    def approval_started(self, child: ApprovalInteraction, source_label: str) -> None:
        self.approval_notify(child.source_access_point, f"approval delegated to {child.target_address}.")
        self.approval_notify(
            child.target_access_point,
            format_approval_request_copy(
                source_label=source_label, target_address=child.target_address, request=child.request,
            ),
        )
        self._logger.event(
            "approval_delegation_started", approval_id=child.approval_id,
            source_address=child.source_address, target_address=child.target_address,
            method=child.request.method, req_id=child.request.req_id,
        )

    def approval_event(self, name: str, **fields: Any) -> None:
        self._logger.event(name, **fields)

    def approval_notify(self, access_point: AccessPointKey, text: str) -> None:
        self._queue_local_reply(access_point=access_point, text=text, source="agent", kind=self._kinds.reply)

    def approval_submit_source(self, child: ApprovalInteraction, decision: str) -> None:
        self._submit_approval_for_child(child, decision, now=True)

    def approval_decline_target_request(self, child: ApprovalInteraction, request: ApprovalRequest) -> None:
        try:
            self._agent_runtime.submit_approval_decision(
                child.target_access_point, "decline", expected_request=request,
            )
        except Exception as exc:
            self._logger.event(
                "approval_delegation_target_decline_failed", approval_id=child.approval_id,
                source_address=child.source_address, target_address=child.target_address,
                error=str(exc), error_type=type(exc).__name__,
            )

    def approval_cancel_target(self, child: ApprovalInteraction) -> None:
        if not self._agent_runtime.has_active_turn(child.target_access_point):
            return
        if child.target_cancel_requested:
            return
        try:
            self._agent_runtime.interrupt_active_turn(child.target_access_point)
        except Exception as exc:
            self._logger.event(
                "approval_delegation_target_interrupt_failed", approval_id=child.approval_id,
                source_address=child.source_address, target_address=child.target_address,
                error=str(exc), error_type=type(exc).__name__,
            )
            return
        child.target_cancel_requested = True
        self._logger.event(
            "approval_delegation_target_interrupt_requested", approval_id=child.approval_id,
            source_address=child.source_address, target_address=child.target_address,
        )

    def _cancel_approval_on_agent_stop(self, access_point: AccessPointKey) -> None:
        self._cancel_pending_approval(access_point, source="agent")
        source_child = self._agent_approval_for_source(access_point)
        if source_child is not None:
            source_child.cancel_source(self, "source_stopped")
        target_child = self._delegation_for_target(access_point)
        if target_child is not None:
            target_child.on_target_interrupted(self)
        conversation = self._agent_conversation_for(access_point)
        if conversation is not None:
            conversation.finish()

    def handle_approval_prompt_delivery_failed(self, access_point: AccessPointKey, exc: Exception) -> None:
        child = self._human_approval_for(access_point)
        if child is None:
            return
        child.on_human_delivery_failed(self, exc)

    def approval_human_delivery_failed(self, child: ApprovalInteraction, exc: Exception) -> None:
        access_point = child.source_access_point
        pending = child.human_prompt
        self._logger.event(
            "steward_approval_prompt_delivery_failed",
            **self._hooks.access_point_fields(access_point),
            route_target=pending.source,
            error=str(exc),
            error_type=type(exc).__name__,
            fallback_decision="decline",
        )
        if self._approval_parent_active(child):
            self._submit_approval_for_child(child, "decline")

    def steward_execute_actions(
        self, conversation: StewardConversation, actions: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        access_point = conversation.access_point
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
        )
        self._remember_resumable_action_results(action_results)
        startup_action_pending = any(
            str(item.get("type") or "").strip().upper() in {"START_AGENT", "RESUME_AGENT"} and bool(item.get("ok"))
            for item in action_results
        )
        if startup_action_pending:
            self._reset_status_runtimes(access_point, False, reason="startup_action")
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
                    },
                )
        self._emit_internal_action_notes(
            access_point=access_point,
            action_results=action_results,
        )
        return action_results

    def steward_event(self, conversation: StewardConversation, name: str, **fields: Any) -> None:
        self._logger.event(name, **self._hooks.access_point_fields(conversation.access_point), **fields)

    def steward_begin_startup_notice(
        self, conversation: StewardConversation, results: list[dict[str, Any]],
    ) -> None:
        self._begin_agent_startup_action_notice(conversation.access_point, results)

    def steward_submit_followup(self, conversation: StewardConversation, prompt: str) -> None:
        self._runtime.submit_request(conversation.access_point, prompt)

    def steward_queue_reply(self, conversation: StewardConversation, text: str) -> None:
        access_point = conversation.access_point
        self._access_point_adapter.queue_text_reply(
            access_point=access_point, text=text, source="steward", kind=self._kinds.reply,
            on_sent=lambda body=text, owner=conversation: self._complete_reply_delivery(
                conversation=owner, text=body,
            ),
            on_failed=lambda exc, owner=conversation: self._fail_reply_delivery(
                conversation=owner, exc=exc,
            ),
        )

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
        if isinstance(payload, InteractiveSteerSubmitted):
            confirmed = False
            if event.source == "agent":
                conversation = self._agent_conversation_for(event.access_point)
                confirmed = conversation is not None and conversation.confirm_steer(payload.prompt)
            self._logger.event(
                "steward_steer_submitted_confirmed",
                **self._hooks.access_point_fields(event.access_point),
                route_target=event.source,
                turn_id=payload.turn_id,
                prompt_preview=payload.prompt.strip()[:200],
                interaction_confirmed=confirmed,
            )
            return
        if isinstance(payload, InteractiveSteerFallbackActivated):
            if event.source == "agent":
                self._interaction_queue.append(
                    self._new_human_agent_conversation(event.access_point)
                )
            else:
                self._interaction_queue.append(
                    StewardConversation(
                        access_point=event.access_point,
                        allow_steer=True,
                    )
                )
            self._logger.event(
                "steward_steer_fallback_activated",
                **self._hooks.access_point_fields(event.access_point),
                route_target=event.source,
                turn_id=payload.turn_id,
                prompt_preview=payload.prompt.strip()[:200],
            )
            return
        if event.source == "agent" and isinstance(payload, InteractiveAgentResult):
            conversation = self._agent_conversation_for(event.access_point)
            if conversation is not None:
                conversation.handle_result(self, payload)
                return
        if event.source == "steward" and isinstance(payload, InteractiveAgentResult):
            conversation = self._steward_conversation_for(event.access_point)
            if conversation is not None and conversation.interrupt_notice is None:
                self._cancel_pending_approval(event.access_point, source="steward")
                self._logger.event(
                    "steward_driver_result_received",
                    **self._hooks.access_point_fields(event.access_point),
                    route_target="steward", reply_preview=str(payload.reply or "").strip()[:200],
                )
                conversation.handle_result(self, payload)
                return
        self._apply_driver_event(event)

    def _apply_driver_event(self, event: StewardDriverEvent) -> None:
        payload = event.event
        if isinstance(payload, InteractiveStatusEvent):
            if payload.suppress_status:
                return
            if event.source == "agent":
                conversation = self.active_conversation_for(event.access_point)
                handoff = conversation.routed_handoff if isinstance(conversation, AgentConversation) else None
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
            self._reset_status_runtimes(
                event.access_point,
                False,
                reason="runtime_failure",
            )
            operation = self.active_conversation_for(event.access_point)
            self._cancel_pending_approval(event.access_point, source="agent")
            source_child = self._agent_approval_for_source(event.access_point)
            if source_child is not None:
                source_child.cancel_source(self, "source_runtime_failed")
            target_child = self._delegation_for_target(event.access_point)
            if target_child is not None:
                target_child.on_target_interrupted(self)
            conversation = self._agent_conversation_for(event.access_point)
            if conversation is not None:
                conversation.finish()
            if (operation is not None and operation.source != "agent") or (
                operation is None and target_child is None
            ):
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
            source_delegation = self._agent_approval_for_source(event.access_point)
            if source_delegation is not None and event.source == "agent":
                source_delegation.cancel_source(self, "source_interrupted")
            delegation = self._delegation_for_target(event.access_point)
            if delegation is not None and event.source == "agent":
                delegation.on_target_interrupted(self)
            pending_interrupt = self._pending_interrupt_for(event.access_point)
            if pending_interrupt is not None and pending_interrupt.source == event.source:
                pending_interrupt.set_result(
                    self,
                    text=self._interrupt_final_text(success=True),
                    outcome=LocalCommandOutcome(
                        command="/interrupt",
                        result={"ok": True, "code": "interrupted", "target": event.source},
                    ),
                )
            self._cancel_pending_approval(event.access_point, source=event.source)
            operation = self._finish_conversation(event.access_point, source=event.source)
            handoff = operation.routed_handoff if isinstance(operation, AgentConversation) else None
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
            pending_interrupt = self._pending_interrupt_for(event.access_point)
            if pending_interrupt is not None and pending_interrupt.source == event.source:
                source_child = self._agent_approval_for_source(event.access_point)
                if source_child is not None and event.source == "agent":
                    source_child.cancel_source(self, "source_interrupted")
                self._cancel_pending_approval(event.access_point, source=event.source)
                cancelled_delegation = self._delegation_for_target(event.access_point)
                if cancelled_delegation is not None and cancelled_delegation.source_cancelled:
                    cancelled_delegation.finish()
                self._logger.event(
                    "steward_interrupt_reply_suppressed",
                    **self._hooks.access_point_fields(event.access_point),
                    route_target=event.source,
                    reply_preview=reply_preview,
                )
                if pending_interrupt.final_text is None:
                    pending_interrupt.suppress_reply(self, text=self._interrupt_final_text(success=False))
                self._finish_conversation(event.access_point, source=event.source)
                return
            self._cancel_pending_approval(event.access_point, source=event.source)
            source_child = self._agent_approval_for_source(event.access_point)
            if source_child is not None and event.source == "agent":
                source_child.cancel_source(self, "source_finished")
            self._logger.event(
                "steward_driver_result_received",
                **self._hooks.access_point_fields(event.access_point),
                route_target=event.source,
                reply_preview=reply_preview,
            )
            if event.source == "agent":
                delegation = self._delegation_for_target(event.access_point)
                if delegation is not None:
                    if self._approval_parent_active(delegation) or delegation.source_cancelled:
                        if reply_text.startswith("agent error: "):
                            delegation.on_target_interrupted(self)
                        else:
                            delegation.on_target_result(self, reply_text)
                    else:
                        delegation.finish()
                return
            # Direct lifecycle operations do not own a Steward model result.
            return
        raise AssertionError(f"unsupported steward driver event: {payload!r}")

    def agent_result_suppressed(self, conversation: AgentConversation, preview: str) -> None:
        pending = conversation.interrupt_notice
        if pending is None:
            raise AssertionError("agent result suppressed without a pending interrupt")
        self._cancel_pending_approval(conversation.access_point, source="agent")
        source_child = self._agent_approval_for_source(conversation.access_point)
        if source_child is not None:
            source_child.cancel_source(self, "source_interrupted")
        cancelled_delegation = self._delegation_for_target(conversation.access_point)
        if cancelled_delegation is not None and cancelled_delegation.source_cancelled:
            cancelled_delegation.finish()
        self._logger.event(
            "steward_interrupt_reply_suppressed",
            **self._hooks.access_point_fields(conversation.access_point),
            route_target="agent",
            reply_preview=preview,
        )
        if pending.final_text is None:
            pending.suppress_reply(self, text=self._interrupt_final_text(success=False))

    def agent_result_received(self, conversation: AgentConversation, preview: str) -> None:
        self._cancel_pending_approval(conversation.access_point, source="agent")
        source_child = self._agent_approval_for_source(conversation.access_point)
        if source_child is not None:
            source_child.cancel_source(self, "source_finished")
        self._logger.event(
            "steward_driver_result_received",
            **self._hooks.access_point_fields(conversation.access_point),
            route_target="agent",
            reply_preview=preview,
        )

    def agent_turn_error(self, conversation: AgentConversation, error: str) -> None:
        self._logger.event(
            self._hooks.turn_error_event,
            **self._hooks.access_point_fields(conversation.access_point),
            route_target="agent",
            error=error,
            error_type="RuntimeError",
        )

    def agent_shaman_address(self, access_point: AccessPointKey) -> str:
        return self._shaman_address(access_point)

    def agent_grunt_address(self, access_point: AccessPointKey) -> str:
        return self._grunt_address(access_point)

    def agent_resolve_target(self, address: str) -> ResolvedRoutingIdentity | None:
        return self._agent_runtime.resolve_routing_identity(address)

    def agent_routing_event(
        self,
        name: str,
        access_point: AccessPointKey,
        *,
        target_access_point: AccessPointKey | None = None,
        prefix: str = "sender",
        **fields: Any,
    ) -> None:
        self._logger.event(
            name,
            **self._routing_access_point_fields(access_point, prefix=prefix),
            **(
                self._routing_access_point_fields(target_access_point, prefix="target")
                if target_access_point is not None else {}
            ),
            **fields,
        )

    def agent_escalate_routing_failure(
        self,
        conversation: AgentConversation,
        *,
        sender_address: str,
        target_address: str,
        error_code: str,
        details: str,
        turn_id: str,
        target_access_point: AccessPointKey | None = None,
        target_state: str = "",
    ) -> None:
        if target_access_point is not None and not target_state:
            target_state = self._agent_runtime.runtime_state(target_access_point)
        self._escalate_routing_failure(
            sender_access_point=conversation.access_point,
            sender_address=sender_address,
            target_address=target_address,
            error_code=error_code,
            details=details,
            target_access_point=target_access_point,
            target_state=target_state,
            turn_id=turn_id,
        )

    def agent_send_reply(self, conversation: AgentConversation, text: str) -> None:
        self._access_point_adapter.queue_text_reply(
            access_point=conversation.access_point,
            text=text,
            source="agent",
            kind=self._kinds.reply,
            on_sent=lambda body=text, owner=conversation: self._complete_reply_delivery(
                conversation=owner, text=body,
            ),
            on_failed=lambda exc, owner=conversation: self._fail_reply_delivery(
                conversation=owner, exc=exc,
            ),
        )

    def agent_submit_request(self, access_point: AccessPointKey, prompt: str) -> None:
        self._agent_runtime.submit_request(access_point, prompt)

    def agent_send_notice(self, conversation: AgentConversation, text: str) -> None:
        self._queue_local_reply(
            access_point=conversation.access_point,
            text=text,
            source="steward",
            kind=self._kinds.reply,
        )

    def agent_deliver_to_human(
        self,
        conversation: AgentConversation,
        *,
        delivery_access_point: AccessPointKey,
        sender_address: str,
        text: str,
    ) -> None:
        source_access_point = conversation.access_point
        self._logger.event(
            "routing_response_completed",
            **self._routing_access_point_fields(source_access_point, prefix="sender"),
            sender_address=sender_address,
            target_address=HUMAN_ADDRESS,
            turn_id=conversation.routing.turn_id,
            repair_attempt=conversation.routing.repair_attempt,
        )
        self._access_point_adapter.queue_text_reply(
            access_point=delivery_access_point,
            text=text,
            source="agent",
            kind=self._kinds.reply,
            on_sent=lambda body=text, owner=conversation: self._complete_reply_delivery(
                conversation=owner, text=body,
            ),
            on_failed=lambda exc, owner=conversation: self._fail_reply_delivery(
                conversation=owner, exc=exc,
            ),
        )

    def agent_dispatch_routing(
        self,
        conversation: AgentConversation,
        *,
        sender_address: str,
        reply_text: str,
        decision: RoutingDispatch,
    ) -> None:
        sender_access_point = conversation.access_point
        target_identity = decision.target
        target_access_point = target_identity.binding
        envelope = conversation.routed_envelope_for(target_access_point, decision.envelope)
        routed_reply_text = (
            reply_text
            if envelope is decision.envelope
            else format_routing_envelope(envelope.sender, envelope.target, envelope.body)
        )
        output_kind = self._kinds.reply
        target_is_grunt = target_identity.mode == ROUTING_MODE_GRUNT
        sequence = self._interaction_queue.next_sequence()
        sender_is_grunt = bool(self._grunt_address(sender_access_point))
        sender_copy_text = (
            self._format_grunt_routing_copy(routed_reply_text)
            if sender_is_grunt
            else routed_reply_text
        )
        self._queue_routing_access_point_copy(
            access_point=sender_access_point,
            text=sender_copy_text,
            kind=output_kind,
            sender_address=sender_address,
            target_address=envelope.target,
            turn_id=conversation.routing.turn_id,
            repair_attempt=conversation.routing.repair_attempt,
            copy_role="sender",
        )
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
                source_turn_id=conversation.routing.turn_id,
                error_code="routing_queue_full",
                target_state=target_state,
            )
            self._escalate_routing_failure(
                sender_access_point=sender_access_point,
                sender_address=sender_address,
                target_address=envelope.target,
                error_code="routing_queue_full",
                details=f"routing queue reached capacity {self._routing_queue_capacity}",
                target_access_point=target_access_point,
                target_state=target_state,
                turn_id=conversation.routing.turn_id,
            )
            return
        request = RoutedConversation(
            sequence=sequence,
            sender_access_point=sender_access_point,
            sender_address=sender_address,
            sender_mode=(ROUTING_MODE_GRUNT if sender_is_grunt else ROUTING_MODE_SHAMAN),
            target_access_point=target_access_point,
            target_address=envelope.target,
            target_mode=target_identity.mode,
            target_input=envelope.body if target_is_grunt else routed_reply_text,
            source_turn_id=conversation.routing.turn_id,
            repair_attempt=conversation.routing.repair_attempt,
            routed_reply_to=(
                RoutedReplyTo(
                    access_point=sender_access_point,
                    address=sender_address,
                )
                if target_is_grunt
                else None
            ),
        )
        self._interaction_queue.append(request)
        if target_access_point != sender_access_point:
            self._queue_routing_access_point_copy(
                access_point=target_access_point,
                text=routed_reply_text,
                kind=output_kind,
                sender_address=sender_address,
                target_address=envelope.target,
                turn_id=conversation.routing.turn_id,
                repair_attempt=conversation.routing.repair_attempt,
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
            turn_id=conversation.routing.turn_id,
            repair_attempt=conversation.routing.repair_attempt,
        )

    @staticmethod
    def _format_grunt_routing_copy(envelope_text: str) -> str:
        lines = str(envelope_text or "").splitlines()
        if len(lines) < 4 or not lines[0].startswith("FROM: ") or not lines[1].startswith("TO: "):
            return envelope_text
        return "\n".join((f"> {lines[0]}", f"> {lines[1]}", *lines[2:]))

    def _live_routing_queue_size(self) -> int:
        return sum(
            isinstance(request, RoutedConversation) and request.failure is None
            for request in self._interaction_queue.snapshot()
        )

    def routing_identity(self, address: str) -> ResolvedRoutingIdentity | None:
        return self._agent_runtime.resolve_routing_identity(address)

    def routing_runtime_state(self, access_point: AccessPointKey) -> str:
        return self._agent_runtime.runtime_state(access_point)

    def routing_request_runtime_start(self, request: RoutedConversation) -> None:
        self._request_agent_runtime_start(
            request.target_access_point,
            trigger_kind="routing",
        )

    def _routing_request_event_fields(self, request: RoutedConversation) -> dict[str, Any]:
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

    def tick_interactions(self) -> bool:
        return self._interaction_queue.tick(InteractionTickContext(runtime=self))

    def routing_target_wait_reason(self, access_point: AccessPointKey, state: str) -> str:
        return self._access_point_work_snapshot(
            access_point, runtime_state=state,
        ).routing_target_wait_reason()

    def routing_submit(self, request: RoutedConversation) -> None:
        target = request.target_access_point
        self._agent_runtime.submit_request(target, request.target_input)
        self._interaction_queue.append(
            AgentConversation(
                access_point=target,
                routed_handoff=RoutedHandoff(
                    sender_address=request.sender_address,
                    target_address=request.target_address,
                    source_turn_id=request.source_turn_id,
                ),
                routed_reply_to=request.routed_reply_to,
            )
        )

    def routing_dispatched(self, request: RoutedConversation) -> None:
        self._logger.event("routing_request_dispatched", **self._routing_request_event_fields(request))

    def routing_defer(
        self,
        request: RoutedConversation,
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
            kind=self._kinds.reply,
        )

    def routing_fail(
        self,
        request: RoutedConversation,
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

    def routing_sender_busy(self, sender: AccessPointKey) -> bool:
        return self._access_point_work_snapshot(sender).routing_sender_busy

    def routing_escalate(self, request: RoutedConversation) -> None:
        failure = request.failure
        if failure is None:
            raise AssertionError("routed interaction completed without a failure")
        self._escalate_routing_failure(
            sender_access_point=request.sender_access_point,
            sender_address=request.sender_address,
            target_address=request.target_address,
            error_code=failure.code,
            details=failure.details,
            target_access_point=request.target_access_point,
            target_state=failure.target_state,
            turn_id=request.source_turn_id,
        )

    def _fail_routing_requests_for_target(
        self,
        target_access_point: AccessPointKey,
        *,
        error_code: str,
        details: str,
        target_state: str,
    ) -> None:
        for request in self._interaction_queue.snapshot():
            if not isinstance(request, RoutedConversation):
                continue
            if request.target_access_point != target_access_point or request.failure is not None:
                continue
            self.routing_fail(
                request,
                RoutingFailure(
                    code=error_code,
                    details=details,
                    target_state=target_state,
                ),
            )

    def _escalate_routing_failure(
        self,
        *,
        sender_access_point: AccessPointKey,
        sender_address: str,
        target_address: str,
        error_code: str,
        details: str,
        target_access_point: AccessPointKey | None = None,
        target_state: str = "",
        turn_id: str = "",
    ) -> None:
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
        self._interaction_queue.append(
            StewardConversation(
                access_point=sender_access_point, allow_steer=False,
            )
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

    def _complete_reply_delivery(
        self, *, conversation: AgentConversation | StewardConversation, text: str,
    ) -> None:
        access_point = conversation.access_point
        source = conversation.source
        decorated = self._access_point_adapter.decorate_reply(text=text, source=source)
        self._logger.event(
            self._hooks.response_sent_event,
            **self._hooks.access_point_fields(access_point),
            text=decorated,
        )
        self._writer(self._hooks.write_output_line(access_point, text))
        if isinstance(conversation, AgentConversation) and conversation.routing.target_address:
            self._logger.event(
                "routing_message_delivered",
                **self._routing_access_point_fields(access_point, prefix="sender"),
                sender_address=conversation.routing.sender_address,
                target_address=conversation.routing.target_address,
                turn_id=conversation.routing.turn_id,
                repair_attempt=conversation.routing.repair_attempt,
            )
        conversation.finish()

    def _fail_reply_delivery(
        self, *, conversation: AgentConversation | StewardConversation, exc: Exception,
    ) -> None:
        access_point = conversation.access_point
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
        conversation.finish()

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
        if self._runtime.consume_poll_progress():
            progressed = True
        _drain(self._agent_runtime.poll_once())
        if self._agent_runtime.consume_poll_progress():
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

    def _load_agent_catalog(
        self,
        *,
        access_point: AccessPointKey,
        scope: str,
    ) -> tuple[str, list[AgentCatalogEntry]] | None:
        target_cwd = ""
        if scope == "here":
            binding = self._agent_runtime.get_binding_info(access_point) or {}
            target_cwd = resolve_catalog_cwd(str(binding.get("cwd") or ""))
            if not target_cwd:
                return None
        sessions = list_codex_cli_sessions(
            project_cwd=target_cwd or None,
            sessions_root=self._sessions_root,
            limit=None,
        )
        return (
            target_cwd,
            build_agent_catalog(
                sessions=sessions,
                bindings=self._agent_runtime.snapshot_persisted(),
                target_cwd=target_cwd,
            ),
        )

    def _execute_agents_command(
        self,
        *,
        access_point: AccessPointKey,
        command: AgentsCommand,
    ) -> tuple[list[str], dict[str, Any]]:
        catalog = self._load_agent_catalog(access_point=access_point, scope=command.scope)
        if catalog is None:
            return (
                [
                    "agents here unavailable: current access point has no folder. "
                    "Use /agents all."
                ],
                {"ok": False, "code": "no_current_folder", "scope": command.scope},
            )
        target_cwd, records = catalog
        self._logger.event(
            "agents_command_executed",
            **self._hooks.access_point_fields(access_point),
            scope=command.scope,
            cwd=target_cwd,
            count=len(records),
        )
        return (
            format_agent_catalog_chunks(
                scope=command.scope,
                records=records,
                message_char_limit=self._AGENTS_MESSAGE_CHAR_LIMIT,
            ),
            {"ok": True, "code": "listed", "scope": command.scope, "count": len(records)},
        )

    def _execute_resume_command(
        self,
        *,
        access_point: AccessPointKey,
        command: ResumeCommand,
        agent_state: str,
        raw_command: str,
    ) -> tuple[list[str], dict[str, Any]] | None:
        block_reason = self._access_point_work_snapshot(
            access_point,
            runtime_state=agent_state,
        ).resume_block_reason()
        if block_reason:
            message = {
                "approval_pending": (
                    "resume unavailable: an approval is pending. "
                    "Resolve it before resuming another session."
                ),
                "approval_delegation_active": (
                    "resume unavailable: approval delegation is active. "
                    "Wait for it to finish or use /interrupt."
                ),
                "lifecycle_active": (
                    "resume unavailable: a lifecycle operation is in progress. "
                    "Wait for it to finish."
                ),
                "interrupt_active": (
                    "resume unavailable: an interrupt is in progress. "
                    "Wait for it to finish."
                ),
                "steward_active": (
                    "resume unavailable: access point has active steward work. "
                    "Wait for it to finish or use /interrupt."
                ),
                "agent_active": (
                    "resume unavailable: access point has active agent work. "
                    "Wait for it to finish or use /interrupt."
                ),
                "routing_active": (
                    "resume unavailable: access point has pending routing work. "
                    "Wait for it to finish."
                ),
                "runtime_starting": (
                    "resume unavailable: current access point runtime is STARTING. "
                    "Use /stop before resuming another session."
                ),
                "runtime_running": (
                    "resume unavailable: current access point runtime is RUNNING. "
                    "Use /stop before resuming another session."
                ),
                "runtime_unknown": (
                    f"resume unavailable: current access point runtime is {agent_state}. "
                    "Resolve the runtime state before resuming another session."
                ),
            }[block_reason]
            return (
                [message],
                {
                    "ok": False,
                    "code": "access_point_busy",
                    "state": agent_state,
                    "reason": block_reason,
                },
            )
        catalog = self._load_agent_catalog(access_point=access_point, scope=command.scope)
        if catalog is None:
            return (
                [
                    "resume here unavailable: current access point has no folder. "
                    "Use /resume all <name|uuid>."
                ],
                {"ok": False, "code": "no_current_folder", "scope": command.scope},
            )
        _target_cwd, records = catalog
        matches = resolve_agent_catalog_entries(records, command.selector)
        if not matches:
            return (
                [f"resume target not found ({command.scope}): {command.selector}"],
                {
                    "ok": False,
                    "code": "not_found",
                    "scope": command.scope,
                    "selector": command.selector,
                },
            )
        if len(matches) > 1:
            replies = format_agent_catalog_chunks(
                scope=command.scope,
                records=matches,
                message_char_limit=self._AGENTS_MESSAGE_CHAR_LIMIT,
            )
            replies[0] = (
                f"resume target is ambiguous ({command.scope}): {command.selector}\n\n"
                f"{replies[0]}"
            )
            replies[-1] += f"\n\nUse /resume {command.scope} <uuid>."
            return (
                replies,
                {
                    "ok": False,
                    "code": "ambiguous",
                    "scope": command.scope,
                    "selector": command.selector,
                    "count": len(matches),
                },
            )

        selected = matches[0]
        other_access_points = [
            item for item in selected.access_points if item != access_point
        ]
        if other_access_points:
            reply = (
                "resume unavailable: selected session is already bound to another "
                "access point.\n\n"
                + format_agent_catalog_entry(selected)
            )
            return (
                [reply],
                {
                    "ok": False,
                    "code": "bound_elsewhere",
                    "scope": command.scope,
                    "thread_id": selected.session_id,
                },
            )
        if not selected.cwd:
            return (
                [
                    f"resume unavailable: {selected.thread_name or selected.session_id} "
                    "has no folder."
                ],
                {
                    "ok": False,
                    "code": "missing_folder",
                    "thread_id": selected.session_id,
                },
            )

        pending = LocalLifecycleOperation(
            access_point=access_point,
            operation="RESUME_AGENT",
            thread_name=selected.thread_name,
            runtime_thread_id=selected.session_id,
            cwd=selected.cwd,
        )
        self._begin_agent_startup_notice(
            access_point,
            operation=pending.operation,
            initial_text=self._agent_startup_action_initial_text(pending),
            thread_name=pending.thread_name,
            runtime_thread_id=pending.runtime_thread_id,
            cwd=pending.cwd,
            local_command=raw_command,
        )
        try:
            current_binding = self._agent_runtime.get_binding_info(access_point) or {}
            spec = {
                "cwd": selected.cwd,
                "thread_id": selected.session_id,
                "thread_name": selected.thread_name,
                "mode": "proxy",
            }
            approval_target = str(current_binding.get("approval_target") or "").strip()
            if approval_target:
                spec["approval_target"] = approval_target
            self._agent_runtime.start_agent(
                access_point,
                spec,
            )
            self._persist_registry("resume_command_starting", access_point)
            self._reset_status_runtimes(access_point, False, reason="resume_command")
            self._logger.event(
                "resume_command_started",
                **self._hooks.access_point_fields(access_point),
                scope=command.scope,
                selector=command.selector,
                runtime_thread_id=selected.session_id,
                thread_name=selected.thread_name,
                cwd=selected.cwd,
            )
        except Exception as exc:
            self._logger.event(
                "resume_command_failed",
                **self._hooks.access_point_fields(access_point),
                scope=command.scope,
                selector=command.selector,
                runtime_thread_id=selected.session_id,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            self._handle_agent_startup_result(
                access_point,
                self._agent_runtime.startup_failure_result(
                    access_point,
                    error=str(exc),
                    default_cwd=selected.cwd,
                ),
            )
        return None

    def _execute_approver_command(
        self,
        *,
        access_point: AccessPointKey,
        command: ApproverCommand,
    ) -> tuple[str, dict[str, Any]]:
        operation = command.operation
        action_type = f"APPROVAL_TARGET_{operation.upper()}"
        action: dict[str, Any] = {"type": action_type}
        if operation == "assign":
            action["approval_target"] = command.address
        result = execute_approval_target_action(
            action,
            assign_provider=lambda target: self._agent_runtime.assign_approval_target(
                access_point, target
            ),
            show_provider=lambda: self._agent_runtime.show_approval_target(access_point),
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
        pending_approval = self._approval_for_source(access_point)
        if operation == "show":
            lines = [f"approver: {approval_target}"]
            command_result = {
                "ok": True,
                "code": "shown",
                "approval_target": approval_target,
            }
            if (
                pending_approval is not None
                and pending_approval.waiting_target != approval_target
            ):
                lines.append(
                    "current approval request still waits for "
                    f"{pending_approval.waiting_target}."
                )
                command_result["pending_approval_target"] = pending_approval.waiting_target
            return "\n".join(lines), command_result

        previous_target = str(
            result.get("previous_approval_target")
            or HUMAN_APPROVAL_TARGET
        )
        persisted = self._persist_registry("approver_command", access_point)
        qualifier = "" if persisted else " in memory"
        lines = [
            f"approver assigned{qualifier}: {previous_target} -> {approval_target}",
            f"persisted: {'yes' if persisted else 'no'}",
        ]
        if not persisted:
            lines.append("registry persistence failed; the change will be lost on restart.")
        if pending_approval is not None:
            lines.append(
                "current approval request still waits for "
                f"{pending_approval.waiting_target}."
            )
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
        if pending_approval is not None:
            command_result["pending_approval_target"] = pending_approval.waiting_target
        return "\n".join(lines), command_result

    def handle_text_update(self, inbound: StewardInboundText) -> None:
        access_point = inbound.access_point
        self._logger.event(
            "steward_inbound_text_handling",
            **self._hooks.access_point_fields(access_point),
            text_preview=inbound.text.strip()[:200],
        )
        self._access_point_adapter.drop_pending_status_updates(
            access_point=access_point,
            include_steward=True,
        )
        snapshot = self._runtime_snapshot(access_point)
        agent_state = snapshot.state
        is_bound = agent_state != "UNBOUND"
        is_running_bound = agent_state == "RUNNING"
        parsed_command = parse_steward_user_command(inbound.text)
        fallback_cmd = extract_fallback_command(inbound.text)
        self._logger.event(
            "steward_inbound_state_snapshot",
            **self._hooks.access_point_fields(access_point),
            runtime_state=agent_state,
            is_bound=is_bound,
            is_running_bound=is_running_bound,
            fallback_cmd=fallback_cmd,
        )
        if parsed_command.is_unknown:
            self._logger.event(
                "unknown_user_command_rejected",
                **self._hooks.access_point_fields(access_point),
                command=parsed_command.name,
            )
            self._queue_local_command_reply(
                access_point=access_point,
                outcome=LocalCommandOutcome(
                    command=str(inbound.text or "").strip(),
                    result={"ok": False, "code": "unknown_command"},
                ),
                text=UNKNOWN_USER_COMMAND_TEXT,
                kind=self._kinds.warning,
            )
            return
        if fallback_cmd == "help":
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            self._queue_local_command_reply(
                access_point=access_point,
                outcome=LocalCommandOutcome(
                    command="/help",
                    result={"ok": True, "code": "shown", "state": agent_state},
                ),
                text=self._access_point_adapter.build_help_text(
                    state=agent_state,
                    startup=self._help_startup_context(
                        access_point=access_point,
                        snapshot=snapshot,
                    ),
                ),
                kind=self._kinds.command,
            )
            return
        if fallback_cmd == "agents":
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            command = parse_agents_command(inbound.text)
            if command.usage_error:
                replies = [command.usage_error]
                command_result = {"ok": False, "code": "usage_error"}
                kind = self._kinds.warning
            else:
                replies, command_result = self._execute_agents_command(
                    access_point=access_point,
                    command=command,
                )
                kind = self._kinds.command if command_result["ok"] else self._kinds.warning
            self._queue_local_command_reply_chunks(
                access_point=access_point,
                outcome=LocalCommandOutcome(
                    command=str(inbound.text or "").strip(),
                    result=command_result,
                ),
                texts=replies,
                kind=kind,
            )
            return
        if fallback_cmd == "resume":
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            command = parse_resume_command(inbound.text)
            if command.usage_error:
                replies = [command.usage_error]
                command_result = {"ok": False, "code": "usage_error"}
            else:
                execution = self._execute_resume_command(
                    access_point=access_point,
                    command=command,
                    agent_state=agent_state,
                    raw_command=str(inbound.text or "").strip(),
                )
                if execution is None:
                    return
                replies, command_result = execution
            self._queue_local_command_reply_chunks(
                access_point=access_point,
                outcome=LocalCommandOutcome(
                    command=str(inbound.text or "").strip(),
                    result=command_result,
                ),
                texts=replies,
                kind=(
                    self._kinds.command
                    if command_result.get("code") in {"ambiguous", "bound_elsewhere"}
                    else self._kinds.warning
                ),
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
            command = parse_inspect_command(inbound.text)
            if command.usage_error:
                self._queue_local_command_reply(
                    access_point=access_point,
                    outcome=LocalCommandOutcome(
                        command=str(inbound.text or "").strip(),
                        result={"ok": False, "code": "usage_error"},
                    ),
                    text=command.usage_error,
                    kind=self._kinds.warning,
                )
            elif command.scope == "all":
                self._broadcast_inspect(access_point)
            else:
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
                    self._persist_registry("bind_command_starting", access_point)
                    self._reset_status_runtimes(access_point, False, reason="bind_command")
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
                if stopped:
                    self._cancel_approval_on_agent_stop(access_point)
                    self._reset_status_runtimes(access_point, False, reason="stop_command")
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
                deferred_start = (
                    self._agent_runtime.start_decision(access_point)
                    is AgentRuntimeStartDecision.DEFERRED
                )
                if deferred_start:
                    stopped = self._agent_runtime.stop_agent(access_point)
                    persisted = not stopped or self._persist_registry("stop_command", access_point)
                    reply = "runtime agent stopped. state: BOUND_IDLE."
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
                else:
                    reply = "runtime agent is already stopped (state: BOUND_IDLE)."
                    command_result = {"ok": True, "code": "already_stopped", "state": "BOUND_IDLE"}
            else:
                reply = "no bound runtime agent. Use /bind to create one."
                command_result = {"ok": False, "code": "no_binding", "state": "UNBOUND"}
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            pending_startup = self._lifecycle_operation_for(access_point)
            if pending_startup is not None and agent_state == "STARTING":
                pending_startup.replace_with_command(
                    self, operation="stop",
                    outcome=LocalCommandOutcome(command="/stop", result=command_result),
                    text=reply,
                )
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
                if self._lifecycle_operation_for(access_point) is None:
                    reply = "runtime agent is already starting for this access point."
                    command_result = {"ok": True, "code": "already_starting", "state": "STARTING"}
            elif is_bound:
                self._begin_bound_agent_startup_notice(access_point)
                try:
                    self._agent_runtime.start_bound_agent(access_point)
                    self._persist_registry("start_command_starting", access_point)
                    self._reset_status_runtimes(access_point, False, reason="start_command")
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
            self._reset_status_runtimes(access_point, True, reason="reset_command")
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
                self._pending_uploads_by_access_point.pop(access_point, None)
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
            pending_startup = self._lifecycle_operation_for(access_point)
            if pending_startup is not None:
                pending_startup.replace_with_command(
                    self, operation="reset",
                    outcome=LocalCommandOutcome(command="/reset", result=command_result),
                    text=reply,
                )
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
                self._submit_steward_user_request(
                    access_point,
                    self._input_with_uploads(inbound, steward_once_payload),
                )
                self._interaction_queue.append(
                    StewardConversation(
                        access_point=access_point, allow_steer=False,
                    )
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
                prompt = self._format_human_agent_input(
                    access_point,
                    self._input_with_uploads(inbound),
                )
                self._agent_runtime.submit_request(access_point, prompt)
                self._interaction_queue.append(
                    self._new_human_agent_conversation(access_point)
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
        self._submit_steward_user_request(
            access_point,
            self._input_with_uploads(inbound),
        )
        self._interaction_queue.append(
            StewardConversation(
                access_point=access_point, allow_steer=is_bound,
            )
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
