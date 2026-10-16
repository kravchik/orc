from __future__ import annotations

from dataclasses import dataclass, replace
import json
from pathlib import Path
import time
from typing import Any, Callable, Protocol

from orchestrator import clock
from orchestrator.access_point_common import (
    AccessPointKey,
    AccessPointMessageRef,
    access_point_sort_key,
    format_access_point,
)
from orchestrator.access_point_work import AccessPointWorkSnapshot
from orchestrator.agent_capabilities import (
    AgentCapability,
    unsupported_capability_text,
)
from orchestrator.agent_io import AgentInput, AgentInputOrigin
from orchestrator.agent_identity import (
    InspectionRef,
    ProviderExecutionProfile,
    ProviderKind,
    ProviderProfileId,
    ProviderSessionRef,
    TurnRef,
)
from orchestrator.runtime_profile_catalog import (
    BUILTIN_RUNTIME_PROFILE_CATALOG,
    RuntimeProfileCatalog,
)
from orchestrator.provider_installation import (
    ProviderInstallationStatus,
    ProviderInstallationStatusKind,
)
from orchestrator.agent_inspection import (
    AgentRuntimeProjection,
    SnapshotAvailability,
    projection_with_session_error,
)
from orchestrator.agent_session import (
    AgentFormRequest,
    ApprovalDecisionResult,
    AgentEffectOutcome,
    AgentEffectResult,
    ElicitationAction,
    RawProviderErrorPayload,
    WorkStatusKind,
    raw_provider_error_lifecycle_fields,
)
from orchestrator.agent_catalog import (
    AgentCatalogFailure,
    AgentCatalogListing,
    agent_catalog_failure_payloads,
    build_agent_catalog,
    format_agent_catalog_failure,
    format_agent_catalog_chunks,
    format_agent_catalog_entry,
    resolve_agent_catalog_entries,
    resolve_catalog_cwd,
)
from orchestrator.agent_catalog_interactions import (
    AgentCatalogInteraction,
    AgentResumeInteraction,
    CatalogInteractionKind,
    CatalogOperationCapacityError,
    CatalogOperationExecutor,
    CatalogOperationHandle,
    CatalogOperationProgressContext,
    catalog_access_point_owner_ref,
)
from orchestrator.agent_session import AgentApprovalRequest
from orchestrator.agent_provider import (
    ListProviderSessions,
    ProviderSessionCatalogEntry,
    ProviderSessionCatalogErrorKind,
    ProviderSessionsListFailed,
    ProviderSessionsListed,
    ProviderSessionsListResult,
    ProviderUnavailableError,
)
from orchestrator.approval_delegation import format_approval_request_copy
from orchestrator.approval_interactions import ApprovalInteraction
from orchestrator.form_elicitation_interactions import FormElicitationInteraction
from orchestrator.approval_target import HUMAN_APPROVAL_TARGET
from orchestrator.agent_interactions import AgentConversation, RoutedHandoff
from orchestrator.agent_turn_interactions import (
    AgentSteerInteraction,
    AgentTurnInteraction,
    TerminalAcceptance,
)
from orchestrator.delivery_errors import extract_http_code
from orchestrator.inspect_interactions import InspectInteraction
from orchestrator.inspect_text import (
    append_background_snapshot_lines,
    append_work_snapshot_lines,
    format_context_snapshot,
    format_session_snapshot_lines,
)
from orchestrator.interaction_queue import InteractionQueue, InteractionTickContext
from orchestrator.interactive_driver_events import (
    ApprovalDeliveryReceipt,
    InteractiveAgentResult,
    InteractiveApprovalPromptEvent,
    InteractiveElicitationPromptEvent,
    InteractiveEffortApplyResult,
    InteractiveInterruptResult,
    InteractiveInspectionResult,
    InteractiveModelApplyResult,
    InteractiveOutboundNote,
    InteractiveRuntimeFailure,
    InteractiveStartupResult,
    InteractiveStatusEvent,
    StatusStreamKey,
    coalesce_status_events,
    status_stream_key,
)
from orchestrator.local_command_journal import (
    LocalCommandOutcome,
    format_local_command_journal_context,
)
from orchestrator.proxy_model_choices import (
    configuration_choice_view,
    format_configuration_choices,
    format_effort_inspect_lines,
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
    build_list_resumable_action_result,
    execute_approval_target_action,
    execute_steward_actions,
    prepare_list_resumable_action,
)
from orchestrator.steward_interactions import (
    StewardActionInteraction,
    StewardConversation,
)
from orchestrator.steward_commands import (
    AgentsCommand,
    ApproverCommand,
    ResumeCommand,
    RoutingIdentityCommand,
    StewardHelpContext,
    build_providers_listing,
    extract_fallback_command,
    format_folder_only_state,
    is_local_immediate_command,
    parse_agents_command,
    parse_approver_command,
    parse_effort_command,
    parse_grunt_command,
    parse_inspect_command,
    parse_model_command,
    parse_providers_command,
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
    ProviderSessionBoundElsewhereError,
    StewardDeliveryLane,
    StewardDriverEvent,
    StewardTickDiagnosticContext,
)
from orchestrator.uploads import (
    PendingUpload,
    agent_input_from_uploads,
    save_upload as save_upload_to_project,
    validate_pending_capacity,
)


def _access_point_payload(access_point: AccessPointKey) -> dict[str, object]:
    return {
        "type": access_point.type,
        "chat_id": access_point.chat_id,
        "thread_id": access_point.thread_id,
    }


def _bound_elsewhere_payload(
    *,
    projected_session_id: str,
    owners: tuple[AccessPointKey, ...] | list[AccessPointKey],
) -> dict[str, object]:
    return {
        "ok": False,
        "code": "bound_elsewhere",
        "thread_id": projected_session_id,
        "owners": [_access_point_payload(owner) for owner in owners],
    }


class StewardAccessPointAdapter(Protocol):
    def build_help_text(self, *, state: str, startup: StewardHelpContext) -> str: ...
    def build_status_text(
        self,
        *,
        snapshot: AccessPointRuntimeSnapshot,
    ) -> str: ...
    def register_approval(
        self,
        *,
        access_point: AccessPointKey,
        source: str,
        request: AgentApprovalRequest,
    ) -> Any: ...
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
    def queue_turn_failure(
        self,
        *,
        access_point: AccessPointKey,
        error: str,
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
    pending_approval_on_arrival: bool = False
    pending_form_on_arrival: bool = False


class StewardCore:
    _DEFAULT_ROUTING_QUEUE_CAPACITY = 256
    _AGENT_METADATA_PERSIST_RETRY_SEC = 5.0
    _AGENTS_MESSAGE_CHAR_LIMIT = 3000
    _AGENTS_RESULT_LIMIT = 30
    _AGENT_CATALOG_STATUS_TEXT = "Looking up sessions..."
    _AGENT_CATALOG_TIMEOUT_SEC = 20.0
    _AGENT_CATALOG_CLEANUP_TIMEOUT_SEC = 30.0
    _RESUME_PROVIDER_OWNED_PHASES = frozenset({"starting_provider", "startup"})
    _INSPECTION_TIMEOUT_SEC = 5.0

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
        list_provider_sessions: Callable[
            [ListProviderSessions], ProviderSessionsListResult
        ],
        list_agent_sessions: Callable[
            [ListProviderSessions],
            tuple[ProviderSessionsListResult, ProviderInstallationStatus],
        ],
        provider_installation_status: Callable[
            [ProviderExecutionProfile], ProviderInstallationStatus
        ],
        persisted_state_by_access_point: dict[AccessPointKey, PersistedAccessPointState],
        persist_registry: Callable[[str, AccessPointKey | None], bool],
        drop_persisted_state: Callable[[AccessPointKey], bool],
        clear_status_runtimes: Callable[[AccessPointKey, bool], None],
        split_status: Callable[[AccessPointKey], None],
        kinds: StewardCoreKinds,
        hooks: StewardCoreHooks,
        routing_queue_capacity: int = _DEFAULT_ROUTING_QUEUE_CAPACITY,
        profile_catalog: RuntimeProfileCatalog = BUILTIN_RUNTIME_PROFILE_CATALOG,
    ) -> None:
        self._logger = logger
        self._writer = writer
        self._runtime = runtime
        self._agent_runtime = agent_runtime
        self._access_point_adapter = access_point_adapter
        self._list_provider_sessions = list_provider_sessions
        self._list_agent_sessions = list_agent_sessions
        self._provider_installation_status = provider_installation_status
        self._persisted_state_by_access_point = persisted_state_by_access_point
        self._persist_registry = persist_registry
        self._drop_persisted_state = drop_persisted_state
        self._clear_status_runtimes = clear_status_runtimes
        self._split_status = split_status
        self._kinds = kinds
        self._hooks = hooks
        if not isinstance(profile_catalog, RuntimeProfileCatalog):
            raise ValueError("Steward core requires RuntimeProfileCatalog")
        self._profile_catalog = profile_catalog
        self._catalog_operations = CatalogOperationExecutor(
            operation_progress=lambda progress: self._logger.event(
                "catalog_operation_progress",
                **progress.lifecycle_fields(),
            )
        )
        self._next_catalog_interaction_sequence = 1
        self._pending_text_updates: list[_PendingTextUpdate] = []
        self._pending_uploads_by_access_point: dict[AccessPointKey, list[PendingUpload]] = {
            access_point: list(state.pending_uploads)
            for access_point, state in persisted_state_by_access_point.items()
            if state.pending_uploads
        }
        self._pending_startup_notice_context: dict[AccessPointKey, str] = {}
        self._pending_local_command_deliveries: dict[AccessPointKey, int] = {}
        self._pending_inspect_commands: dict[AccessPointKey, int] = {}
        self._pending_model_commands: set[AccessPointKey] = set()
        self._pending_effort_commands: set[AccessPointKey] = set()
        self._agent_metadata_persist_retry_at: dict[AccessPointKey, float] = {}
        self._interaction_queue: InteractionQueue[
            RoutedConversation | AgentConversation | StewardConversation | ApprovalInteraction
            | FormElicitationInteraction
            | LocalLifecycleOperation | PendingInterruptRequest | AgentTurnInteraction
            | AgentSteerInteraction
            | InspectInteraction
            | AgentCatalogInteraction
            | AgentResumeInteraction
            | StewardActionInteraction
        ] = InteractionQueue()
        self._terminal_resume_agent_ids: list[str] = []
        self._idle_poll_blocked_resources: frozenset[object] = frozenset()
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
        on_sent: Callable[[], None] | None = None,
    ) -> None:
        self._access_point_adapter.queue_text_reply(
            access_point=access_point,
            text=text,
            source=source,
            kind=kind,
            on_sent=on_sent or (lambda: None),
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
        self._begin_local_command_delivery(access_point)
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

    def _begin_local_command_delivery(self, access_point: AccessPointKey) -> None:
        self._pending_local_command_deliveries[access_point] = (
            self._pending_local_command_deliveries.get(access_point, 0) + 1
        )

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
        turn_ref: TurnRef,
        repair_attempt: int,
        copy_role: str = "sender",
        receipt_owner: RoutedConversation | None = None,
    ) -> None:
        correlation = {
            **self._routing_access_point_fields(access_point, prefix=copy_role),
            "sender_address": sender_address,
            "target_address": target_address,
            "turn_ref_sequence": turn_ref.sequence,
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
            if receipt_owner is not None and not receipt_owner.dispatched:
                receipt_owner.pending_copy_delivery_receipts.append(correlation)
            else:
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
                failure_count = len(item.get("catalog_failures") or [])
                degraded = (
                    f"; {failure_count} catalog unavailable"
                    if failure_count
                    else ""
                )
                text = (
                    f"🧑‍✈️ Completed LIST_RESUMABLE{suffix}: "
                    f"{count} found{degraded}"
                )
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

    def _form_elicitation_for(
        self,
        access_point: AccessPointKey,
    ) -> FormElicitationInteraction | None:
        return next(
            (
                item
                for item in reversed(self._interaction_queue.snapshot())
                if isinstance(item, FormElicitationInteraction)
                and item.access_point == access_point
                and item.pending
            ),
            None,
        )

    def _human_approval_for(self, access_point: AccessPointKey) -> ApprovalInteraction | None:
        return next((child for child in reversed(self._approval_children())
                     if child.source_access_point == access_point
                     and child.phase in {"human", "human_submitting"}), None)

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
                    and item.phase in {"human", "human_submitting"}
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
            elif isinstance(item, AgentResumeInteraction):
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

    def _steward_action_interaction_for(
        self,
        access_point: AccessPointKey,
    ) -> StewardActionInteraction | None:
        return next(
            (
                item
                for item in reversed(self._interaction_queue.snapshot())
                if isinstance(item, StewardActionInteraction)
                and item.access_point == access_point
                and not item.finished
            ),
            None,
        )

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

    def _queue_agent_turn(
        self,
        *,
        access_point: AccessPointKey,
        agent_input: AgentInput,
        owner: AgentConversation | ApprovalInteraction,
        phase: str = "queued",
    ) -> AgentTurnInteraction:
        interaction = AgentTurnInteraction(
            target=access_point,
            request=agent_input,
            turn_ref=self._agent_runtime.next_turn_ref(access_point),
            owner=owner,
            lane="agent",
            phase=phase,
        )
        self._interaction_queue.append(interaction)
        return interaction

    def _queue_steward_turn(
        self,
        *,
        access_point: AccessPointKey,
        agent_input: AgentInput,
        owner: StewardConversation,
        context_note: str | None = None,
    ) -> AgentTurnInteraction:
        interaction = AgentTurnInteraction(
            target=("steward", access_point),
            request=agent_input,
            turn_ref=self._runtime.next_turn_ref(access_point),
            owner=owner,
            lane="steward",
            context_note=context_note,
        )
        self._interaction_queue.append(interaction)
        return interaction

    def _queue_steer(
        self,
        *,
        turn: AgentTurnInteraction,
        agent_input: AgentInput,
        owner: AgentConversation | StewardConversation,
        context_note: str | None = None,
    ) -> AgentSteerInteraction:
        interaction = AgentSteerInteraction(
            target=turn.target,
            request=agent_input,
            expected_turn_ref=turn.turn_ref,
            owner=owner,
            lane=turn.lane,
            context_note=context_note,
        )
        self._interaction_queue.append(interaction)
        return interaction

    def _turn_interaction_for(self, turn_ref: TurnRef | None) -> AgentTurnInteraction | None:
        if turn_ref is None:
            return None
        for item in self._interaction_queue.snapshot():
            if isinstance(item, AgentTurnInteraction) and item.turn_ref == turn_ref:
                return item
        return None

    def _log_turn_terminal_ignored(
        self,
        event: StewardDriverEvent,
        turn_ref: TurnRef,
        *,
        reason: str,
        terminal_kind: str,
    ) -> None:
        self._logger.event(
            "turn_terminal_ignored",
            **self._hooks.access_point_fields(event.access_point),
            route_target=event.source,
            reason=reason,
            terminal_kind=terminal_kind,
            runtime_generation=turn_ref.runtime_generation.value,
            turn_ref_sequence=turn_ref.sequence,
        )

    def _has_pending_steer(self, access_point: AccessPointKey) -> bool:
        return any(
            isinstance(item, AgentSteerInteraction)
            and item.is_pending
            and self._turn_access_point(item) == access_point
            for item in self._interaction_queue.snapshot()
        )

    def _turn_interaction_for_owner(
        self,
        owner: AgentConversation | StewardConversation,
    ) -> AgentTurnInteraction | None:
        for item in reversed(self._interaction_queue.snapshot()):
            if isinstance(item, AgentTurnInteraction) and item.owner is owner:
                return item
        return None

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

    def tick_diagnostic_context(
        self,
        phase_name: str,
    ) -> StewardTickDiagnosticContext | None:
        if phase_name == "drain_pending_inputs" and self._pending_text_updates:
            access_points = {
                pending.inbound.access_point
                for pending in self._pending_text_updates
            }
            return StewardTickDiagnosticContext(
                operation="pending_input",
                access_point=next(iter(access_points)) if len(access_points) == 1 else None,
            )
        if phase_name != "tick_interactions":
            return None

        interactions = self._interaction_queue.snapshot()
        if not interactions:
            return None
        access_points: set[AccessPointKey] = set()
        for interaction in interactions:
            if isinstance(interaction, (AgentConversation, StewardConversation)):
                access_points.add(interaction.access_point)
            elif isinstance(interaction, ApprovalInteraction):
                access_points.add(interaction.source_access_point)
                if interaction.target_access_point is not None:
                    access_points.add(interaction.target_access_point)
            elif isinstance(
                interaction,
                (
                    FormElicitationInteraction,
                    LocalLifecycleOperation,
                    PendingInterruptRequest,
                    AgentCatalogInteraction,
                    StewardActionInteraction,
                ),
            ):
                access_points.add(interaction.access_point)
            elif isinstance(interaction, (AgentTurnInteraction, AgentSteerInteraction)):
                if isinstance(interaction.target, AccessPointKey):
                    access_points.add(interaction.target)
            elif isinstance(interaction, InspectInteraction):
                if isinstance(interaction.target, AccessPointKey):
                    access_points.add(interaction.target)
            elif isinstance(interaction, RoutedConversation):
                access_points.add(interaction.sender_access_point)
                access_points.add(interaction.target_access_point)
            elif isinstance(interaction, AgentResumeInteraction):
                access_points.add(interaction.access_point)
        return StewardTickDiagnosticContext(
            operation="interaction_tick",
            access_point=next(iter(access_points)) if len(access_points) == 1 else None,
        )

    def is_idle(self) -> bool:
        return (
            not self._pending_text_updates
            and not any(child.pending for child in self._approval_children())
            and not self._interaction_queue
            and not self._pending_local_command_deliveries
            and not self._agent_runtime.has_pending_startup()
        )

    def cancel_catalog_operations(self) -> None:
        for interaction in self._interaction_queue.snapshot():
            if isinstance(interaction, AgentCatalogInteraction):
                for operation in interaction.operations:
                    operation.cancel()
            elif isinstance(interaction, AgentResumeInteraction):
                self._cancel_resume_interaction(
                    interaction,
                    reason="cleanup",
                    deliver=False,
                )
            elif isinstance(interaction, StewardActionInteraction):
                interaction.cancel()

    def close_catalog_operations(self) -> None:
        self.cancel_catalog_operations()
        self._catalog_operations.close(
            timeout_sec=self._AGENT_CATALOG_CLEANUP_TIMEOUT_SEC,
        )

    def enqueue_inbound(self, inbound: StewardInboundText) -> None:
        parsed_command = parse_steward_user_command(inbound.text)
        pending_approval_on_arrival = (
            self.pending_approval_for(inbound.access_point) is not None
        )
        pending_form_on_arrival = self._form_elicitation_for(inbound.access_point) is not None
        if (
            not inbound.uploads
            and not pending_approval_on_arrival
            and not pending_form_on_arrival
            and self._delegation_for_target(inbound.access_point) is None
            and (not parsed_command.is_command or parsed_command.name == "steward")
        ):
            inbound = self._reserve_pending_uploads(inbound)
        self._pending_text_updates.append(
            _PendingTextUpdate(
                inbound=inbound,
                pending_approval_on_arrival=pending_approval_on_arrival,
                pending_form_on_arrival=pending_form_on_arrival,
            )
        )

    def upload_project_cwd(self, access_point: AccessPointKey) -> str:
        cwd = self._current_project_cwd(access_point)
        if not cwd:
            raise ValueError("this access point has no bound working directory")
        return cwd

    def _current_project_cwd(self, access_point: AccessPointKey) -> str:
        binding = self._agent_runtime.get_binding_info(access_point) or {}
        cwd = resolve_catalog_cwd(str(binding.get("cwd") or ""))
        if cwd:
            return cwd
        persisted = self._persisted_state_by_access_point.get(access_point)
        return resolve_catalog_cwd(
            str(persisted.project_cwd if persisted is not None else "")
        )

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
    def _input_with_uploads(
        inbound: StewardInboundText,
        text: str | None = None,
    ) -> AgentInput:
        return agent_input_from_uploads(
            uploads=inbound.uploads,
            text=inbound.text if text is None else text,
            origin=AgentInputOrigin.HUMAN,
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

    def approval_human_decision(
        self,
        child: ApprovalInteraction,
        decision: str,
        via: str,
    ) -> ApprovalDeliveryReceipt:
        _ = via
        if not self._approval_parent_active(child):
            receipt = ApprovalDeliveryReceipt(child.request, decision)
            receipt.complete(
                ApprovalDecisionResult.preflight(
                    AgentEffectResult.rejected("approval parent is no longer active")
                )
            )
            return receipt
        lane = self._agent_runtime if child.source == "agent" else self._runtime
        return lane.submit_approval_decision(
            child.source_access_point,
            decision,
            expected_request=child.request,
        )

    def approval_human_submission_completed(
        self,
        child: ApprovalInteraction,
        decision: str,
        via: str,
        resolution: ApprovalDecisionResult,
    ) -> bool:
        pending = child.human_prompt
        if not resolution.delivery_attempted:
            self._logger.event(
                "steward_approval_preflight_rejected",
                **self._hooks.access_point_fields(pending.access_point),
                approval_ref=pending.request.approval_ref.value,
                decision=decision,
                reason=resolution.result.error,
                outcome=resolution.result.outcome.value,
                route_target=pending.source,
            )
            self._access_point_adapter.send_invalid_approval_reply(pending)
            return False
        if self._hooks.on_pre_approval_decision is not None:
            self._hooks.on_pre_approval_decision(pending)
        self._split_status(pending.access_point)
        fields = self._hooks.access_point_fields(pending.access_point)
        self._logger.event(
            self._hooks.approval_human_response_event,
            **fields,
            approval_ref=pending.request.approval_ref.value,
            response=decision,
            via=via,
            route_target=pending.source,
        )
        self._access_point_adapter.cancel_pending_approval_prompt(pending)
        return True

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

    def _apply_stop_input_barrier(self, barrier_index: int) -> int:
        barrier = self._pending_text_updates[barrier_index]
        access_point = barrier.inbound.access_point
        cancelled = [
            pending
            for pending in self._pending_text_updates[:barrier_index]
            if pending.inbound.access_point == access_point
        ]
        if not cancelled:
            return barrier_index
        cancelled_ids = {id(pending) for pending in cancelled}
        self._pending_text_updates = [
            pending
            for pending in self._pending_text_updates
            if id(pending) not in cancelled_ids
        ]
        for pending in cancelled:
            raw = pending.inbound.text.strip()
            command = raw or "<empty input>"
            self._logger.event(
                "steward_stop_barrier_input_cancelled",
                **self._hooks.access_point_fields(access_point),
                command=command,
                had_uploads=bool(pending.inbound.uploads),
            )
            self._queue_local_command_reply(
                access_point=access_point,
                outcome=LocalCommandOutcome(
                    command=command,
                    result={"ok": False, "code": "cancelled_by_stop"},
                ),
                text=f"request cancelled by /stop: {command}",
                kind=self._kinds.command,
            )
        return barrier_index - len(cancelled)

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

    def _cancel_pending_form(
        self,
        access_point: AccessPointKey,
        *,
        source: str,
    ) -> None:
        interaction = self._form_elicitation_for(access_point)
        if interaction is not None and interaction.source == source:
            interaction.cancel()

    def _approval_parent_active(self, child: ApprovalInteraction) -> bool:
        parent = child.parent
        if parent.finished or child.phase == "done" or child.source_cancelled:
            return False
        items = self._interaction_queue.snapshot()
        return (any(item is child for item in items)
                and any(item is parent for item in items)
                and child.belongs_to(parent, parent.pending_approval_request))

    def _submit_approval_for_child(
        self,
        child: ApprovalInteraction,
        decision: str,
        *,
        now: bool = False,
    ) -> ApprovalDecisionResult | None:
        if not self._approval_parent_active(child):
            raise RuntimeError("approval parent is no longer active")
        lane = self._agent_runtime if child.source == "agent" else self._runtime
        if now:
            return lane.submit_approval_decision_now(
                child.source_access_point, decision, expected_request=child.request,
            )
        lane.submit_approval_decision(
            child.source_access_point, decision, expected_request=child.request,
        )
        return None

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
        action_interaction = self._steward_action_interaction_for(access_point)
        if action_interaction is not None:
            if action_interaction.cancel():
                action_interaction.parent.finish()
                self._logger.event(
                    "steward_action_collection_cancelled",
                    **self._hooks.access_point_fields(access_point),
                    action_round=action_interaction.action_round,
                    reason="interrupted",
                )
            self._queue_local_command_reply(
                access_point=access_point,
                outcome=LocalCommandOutcome(
                    command="/interrupt",
                    result={
                        "ok": True,
                        "code": "interrupted",
                        "target": "steward_action",
                    },
                ),
                text="steward action interrupted.",
                kind=self._kinds.command,
            )
            return True
        resume = self._resume_interaction_for(access_point)
        if resume is not None and not resume.final_texts:
            self._cancel_resume_interaction(
                resume,
                reason="interrupted",
                deliver=True,
            )
            self._queue_local_command_reply(
                access_point=access_point,
                outcome=LocalCommandOutcome(
                    command="/interrupt",
                    result={"ok": True, "code": "interrupted", "target": "resume"},
                ),
                text="resume interrupted.",
                kind=self._kinds.command,
            )
            return True
        target_source: str | None = None
        interrupt_lane: StewardDeliveryLane | AgentDeliveryLane | None = None
        if self._agent_runtime.has_active_turn(access_point):
            target_source = "agent"
            interrupt_lane = self._agent_runtime
        elif self._runtime.has_active_turn(access_point):
            target_source = "steward"
            interrupt_lane = self._runtime
        if target_source is None or interrupt_lane is None:
            idle_agent_capabilities = self._agent_runtime.get_capability_snapshot(
                access_point
            )
            if (
                idle_agent_capabilities is not None
                and not idle_agent_capabilities.supports(
                    AgentCapability.INTERRUPT_TURN
                )
            ):
                self._queue_local_command_reply(
                    access_point=access_point,
                    outcome=LocalCommandOutcome(
                        command="/interrupt",
                        result={
                            "ok": False,
                            "code": "unsupported_capability",
                            "target": "agent",
                            "capability": AgentCapability.INTERRUPT_TURN.value,
                        },
                    ),
                    text=unsupported_capability_text(
                        AgentCapability.INTERRUPT_TURN
                    ),
                    kind=self._kinds.warning,
                )
                return True
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
        existing_interrupt = self._pending_interrupt_for(access_point)
        if existing_interrupt is not None and not existing_interrupt.finished:
            self._queue_local_command_reply(
                access_point=access_point,
                outcome=LocalCommandOutcome(
                    command="/interrupt",
                    result={
                        "ok": False,
                        "code": "already_requested",
                        "target": existing_interrupt.source,
                    },
                ),
                text="interrupt already requested.",
                kind=self._kinds.command,
            )
            return True
        capability_snapshot = interrupt_lane.get_capability_snapshot(access_point)
        if (
            capability_snapshot is None
            or not capability_snapshot.supports(AgentCapability.INTERRUPT_TURN)
        ):
            self._queue_local_command_reply(
                access_point=access_point,
                outcome=LocalCommandOutcome(
                    command="/interrupt",
                    result={
                        "ok": False,
                        "code": "unsupported_capability",
                        "target": target_source,
                        "capability": AgentCapability.INTERRUPT_TURN.value,
                    },
                ),
                text=unsupported_capability_text(AgentCapability.INTERRUPT_TURN),
                kind=self._kinds.warning,
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
        projected_session_id: str = "",
        agent_id: str = "",
        provider: str = "",
        profile: str = "",
        adapter: str = "",
        adapter_version: str = "",
        cwd: str = "",
        local_command: str = "",
        notify_steward: bool = False,
        resume_existing: bool = False,
        show_session_identity: bool = False,
        catalog_failures: tuple[AgentCatalogFailure, ...] = (),
    ) -> LocalLifecycleOperation:
        pending = LocalLifecycleOperation(
            access_point=access_point,
            operation=operation,
            thread_name=thread_name,
            runtime_thread_id=runtime_thread_id,
            projected_session_id=projected_session_id,
            agent_id=agent_id,
            provider=provider,
            profile=profile,
            adapter=adapter,
            adapter_version=adapter_version,
            cwd=cwd,
            local_command=local_command,
            notify_steward=notify_steward,
            resume_existing=resume_existing,
            show_session_identity=show_session_identity,
            catalog_failures=catalog_failures,
        )
        self._interaction_queue.append(pending)
        pending.begin(self, initial_text)
        return pending

    def _begin_bound_agent_startup_notice(
        self,
        access_point: AccessPointKey,
        *,
        initial_text: str | None = None,
    ) -> LocalLifecycleOperation:
        binding_info = self._agent_runtime.get_binding_info(access_point) or {}
        runtime_thread_id = str(binding_info.get("thread_id") or "").strip()
        thread_name = str(binding_info.get("thread_name") or "").strip()
        provider = str(binding_info.get("provider") or "").strip()
        profile = str(binding_info.get("profile") or "").strip()
        projected_session_id = self._project_session_id(
            provider=provider,
            profile=profile,
            native_id=runtime_thread_id,
        )
        return self._begin_agent_startup_notice(
            access_point,
            operation="start",
            initial_text=(
                initial_text
                if initial_text is not None
                else self._bound_startup_initial_text(
                    thread_name=thread_name,
                    projected_session_id=projected_session_id,
                )
            ),
            thread_name=thread_name,
            runtime_thread_id=runtime_thread_id,
            projected_session_id=projected_session_id,
            provider=provider,
            profile=profile,
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
        self._begin_bound_agent_startup_notice(
            access_point,
            initial_text="runtime agent starting...",
        )
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
            provider_session_ref=pending.projected_session_id,
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
            provider_session_ref=pending.projected_session_id,
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
    def _resume_lifecycle_label(*, thread_name: str, projected_session_id: str) -> str:
        id_label = (
            f"[{projected_session_id}]"
            if projected_session_id
            else "[unknown session]"
        )
        return thread_name or id_label

    def _project_session_id(
        self,
        *,
        provider: str,
        profile: str,
        native_id: str,
    ) -> str:
        native_id = str(native_id or "").strip()
        if not native_id:
            return ""
        try:
            execution_profile = self._profile_catalog.resolve(provider, profile or None)
            return self._profile_catalog.session_projector.namespaced_id(
                ProviderSessionRef(
                    execution_profile.provider,
                    native_id,
                    execution_profile.profile,
                )
            )
        except ValueError:
            return ""

    @staticmethod
    def _bound_startup_initial_text(
        *,
        thread_name: str,
        projected_session_id: str,
    ) -> str:
        if not projected_session_id:
            return "runtime agent starting new session..."
        label = StewardCore._resume_lifecycle_label(
            thread_name=thread_name,
            projected_session_id=projected_session_id,
        )
        return f"runtime agent resuming...\nsession: {label}"

    @classmethod
    def _agent_startup_action_initial_text(cls, pending: LocalLifecycleOperation) -> str:
        if pending.operation == "RESUME_AGENT":
            label = cls._resume_lifecycle_label(
                thread_name=pending.thread_name,
                projected_session_id=pending.projected_session_id,
            )
            text = f"Starting RESUME_AGENT: {label}"
            if pending.cwd:
                text += f" in {pending.cwd}"
            lines = [text]
            if pending.provider:
                lines.append(f"provider: {pending.provider}")
            if pending.profile:
                lines.append(f"profile: {pending.profile}")
            if pending.adapter:
                lines.append(
                    f"adapter: {pending.adapter}@{pending.adapter_version or 'unknown'}"
                )
            return "\n".join(lines)

        lines = ["Starting START_AGENT"]
        if pending.provider:
            lines.append(f"provider: {pending.provider}")
        if pending.profile:
            lines.append(f"profile: {pending.profile}")
        if pending.adapter:
            lines.append(
                f"adapter: {pending.adapter}@{pending.adapter_version or 'unknown'}"
            )
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
        projected_session_id: str,
    ) -> str:
        if operation == "RESUME_AGENT":
            label = cls._resume_lifecycle_label(
                thread_name=thread_name,
                projected_session_id=projected_session_id,
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
        component_lines = [
            f"profile: {result.profile or 'unknown'}",
            f"adapter: {(result.adapter or 'unknown')}@{(result.adapter_version or 'unknown')}",
        ]
        if result.result == "completed":
            return "\n".join(
                [
                    "Started START_AGENT",
                    f"provider: {result.provider or 'unknown'}",
                    *component_lines,
                    f"{ORC_RUNTIME_ID_LABEL}: {label}",
                    f"state: {result.state}",
                ]
            )
        return "\n".join(
            [
                "Start failed",
                f"provider: {result.provider or 'unknown'}",
                *component_lines,
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
            projected_session_id=str(item.get("thread_id") or "").strip(),
            agent_id=str(item.get("agent_id") or "").strip(),
            provider=str(item.get("provider") or "").strip(),
            profile=str(item.get("profile") or "").strip(),
            adapter=str(item.get("adapter") or "").strip(),
            adapter_version=str(item.get("adapter_version") or "").strip(),
            cwd=str(item.get("cwd") or "").strip(),
        )
        self._begin_agent_startup_notice(
            access_point,
            operation=pending.operation,
            initial_text=self._agent_startup_action_initial_text(pending),
            thread_name=pending.thread_name,
            runtime_thread_id=pending.runtime_thread_id,
            projected_session_id=pending.projected_session_id,
            agent_id=pending.agent_id,
            provider=pending.provider,
            profile=pending.profile,
            adapter=pending.adapter,
            adapter_version=pending.adapter_version,
            cwd=pending.cwd,
            notify_steward=True,
        )

    def _agent_startup_result_text(
        self,
        result: InteractiveStartupResult,
        *,
        operation: str,
        persisted: bool,
        resume_existing: bool = False,
        show_session_identity: bool = False,
        projected_session_id: str = "",
    ) -> str:
        if result.result != "completed":
            lines = ["runtime agent start failed."]
            if result.provider:
                lines.append(f"provider: {result.provider}")
            if result.profile:
                lines.append(f"profile: {result.profile}")
            if result.adapter:
                lines.append(
                    f"adapter: {result.adapter}@{result.adapter_version or 'unknown'}"
                )
            lines.extend(
                (
                    f"error: {result.error or 'startup failed'}",
                    "state: BOUND_IDLE",
                )
            )
            return "\n".join(lines)
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
                + self._resume_lifecycle_label(
                    thread_name=result.thread_name,
                    projected_session_id=projected_session_id,
                )
            )
        lines.extend((f"cwd: {result.cwd}", f"mode: {result.mode}"))
        if operation != "bind":
            if result.provider:
                lines.append(f"provider: {result.provider}")
            if result.profile:
                lines.append(f"profile: {result.profile}")
            if result.adapter:
                lines.append(
                    f"adapter: {result.adapter}@{result.adapter_version or 'unknown'}"
                )
            if result.effective_model:
                lines.append(f"effective model: {result.effective_model}")
            if result.effective_backend:
                lines.append(f"effective backend: {result.effective_backend}")
        return "\n".join(lines)

    def _handle_agent_startup_result(
        self,
        access_point: AccessPointKey,
        result: InteractiveStartupResult,
    ) -> None:
        resume = self._resume_interaction_for(access_point)
        if resume is not None and self._resume_has_provider_owner(resume):
            self._receive_resume_startup_result(resume, result)
            return
        if resume is not None and resume.final_texts:
            self._logger.event(
                "resume_startup_terminal_ignored",
                **self._hooks.access_point_fields(access_point),
                result=result.result,
            )
            return
        if result.agent_id and result.agent_id in self._terminal_resume_agent_ids:
            self._logger.event(
                "resume_startup_terminal_ignored",
                **self._hooks.access_point_fields(access_point),
                result=result.result,
            )
            return
        pending = self._lifecycle_operation_for(access_point)
        conversation = self._steward_conversation_for(access_point)
        if (
            pending is None
            and conversation is not None
            and conversation.defer_startup_completion(result)
        ):
            self._logger.event(
                "steward_startup_result_deferred",
                **self._hooks.access_point_fields(access_point),
            )
            return
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
        pending.projected_session_id = (
            pending.projected_session_id
            or self._project_session_id(
                provider=result.provider or pending.provider,
                profile=result.profile or pending.profile,
                native_id=result.thread_id or pending.runtime_thread_id,
            )
        )
        if pending.operation in {"START_AGENT", "RESUME_AGENT"}:
            projected_session_id = pending.projected_session_id
            catalog_failure_payloads = agent_catalog_failure_payloads(
                pending.catalog_failures
            )
            outcome = None
            if pending.local_command:
                if result.result == "completed":
                    command_result = self._persisted_mutation_result(
                        code="resumed",
                        persisted=persisted,
                        state=result.state,
                        thread_id=projected_session_id,
                        catalog_failures=catalog_failure_payloads,
                    )
                else:
                    command_result = {
                        "ok": False,
                        "code": "startup_failed",
                        "state": result.state,
                        "persisted": persisted,
                        "thread_id": projected_session_id,
                        "catalog_failures": catalog_failure_payloads,
                    }
                outcome = LocalCommandOutcome(
                    command=pending.local_command,
                    result=command_result,
                )
            pending.apply_startup_result(
                self, result, persisted=persisted, outcome=outcome,
                final_text="\n\n".join(
                    self._append_catalog_warnings(
                        [
                            self._agent_startup_action_result_text(
                                result,
                                operation=pending.operation,
                                thread_name=(
                                    pending.thread_name
                                    if pending.local_command
                                    else result.thread_name or pending.thread_name
                                ),
                                projected_session_id=projected_session_id,
                            )
                        ],
                        list(pending.catalog_failures),
                    )
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
                projected_session_id=(
                    pending.projected_session_id
                ),
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
            fallback_cmd = extract_fallback_command(inbound.text)
            if fallback_cmd == "stop":
                idx = self._apply_stop_input_barrier(idx)
                pending_update = self._pending_text_updates[idx]
                inbound = pending_update.inbound
                access_point = inbound.access_point
            pending = self.pending_approval_for(access_point)
            pending_form = self._form_elicitation_for(access_point)
            active = self.active_conversation_for(access_point)
            parsed_command = parse_steward_user_command(inbound.text)
            has_earlier_same_access_point = any(
                pending.inbound.access_point == access_point
                for pending in self._pending_text_updates[:idx]
            )
            lifecycle_tears_down_pending_work = (
                fallback_cmd in {"stop", "reset"}
                and (
                    self._has_pending_steer(access_point)
                    or self._has_pending_inspect(access_point)
                    or (
                        fallback_cmd == "stop"
                        and pending is not None
                        and pending_update.pending_approval_on_arrival
                    )
                )
            )
            local_immediate = (
                parsed_command.is_unknown
                or is_local_immediate_command(fallback_cmd)
                or lifecycle_tears_down_pending_work
                or (
                    fallback_cmd == "stop"
                    and self._agent_runtime.start_decision(access_point)
                    is AgentRuntimeStartDecision.STARTING
                )
            )
            catalog_interaction_pending = (
                self._catalog_interaction_for(access_point) is not None
                or self._resume_interaction_for(access_point) is not None
            )
            is_slash_command = parsed_command.is_command
            if pending_form is not None and not is_slash_command:
                self._pending_text_updates.pop(idx)
                self._handle_form_elicitation_input(pending_form, inbound.text)
                progressed = True
                continue
            has_later_interrupt_same_access_point = any(
                pending.inbound.access_point == access_point
                and extract_fallback_command(pending.inbound.text) == "interrupt"
                for pending in self._pending_text_updates[idx + 1 :]
            )
            if (
                self._pending_local_command_deliveries.get(access_point, 0) > 0
                and not (local_immediate and catalog_interaction_pending)
                and fallback_cmd != "stop"
            ):
                idx += 1
                continue
            if (
                self._pending_inspect_commands.get(access_point, 0) > 0
                and not local_immediate
            ):
                idx += 1
                continue
            if (
                access_point in self._pending_model_commands
                or access_point in self._pending_effort_commands
            ) and fallback_cmd != "stop":
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
                        and (
                            pending_update.runtime_start_decision is None
                            or not inbound.uploads
                        )
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

    def _handle_form_elicitation_input(
        self,
        interaction: FormElicitationInteraction,
        text: str,
    ) -> None:
        normalized = text.strip()
        try:
            if normalized.lower() == "decline":
                interaction.answer(ElicitationAction.DECLINE)
                return
            if normalized.lower() == "cancel":
                interaction.answer(ElicitationAction.CANCEL)
                return
            parsed = json.loads(normalized)
            if not isinstance(parsed, dict):
                raise ValueError("form response must be a JSON object")
            content = interaction.request.validate_content(parsed)
            interaction.answer(ElicitationAction.ACCEPT, content)
        except (json.JSONDecodeError, ValueError) as exc:
            self._queue_local_reply(
                access_point=interaction.access_point,
                text=f"form response rejected: {str(exc)}",
                source="steward",
                kind=self._kinds.warning,
            )

    def _register_form_elicitation(
        self,
        event: StewardDriverEvent,
        request: AgentFormRequest,
    ) -> None:
        if self._form_elicitation_for(event.access_point) is not None:
            raise RuntimeError("form elicitation owner already exists")
        interaction = FormElicitationInteraction(
            access_point=event.access_point,
            source=event.source,
            request=request,
        )
        self._interaction_queue.append(interaction)
        field_lines = [
            f"- {field.name}: {field.value_type}{' (required)' if field.required else ''}"
            for field in request.fields
        ]
        self._queue_local_reply(
            access_point=event.access_point,
            text="\n".join(
                (
                    "form input requested",
                    request.message,
                    *field_lines,
                    "reply with a JSON object, decline, or cancel",
                )
            ),
            source=event.source,
            kind=self._kinds.command,
        )

    def form_elicitation_submit(
        self,
        interaction: FormElicitationInteraction,
    ) -> ApprovalDecisionResult:
        lane = self._agent_runtime if interaction.source == "agent" else self._runtime
        if interaction.action is None:
            raise RuntimeError("elicitation response action is missing")
        receipt = lane.submit_elicitation_response(
            interaction.access_point,
            interaction.action,
            expected_request=interaction.request,
            content=dict(interaction.content),
        )
        if not receipt.completed or receipt.result is None:
            raise RuntimeError("elicitation response did not complete synchronously")
        return receipt.result

    def form_elicitation_rejected(
        self,
        interaction: FormElicitationInteraction,
        error: str,
    ) -> None:
        self._queue_local_reply(
            access_point=interaction.access_point,
            text=f"form response rejected: {error}",
            source="steward",
            kind=self._kinds.warning,
        )

    def form_elicitation_failed(
        self,
        interaction: FormElicitationInteraction,
        error: str,
    ) -> None:
        self._logger.event(
            "form_elicitation_delivery_failed",
            **self._hooks.access_point_fields(interaction.access_point),
            source=interaction.source,
            error=error,
        )

    def form_elicitation_completed(
        self,
        interaction: FormElicitationInteraction,
        outcome: str,
    ) -> None:
        self._logger.event(
            "form_elicitation_completed",
            **self._hooks.access_point_fields(interaction.access_point),
            source=interaction.source,
            outcome=outcome,
        )

    def _submit_busy_followup_to_active_lane(
        self,
        inbound: StewardInboundText,
        active: AgentConversation | StewardConversation,
    ) -> bool:
        access_point = inbound.access_point
        try:
            if active.phase != "initial":
                return False
            turn_interaction = self._turn_interaction_for_owner(active)
            if turn_interaction is None or not turn_interaction.active:
                return False
            if isinstance(active, AgentConversation):
                conversation = self._agent_conversation_for(access_point)
                if conversation is None:
                    return False
                source_input = self._input_with_uploads(inbound)
                agent_input = self._format_human_agent_input(
                    access_point,
                    source_input,
                )
                if not conversation.queue_steer(
                    agent_input,
                    source_text=source_input.text,
                ):
                    return False
                self._queue_steer(
                    turn=turn_interaction,
                    agent_input=agent_input,
                    owner=conversation,
                )
            elif isinstance(active, StewardConversation):
                if not active.accepts_steer():
                    return False
                conversation = self._steward_conversation_for(access_point)
                if conversation is None:
                    return False
                self._queue_steer(
                    turn=turn_interaction,
                    agent_input=self._input_with_uploads(inbound),
                    owner=conversation,
                    context_note=self._prepare_steward_context(access_point),
                )
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

    def _format_human_agent_input(
        self,
        access_point: AccessPointKey,
        agent_input: AgentInput,
    ) -> AgentInput:
        address = self._shaman_address(access_point)
        if not address:
            return agent_input
        envelope = format_human_routing_envelope(address, agent_input.text)
        self._logger.event(
            "routing_envelope_created",
            **self._routing_access_point_fields(access_point, prefix="sender"),
            sender_address=HUMAN_ADDRESS,
            target_address=address,
        )
        return agent_input.with_text(envelope)

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
        project_cwd = self._current_project_cwd(access_point)
        has_binding = snapshot.binding is not None or bool(
            state is not None and state.agent is not None
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

    def _queue_inspect(self, source: AccessPointKey, *, all_targets: bool) -> None:
        targets = self._inspect_targets(source) if all_targets else [source]
        command = "/inspect all" if all_targets else "/inspect"
        self._pending_inspect_commands[source] = (
            self._pending_inspect_commands.get(source, 0) + 1
        )
        for target in targets:
            self._interaction_queue.append(
                InspectInteraction(
                    target=target,
                    requester=source,
                    command=command,
                )
            )

    def _start_agent_action(
        self,
        access_point: AccessPointKey,
        spec: dict[str, Any],
        *,
        conversation: StewardConversation | None = None,
    ) -> dict[str, Any]:
        resolved_spec = dict(spec)
        projected_session_id = str(resolved_spec.get("thread_id") or "").strip()
        projected_session_ref: ProviderSessionRef | None = None
        if projected_session_id:
            try:
                projected_session_ref = (
                    self._profile_catalog.session_projector.parse_namespaced_id(
                        projected_session_id
                    )
                )
            except ValueError:
                projected_session_ref = None
        requested_provider = resolved_spec.get("provider") or ProviderKind.CODEX
        if projected_session_ref is not None:
            requested_provider = projected_session_ref.provider
            resolved_spec["profile"] = projected_session_ref.profile.value
            resolved_spec["thread_id"] = projected_session_ref.native_id
        try:
            provider = self._profile_catalog.require_provider(requested_provider)
        except ValueError:
            self._logger.event(
                "steward_action_provider_rejected",
                **self._hooks.access_point_fields(access_point),
                provider=str(requested_provider or "").strip().lower(),
                reason="unknown",
            )
            raise
        try:
            execution_profile = self._profile_catalog.resolve(
                provider,
                resolved_spec.get("profile"),
            )
        except ValueError:
            self._logger.event(
                "steward_action_provider_rejected",
                **self._hooks.access_point_fields(access_point),
                provider=provider.value,
                profile=str(resolved_spec.get("profile") or "").strip().lower(),
                reason="invalid_profile",
            )
            raise
        profile = execution_profile.profile
        resolved_spec["provider"] = provider.value
        resolved_spec["profile"] = profile.value
        runtime_thread_id = str(resolved_spec.get("thread_id") or "").strip()
        if runtime_thread_id:
            session_ref = ProviderSessionRef(provider, runtime_thread_id, profile)
            thread_name = ""
            try:
                catalog_result = self._list_provider_sessions(
                    ListProviderSessions(
                        execution_profile=session_ref.execution_profile,
                        cwd=str(resolved_spec.get("cwd") or ""),
                    )
                )
            except ProviderUnavailableError:
                catalog_result = ProviderSessionsListFailed(
                    ProviderSessionCatalogErrorKind.UNAVAILABLE,
                    "provider session catalog is unavailable",
                )
            if isinstance(catalog_result, ProviderSessionsListed):
                catalog_entry = next(
                    (
                        entry
                        for entry in catalog_result.entries
                        if entry.session_ref == session_ref
                    ),
                    None,
                )
                if catalog_entry is not None:
                    thread_name = catalog_entry.name
            resolved_spec["thread_name"] = thread_name
        if conversation is not None:
            conversation.expect_startup_completion()
        try:
            result = self._agent_runtime.start_agent(access_point, resolved_spec)
        except ProviderSessionBoundElsewhereError as exc:
            if conversation is not None:
                conversation.abandon_startup_completion()
            projected_conflict_id = (
                self._profile_catalog.session_projector.namespaced_id(exc.session_ref)
            )
            self._logger.event(
                "steward_action_provider_rejected",
                **self._hooks.access_point_fields(access_point),
                provider=provider.value,
                profile=profile.value,
                reason=exc.code,
                owner_count=len(exc.owners),
            )
            return {
                **_bound_elsewhere_payload(
                    projected_session_id=projected_conflict_id,
                    owners=exc.owners,
                ),
                "provider": exc.session_ref.provider.value,
                "profile": exc.session_ref.profile.value,
                "thread_name": str(resolved_spec.get("thread_name") or ""),
                "error": "provider session is already bound to another access point",
            }
        except ProviderUnavailableError:
            if conversation is not None:
                conversation.abandon_startup_completion()
            self._logger.event(
                "steward_action_provider_rejected",
                **self._hooks.access_point_fields(access_point),
                provider=provider.value,
                profile=profile.value,
                reason="unavailable",
            )
            raise
        except Exception:
            if conversation is not None:
                conversation.abandon_startup_completion()
            raise
        if runtime_thread_id:
            result = dict(result)
            result["thread_id"] = self._profile_catalog.session_projector.namespaced_id(
                ProviderSessionRef(provider, runtime_thread_id, profile)
            )
        self._persist_registry("start_agent_action_starting", access_point)
        return result

    def _set_project_cwd_action(
        self,
        access_point: AccessPointKey,
        cwd: str,
    ) -> dict[str, Any]:
        runtime_state = self._agent_runtime.runtime_state(access_point)
        previous = self._persisted_state_by_access_point.get(access_point)
        if self._agent_runtime.has_binding(access_point) or (
            previous is not None and previous.agent is not None
        ):
            return {
                "ok": False,
                "code": "binding_exists",
                "state": runtime_state,
                "persisted": False,
                "error": "reset the bound runtime agent before selecting a folder",
            }
        candidate = (
            replace(previous, project_cwd=cwd)
            if previous is not None
            else PersistedAccessPointState(project_cwd=cwd, agent=None)
        )
        self._persisted_state_by_access_point[access_point] = candidate
        persisted = self._persist_registry("set_project_cwd_action", access_point)
        if not persisted:
            if previous is None:
                self._persisted_state_by_access_point.pop(access_point, None)
            else:
                self._persisted_state_by_access_point[access_point] = previous
            self._logger.event(
                "access_point_project_cwd_rejected",
                **self._hooks.access_point_fields(access_point),
                reason="persistence_failed",
            )
            return {
                "ok": False,
                "code": "persistence_failed",
                "state": "UNBOUND",
                "persisted": False,
                "error": "registry persistence failed; folder was not changed",
            }
        self._logger.event(
            "access_point_project_cwd_selected",
            **self._hooks.access_point_fields(access_point),
            state="UNBOUND",
            persisted=True,
        )
        return {
            "ok": True,
            "code": "project_cwd_set",
            "state": "UNBOUND",
            "persisted": True,
        }

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

        self._cancel_pending_configuration_commands(
            access_point,
            reason="runtime stopped",
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
        agent_input: AgentInput,
        *,
        owner: StewardConversation | None = None,
    ) -> None:
        if owner is None:
            raise ValueError("Steward base turn requires an owning interaction")
        self._queue_steward_turn(
            access_point=access_point,
            agent_input=agent_input,
            owner=owner,
            context_note=self._prepare_steward_context(access_point),
        )

    def _prepare_steward_context(self, access_point: AccessPointKey) -> str:
        ensure_started = getattr(self._runtime, "ensure_started", None)
        if callable(ensure_started):
            ensure_started(access_point)
        context_parts = [format_runtime_snapshot_context(self._runtime_snapshot(access_point))]
        project_cwd = self._current_project_cwd(access_point)
        if project_cwd and not self._agent_runtime.has_binding(access_point):
            context_parts.append(f"Selected folder: {project_cwd}")
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
        context_note = "\n\n".join(context_parts)
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
        return context_note

    def build_status_text(self, *, snapshot: AccessPointRuntimeSnapshot) -> str:
        text = self._access_point_adapter.build_status_text(snapshot=snapshot)
        project_cwd = self._current_project_cwd(snapshot.access_point)
        if snapshot.state == "UNBOUND" and project_cwd:
            text += "\n" + format_folder_only_state(project_cwd)
        return text

    def _build_inspect_text(
        self,
        *,
        access_point: AccessPointKey,
        snapshot: AccessPointRuntimeSnapshot,
        steward_projection: AgentRuntimeProjection,
        agent_projection: AgentRuntimeProjection,
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
        steward = snapshot.steward
        steward_capabilities = steward_projection.capabilities
        agent_capabilities = agent_projection.capabilities
        show_terminal_agent_projection = (
            binding is None
            and agent_projection.session.availability is SnapshotAvailability.ERROR
        )
        active_projection = (
            agent_projection
            if binding is not None or show_terminal_agent_projection
            else steward_projection
        )
        active_turn_ref = active_projection.work.active_turn_ref
        active_turn_id = (
            f"turn:{active_turn_ref.sequence}" if active_turn_ref is not None else "none"
        )

        lines = [
            "inspect",
            "access point:",
            format_access_point(snapshot.access_point),
            f"state: {snapshot.state}",
        ]
        project_cwd = self._current_project_cwd(access_point)
        if snapshot.state == "UNBOUND" and project_cwd:
            lines.append(format_folder_only_state(project_cwd))
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
                    f"provider: {steward.provider or 'unknown'}",
                    f"configured model: {steward.configured_model or 'default'}",
                    f"configured backend: {steward.configured_backend or 'default'}",
                    f"effective model: {steward.effective_model or 'pending'}",
                    f"effective backend: {steward.effective_backend or 'pending'}",
                    format_context_snapshot(steward_projection.context),
                ]
            )
            if (
                steward_capabilities is not None
                and steward_capabilities.supports(AgentCapability.READ_EFFORT)
            ):
                lines.extend(format_effort_inspect_lines(steward_projection))
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
                    f"provider: {binding.provider or 'unknown'}",
                    f"profile: {binding.profile or 'unknown'}",
                    f"adapter: {(binding.adapter or 'unknown')}@{(binding.adapter_version or 'unknown')}",
                    f"runtime: {(binding.runtime or 'unknown')}@{(binding.runtime_version or 'unknown')}",
                    f"cwd: {binding.cwd}",
                    f"mode: {binding.mode}",
                    f"configured model: {binding.configured_model or 'default'}",
                    f"configured backend: {binding.configured_backend or 'default'}",
                    f"effective model: {binding.effective_model or 'pending'}",
                    f"effective backend: {binding.effective_backend or 'pending'}",
                    *(
                        "configured launch "
                        + key.replace("_", " ")
                        + ": "
                        + (
                            json.dumps(list(value))
                            if isinstance(value, tuple)
                            else str(value)
                        )
                        for key, value in binding.configured_launch.items()
                    ),
                    format_context_snapshot(agent_projection.context),
                ]
            )
            if (
                agent_capabilities is not None
                and agent_capabilities.supports(AgentCapability.READ_EFFORT)
            ):
                lines.extend(format_effort_inspect_lines(agent_projection))
            metadata_lines = format_session_snapshot_lines(
                agent_projection.session,
                session_projector=self._profile_catalog.session_projector,
                thread_id_fallback=binding.thread_id,
                thread_name_fallback=binding.thread_name,
                session_provider=binding.provider,
                session_profile=binding.profile,
                include_model=False,
            )
            if metadata_lines:
                lines.extend(["", "session:", *metadata_lines])
        elif show_terminal_agent_projection:
            metadata_lines = format_session_snapshot_lines(
                agent_projection.session,
                session_projector=self._profile_catalog.session_projector,
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
                    f"approval kind: {delegation.request.kind.value}",
                ]
            )
        if approval_request is not None:
            if approval_request.command:
                lines.append(f"approval command: {approval_request.command}")
            if approval_request.cwd:
                lines.append(f"approval cwd: {approval_request.cwd}")
        lines.extend(["", "active work:"])
        append_work_snapshot_lines(lines, active_projection.work)
        lines.append("")
        append_background_snapshot_lines(lines, active_projection.background)
        return "\n".join(lines)

    def register_approval(
        self,
        event: StewardDriverEvent,
        request: AgentApprovalRequest,
    ) -> None:
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
        if child.target_access_point is None:
            raise RuntimeError("approval target access point is not resolved")
        phase = (
            "waiting"
            if child.phase in {"target_starting", "target_waiting"}
            else "queued"
        )
        self._queue_agent_turn(
            access_point=child.target_access_point,
            agent_input=AgentInput(
                text=prompt,
                origin=AgentInputOrigin.CONTROL,
                verbatim=True,
            ),
            owner=child,
            phase=phase,
        )

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
        if not child.target_notice_pending:
            self.approval_target_started(child, source_label)
        self._logger.event(
            "approval_delegation_started", approval_id=child.approval_id,
            source_address=child.source_address, target_address=child.target_address,
            approval_kind=child.request.kind.value,
            approval_ref=child.request.approval_ref.value,
        )

    def approval_target_started(
        self,
        child: ApprovalInteraction,
        source_label: str,
        *,
        on_sent: Callable[[], None] | None = None,
    ) -> None:
        self.approval_notify(
            child.target_access_point,
            format_approval_request_copy(
                source_label=source_label, target_address=child.target_address, request=child.request,
            ),
            on_sent=on_sent,
        )

    def approval_event(self, name: str, **fields: Any) -> None:
        self._logger.event(name, **fields)

    def approval_notify(
        self,
        access_point: AccessPointKey,
        text: str,
        *,
        on_sent: Callable[[], None] | None = None,
    ) -> None:
        self._queue_local_reply(
            access_point=access_point,
            text=text,
            source="agent",
            kind=self._kinds.reply,
            on_sent=on_sent,
        )

    def approval_target_interrupt_notice_sent(self, child: ApprovalInteraction) -> None:
        target = child.target_access_point
        if target is None:
            return
        pending = self._pending_interrupt_for(target)
        if pending is None or pending.final_text is not None:
            return
        pending.set_result(
            self,
            text=self._interrupt_final_text(success=True),
            outcome=LocalCommandOutcome(
                command="/interrupt",
                result={"ok": True, "code": "interrupted", "target": "agent"},
            ),
        )

    def approval_submit_source(
        self,
        child: ApprovalInteraction,
        decision: str,
    ) -> ApprovalDecisionResult:
        if not self._approval_parent_active(child):
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected("approval parent is no longer active")
            )
        resolution = self._submit_approval_for_child(child, decision, now=True)
        if resolution is None:
            raise AssertionError("immediate approval decision did not return a result")
        return resolution

    def approval_decline_target_request(
        self,
        child: ApprovalInteraction,
        request: AgentApprovalRequest,
    ) -> None:
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

    def _cancel_approval_on_agent_stop(
        self,
        access_point: AccessPointKey,
        *,
        turn_error: str = "runtime agent stopped",
    ) -> None:
        self._terminate_session_turns(access_point, "agent", turn_error)
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
    ) -> list[dict[str, Any]] | None:
        if any(
            str(action.get("type") or "").strip().upper() == "LIST_RESUMABLE"
            for action in actions
        ):
            if self._steward_action_interaction_for(conversation.access_point) is not None:
                raise RuntimeError("Steward action collection is already active")
            interaction = StewardActionInteraction(
                parent=conversation,
                actions=tuple(actions),
                action_round=conversation.action_round,
            )
            self._interaction_queue.append(interaction)
            self._logger.event(
                "steward_action_collection_started",
                **self._hooks.access_point_fields(conversation.access_point),
                action_round=conversation.action_round,
                action_count=len(actions),
                action_types=conversation.action_types(actions),
            )
            return None
        access_point = conversation.access_point
        action_results = execute_steward_actions(
            actions,
            show_running_provider=lambda: self._runtime_snapshot(access_point).action_payload(),
            show_running_access_point={
                "type": access_point.type,
                "chat_id": access_point.chat_id,
                "thread_id": access_point.thread_id,
            },
            stop_agent_provider=lambda agent_id: self._stop_agent_action(access_point, agent_id),
            start_agent_provider=lambda spec: self._start_agent_action(
                access_point,
                spec,
                conversation=conversation,
            ),
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
            set_project_cwd_provider=lambda cwd: self._set_project_cwd_action(
                access_point,
                cwd,
            ),
            profile_catalog=self._profile_catalog,
        )
        for action, result in zip(actions, action_results, strict=True):
            if (
                str(action.get("type") or "").strip().upper() != "START_AGENT"
                or bool(result.get("ok"))
            ):
                continue
            requested_provider = str(action.get("provider") or ProviderKind.CODEX).strip().lower()
            try:
                provider = self._profile_catalog.require_provider(
                    requested_provider
                )
            except ValueError:
                self._logger.event(
                    "steward_action_provider_rejected",
                    **self._hooks.access_point_fields(access_point),
                    provider=requested_provider,
                    reason="unknown",
                )
                continue
            requested_profile = action.get("profile")
            try:
                self._profile_catalog.resolve(provider, requested_profile)
            except ValueError:
                self._logger.event(
                    "steward_action_provider_rejected",
                    **self._hooks.access_point_fields(access_point),
                    provider=provider.value,
                    profile=str(requested_profile or "").strip().lower(),
                    reason="invalid_profile",
                )
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

    def _clear_steward_action_catalog_slot(
        self,
        interaction: StewardActionInteraction,
    ) -> None:
        if interaction.operation_sequence:
            self._logger.event(
                "steward_action_catalog_completed",
                **self._hooks.access_point_fields(interaction.access_point),
                access_point_owner_ref=catalog_access_point_owner_ref(
                    interaction.access_point
                ),
                interaction_sequence=interaction.operation_sequence,
                action_round=interaction.action_round,
                slot=interaction.slot_index,
            )
        interaction.prepared_list_request = None
        interaction.operations = ()
        interaction.operation_started_at = None
        interaction.operation_sequence = 0

    def _complete_steward_action_slot(
        self,
        interaction: StewardActionInteraction,
        result: dict[str, Any],
    ) -> None:
        if interaction.slot_index >= len(interaction.results):
            raise AssertionError("Steward action slot is out of range")
        if interaction.results[interaction.slot_index] is not None:
            raise AssertionError("Steward action slot completed more than once")
        interaction.results[interaction.slot_index] = result
        interaction.slot_index += 1
        self._clear_steward_action_catalog_slot(interaction)

    def steward_action_interaction_tick(
        self,
        interaction: StewardActionInteraction,
    ) -> str | None:
        if interaction.finished:
            return "completed"
        parent = interaction.parent
        if (
            interaction.cancelled
            or parent.finished
            or parent.action_round != interaction.action_round
        ):
            interaction.cancel()
            return "completed"

        while interaction.slot_index < len(interaction.actions):
            action = interaction.actions[interaction.slot_index]
            action_type = str(action.get("type") or "").strip().upper()
            if action_type != "LIST_RESUMABLE":
                immediate = self.steward_execute_actions(parent, [action])
                if immediate is None or len(immediate) != 1:
                    raise AssertionError("synchronous Steward action did not settle one slot")
                self._complete_steward_action_slot(interaction, immediate[0])
                continue

            if interaction.prepared_list_request is None:
                prepared = prepare_list_resumable_action(action)
                if isinstance(prepared, dict):
                    self._complete_steward_action_slot(interaction, prepared)
                    continue
                interaction.prepared_list_request = prepared
                interaction.operation_sequence = self._catalog_interaction_sequence()
                try:
                    _profiles, operations, busy_profiles = (
                        self._submit_profile_catalog_operations(
                            interaction_kind=CatalogInteractionKind.STEWARD_ACTION,
                            access_point=interaction.access_point,
                            interaction_sequence=interaction.operation_sequence,
                            cwd=prepared.cwd,
                            limit=prepared.limit,
                            include_installation=False,
                        )
                    )
                except (CatalogOperationCapacityError, RuntimeError) as exc:
                    self._logger.event(
                        "steward_action_catalog_rejected",
                        **self._hooks.access_point_fields(interaction.access_point),
                        action_round=interaction.action_round,
                        slot=interaction.slot_index,
                        error_type=type(exc).__name__,
                    )
                    self._complete_steward_action_slot(
                        interaction,
                        {
                            "type": "LIST_RESUMABLE",
                            "ok": False,
                            "code": "lookup_failed",
                            "cwd": prepared.cwd,
                            "error": "provider session catalog lookup failed",
                        },
                    )
                    continue
                interaction.operations = operations
                interaction.operation_started_at = clock.monotonic()
                for execution_profile in busy_profiles:
                    self._logger.event(
                        "steward_action_catalog_profile_busy",
                        **self._hooks.access_point_fields(interaction.access_point),
                        action_round=interaction.action_round,
                        slot=interaction.slot_index,
                        provider=execution_profile.provider.value,
                        profile=execution_profile.profile.value,
                    )
                return None

            if all(operation.settled for operation in interaction.operations):
                try:
                    rows = []
                    for operation in interaction.operations:
                        row = operation.take_result()
                        if row is None:
                            return None
                        rows.append(row)
                    listing = self._catalog_listing_from_profile_results(rows)
                    result = build_list_resumable_action_result(
                        interaction.prepared_list_request,
                        listing,
                        project_session_ref=(
                            self._profile_catalog.session_projector.namespaced_id
                        ),
                    )
                except Exception as exc:
                    self._logger.event(
                        "steward_action_catalog_failed",
                        **self._hooks.access_point_fields(interaction.access_point),
                        action_round=interaction.action_round,
                        slot=interaction.slot_index,
                        error_type=type(exc).__name__,
                    )
                    result = {
                        "type": "LIST_RESUMABLE",
                        "ok": False,
                        "code": "lookup_failed",
                        "cwd": interaction.prepared_list_request.cwd,
                        "error": "provider session catalog lookup failed",
                    }
                self._complete_steward_action_slot(interaction, result)
                continue

            assert interaction.operation_started_at is not None
            if (
                clock.monotonic() - interaction.operation_started_at
                > self._AGENT_CATALOG_TIMEOUT_SEC
            ):
                cancelled = [
                    operation.cancel()
                    for operation in interaction.operations
                    if not operation.done
                ]
                if not any(cancelled):
                    continue
                self._logger.event(
                    "steward_action_catalog_timed_out",
                    **self._hooks.access_point_fields(interaction.access_point),
                    action_round=interaction.action_round,
                    slot=interaction.slot_index,
                    timeout_sec=self._AGENT_CATALOG_TIMEOUT_SEC,
                )
                self._complete_steward_action_slot(
                    interaction,
                    {
                        "type": "LIST_RESUMABLE",
                        "ok": False,
                        "code": "lookup_timeout",
                        "cwd": interaction.prepared_list_request.cwd,
                        "error": "provider session catalog lookup timed out",
                    },
                )
                continue
            return None

        results = [item for item in interaction.results if item is not None]
        if len(results) != len(interaction.results):
            raise AssertionError("Steward action collector completed with empty slots")
        interaction.finished = True
        self._logger.event(
            "steward_action_collection_completed",
            **self._hooks.access_point_fields(interaction.access_point),
            action_round=interaction.action_round,
            action_count=len(results),
        )
        self._emit_internal_action_notes(
            access_point=interaction.access_point,
            action_results=results,
        )
        parent.complete_action_collection(self, results)
        return "completed"

    def steward_action_interaction_completed(
        self,
        interaction: StewardActionInteraction,
        outcome: str,
    ) -> None:
        if outcome != "completed":
            raise AssertionError(f"unexpected Steward action interaction outcome: {outcome}")
        if interaction.completion_recorded:
            return
        interaction.completion_recorded = True
        for operation in interaction.operations:
            if not operation.done:
                operation.cancel()
        self._clear_steward_action_catalog_slot(interaction)
        self._logger.event(
            "steward_action_interaction_completed",
            **self._hooks.access_point_fields(interaction.access_point),
            action_round=interaction.action_round,
            outcome="cancelled" if interaction.cancelled else "completed",
        )

    def steward_event(self, conversation: StewardConversation, name: str, **fields: Any) -> None:
        self._logger.event(name, **self._hooks.access_point_fields(conversation.access_point), **fields)

    def steward_begin_startup_notice(
        self, conversation: StewardConversation, results: list[dict[str, Any]],
    ) -> None:
        self._begin_agent_startup_action_notice(conversation.access_point, results)
        deferred = conversation.take_deferred_startup_completion()
        if deferred is not None:
            self._handle_agent_startup_result(conversation.access_point, deferred)

    def steward_submit_followup(self, conversation: StewardConversation, prompt: str) -> None:
        self._queue_steward_turn(
            access_point=conversation.access_point,
            agent_input=AgentInput(
                text=prompt,
                origin=AgentInputOrigin.CONTROL,
                verbatim=True,
            ),
            owner=conversation,
        )

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

    def steward_queue_failure(self, conversation: StewardConversation, error: str) -> None:
        self._queue_terminal_failure(conversation, error=error, source="steward")

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
        if isinstance(payload, InteractiveInspectionResult):
            for interaction in self._interaction_queue.snapshot():
                if not isinstance(interaction, InspectInteraction):
                    continue
                if interaction.target != event.access_point:
                    continue
                if interaction.receive(
                    event.source,
                    payload.inspection_ref,
                    payload.projection,
                ):
                    return
            self._logger.event(
                "stale_inspection_result_ignored",
                **self._hooks.access_point_fields(event.access_point),
                source=event.source,
                inspection_ref=payload.inspection_ref.value,
            )
            return
        if isinstance(payload, InteractiveInterruptResult):
            interaction = self._turn_interaction_for(payload.turn_ref)
            if interaction is not None:
                if (
                    interaction.receive_terminal(payload, outcome="interrupted")
                    is TerminalAcceptance.DUPLICATE
                ):
                    self._log_turn_terminal_ignored(
                        event,
                        payload.turn_ref,
                        reason="duplicate",
                        terminal_kind="interrupted",
                    )
                    return
                owner = interaction.owner
                if isinstance(owner, ApprovalInteraction):
                    owner.on_target_interrupted(self)
                    return
                if isinstance(owner, (AgentConversation, StewardConversation)):
                    self._cancel_pending_form(event.access_point, source=owner.source)
                    pending = owner.interrupt_notice
                    if pending is not None:
                        pending.set_result(
                            self,
                            text=self._interrupt_final_text(success=True),
                            outcome=LocalCommandOutcome(
                                command="/interrupt",
                                result={
                                    "ok": True,
                                    "code": "interrupted",
                                    "target": owner.source,
                                },
                            ),
                        )
                    if isinstance(owner, AgentConversation):
                        source_child = self._agent_approval_for_source(event.access_point)
                        if source_child is not None:
                            source_child.cancel_source(self, "source_interrupted")
                    self._cancel_pending_approval(event.access_point, source=owner.source)
                    owner.finish()
                    if isinstance(owner, AgentConversation) and owner.routed_handoff is not None:
                        handoff = owner.routed_handoff
                        self._logger.event(
                            "routing_target_interrupted",
                            **self._routing_access_point_fields(event.access_point, prefix="target"),
                            sender_address=handoff.sender_address,
                            target_address=handoff.target_address,
                            source_turn_ref_sequence=handoff.source_turn_ref.sequence,
                        )
                    return
            self._log_turn_terminal_ignored(
                event,
                payload.turn_ref,
                reason="unknown",
                terminal_kind="interrupted",
            )
            return
        if isinstance(payload, InteractiveAgentResult):
            interaction = self._turn_interaction_for(payload.turn_ref)
            if interaction is not None:
                if (
                    interaction.receive_terminal(payload, outcome=payload.outcome.value)
                    is TerminalAcceptance.DUPLICATE
                ):
                    self._log_turn_terminal_ignored(
                        event,
                        payload.turn_ref,
                        reason="duplicate",
                        terminal_kind=payload.outcome.value,
                    )
                    return
                owner = interaction.owner
                if isinstance(owner, AgentConversation):
                    self._cancel_pending_form(event.access_point, source="agent")
                    owner.handle_result(self, payload)
                    return
                if isinstance(owner, StewardConversation):
                    self._cancel_pending_form(event.access_point, source="steward")
                    if owner.interrupt_notice is None:
                        self._cancel_pending_approval(event.access_point, source="steward")
                        self._logger.event(
                            "steward_driver_result_received",
                            **self._hooks.access_point_fields(event.access_point),
                            route_target="steward",
                            reply_preview=(
                                payload.rendered_error
                                if payload.failed
                                else payload.require_output().text
                            ).strip()[:200],
                        )
                        if payload.failed:
                            owner.handle_failure(self, payload.rendered_error)
                        else:
                            owner.handle_result(self, payload)
                    return
                if isinstance(owner, ApprovalInteraction):
                    if owner.phase != "delegated":
                        return
                    if not (self._approval_parent_active(owner) or owner.source_cancelled):
                        owner.finish()
                        return
                    if payload.failed:
                        owner.on_target_failed(self, payload.rendered_error)
                    else:
                        owner.on_target_result(self, payload.require_output().text)
                    return
            self._log_turn_terminal_ignored(
                event,
                payload.turn_ref,
                reason="unknown",
                terminal_kind=payload.outcome.value,
            )
            return
        self._apply_driver_event(event)

    def _apply_driver_event(self, event: StewardDriverEvent) -> None:
        payload = event.event
        if isinstance(payload, InteractiveModelApplyResult):
            if event.access_point not in self._pending_model_commands:
                self._logger.event(
                    "steward_stale_model_result_ignored",
                    **self._hooks.access_point_fields(event.access_point),
                    result=payload.result,
                )
                return
            self._pending_model_commands.discard(event.access_point)
            succeeded = payload.result in {"requested", "completed"}
            if payload.result == "requested":
                text = f"model requested: {payload.requested_model}"
                code = "requested"
            elif payload.result == "completed":
                text = f"model applied: {payload.actual_model or payload.requested_model}"
                code = "completed"
            elif payload.result == "uncertain":
                text = "model request outcome uncertain; provider session stopped"
                code = "uncertain"
            else:
                text = "model request failed: provider rejected the configuration"
                code = "rejected"
            self._queue_local_command_reply(
                access_point=event.access_point,
                outcome=LocalCommandOutcome(
                    command=f"/model {payload.requested_model}",
                    result={
                        "ok": succeeded,
                        "code": code,
                        "requested_model": payload.requested_model,
                        "effective_model": payload.actual_model,
                    },
                ),
                text=text,
                kind=self._kinds.command if succeeded else self._kinds.warning,
            )
            return
        if isinstance(payload, InteractiveEffortApplyResult):
            if event.access_point not in self._pending_effort_commands:
                self._logger.event(
                    "steward_stale_effort_result_ignored",
                    **self._hooks.access_point_fields(event.access_point),
                    result=payload.result,
                )
                return
            self._pending_effort_commands.discard(event.access_point)
            succeeded = payload.result in {"requested", "completed"}
            if payload.result == "requested":
                text = f"effort applied: {payload.actual_effort or payload.requested_effort}"
                code = "requested"
            elif payload.result == "completed":
                text = f"effort applied: {payload.actual_effort or payload.requested_effort}"
                code = "completed"
            elif payload.result == "uncertain":
                text = "effort request outcome uncertain; provider session stopped"
                code = "uncertain"
            else:
                text = "effort request failed: provider rejected the configuration"
                code = "rejected"
            self._queue_local_command_reply(
                access_point=event.access_point,
                outcome=LocalCommandOutcome(
                    command=f"/effort {payload.requested_effort}",
                    result={
                        "ok": succeeded,
                        "code": code,
                        "requested_effort": payload.requested_effort,
                        "effective_effort": payload.actual_effort,
                    },
                ),
                text=text,
                kind=self._kinds.command if succeeded else self._kinds.warning,
            )
            return
        if isinstance(payload, InteractiveStatusEvent):
            if payload.suppress_status:
                return
            if event.source == "agent":
                conversation = self.active_conversation_for(event.access_point)
                handoff = conversation.routed_handoff if isinstance(conversation, AgentConversation) else None
                if (
                    handoff is not None
                    and payload.kind is WorkStatusKind.TURN_STARTED
                    and not handoff.target_turn_started
                ):
                    handoff.target_turn_started = True
                    self._logger.event(
                        "routing_target_turn_started",
                        **self._routing_access_point_fields(event.access_point, prefix="target"),
                        sender_address=handoff.sender_address,
                        target_address=handoff.target_address,
                        source_turn_ref_sequence=handoff.source_turn_ref.sequence,
                        target_turn_ref_sequence=payload.turn_ref.sequence,
                    )
            _output_runtime, status_store = self._access_point_adapter.status_runtime_for(
                access_point=event.access_point,
                source=self._kinds.agent_status_source if event.source == "agent" else self._kinds.steward_status_source,
            )
            stream_key = status_stream_key(
                payload,
                semantic_source=event.source,
                access_point=(event.access_point.chat_id, event.access_point.thread_id),
            )
            status_store.apply_agent_status(
                stream_key=stream_key,
                status_text=payload.status_text,
                kind=payload.kind,
                status_text_kind=payload.status_text_kind,
                work=payload.work,
                item_ref=payload.item_ref,
                begins_turn=payload.begins_turn,
                preserve_work_snapshot=payload.preserve_work_snapshot,
            )
            return
        if isinstance(payload, InteractiveApprovalPromptEvent):
            self._logger.event(
                "steward_driver_approval_registered",
                **self._hooks.access_point_fields(event.access_point),
                route_target=event.source,
                approval_ref=payload.request.approval_ref.value,
            )
            self.register_approval(event, payload.request)
            return
        if isinstance(payload, InteractiveElicitationPromptEvent):
            self._logger.event(
                "steward_driver_elicitation_registered",
                **self._hooks.access_point_fields(event.access_point),
                route_target=event.source,
            )
            self._register_form_elicitation(event, payload.request)
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
            self._cancel_pending_configuration_commands(
                event.access_point,
                reason="runtime stopped unexpectedly",
            )
            self._terminate_session_turns(
                event.access_point,
                "agent",
                payload.error or "target runtime stopped unexpectedly",
            )
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
            self._cancel_pending_form(event.access_point, source="agent")
            source_child = self._agent_approval_for_source(event.access_point)
            if source_child is not None:
                source_child.cancel_source(self, "source_runtime_failed")
            target_child = self._delegation_for_target(event.access_point)
            if target_child is not None:
                target_child.on_target_failed(
                    self,
                    payload.error or "target runtime stopped unexpectedly",
                )
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
        raise AssertionError(f"unsupported steward driver event: {payload!r}")

    def _cancel_pending_configuration_commands(
        self,
        access_point: AccessPointKey,
        *,
        reason: str,
    ) -> None:
        for pending, command, label in (
            (self._pending_model_commands, "/model", "model"),
            (self._pending_effort_commands, "/effort", "effort"),
        ):
            if access_point not in pending:
                continue
            pending.discard(access_point)
            self._queue_local_command_reply(
                access_point=access_point,
                outcome=LocalCommandOutcome(
                    command=command,
                    result={"ok": False, "code": "cancelled"},
                ),
                text=f"{label} request cancelled: {reason}",
                kind=self._kinds.warning,
            )

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

    def agent_turn_error(
        self,
        conversation: AgentConversation,
        error: str,
        raw_provider_error: RawProviderErrorPayload | None,
    ) -> None:
        self._logger.event(
            self._hooks.turn_error_event,
            **self._hooks.access_point_fields(conversation.access_point),
            route_target="agent",
            error=error,
            error_type="RuntimeError",
            **raw_provider_error_lifecycle_fields(raw_provider_error),
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
        turn_ref: TurnRef,
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
            turn_ref=turn_ref,
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

    def agent_send_failure(self, conversation: AgentConversation, error: str) -> None:
        self._queue_terminal_failure(conversation, error=error, source="agent")

    def _queue_terminal_failure(
        self,
        conversation: AgentConversation | StewardConversation,
        *,
        error: str,
        source: str,
    ) -> None:
        self._access_point_adapter.queue_turn_failure(
            access_point=conversation.access_point,
            error=error,
            source=source,
            kind=self._kinds.reply,
            on_sent=lambda body=error, owner=conversation: self._complete_reply_delivery(
                conversation=owner, text=body,
            ),
            on_failed=lambda exc, owner=conversation: self._fail_reply_delivery(
                conversation=owner, exc=exc,
            ),
        )

    def agent_submit_request(self, access_point: AccessPointKey, agent_input: AgentInput) -> None:
        owner = self._agent_conversation_for(access_point)
        if owner is None:
            raise RuntimeError("agent turn has no owning conversation")
        self._queue_agent_turn(
            access_point=access_point,
            agent_input=agent_input,
            owner=owner,
        )

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
        turn_ref = conversation.routing_turn_ref()
        self._logger.event(
            "routing_response_completed",
            **self._routing_access_point_fields(source_access_point, prefix="sender"),
            sender_address=sender_address,
            target_address=HUMAN_ADDRESS,
            turn_ref_sequence=turn_ref.sequence,
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
        turn_ref = conversation.routing_turn_ref()
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
        live_queue_size = self._live_routing_queue_size()
        if live_queue_size >= self._routing_queue_capacity:
            self._queue_routing_access_point_copy(
                access_point=sender_access_point,
                text=sender_copy_text,
                kind=output_kind,
                sender_address=sender_address,
                target_address=envelope.target,
                turn_ref=turn_ref,
                repair_attempt=conversation.routing.repair_attempt,
                copy_role="sender",
            )
            target_state = self._agent_runtime.runtime_state(target_access_point)
            self._logger.event(
                "routing_request_failed",
                **self._routing_access_point_fields(sender_access_point, prefix="sender"),
                **self._routing_access_point_fields(target_access_point, prefix="target"),
                queue_sequence=sequence,
                queue_capacity=self._routing_queue_capacity,
                sender_address=sender_address,
                target_address=envelope.target,
                source_turn_ref_sequence=turn_ref.sequence,
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
                turn_ref=turn_ref,
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
            source_turn_ref=turn_ref,
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
        self._queue_routing_access_point_copy(
            access_point=sender_access_point,
            text=sender_copy_text,
            kind=output_kind,
            sender_address=sender_address,
            target_address=envelope.target,
            turn_ref=turn_ref,
            repair_attempt=conversation.routing.repair_attempt,
            copy_role="sender",
            receipt_owner=request,
        )
        self._interaction_queue.append(request)
        if target_access_point != sender_access_point:
            self._queue_routing_access_point_copy(
                access_point=target_access_point,
                text=routed_reply_text,
                kind=output_kind,
                sender_address=sender_address,
                target_address=envelope.target,
                turn_ref=turn_ref,
                repair_attempt=conversation.routing.repair_attempt,
                copy_role="target",
                receipt_owner=request,
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
            turn_ref_sequence=turn_ref.sequence,
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

    def routing_request_runtime_start(self, request: RoutedConversation) -> str:
        return self._request_agent_runtime_start(
            request.target_access_point,
            trigger_kind="routing",
        ).value

    def _routing_request_event_fields(self, request: RoutedConversation) -> dict[str, Any]:
        return {
            **self._routing_access_point_fields(request.sender_access_point, prefix="sender"),
            **self._routing_access_point_fields(request.target_access_point, prefix="target"),
            "queue_sequence": request.sequence,
            "queue_size": self._live_routing_queue_size(),
            "queue_capacity": self._routing_queue_capacity,
            "sender_address": request.sender_address,
            "target_address": request.target_address,
            "source_turn_ref_sequence": request.source_turn_ref.sequence,
            "repair_attempt": request.repair_attempt,
        }

    def tick_interactions(self) -> bool:
        context = InteractionTickContext(runtime=self)
        progressed = self._interaction_queue.tick(context)
        self._idle_poll_blocked_resources = frozenset(context.blocked_targets)
        return progressed

    def inspect_begin(self, interaction: InspectInteraction) -> None:
        access_point = interaction.target
        if not isinstance(access_point, AccessPointKey):
            raise TypeError("Steward inspect target must be AccessPointKey")
        snapshot = self._runtime_snapshot(access_point)
        lanes = (
            ("steward", self._runtime, snapshot.steward),
            ("agent", self._agent_runtime, snapshot.agent),
        )
        for source, lane, node in lanes:
            projection = lane.get_local_projection(access_point)
            interaction.projections[source] = projection
            if node is None or node.state != "RUNNING":
                continue
            inspection_ref = InspectionRef.new()
            receipt = lane.request_inspection(
                access_point,
                inspection_ref,
                self._INSPECTION_TIMEOUT_SEC,
            )
            if receipt.outcome is AgentEffectOutcome.ACCEPTED:
                interaction.expect(source, inspection_ref)
                self._logger.event(
                    "inspection_refresh_requested",
                    **self._hooks.access_point_fields(access_point),
                    source=source,
                    inspection_ref=inspection_ref.value,
                    timeout_sec=self._INSPECTION_TIMEOUT_SEC,
                )
                continue
            interaction.projections[source] = projection_with_session_error(
                projection,
                receipt.error or f"refresh {receipt.outcome.value}",
            )
        interaction.complete_without_refresh()

    def inspect_interaction_completed(self, interaction: InspectInteraction) -> None:
        access_point = interaction.target
        requester = interaction.requester
        if not isinstance(access_point, AccessPointKey) or not isinstance(requester, AccessPointKey):
            raise TypeError("Steward inspect endpoints must be AccessPointKey")
        text = self._build_inspect_text(
            access_point=access_point,
            snapshot=self._runtime_snapshot(access_point),
            steward_projection=interaction.projections.get(
                "steward",
                self._runtime.get_local_projection(access_point),
            ),
            agent_projection=interaction.projections.get(
                "agent",
                self._agent_runtime.get_local_projection(access_point),
            ),
        )
        if access_point == requester:
            remaining = self._pending_inspect_commands.get(access_point, 0) - 1
            if remaining > 0:
                self._pending_inspect_commands[access_point] = remaining
            else:
                self._pending_inspect_commands.pop(access_point, None)
            self._queue_local_command_reply(
                access_point=access_point,
                outcome=LocalCommandOutcome(
                    command=interaction.command,
                    result={"ok": True, "code": "shown", "state": self._runtime_snapshot(access_point).state},
                ),
                text=text,
                kind=self._kinds.command,
            )
        else:
            self._queue_local_reply(
                access_point=access_point,
                text=text,
                source="steward",
                kind=self._kinds.command,
            )
        self._logger.event(
            "inspection_completed",
            **self._hooks.access_point_fields(access_point),
            requested_by_chat_id=requester.chat_id,
            requested_by_thread_id=requester.thread_id,
            command=interaction.command,
        )

    def agent_turn_wait_reason(self, interaction: AgentTurnInteraction) -> str:
        if isinstance(interaction.owner, ApprovalInteraction):
            reason = interaction.owner.target_turn_wait_reason(self)
            if reason:
                return reason
        lane = self._turn_lane(interaction)
        return lane.turn_wait_reason(self._turn_access_point(interaction))

    def agent_turn_start(self, interaction: AgentTurnInteraction) -> AgentEffectResult:
        lane = self._turn_lane(interaction)
        request = interaction.request
        if not isinstance(request, AgentInput):
            return AgentEffectResult.rejected("turn interaction request must be AgentInput")
        receipt = lane.start_turn(
            self._turn_access_point(interaction),
            request,
            interaction.turn_ref,
            context_note=interaction.context_note,
        )
        if (
            receipt.outcome is AgentEffectOutcome.ACCEPTED
            and isinstance(interaction.owner, ApprovalInteraction)
        ):
            interaction.owner.on_target_turn_started(self)
        return receipt

    def agent_steer_wait_reason(self, interaction: AgentSteerInteraction) -> str:
        lane = self._turn_lane(interaction)
        return lane.steer_wait_reason(
            self._turn_access_point(interaction),
            interaction.expected_turn_ref,
        )

    def agent_steer_submit(self, interaction: AgentSteerInteraction) -> AgentEffectResult:
        request = interaction.request
        if not isinstance(request, AgentInput):
            return AgentEffectResult.rejected("steer interaction request must be AgentInput")
        lane = self._turn_lane(interaction)
        return lane.try_steer_turn(
            self._turn_access_point(interaction),
            request,
            interaction.expected_turn_ref,
            context_note=interaction.context_note,
        )

    def agent_steer_interaction_completed(
        self,
        interaction: AgentSteerInteraction,
        outcome: str,
    ) -> None:
        access_point = self._turn_access_point(interaction)
        request = interaction.request
        if not isinstance(request, AgentInput):
            raise TypeError("steer interaction request must be AgentInput")
        owner = interaction.owner
        fallback = "none"
        confirmed = False
        if interaction.accepted:
            if isinstance(owner, AgentConversation):
                confirmed = owner.confirm_steer(request)
        else:
            if isinstance(owner, AgentConversation):
                owner.discard_steer(request)
        if interaction.requires_fallback:
            fallback = "next_turn"
            if interaction.lane == "agent":
                fallback_owner = self._new_human_agent_conversation(access_point)
                self._interaction_queue.append(fallback_owner)
                self._queue_agent_turn(
                    access_point=access_point,
                    agent_input=request,
                    owner=fallback_owner,
                )
            elif interaction.lane == "steward":
                fallback_owner = StewardConversation(access_point=access_point, allow_steer=True)
                self._interaction_queue.append(fallback_owner)
                self._queue_steward_turn(
                    access_point=access_point,
                    agent_input=request,
                    owner=fallback_owner,
                    context_note=interaction.context_note,
                )
            else:
                raise AssertionError(f"unknown steer lane: {interaction.lane!r}")
        self._logger.event(
            "agent_steer_interaction_completed",
            **self._hooks.access_point_fields(access_point),
            lane=interaction.lane,
            runtime_generation=interaction.expected_turn_ref.runtime_generation.value,
            turn_ref_sequence=interaction.expected_turn_ref.sequence,
            outcome=outcome,
            fallback=fallback,
            interaction_confirmed=confirmed,
            error=interaction.error or None,
            prompt_preview=request.text.strip()[:200],
        )

    def agent_turn_start_failed(
        self,
        interaction: AgentTurnInteraction,
        receipt: AgentEffectResult,
    ) -> None:
        error = receipt.error or f"turn start {receipt.outcome.value}"
        access_point = self._turn_access_point(interaction)
        self._logger.event(
            "agent_turn_start_failed",
            **self._hooks.access_point_fields(access_point),
            lane=interaction.lane,
            runtime_generation=interaction.turn_ref.runtime_generation.value,
            turn_ref_sequence=interaction.turn_ref.sequence,
            outcome=receipt.outcome.value,
            error=error,
        )
        result = self._turn_error_result(interaction, error)
        if (
            interaction.receive_terminal(result, outcome="failed")
            is TerminalAcceptance.ACCEPTED
        ):
            self._deliver_failed_turn_owner(interaction, result)

    def agent_turn_interaction_completed(
        self,
        interaction: AgentTurnInteraction,
        outcome: str,
    ) -> None:
        self._logger.event(
            "agent_turn_interaction_completed",
            lane=interaction.lane,
            turn_ref_sequence=interaction.turn_ref.sequence,
            outcome=outcome,
        )

    def _turn_lane(
        self,
        interaction: AgentTurnInteraction | AgentSteerInteraction,
    ) -> StewardDeliveryLane:
        if interaction.lane == "agent":
            return self._agent_runtime
        if interaction.lane == "steward":
            return self._runtime
        raise AssertionError(f"unknown turn lane: {interaction.lane!r}")

    @staticmethod
    def _turn_access_point(
        interaction: AgentTurnInteraction | AgentSteerInteraction,
    ) -> AccessPointKey:
        owner = interaction.owner
        access_point = getattr(owner, "access_point", None)
        if not isinstance(access_point, AccessPointKey):
            raise RuntimeError("turn interaction owner has no access point")
        return access_point

    def _turn_error_result(
        self,
        interaction: AgentTurnInteraction,
        error: str,
    ) -> InteractiveAgentResult:
        access_point = self._turn_access_point(interaction)
        request = interaction.request
        if not isinstance(request, AgentInput):
            request = AgentInput(text="", origin=AgentInputOrigin.CONTROL, verbatim=True)
        return InteractiveAgentResult.failed_result(
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            agent_input=request,
            error=error,
            turn_ref=interaction.turn_ref,
        )

    def _terminate_session_turns(
        self,
        access_point: AccessPointKey,
        lane: str,
        error: str,
    ) -> None:
        for interaction in self._interaction_queue.snapshot():
            if (
                isinstance(interaction, AgentTurnInteraction)
                and interaction.lane == lane
                and self._turn_access_point(interaction) == access_point
                and not interaction.terminal_outcome
            ):
                interaction.receive_terminal(
                    self._turn_error_result(interaction, error),
                    outcome="failed",
                )
            elif (
                isinstance(interaction, AgentSteerInteraction)
                and interaction.lane == lane
                and self._turn_access_point(interaction) == access_point
            ):
                interaction.cancel(error)
            elif (
                isinstance(interaction, InspectInteraction)
                and interaction.target == access_point
                and interaction.fail(lane, error)
            ):
                self._logger.event(
                    "inspection_source_terminated",
                    **self._hooks.access_point_fields(access_point),
                    source=lane,
                    error=error,
                )

    def _has_pending_inspect(self, access_point: AccessPointKey) -> bool:
        return any(
            isinstance(interaction, InspectInteraction)
            and interaction.target == access_point
            and bool(interaction.pending)
            for interaction in self._interaction_queue.snapshot()
        )

    def _deliver_failed_turn_owner(
        self,
        interaction: AgentTurnInteraction,
        result: InteractiveAgentResult,
    ) -> None:
        owner = interaction.owner
        if isinstance(owner, AgentConversation):
            owner.handle_result(self, result)
        elif isinstance(owner, StewardConversation):
            owner.handle_failure(self, result.rendered_error)
        elif isinstance(owner, ApprovalInteraction):
            owner.on_target_failed(self, result.rendered_error)

    def routing_target_wait_reason(self, access_point: AccessPointKey, state: str) -> str:
        return self._access_point_work_snapshot(
            access_point, runtime_state=state,
        ).routing_target_wait_reason()

    def routing_submit(self, request: RoutedConversation) -> None:
        target = request.target_access_point
        conversation = AgentConversation(
            access_point=target,
            routed_handoff=RoutedHandoff(
                sender_address=request.sender_address,
                target_address=request.target_address,
                source_turn_ref=request.source_turn_ref,
            ),
            routed_reply_to=request.routed_reply_to,
        )
        self._interaction_queue.append(conversation)
        self._queue_agent_turn(
            access_point=target,
            agent_input=AgentInput(
                text=request.target_input,
                origin=AgentInputOrigin.ROUTING,
                verbatim=True,
            ),
            owner=conversation,
            phase="waiting",
        )

    def routing_dispatched(self, request: RoutedConversation) -> None:
        request.dispatched = True
        self._logger.event("routing_request_dispatched", **self._routing_request_event_fields(request))
        request.set_notice_state(
            self,
            state="delivered",
            text=(
                f"routing to {request.target_address} delivered. "
                "Request submitted to target runtime; response is still pending."
            ),
        )
        for correlation in request.pending_copy_delivery_receipts:
            self._logger.event("routing_access_point_copy_delivered", **correlation)
        request.pending_copy_delivery_receipts.clear()

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
        request.start_notice(
            self,
            (
                f"routing to {request.target_address} queued ({reason}). "
                "Pending routing is kept in memory and will not survive an ORC restart."
            ),
        )

    def routing_fail(
        self,
        request: RoutedConversation,
        failure: RoutingFailure,
    ) -> None:
        if request.failure is not None:
            return
        request.failure = failure
        details = failure.details.rstrip()
        if details and details[-1] not in ".!?":
            details += "."
        self._logger.event(
            "routing_request_failed",
            **self._routing_request_event_fields(request),
            error_code=failure.code,
            error=failure.details,
            target_state=failure.target_state,
        )
        request.set_notice_state(
            self,
            state="failed",
            text=(
                f"routing to {request.target_address} failed ({failure.code}): "
                f"{details}"
            ),
        )

    def routing_notice_queue_initial(
        self,
        request: RoutedConversation,
        *,
        text: str,
    ) -> None:
        try:
            self._access_point_adapter.queue_text_reply_with_result(
                access_point=request.sender_access_point,
                text=text,
                source="agent",
                kind=self._kinds.reply,
                on_sent=lambda message_ref: request.notice_sent(self, message_ref),
                on_failed=lambda exc: request.notice_send_failed(self, exc),
            )
        except Exception as exc:
            request.notice_send_failed(self, exc)

    def routing_notice_queue_edit(
        self,
        request: RoutedConversation,
        *,
        message_ref: AccessPointMessageRef,
        text: str,
        state: str,
    ) -> None:
        try:
            self._access_point_adapter.queue_text_reply_edit(
                access_point=request.sender_access_point,
                message_ref=message_ref,
                text=text,
                source="agent",
                kind=self._kinds.reply,
                on_sent=lambda: request.notice_edited(self, state=state, text=text),
                on_failed=lambda exc: request.notice_edit_failed(
                    self,
                    exc,
                    state=state,
                    text=text,
                ),
            )
        except Exception as exc:
            request.notice_edit_failed(self, exc, state=state, text=text)

    def routing_notice_sent(self, request: RoutedConversation) -> None:
        self._logger.event(
            "routing_notice_sent",
            **self._routing_request_event_fields(request),
            notice_state=request.notice_state,
        )

    def routing_notice_updated(
        self,
        request: RoutedConversation,
        *,
        state: str,
    ) -> None:
        self._logger.event(
            "routing_notice_updated",
            **self._routing_request_event_fields(request),
            notice_state=state,
        )

    def routing_notice_update_failed(
        self,
        request: RoutedConversation,
        exc: Exception,
        *,
        phase: str,
        state: str,
    ) -> None:
        self._logger.event(
            "routing_notice_update_failed",
            **self._routing_request_event_fields(request),
            notice_state=state,
            phase=phase,
            error=str(exc),
            error_type=type(exc).__name__,
            http_code=extract_http_code(exc),
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
            turn_ref=request.source_turn_ref,
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
        turn_ref: TurnRef,
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
            turn_ref_sequence=turn_ref.sequence,
        )
        try:
            conversation = StewardConversation(
                access_point=sender_access_point,
                allow_steer=False,
            )
            self._interaction_queue.append(conversation)
            self._queue_steward_turn(
                access_point=sender_access_point,
                agent_input=AgentInput(
                    text=prompt,
                    origin=AgentInputOrigin.CONTROL,
                    verbatim=True,
                ),
                owner=conversation,
            )
        except Exception as exc:
            self._logger.event(
                "routing_exception_escalation_failed",
                **self._routing_access_point_fields(sender_access_point, prefix="sender"),
                sender_address=sender_address,
                target_address=target_address,
                error_code=error_code,
                error=str(exc),
                error_type=type(exc).__name__,
                turn_ref_sequence=turn_ref.sequence,
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
        projected_session_id = self._project_session_id(
            provider=str(binding.get("provider") or ""),
            profile=str(binding.get("profile") or ""),
            native_id=str(binding.get("thread_id") or ""),
        )
        return {
            f"{prefix}_access_point_type": access_point.type,
            f"{prefix}_chat_id": access_point.chat_id,
            f"{prefix}_thread_id": access_point.thread_id,
            f"{prefix}_agent_id": str(binding.get("agent_id") or ""),
            f"{prefix}_provider_session_ref": projected_session_id,
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
            turn_ref = conversation.routing_turn_ref()
            self._logger.event(
                "routing_message_delivered",
                **self._routing_access_point_fields(access_point, prefix="sender"),
                sender_address=conversation.routing.sender_address,
                target_address=conversation.routing.target_address,
                turn_ref_sequence=turn_ref.sequence,
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
        pending_statuses: dict[StatusStreamKey, StewardDriverEvent] = {}

        def _flush_pending_statuses() -> None:
            nonlocal pending_statuses
            if not pending_statuses:
                return
            for key in sorted(
                pending_statuses.keys(),
                key=lambda value: (
                    type(value.access_point[0]).__name__,
                    str(value.access_point[0]),
                    type(value.access_point[1]).__name__,
                    str(value.access_point[1]),
                    value.semantic_source,
                    value.turn_ref.runtime_generation.value,
                    value.turn_ref.sequence,
                    (
                        value.status_ref.runtime_generation.value
                        if value.status_ref is not None
                        else ""
                    ),
                    value.status_ref.sequence if value.status_ref is not None else -1,
                    value.stream_kind.value,
                ),
            ):
                self.handle_driver_event(pending_statuses[key])
            pending_statuses = {}

        def _drain(events: list[StewardDriverEvent]) -> None:
            nonlocal progressed
            for event in events:
                progressed = True
                if isinstance(event.event, InteractiveStatusEvent):
                    key = status_stream_key(
                        event.event,
                        semantic_source=event.source,
                        access_point=(
                            event.access_point.chat_id,
                            event.access_point.thread_id,
                        ),
                    )
                    previous = pending_statuses.get(key)
                    if previous is None:
                        pending_statuses[key] = event
                    else:
                        pending_statuses[key] = StewardDriverEvent(
                            access_point=event.access_point,
                            source=event.source,
                            event=coalesce_status_events(
                                previous.event,
                                event.event,
                                semantic_source=event.source,
                            ),
                        )
                    continue
                _flush_pending_statuses()
                self.handle_driver_event(event)

        blocked_resources = self._idle_poll_blocked_resources
        _drain(
            self._runtime.poll_once(
                idle_poll_blocked_resources=blocked_resources,
            )
        )
        if self._runtime.consume_poll_progress():
            progressed = True
        _drain(
            self._agent_runtime.poll_once(
                idle_poll_blocked_resources=blocked_resources,
            )
        )
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

    def _catalog_execution_profiles(self) -> tuple[ProviderExecutionProfile, ...]:
        return tuple(
            execution_profile
            for execution_profile in self._profile_catalog.execution_profiles
            if AgentCapability.LIST_SESSIONS
            in self._profile_catalog.supported_capabilities(execution_profile)
        )

    def _catalog_interaction_sequence(self) -> int:
        sequence = self._next_catalog_interaction_sequence
        self._next_catalog_interaction_sequence += 1
        return sequence

    def _submit_profile_catalog_operations(
        self,
        *,
        interaction_kind: CatalogInteractionKind,
        access_point: AccessPointKey,
        interaction_sequence: int,
        cwd: str,
        limit: int | None,
        include_installation: bool,
    ) -> tuple[
        tuple[ProviderExecutionProfile, ...],
        tuple[CatalogOperationHandle[Any], ...],
        tuple[ProviderExecutionProfile, ...],
    ]:
        profiles = self._catalog_execution_profiles()
        operations: list[CatalogOperationHandle[Any]] = []
        busy_profiles: list[ProviderExecutionProfile] = []
        try:
            for operation_ordinal, execution_profile in enumerate(profiles):
                if include_installation:
                    operation = self._catalog_operations.submit(
                        lambda profile=execution_profile: self._list_agent_profile_catalog(
                            execution_profile=profile,
                            cwd=cwd,
                            limit=limit,
                        ),
                        admission_owner=execution_profile,
                        progress_context=CatalogOperationProgressContext(
                            interaction_kind=interaction_kind,
                            access_point_owner_ref=catalog_access_point_owner_ref(access_point),
                            interaction_sequence=interaction_sequence,
                            operation_ordinal=operation_ordinal,
                        ),
                    )
                    if operation is None:
                        busy_profiles.append(execution_profile)
                        operation = CatalogOperationHandle.completed(
                            (
                                execution_profile,
                                ProviderSessionsListFailed(
                                    ProviderSessionCatalogErrorKind.UNAVAILABLE,
                                    "provider session catalog request is already active",
                                ),
                                ProviderInstallationStatus(
                                    execution_profile,
                                    ProviderInstallationStatusKind.AVAILABLE,
                                ),
                            )
                        )
                else:
                    operation = self._catalog_operations.submit(
                        lambda profile=execution_profile: self._list_profile_catalog(
                            execution_profile=profile,
                            cwd=cwd,
                            limit=limit,
                        ),
                        admission_owner=execution_profile,
                        progress_context=CatalogOperationProgressContext(
                            interaction_kind=interaction_kind,
                            access_point_owner_ref=catalog_access_point_owner_ref(access_point),
                            interaction_sequence=interaction_sequence,
                            operation_ordinal=operation_ordinal,
                        ),
                    )
                    if operation is None:
                        busy_profiles.append(execution_profile)
                        operation = CatalogOperationHandle.completed(
                            (
                                execution_profile,
                                ProviderSessionsListFailed(
                                    ProviderSessionCatalogErrorKind.UNAVAILABLE,
                                    "provider session catalog request is already active",
                                ),
                            )
                        )
                operations.append(operation)
        except Exception:
            for operation in operations:
                operation.cancel()
            raise
        return profiles, tuple(operations), tuple(busy_profiles)

    def _list_profile_catalog(
        self,
        *,
        execution_profile: ProviderExecutionProfile,
        cwd: str,
        limit: int | None,
    ) -> tuple[ProviderExecutionProfile, ProviderSessionsListResult]:
        request = ListProviderSessions(
            execution_profile=execution_profile,
            cwd=cwd,
            limit=limit,
        )
        try:
            result = self._list_provider_sessions(request)
        except ProviderUnavailableError as exc:
            result = ProviderSessionsListFailed(
                kind=ProviderSessionCatalogErrorKind.UNAVAILABLE,
                error=str(exc),
            )
        return execution_profile, result

    def _list_agent_profile_catalog(
        self,
        *,
        execution_profile: ProviderExecutionProfile,
        cwd: str,
        limit: int | None,
    ) -> tuple[
        ProviderExecutionProfile,
        ProviderSessionsListResult,
        ProviderInstallationStatus,
    ]:
        request = ListProviderSessions(
            execution_profile=execution_profile,
            cwd=cwd,
            limit=limit,
        )
        try:
            result, installation = self._list_agent_sessions(request)
        except ProviderUnavailableError as exc:
            result = ProviderSessionsListFailed(
                kind=ProviderSessionCatalogErrorKind.UNAVAILABLE,
                error=str(exc),
            )
            installation = ProviderInstallationStatus(
                execution_profile,
                ProviderInstallationStatusKind.AVAILABLE,
            )
        if installation.execution_profile != execution_profile:
            raise RuntimeError("installation status belongs to another profile")
        return execution_profile, result, installation

    def _agent_catalog_listing_from_profile_results(
        self,
        rows: list[
            tuple[
                ProviderExecutionProfile,
                ProviderSessionsListResult,
                ProviderInstallationStatus,
            ]
        ],
    ) -> AgentCatalogListing:
        base = self._catalog_listing_from_profile_results(
            [(execution_profile, result) for execution_profile, result, _ in rows]
        )
        failures = list(base.failures)
        for execution_profile, _result, installation in rows:
            if installation.available:
                continue
            failures.append(
                AgentCatalogFailure(
                    provider=execution_profile.provider,
                    profile=execution_profile.profile,
                    kind=(
                        ProviderSessionCatalogErrorKind.INSTALLATION_UNAVAILABLE
                    ),
                    installation_reason=installation.kind,
                )
            )
        return AgentCatalogListing(base.entries, tuple(failures))

    def _catalog_listing_from_profile_results(
        self,
        rows: list[tuple[ProviderExecutionProfile, ProviderSessionsListResult]],
    ) -> AgentCatalogListing:
        entries: list[ProviderSessionCatalogEntry] = []
        failures: list[AgentCatalogFailure] = []
        for execution_profile, result in rows:
            if isinstance(result, ProviderSessionsListFailed):
                self._logger.event(
                    "provider_session_catalog_failed",
                    provider=execution_profile.provider.value,
                    profile=execution_profile.profile.value,
                    error_kind=result.kind.value,
                    error=result.error,
                )
                failures.append(
                    AgentCatalogFailure(
                        provider=execution_profile.provider,
                        profile=execution_profile.profile,
                        kind=result.kind,
                    )
                )
                continue
            if not isinstance(result, ProviderSessionsListed):
                raise RuntimeError("provider session catalog returned an invalid result")
            entries.extend(result.entries)
        return AgentCatalogListing(tuple(entries), tuple(failures))

    @staticmethod
    def _catalog_failure_lines(failures: list[AgentCatalogFailure]) -> list[str]:
        return [format_agent_catalog_failure(failure) for failure in failures]

    @classmethod
    def _append_catalog_warnings(
        cls,
        replies: list[str],
        failures: list[AgentCatalogFailure],
    ) -> list[str]:
        failure_lines = cls._catalog_failure_lines(failures)
        if failure_lines:
            replies.append("catalog warnings:\n" + "\n".join(failure_lines))
        return replies

    @classmethod
    def _merge_resume_catalog_warnings(
        cls,
        replies: list[str],
        failures: list[AgentCatalogFailure],
    ) -> list[str]:
        failure_lines = cls._catalog_failure_lines(failures)
        if failure_lines:
            replies[-1] += "\n\ncatalog warnings:\n" + "\n".join(failure_lines)
        return replies

    def _catalog_interaction_for(
        self,
        access_point: AccessPointKey,
    ) -> AgentCatalogInteraction | None:
        return next(
            (
                item
                for item in reversed(self._interaction_queue.snapshot())
                if isinstance(item, AgentCatalogInteraction)
                and item.access_point == access_point
                and not item.finished
            ),
            None,
        )

    def _begin_agents_catalog_interaction(
        self,
        *,
        access_point: AccessPointKey,
        command: AgentsCommand,
        raw_command: str,
    ) -> bool:
        target_cwd = ""
        if command.scope == "here":
            target_cwd = self._current_project_cwd(access_point)
            if not target_cwd:
                self._queue_local_command_reply(
                    access_point=access_point,
                    outcome=LocalCommandOutcome(
                        command=raw_command,
                        result={
                            "ok": False,
                            "code": "no_current_folder",
                            "scope": command.scope,
                        },
                    ),
                    text=(
                        "agents here unavailable: current access point has no folder. "
                        "Use /agents all."
                    ),
                    kind=self._kinds.warning,
                )
                return False
        if self._catalog_interaction_for(access_point) is not None:
            self._queue_local_command_reply(
                access_point=access_point,
                outcome=LocalCommandOutcome(
                    command=raw_command,
                    result={"ok": False, "code": "catalog_lookup_pending"},
                ),
                text="agents lookup is already pending for this access point.",
                kind=self._kinds.warning,
            )
            return False

        interaction = AgentCatalogInteraction(
            access_point=access_point,
            interaction_sequence=self._catalog_interaction_sequence(),
            raw_command=raw_command,
            scope=command.scope,
            target_cwd=target_cwd,
        )
        self._interaction_queue.append(interaction)
        self._begin_local_command_delivery(access_point)
        try:
            self._access_point_adapter.queue_text_reply_with_result(
                access_point=access_point,
                text=self._AGENT_CATALOG_STATUS_TEXT,
                source="steward",
                kind=self._kinds.command,
                on_sent=lambda message_ref: self._catalog_initial_sent(
                    interaction,
                    message_ref,
                ),
                on_failed=lambda exc: self._catalog_delivery_failed(
                    interaction,
                    exc,
                    phase="initial",
                ),
            )
        except Exception as exc:
            self._catalog_delivery_failed(interaction, exc, phase="initial")
        return True

    def _catalog_initial_sent(
        self,
        interaction: AgentCatalogInteraction,
        message_ref: AccessPointMessageRef,
    ) -> None:
        if interaction.finished or interaction.message_ref is not None:
            return
        if message_ref.access_point != interaction.access_point:
            self._catalog_delivery_failed(
                interaction,
                RuntimeError("catalog status returned a reference for another access point"),
                phase="initial",
            )
            return
        interaction.message_ref = message_ref
        self._logger.event(
            "agent_catalog_status_sent",
            **self._hooks.access_point_fields(interaction.access_point),
            scope=interaction.scope,
        )

    def _catalog_delivery_failed(
        self,
        interaction: AgentCatalogInteraction,
        exc: Exception,
        *,
        phase: str,
    ) -> None:
        if interaction.finished:
            return
        interaction.delivery_failed = True
        interaction.finished = True
        for operation in interaction.operations:
            operation.cancel()
        if interaction.outcome is None:
            interaction.outcome = LocalCommandOutcome(
                command=interaction.raw_command,
                result={"ok": False, "code": "delivery_failed", "scope": interaction.scope},
            )
        self._logger.event(
            "agent_catalog_delivery_failed",
            **self._hooks.access_point_fields(interaction.access_point),
            scope=interaction.scope,
            phase=phase,
            error_type=type(exc).__name__,
            http_code=extract_http_code(exc),
        )

    def _catalog_terminal_texts(
        self,
        *,
        interaction: AgentCatalogInteraction,
        listing: AgentCatalogListing,
    ) -> tuple[tuple[str, ...], LocalCommandOutcome]:
        failures = list(listing.failures)
        records = build_agent_catalog(
            sessions=listing.entries,
            bindings=self._catalog_binding_snapshot(),
            target_cwd=interaction.target_cwd,
        )
        replies = format_agent_catalog_chunks(
            scope=interaction.scope,
            records=records,
            message_char_limit=self._AGENTS_MESSAGE_CHAR_LIMIT,
            session_projector=self._profile_catalog.session_projector,
            record_limit=self._AGENTS_RESULT_LIMIT,
        )

        warning_lines = self._catalog_failure_lines(failures)
        if warning_lines:
            warning = "catalog warnings:\n" + "\n".join(warning_lines)
            combined = f"{replies[-1]}\n\n{warning}"
            if len(combined) <= self._AGENTS_MESSAGE_CHAR_LIMIT:
                replies[-1] = combined
            else:
                replies.append(warning)
        self._logger.event(
            "agents_command_executed",
            **self._hooks.access_point_fields(interaction.access_point),
            scope=interaction.scope,
            cwd=interaction.target_cwd,
            count=len(records),
        )
        return (
            tuple(replies),
            LocalCommandOutcome(
                command=interaction.raw_command,
                result={
                    "ok": True,
                    "code": "listed_partial" if failures else "listed",
                    "scope": interaction.scope,
                    "count": len(records),
                    "catalog_failures": agent_catalog_failure_payloads(failures),
                },
            ),
        )

    def _catalog_binding_snapshot(
        self,
    ) -> dict[AccessPointKey, PersistedAccessPointState]:
        bindings = dict(self._persisted_state_by_access_point)
        bindings.update(self._agent_runtime.snapshot_persisted())
        return bindings

    def _set_catalog_terminal(
        self,
        interaction: AgentCatalogInteraction,
        *,
        texts: tuple[str, ...],
        outcome: LocalCommandOutcome,
        kind: Any,
        reason: str,
    ) -> None:
        if interaction.final_texts or interaction.finished:
            return
        if not texts or any(not text for text in texts):
            raise ValueError("catalog terminal requires non-empty text")
        interaction.final_texts = texts
        interaction.outcome = outcome
        interaction.kind = kind
        interaction.terminal_reason = reason
        if interaction.message_ref is None:
            return
        try:
            self._access_point_adapter.queue_text_reply_edit(
                access_point=interaction.access_point,
                message_ref=interaction.message_ref,
                text=texts[0],
                source="steward",
                kind=kind,
                on_sent=lambda: self._catalog_terminal_edited(interaction),
                on_failed=lambda exc: self._catalog_delivery_failed(
                    interaction,
                    exc,
                    phase="terminal_edit",
                ),
            )
        except Exception as exc:
            self._catalog_delivery_failed(interaction, exc, phase="terminal_edit")

    def _catalog_terminal_edited(self, interaction: AgentCatalogInteraction) -> None:
        if interaction.finished or interaction.edit_delivered:
            return
        interaction.edit_delivered = True
        followups = interaction.final_texts[1:]
        interaction.pending_followups = len(followups)
        if not followups:
            interaction.finished = True
            return
        for index, text in enumerate(followups):
            try:
                self._access_point_adapter.queue_text_reply(
                    access_point=interaction.access_point,
                    text=text,
                    source="steward",
                    kind=interaction.kind,
                    on_sent=lambda item=interaction: self._catalog_followup_delivered(item),
                    on_failed=lambda exc, item=interaction: self._catalog_followup_failed(
                        item,
                        exc,
                    ),
                )
            except Exception as exc:
                interaction.pending_followups -= len(followups) - index
                interaction.delivery_failed = True
                self._logger.event(
                    "agent_catalog_delivery_failed",
                    **self._hooks.access_point_fields(interaction.access_point),
                    scope=interaction.scope,
                    phase="terminal_followup",
                    error_type=type(exc).__name__,
                    http_code=extract_http_code(exc),
                )
                if interaction.pending_followups == 0:
                    interaction.finished = True
                break

    def _catalog_followup_delivered(self, interaction: AgentCatalogInteraction) -> None:
        if interaction.finished or interaction.pending_followups <= 0:
            return
        interaction.pending_followups -= 1
        if interaction.pending_followups == 0:
            interaction.finished = True

    def _catalog_followup_failed(
        self,
        interaction: AgentCatalogInteraction,
        exc: Exception,
    ) -> None:
        if interaction.finished or interaction.pending_followups <= 0:
            return
        interaction.delivery_failed = True
        self._logger.event(
            "agent_catalog_delivery_failed",
            **self._hooks.access_point_fields(interaction.access_point),
            scope=interaction.scope,
            phase="terminal_followup",
            error_type=type(exc).__name__,
            http_code=extract_http_code(exc),
        )
        self._catalog_followup_delivered(interaction)

    def catalog_interaction_tick(
        self,
        interaction: AgentCatalogInteraction,
    ) -> str | None:
        if interaction.finished:
            return "completed"
        if interaction.message_ref is None:
            return None
        if not interaction.operations_started:
            try:
                operation_profiles, operations, busy_profiles = (
                    self._submit_profile_catalog_operations(
                        interaction_kind=CatalogInteractionKind.AGENTS,
                        access_point=interaction.access_point,
                        interaction_sequence=interaction.interaction_sequence,
                        cwd=interaction.target_cwd,
                        limit=None,
                        include_installation=True,
                    )
                )
            except (CatalogOperationCapacityError, RuntimeError) as exc:
                self._logger.event(
                    "agent_catalog_operation_rejected",
                    **self._hooks.access_point_fields(interaction.access_point),
                    scope=interaction.scope,
                    error_type=type(exc).__name__,
                )
                self._set_catalog_terminal(
                    interaction,
                    texts=(f"agents ({interaction.scope}) lookup failed.",),
                    outcome=LocalCommandOutcome(
                        command=interaction.raw_command,
                        result={
                            "ok": False,
                            "code": "lookup_failed",
                            "scope": interaction.scope,
                        },
                    ),
                    kind=self._kinds.warning,
                    reason="operation_rejected",
                )
                return None
            for execution_profile in busy_profiles:
                self._logger.event(
                    "agent_catalog_profile_busy",
                    **self._hooks.access_point_fields(interaction.access_point),
                    scope=interaction.scope,
                    provider=execution_profile.provider.value,
                    profile=execution_profile.profile.value,
                )
            interaction.operations_started = True
            interaction.operation_profiles = operation_profiles
            interaction.operations = operations
            interaction.operation_started_at = time.monotonic()
            self._logger.event(
                "agent_catalog_operation_started",
                **self._hooks.access_point_fields(interaction.access_point),
                scope=interaction.scope,
            )
            return None

        if interaction.final_texts:
            return None
        if all(operation.done for operation in interaction.operations):
            try:
                rows = []
                for operation in interaction.operations:
                    row = operation.take_result()
                    if row is None:
                        return None
                    rows.append(row)
            except Exception as exc:
                self._logger.event(
                    "agent_catalog_operation_failed",
                    **self._hooks.access_point_fields(interaction.access_point),
                    scope=interaction.scope,
                    error_type=type(exc).__name__,
                )
                self._set_catalog_terminal(
                    interaction,
                    texts=(f"agents ({interaction.scope}) lookup failed.",),
                    outcome=LocalCommandOutcome(
                        command=interaction.raw_command,
                        result={
                            "ok": False,
                            "code": "lookup_failed",
                            "scope": interaction.scope,
                        },
                    ),
                    kind=self._kinds.warning,
                    reason="operation_failed",
                )
                return None
            listing = self._agent_catalog_listing_from_profile_results(rows)
            texts, command_outcome = self._catalog_terminal_texts(
                interaction=interaction,
                listing=listing,
            )
            self._set_catalog_terminal(
                interaction,
                texts=texts,
                outcome=command_outcome,
                kind=self._kinds.command,
                reason="listed_partial" if listing.failures else "listed",
            )
            return None

        assert interaction.operation_started_at is not None
        if time.monotonic() - interaction.operation_started_at > self._AGENT_CATALOG_TIMEOUT_SEC:
            cancelled_profiles = [
                execution_profile.profile.value
                for execution_profile, operation in zip(
                    interaction.operation_profiles,
                    interaction.operations,
                    strict=True,
                )
                if operation.cancel()
            ]
            if not cancelled_profiles:
                return self.catalog_interaction_tick(interaction)
            self._logger.event(
                "agent_catalog_operation_timed_out",
                **self._hooks.access_point_fields(interaction.access_point),
                scope=interaction.scope,
                timeout_sec=self._AGENT_CATALOG_TIMEOUT_SEC,
                pending_profiles=cancelled_profiles,
            )
            self._set_catalog_terminal(
                interaction,
                texts=(f"agents ({interaction.scope}) lookup timed out.",),
                outcome=LocalCommandOutcome(
                    command=interaction.raw_command,
                    result={
                        "ok": False,
                        "code": "lookup_timeout",
                        "scope": interaction.scope,
                    },
                ),
                kind=self._kinds.warning,
                reason="timeout",
            )
        return None

    def catalog_interaction_completed(
        self,
        interaction: AgentCatalogInteraction,
        outcome: str,
    ) -> None:
        if outcome != "completed":
            raise AssertionError(f"unexpected catalog interaction outcome: {outcome}")
        for operation in interaction.operations:
            if not operation.done:
                operation.cancel()
        command_outcome = interaction.outcome or LocalCommandOutcome(
            command=interaction.raw_command,
            result={"ok": False, "code": "cancelled", "scope": interaction.scope},
        )
        try:
            self._complete_local_command(
                access_point=interaction.access_point,
                outcome=command_outcome,
                user_delivery="failed" if interaction.delivery_failed else "sent",
            )
        finally:
            self._finish_local_command_delivery(interaction.access_point)
        self._logger.event(
            "agent_catalog_interaction_completed",
            **self._hooks.access_point_fields(interaction.access_point),
            access_point_owner_ref=catalog_access_point_owner_ref(
                interaction.access_point
            ),
            interaction_sequence=interaction.interaction_sequence,
            scope=interaction.scope,
            outcome=interaction.terminal_reason or "delivery_failed",
            user_delivery="failed" if interaction.delivery_failed else "sent",
        )

    def _resume_interaction_for(
        self,
        access_point: AccessPointKey,
    ) -> AgentResumeInteraction | None:
        return next(
            (
                item
                for item in reversed(self._interaction_queue.snapshot())
                if isinstance(item, AgentResumeInteraction)
                and item.access_point == access_point
                and not item.finished
            ),
            None,
        )

    @staticmethod
    def _resume_block_text(block_reason: str, agent_state: str) -> str:
        return {
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

    def _begin_resume_interaction(
        self,
        *,
        access_point: AccessPointKey,
        command: ResumeCommand,
        agent_state: str,
        raw_command: str,
    ) -> None:
        if self._resume_interaction_for(access_point) is not None:
            self._queue_local_command_reply(
                access_point=access_point,
                outcome=LocalCommandOutcome(
                    command=raw_command,
                    result={
                        "ok": False,
                        "code": "access_point_busy",
                        "state": agent_state,
                        "reason": "lifecycle_active",
                    },
                ),
                text=self._resume_block_text("lifecycle_active", agent_state),
                kind=self._kinds.warning,
            )
            return
        block_reason = self._access_point_work_snapshot(
            access_point,
            runtime_state=agent_state,
        ).resume_block_reason()
        interaction = AgentResumeInteraction(
            access_point=access_point,
            interaction_sequence=self._catalog_interaction_sequence(),
            raw_command=raw_command,
            scope=command.scope,
            selector=command.selector,
            initial_agent_state=agent_state,
            initial_block_reason=block_reason or "",
        )
        self._interaction_queue.append(interaction)
        self._begin_local_command_delivery(access_point)
        try:
            self._access_point_adapter.queue_text_reply_with_result(
                access_point=access_point,
                text=self._AGENT_CATALOG_STATUS_TEXT,
                source="steward",
                kind=self._kinds.session,
                on_sent=lambda message_ref: self._resume_initial_sent(
                    interaction,
                    message_ref,
                ),
                on_failed=lambda exc: self._resume_delivery_failed(
                    interaction,
                    exc,
                    phase="initial",
                ),
            )
        except Exception as exc:
            self._resume_delivery_failed(interaction, exc, phase="initial")

    def _resume_initial_sent(
        self,
        interaction: AgentResumeInteraction,
        message_ref: AccessPointMessageRef,
    ) -> None:
        if interaction.finished or interaction.message_ref is not None:
            return
        if message_ref.access_point != interaction.access_point:
            self._resume_delivery_failed(
                interaction,
                RuntimeError("resume status returned a reference for another access point"),
                phase="initial",
            )
            return
        interaction.message_ref = message_ref
        if interaction.final_texts:
            interaction.phase = "terminal"
            self._queue_resume_edit(
                interaction,
                text=interaction.final_texts[0],
                purpose="terminal",
                kind=interaction.kind,
            )
            return
        interaction.phase = "catalog"
        self._logger.event(
            "resume_status_sent",
            **self._hooks.access_point_fields(interaction.access_point),
            scope=interaction.scope,
        )

    def _resume_delivery_failed(
        self,
        interaction: AgentResumeInteraction,
        exc: Exception,
        *,
        phase: str,
    ) -> None:
        if interaction.finished:
            return
        interaction.delivery_failed = True
        interaction.finished = True
        for operation in interaction.operations:
            operation.cancel()
        interaction.outcome = interaction.outcome or LocalCommandOutcome(
            command=interaction.raw_command,
            result={"ok": False, "code": "delivery_failed", "scope": interaction.scope},
        )
        self._logger.event(
            "resume_delivery_failed",
            **self._hooks.access_point_fields(interaction.access_point),
            scope=interaction.scope,
            phase=phase,
            error_type=type(exc).__name__,
            http_code=extract_http_code(exc),
        )

    def _queue_resume_edit(
        self,
        interaction: AgentResumeInteraction,
        *,
        text: str,
        purpose: str,
        kind: Any,
    ) -> None:
        if interaction.finished or interaction.edit_pending:
            return
        if interaction.message_ref is None:
            return
        interaction.edit_pending = True
        interaction.edit_purpose = purpose
        interaction.edit_text = text
        try:
            self._access_point_adapter.queue_text_reply_edit(
                access_point=interaction.access_point,
                message_ref=interaction.message_ref,
                text=text,
                source="steward",
                kind=kind,
                on_sent=lambda: self._resume_edit_sent(
                    interaction,
                    purpose=purpose,
                    text=text,
                ),
                on_failed=lambda exc: self._resume_delivery_failed(
                    interaction,
                    exc,
                    phase=f"{purpose}_edit",
                ),
            )
        except Exception as exc:
            self._resume_delivery_failed(
                interaction,
                exc,
                phase=f"{purpose}_edit",
            )

    def _resume_edit_sent(
        self,
        interaction: AgentResumeInteraction,
        *,
        purpose: str,
        text: str,
    ) -> None:
        if interaction.finished:
            return
        if (
            not interaction.edit_pending
            or interaction.edit_purpose != purpose
            or interaction.edit_text != text
        ):
            return
        interaction.edit_pending = False
        interaction.edit_purpose = ""
        interaction.edit_text = ""
        if purpose == "starting":
            interaction.phase = (
                "terminal" if interaction.final_texts else "start_ready"
            )
            return
        if purpose != "terminal":
            raise AssertionError(f"unknown resume edit purpose: {purpose}")
        followups = interaction.final_texts[1:]
        interaction.pending_followups = len(followups)
        if not followups:
            interaction.finished = True
            return
        for index, followup in enumerate(followups):
            try:
                self._access_point_adapter.queue_text_reply(
                    access_point=interaction.access_point,
                    text=followup,
                    source="steward",
                    kind=interaction.kind,
                    on_sent=lambda item=interaction: self._resume_followup_delivered(item),
                    on_failed=lambda exc, item=interaction: self._resume_followup_failed(
                        item,
                        exc,
                    ),
                )
            except Exception as exc:
                interaction.pending_followups -= len(followups) - index
                interaction.delivery_failed = True
                self._logger.event(
                    "resume_delivery_failed",
                    **self._hooks.access_point_fields(interaction.access_point),
                    scope=interaction.scope,
                    phase="terminal_followup",
                    error_type=type(exc).__name__,
                    http_code=extract_http_code(exc),
                )
                if interaction.pending_followups == 0:
                    interaction.finished = True
                break

    def _resume_followup_delivered(self, interaction: AgentResumeInteraction) -> None:
        if interaction.finished or interaction.pending_followups <= 0:
            return
        interaction.pending_followups -= 1
        if interaction.pending_followups == 0:
            interaction.finished = True

    def _resume_followup_failed(
        self,
        interaction: AgentResumeInteraction,
        exc: Exception,
    ) -> None:
        if interaction.finished or interaction.pending_followups <= 0:
            return
        interaction.delivery_failed = True
        self._logger.event(
            "resume_delivery_failed",
            **self._hooks.access_point_fields(interaction.access_point),
            scope=interaction.scope,
            phase="terminal_followup",
            error_type=type(exc).__name__,
            http_code=extract_http_code(exc),
        )
        self._resume_followup_delivered(interaction)

    def _set_resume_terminal(
        self,
        interaction: AgentResumeInteraction,
        *,
        texts: tuple[str, ...],
        outcome: LocalCommandOutcome,
        kind: Any,
        reason: str,
    ) -> None:
        if interaction.final_texts or interaction.finished:
            return
        if not texts or any(not text for text in texts):
            raise ValueError("resume terminal requires non-empty text")
        interaction.final_texts = texts
        interaction.outcome = outcome
        interaction.kind = kind
        interaction.terminal_reason = reason
        interaction.phase = "terminal"
        if not interaction.edit_pending:
            self._queue_resume_edit(
                interaction,
                text=texts[0],
                purpose="terminal",
                kind=kind,
            )

    def _resume_resolution_terminal(
        self,
        interaction: AgentResumeInteraction,
        listing: AgentCatalogListing,
    ) -> bool:
        failures = list(listing.failures)
        failure_payloads = agent_catalog_failure_payloads(failures)
        records = build_agent_catalog(
            sessions=listing.entries,
            bindings=self._catalog_binding_snapshot(),
            target_cwd=interaction.target_cwd,
        )
        matches = resolve_agent_catalog_entries(
            records,
            interaction.selector,
            session_projector=self._profile_catalog.session_projector,
        )
        if not matches:
            replies = [
                f"resume target not found ({interaction.scope}): {interaction.selector}"
            ]
            self._merge_resume_catalog_warnings(replies, failures)
            self._set_resume_terminal(
                interaction,
                texts=tuple(replies),
                outcome=LocalCommandOutcome(
                    command=interaction.raw_command,
                    result={
                        "ok": False,
                        "code": "not_found",
                        "scope": interaction.scope,
                        "selector": interaction.selector,
                        "catalog_failures": failure_payloads,
                    },
                ),
                kind=self._kinds.warning,
                reason="not_found",
            )
            return True
        if len(matches) > 1:
            replies = format_agent_catalog_chunks(
                scope=interaction.scope,
                records=matches,
                message_char_limit=self._AGENTS_MESSAGE_CHAR_LIMIT,
                session_projector=self._profile_catalog.session_projector,
                reveal_named_session_ids=True,
            )
            replies[0] = (
                f"resume target is ambiguous ({interaction.scope}): "
                f"{interaction.selector}\n\n{replies[0]}"
            )
            replies[-1] += (
                f"\n\nUse /resume {interaction.scope} "
                "<provider[/profile]:session-id>."
            )
            self._merge_resume_catalog_warnings(replies, failures)
            self._set_resume_terminal(
                interaction,
                texts=tuple(replies),
                outcome=LocalCommandOutcome(
                    command=interaction.raw_command,
                    result={
                        "ok": False,
                        "code": "ambiguous",
                        "scope": interaction.scope,
                        "selector": interaction.selector,
                        "count": len(matches),
                        "catalog_failures": failure_payloads,
                    },
                ),
                kind=self._kinds.command,
                reason="ambiguous",
            )
            return True

        selected = matches[0]
        projected_session_id = self._profile_catalog.session_projector.namespaced_id(
            selected.session
        )
        other_access_points = [
            item for item in selected.access_points if item != interaction.access_point
        ]
        if other_access_points:
            replies = [
                "resume unavailable: selected session is already bound to another "
                "access point.\n\n"
                + format_agent_catalog_entry(
                    selected,
                    session_projector=self._profile_catalog.session_projector,
                )
            ]
            self._merge_resume_catalog_warnings(replies, failures)
            self._set_resume_terminal(
                interaction,
                texts=tuple(replies),
                outcome=LocalCommandOutcome(
                    command=interaction.raw_command,
                    result={
                        **_bound_elsewhere_payload(
                            projected_session_id=projected_session_id,
                            owners=other_access_points,
                        ),
                        "scope": interaction.scope,
                        "catalog_failures": failure_payloads,
                    },
                ),
                kind=self._kinds.command,
                reason="bound_elsewhere",
            )
            return True
        if not selected.cwd:
            replies = [
                "resume unavailable: "
                f"{selected.thread_name or projected_session_id} has no folder."
            ]
            self._merge_resume_catalog_warnings(replies, failures)
            self._set_resume_terminal(
                interaction,
                texts=tuple(replies),
                outcome=LocalCommandOutcome(
                    command=interaction.raw_command,
                    result={
                        "ok": False,
                        "code": "missing_folder",
                        "thread_id": projected_session_id,
                        "catalog_failures": failure_payloads,
                    },
                ),
                kind=self._kinds.warning,
                reason="missing_folder",
            )
            return True

        interaction.selected = selected
        interaction.catalog_failures = tuple(failures)
        interaction.projected_session_id = projected_session_id
        pending = LocalLifecycleOperation(
            access_point=interaction.access_point,
            operation="RESUME_AGENT",
            thread_name=selected.thread_name,
            runtime_thread_id=selected.session.native_id,
            projected_session_id=projected_session_id,
            provider=selected.provider.value,
            profile=selected.profile.value,
            adapter=selected.adapter,
            adapter_version=selected.adapter_version,
            cwd=selected.cwd,
        )
        interaction.phase = "starting_edit"
        self._queue_resume_edit(
            interaction,
            text=self._agent_startup_action_initial_text(pending),
            purpose="starting",
            kind=self._kinds.session,
        )
        return False

    def _start_resume_provider(self, interaction: AgentResumeInteraction) -> None:
        selected = interaction.selected
        if selected is None:
            raise AssertionError("resume startup requires a selected session")
        interaction.phase = "starting_provider"
        try:
            current_binding = self._agent_runtime.get_binding_info(
                interaction.access_point
            ) or {}
            spec = {
                "provider": selected.provider.value,
                "profile": selected.profile.value,
                "cwd": selected.cwd,
                "thread_id": selected.session.native_id,
                "thread_name": selected.thread_name,
                "mode": "proxy",
            }
            approval_target = str(current_binding.get("approval_target") or "").strip()
            if approval_target:
                spec["approval_target"] = approval_target
            start_result = self._agent_runtime.start_agent(
                interaction.access_point,
                spec,
            )
            interaction.agent_id = str(start_result.get("agent_id") or "").strip()
            interaction.phase = "startup"
            self._persist_registry("resume_command_starting", interaction.access_point)
            self._reset_status_runtimes(
                interaction.access_point,
                False,
                reason="resume_command",
            )
            self._logger.event(
                "resume_command_started",
                **self._hooks.access_point_fields(interaction.access_point),
                scope=interaction.scope,
                selector=interaction.selector,
                provider_session_ref=interaction.projected_session_id,
                thread_name=selected.thread_name,
                cwd=selected.cwd,
            )
        except ProviderSessionBoundElsewhereError as exc:
            self._logger.event(
                "resume_command_failed",
                **self._hooks.access_point_fields(interaction.access_point),
                scope=interaction.scope,
                selector=interaction.selector,
                provider_session_ref=interaction.projected_session_id,
                error_type=type(exc).__name__,
                reason=exc.code,
                owner_count=len(exc.owners),
            )
            replies = [
                "resume unavailable: selected session is already bound to another "
                "access point.\n\n"
                + "\n".join(
                    f"bound to: {format_access_point(owner)}" for owner in exc.owners
                )
            ]
            self._merge_resume_catalog_warnings(
                replies,
                list(interaction.catalog_failures),
            )
            self._set_resume_terminal(
                interaction,
                texts=tuple(replies),
                outcome=LocalCommandOutcome(
                    command=interaction.raw_command,
                    result={
                        **_bound_elsewhere_payload(
                            projected_session_id=interaction.projected_session_id,
                            owners=exc.owners,
                        ),
                        "scope": interaction.scope,
                        "catalog_failures": agent_catalog_failure_payloads(
                            list(interaction.catalog_failures)
                        ),
                    },
                ),
                kind=self._kinds.command,
                reason=exc.code,
            )
        except Exception as exc:
            self._logger.event(
                "resume_command_failed",
                **self._hooks.access_point_fields(interaction.access_point),
                scope=interaction.scope,
                selector=interaction.selector,
                provider_session_ref=interaction.projected_session_id,
                error_type=type(exc).__name__,
            )
            self._receive_resume_startup_result(
                interaction,
                self._agent_runtime.startup_failure_result(
                    interaction.access_point,
                    error=str(exc),
                    default_cwd=selected.cwd,
                ),
            )

    def _remember_terminal_resume_agent_id(self, agent_id: str) -> None:
        normalized = str(agent_id or "").strip()
        if not normalized:
            return
        if normalized in self._terminal_resume_agent_ids:
            self._terminal_resume_agent_ids.remove(normalized)
        self._terminal_resume_agent_ids.append(normalized)
        del self._terminal_resume_agent_ids[:-64]

    def _receive_resume_startup_result(
        self,
        interaction: AgentResumeInteraction,
        result: InteractiveStartupResult,
    ) -> None:
        if interaction.finished or interaction.final_texts:
            self._remember_terminal_resume_agent_id(interaction.agent_id)
            return
        if not self._resume_has_provider_owner(interaction):
            return
        self._remember_terminal_resume_agent_id(interaction.agent_id)
        interaction.agent_id = interaction.agent_id or result.agent_id
        persisted = self._persist_registry("resume_command", interaction.access_point)
        if persisted:
            self._agent_runtime.acknowledge_metadata_change(interaction.access_point)
        if result.result != "completed":
            self._reset_status_runtimes(
                interaction.access_point,
                False,
                reason="startup_failure",
            )
            self._pending_startup_notice_context[interaction.access_point] = (
                f"Runtime startup failed: {result.error or 'startup failed'}. "
                "State: BOUND_IDLE."
            )
        interaction.projected_session_id = (
            interaction.projected_session_id
            or self._project_session_id(
                provider=result.provider,
                profile=result.profile,
                native_id=result.thread_id,
            )
        )
        failures = list(interaction.catalog_failures)
        text = self._agent_startup_action_result_text(
            result,
            operation="RESUME_AGENT",
            thread_name=(
                interaction.selected.thread_name
                if interaction.selected is not None
                else result.thread_name
            ),
            projected_session_id=interaction.projected_session_id,
        )
        replies = self._merge_resume_catalog_warnings([text], failures)
        if result.result == "completed":
            command_result = self._persisted_mutation_result(
                code="resumed",
                persisted=persisted,
                state=result.state,
                thread_id=interaction.projected_session_id,
                catalog_failures=agent_catalog_failure_payloads(failures),
            )
            reason = "resumed"
            kind = self._kinds.session
        else:
            command_result = {
                "ok": False,
                "code": "startup_failed",
                "state": result.state,
                "persisted": persisted,
                "thread_id": interaction.projected_session_id,
                "catalog_failures": agent_catalog_failure_payloads(failures),
            }
            reason = "startup_failed"
            kind = self._kinds.session
        self._set_resume_terminal(
            interaction,
            texts=tuple(replies),
            outcome=LocalCommandOutcome(
                command=interaction.raw_command,
                result=command_result,
            ),
            kind=kind,
            reason=reason,
        )

    def resume_interaction_tick(
        self,
        interaction: AgentResumeInteraction,
    ) -> str | None:
        if interaction.finished:
            return "completed"
        if interaction.phase == "terminal":
            if not interaction.edit_pending and interaction.final_texts:
                self._queue_resume_edit(
                    interaction,
                    text=interaction.final_texts[0],
                    purpose="terminal",
                    kind=interaction.kind,
                )
            return None
        if interaction.phase == "awaiting_status":
            return None
        if interaction.phase == "catalog" and not interaction.operations_started:
            if interaction.initial_block_reason:
                self._set_resume_terminal(
                    interaction,
                    texts=(
                        self._resume_block_text(
                            interaction.initial_block_reason,
                            interaction.initial_agent_state,
                        ),
                    ),
                    outcome=LocalCommandOutcome(
                        command=interaction.raw_command,
                        result={
                            "ok": False,
                            "code": "access_point_busy",
                            "state": interaction.initial_agent_state,
                            "reason": interaction.initial_block_reason,
                        },
                    ),
                    kind=self._kinds.warning,
                    reason="access_point_busy",
                )
                return None
            if interaction.scope == "here":
                interaction.target_cwd = self._current_project_cwd(
                    interaction.access_point
                )
                if not interaction.target_cwd:
                    self._set_resume_terminal(
                        interaction,
                        texts=(
                            "resume here unavailable: current access point has no folder. "
                            "Use /resume all <name|provider[/profile]:session-id>.",
                        ),
                        outcome=LocalCommandOutcome(
                            command=interaction.raw_command,
                            result={
                                "ok": False,
                                "code": "no_current_folder",
                                "scope": interaction.scope,
                            },
                        ),
                        kind=self._kinds.warning,
                        reason="no_current_folder",
                    )
                    return None
            try:
                _operation_profiles, operations, _busy_profiles = (
                    self._submit_profile_catalog_operations(
                        interaction_kind=CatalogInteractionKind.RESUME,
                        access_point=interaction.access_point,
                        interaction_sequence=interaction.interaction_sequence,
                        cwd=interaction.target_cwd,
                        limit=None,
                        include_installation=True,
                    )
                )
            except (CatalogOperationCapacityError, RuntimeError) as exc:
                self._logger.event(
                    "resume_catalog_operation_rejected",
                    **self._hooks.access_point_fields(interaction.access_point),
                    error_type=type(exc).__name__,
                )
                self._set_resume_terminal(
                    interaction,
                    texts=("resume session lookup failed.",),
                    outcome=LocalCommandOutcome(
                        command=interaction.raw_command,
                        result={"ok": False, "code": "lookup_failed"},
                    ),
                    kind=self._kinds.warning,
                    reason="lookup_failed",
                )
                return None
            interaction.operations = operations
            interaction.operations_started = True
            interaction.operation_started_at = time.monotonic()
            return None

        if interaction.phase == "catalog":
            if all(operation.done for operation in interaction.operations):
                try:
                    rows = []
                    for operation in interaction.operations:
                        row = operation.take_result()
                        if row is None:
                            return None
                        rows.append(row)
                except Exception as exc:
                    self._logger.event(
                        "resume_catalog_operation_failed",
                        **self._hooks.access_point_fields(interaction.access_point),
                        error_type=type(exc).__name__,
                    )
                    self._set_resume_terminal(
                        interaction,
                        texts=("resume session lookup failed.",),
                        outcome=LocalCommandOutcome(
                            command=interaction.raw_command,
                            result={"ok": False, "code": "lookup_failed"},
                        ),
                        kind=self._kinds.warning,
                        reason="lookup_failed",
                    )
                    return None
                listing = self._agent_catalog_listing_from_profile_results(rows)
                self._resume_resolution_terminal(interaction, listing)
                return None
            assert interaction.operation_started_at is not None
            if (
                time.monotonic() - interaction.operation_started_at
                > self._AGENT_CATALOG_TIMEOUT_SEC
            ):
                cancelled = [
                    operation.cancel()
                    for operation in interaction.operations
                ]
                if not any(cancelled):
                    return self.resume_interaction_tick(interaction)
                self._set_resume_terminal(
                    interaction,
                    texts=("resume session lookup timed out.",),
                    outcome=LocalCommandOutcome(
                        command=interaction.raw_command,
                        result={"ok": False, "code": "lookup_timeout"},
                    ),
                    kind=self._kinds.warning,
                    reason="lookup_timeout",
                )
            return None

        if interaction.phase == "start_ready":
            self._start_resume_provider(interaction)
        return None

    def resume_interaction_completed(
        self,
        interaction: AgentResumeInteraction,
        outcome: str,
    ) -> None:
        if outcome != "completed":
            raise AssertionError(f"unexpected resume interaction outcome: {outcome}")
        if interaction.completion_recorded:
            return
        interaction.completion_recorded = True
        for operation in interaction.operations:
            if not operation.done:
                operation.cancel()
        command_outcome = interaction.outcome or LocalCommandOutcome(
            command=interaction.raw_command,
            result={"ok": False, "code": "cancelled", "scope": interaction.scope},
        )
        try:
            self._complete_local_command(
                access_point=interaction.access_point,
                outcome=command_outcome,
                user_delivery="failed" if interaction.delivery_failed else "sent",
            )
        finally:
            self._finish_local_command_delivery(interaction.access_point)
        self._logger.event(
            "resume_interaction_completed",
            **self._hooks.access_point_fields(interaction.access_point),
            access_point_owner_ref=catalog_access_point_owner_ref(
                interaction.access_point
            ),
            interaction_sequence=interaction.interaction_sequence,
            scope=interaction.scope,
            outcome=interaction.terminal_reason or "delivery_failed",
            user_delivery="failed" if interaction.delivery_failed else "sent",
        )

    def _cancel_resume_interaction(
        self,
        interaction: AgentResumeInteraction,
        *,
        reason: str,
        deliver: bool,
    ) -> None:
        if interaction.finished or interaction.final_texts:
            return
        for operation in interaction.operations:
            operation.cancel()
        self._settle_resume_provider_owner(
            interaction,
            persist_reason="resume_command_cancelled",
            reset_reason="resume_cancelled",
            cleanup_failure_event="resume_cancel_cleanup_failed",
        )
        outcome = LocalCommandOutcome(
            command=interaction.raw_command,
            result={"ok": False, "code": "cancelled", "reason": reason},
        )
        if deliver:
            self._set_resume_terminal(
                interaction,
                texts=("resume cancelled.",),
                outcome=outcome,
                kind=self._kinds.warning,
                reason=reason,
            )
            return
        interaction.outcome = outcome
        interaction.terminal_reason = reason
        interaction.delivery_failed = True
        interaction.finished = True
        interaction.completion_recorded = True
        try:
            self._complete_local_command(
                access_point=interaction.access_point,
                outcome=outcome,
                user_delivery="failed",
            )
        finally:
            self._finish_local_command_delivery(interaction.access_point)

    def _stop_resume_interaction(
        self,
        interaction: AgentResumeInteraction,
    ) -> None:
        if interaction.finished or interaction.final_texts:
            return
        for operation in interaction.operations:
            operation.cancel()
        stopped, cleanup_failed = self._settle_resume_provider_owner(
            interaction,
            persist_reason="stop_command",
            reset_reason="stop_command",
            cleanup_failure_event="resume_stop_cleanup_failed",
        )
        if cleanup_failed:
            state = self._agent_runtime.runtime_state(interaction.access_point)
            text = f"runtime agent stop cleanup failed. state: {state}."
            code = "stop_cleanup_failed"
        elif stopped:
            state = "BOUND_IDLE"
            text = "runtime agent stopped. state: BOUND_IDLE."
            code = "stopped"
        else:
            state = interaction.initial_agent_state
            text = f"resume cancelled. state: {state}."
            code = "resume_cancelled"
        self._set_resume_terminal(
            interaction,
            texts=(text,),
            outcome=LocalCommandOutcome(
                command="/stop",
                result={
                    "ok": not cleanup_failed,
                    "code": code,
                    "state": state,
                },
            ),
            kind=self._kinds.warning if cleanup_failed else self._kinds.command,
            reason=code,
        )

    @classmethod
    def _resume_has_provider_owner(
        cls,
        interaction: AgentResumeInteraction,
    ) -> bool:
        return interaction.phase in cls._RESUME_PROVIDER_OWNED_PHASES

    def _settle_resume_provider_owner(
        self,
        interaction: AgentResumeInteraction,
        *,
        persist_reason: str,
        reset_reason: str,
        cleanup_failure_event: str,
    ) -> tuple[bool, bool]:
        if not self._resume_has_provider_owner(interaction):
            return False, False
        self._remember_terminal_resume_agent_id(interaction.agent_id)
        try:
            stopped = self._agent_runtime.stop_agent(interaction.access_point)
        except Exception as exc:
            self._logger.event(
                cleanup_failure_event,
                **self._hooks.access_point_fields(interaction.access_point),
                error_type=type(exc).__name__,
            )
            return False, True
        if stopped:
            persisted = self._persist_registry(
                persist_reason,
                interaction.access_point,
            )
            if persisted:
                self._agent_runtime.acknowledge_metadata_change(
                    interaction.access_point
                )
            self._reset_status_runtimes(
                interaction.access_point,
                False,
                reason=reset_reason,
            )
        return stopped, False

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
            raw_command = str(inbound.text or "").strip()
            if command.usage_error:
                self._queue_local_command_reply(
                    access_point=access_point,
                    outcome=LocalCommandOutcome(
                        command=raw_command,
                        result={"ok": False, "code": "usage_error"},
                    ),
                    text=command.usage_error,
                    kind=self._kinds.warning,
                )
            else:
                self._begin_agents_catalog_interaction(
                    access_point=access_point,
                    command=command,
                    raw_command=raw_command,
                )
            return
        if fallback_cmd == "providers":
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            command = parse_providers_command(inbound.text)
            raw_command = str(inbound.text or "").strip()
            if command.usage_error:
                self._queue_local_command_reply(
                    access_point=access_point,
                    outcome=LocalCommandOutcome(
                        command=raw_command,
                        result={"ok": False, "code": "usage_error"},
                    ),
                    text=command.usage_error,
                    kind=self._kinds.warning,
                )
            else:
                listing = build_providers_listing(
                    profile_catalog=self._profile_catalog,
                    installation_status=self._provider_installation_status,
                )
                self._queue_local_command_reply(
                    access_point=access_point,
                    outcome=LocalCommandOutcome(
                        command=raw_command,
                        result=listing.result,
                    ),
                    text=listing.text,
                    kind=(
                        self._kinds.warning
                        if listing.result["warnings"]
                        else self._kinds.command
                    ),
                )
            return
        if fallback_cmd == "resume":
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            command = parse_resume_command(inbound.text)
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
            else:
                self._begin_resume_interaction(
                    access_point=access_point,
                    command=command,
                    agent_state=agent_state,
                    raw_command=str(inbound.text or "").strip(),
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
                self._queue_inspect(access_point, all_targets=True)
            else:
                self._queue_inspect(access_point, all_targets=False)
            return
        if fallback_cmd == "model":
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            command = parse_model_command(inbound.text)
            capabilities = self._agent_runtime.get_capability_snapshot(access_point)
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
                return
            required = (
                AgentCapability.LIST_MODELS
                if command.operation == "list"
                else AgentCapability.SET_MODEL
            )
            if capabilities is None or not capabilities.supports(required):
                self._queue_local_command_reply(
                    access_point=access_point,
                    outcome=LocalCommandOutcome(
                        command=str(inbound.text or "").strip(),
                        result={
                            "ok": False,
                            "code": "unsupported",
                            "capability": required.value,
                        },
                    ),
                    text=unsupported_capability_text(required),
                    kind=self._kinds.warning,
                )
                return
            if command.operation == "list":
                projection = self._agent_runtime.get_local_projection(access_point)
                view = configuration_choice_view(projection, kind="model")
                self._queue_local_command_reply(
                    access_point=access_point,
                    outcome=LocalCommandOutcome(
                        command=str(inbound.text or "").strip(),
                        result={
                            "ok": view.available,
                            "code": "shown" if view.available else "unavailable",
                            "current_model": view.current,
                            "models": list(view.values),
                        },
                    ),
                    text=format_configuration_choices(view),
                    kind=self._kinds.command if view.available else self._kinds.warning,
                )
                return
            if not is_running_bound:
                self._queue_local_command_reply(
                    access_point=access_point,
                    outcome=LocalCommandOutcome(
                        command=str(inbound.text or "").strip(),
                        result={"ok": False, "code": "not_running"},
                    ),
                    text="model change unavailable: agent session is not running",
                    kind=self._kinds.warning,
                )
                return
            if self._agent_runtime.has_active_turn(access_point):
                self._queue_local_command_reply(
                    access_point=access_point,
                    outcome=LocalCommandOutcome(
                        command=str(inbound.text or "").strip(),
                        result={"ok": False, "code": "busy"},
                    ),
                    text="model change unavailable: agent turn is active",
                    kind=self._kinds.warning,
                )
                return
            try:
                self._agent_runtime.submit_model_selection(access_point, command.model)
            except Exception as exc:
                self._queue_local_command_reply(
                    access_point=access_point,
                    outcome=LocalCommandOutcome(
                        command=str(inbound.text or "").strip(),
                        result={"ok": False, "code": "rejected"},
                    ),
                    text=f"model request failed: {str(exc).strip() or type(exc).__name__}",
                    kind=self._kinds.warning,
                )
                return
            self._pending_model_commands.add(access_point)
            return
        if fallback_cmd == "effort":
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            command = parse_effort_command(inbound.text)
            capabilities = self._agent_runtime.get_capability_snapshot(access_point)
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
                return
            required = (
                AgentCapability.READ_EFFORT
                if command.operation == "read"
                else AgentCapability.SET_EFFORT
            )
            if capabilities is None or not capabilities.supports(required):
                self._queue_local_command_reply(
                    access_point=access_point,
                    outcome=LocalCommandOutcome(
                        command=str(inbound.text or "").strip(),
                        result={
                            "ok": False,
                            "code": "unsupported",
                            "capability": required.value,
                        },
                    ),
                    text=unsupported_capability_text(required),
                    kind=self._kinds.warning,
                )
                return
            projection = self._agent_runtime.get_local_projection(access_point)
            view = configuration_choice_view(projection, kind="effort")
            if command.operation == "read":
                self._queue_local_command_reply(
                    access_point=access_point,
                    outcome=LocalCommandOutcome(
                        command=str(inbound.text or "").strip(),
                        result={
                            "ok": view.available,
                            "code": "shown" if view.available else "unavailable",
                            "current_effort": view.current,
                            "effort_values": list(view.values),
                        },
                    ),
                    text=format_configuration_choices(view),
                    kind=self._kinds.command if view.available else self._kinds.warning,
                )
                return
            if not is_running_bound:
                error = "effort change unavailable: agent session is not running"
            elif self._agent_runtime.has_active_turn(access_point):
                error = "effort change unavailable: agent turn is active"
            elif (
                capabilities.supports(AgentCapability.READ_EFFORT)
                and command.effort not in view.values
            ):
                error = "effort request failed: value is not offered by the provider"
            else:
                error = ""
            if error:
                self._queue_local_command_reply(
                    access_point=access_point,
                    outcome=LocalCommandOutcome(
                        command=str(inbound.text or "").strip(),
                        result={"ok": False, "code": "rejected"},
                    ),
                    text=error,
                    kind=self._kinds.warning,
                )
                return
            try:
                self._agent_runtime.submit_effort_selection(access_point, command.effort)
            except Exception as exc:
                self._queue_local_command_reply(
                    access_point=access_point,
                    outcome=LocalCommandOutcome(
                        command=str(inbound.text or "").strip(),
                        result={"ok": False, "code": "rejected"},
                    ),
                    text=f"effort request failed: {str(exc).strip() or type(exc).__name__}",
                    kind=self._kinds.warning,
                )
                return
            self._pending_effort_commands.add(access_point)
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
            pending_resume = self._resume_interaction_for(access_point)
            if pending_resume is not None and not pending_resume.final_texts:
                self._stop_resume_interaction(pending_resume)
                if self._hooks.on_fallback_command is not None:
                    self._hooks.on_fallback_command(
                        access_point,
                        fallback_cmd,
                        is_bound,
                    )
                return
            if is_bound:
                self._cancel_approval_on_agent_stop(access_point)
                self._logger.event(
                    "steward_stop_barrier_interactions_cancelled",
                    **self._hooks.access_point_fields(access_point),
                )
            if agent_state in {"STARTING", "RUNNING"}:
                stopped = self._agent_runtime.stop_agent(access_point)
                reply = "runtime agent stopped. state: BOUND_IDLE." if stopped else "runtime agent stop failed."
                persisted = not stopped or self._persist_registry("stop_command", access_point)
                if stopped and not persisted:
                    reply += " Registry persistence failed; this state may be lost on restart."
                if stopped:
                    self._cancel_pending_configuration_commands(
                        access_point,
                        reason="runtime stopped",
                    )
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
                    if stopped:
                        self._cancel_pending_configuration_commands(
                            access_point,
                            reason="runtime stopped",
                        )
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
                conversation = self._steward_conversation_for(access_point)
                if conversation is not None and conversation.cancel_waiting_startup():
                    self._logger.event(
                        "steward_startup_action_cancelled_by_stop",
                        **self._hooks.access_point_fields(access_point),
                    )
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
            folder_only_reset = bool(
                not self._agent_runtime.has_binding(access_point)
                and self._current_project_cwd(access_point)
            )
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
            if runtime_reset:
                self._cancel_pending_configuration_commands(
                    access_point,
                    reason="runtime reset",
                )
            if runtime_reset:
                self._cancel_approval_on_agent_stop(
                    access_point,
                    turn_error="runtime agent reset",
                )
            if steward_reset:
                self._terminate_session_turns(
                    access_point,
                    "steward",
                    "steward runtime reset",
                )
                conversation = self._steward_conversation_for(access_point)
                if conversation is not None:
                    conversation.finish()
            self._agent_metadata_persist_retry_at.pop(access_point, None)
            self._reset_status_runtimes(access_point, True, reason="reset_command")
            self._pending_startup_notice_context.pop(access_point, None)
            reset_any = runtime_reset or steward_reset or folder_only_reset
            if reset_any:
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
                "code": "reset" if reset_any else "nothing_to_reset",
                "state": "UNBOUND",
                "runtime_reset": runtime_reset,
                "steward_reset": steward_reset,
                "folder_reset": folder_only_reset,
            }
            if reset_any:
                self._pending_uploads_by_access_point.pop(access_point, None)
                reset_persisted = self._drop_persisted_state(access_point)
                command_result = self._persisted_mutation_result(
                    code="reset",
                    persisted=reset_persisted,
                    state="UNBOUND",
                    runtime_reset=runtime_reset,
                    steward_reset=steward_reset,
                    folder_reset=folder_only_reset,
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
                conversation = StewardConversation(
                    access_point=access_point, allow_steer=False,
                )
                self._interaction_queue.append(conversation)
                self._submit_steward_user_request(
                    access_point,
                    self._input_with_uploads(inbound, steward_once_payload),
                    owner=conversation,
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
                conversation = self._new_human_agent_conversation(access_point)
                self._interaction_queue.append(conversation)
                self._queue_agent_turn(
                    access_point=access_point,
                    agent_input=prompt,
                    owner=conversation,
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
        conversation = StewardConversation(
            access_point=access_point, allow_steer=is_bound,
        )
        self._interaction_queue.append(conversation)
        self._submit_steward_user_request(
            access_point,
            self._input_with_uploads(inbound),
            owner=conversation,
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
