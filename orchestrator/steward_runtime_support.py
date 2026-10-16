from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import StrEnum
import time
from typing import Any, Callable, Iterable, Protocol

from orchestrator.access_point_common import (
    AccessPointKey,
    AccessPointMessageRef,
    access_point_sort_key,
    format_access_point,
)
from orchestrator.agent_identity import (
    ApprovalRef,
    InspectionRef,
    ProviderExecutionProfile,
    ProviderKind,
    ProviderProfileId,
    ProviderSessionRef,
    RuntimeGeneration,
    TurnRef,
)
from orchestrator.runtime_profile_catalog import (
    BUILTIN_RUNTIME_PROFILE_CATALOG,
    DEFAULT_RUNTIME_LAUNCH_DEFAULTS,
    RuntimeLaunchDefaults,
    RuntimeProfileCatalog,
)
from orchestrator.agent_inspection import (
    AgentBackgroundSnapshot,
    AgentContextSnapshot,
    AgentRuntimeProjection,
    AgentSessionSnapshot,
    AgentWorkSnapshot,
    SnapshotAvailability,
    unavailable_projection,
)
from orchestrator.agent_capabilities import (
    AgentCapability,
    AgentCapabilitySnapshot,
    codex_capability_snapshot,
)
from orchestrator.agent_io import AgentInput, AgentInputOrigin
from orchestrator.agent_session import ApprovalDecisionResult, AgentEffectOutcome, AgentEffectResult
from orchestrator.agent_session import AgentApprovalRequest, AgentFormRequest, ElicitationAction
from orchestrator.interactive_driver_events import (
    ApprovalDeliveryReceipt,
    ElicitationDeliveryReceipt,
)
from orchestrator.approval_target import (
    HUMAN_APPROVAL_TARGET,
    approval_target_referrers,
    normalize_approval_target,
    validate_approval_target_assignment,
)
from orchestrator import clock
from orchestrator.binding_address import (
    ROUTING_MODE_SHAMAN,
    ResolvedRoutingIdentity,
    RoutingIdentityRegistry,
    normalize_address,
)
from orchestrator.interactive_driver_events import (
    AgentEffortSelection,
    AgentModelSelection,
    AgentDeliveryEvent,
    AgentDeliveryDriverPort,
    InteractiveApprovalPromptEvent,
    InteractiveAgentResult,
    InteractiveAgentRequest,
    InteractiveInterruptResult,
    InteractiveEffortApplyResult,
    InteractiveModelApplyResult,
    InteractiveRuntimeFailure,
    InteractiveStartupResult,
    RuntimeDeliveryDriverFactory,
    StewardDeliveryDriverFactory,
)
from orchestrator.local_command_journal import LocalCommandOutcome
from orchestrator.processes import (
    CleanupError,
    CleanupStep,
    LifecycleLogger,
    cleanup_owned_resources,
)
from orchestrator.interaction_queue import InteractionTickContext
from orchestrator.steward_state import (
    RUNTIME_INTENT_BOUND_IDLE,
    RUNTIME_INTENT_RUNNING,
    ConfiguredProviderFields,
    PersistedAgentBinding,
    PersistedAccessPointState,
    normalize_runtime_intent,
)


@dataclass
class StewardDriverEvent:
    access_point: AccessPointKey
    source: str
    event: AgentDeliveryEvent


class AgentRuntimeStartDecision(StrEnum):
    UNBOUND = "unbound"
    DEFERRED = "deferred"
    STARTING = "starting"
    RUNNING = "running"
    EXPLICITLY_STOPPED = "explicitly_stopped"


class ProviderSessionBoundElsewhereError(RuntimeError):
    code = "bound_elsewhere"

    def __init__(
        self,
        session_ref: ProviderSessionRef,
        owners: Iterable[AccessPointKey],
    ) -> None:
        if not isinstance(session_ref, ProviderSessionRef):
            raise ValueError("provider session conflict requires ProviderSessionRef")
        normalized_owners = tuple(sorted(set(owners), key=access_point_sort_key))
        if not normalized_owners:
            raise ValueError("provider session conflict requires an existing owner")
        self.session_ref = session_ref
        self.owners = normalized_owners
        super().__init__(
            "provider session is already bound to another access point: "
            + ", ".join(format_access_point(owner) for owner in normalized_owners)
        )


class ProviderSessionOwnershipGuard:
    """Atomically owns full provider-session identities for access points."""

    def __init__(self) -> None:
        self._owner_by_session: dict[ProviderSessionRef, AccessPointKey] = {}
        self._claims_by_owner: dict[AccessPointKey, frozenset[ProviderSessionRef]] = {}

    def replace_claims(
        self,
        owner: AccessPointKey,
        session_refs: Iterable[ProviderSessionRef],
    ) -> None:
        claims = frozenset(session_refs)
        if any(not isinstance(item, ProviderSessionRef) for item in claims):
            raise ValueError("provider session claims require ProviderSessionRef values")
        for session_ref in sorted(
            claims,
            key=lambda item: (
                item.provider.value,
                item.profile.value,
                item.native_id,
            ),
        ):
            existing = self._owner_by_session.get(session_ref)
            if existing is not None and existing != owner:
                raise ProviderSessionBoundElsewhereError(session_ref, (existing,))

        previous = self._claims_by_owner.get(owner, frozenset())
        for session_ref in previous - claims:
            if self._owner_by_session.get(session_ref) == owner:
                self._owner_by_session.pop(session_ref, None)
        for session_ref in claims:
            self._owner_by_session[session_ref] = owner
        if claims:
            self._claims_by_owner[owner] = claims
        else:
            self._claims_by_owner.pop(owner, None)

    def release(self, owner: AccessPointKey) -> None:
        self.replace_claims(owner, ())

    def claims(self, owner: AccessPointKey) -> tuple[ProviderSessionRef, ...]:
        return tuple(
            sorted(
                self._claims_by_owner.get(owner, frozenset()),
                key=lambda item: (
                    item.provider.value,
                    item.profile.value,
                    item.native_id,
                ),
            )
        )

    def owners(self, session_ref: ProviderSessionRef) -> tuple[AccessPointKey, ...]:
        owner = self._owner_by_session.get(session_ref)
        return () if owner is None else (owner,)


class InterruptNoticeRuntime(Protocol):
    def interrupt_queue_initial(self, notice: PendingInterruptRequest) -> int | None: ...
    def interrupt_supersede(self, notice: PendingInterruptRequest) -> bool: ...
    def interrupt_queue_edit(self, notice: PendingInterruptRequest, text: str) -> None: ...
    def interrupt_log_final_sent(self, notice: PendingInterruptRequest) -> None: ...
    def interrupt_log_failure(self, notice: PendingInterruptRequest, exc: Exception, *, phase: str) -> None: ...
    def interrupt_complete(self, notice: PendingInterruptRequest, *, user_delivery: str) -> None: ...


@dataclass
class PendingInterruptRequest:
    access_point: AccessPointKey
    source: str
    ack_message_ref: AccessPointMessageRef | None = None
    final_text: str | None = None
    outcome: LocalCommandOutcome | None = None
    delivery_failed: bool = False
    queue_token: int | None = None
    final_sent_as_initial: bool = False
    superseded_text: str | None = None
    edit_text: str | None = None
    orphan: bool = False
    finished: bool = False

    def begin(self, runtime: InterruptNoticeRuntime) -> None:
        self.queue_token = runtime.interrupt_queue_initial(self)

    def set_result(self, runtime: InterruptNoticeRuntime, *, text: str, outcome: LocalCommandOutcome) -> None:
        if self.finished:
            return
        self.final_text = text
        self.outcome = outcome
        self._advance(runtime)

    def suppress_reply(self, runtime: InterruptNoticeRuntime, *, text: str) -> None:
        if not self.finished and self.final_text is None:
            self.final_text = text
            self._advance(runtime)

    def notice_sent(self, runtime: InterruptNoticeRuntime, message_ref: AccessPointMessageRef) -> None:
        if self.finished:
            return
        self.ack_message_ref = message_ref
        self._advance(runtime)

    def notice_failed(self, runtime: InterruptNoticeRuntime, exc: Exception) -> None:
        if self.finished:
            return
        runtime.interrupt_log_failure(self, exc, phase="send")
        self.delivery_failed = True
        self._advance(runtime)

    def notice_edited(self, runtime: InterruptNoticeRuntime, text: str) -> None:
        if self.finished:
            return
        self.edit_text = None
        if text == self.final_text:
            self._complete(runtime, user_delivery="sent")
        else:
            self._advance(runtime)

    def edit_failed(self, runtime: InterruptNoticeRuntime, exc: Exception, text: str) -> None:
        if self.finished:
            return
        self.edit_text = None
        if text == self.final_text:
            runtime.interrupt_log_failure(self, exc, phase="edit")
            self._complete(runtime, user_delivery="failed")
        else:
            self._advance(runtime)

    def _advance(self, runtime: InterruptNoticeRuntime) -> None:
        if self.finished or self.final_text is None or self.outcome is None:
            return
        if self.delivery_failed:
            self._complete(runtime, user_delivery="failed")
            return
        if self.ack_message_ref is None:
            if isinstance(self.queue_token, int) and self.superseded_text != self.final_text:
                if runtime.interrupt_supersede(self):
                    self.final_sent_as_initial = True
                    self.superseded_text = self.final_text
            return
        if self.final_sent_as_initial and self.superseded_text == self.final_text:
            runtime.interrupt_log_final_sent(self)
            self._complete(runtime, user_delivery="sent")
            return
        if self.edit_text is None:
            self.edit_text = self.final_text
            runtime.interrupt_queue_edit(self, self.edit_text)

    def _complete(self, runtime: InterruptNoticeRuntime, *, user_delivery: str) -> None:
        if self.finished:
            return
        self.finished = True
        runtime.interrupt_complete(self, user_delivery=user_delivery)

    def tick(self, context: InteractionTickContext) -> str | None:
        return "completed" if self.finished else None

    def on_completed(self, context: InteractionTickContext, outcome: str) -> None:
        if outcome != "completed":
            raise AssertionError(f"unexpected interrupt notice outcome: {outcome}")


@dataclass
class _InteractiveStewardHandle:
    node_id: str
    driver: AgentDeliveryDriverPort
    generation: RuntimeGeneration = field(default_factory=RuntimeGeneration.new)
    turn_sequence: int = 0
    steward_prompt_sent: bool = False


class StewardDeliveryLane(Protocol):
    def next_turn_ref(self, access_point: AccessPointKey) -> TurnRef: ...
    def turn_wait_reason(self, access_point: AccessPointKey) -> str: ...
    def start_turn(
        self,
        access_point: AccessPointKey,
        agent_input: AgentInput,
        turn_ref: TurnRef,
        *,
        context_note: str | None = None,
    ) -> AgentEffectResult: ...
    def steer_wait_reason(
        self,
        access_point: AccessPointKey,
        expected_turn_ref: TurnRef,
    ) -> str: ...
    def try_steer_turn(
        self,
        access_point: AccessPointKey,
        agent_input: AgentInput,
        expected_turn_ref: TurnRef,
        *,
        context_note: str | None = None,
    ) -> AgentEffectResult: ...
    def poll_once(
        self,
        *,
        idle_poll_blocked_resources: frozenset[object] = frozenset(),
    ) -> list[StewardDriverEvent]: ...
    def consume_poll_progress(self) -> bool: ...
    def submit_approval_decision(
        self,
        access_point: AccessPointKey,
        decision: str,
        *,
        expected_request: AgentApprovalRequest,
    ) -> ApprovalDeliveryReceipt: ...
    def submit_elicitation_response(
        self,
        access_point: AccessPointKey,
        action: ElicitationAction,
        *,
        expected_request: AgentFormRequest,
        content: dict[str, object] | None = None,
    ) -> ElicitationDeliveryReceipt: ...
    def show_running(self, access_point: AccessPointKey) -> list[dict[str, Any]]: ...
    def has_active_turn(self, access_point: AccessPointKey) -> bool: ...
    def interrupt_active_turn(self, access_point: AccessPointKey) -> None: ...
    def submit_model_selection(
        self,
        access_point: AccessPointKey,
        model: str,
    ) -> None: ...
    def submit_effort_selection(
        self,
        access_point: AccessPointKey,
        effort: str,
    ) -> None: ...
    def get_capability_snapshot(self, access_point: AccessPointKey) -> AgentCapabilitySnapshot | None: ...
    def get_local_projection(self, access_point: AccessPointKey) -> AgentRuntimeProjection: ...
    def request_inspection(
        self,
        access_point: AccessPointKey,
        inspection_ref: InspectionRef,
        timeout_sec: float,
    ) -> AgentEffectResult: ...
    def reset(self, access_point: AccessPointKey) -> bool: ...
    def close(self) -> None: ...


class AgentDeliveryLane(StewardDeliveryLane, Protocol):
    def submit_approval_decision_now(
        self,
        access_point: AccessPointKey,
        decision: str,
        *,
        expected_request: AgentApprovalRequest,
    ) -> ApprovalDecisionResult: ...
    def start_agent(self, access_point: AccessPointKey, spec: dict[str, Any]) -> dict[str, Any]: ...
    def is_bound(self, access_point: AccessPointKey) -> bool: ...
    def has_binding(self, access_point: AccessPointKey) -> bool: ...
    def runtime_state(self, access_point: AccessPointKey) -> str: ...
    def start_decision(self, access_point: AccessPointKey) -> AgentRuntimeStartDecision: ...
    def get_binding_info(self, access_point: AccessPointKey) -> dict[str, str] | None: ...
    def assign_address(
        self,
        access_point: AccessPointKey,
        address: str,
        *,
        routing_mode: str = ROUTING_MODE_SHAMAN,
    ) -> dict[str, Any]: ...
    def show_address(
        self,
        access_point: AccessPointKey,
        *,
        routing_mode: str = ROUTING_MODE_SHAMAN,
    ) -> dict[str, str]: ...
    def rename_address(
        self,
        access_point: AccessPointKey,
        address: str,
        *,
        routing_mode: str = ROUTING_MODE_SHAMAN,
    ) -> dict[str, Any]: ...
    def remove_address(
        self,
        access_point: AccessPointKey,
        *,
        routing_mode: str = ROUTING_MODE_SHAMAN,
    ) -> dict[str, str]: ...
    def resolve_routing_identity(
        self,
        address: str,
    ) -> ResolvedRoutingIdentity[AccessPointKey] | None: ...
    def pending_metadata_changes(self) -> tuple[AccessPointKey, ...]: ...
    def acknowledge_metadata_change(self, access_point: AccessPointKey) -> None: ...
    def has_pending_startup(self) -> bool: ...
    def approval_target(self, access_point: AccessPointKey) -> str: ...
    def assign_approval_target(self, access_point: AccessPointKey, target: str) -> dict[str, str]: ...
    def show_approval_target(self, access_point: AccessPointKey) -> dict[str, str]: ...
    def stop_agent(self, access_point: AccessPointKey) -> bool: ...
    def start_bound_agent(self, access_point: AccessPointKey) -> dict[str, Any]: ...
    def restore_binding(
        self,
        access_point: AccessPointKey,
        *,
        spec: dict[str, Any],
        agent_id: str = "",
        thread_id: str = "",
        runtime_intent: str = RUNTIME_INTENT_BOUND_IDLE,
    ) -> dict[str, str]: ...
    def snapshot_persisted(self) -> dict[AccessPointKey, PersistedAccessPointState]: ...


class InteractiveStewardRuntime:
    def __init__(
        self,
        *,
        logger: LifecycleLogger,
        steward_prompt: str | None,
        driver_factory: StewardDeliveryDriverFactory,
    ) -> None:
        self._logger = logger
        self._steward_prompt = (steward_prompt or "").strip()
        self._driver_factory = driver_factory
        self._handles: dict[AccessPointKey, _InteractiveStewardHandle] = {}
        self._startup_cleanup_drivers: dict[str, AgentDeliveryDriverPort] = {}
        self._poll_progressed = False

    def ensure_started(self, access_point: AccessPointKey) -> None:
        self._ensure_handle(access_point)

    def steer_wait_reason(
        self,
        access_point: AccessPointKey,
        expected_turn_ref: TurnRef,
    ) -> str:
        handle = self._handles.get(access_point)
        if handle is None:
            return "steward_starting"
        return handle.driver.steer_wait_reason(expected_turn_ref)

    def try_steer_turn(
        self,
        access_point: AccessPointKey,
        agent_input: AgentInput,
        expected_turn_ref: TurnRef,
        *,
        context_note: str | None = None,
    ) -> AgentEffectResult:
        handle = self._ensure_handle(access_point)
        request, prepend_prompt = self._build_request(
            handle=handle,
            access_point=access_point,
            agent_input=agent_input,
            context_note=context_note,
        )
        receipt = handle.driver.try_steer_turn(request, expected_turn_ref)
        if receipt.outcome is AgentEffectOutcome.ACCEPTED and prepend_prompt:
            handle.steward_prompt_sent = True
        return receipt

    def start_turn(
        self,
        access_point: AccessPointKey,
        agent_input: AgentInput,
        turn_ref: TurnRef,
        *,
        context_note: str | None = None,
    ) -> AgentEffectResult:
        handle = self._ensure_handle(access_point)
        request, prepend_prompt = self._build_request(
            handle=handle,
            access_point=access_point,
            agent_input=agent_input,
            context_note=context_note,
        )
        receipt = handle.driver.start_turn(request, turn_ref)
        if receipt.outcome is AgentEffectOutcome.ACCEPTED and prepend_prompt:
            handle.steward_prompt_sent = True
        return receipt

    def next_turn_ref(self, access_point: AccessPointKey) -> TurnRef:
        handle = self._ensure_handle(access_point)
        handle.turn_sequence += 1
        return TurnRef(handle.generation, handle.turn_sequence)

    def turn_wait_reason(self, access_point: AccessPointKey) -> str:
        handle = self._handles.get(access_point)
        if handle is None:
            return "steward_starting"
        if not handle.driver.is_ready():
            return "steward_starting"
        if handle.driver.has_busy_work():
            return "steward_busy"
        return ""

    def _build_request(
        self,
        *,
        handle: _InteractiveStewardHandle,
        access_point: AccessPointKey,
        agent_input: AgentInput,
        context_note: str | None,
    ) -> tuple[InteractiveAgentRequest, bool]:
        request_text = agent_input.text
        prepend_prompt = bool(self._steward_prompt and not handle.steward_prompt_sent)
        context_block = ""
        if context_note:
            context_block = f"Current control-plane context:\n{str(context_note).strip()}\n\n"
        if prepend_prompt:
            request_text = (
                "You are running under Steward control-plane contract.\n"
                "Follow these rules strictly:\n\n"
                f"{self._steward_prompt}\n\n"
                "---\n"
                f"{context_block}"
                "Human message:\n"
                f"{request_text}"
            )
        elif context_block:
            request_text = f"{context_block}Human message:\n{request_text}"
        self._logger.event(
            "steward_runtime_submit_request",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            node_id=handle.node_id,
            prepend_prompt=prepend_prompt,
            context_note=bool(context_note),
            text_preview=agent_input.text.strip()[:200],
        )
        return (
            InteractiveAgentRequest(
                chat_id=access_point.chat_id,
                thread_id=access_point.thread_id,
                agent_input=agent_input.with_text(request_text),
            ),
            prepend_prompt,
        )

    def poll_once(
        self,
        *,
        idle_poll_blocked_resources: frozenset[object] = frozenset(),
    ) -> list[StewardDriverEvent]:
        self._poll_progressed = False
        out: list[StewardDriverEvent] = []
        for access_point, handle in list(self._handles.items()):
            if (
                ("steward", access_point) in idle_poll_blocked_resources
                and handle.driver.is_ready()
                and not handle.driver.has_active_turn()
            ):
                continue
            events = handle.driver.poll_once()
            handle.driver.acknowledge_event_handoff()
            if handle.driver.consume_poll_progress():
                self._poll_progressed = True
            for event in events:
                self._logger.event(
                    "steward_runtime_driver_event",
                    access_point_type=access_point.type,
                    chat_id=access_point.chat_id,
                    thread_id=access_point.thread_id,
                    node_id=handle.node_id,
                    event_type=type(event).__name__,
                )
                out.append(StewardDriverEvent(access_point=access_point, source="steward", event=event))
        return out

    def consume_poll_progress(self) -> bool:
        progressed = self._poll_progressed
        self._poll_progressed = False
        return progressed

    def show_running(self, access_point: AccessPointKey) -> list[dict[str, Any]]:
        handle = self._handles.get(access_point)
        if handle is None:
            return []
        state = "RUNNING" if handle.driver.is_ready() else "STARTING"
        session = handle.driver.get_local_projection().session
        return [
            {
                "role": "steward",
                "agent_id": handle.node_id,
                "state": state,
                "controllable": False,
                "provider": ProviderKind.CODEX.value,
                "configured_model": "",
                "configured_backend": "",
                "effective_model": session.model,
                "effective_backend": session.backend,
            }
        ]

    def has_active_turn(self, access_point: AccessPointKey) -> bool:
        handle = self._handles.get(access_point)
        if handle is None:
            return False
        return bool(handle.driver.has_active_turn())

    def interrupt_active_turn(self, access_point: AccessPointKey) -> None:
        handle = self._handles.get(access_point)
        if handle is None:
            raise RuntimeError("no running steward node for this access point")
        handle.driver.interrupt_active_turn()

    def get_capability_snapshot(
        self,
        access_point: AccessPointKey,
    ) -> AgentCapabilitySnapshot | None:
        handle = self._handles.get(access_point)
        if handle is None:
            return None
        return handle.driver.get_capability_snapshot()

    def get_local_projection(self, access_point: AccessPointKey) -> AgentRuntimeProjection:
        handle = self._handles.get(access_point)
        if handle is None:
            return unavailable_projection()
        return handle.driver.get_local_projection()

    def request_inspection(
        self,
        access_point: AccessPointKey,
        inspection_ref: InspectionRef,
        timeout_sec: float,
    ) -> AgentEffectResult:
        handle = self._handles.get(access_point)
        if handle is None or not handle.driver.is_ready():
            return AgentEffectResult.rejected("steward session is not ready")
        return handle.driver.request_inspection(
            inspection_ref,
            timeout_sec,
            (access_point.chat_id, access_point.thread_id),
        )

    def submit_approval_decision(
        self,
        access_point: AccessPointKey,
        decision: str,
        *,
        expected_request: AgentApprovalRequest,
    ) -> ApprovalDeliveryReceipt:
        handle = self._handles.get(access_point)
        if handle is None:
            raise RuntimeError("no running steward node for this access point")
        self._logger.event(
            "steward_runtime_submit_approval_decision",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            node_id=handle.node_id,
            decision=decision,
        )
        return handle.driver.submit_approval_decision(
            decision,
            expected_request=expected_request,
        )

    def submit_elicitation_response(
        self,
        access_point: AccessPointKey,
        action: ElicitationAction,
        *,
        expected_request: AgentFormRequest,
        content: dict[str, object] | None = None,
    ) -> ElicitationDeliveryReceipt:
        handle = self._handles.get(access_point)
        if handle is None:
            raise RuntimeError("no running steward node for this access point")
        return handle.driver.submit_elicitation_response(
            action,
            expected_request=expected_request,
            content=content,
        )

    def reset(self, access_point: AccessPointKey) -> bool:
        handle = self._handles.get(access_point)
        node_id = self._node_id(access_point)
        pending = self._startup_cleanup_drivers.get(node_id)
        if handle is None and pending is None:
            return False
        steps: list[CleanupStep] = []
        if handle is not None:
            steps.append(self._handle_cleanup_step(access_point, handle))
        if pending is not None:
            steps.append(self._startup_cleanup_step(node_id, pending))
        cleanup_owned_resources(steps)
        return True

    def close(self) -> None:
        steps = [
            self._handle_cleanup_step(access_point, handle)
            for access_point, handle in list(self._handles.items())
        ]
        steps.extend(
            self._startup_cleanup_step(node_id, driver)
            for node_id, driver in list(self._startup_cleanup_drivers.items())
        )
        cleanup_owned_resources(steps)

    def _ensure_handle(self, access_point: AccessPointKey) -> _InteractiveStewardHandle:
        existing = self._handles.get(access_point)
        if existing is not None:
            return existing
        node_id = self._node_id(access_point)
        pending = self._startup_cleanup_drivers.get(node_id)
        if pending is not None:
            cleanup_owned_resources([self._startup_cleanup_step(node_id, pending)])
        driver = self._driver_factory(access_point, self._logger)
        self._startup_cleanup_drivers[node_id] = driver
        try:
            driver.start()
        except Exception as startup_error:
            try:
                cleanup_owned_resources([self._startup_cleanup_step(node_id, driver)])
            except CleanupError as cleanup_error:
                raise RuntimeError(
                    f"steward driver startup failed: {startup_error}; "
                    f"rollback failed: {cleanup_error}"
                ) from startup_error
            raise
        handle = _InteractiveStewardHandle(node_id=node_id, driver=driver)
        self._handles[access_point] = handle
        self._startup_cleanup_drivers.pop(node_id, None)
        self._logger.event(
            "access_point_node_spawned",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            node_id=node_id,
        )
        return handle

    @staticmethod
    def _node_id(access_point: AccessPointKey) -> str:
        return (
            f"ap-{access_point.type}-"
            f"{access_point.chat_id}-"
            f"{access_point.thread_id if access_point.thread_id is not None else 'main'}"
        )

    def _handle_cleanup_step(
        self,
        access_point: AccessPointKey,
        handle: _InteractiveStewardHandle,
    ) -> CleanupStep:
        def release() -> None:
            if self._handles.get(access_point) is handle:
                self._handles.pop(access_point, None)

        return CleanupStep(
            owner=f"steward driver {handle.node_id}",
            cleanup=handle.driver.stop,
            release=release,
        )

    def _startup_cleanup_step(
        self,
        node_id: str,
        driver: AgentDeliveryDriverPort,
    ) -> CleanupStep:
        def release() -> None:
            if self._startup_cleanup_drivers.get(node_id) is driver:
                self._startup_cleanup_drivers.pop(node_id, None)

        return CleanupStep(
            owner=f"steward startup driver {node_id}",
            cleanup=driver.stop,
            release=release,
        )


@dataclass
class _EffectiveAgentRuntime:
    generation: RuntimeGeneration
    session: ProviderSessionRef | None = None
    model: str = ""
    backend: str = ""
    turn_ref: TurnRef | None = None
    approval_ref: ApprovalRef | None = None


@dataclass
class _InteractiveAgentBinding:
    node_id: str
    provider: ProviderKind
    profile: ProviderProfileId
    configured: ConfiguredProviderFields
    session: ProviderSessionRef | None
    requested_session: ProviderSessionRef | None
    driver: AgentDeliveryDriverPort | None
    adapter_name: str = ""
    adapter_version: str = ""
    runtime_name: str = ""
    runtime_version: str = ""
    runtime_intent: str = RUNTIME_INTENT_BOUND_IDLE
    session_name: str = ""
    approval_target: str = HUMAN_APPROVAL_TARGET
    effective: _EffectiveAgentRuntime | None = None
    turn_sequence: int = 0
    startup_logged: bool = False
    state: str = "BOUND_IDLE"
    rollback_binding: _InteractiveAgentBinding | None = None

    @property
    def cwd(self) -> str:
        return self.configured.cwd

    @property
    def model(self) -> str:
        return self.configured.model

    @property
    def backend(self) -> str:
        return self.configured.backend

    @property
    def mode(self) -> str:
        return self.configured.mode

    @property
    def thread_id(self) -> str:
        return self.session.native_id if self.session is not None else ""

    @property
    def requested_thread_id(self) -> str:
        return self.requested_session.native_id if self.requested_session is not None else ""

    @property
    def thread_name(self) -> str:
        return self.session_name

    @property
    def effective_model(self) -> str:
        return self.effective.model if self.effective is not None else ""

    @property
    def effective_backend(self) -> str:
        return self.effective.backend if self.effective is not None else ""

    @property
    def runtime_generation(self) -> RuntimeGeneration | None:
        return self.effective.generation if self.effective is not None else None


@dataclass(frozen=True)
class _PendingAgentDriverCleanup:
    access_point: AccessPointKey
    driver: AgentDeliveryDriverPort
    session_claims: frozenset[ProviderSessionRef]


def _persisted_binding(binding: _InteractiveAgentBinding) -> PersistedAgentBinding:
    return PersistedAgentBinding(
        agent_id=binding.node_id,
        provider=binding.provider,
        profile=binding.profile,
        session=binding.session,
        session_name=binding.thread_name,
        configured=binding.configured,
        adapter_name=binding.adapter_name,
        adapter_version=binding.adapter_version,
        runtime_name=binding.runtime_name,
        runtime_version=binding.runtime_version,
    )


def _binding_component_fields(binding: _InteractiveAgentBinding) -> dict[str, str]:
    return {
        "adapter": binding.adapter_name or "unknown",
        "adapter_version": binding.adapter_version or "unknown",
        "runtime": binding.runtime_name or "unknown",
        "runtime_version": binding.runtime_version or "unknown",
    }


class InteractiveAgentRuntime:
    _DEFAULT_STARTUP_TIMEOUT_SEC = 120.0

    def __init__(
        self,
        *,
        logger: LifecycleLogger,
        driver_factory: RuntimeDeliveryDriverFactory,
        provider_validator: Callable[[ProviderExecutionProfile], object],
        startup_timeout_sec: float = _DEFAULT_STARTUP_TIMEOUT_SEC,
        profile_catalog: RuntimeProfileCatalog = BUILTIN_RUNTIME_PROFILE_CATALOG,
        launch_defaults: RuntimeLaunchDefaults = DEFAULT_RUNTIME_LAUNCH_DEFAULTS,
    ) -> None:
        self._logger = logger
        self._driver_factory = driver_factory
        self._provider_validator = provider_validator
        if not isinstance(profile_catalog, RuntimeProfileCatalog):
            raise ValueError("agent runtime requires RuntimeProfileCatalog")
        if not isinstance(launch_defaults, RuntimeLaunchDefaults):
            raise ValueError("agent runtime requires RuntimeLaunchDefaults")
        self._profile_catalog = profile_catalog
        self._launch_defaults = launch_defaults
        self._startup_timeout_sec = float(startup_timeout_sec)
        if self._startup_timeout_sec <= 0.0:
            raise ValueError("startup_timeout_sec must be positive")
        self._id_seq = 0
        self._binding_by_access_point: dict[AccessPointKey, _InteractiveAgentBinding] = {}
        self._session_ownership = ProviderSessionOwnershipGuard()
        self._restore_conflict_by_access_point: dict[
            AccessPointKey,
            ProviderSessionBoundElsewhereError,
        ] = {}
        self._pending_driver_cleanup: dict[
            str,
            _PendingAgentDriverCleanup,
        ] = {}
        self._address_registry = RoutingIdentityRegistry[AccessPointKey]()
        self._metadata_changes: set[AccessPointKey] = set()
        self._poll_progressed = False

    @classmethod
    def _binding_session_claims(
        cls,
        binding: _InteractiveAgentBinding | None,
    ) -> tuple[ProviderSessionRef, ...]:
        if binding is None:
            return ()
        claims = {binding.session} if binding.session is not None else set()
        if binding.state == "STARTING":
            if binding.requested_session is not None:
                claims.add(binding.requested_session)
            claims.update(cls._binding_session_claims(binding.rollback_binding))
        return tuple(claims)

    def _replace_binding(
        self,
        access_point: AccessPointKey,
        binding: _InteractiveAgentBinding,
    ) -> None:
        self._sync_session_claims(access_point, binding=binding)
        self._binding_by_access_point[access_point] = binding
        self._restore_conflict_by_access_point.pop(access_point, None)

    def _remove_binding(self, access_point: AccessPointKey) -> None:
        self._binding_by_access_point.pop(access_point, None)
        self._sync_session_claims(access_point, binding=None)
        self._metadata_changes.discard(access_point)

    def _session_claims_with_pending_cleanup(
        self,
        access_point: AccessPointKey,
        *,
        binding: _InteractiveAgentBinding | None,
    ) -> tuple[ProviderSessionRef, ...]:
        claims = set(self._binding_session_claims(binding))
        for pending in self._pending_driver_cleanup.values():
            if pending.access_point == access_point:
                claims.update(pending.session_claims)
        return tuple(claims)

    def _sync_session_claims(
        self,
        access_point: AccessPointKey,
        *,
        binding: _InteractiveAgentBinding | None = None,
    ) -> None:
        if binding is None:
            binding = self._binding_by_access_point.get(access_point)
        self._session_ownership.replace_claims(
            access_point,
            self._session_claims_with_pending_cleanup(
                access_point,
                binding=binding,
            ),
        )

    def _retain_pending_driver_cleanup(
        self,
        *,
        access_point: AccessPointKey,
        binding: _InteractiveAgentBinding,
        driver: AgentDeliveryDriverPort,
    ) -> None:
        self._pending_driver_cleanup[binding.node_id] = _PendingAgentDriverCleanup(
            access_point=access_point,
            driver=driver,
            session_claims=frozenset(self._binding_session_claims(binding)),
        )

    def _normalize_spec(self, spec: dict[str, Any]) -> dict[str, Any]:
        resolved_spec = dict(spec)
        resolved_spec["cwd"] = str(resolved_spec.get("cwd") or "")
        resolved_spec["model"] = str(resolved_spec.get("model") or "")
        resolved_spec["backend"] = str(resolved_spec.get("backend") or "")
        resolved_spec["mode"] = str(resolved_spec.get("mode") or "proxy")
        execution_profile = self._profile_catalog.resolve(
            resolved_spec.get("provider") or ProviderKind.CODEX,
            resolved_spec.get("profile"),
        )
        resolved_spec["provider"] = execution_profile.provider.value
        resolved_spec["profile"] = execution_profile.profile.value
        launch = self._profile_catalog.normalize_launch_configuration(
            execution_profile,
            args=resolved_spec.get("args"),
            provider_approval_policy=resolved_spec.get("approval_policy"),
            sandbox=resolved_spec.get("sandbox"),
            defaults=self._launch_defaults,
        )
        resolved_spec["args"] = list(launch.args)
        resolved_spec["approval_policy"] = launch.provider_approval_policy
        resolved_spec["sandbox"] = launch.sandbox
        resolved_spec["approval_target"] = normalize_approval_target(
            resolved_spec.get("approval_target") or HUMAN_APPROVAL_TARGET
        )
        if "thread_id" in resolved_spec and resolved_spec.get("thread_id") is None:
            resolved_spec.pop("thread_id", None)
        return resolved_spec

    def start_agent(self, access_point: AccessPointKey, spec: dict[str, Any]) -> dict[str, Any]:
        resolved_spec = self._normalize_spec(spec)
        execution_profile = self._profile_catalog.resolve(
            resolved_spec["provider"],
            resolved_spec["profile"],
        )
        provider = execution_profile.provider
        profile = execution_profile.profile
        restore_conflict = self._restore_conflict_by_access_point.get(access_point)
        if restore_conflict is not None:
            raise restore_conflict
        existing = self._binding_by_access_point.get(access_point)
        if existing is not None and existing.driver is not None:
            raise RuntimeError("agent is already running for this access point")
        native_session_id = str(resolved_spec.get("thread_id") or "").strip()
        session = (
            ProviderSessionRef(provider, native_session_id, profile)
            if native_session_id
            else None
        )
        startup_claims = set(self._binding_session_claims(existing))
        if session is not None:
            startup_claims.add(session)
        reservation_claims = set(
            self._session_claims_with_pending_cleanup(
                access_point,
                binding=existing,
            )
        )
        reservation_claims.update(startup_claims)
        self._session_ownership.replace_claims(access_point, reservation_claims)
        try:
            self._provider_validator(execution_profile)
            self._cleanup_pending_drivers(access_point=access_point)
            self._session_ownership.replace_claims(access_point, startup_claims)
            driver = self._build_driver(access_point=access_point, spec=resolved_spec)
            capabilities = driver.get_capability_snapshot()
        except Exception:
            self._sync_session_claims(access_point)
            raise
        self._id_seq += 1
        node_id = (
            f"agent-{access_point.type}-"
            f"{access_point.chat_id}-"
            f"{access_point.thread_id if access_point.thread_id is not None else 'main'}-"
            f"{self._id_seq}"
        )
        binding = _InteractiveAgentBinding(
            node_id=node_id,
            provider=provider,
            profile=profile,
            configured=ConfiguredProviderFields(
                cwd=str(resolved_spec.get("cwd") or ""),
                model=str(resolved_spec.get("model") or ""),
                backend=str(resolved_spec.get("backend") or ""),
                mode=str(resolved_spec.get("mode") or "proxy"),
                approval_policy=str(resolved_spec.get("approval_policy") or ""),
                sandbox=str(resolved_spec.get("sandbox") or ""),
                args=tuple(
                    str(item)
                    for item in (
                        resolved_spec.get("args")
                        if isinstance(resolved_spec.get("args"), list)
                        else []
                    )
                ),
            ),
            session=session,
            requested_session=session,
            driver=driver,
            adapter_name=capabilities.adapter.name,
            adapter_version=capabilities.adapter.version,
            runtime_name=capabilities.runtime.name,
            runtime_version=capabilities.runtime.version,
            runtime_intent=RUNTIME_INTENT_RUNNING,
            session_name=str(resolved_spec.get("thread_name") or "").strip(),
            approval_target=str(resolved_spec.get("approval_target") or HUMAN_APPROVAL_TARGET),
            effective=_EffectiveAgentRuntime(generation=RuntimeGeneration.new()),
            state="STARTING",
            rollback_binding=existing,
        )
        self._replace_binding(access_point, binding)
        driver = binding.driver
        try:
            driver.start()
        except Exception as startup_error:
            self._retain_pending_driver_cleanup(
                access_point=access_point,
                binding=binding,
                driver=driver,
            )
            if existing is None:
                binding.driver = None
                binding.state = "BOUND_IDLE"
                binding.runtime_intent = RUNTIME_INTENT_BOUND_IDLE
                binding.effective = None
                binding.turn_sequence = 0
                self._replace_binding(access_point, binding)
            else:
                existing.runtime_intent = RUNTIME_INTENT_BOUND_IDLE
                self._replace_binding(access_point, existing)
            try:
                self._cleanup_pending_drivers(node_id=node_id)
            except CleanupError as cleanup_error:
                raise RuntimeError(
                    f"runtime agent startup failed: {startup_error}; "
                    f"rollback failed: {cleanup_error}"
                ) from startup_error
            raise
        self._logger.event(
            "access_point_agent_starting",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            node_id=node_id,
            cwd=binding.cwd,
            model=binding.model,
            mode=binding.mode,
            provider=binding.provider.value,
            profile=binding.profile.value,
            startup_timeout_sec=self._startup_timeout_sec,
            runtime_generation=(
                binding.runtime_generation.value if binding.runtime_generation is not None else ""
            ),
        )
        return self._binding_start_result(binding)

    @staticmethod
    def _binding_start_result(binding: _InteractiveAgentBinding) -> dict[str, Any]:
        return {
            "agent_id": binding.node_id,
            "state": binding.state.lower(),
            "cwd": binding.cwd,
            "model": binding.model,
            "provider": binding.provider.value,
            "profile": binding.profile.value,
            **_binding_component_fields(binding),
            "configured_backend": binding.backend,
            "effective_backend": binding.effective_backend,
            "mode": binding.mode,
            "thread_id": binding.thread_id,
            "thread_name": binding.thread_name,
        }

    def is_bound(self, access_point: AccessPointKey) -> bool:
        return access_point in self._binding_by_access_point

    def has_binding(self, access_point: AccessPointKey) -> bool:
        return access_point in self._binding_by_access_point

    def runtime_state(self, access_point: AccessPointKey) -> str:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            return "UNBOUND"
        return binding.state

    def start_decision(self, access_point: AccessPointKey) -> AgentRuntimeStartDecision:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            return AgentRuntimeStartDecision.UNBOUND
        if binding.state == "STARTING":
            return AgentRuntimeStartDecision.STARTING
        if binding.state == "RUNNING":
            return AgentRuntimeStartDecision.RUNNING
        if binding.runtime_intent == RUNTIME_INTENT_RUNNING:
            return AgentRuntimeStartDecision.DEFERRED
        return AgentRuntimeStartDecision.EXPLICITLY_STOPPED

    def get_binding_info(self, access_point: AccessPointKey) -> dict[str, str] | None:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            return None
        info = {
            "agent_id": binding.node_id,
            "provider": binding.provider.value,
            "profile": binding.profile.value,
            **_binding_component_fields(binding),
            "cwd": binding.cwd,
            "model": binding.model,
            "configured_model": binding.model,
            "effective_model": binding.effective_model,
            "configured_backend": binding.backend,
            "effective_backend": binding.effective_backend,
            "mode": binding.mode,
            "thread_id": binding.thread_id,
            "thread_name": binding.thread_name,
            "approval_target": binding.approval_target,
            "approval_policy": binding.configured.approval_policy,
            "sandbox": binding.configured.sandbox,
            "args": list(binding.configured.args),
        }
        identity = self._address_registry.identity(access_point)
        if identity is not None:
            info["address"] = identity.address
            info["routing_mode"] = identity.mode
        return info

    def assign_address(
        self,
        access_point: AccessPointKey,
        address: str,
        *,
        routing_mode: str = ROUTING_MODE_SHAMAN,
    ) -> dict[str, Any]:
        self._require_idle_binding(access_point)
        address = self._validate_address_against_approval_target(access_point, address)
        previous_identity = self._address_registry.identity(access_point)
        assigned = self._address_registry.assign(access_point, address, routing_mode)
        previous = previous_identity.address if previous_identity is not None else None
        updated = self._replace_approval_target_references(previous, assigned.address)
        self._log_address_event(
            "routing_address_assigned",
            access_point,
            address=assigned.address,
            routing_mode=assigned.mode,
        )
        result: dict[str, Any] = {"address": assigned.address}
        if updated:
            result["updated_approval_targets"] = updated
        return result

    def show_address(
        self,
        access_point: AccessPointKey,
        *,
        routing_mode: str = ROUTING_MODE_SHAMAN,
    ) -> dict[str, str]:
        if access_point not in self._binding_by_access_point:
            raise RuntimeError("no bound agent for this access point")
        identity = self._address_registry.identity(access_point)
        address = identity.address if identity is not None and identity.mode == routing_mode else ""
        return {"address": address}

    def rename_address(
        self,
        access_point: AccessPointKey,
        address: str,
        *,
        routing_mode: str = ROUTING_MODE_SHAMAN,
    ) -> dict[str, Any]:
        self._require_idle_binding(access_point)
        previous_identity = self._address_registry.identity(access_point)
        if previous_identity is None or previous_identity.mode != routing_mode:
            raise ValueError("binding has no address")
        previous = previous_identity.address
        address = self._validate_address_against_approval_target(access_point, address)
        renamed = self._address_registry.rename(access_point, address, routing_mode)
        updated = self._replace_approval_target_references(previous, renamed.address)
        self._log_address_event(
            "routing_address_renamed",
            access_point,
            previous_address=previous,
            address=renamed.address,
            routing_mode=renamed.mode,
        )
        result: dict[str, Any] = {
            "previous_address": previous or "",
            "address": renamed.address,
        }
        if updated:
            result["updated_approval_targets"] = updated
        return result

    def remove_address(
        self,
        access_point: AccessPointKey,
        *,
        routing_mode: str = ROUTING_MODE_SHAMAN,
    ) -> dict[str, str]:
        self._require_idle_binding(access_point)
        current_identity = self._address_registry.identity(access_point)
        current = (
            current_identity.address
            if current_identity is not None and current_identity.mode == routing_mode
            else None
        )
        if current is not None:
            referrers = approval_target_referrers(
                target_address=current,
                addresses=dict(self._address_registry.items()),
                targets={key: binding.approval_target for key, binding in self._binding_by_access_point.items()},
            )
            if referrers:
                raise ValueError(f"address is used as approval target by: {', '.join(referrers)}")
        removed = self._address_registry.remove(access_point, routing_mode)
        self._log_address_event(
            "routing_address_removed",
            access_point,
            removed_address=removed.address,
            routing_mode=removed.mode,
        )
        return {"removed_address": removed.address}

    def _replace_approval_target_references(self, previous: str | None, address: str) -> int:
        if not previous or previous == address:
            return 0
        updated = 0
        for binding in self._binding_by_access_point.values():
            if binding.approval_target == previous:
                self._set_binding_approval_target(binding, address)
                updated += 1
        return updated

    @staticmethod
    def _set_binding_approval_target(
        binding: _InteractiveAgentBinding,
        target: str,
    ) -> None:
        approval_target = normalize_approval_target(target)
        binding.approval_target = approval_target

    def assign_approval_target(self, access_point: AccessPointKey, target: str) -> dict[str, str]:
        binding = self._require_approval_target_binding(access_point)
        previous = binding.approval_target
        approval_target = self._validate_approval_target(access_point, target)
        self._set_binding_approval_target(binding, approval_target)
        self._log_address_event(
            "approval_target_assigned",
            access_point,
            previous_approval_target=previous,
            approval_target=approval_target,
        )
        result: dict[str, str] = {}
        if previous != HUMAN_APPROVAL_TARGET:
            result["previous_approval_target"] = previous
        result["approval_target"] = approval_target
        return result

    def show_approval_target(self, access_point: AccessPointKey) -> dict[str, str]:
        binding = self._require_approval_target_binding(access_point)
        return {"approval_target": binding.approval_target}

    def approval_target(self, access_point: AccessPointKey) -> str:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            raise RuntimeError("no bound agent for this access point")
        return normalize_approval_target(binding.approval_target)

    def _require_approval_target_binding(
        self,
        access_point: AccessPointKey,
    ) -> _InteractiveAgentBinding:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            raise RuntimeError("no bound agent for this access point")
        return binding

    def _validate_approval_target(self, access_point: AccessPointKey, target: str) -> str:
        return validate_approval_target_assignment(
            source=access_point,
            raw_target=target,
            addresses=dict(self._address_registry.items()),
            targets={key: binding.approval_target for key, binding in self._binding_by_access_point.items()},
        )

    def resolve_routing_identity(
        self,
        address: str,
    ) -> ResolvedRoutingIdentity[AccessPointKey] | None:
        return self._address_registry.resolve(address)

    def _validate_address_against_approval_target(
        self,
        access_point: AccessPointKey,
        raw_address: object,
    ) -> str:
        address = normalize_address(raw_address)
        binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            raise RuntimeError("no bound agent for this access point")
        approval_target = normalize_approval_target(binding.approval_target)
        if approval_target != HUMAN_APPROVAL_TARGET and address == approval_target:
            raise ValueError(
                f"routing address cannot equal its own approval target: {address}"
            )
        return address

    def _require_idle_binding(self, access_point: AccessPointKey) -> None:
        if access_point not in self._binding_by_access_point:
            raise RuntimeError("no bound agent for this access point")
        if self.has_active_turn(access_point):
            raise RuntimeError("cannot change address during active turn")

    def _log_address_event(self, event: str, access_point: AccessPointKey, **fields: object) -> None:
        self._logger.event(
            event,
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            **fields,
        )

    def stop_agent(self, access_point: AccessPointKey) -> bool:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            return False
        if binding.driver is None:
            self._cleanup_pending_drivers(access_point=access_point)
            binding.runtime_intent = RUNTIME_INTENT_BOUND_IDLE
            return True
        startup_generation = (
            binding.runtime_generation.value
            if binding.state == "STARTING"
            and binding.runtime_generation is not None
            else ""
        )
        _idle_binding, cleanup_error = self._transition_to_bound_idle(
            access_point=access_point,
            binding=binding,
            driver=binding.driver,
            use_startup_rollback=True,
            cleanup_event="access_point_agent_stop_cleanup_failed",
        )
        if startup_generation:
            self._logger.event(
                "access_point_agent_start_cancelled",
                access_point_type=access_point.type,
                chat_id=access_point.chat_id,
                thread_id=access_point.thread_id,
                runtime_generation=startup_generation,
                reason="stop",
            )
        if cleanup_error:
            raise RuntimeError(cleanup_error)
        return True

    def startup_failure_result(
        self,
        access_point: AccessPointKey,
        *,
        error: str,
        default_cwd: str = "",
    ) -> InteractiveStartupResult:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            return InteractiveStartupResult(
                agent_id="",
                result="failed",
                state="BOUND_IDLE",
                error=str(error),
                cwd=default_cwd,
                configured_model="",
                effective_model="",
                mode="proxy",
                thread_id="",
                thread_name="",
            )
        return self._startup_result(binding=binding, result="failed", error=str(error))

    def start_bound_agent(self, access_point: AccessPointKey) -> dict[str, Any]:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            raise RuntimeError("no bound agent for this access point")
        if binding.driver is not None:
            return self._binding_start_result(binding)
        spec = _persisted_binding(binding).to_runtime_spec()
        spec["approval_target"] = binding.approval_target
        return self.start_agent(access_point, spec)

    def restore_binding(
        self,
        access_point: AccessPointKey,
        *,
        binding: PersistedAgentBinding,
        runtime_intent: str = RUNTIME_INTENT_BOUND_IDLE,
        address: str = "",
        routing_mode: str = "",
        approval_target: str = HUMAN_APPROVAL_TARGET,
    ) -> dict[str, str]:
        restored_id = binding.agent_id.strip() or (
            f"restored-agent-{access_point.type}-"
            f"{access_point.chat_id}-"
            f"{access_point.thread_id if access_point.thread_id is not None else 'main'}"
        )
        runtime_binding = _InteractiveAgentBinding(
            node_id=restored_id,
            provider=binding.provider,
            profile=binding.profile,
            configured=binding.configured,
            session=binding.session,
            requested_session=binding.session,
            driver=None,
            adapter_name=binding.adapter_name,
            adapter_version=binding.adapter_version,
            runtime_name=binding.runtime_name,
            runtime_version=binding.runtime_version,
            runtime_intent=normalize_runtime_intent(runtime_intent),
            session_name=binding.session_name,
            approval_target=normalize_approval_target(approval_target),
        )
        previous_claims = self._session_ownership.claims(access_point)
        try:
            self._session_ownership.replace_claims(
                access_point,
                self._session_claims_with_pending_cleanup(
                    access_point,
                    binding=runtime_binding,
                ),
            )
        except ProviderSessionBoundElsewhereError as exc:
            self._restore_conflict_by_access_point[access_point] = exc
            raise
        restored_address = str(address or "").strip()
        try:
            if restored_address:
                self._address_registry.assign(
                    access_point,
                    restored_address,
                    str(routing_mode or ROUTING_MODE_SHAMAN).strip(),
                )
            self._replace_binding(access_point, runtime_binding)
        except Exception:
            self._session_ownership.replace_claims(access_point, previous_claims)
            raise
        self._logger.event(
            "access_point_agent_binding_restored",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            agent_id=runtime_binding.node_id,
            provider=runtime_binding.provider.value,
            profile=runtime_binding.profile.value,
            cwd=runtime_binding.cwd,
            configured_model=runtime_binding.model,
            configured_backend=runtime_binding.backend,
            mode=runtime_binding.mode,
            restored_session_id=runtime_binding.thread_id,
        )
        return {
            "agent_id": runtime_binding.node_id,
            "provider": runtime_binding.provider.value,
            "profile": runtime_binding.profile.value,
            "cwd": runtime_binding.cwd,
            "model": runtime_binding.model,
            "configured_backend": runtime_binding.backend,
            "mode": runtime_binding.mode,
            "thread_id": runtime_binding.thread_id,
            "thread_name": runtime_binding.thread_name,
        }

    def snapshot_persisted(self) -> dict[AccessPointKey, PersistedAccessPointState]:
        snapshot: dict[AccessPointKey, PersistedAccessPointState] = {}
        for access_point, binding in self._binding_by_access_point.items():
            identity = self._address_registry.identity(access_point)
            snapshot[access_point] = PersistedAccessPointState(
                project_cwd=binding.cwd,
                runtime_intent=binding.runtime_intent,
                address=identity.address if identity is not None else "",
                routing_mode=identity.mode if identity is not None else "",
                approval_target=binding.approval_target,
                agent=_persisted_binding(binding),
            )
        return snapshot

    def reset(self, access_point: AccessPointKey) -> bool:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            conflict = self._restore_conflict_by_access_point.pop(access_point, None)
            if conflict is None:
                return False
            self._sync_session_claims(access_point)
            self._metadata_changes.discard(access_point)
            return True
        current_identity = self._address_registry.identity(access_point)
        current = current_identity.address if current_identity is not None else None
        if current is not None:
            referrers = approval_target_referrers(
                target_address=current,
                addresses=dict(self._address_registry.items()),
                targets={key: item.approval_target for key, item in self._binding_by_access_point.items()},
            )
            if referrers:
                raise ValueError(f"address is used as approval target by: {', '.join(referrers)}")
        self._cleanup_pending_drivers(access_point=access_point)
        if binding.driver is not None:
            cleanup_owned_resources(
                [
                    CleanupStep(
                        owner=f"runtime agent driver {binding.node_id}",
                        cleanup=binding.driver.stop,
                    )
                ]
            )
        self._remove_binding(access_point)
        if current_identity is not None:
            self._address_registry.remove(access_point, current_identity.mode)
        return True

    def steer_wait_reason(
        self,
        access_point: AccessPointKey,
        expected_turn_ref: TurnRef,
    ) -> str:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None:
            return ""
        return binding.driver.steer_wait_reason(expected_turn_ref)

    def try_steer_turn(
        self,
        access_point: AccessPointKey,
        agent_input: AgentInput,
        expected_turn_ref: TurnRef,
        *,
        context_note: str | None = None,
    ) -> AgentEffectResult:
        if context_note:
            return AgentEffectResult.rejected("runtime agent steer does not accept Steward context notes")
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None or binding.effective is None:
            return AgentEffectResult.rejected("no running agent is bound to this access point")
        if expected_turn_ref.runtime_generation != binding.effective.generation:
            return AgentEffectResult.rejected("agent runtime generation no longer matches")
        self._logger.event(
            "runtime_agent_steer_requested",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            agent_id=binding.node_id,
            runtime_thread_id=binding.thread_id,
            turn_ref_sequence=expected_turn_ref.sequence,
            text_preview=agent_input.text.strip()[:200],
        )
        return binding.driver.try_steer_turn(
            InteractiveAgentRequest(
                chat_id=access_point.chat_id,
                thread_id=access_point.thread_id,
                agent_input=agent_input,
            ),
            expected_turn_ref,
        )

    def next_turn_ref(self, access_point: AccessPointKey) -> TurnRef:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None or binding.effective is None:
            raise RuntimeError("no running agent is bound to this access point")
        binding.turn_sequence += 1
        return TurnRef(binding.effective.generation, binding.turn_sequence)

    def turn_wait_reason(self, access_point: AccessPointKey) -> str:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None:
            return "agent_not_running"
        if binding.state != "RUNNING" or not binding.driver.is_ready():
            return "agent_starting"
        if binding.driver.has_busy_work():
            return "agent_busy"
        return ""

    def start_turn(
        self,
        access_point: AccessPointKey,
        agent_input: AgentInput,
        turn_ref: TurnRef,
        *,
        context_note: str | None = None,
    ) -> AgentEffectResult:
        if context_note:
            raise ValueError("runtime agent turns do not accept Steward context notes")
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None or binding.effective is None:
            return AgentEffectResult.rejected("no running agent is bound to this access point")
        if turn_ref.runtime_generation != binding.effective.generation:
            return AgentEffectResult.rejected("agent runtime generation no longer matches")
        request = InteractiveAgentRequest(
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            agent_input=agent_input,
        )
        receipt = binding.driver.start_turn(request, turn_ref)
        if receipt.outcome is AgentEffectOutcome.ACCEPTED:
            binding.effective.turn_ref = turn_ref
            binding.effective.approval_ref = None
            self._logger.event(
                "runtime_agent_turn_started",
                access_point_type=access_point.type,
                chat_id=access_point.chat_id,
                thread_id=access_point.thread_id,
                agent_id=binding.node_id,
                runtime_thread_id=binding.thread_id,
                turn_ref_sequence=turn_ref.sequence,
                text_preview=agent_input.text.strip()[:200],
            )
        return receipt

    def submit_approval_decision(
        self,
        access_point: AccessPointKey,
        decision: str,
        *,
        expected_request: AgentApprovalRequest,
    ) -> ApprovalDeliveryReceipt:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None:
            raise RuntimeError("no running agent is bound to this access point")
        self._logger.event(
            "runtime_agent_submit_approval_decision",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            agent_id=binding.node_id,
            runtime_thread_id=binding.thread_id,
            decision=decision,
        )
        return binding.driver.submit_approval_decision(
            decision,
            expected_request=expected_request,
        )

    def submit_approval_decision_now(
        self,
        access_point: AccessPointKey,
        decision: str,
        *,
        expected_request: AgentApprovalRequest,
    ) -> ApprovalDecisionResult:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None:
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected(
                    "no running agent is bound to this access point"
                )
            )
        self._logger.event(
            "runtime_agent_submit_approval_decision_now",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            agent_id=binding.node_id,
            runtime_thread_id=binding.thread_id,
            decision=decision,
        )
        return binding.driver.submit_approval_decision_now(
            decision,
            expected_request=expected_request,
        )

    def submit_elicitation_response(
        self,
        access_point: AccessPointKey,
        action: ElicitationAction,
        *,
        expected_request: AgentFormRequest,
        content: dict[str, object] | None = None,
    ) -> ElicitationDeliveryReceipt:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None:
            raise RuntimeError("no running agent is bound to this access point")
        return binding.driver.submit_elicitation_response(
            action,
            expected_request=expected_request,
            content=content,
        )

    def submit_model_selection(
        self,
        access_point: AccessPointKey,
        model: str,
    ) -> None:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None or binding.state != "RUNNING":
            raise RuntimeError("agent session is not running")
        binding.driver.submit_model_selection(
            AgentModelSelection(
                access_point=(access_point.chat_id, access_point.thread_id),
                selected_model=model,
            )
        )

    def submit_effort_selection(
        self,
        access_point: AccessPointKey,
        effort: str,
    ) -> None:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None or binding.state != "RUNNING":
            raise RuntimeError("agent session is not running")
        binding.driver.submit_effort_selection(
            AgentEffortSelection(
                access_point=(access_point.chat_id, access_point.thread_id),
                selected_effort=effort,
            )
        )

    def poll_once(
        self,
        *,
        idle_poll_blocked_resources: frozenset[object] = frozenset(),
    ) -> list[StewardDriverEvent]:
        self._poll_progressed = False
        out: list[StewardDriverEvent] = []
        for access_point, binding in self._ordered_bindings_for_poll():
            driver = binding.driver
            if driver is None:
                continue
            had_active_turn = driver.has_active_turn()
            active_turn_ref = (
                binding.effective.turn_ref
                if had_active_turn and binding.effective is not None
                else None
            )
            if had_active_turn and active_turn_ref is None:
                self._logger.event(
                    "runtime_agent_active_turn_uncorrelated",
                    access_point_type=access_point.type,
                    chat_id=access_point.chat_id,
                    thread_id=access_point.thread_id,
                    agent_id=binding.node_id,
                )
            if (
                access_point in idle_poll_blocked_resources
                and binding.state == "RUNNING"
                and driver.is_ready()
                and not had_active_turn
            ):
                continue
            try:
                events = driver.poll_once()
                driver.acknowledge_event_handoff()
            except Exception as exc:
                if binding.state == "STARTING":
                    event = self._fail_startup(
                        access_point=access_point,
                        binding=binding,
                        driver=driver,
                        error=str(exc),
                    )
                    if event is not None:
                        out.append(event)
                        self._poll_progressed = True
                    continue
                out.extend(
                    self._fail_running_runtime(
                        access_point=access_point,
                        binding=binding,
                        driver=driver,
                        error=str(exc),
                        had_active_turn=had_active_turn,
                        active_turn_ref=active_turn_ref,
                        emitted_events=[],
                    )
                )
                self._poll_progressed = True
                continue
            if driver.consume_poll_progress():
                self._poll_progressed = True
            for event in events:
                if isinstance(event, InteractiveModelApplyResult):
                    if event.result == "requested":
                        binding.configured = replace(
                            binding.configured,
                            model=event.requested_model,
                        )
                        self._metadata_changes.add(access_point)
                if isinstance(event, InteractiveEffortApplyResult):
                    self._logger.event(
                        "agent_effort_apply_result",
                        access_point_type=access_point.type,
                        chat_id=access_point.chat_id,
                        thread_id=access_point.thread_id,
                        requested_effort=event.requested_effort,
                        actual_effort=event.actual_effort,
                        result=event.result,
                    )
                if (
                    isinstance(event, InteractiveApprovalPromptEvent)
                    and binding.effective is not None
                    and event.request.turn_ref == binding.effective.turn_ref
                ):
                    binding.effective.approval_ref = event.request.approval_ref
                self._logger.event(
                    "runtime_agent_driver_event",
                    access_point_type=access_point.type,
                    chat_id=access_point.chat_id,
                    thread_id=access_point.thread_id,
                    agent_id=binding.node_id,
                    runtime_thread_id=binding.thread_id,
                    event_type=type(event).__name__,
                )
                out.append(StewardDriverEvent(access_point=access_point, source="agent", event=event))
                if (
                    isinstance(event, (InteractiveAgentResult, InteractiveInterruptResult))
                    and binding.effective is not None
                ):
                    if binding.effective.turn_ref == event.turn_ref:
                        binding.effective.turn_ref = None
                        binding.effective.approval_ref = None
                    else:
                        self._logger.event(
                            "runtime_agent_stale_terminal_state_ignored",
                            access_point_type=access_point.type,
                            chat_id=access_point.chat_id,
                            thread_id=access_point.thread_id,
                            agent_id=binding.node_id,
                            runtime_generation=event.turn_ref.runtime_generation.value,
                            turn_ref_sequence=event.turn_ref.sequence,
                        )
            if (
                binding.state == "RUNNING"
                and binding.effective is not None
                and any(isinstance(event, InteractiveAgentResult) for event in events)
            ):
                session = driver.get_local_projection().session
                if session.availability is SnapshotAvailability.AVAILABLE:
                    binding.effective.model = session.model.strip()
                    binding.effective.backend = session.backend.strip()
            terminal_error = str(driver.get_terminal_error() or "")
            if binding.state == "RUNNING" and terminal_error:
                out.extend(
                    self._fail_running_runtime(
                        access_point=access_point,
                        binding=binding,
                        driver=driver,
                        error=terminal_error,
                        had_active_turn=had_active_turn,
                        active_turn_ref=active_turn_ref,
                        emitted_events=events,
                    )
                )
                self._poll_progressed = True
                continue
            if binding.state == "STARTING" and driver.is_ready():
                persisted_metadata_changed = False
                capabilities = driver.get_capability_snapshot()
                observed_components = (
                    capabilities.adapter.name,
                    capabilities.adapter.version,
                    capabilities.runtime.name,
                    capabilities.runtime.version,
                )
                configured_components = (
                    binding.adapter_name,
                    binding.adapter_version,
                    binding.runtime_name,
                    binding.runtime_version,
                )
                if observed_components != configured_components:
                    (
                        binding.adapter_name,
                        binding.adapter_version,
                        binding.runtime_name,
                        binding.runtime_version,
                    ) = observed_components
                    persisted_metadata_changed = True
                session = driver.get_local_projection().session
                actual_model = session.model.strip()
                if binding.effective is None:
                    raise AssertionError("starting binding must have a runtime generation")
                binding.effective.model = actual_model
                binding.effective.backend = session.backend.strip()
                actual_session = session.session_ref
                if (
                    actual_session is not None
                    and actual_session.execution_profile
                    != ProviderExecutionProfile(binding.provider, binding.profile)
                ):
                    raise RuntimeError(
                        "started provider session does not match configured provider profile"
                    )
                startup_claims = set(self._binding_session_claims(binding))
                if actual_session is not None:
                    startup_claims.add(actual_session)
                try:
                    self._session_ownership.replace_claims(
                        access_point,
                        set(
                            self._session_claims_with_pending_cleanup(
                                access_point,
                                binding=binding,
                            )
                        )
                        | startup_claims,
                    )
                except ProviderSessionBoundElsewhereError as exc:
                    event = self._fail_startup(
                        access_point=access_point,
                        binding=binding,
                        driver=driver,
                        error=str(exc),
                    )
                    if event is not None:
                        out.append(event)
                        self._poll_progressed = True
                    continue
                binding.effective.session = actual_session
                if actual_session is not None and actual_session != binding.session:
                    binding.session = actual_session
                    persisted_metadata_changed = True
                binding.state = "RUNNING"
                binding.runtime_intent = RUNTIME_INTENT_RUNNING
                binding.rollback_binding = None
                self._sync_session_claims(access_point, binding=binding)
                if persisted_metadata_changed:
                    self._metadata_changes.add(access_point)
                if not binding.startup_logged:
                    lifecycle_event = "thread_resumed" if binding.requested_thread_id else "thread_started"
                    self._logger.event(
                        lifecycle_event,
                        access_point_type=access_point.type,
                        chat_id=access_point.chat_id,
                        thread_id=access_point.thread_id,
                        agent_id=binding.node_id,
                        runtime_thread_id=binding.thread_id,
                        requested_thread_id=binding.requested_thread_id,
                        mode=binding.mode,
                        provider=binding.provider.value,
                        profile=binding.profile.value,
                        configured_model=binding.model,
                        configured_backend=binding.backend,
                        effective_model=binding.effective_model,
                        effective_backend=binding.effective_backend,
                        cwd=binding.cwd,
                    )
                    binding.startup_logged = True
                out.append(
                    StewardDriverEvent(
                        access_point=access_point,
                        source="agent",
                        event=self._startup_result(
                            binding=binding,
                            result="completed",
                            error="",
                        ),
                    )
                )
                self._logger.event(
                    "access_point_agent_start_completed",
                    access_point_type=access_point.type,
                    chat_id=access_point.chat_id,
                    thread_id=access_point.thread_id,
                    node_id=binding.node_id,
                    runtime_thread_id=binding.thread_id,
                    configured_model=binding.model,
                    provider=binding.provider.value,
                    profile=binding.profile.value,
                    configured_backend=binding.backend,
                    effective_model=binding.effective_model,
                    effective_backend=binding.effective_backend,
                    runtime_generation=binding.runtime_generation.value,
                )
                self._poll_progressed = True
        return out

    def _ordered_bindings_for_poll(self) -> list[tuple[AccessPointKey, _InteractiveAgentBinding]]:
        return list(self._binding_by_access_point.items())

    def _fail_running_runtime(
        self,
        *,
        access_point: AccessPointKey,
        binding: _InteractiveAgentBinding,
        driver: AgentDeliveryDriverPort,
        error: str,
        had_active_turn: bool,
        active_turn_ref: TurnRef | None,
        emitted_events: list[Any],
    ) -> list[StewardDriverEvent]:
        current = self._binding_by_access_point.get(access_point)
        if current is not binding or binding.driver is not driver or binding.state != "RUNNING":
            return []
        failure = str(error).strip() or "runtime driver failed"
        out: list[StewardDriverEvent] = []
        has_turn_failure = any(
            isinstance(event, InteractiveAgentResult)
            and event.failed
            and event.turn_ref == active_turn_ref
            for event in emitted_events
        )
        if had_active_turn and active_turn_ref is not None and not has_turn_failure:
            out.append(
                StewardDriverEvent(
                    access_point=access_point,
                    source="agent",
                    event=InteractiveAgentResult.failed_result(
                        chat_id=access_point.chat_id,
                        thread_id=access_point.thread_id,
                        agent_input=AgentInput(
                            text="",
                            origin=AgentInputOrigin.CONTROL,
                            verbatim=True,
                        ),
                        error=failure,
                        turn_ref=active_turn_ref,
                    ),
                )
            )
        resume_supported = driver.get_capability_snapshot().supports(
            AgentCapability.RESUME_SESSION
        )
        failed_binding, cleanup_error = self._transition_to_bound_idle(
            access_point=access_point,
            binding=binding,
            driver=driver,
            use_startup_rollback=False,
            cleanup_event="access_point_agent_runtime_cleanup_failed",
        )
        if not resume_supported:
            failed_binding.session = None
            failed_binding.requested_session = None
            self._sync_session_claims(access_point, binding=failed_binding)
            self._metadata_changes.add(access_point)
        if cleanup_error:
            failure = f"{failure}; cleanup failed: {cleanup_error}"
        self._logger.event(
            "access_point_agent_runtime_failed",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            node_id=failed_binding.node_id,
            state="BOUND_IDLE",
            error=failure,
        )
        out.append(
            StewardDriverEvent(
                access_point=access_point,
                source="agent",
                event=InteractiveRuntimeFailure(
                    agent_id=failed_binding.node_id,
                    error=failure,
                ),
            )
        )
        return out

    def _fail_startup(
        self,
        *,
        access_point: AccessPointKey,
        binding: _InteractiveAgentBinding,
        driver: AgentDeliveryDriverPort,
        error: str,
    ) -> StewardDriverEvent | None:
        current = self._binding_by_access_point.get(access_point)
        if current is not binding or binding.driver is not driver or binding.state != "STARTING":
            self._logger.event(
                "access_point_agent_start_failure_ignored",
                access_point_type=access_point.type,
                chat_id=access_point.chat_id,
                thread_id=access_point.thread_id,
                runtime_generation=(
                    binding.runtime_generation.value
                    if binding.runtime_generation is not None
                    else ""
                ),
                error=str(error),
            )
            return None

        runtime_generation = (
            binding.runtime_generation.value
            if binding.runtime_generation is not None
            else ""
        )
        failed_binding, stop_error = self._transition_to_bound_idle(
            access_point=access_point,
            binding=binding,
            driver=driver,
            use_startup_rollback=True,
            cleanup_event="access_point_agent_start_cleanup_failed",
        )
        failure = str(error).strip() or "startup failed"
        if stop_error:
            failure = f"{failure}; cleanup failed: {stop_error}"
        self._logger.event(
            "access_point_agent_start_failed",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            node_id=failed_binding.node_id,
            runtime_generation=runtime_generation,
            state="BOUND_IDLE",
            error=failure,
        )
        return StewardDriverEvent(
            access_point=access_point,
            source="agent",
            event=self._startup_result(
                binding=failed_binding,
                result="failed",
                error=failure,
            ),
        )

    def _transition_to_bound_idle(
        self,
        *,
        access_point: AccessPointKey,
        binding: _InteractiveAgentBinding,
        driver: AgentDeliveryDriverPort,
        use_startup_rollback: bool,
        cleanup_event: str,
    ) -> tuple[_InteractiveAgentBinding, str]:
        runtime_generation = (
            binding.runtime_generation.value
            if binding.runtime_generation is not None
            else ""
        )
        rollback = binding.rollback_binding if use_startup_rollback else None
        self._retain_pending_driver_cleanup(
            access_point=access_point,
            binding=binding,
            driver=driver,
        )
        if rollback is not None:
            self._replace_binding(access_point, rollback)
            idle_binding = rollback
        else:
            binding.driver = None
            binding.state = "BOUND_IDLE"
            binding.effective = None
            binding.turn_sequence = 0
            binding.rollback_binding = None
            self._replace_binding(access_point, binding)
            idle_binding = binding
        idle_binding.runtime_intent = RUNTIME_INTENT_BOUND_IDLE
        cleanup_error = ""
        try:
            self._cleanup_pending_drivers(node_id=binding.node_id)
        except CleanupError as exc:
            cleanup_error = str(exc)
            self._logger.event(
                cleanup_event,
                access_point_type=access_point.type,
                chat_id=access_point.chat_id,
                thread_id=access_point.thread_id,
                runtime_generation=runtime_generation,
                error=cleanup_error,
                error_type=type(exc).__name__,
            )
        return idle_binding, cleanup_error

    @staticmethod
    def _startup_result(
        *,
        binding: _InteractiveAgentBinding,
        result: str,
        error: str,
    ) -> InteractiveStartupResult:
        return InteractiveStartupResult(
            agent_id=binding.node_id,
            result=result,
            state=binding.state,
            error=error,
            cwd=binding.cwd,
            provider=binding.provider.value,
            profile=binding.profile.value,
            **_binding_component_fields(binding),
            configured_model=binding.model,
            configured_backend=binding.backend,
            effective_model=binding.effective_model,
            effective_backend=binding.effective_backend,
            mode=binding.mode,
            thread_id=binding.thread_id,
            thread_name=binding.thread_name,
        )

    def consume_poll_progress(self) -> bool:
        progressed = self._poll_progressed
        self._poll_progressed = False
        return progressed

    def pending_metadata_changes(self) -> tuple[AccessPointKey, ...]:
        return tuple(sorted(self._metadata_changes, key=access_point_sort_key))

    def acknowledge_metadata_change(self, access_point: AccessPointKey) -> None:
        self._metadata_changes.discard(access_point)

    def has_pending_startup(self) -> bool:
        return any(
            binding.state == "STARTING"
            for binding in self._binding_by_access_point.values()
        )

    def show_running(self, access_point: AccessPointKey) -> list[dict[str, Any]]:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            return []
        state = self.runtime_state(access_point)
        return [
            {
                "role": "agent",
                "agent_id": binding.node_id,
                "provider": binding.provider.value,
                "profile": binding.profile.value,
                **_binding_component_fields(binding),
                "state": state,
                "controllable": state in {"STARTING", "RUNNING"},
                "cwd": binding.cwd,
                "model": binding.model,
                "configured_model": binding.model,
                "effective_model": binding.effective_model,
                "configured_backend": binding.backend,
                "effective_backend": binding.effective_backend,
                "mode": binding.mode,
                "thread_id": binding.thread_id,
            }
        ]

    def has_active_turn(self, access_point: AccessPointKey) -> bool:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None:
            return False
        return bool(binding.driver.has_active_turn())

    def interrupt_active_turn(self, access_point: AccessPointKey) -> None:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None:
            raise RuntimeError("no running agent is bound to this access point")
        binding.driver.interrupt_active_turn()

    def get_capability_snapshot(
        self,
        access_point: AccessPointKey,
    ) -> AgentCapabilitySnapshot | None:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            return None
        if binding.driver is not None:
            return binding.driver.get_capability_snapshot()
        if binding.provider != ProviderKind.CODEX:
            return None
        return codex_capability_snapshot(backend_name=binding.backend or "unknown")

    def get_local_projection(self, access_point: AccessPointKey) -> AgentRuntimeProjection:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            return unavailable_projection()
        if binding.driver is not None:
            return binding.driver.get_local_projection()
        availability = (
            SnapshotAvailability.UNKNOWN
            if binding.provider == ProviderKind.CODEX
            else SnapshotAvailability.UNSUPPORTED
        )
        capabilities = (
            codex_capability_snapshot(backend_name=binding.backend or "unknown")
            if binding.provider == ProviderKind.CODEX
            else None
        )
        return AgentRuntimeProjection(
            capabilities=capabilities,
            session=AgentSessionSnapshot(
                availability,
                session_ref=binding.session,
                name=binding.thread_name,
                model=binding.model,
                backend=binding.backend,
            ),
            work=AgentWorkSnapshot(availability),
            context=AgentContextSnapshot(
                SnapshotAvailability.UNKNOWN
                if binding.provider == ProviderKind.CODEX
                else SnapshotAvailability.UNSUPPORTED
            ),
            background=AgentBackgroundSnapshot(SnapshotAvailability.UNSUPPORTED),
        )

    def request_inspection(
        self,
        access_point: AccessPointKey,
        inspection_ref: InspectionRef,
        timeout_sec: float,
    ) -> AgentEffectResult:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None or binding.state != "RUNNING":
            return AgentEffectResult.rejected("agent session is not running")
        return binding.driver.request_inspection(
            inspection_ref,
            timeout_sec,
            (access_point.chat_id, access_point.thread_id),
        )

    def close(self) -> None:
        steps: list[CleanupStep] = []
        for access_point, binding in list(self._binding_by_access_point.items()):
            driver = binding.driver
            if driver is None:
                self._remove_binding(access_point)
                continue

            def release_binding(
                access_point: AccessPointKey = access_point,
                binding: _InteractiveAgentBinding = binding,
            ) -> None:
                if self._binding_by_access_point.get(access_point) is binding:
                    self._remove_binding(access_point)

            steps.append(
                CleanupStep(
                    owner=f"runtime agent driver {binding.node_id}",
                    cleanup=driver.stop,
                    release=release_binding,
                )
            )
        steps.extend(self._pending_driver_cleanup_steps())
        cleanup_owned_resources(steps)
        self._restore_conflict_by_access_point.clear()

    def _build_driver(
        self,
        *,
        access_point: AccessPointKey,
        spec: dict[str, Any],
    ) -> AgentDeliveryDriverPort:
        return self._driver_factory(access_point, spec, self._logger)

    def _pending_driver_cleanup_steps(
        self,
        *,
        access_point: AccessPointKey | None = None,
        node_id: str | None = None,
    ) -> list[CleanupStep]:
        steps: list[CleanupStep] = []
        for pending_node_id, pending in list(self._pending_driver_cleanup.items()):
            pending_access_point = pending.access_point
            driver = pending.driver
            if access_point is not None and pending_access_point != access_point:
                continue
            if node_id is not None and pending_node_id != node_id:
                continue

            def release(
                pending_node_id: str = pending_node_id,
                pending_access_point: AccessPointKey = pending_access_point,
                driver: AgentDeliveryDriverPort = driver,
            ) -> None:
                current = self._pending_driver_cleanup.get(pending_node_id)
                if current is not None and current.driver is driver:
                    self._pending_driver_cleanup.pop(pending_node_id, None)
                    self._sync_session_claims(pending_access_point)

            steps.append(
                CleanupStep(
                    owner=f"runtime startup driver {pending_node_id}",
                    cleanup=driver.stop,
                    release=release,
                )
            )
        return steps

    def _cleanup_pending_drivers(
        self,
        *,
        access_point: AccessPointKey | None = None,
        node_id: str | None = None,
    ) -> None:
        cleanup_owned_resources(
            self._pending_driver_cleanup_steps(
                access_point=access_point,
                node_id=node_id,
            )
        )


class StopStewardLoop(Exception):
    def __init__(self, code: int, *, drain: bool) -> None:
        super().__init__(code)
        self.code = code
        self.drain = drain


STEWARD_SLOW_TICK_PHASE_WARNING_SEC = 1.0
_STEWARD_TICK_DIAGNOSTIC_NAME_MAX_CHARS = 64


@dataclass(frozen=True)
class StewardTickDiagnosticContext:
    operation: str
    access_point: AccessPointKey | None = None

    def __post_init__(self) -> None:
        operation = str(self.operation).strip()
        if not operation:
            raise ValueError("Steward tick diagnostic operation must not be empty")
        if len(operation) > _STEWARD_TICK_DIAGNOSTIC_NAME_MAX_CHARS:
            raise ValueError("Steward tick diagnostic operation is too long")
        object.__setattr__(self, "operation", operation)


class StewardTickLoop:
    _DRAIN_IDLE_TIMEOUT_SEC = 0.5
    _DRAIN_WALL_TIMEOUT_SEC = 5.0

    def __init__(
        self,
        *,
        drain_driver_events: Callable[[], bool],
        tick_interactions: Callable[[], bool],
        flush_due_status_runtimes: Callable[[], bool],
        consume_transport: Callable[[], bool],
        drain_pending_inputs: Callable[[], bool],
        is_idle: Callable[[], bool],
        consume_control_events: Callable[[], bool] | None = None,
        drain_sleep_sec: float = 0.02,
        sleep_fn: Callable[[float], None] = time.sleep,
        wall_monotonic_now: Callable[[], float] = time.monotonic,
        lifecycle_logger: LifecycleLogger | None = None,
        phase_monotonic_now: Callable[[], float] = time.monotonic,
        phase_context: Callable[[str], StewardTickDiagnosticContext | None] | None = None,
    ) -> None:
        self._drain_driver_events = drain_driver_events
        self._tick_interactions = tick_interactions
        self._flush_due_status_runtimes = flush_due_status_runtimes
        self._consume_transport = consume_transport
        self._drain_pending_inputs = drain_pending_inputs
        self._is_idle = is_idle
        self._consume_control_events = consume_control_events or (lambda: False)
        self._drain_sleep_sec = drain_sleep_sec
        self._sleep_fn = sleep_fn
        self._wall_monotonic_now = wall_monotonic_now
        self._lifecycle_logger = lifecycle_logger
        self._phase_monotonic_now = phase_monotonic_now
        self._phase_context = phase_context

    def tick_once(self) -> bool:
        return self._run_phases(self._phase_plan(consume_transport=True))

    def tick_without_transport(self) -> bool:
        return self._run_phases(self._phase_plan(consume_transport=False))

    def _phase_plan(
        self,
        *,
        consume_transport: bool,
    ) -> tuple[tuple[str, Callable[[], bool]], ...]:
        phases: list[tuple[str, Callable[[], bool]]] = [
            ("consume_control_events", self._consume_control_events),
            ("flush_status_before_drivers", self._flush_due_status_runtimes),
            ("drain_driver_events", self._drain_driver_events),
            ("tick_interactions", self._tick_interactions),
            (
                "flush_status_before_transport"
                if consume_transport
                else "flush_status_before_inputs",
                self._flush_due_status_runtimes,
            ),
        ]
        if consume_transport:
            phases.append(("consume_transport", self._consume_transport))
        phases.extend(
            (
                ("drain_pending_inputs", self._drain_pending_inputs),
                ("flush_status_after_inputs", self._flush_due_status_runtimes),
            )
        )
        return tuple(phases)

    def _run_phases(
        self,
        phases: tuple[tuple[str, Callable[[], bool]], ...],
    ) -> bool:
        progressed = False
        for phase_name, phase in phases:
            progressed = self._run_phase(phase_name, phase) or progressed
        return progressed

    def _run_phase(self, phase_name: str, phase: Callable[[], bool]) -> bool:
        logger = self._lifecycle_logger
        if logger is None:
            return phase()

        context: StewardTickDiagnosticContext | None = None
        if self._phase_context is not None:
            try:
                candidate = self._phase_context(phase_name)
                if isinstance(candidate, StewardTickDiagnosticContext):
                    context = candidate
            except Exception:
                context = None

        try:
            started_at = float(self._phase_monotonic_now())
        except Exception:
            return phase()

        result = phase()

        try:
            duration_sec = max(
                0.0,
                float(self._phase_monotonic_now()) - started_at,
            )
        except Exception:
            return result
        if duration_sec <= STEWARD_SLOW_TICK_PHASE_WARNING_SEC:
            return result

        fields: dict[str, object] = {
            "phase": phase_name[:_STEWARD_TICK_DIAGNOSTIC_NAME_MAX_CHARS],
            "duration_ms": round(duration_sec * 1000.0, 3),
        }
        if context is not None:
            fields["operation"] = context.operation
            if context.access_point is not None:
                fields.update(
                    {
                        "access_point_type": context.access_point.type,
                        "chat_id": context.access_point.chat_id,
                        "thread_id": context.access_point.thread_id,
                    }
                )
        try:
            logger.event("steward_slow_tick_phase", **fields)
        except Exception:
            pass
        return result

    def drain_until_idle(self, *, consume_transport: bool = True) -> int:
        idle_timeout_sec = self._DRAIN_IDLE_TIMEOUT_SEC
        idle_deadline = clock.monotonic() + idle_timeout_sec
        # Productive ticks extend inactivity, but cannot turn shutdown drain into a livelock.
        wall_deadline = self._wall_monotonic_now() + self._DRAIN_WALL_TIMEOUT_SEC
        while (
            clock.monotonic() < idle_deadline
            and self._wall_monotonic_now() < wall_deadline
        ):
            try:
                progressed = self.tick_once() if consume_transport else self.tick_without_transport()
            except StopStewardLoop as stop:
                return stop.code
            if self._is_idle() and not progressed:
                return 0
            if progressed:
                idle_deadline = clock.monotonic() + idle_timeout_sec
            self._sleep_fn(self._drain_sleep_sec)
        return 0
