from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
import time
from typing import Any, Callable, Protocol

from orchestrator.access_point_common import (
    AccessPointKey,
    AccessPointMessageRef,
    access_point_sort_key,
)
from orchestrator.approval import ApprovalPolicy, ApprovalRequest
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
from orchestrator.interactive_codex_driver import CodexInteractiveDriver
from orchestrator.interactive_driver_events import (
    InteractiveAgentResult,
    InteractiveAgentRequest,
    InteractiveCodexDriver,
    InteractiveRuntimeFailure,
    InteractiveStartupResult,
    RuntimeDriverFactory,
    StewardDriverFactory,
)
from orchestrator.local_command_journal import LocalCommandOutcome
from orchestrator.processes import LifecycleLogger
from orchestrator.interaction_queue import InteractionTickContext
from orchestrator.steward_state import (
    RUNTIME_INTENT_BOUND_IDLE,
    RUNTIME_INTENT_RUNNING,
    PersistedAccessPointState,
    normalize_runtime_intent,
)


@dataclass
class StewardDriverEvent:
    access_point: AccessPointKey
    source: str
    event: Any


class AgentRuntimeStartDecision(StrEnum):
    UNBOUND = "unbound"
    DEFERRED = "deferred"
    STARTING = "starting"
    RUNNING = "running"
    EXPLICITLY_STOPPED = "explicitly_stopped"


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
    driver: InteractiveCodexDriver
    steward_prompt_sent: bool = False


class StewardDeliveryLane(Protocol):
    def submit_request(
        self,
        access_point: AccessPointKey,
        text: str,
        *,
        context_note: str | None = None,
        as_steer: bool = False,
    ) -> None: ...
    def poll_once(self) -> list[StewardDriverEvent]: ...
    def consume_poll_progress(self) -> bool: ...
    def submit_approval_decision(self, access_point: AccessPointKey, decision: str, *, expected_request: ApprovalRequest | None = None) -> None: ...
    def show_running(self, access_point: AccessPointKey) -> list[dict[str, Any]]: ...
    def get_item_status_snapshot(self, access_point: AccessPointKey) -> list[dict[str, str]]: ...
    def get_item_status_snapshot_for_turn(self, access_point: AccessPointKey, turn_id: str) -> list[dict[str, str]]: ...
    def get_last_item_apply_info(self, access_point: AccessPointKey) -> dict | None: ...
    def has_active_turn(self, access_point: AccessPointKey) -> bool: ...
    def interrupt_active_turn(self, access_point: AccessPointKey) -> None: ...
    def get_thread_metadata(self, access_point: AccessPointKey) -> dict[str, Any]: ...
    def get_context_usage(self, access_point: AccessPointKey) -> dict[str, object]: ...
    def reset(self, access_point: AccessPointKey) -> bool: ...
    def close(self) -> None: ...


class AgentDeliveryLane(StewardDeliveryLane, Protocol):
    def submit_approval_decision_now(self, access_point: AccessPointKey, decision: str, *, expected_request: ApprovalRequest | None = None) -> None: ...
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
    def get_thread_metadata(self, access_point: AccessPointKey) -> dict[str, Any]: ...
    def get_context_usage(self, access_point: AccessPointKey) -> dict[str, object]: ...
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
        agent_command: list[str],
        request_timeout_sec: float,
        rpc_timeout_sec: float,
        rpc_retries: int,
        approval_policy: ApprovalPolicy,
        thread_approval_policy: str,
        thread_sandbox: str,
        steward_prompt: str | None,
        client_factory: Callable[..., Any] | None,
        driver_factory: StewardDriverFactory | None = None,
        edge_thread_name: str = "telegram-steward-jsonrpc-client-thread",
    ) -> None:
        self._logger = logger
        self._agent_command = list(agent_command)
        self._request_timeout_sec = request_timeout_sec
        self._rpc_timeout_sec = rpc_timeout_sec
        self._rpc_retries = rpc_retries
        self._approval_policy = approval_policy
        self._thread_approval_policy = thread_approval_policy
        self._thread_sandbox = thread_sandbox
        self._steward_prompt = (steward_prompt or "").strip()
        self._client_factory = client_factory
        self._driver_factory = driver_factory
        self._edge_thread_name = edge_thread_name
        self._handles: dict[AccessPointKey, _InteractiveStewardHandle] = {}
        self._poll_progressed = False

    def ensure_started(self, access_point: AccessPointKey) -> None:
        self._ensure_handle(access_point)

    def submit_request(
        self,
        access_point: AccessPointKey,
        text: str,
        *,
        context_note: str | None = None,
        as_steer: bool = False,
    ) -> None:
        handle = self._ensure_handle(access_point)
        request_text = str(text)
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
            text_preview=str(text).strip()[:200],
        )
        handle.driver.submit_request(
            InteractiveAgentRequest(
                chat_id=access_point.chat_id,
                thread_id=access_point.thread_id,
                prompt=request_text,
                as_steer=as_steer,
            )
        )
        if prepend_prompt:
            handle.steward_prompt_sent = True

    def poll_once(self) -> list[StewardDriverEvent]:
        self._poll_progressed = False
        out: list[StewardDriverEvent] = []
        for access_point, handle in list(self._handles.items()):
            events = handle.driver.poll_once()
            consume_progress = getattr(handle.driver, "consume_poll_progress", None)
            if callable(consume_progress) and consume_progress():
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
        return [
            {
                "role": "steward",
                "agent_id": handle.node_id,
                "state": state,
                "controllable": False,
                "configured_model": "",
                "effective_model": handle.driver.get_actual_thread_model(),
            }
        ]

    def get_item_status_snapshot(self, access_point: AccessPointKey) -> list[dict[str, str]]:
        handle = self._handles.get(access_point)
        if handle is None:
            return []
        return handle.driver.get_item_status_snapshot()

    def get_item_status_snapshot_for_turn(self, access_point: AccessPointKey, turn_id: str) -> list[dict[str, str]]:
        handle = self._handles.get(access_point)
        if handle is None:
            return []
        return handle.driver.get_item_status_snapshot_for_turn(turn_id)

    def get_last_item_apply_info(self, access_point: AccessPointKey) -> dict | None:
        handle = self._handles.get(access_point)
        if handle is None:
            return None
        return handle.driver.get_last_item_apply_info()

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

    def get_thread_metadata(self, access_point: AccessPointKey) -> dict[str, Any]:
        handle = self._handles.get(access_point)
        if handle is None:
            return {}
        getter = getattr(handle.driver, "get_thread_metadata", None)
        if not callable(getter):
            return {}
        raw = getter()
        return dict(raw) if isinstance(raw, dict) else {}

    def get_context_usage(self, access_point: AccessPointKey) -> dict[str, object]:
        handle = self._handles.get(access_point)
        if handle is None:
            return {}
        getter = getattr(handle.driver, "get_context_usage", None)
        raw = getter() if callable(getter) else {}
        return dict(raw) if isinstance(raw, dict) else {}

    def submit_approval_decision(self, access_point: AccessPointKey, decision: str, *, expected_request: ApprovalRequest | None = None) -> None:
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
        handle.driver.submit_approval_decision(decision, expected_request=expected_request)

    def reset(self, access_point: AccessPointKey) -> bool:
        handle = self._handles.pop(access_point, None)
        if handle is None:
            return False
        handle.driver.stop()
        return True

    def close(self) -> None:
        handles = list(self._handles.values())
        self._handles.clear()
        for handle in handles:
            handle.driver.stop()

    def _ensure_handle(self, access_point: AccessPointKey) -> _InteractiveStewardHandle:
        existing = self._handles.get(access_point)
        if existing is not None:
            return existing
        node_id = (
            f"ap-{access_point.type}-"
            f"{access_point.chat_id}-"
            f"{access_point.thread_id if access_point.thread_id is not None else 'main'}"
        )
        driver = (
            self._driver_factory(access_point, self._logger)
            if self._driver_factory is not None
            else CodexInteractiveDriver(
                command=list(self._agent_command),
                logger=self._logger,
                request_timeout_sec=self._request_timeout_sec,
                rpc_timeout_sec=self._rpc_timeout_sec,
                rpc_retries=self._rpc_retries,
                approval_policy=self._approval_policy,
                thread_approval_policy=self._thread_approval_policy,
                thread_sandbox=self._thread_sandbox,
                resume_thread_id=None,
                working_dir=None,
                initial_model=None,
                client_factory=self._client_factory,
                edge_thread_name=self._edge_thread_name,
            )
        )
        driver.start()
        handle = _InteractiveStewardHandle(node_id=node_id, driver=driver)
        self._handles[access_point] = handle
        self._logger.event(
            "access_point_node_spawned",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            node_id=node_id,
        )
        return handle


@dataclass
class _InteractiveAgentBinding:
    node_id: str
    cwd: str
    model: str
    mode: str
    spec: dict[str, Any]
    thread_id: str
    requested_thread_id: str
    driver: InteractiveCodexDriver | None
    runtime_intent: str = RUNTIME_INTENT_BOUND_IDLE
    thread_name: str = ""
    approval_target: str = HUMAN_APPROVAL_TARGET
    effective_model: str = ""
    startup_logged: bool = False
    state: str = "BOUND_IDLE"
    startup_started_at: float | None = None
    startup_deadline: float | None = None
    startup_generation: int = 0
    rollback_binding: _InteractiveAgentBinding | None = None
    pending_request_sequences: list[int] = field(default_factory=list)


class InteractiveAgentRuntime:
    _DEFAULT_STARTUP_TIMEOUT_SEC = 120.0

    def __init__(
        self,
        *,
        logger: LifecycleLogger,
        agent_command: list[str],
        request_timeout_sec: float,
        rpc_timeout_sec: float,
        rpc_retries: int,
        approval_policy: ApprovalPolicy,
        default_thread_approval_policy: str,
        default_thread_sandbox: str,
        client_factory: Callable[..., Any] | None,
        driver_factory: RuntimeDriverFactory | None = None,
        edge_thread_name: str = "telegram-runtime-jsonrpc-client-thread",
        startup_timeout_sec: float = _DEFAULT_STARTUP_TIMEOUT_SEC,
    ) -> None:
        self._logger = logger
        self._agent_command = list(agent_command)
        self._request_timeout_sec = request_timeout_sec
        self._rpc_timeout_sec = rpc_timeout_sec
        self._rpc_retries = rpc_retries
        self._approval_policy = approval_policy
        self._default_thread_approval_policy = default_thread_approval_policy
        self._default_thread_sandbox = default_thread_sandbox
        self._client_factory = client_factory
        self._driver_factory = driver_factory
        self._edge_thread_name = edge_thread_name
        self._startup_timeout_sec = float(startup_timeout_sec)
        if self._startup_timeout_sec <= 0.0:
            raise ValueError("startup_timeout_sec must be positive")
        self._id_seq = 0
        self._startup_generation = 0
        self._next_request_sequence = 1
        self._binding_by_access_point: dict[AccessPointKey, _InteractiveAgentBinding] = {}
        self._address_registry = RoutingIdentityRegistry[AccessPointKey]()
        self._metadata_changes: set[AccessPointKey] = set()
        self._poll_progressed = False

    def _normalize_spec(self, spec: dict[str, Any]) -> dict[str, Any]:
        resolved_spec = dict(spec)
        resolved_spec["cwd"] = str(resolved_spec.get("cwd") or "")
        resolved_spec["model"] = str(resolved_spec.get("model") or "")
        resolved_spec["mode"] = str(resolved_spec.get("mode") or "proxy")
        resolved_spec["approval_target"] = normalize_approval_target(
            resolved_spec.get("approval_target") or HUMAN_APPROVAL_TARGET
        )
        if "thread_id" in resolved_spec and resolved_spec.get("thread_id") is None:
            resolved_spec.pop("thread_id", None)
        return resolved_spec

    def start_agent(self, access_point: AccessPointKey, spec: dict[str, Any]) -> dict[str, Any]:
        resolved_spec = self._normalize_spec(spec)
        existing = self._binding_by_access_point.get(access_point)
        if existing is not None and existing.driver is not None:
            raise RuntimeError("agent is already running for this access point")
        self._id_seq += 1
        self._startup_generation += 1
        node_id = (
            f"agent-{access_point.type}-"
            f"{access_point.chat_id}-"
            f"{access_point.thread_id if access_point.thread_id is not None else 'main'}-"
            f"{self._id_seq}"
        )
        binding = _InteractiveAgentBinding(
            node_id=node_id,
            cwd=str(resolved_spec.get("cwd") or ""),
            model=str(resolved_spec.get("model") or ""),
            mode=str(resolved_spec.get("mode") or "proxy"),
            spec=resolved_spec,
            thread_id=str(resolved_spec.get("thread_id") or ""),
            requested_thread_id=str(resolved_spec.get("thread_id") or ""),
            driver=self._build_driver(access_point=access_point, spec=resolved_spec),
            runtime_intent=RUNTIME_INTENT_RUNNING,
            thread_name=str(resolved_spec.get("thread_name") or "").strip(),
            approval_target=str(resolved_spec.get("approval_target") or HUMAN_APPROVAL_TARGET),
            state="STARTING",
            startup_started_at=clock.monotonic(),
            startup_deadline=clock.monotonic() + self._startup_timeout_sec,
            startup_generation=self._startup_generation,
            rollback_binding=existing,
        )
        self._binding_by_access_point[access_point] = binding
        driver = binding.driver
        try:
            driver.start()
        except Exception:
            if existing is None:
                binding.driver = None
                binding.state = "BOUND_IDLE"
                binding.runtime_intent = RUNTIME_INTENT_BOUND_IDLE
            else:
                existing.runtime_intent = RUNTIME_INTENT_BOUND_IDLE
                self._binding_by_access_point[access_point] = existing
            try:
                driver.stop()
            except Exception:
                pass
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
            startup_timeout_sec=self._startup_timeout_sec,
            startup_generation=binding.startup_generation,
        )
        return self._binding_start_result(binding)

    @staticmethod
    def _binding_start_result(binding: _InteractiveAgentBinding) -> dict[str, Any]:
        return {
            "agent_id": binding.node_id,
            "state": binding.state.lower(),
            "cwd": binding.cwd,
            "model": binding.model,
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
            "cwd": binding.cwd,
            "model": binding.model,
            "configured_model": binding.model,
            "effective_model": binding.effective_model,
            "mode": binding.mode,
            "thread_id": binding.thread_id,
            "thread_name": binding.thread_name,
            "approval_target": binding.approval_target,
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
        binding.spec["approval_target"] = approval_target

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
            binding.runtime_intent = RUNTIME_INTENT_BOUND_IDLE
            return True
        self._transition_to_bound_idle(
            access_point=access_point,
            binding=binding,
            driver=binding.driver,
            use_startup_rollback=True,
            cleanup_event="access_point_agent_stop_cleanup_failed",
        )
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
        return self.start_agent(access_point, dict(binding.spec))

    def restore_binding(
        self,
        access_point: AccessPointKey,
        *,
        spec: dict[str, Any],
        agent_id: str = "",
        thread_id: str = "",
        runtime_intent: str = RUNTIME_INTENT_BOUND_IDLE,
    ) -> dict[str, str]:
        resolved_spec = self._normalize_spec(spec)
        restored_address = str(resolved_spec.pop("address", "") or "").strip()
        restored_routing_mode = str(
            resolved_spec.pop("routing_mode", ROUTING_MODE_SHAMAN) or ROUTING_MODE_SHAMAN
        ).strip()
        if thread_id.strip():
            resolved_spec["thread_id"] = thread_id.strip()
        restored_id = agent_id.strip() or (
            f"restored-agent-{access_point.type}-"
            f"{access_point.chat_id}-"
            f"{access_point.thread_id if access_point.thread_id is not None else 'main'}"
        )
        binding = _InteractiveAgentBinding(
            node_id=restored_id,
            cwd=str(resolved_spec.get("cwd") or ""),
            model=str(resolved_spec.get("model") or ""),
            mode=str(resolved_spec.get("mode") or "proxy"),
            spec=resolved_spec,
            thread_id=str(resolved_spec.get("thread_id") or ""),
            requested_thread_id=str(resolved_spec.get("thread_id") or ""),
            driver=None,
            runtime_intent=normalize_runtime_intent(runtime_intent),
            thread_name=str(resolved_spec.get("thread_name") or "").strip(),
            approval_target=str(resolved_spec.get("approval_target") or HUMAN_APPROVAL_TARGET),
        )
        if restored_address:
            self._address_registry.assign(
                access_point,
                restored_address,
                restored_routing_mode,
            )
        self._binding_by_access_point[access_point] = binding
        self._logger.event(
            "access_point_agent_binding_restored",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            agent_id=binding.node_id,
            cwd=binding.cwd,
            model=binding.model,
            mode=binding.mode,
            restored_thread_id=binding.thread_id,
        )
        return {
            "agent_id": binding.node_id,
            "cwd": binding.cwd,
            "model": binding.model,
            "mode": binding.mode,
            "thread_id": binding.thread_id,
            "thread_name": binding.thread_name,
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
                agent={
                    "agent_id": binding.node_id,
                    "thread_id": binding.thread_id,
                    "thread_name": binding.thread_name,
                    "cwd": binding.cwd,
                    "model": binding.model,
                    "mode": binding.mode,
                    "spec": dict(binding.spec),
                },
            )
        return snapshot

    def reset(self, access_point: AccessPointKey) -> bool:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            return False
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
        self._binding_by_access_point.pop(access_point, None)
        self._metadata_changes.discard(access_point)
        if current_identity is not None:
            self._address_registry.remove(access_point, current_identity.mode)
        if binding.driver is not None:
            binding.driver.stop()
        return True

    def submit_request(
        self,
        access_point: AccessPointKey,
        text: str,
        *,
        context_note: str | None = None,
        as_steer: bool = False,
    ) -> None:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None:
            raise RuntimeError("no running agent is bound to this access point")
        request_sequence = self._next_request_sequence
        self._next_request_sequence += 1
        self._logger.event(
            "runtime_agent_submit_request",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            agent_id=binding.node_id,
            runtime_thread_id=binding.thread_id,
            request_sequence=request_sequence,
            text_preview=str(text).strip()[:200],
        )
        binding.driver.submit_request(
            InteractiveAgentRequest(
                chat_id=access_point.chat_id,
                thread_id=access_point.thread_id,
                prompt=str(text),
                as_steer=as_steer,
            )
        )
        binding.pending_request_sequences.append(request_sequence)

    def submit_approval_decision(self, access_point: AccessPointKey, decision: str, *, expected_request: ApprovalRequest | None = None) -> None:
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
        binding.driver.submit_approval_decision(decision, expected_request=expected_request)

    def submit_approval_decision_now(self, access_point: AccessPointKey, decision: str, *, expected_request: ApprovalRequest | None = None) -> None:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None:
            raise RuntimeError("no running agent is bound to this access point")
        self._logger.event(
            "runtime_agent_submit_approval_decision_now",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            agent_id=binding.node_id,
            runtime_thread_id=binding.thread_id,
            decision=decision,
        )
        submit_now = getattr(binding.driver, "submit_approval_decision_now", None)
        if callable(submit_now):
            submit_now(decision, expected_request=expected_request)
            return
        binding.driver.submit_approval_decision(decision, expected_request=expected_request)

    def poll_once(self) -> list[StewardDriverEvent]:
        self._poll_progressed = False
        out: list[StewardDriverEvent] = []
        for access_point, binding in self._ordered_bindings_for_poll():
            driver = binding.driver
            if driver is None:
                continue
            if binding.state == "STARTING" and self._startup_timed_out(binding):
                event = self._fail_startup(
                    access_point=access_point,
                    binding=binding,
                    driver=driver,
                    error=f"startup timeout after {self._startup_timeout_sec:g}s",
                )
                if event is not None:
                    out.append(event)
                    self._poll_progressed = True
                continue
            has_active_turn = getattr(driver, "has_active_turn", None)
            had_active_turn = bool(has_active_turn()) if callable(has_active_turn) else False
            pending_request_count = getattr(driver, "pending_request_count", None)
            pending_requests_before_poll = (
                int(pending_request_count()) if callable(pending_request_count) else None
            )
            try:
                events = driver.poll_once()
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
                        emitted_events=[],
                    )
                )
                self._poll_progressed = True
                continue
            consume_progress = getattr(driver, "consume_poll_progress", None)
            if callable(consume_progress) and consume_progress():
                self._poll_progressed = True
            has_active_turn_after_poll = bool(has_active_turn()) if callable(has_active_turn) else False
            if pending_requests_before_poll is not None and callable(pending_request_count):
                consumed_requests = max(
                    0,
                    pending_requests_before_poll - int(pending_request_count()),
                )
            else:
                consumed_requests = int(not had_active_turn and has_active_turn_after_poll)
            if consumed_requests:
                del binding.pending_request_sequences[:consumed_requests]
            for event in events:
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
            terminal_error_getter = getattr(driver, "get_terminal_error", None)
            terminal_error = str(terminal_error_getter() or "") if callable(terminal_error_getter) else ""
            if binding.state == "RUNNING" and terminal_error:
                out.extend(
                    self._fail_running_runtime(
                        access_point=access_point,
                        binding=binding,
                        driver=driver,
                        error=terminal_error,
                        had_active_turn=had_active_turn,
                        emitted_events=events,
                    )
                )
                self._poll_progressed = True
                continue
            if binding.state == "STARTING" and driver.is_ready():
                persisted_metadata_changed = False
                actual_model = driver.get_actual_thread_model().strip()
                if actual_model and actual_model != binding.effective_model:
                    binding.effective_model = actual_model
                if actual_model and binding.model and actual_model != binding.model:
                    binding.model = actual_model
                    binding.spec["model"] = actual_model
                    persisted_metadata_changed = True
                actual_thread_id = driver.get_thread_id().strip()
                if actual_thread_id and actual_thread_id != binding.thread_id:
                    binding.thread_id = actual_thread_id
                    binding.spec["thread_id"] = actual_thread_id
                    persisted_metadata_changed = True
                binding.state = "RUNNING"
                binding.runtime_intent = RUNTIME_INTENT_RUNNING
                binding.startup_started_at = None
                binding.startup_deadline = None
                binding.rollback_binding = None
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
                        configured_model=binding.model,
                        effective_model=binding.effective_model,
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
                    effective_model=binding.effective_model,
                    startup_generation=binding.startup_generation,
                )
                self._poll_progressed = True
        return out

    def _ordered_bindings_for_poll(self) -> list[tuple[AccessPointKey, _InteractiveAgentBinding]]:
        bindings = list(self._binding_by_access_point.items())
        eligible_indices: list[int] = []
        eligible_bindings: list[tuple[AccessPointKey, _InteractiveAgentBinding]] = []
        for index, item in enumerate(bindings):
            _access_point, binding = item
            driver = binding.driver
            has_active_turn = getattr(driver, "has_active_turn", None) if driver is not None else None
            if (
                binding.state == "RUNNING"
                and callable(has_active_turn)
                and not has_active_turn()
                and binding.pending_request_sequences
            ):
                eligible_indices.append(index)
                eligible_bindings.append(item)
        eligible_bindings.sort(key=lambda item: item[1].pending_request_sequences[0])
        for index, item in zip(eligible_indices, eligible_bindings):
            bindings[index] = item
        return bindings

    def _fail_running_runtime(
        self,
        *,
        access_point: AccessPointKey,
        binding: _InteractiveAgentBinding,
        driver: InteractiveCodexDriver,
        error: str,
        had_active_turn: bool,
        emitted_events: list[Any],
    ) -> list[StewardDriverEvent]:
        current = self._binding_by_access_point.get(access_point)
        if current is not binding or binding.driver is not driver or binding.state != "RUNNING":
            return []
        failure = str(error).strip() or "runtime driver failed"
        out: list[StewardDriverEvent] = []
        has_turn_failure = any(
            isinstance(event, InteractiveAgentResult)
            and str(event.reply or "").startswith("agent error: ")
            for event in emitted_events
        )
        if had_active_turn and not has_turn_failure:
            out.append(
                StewardDriverEvent(
                    access_point=access_point,
                    source="agent",
                    event=InteractiveAgentResult(
                        chat_id=access_point.chat_id,
                        thread_id=access_point.thread_id,
                        prompt="",
                        reply=f"agent error: {failure}",
                    ),
                )
            )
        failed_binding, cleanup_error = self._transition_to_bound_idle(
            access_point=access_point,
            binding=binding,
            driver=driver,
            use_startup_rollback=False,
            cleanup_event="access_point_agent_runtime_cleanup_failed",
        )
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

    @staticmethod
    def _startup_timed_out(binding: _InteractiveAgentBinding) -> bool:
        deadline = binding.startup_deadline
        return deadline is not None and clock.monotonic() >= deadline

    def _fail_startup(
        self,
        *,
        access_point: AccessPointKey,
        binding: _InteractiveAgentBinding,
        driver: InteractiveCodexDriver,
        error: str,
    ) -> StewardDriverEvent | None:
        current = self._binding_by_access_point.get(access_point)
        if current is not binding or binding.driver is not driver or binding.state != "STARTING":
            self._logger.event(
                "access_point_agent_start_failure_ignored",
                access_point_type=access_point.type,
                chat_id=access_point.chat_id,
                thread_id=access_point.thread_id,
                startup_generation=binding.startup_generation,
                error=str(error),
            )
            return None

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
            startup_generation=binding.startup_generation,
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
        driver: InteractiveCodexDriver,
        use_startup_rollback: bool,
        cleanup_event: str,
    ) -> tuple[_InteractiveAgentBinding, str]:
        binding.pending_request_sequences.clear()
        rollback = binding.rollback_binding if use_startup_rollback else None
        if rollback is not None:
            self._binding_by_access_point[access_point] = rollback
            idle_binding = rollback
        else:
            binding.driver = None
            binding.state = "BOUND_IDLE"
            binding.effective_model = ""
            binding.startup_started_at = None
            binding.startup_deadline = None
            binding.rollback_binding = None
            idle_binding = binding
        idle_binding.runtime_intent = RUNTIME_INTENT_BOUND_IDLE
        cleanup_error = ""
        try:
            driver.stop()
        except Exception as exc:
            cleanup_error = str(exc)
            self._logger.event(
                cleanup_event,
                access_point_type=access_point.type,
                chat_id=access_point.chat_id,
                thread_id=access_point.thread_id,
                startup_generation=binding.startup_generation,
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
            configured_model=binding.model,
            effective_model=binding.effective_model,
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
                "state": state,
                "controllable": state in {"STARTING", "RUNNING"},
                "cwd": binding.cwd,
                "model": binding.model,
                "configured_model": binding.model,
                "effective_model": binding.effective_model,
                "mode": binding.mode,
                "thread_id": binding.thread_id,
            }
        ]

    def get_item_status_snapshot(self, access_point: AccessPointKey) -> list[dict[str, str]]:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None:
            return []
        return binding.driver.get_item_status_snapshot()

    def get_item_status_snapshot_for_turn(self, access_point: AccessPointKey, turn_id: str) -> list[dict[str, str]]:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None:
            return []
        return binding.driver.get_item_status_snapshot_for_turn(turn_id)

    def get_last_item_apply_info(self, access_point: AccessPointKey) -> dict | None:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None:
            return None
        return binding.driver.get_last_item_apply_info()

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

    def get_thread_metadata(self, access_point: AccessPointKey) -> dict[str, Any]:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None:
            return {}
        getter = getattr(binding.driver, "get_thread_metadata", None)
        if not callable(getter):
            return {}
        raw = getter()
        return dict(raw) if isinstance(raw, dict) else {}

    def get_context_usage(self, access_point: AccessPointKey) -> dict[str, object]:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None:
            return {}
        getter = getattr(binding.driver, "get_context_usage", None)
        raw = getter() if callable(getter) else {}
        return dict(raw) if isinstance(raw, dict) else {}

    def close(self) -> None:
        bindings = list(self._binding_by_access_point.values())
        self._binding_by_access_point.clear()
        self._metadata_changes.clear()
        for binding in bindings:
            if binding.driver is not None:
                binding.driver.stop()

    def _build_driver(self, *, access_point: AccessPointKey, spec: dict[str, Any]) -> InteractiveCodexDriver:
        if self._driver_factory is not None:
            return self._driver_factory(access_point, spec, self._logger)

        command = list(self._agent_command)
        raw_args = spec.get("args") or []
        if isinstance(raw_args, list):
            command.extend(str(item) for item in raw_args)
        model = str(spec.get("model") or "").strip()
        thread_approval_policy = str(spec.get("approval_policy") or self._default_thread_approval_policy)
        thread_sandbox = str(spec.get("sandbox") or self._default_thread_sandbox)
        return CodexInteractiveDriver(
            command=command,
            logger=self._logger,
            request_timeout_sec=self._request_timeout_sec,
            rpc_timeout_sec=self._rpc_timeout_sec,
            rpc_retries=self._rpc_retries,
            approval_policy=self._approval_policy,
            thread_approval_policy=thread_approval_policy,
            thread_sandbox=thread_sandbox,
            resume_thread_id=(str(spec.get("thread_id") or "").strip() or None),
            working_dir=str(spec.get("cwd") or ""),
            initial_model=model or None,
            client_factory=self._client_factory,
            edge_thread_name=self._edge_thread_name,
        )


class StopStewardLoop(Exception):
    def __init__(self, code: int, *, drain: bool) -> None:
        super().__init__(code)
        self.code = code
        self.drain = drain


class StewardTickLoop:
    def __init__(
        self,
        *,
        request_timeout_sec: float,
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
    ) -> None:
        self._request_timeout_sec = request_timeout_sec
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

    def tick_once(self) -> bool:
        return self._run_phases(
            (
                self._consume_control_events,
                self._flush_due_status_runtimes,
                self._drain_driver_events,
                self._tick_interactions,
                self._flush_due_status_runtimes,
                self._consume_transport,
                self._drain_pending_inputs,
                self._flush_due_status_runtimes,
            )
        )

    def tick_without_transport(self) -> bool:
        return self._run_phases(
            (
                self._consume_control_events,
                self._flush_due_status_runtimes,
                self._drain_driver_events,
                self._tick_interactions,
                self._flush_due_status_runtimes,
                self._drain_pending_inputs,
                self._flush_due_status_runtimes,
            )
        )

    @staticmethod
    def _run_phases(phases: tuple[Callable[[], bool], ...]) -> bool:
        progressed = False
        for phase in phases:
            progressed = phase() or progressed
        return progressed

    def drain_until_idle(self, *, consume_transport: bool = True) -> int:
        idle_timeout_sec = max(0.5, float(self._request_timeout_sec) + 0.5)
        idle_deadline = clock.monotonic() + idle_timeout_sec
        wall_idle_deadline = self._wall_monotonic_now() + idle_timeout_sec
        while (
            clock.monotonic() < idle_deadline
            and self._wall_monotonic_now() < wall_idle_deadline
        ):
            try:
                progressed = self.tick_once() if consume_transport else self.tick_without_transport()
            except StopStewardLoop as stop:
                return stop.code
            if self._is_idle() and not progressed:
                return 0
            if progressed:
                idle_deadline = clock.monotonic() + idle_timeout_sec
                wall_idle_deadline = self._wall_monotonic_now() + idle_timeout_sec
            self._sleep_fn(self._drain_sleep_sec)
        return 0
