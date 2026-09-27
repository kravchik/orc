"""Routed interactions waiting for a target agent turn."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from orchestrator.access_point_common import AccessPointKey
from orchestrator.binding_address import ResolvedRoutingIdentity
from orchestrator.interaction_queue import InteractionTickContext


@dataclass(frozen=True)
class RoutedReplyTo:
    access_point: AccessPointKey
    address: str


@dataclass(frozen=True)
class RoutingFailure:
    code: str
    details: str
    target_state: str


class RoutedConversationRuntime(Protocol):
    def routing_identity(self, address: str) -> ResolvedRoutingIdentity | None: ...
    def routing_request_runtime_start(self, request: RoutedConversation) -> None: ...
    def routing_runtime_state(self, access_point: AccessPointKey) -> str: ...
    def routing_target_wait_reason(self, access_point: AccessPointKey, state: str) -> str: ...
    def routing_sender_busy(self, access_point: AccessPointKey) -> bool: ...
    def routing_defer(self, request: RoutedConversation, *, reason: str, target_state: str = "") -> None: ...
    def routing_fail(self, request: RoutedConversation, failure: RoutingFailure) -> None: ...
    def routing_submit(self, request: RoutedConversation) -> None: ...
    def routing_dispatched(self, request: RoutedConversation) -> None: ...
    def routing_escalate(self, request: RoutedConversation) -> None: ...


@dataclass(eq=False)
class RoutedConversation:
    """Owns the queued phase until an AgentConversation takes the handoff."""

    sequence: int
    sender_access_point: AccessPointKey
    sender_address: str
    sender_mode: str
    target_access_point: AccessPointKey
    target_address: str
    target_mode: str
    target_input: str
    source_turn_id: str
    repair_attempt: int
    routed_reply_to: RoutedReplyTo | None = None
    deferred_notice_sent: bool = False
    last_deferred_reason: str = ""
    failure: RoutingFailure | None = None
    runtime_start_registered: bool = False

    def tick(self, context: InteractionTickContext) -> str | None:
        runtime = context.runtime
        if self.failure is not None:
            return None if runtime.routing_sender_busy(self.sender_access_point) else "failed"

        sender_failure = self._sender_identity_failure(runtime)
        if sender_failure is not None:
            runtime.routing_fail(self, sender_failure)
            return None if runtime.routing_sender_busy(self.sender_access_point) else "failed"

        if self.target_access_point in context.blocked_targets:
            self._register_runtime_start(runtime)
            runtime.routing_defer(self, reason="target_fifo_wait")
            return None

        target_failure = self._target_identity_failure(runtime)
        if target_failure is not None:
            runtime.routing_fail(self, target_failure)
            return None if runtime.routing_sender_busy(self.sender_access_point) else "failed"

        self._register_runtime_start(runtime)
        target_state = runtime.routing_runtime_state(self.target_access_point)
        reason = runtime.routing_target_wait_reason(self.target_access_point, target_state)
        if reason:
            context.blocked_targets.add(self.target_access_point)
            runtime.routing_defer(self, reason=reason, target_state=target_state)
            return None

        runtime.routing_submit(self)
        context.blocked_targets.add(self.target_access_point)
        return "dispatched"

    def _register_runtime_start(self, runtime: RoutedConversationRuntime) -> None:
        if self.runtime_start_registered:
            return
        self.runtime_start_registered = True
        runtime.routing_request_runtime_start(self)

    def on_completed(self, context: InteractionTickContext, outcome: str) -> None:
        if outcome == "dispatched":
            context.runtime.routing_dispatched(self)
        elif outcome == "failed":
            context.runtime.routing_escalate(self)
        else:
            raise AssertionError(f"unexpected routed interaction outcome: {outcome}")

    def _sender_identity_failure(self, runtime: RoutedConversationRuntime) -> RoutingFailure | None:
        target_state = runtime.routing_runtime_state(self.target_access_point)
        identity = runtime.routing_identity(self.sender_address)
        if identity is None:
            return RoutingFailure(
                code="sender_unresolved",
                details="sender routing address is no longer assigned",
                target_state=target_state,
            )
        if identity.binding != self.sender_access_point:
            return RoutingFailure(
                code="sender_reassigned",
                details="sender routing address now belongs to another binding",
                target_state=target_state,
            )
        if identity.mode != self.sender_mode:
            return RoutingFailure(
                code="sender_mode_changed",
                details="sender routing mode changed while the request was queued",
                target_state=target_state,
            )
        return None

    def _target_identity_failure(self, runtime: RoutedConversationRuntime) -> RoutingFailure | None:
        identity = runtime.routing_identity(self.target_address)
        if identity is None:
            return RoutingFailure(
                code="target_unresolved",
                details="target routing address is no longer assigned",
                target_state="UNRESOLVED",
            )
        if identity.binding != self.target_access_point:
            return RoutingFailure(
                code="target_reassigned",
                details="target routing address now belongs to another binding",
                target_state=runtime.routing_runtime_state(identity.binding),
            )
        if identity.mode != self.target_mode:
            return RoutingFailure(
                code="target_mode_changed",
                details="target routing mode changed while the request was queued",
                target_state=runtime.routing_runtime_state(self.target_access_point),
            )
        return None
