"""Routed interactions waiting for a target agent turn."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from orchestrator.access_point_common import AccessPointKey, AccessPointMessageRef
from orchestrator.agent_identity import TurnRef
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
    def routing_request_runtime_start(self, request: RoutedConversation) -> str: ...
    def routing_runtime_state(self, access_point: AccessPointKey) -> str: ...
    def routing_target_wait_reason(self, access_point: AccessPointKey, state: str) -> str: ...
    def routing_sender_busy(self, access_point: AccessPointKey) -> bool: ...
    def routing_defer(self, request: RoutedConversation, *, reason: str, target_state: str = "") -> None: ...
    def routing_fail(self, request: RoutedConversation, failure: RoutingFailure) -> None: ...
    def routing_submit(self, request: RoutedConversation) -> None: ...
    def routing_dispatched(self, request: RoutedConversation) -> None: ...
    def routing_escalate(self, request: RoutedConversation) -> None: ...
    def routing_notice_queue_initial(self, request: RoutedConversation, *, text: str) -> None: ...
    def routing_notice_queue_edit(
        self,
        request: RoutedConversation,
        *,
        message_ref: AccessPointMessageRef,
        text: str,
        state: str,
    ) -> None: ...
    def routing_notice_sent(self, request: RoutedConversation) -> None: ...
    def routing_notice_updated(self, request: RoutedConversation, *, state: str) -> None: ...
    def routing_notice_update_failed(
        self,
        request: RoutedConversation,
        exc: Exception,
        *,
        phase: str,
        state: str,
    ) -> None: ...


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
    source_turn_ref: TurnRef
    repair_attempt: int
    routed_reply_to: RoutedReplyTo | None = None
    deferred_notice_sent: bool = False
    last_deferred_reason: str = ""
    failure: RoutingFailure | None = None
    runtime_start_registered: bool = False
    dispatched: bool = False
    pending_copy_delivery_receipts: list[dict[str, object]] = field(default_factory=list)
    notice_message_ref: AccessPointMessageRef | None = None
    notice_state: str = ""
    notice_text: str = ""
    notice_edit_queued: bool = False
    notice_delivery_failed: bool = False

    def start_notice(self, runtime: RoutedConversationRuntime, text: str) -> None:
        if self.deferred_notice_sent:
            return
        self.deferred_notice_sent = True
        self.notice_state = "queued"
        self.notice_text = text
        runtime.routing_notice_queue_initial(self, text=text)

    def set_notice_state(
        self,
        runtime: RoutedConversationRuntime,
        *,
        state: str,
        text: str,
    ) -> None:
        if not self.deferred_notice_sent or self.notice_delivery_failed:
            return
        if state not in {"delivered", "failed"}:
            raise ValueError(f"invalid routing notice terminal state: {state!r}")
        self.notice_state = state
        self.notice_text = text
        self._advance_notice(runtime)

    def notice_sent(
        self,
        runtime: RoutedConversationRuntime,
        message_ref: AccessPointMessageRef,
    ) -> None:
        if self.notice_delivery_failed or self.notice_message_ref is not None:
            return
        if message_ref.access_point != self.sender_access_point:
            self.notice_send_failed(
                runtime,
                RuntimeError("routing notice returned a reference for another access point"),
            )
            return
        self.notice_message_ref = message_ref
        runtime.routing_notice_sent(self)
        self._advance_notice(runtime)

    def notice_send_failed(
        self,
        runtime: RoutedConversationRuntime,
        exc: Exception,
    ) -> None:
        if self.notice_delivery_failed or self.notice_message_ref is not None:
            return
        self.notice_delivery_failed = True
        runtime.routing_notice_update_failed(
            self,
            exc,
            phase="send",
            state=self.notice_state or "queued",
        )

    def notice_edited(
        self,
        runtime: RoutedConversationRuntime,
        *,
        state: str,
        text: str,
    ) -> None:
        if self.notice_delivery_failed or not self.notice_edit_queued:
            return
        self.notice_edit_queued = False
        if state == self.notice_state and text == self.notice_text:
            runtime.routing_notice_updated(self, state=state)
            return
        self._advance_notice(runtime)

    def notice_edit_failed(
        self,
        runtime: RoutedConversationRuntime,
        exc: Exception,
        *,
        state: str,
        text: str,
    ) -> None:
        if self.notice_delivery_failed or not self.notice_edit_queued:
            return
        self.notice_edit_queued = False
        self.notice_delivery_failed = True
        runtime.routing_notice_update_failed(
            self,
            exc,
            phase="edit",
            state=state,
        )

    def _advance_notice(self, runtime: RoutedConversationRuntime) -> None:
        if (
            self.notice_delivery_failed
            or self.notice_edit_queued
            or self.notice_message_ref is None
            or self.notice_state not in {"delivered", "failed"}
        ):
            return
        self.notice_edit_queued = True
        runtime.routing_notice_queue_edit(
            self,
            message_ref=self.notice_message_ref,
            text=self.notice_text,
            state=self.notice_state,
        )

    def reserve(self, context: InteractionTickContext) -> None:
        if self.failure is None:
            context.fifo_target_owners.setdefault(self.target_access_point, self)

    def tick(self, context: InteractionTickContext) -> str | None:
        runtime = context.runtime
        if self.failure is not None:
            return None if runtime.routing_sender_busy(self.sender_access_point) else "failed"

        sender_failure = self._sender_identity_failure(runtime)
        if sender_failure is not None:
            runtime.routing_fail(self, sender_failure)
            return None if runtime.routing_sender_busy(self.sender_access_point) else "failed"

        target_failure = self._target_identity_failure(runtime)
        if target_failure is not None:
            runtime.routing_fail(self, target_failure)
            return None if runtime.routing_sender_busy(self.sender_access_point) else "failed"

        self._register_runtime_start(runtime)
        target_state = runtime.routing_runtime_state(self.target_access_point)
        if (
            context.fifo_target_owners.get(self.target_access_point) is not self
        ):
            runtime.routing_defer(self, reason="target_fifo_wait")
            return None
        reason = runtime.routing_target_wait_reason(self.target_access_point, target_state)
        if reason:
            context.blocked_targets.add(self.target_access_point)
            runtime.routing_defer(
                self,
                reason=(
                    "target_fifo_wait"
                    if self.last_deferred_reason == "target_fifo_wait"
                    else reason
                ),
                target_state=target_state,
            )
            return None

        if self.target_access_point in context.blocked_targets:
            runtime.routing_defer(self, reason="target_fifo_wait")
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
