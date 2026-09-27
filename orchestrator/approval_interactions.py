"""Request-scoped approval children of agent and Steward conversations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, Protocol

from orchestrator.access_point_common import AccessPointKey
from orchestrator.access_point_work import is_transient_approval_target_wait_reason
from orchestrator.agent_interactions import AgentConversation
from orchestrator.approval import ApprovalRequest
from orchestrator.approval_delegation import build_approval_delegation_prompt, parse_approval_delegation_decision
from orchestrator.approval_target import HUMAN_APPROVAL_TARGET
from orchestrator.binding_address import ResolvedRoutingIdentity
from orchestrator.interaction_queue import InteractionTickContext
from orchestrator.steward_interactions import StewardConversation


class ApprovalRuntime(Protocol):
    def approval_event(self, name: str, **fields: Any) -> None: ...
    def approval_register_human(self, approval: ApprovalInteraction) -> None: ...
    def approval_notify(self, access_point: AccessPointKey, text: str) -> None: ...
    def approval_submit_source(self, approval: ApprovalInteraction, decision: str) -> None: ...
    def approval_cancel_target(self, approval: ApprovalInteraction) -> None: ...
    def approval_decline_target_request(self, approval: ApprovalInteraction, request: ApprovalRequest) -> None: ...
    def approval_human_decision(self, approval: ApprovalInteraction, decision: str, via: str) -> None: ...
    def approval_invalid_human_input(self, approval: ApprovalInteraction) -> None: ...
    def approval_human_delivery_failed(self, approval: ApprovalInteraction, exc: Exception) -> None: ...
    def approval_target(self, approval: ApprovalInteraction) -> str: ...
    def approval_source_address(self, approval: ApprovalInteraction) -> str: ...
    def approval_resolve_target(self, address: str) -> ResolvedRoutingIdentity | None: ...
    def approval_target_wait_reason(self, target: AccessPointKey) -> str: ...
    def approval_target_runtime_state(self, target: AccessPointKey) -> str: ...
    def approval_request_runtime_start(self, approval: ApprovalInteraction) -> str: ...
    def approval_submit_target(self, approval: ApprovalInteraction, prompt: str) -> None: ...
    def approval_unavailable(self, approval: ApprovalInteraction, reason: str, error: str = "") -> None: ...
    def approval_started(self, approval: ApprovalInteraction, source_label: str) -> None: ...


@dataclass(eq=False)
class ApprovalInteraction:
    MAX_TARGET_RETRIES: ClassVar[int] = 1
    parent: AgentConversation | StewardConversation
    request: ApprovalRequest
    human_prompt: Any | None = None
    approval_id: str = ""
    source_address: str = ""
    approval_target: str = ""
    target_address: str = ""
    target_access_point: AccessPointKey | None = None
    phase: str = "new"
    source_cancelled: bool = False
    target_retry_count: int = 0
    target_cancel_requested: bool = False
    last_deferred_reason: str = ""
    deferred_notice_sent: bool = False

    @property
    def source_access_point(self) -> AccessPointKey:
        return self.parent.access_point

    @property
    def source(self) -> str:
        return self.parent.source

    @property
    def access_point(self) -> AccessPointKey:
        return self.target_access_point or self.source_access_point

    def belongs_to(
        self,
        parent: AgentConversation | StewardConversation,
        request: ApprovalRequest | None,
    ) -> bool:
        return self.parent is parent and self.request is request

    @property
    def waiting_target(self) -> str:
        if self.phase == "human":
            return HUMAN_APPROVAL_TARGET
        return self.target_address or self.approval_target or HUMAN_APPROVAL_TARGET

    @property
    def pending(self) -> bool:
        return self.phase in {"target_starting", "target_waiting", "human", "delegated"}

    def ask_human(self, runtime: ApprovalRuntime) -> None:
        self.phase = "human"
        runtime.approval_register_human(self)

    def start(self, runtime: ApprovalRuntime) -> None:
        if isinstance(self.parent, StewardConversation):
            self.approval_target = HUMAN_APPROVAL_TARGET
            self.ask_human(runtime)
            return
        try:
            if not self.approval_target:
                self.approval_target = runtime.approval_target(self)
        except Exception as exc:
            self.source_address = runtime.approval_source_address(self)
            runtime.approval_unavailable(self, "invalid_configuration", str(exc))
            self.ask_human(runtime)
            return
        target_address = self.approval_target
        if target_address == HUMAN_APPROVAL_TARGET:
            self.ask_human(runtime)
            return
        self.source_address = runtime.approval_source_address(self)
        target_identity = runtime.approval_resolve_target(target_address)
        target = target_identity.binding if target_identity is not None else None
        self.target_address = target_identity.address if target_identity is not None else target_address
        if target is None:
            reason = "target_unresolved"
        elif target == self.source_access_point:
            reason = "self_target"
        else:
            reason = runtime.approval_target_wait_reason(target)
        self.target_access_point = target
        thread = self.source_access_point.thread_id
        self.approval_id = (
            f"approval:{self.source_access_point.type}:{self.source_access_point.chat_id}:"
            f"{thread if thread is not None else 'main'}:{self.request.req_id}"
        )
        if reason == "target_not_running" and target is not None:
            start_decision = runtime.approval_request_runtime_start(self)
            if start_decision in {"deferred", "starting"}:
                self.phase = "target_starting"
                runtime.approval_event(
                    "approval_delegation_waiting_for_startup",
                    approval_id=self.approval_id,
                    source_address=self.source_address,
                    target_address=self.target_address,
                    start_decision=start_decision,
                )
                return
            if start_decision == "running":
                reason = runtime.approval_target_wait_reason(target)
        if is_transient_approval_target_wait_reason(reason):
            self._defer_for_target(runtime, reason)
            return
        if reason:
            runtime.approval_unavailable(self, reason)
            self.ask_human(runtime)
            return
        self._submit_to_target(runtime)

    def _defer_for_target(self, runtime: ApprovalRuntime, reason: str) -> None:
        self.phase = "target_waiting"
        if self.last_deferred_reason != reason:
            self.last_deferred_reason = reason
            runtime.approval_event(
                "approval_delegation_deferred",
                approval_id=self.approval_id,
                source_address=self.source_address,
                target_address=self.target_address,
                reason=reason,
            )
        if self.deferred_notice_sent:
            return
        self.deferred_notice_sent = True
        runtime.approval_notify(
            self.source_access_point,
            f"approval delegation to {self.target_address} queued ({reason}).",
        )

    def _dispatch_or_wait(self, runtime: ApprovalRuntime) -> None:
        reason = runtime.approval_target_wait_reason(self.target_access_point)
        if is_transient_approval_target_wait_reason(reason):
            self._defer_for_target(runtime, reason)
            return
        if reason:
            runtime.approval_unavailable(self, reason)
            self.ask_human(runtime)
            return
        self._submit_to_target(runtime)

    def _submit_to_target(self, runtime: ApprovalRuntime) -> None:
        source_label = self.source_address or "unaddressed agent"
        prompt = self._delegation_prompt()
        try:
            runtime.approval_submit_target(self, prompt)
        except Exception as exc:
            runtime.approval_event(
                "approval_delegation_submit_failed", source_label=source_label,
                target_address=self.target_address, approval_id=self.approval_id,
                error=str(exc), error_type=type(exc).__name__,
            )
            runtime.approval_notify(
                self.source_access_point,
                f"approval delegation to {self.target_address} unavailable (submit_failed); asking human.",
            )
            self.ask_human(runtime)
            return
        self.phase = "delegated"
        runtime.approval_started(self, source_label)

    def on_human_decision(self, runtime: ApprovalRuntime, decision: str, via: str) -> None:
        if self.phase != "human":
            return
        runtime.approval_human_decision(self, decision, via)
        self.finish()

    def on_invalid_human_input(self, runtime: ApprovalRuntime) -> None:
        if self.phase == "human":
            runtime.approval_invalid_human_input(self)

    def on_human_delivery_failed(self, runtime: ApprovalRuntime, exc: Exception) -> None:
        if self.phase != "human":
            return
        runtime.approval_human_delivery_failed(self, exc)
        self.finish()

    def on_target_approval(self, runtime: ApprovalRuntime, request: ApprovalRequest) -> None:
        runtime.approval_decline_target_request(self, request)
        if self.source_cancelled:
            runtime.approval_event(
                "approval_delegation_cancelled_target_approval_declined",
                approval_id=self.approval_id, source_address=self.source_address,
                target_address=self.target_address,
            )
            self.finish()
        else:
            self.fallback(runtime, "target_requested_approval")

    def on_target_result(self, runtime: ApprovalRuntime, reply_text: str) -> None:
        if self.source_cancelled:
            runtime.approval_event(
                "approval_delegation_cancelled_target_result_ignored",
                approval_id=self.approval_id, source_address=self.source_address,
                target_address=self.target_address,
            )
            self.finish()
            return
        try:
            parsed = parse_approval_delegation_decision(reply_text, expected_approval_id=self.approval_id)
        except ValueError as exc:
            runtime.approval_event(
                "approval_delegation_invalid_response", approval_id=self.approval_id,
                source_address=self.source_address, target_address=self.target_address, error=str(exc),
            )
            if self.target_retry_count >= self.MAX_TARGET_RETRIES:
                self.fallback(runtime, "invalid_target_response")
            else:
                self._retry_invalid_target_result(runtime, str(exc))
            return
        runtime.approval_event(
            "approval_delegation_decided", approval_id=self.approval_id,
            source_address=self.source_address, target_address=self.target_address,
            decision=parsed.decision,
        )
        try:
            runtime.approval_submit_source(self, parsed.decision)
        except Exception as exc:
            runtime.approval_event(
                "approval_source_decision_submit_failed", approval_id=self.approval_id,
                decision=parsed.decision, source_address=self.source_address,
                target_address=self.target_address, error=str(exc), error_type=type(exc).__name__,
            )
            runtime.approval_notify(
                self.target_access_point,
                "approval decision delivery to "
                f"{self.source_address or 'unaddressed agent'} failed; source is asking human.",
            )
            runtime.approval_notify(
                self.source_access_point,
                "approval decision could not be submitted to the source turn; asking human.",
            )
            self.ask_human(runtime)
            return
        runtime.approval_event(
            "approval_source_decision_submitted", approval_id=self.approval_id,
            decision=parsed.decision, source_address=self.source_address,
            target_address=self.target_address,
        )
        runtime.approval_notify(
            self.target_access_point,
            "approval decision\n"
            f"from: {self.target_address}\n"
            f"to: {self.source_address or 'unaddressed agent'}\n"
            f"decision: {parsed.decision}",
        )
        word = "accepted" if parsed.decision == "accept" else "declined"
        runtime.approval_notify(self.source_access_point, f"approval {word} by {self.target_address}.")
        self.finish()

    def _delegation_prompt(self) -> str:
        return build_approval_delegation_prompt(
            approval_id=self.approval_id,
            source_label=self.source_address or "unaddressed agent",
            target_address=self.target_address,
            request=self.request,
        )

    def _retry_invalid_target_result(self, runtime: ApprovalRuntime, error: str) -> None:
        attempt = self.target_retry_count + 1
        prompt = (
            f"Previous approval decision was invalid: {error}.\n"
            f"Retry {attempt}/{self.MAX_TARGET_RETRIES}. Do not run tools; "
            "return only the JSON decision for the same approval_id.\n\n"
            f"{self._delegation_prompt()}"
        )
        try:
            runtime.approval_submit_target(self, prompt)
        except Exception as exc:
            runtime.approval_event(
                "approval_delegation_retry_submit_failed", approval_id=self.approval_id,
                attempt=attempt, error=str(exc), error_type=type(exc).__name__,
            )
            self.fallback(runtime, "retry_submit_failed")
            return
        self.target_retry_count = attempt
        runtime.approval_event(
            "approval_delegation_retry", approval_id=self.approval_id,
            source_address=self.source_address, target_address=self.target_address,
            attempt=attempt, max_attempts=self.MAX_TARGET_RETRIES, error=error,
        )
        runtime.approval_notify(
            self.target_access_point,
            f"approval decision invalid ({error}); retrying {attempt}/{self.MAX_TARGET_RETRIES}.",
        )
        runtime.approval_notify(
            self.source_access_point,
            f"approval decision from {self.target_address} malformed; "
            f"requesting retry {attempt}/{self.MAX_TARGET_RETRIES}.",
        )

    def on_target_interrupted(self, runtime: ApprovalRuntime) -> None:
        if self.source_cancelled:
            runtime.approval_event(
                "approval_delegation_cancelled_target_interrupted", approval_id=self.approval_id,
                source_address=self.source_address, target_address=self.target_address,
            )
            self.finish()
        else:
            self.fallback(runtime, "target_interrupted")

    def cancel_source(self, runtime: ApprovalRuntime, reason: str) -> None:
        if self.source_cancelled or self.phase == "done":
            return
        was_delegated = self.phase == "delegated"
        self.source_cancelled = True
        runtime.approval_event(
            "approval_delegation_cancelled", approval_id=self.approval_id,
            source_address=self.source_address, target_address=self.target_address, reason=reason,
        )
        if not was_delegated:
            self.finish()
            return
        runtime.approval_notify(
            self.target_access_point,
            "approval delegation from "
            f"{self.source_address or 'unaddressed agent'} cancelled ({reason}).",
        )
        runtime.approval_cancel_target(self)

    def fallback(self, runtime: ApprovalRuntime, reason: str) -> None:
        runtime.approval_event(
            "approval_delegation_fallback", approval_id=self.approval_id,
            source_address=self.source_address, target_address=self.target_address,
            reason=reason, fallback_target="human",
        )
        runtime.approval_notify(
            self.target_access_point,
            "approval delegation from "
            f"{self.source_address or 'unaddressed agent'} cancelled "
            f"({reason}); source is asking human.",
        )
        runtime.approval_notify(
            self.source_access_point,
            f"approval delegation to {self.target_address} unavailable ({reason}); asking human.",
        )
        self.ask_human(runtime)

    def finish(self) -> None:
        if self.parent.pending_approval_request is self.request:
            self.parent.pending_approval_request = None
        self.phase = "done"
        self.human_prompt = None

    def _target_identity_is_current(self, runtime: ApprovalRuntime) -> bool:
        target_identity = runtime.approval_resolve_target(self.approval_target)
        return bool(
            target_identity is not None
            and target_identity.binding == self.target_access_point
            and target_identity.address == self.target_address
        )

    def tick(self, context: InteractionTickContext) -> str | None:
        if self.phase in {"target_starting", "target_waiting"}:
            if not self._target_identity_is_current(context.runtime):
                context.runtime.approval_unavailable(self, "target_unresolved")
                self.ask_human(context.runtime)
            else:
                state = context.runtime.approval_target_runtime_state(
                    self.target_access_point
                )
                if (
                    self.phase == "target_starting"
                    and state not in {"STARTING", "RUNNING"}
                ):
                    context.runtime.approval_unavailable(self, "target_startup_failed")
                    self.ask_human(context.runtime)
                elif self.target_access_point in context.blocked_targets:
                    waiting_for_startup = self.phase == "target_starting"
                    self._defer_for_target(context.runtime, "target_fifo_wait")
                    if waiting_for_startup:
                        self.phase = "target_starting"
                elif self.phase == "target_starting":
                    if state == "RUNNING":
                        self._dispatch_or_wait(context.runtime)
                elif state == "STARTING":
                    self.phase = "target_starting"
                elif state != "RUNNING":
                    context.runtime.approval_unavailable(self, "target_not_running")
                    self.ask_human(context.runtime)
                else:
                    self._dispatch_or_wait(context.runtime)
            if self.phase in {"target_starting", "target_waiting", "delegated"}:
                context.blocked_targets.add(self.target_access_point)
        if (
            self.source_cancelled
            and self.phase == "delegated"
            and not self.target_cancel_requested
        ):
            context.runtime.approval_cancel_target(self)
        return "completed" if self.phase == "done" else None

    def on_completed(self, context: InteractionTickContext, outcome: str) -> None:
        if outcome != "completed":
            raise AssertionError(f"unexpected approval outcome: {outcome}")
