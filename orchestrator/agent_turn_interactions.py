"""Provider-neutral turn interactions owned by the core tick loop."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from orchestrator.agent_identity import TurnRef
from orchestrator.agent_session import AgentEffectOutcome, AgentEffectResult
from orchestrator.interaction_queue import InteractionTickContext


class TerminalAcceptance(StrEnum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"


@dataclass(eq=False)
class AgentTurnInteraction:
    """A queued turn from dispatch through one terminal delivery receipt."""

    target: object
    request: object
    turn_ref: TurnRef
    owner: object | None = None
    lane: str = "agent"
    context_note: str | None = None
    phase: str = "queued"
    start_receipt: AgentEffectResult | None = None
    terminal_receipt: object | None = None
    terminal_outcome: str = ""

    @property
    def active(self) -> bool:
        return self.phase == "active" and not self.terminal_outcome

    def receive_terminal(
        self,
        receipt: object,
        *,
        outcome: str = "completed",
    ) -> TerminalAcceptance:
        if self.terminal_outcome:
            return TerminalAcceptance.DUPLICATE
        if not outcome:
            raise ValueError("turn interaction terminal outcome must not be empty")
        self.terminal_receipt = receipt
        self.terminal_outcome = outcome
        self.phase = "terminal"
        return TerminalAcceptance.ACCEPTED

    def reserve(self, context: InteractionTickContext) -> None:
        if not self.terminal_outcome:
            context.active_target_owners.setdefault(self.target, self.owner or self)

    def tick(self, context: InteractionTickContext) -> str | None:
        if self.terminal_outcome:
            return self.terminal_outcome
        runtime = context.runtime
        if self.phase == "queued":
            self.phase = "waiting"
            context.blocked_targets.add(self.target)
            context.progressed = True
            return None
        if self.phase == "active":
            context.blocked_targets.add(self.target)
            context.active_target_owners[self.target] = self.owner or self
            return self.terminal_outcome or None
        if self.phase != "waiting":
            raise AssertionError(f"unknown turn interaction phase: {self.phase!r}")
        active_owner = context.active_target_owners.get(self.target)
        if active_owner is not None and active_owner is not (self.owner or self):
            return None
        if self.target in context.blocked_targets and active_owner is None:
            return None
        context.blocked_targets.add(self.target)
        context.active_target_owners[self.target] = self.owner or self
        wait_reason = runtime.agent_turn_wait_reason(self)
        if self.terminal_outcome:
            return self.terminal_outcome
        if wait_reason:
            return None
        receipt = runtime.agent_turn_start(self)
        if not isinstance(receipt, AgentEffectResult):
            raise TypeError("agent turn start must return AgentEffectResult")
        self.start_receipt = receipt
        if receipt.outcome is AgentEffectOutcome.ACCEPTED:
            self.phase = "active"
            context.progressed = True
            return None
        runtime.agent_turn_start_failed(self, receipt)
        if not self.terminal_outcome:
            self.receive_terminal(receipt, outcome="failed")
        return self.terminal_outcome

    def on_completed(self, context: InteractionTickContext, outcome: str) -> None:
        context.runtime.agent_turn_interaction_completed(self, outcome)


@dataclass(eq=False)
class AgentSteerInteraction:
    """One attempt to join an input to an exact active ORC turn."""

    target: object
    request: object
    expected_turn_ref: TurnRef
    owner: object | None = None
    lane: str = "agent"
    context_note: str | None = None
    receipt: AgentEffectResult | None = None
    cancelled_error: str = ""

    @property
    def is_pending(self) -> bool:
        return self.receipt is None and not self.cancelled_error

    @property
    def outcome(self) -> str | None:
        if self.cancelled_error:
            return "cancelled"
        if self.receipt is None:
            return None
        return self.receipt.outcome.value

    @property
    def accepted(self) -> bool:
        return (
            self.receipt is not None
            and self.receipt.outcome is AgentEffectOutcome.ACCEPTED
        )

    @property
    def requires_fallback(self) -> bool:
        return (
            self.receipt is not None
            and self.receipt.outcome in {
                AgentEffectOutcome.REJECTED,
                AgentEffectOutcome.UNSUPPORTED,
            }
        )

    @property
    def error(self) -> str:
        if self.cancelled_error:
            return self.cancelled_error
        return self.receipt.error if self.receipt is not None else ""

    def reserve(self, context: InteractionTickContext) -> None:
        _ = context

    def cancel(self, error: str) -> None:
        if not self.is_pending:
            return
        normalized = str(error).strip()
        if not normalized:
            raise ValueError("steer cancellation error must not be empty")
        self.cancelled_error = normalized

    def tick(self, context: InteractionTickContext) -> str | None:
        if self.outcome is not None:
            return self.outcome
        wait_reason = context.runtime.agent_steer_wait_reason(self)
        if wait_reason:
            return None
        receipt = context.runtime.agent_steer_submit(self)
        if not isinstance(receipt, AgentEffectResult):
            raise TypeError("agent steer must return AgentEffectResult")
        if receipt.outcome is AgentEffectOutcome.COMPLETED:
            raise ValueError("steer effect must return accepted or a terminal failure receipt")
        self.receipt = receipt
        context.progressed = True
        return receipt.outcome.value

    def on_completed(self, context: InteractionTickContext, outcome: str) -> None:
        if self.outcome != outcome:
            raise AssertionError("completed steer interaction outcome does not match its state")
        context.runtime.agent_steer_interaction_completed(self, outcome)
