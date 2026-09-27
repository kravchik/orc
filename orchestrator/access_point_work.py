"""Immutable view of work occupying one access point."""

from __future__ import annotations

from dataclasses import dataclass


TRANSIENT_APPROVAL_TARGET_WAIT_REASONS = frozenset(
    {
        "target_busy",
        "target_delegation_busy",
        "target_input_pending",
        "target_waiting_approval",
    }
)


def is_transient_approval_target_wait_reason(reason: str) -> bool:
    return reason in TRANSIENT_APPROVAL_TARGET_WAIT_REASONS


@dataclass(frozen=True)
class AccessPointWorkSnapshot:
    runtime_state: str
    agent_conversation_active: bool
    steward_conversation_active: bool
    agent_turn_active: bool
    steward_turn_active: bool
    human_approval_pending: bool
    approval_delegation_active: bool
    startup_input_pending: bool = False
    routing_conversation_active: bool = False
    local_lifecycle_active: bool = False
    interrupt_active: bool = False

    def resume_block_reason(self) -> str:
        if self.human_approval_pending:
            return "approval_pending"
        if self.approval_delegation_active:
            return "approval_delegation_active"
        if self.local_lifecycle_active:
            return "lifecycle_active"
        if self.interrupt_active:
            return "interrupt_active"
        if self.steward_conversation_active or self.steward_turn_active:
            return "steward_active"
        if self.agent_conversation_active or self.agent_turn_active:
            return "agent_active"
        if self.routing_conversation_active:
            return "routing_active"
        if self.runtime_state in {"STARTING", "RUNNING"}:
            return f"runtime_{self.runtime_state.lower()}"
        if self.runtime_state not in {"UNBOUND", "BOUND_IDLE"}:
            return "runtime_unknown"
        return ""

    def approval_target_wait_reason(self) -> str:
        if self.approval_delegation_active:
            return "target_delegation_busy"
        if self.human_approval_pending:
            return "target_waiting_approval"
        if self.runtime_state != "RUNNING":
            return "target_not_running"
        if self.startup_input_pending:
            return "target_input_pending"
        if self._target_busy:
            return "target_busy"
        return ""

    def routing_target_wait_reason(self) -> str:
        if self.runtime_state != "RUNNING":
            return "target_not_running"
        if self.startup_input_pending:
            return "target_input_pending"
        if self.human_approval_pending:
            return "target_approval_wait"
        if self.approval_delegation_active:
            return "target_delegation_busy"
        if self._target_busy:
            return "target_busy"
        return ""

    @property
    def routing_sender_busy(self) -> bool:
        return (
            self.agent_conversation_active
            or self.steward_conversation_active
            or self.approval_delegation_active
            or self.human_approval_pending
            or self.agent_turn_active
            or self.steward_turn_active
        )

    @property
    def _target_busy(self) -> bool:
        return (
            self.agent_conversation_active
            or self.steward_conversation_active
            or self.agent_turn_active
        )
