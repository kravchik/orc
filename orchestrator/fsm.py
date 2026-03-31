"""Minimal finite-state machine for ORC1 PoC step 2."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable


class State(str, Enum):
    HUMAN_THINKS = "HUMAN_THINKS"
    LEAD_THINKS = "LEAD_THINKS"
    WORKER_THINKS = "WORKER_THINKS"
    WORKER_WAITS_APPROVAL = "WORKER_WAITS_APPROVAL"
    HALT = "HALT"


class InvalidTransitionError(RuntimeError):
    pass


@dataclass(frozen=True)
class Transition:
    from_state: State
    to_state: State
    reason: str


class Fsm:
    def __init__(self, logger, initial_state: State = State.LEAD_THINKS) -> None:
        self._logger = logger
        self._state = initial_state

    @property
    def state(self) -> State:
        return self._state

    def transition(self, to_state: State, reason: str) -> Transition:
        from_state = self._state
        if to_state not in self._allowed_targets(from_state):
            self._logger.event(
                "fsm_invalid_transition",
                from_state=from_state.value,
                to_state=to_state.value,
                reason=reason,
            )
            raise InvalidTransitionError(
                f"invalid transition: {from_state.value} -> {to_state.value}"
            )

        self._state = to_state
        self._logger.event(
            "fsm_transition",
            from_state=from_state.value,
            to_state=to_state.value,
            reason=reason,
        )
        return Transition(from_state=from_state, to_state=to_state, reason=reason)

    def _allowed_targets(self, state: State) -> Iterable[State]:
        if state is State.HUMAN_THINKS:
            return (State.LEAD_THINKS, State.WORKER_THINKS, State.HALT)
        if state is State.LEAD_THINKS:
            return (State.WORKER_THINKS, State.HUMAN_THINKS, State.HALT)
        if state is State.WORKER_THINKS:
            return (State.LEAD_THINKS, State.WORKER_WAITS_APPROVAL, State.HALT)
        if state is State.WORKER_WAITS_APPROVAL:
            return (State.WORKER_THINKS, State.HUMAN_THINKS, State.HALT)
        return ()
