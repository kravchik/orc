"""Self-contained asynchronous inspect operations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from orchestrator.agent_identity import InspectionRef
from orchestrator.agent_inspection import (
    AgentRuntimeProjection,
    projection_with_session_error,
)
from orchestrator.interaction_queue import InteractionTickContext


class InspectRuntime(Protocol):
    def inspect_begin(self, interaction: InspectInteraction) -> None: ...
    def inspect_interaction_completed(self, interaction: InspectInteraction) -> None: ...


@dataclass(eq=False)
class InspectInteraction:
    target: object
    requester: object
    command: str
    started: bool = False
    finished: bool = False
    pending: dict[str, InspectionRef] = field(default_factory=dict)
    projections: dict[str, AgentRuntimeProjection] = field(default_factory=dict)

    def expect(self, source: str, inspection_ref: InspectionRef) -> None:
        self.pending[source] = inspection_ref

    def receive(
        self,
        source: str,
        inspection_ref: InspectionRef,
        projection: AgentRuntimeProjection,
    ) -> bool:
        if self.pending.get(source) != inspection_ref:
            return False
        del self.pending[source]
        self.projections[source] = projection
        if not self.pending:
            self.finished = True
        return True

    def fail(self, source: str, error: str) -> bool:
        inspection_ref = self.pending.get(source)
        projection = self.projections.get(source)
        if inspection_ref is None or projection is None:
            return False
        return self.receive(
            source,
            inspection_ref,
            projection_with_session_error(projection, error),
        )

    def complete_without_refresh(self) -> None:
        if not self.pending:
            self.finished = True

    def tick(self, context: InteractionTickContext) -> str | None:
        runtime: InspectRuntime = context.runtime
        if not self.started:
            self.started = True
            runtime.inspect_begin(self)
            context.progressed = True
        return "completed" if self.finished else None

    def on_completed(self, context: InteractionTickContext, outcome: str) -> None:
        if outcome != "completed":
            raise AssertionError(f"unexpected inspect outcome: {outcome}")
        runtime: InspectRuntime = context.runtime
        runtime.inspect_interaction_completed(self)
