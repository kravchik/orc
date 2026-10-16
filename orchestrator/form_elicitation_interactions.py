"""Provider-neutral restricted form elicitation interaction."""

from __future__ import annotations

from dataclasses import dataclass

from orchestrator.access_point_common import AccessPointKey
from orchestrator.agent_session import (
    AgentFormRequest,
    ElicitationAction,
    ElicitationValue,
)


@dataclass
class FormElicitationInteraction:
    access_point: AccessPointKey
    source: str
    request: AgentFormRequest
    action: ElicitationAction | None = None
    content: tuple[tuple[str, ElicitationValue], ...] = ()
    finished: bool = False
    cancelled: bool = False

    @property
    def pending(self) -> bool:
        return not self.finished

    def answer(
        self,
        action: ElicitationAction,
        content: tuple[tuple[str, ElicitationValue], ...] = (),
    ) -> None:
        if self.finished or self.action is not None:
            raise RuntimeError("form elicitation responder is stale")
        self.action = action
        self.content = content

    def cancel(self) -> None:
        self.cancelled = True

    def reserve(self, context: object) -> None:
        return

    def tick(self, context: object) -> str | None:
        runtime = context.runtime
        if self.cancelled:
            return "cancelled"
        if self.action is None:
            return None
        result = runtime.form_elicitation_submit(self)
        if result.delivery_attempted and result.succeeded:
            return self.action.value
        if not result.delivery_attempted:
            runtime.form_elicitation_rejected(self, result.result.error)
            self.action = None
            self.content = ()
            return None
        runtime.form_elicitation_failed(self, result.result.error)
        return "failed"

    def on_completed(self, context: object, outcome: str) -> None:
        self.finished = True
        context.runtime.form_elicitation_completed(self, outcome)
