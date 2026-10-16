"""Agent question-to-answer interactions owned by the main tick loop."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, ClassVar, Protocol

from orchestrator.access_point_common import AccessPointKey
from orchestrator.agent_identity import TurnRef
from orchestrator.agent_io import AgentInput, AgentInputOrigin
from orchestrator.agent_session import (
    AgentApprovalRequest,
    RawProviderErrorPayload,
    raw_provider_error_lifecycle_fields,
)
from orchestrator.binding_address import ROUTING_MODE_SHAMAN, ResolvedRoutingIdentity
from orchestrator.interactive_driver_events import InteractiveAgentResult
from orchestrator.routing_decision import (
    RoutingDeliverHuman,
    RoutingDispatch,
    RoutingReject,
    decide_routing_response,
)
from orchestrator.routing_envelope import (
    HUMAN_ADDRESS,
    RoutingEnvelope,
    RoutingEnvelopeError,
    format_routing_envelope,
)
from orchestrator.interaction_queue import InteractionTickContext
from orchestrator.routing_interactions import RoutedReplyTo
from orchestrator.steward_runtime_support import PendingInterruptRequest


@dataclass
class RoutedHandoff:
    sender_address: str
    target_address: str
    source_turn_ref: TurnRef
    target_turn_started: bool = False


@dataclass
class RoutingResponseState:
    sender_address: str = ""
    target_address: str = ""
    turn_ref: TurnRef | None = None
    repair_attempt: int = 0


@dataclass(frozen=True)
class PendingSteerInput:
    agent_input: AgentInput
    source_text: str


class AgentConversationRuntime(Protocol):
    def agent_result_suppressed(self, conversation: AgentConversation, preview: str) -> None: ...
    def agent_result_received(self, conversation: AgentConversation, preview: str) -> None: ...
    def agent_turn_error(
        self,
        conversation: AgentConversation,
        error: str,
        raw_provider_error: RawProviderErrorPayload | None,
    ) -> None: ...
    def agent_shaman_address(self, access_point: AccessPointKey) -> str: ...
    def agent_grunt_address(self, access_point: AccessPointKey) -> str: ...
    def agent_resolve_target(self, address: str) -> ResolvedRoutingIdentity | None: ...
    def agent_routing_event(
        self, name: str, access_point: AccessPointKey, *,
        target_access_point: AccessPointKey | None = None, prefix: str = "sender",
        **fields: Any,
    ) -> None: ...
    def agent_escalate_routing_failure(
        self, conversation: AgentConversation, *, sender_address: str,
        target_address: str, error_code: str, details: str, turn_ref: TurnRef,
        target_access_point: AccessPointKey | None = None, target_state: str = "",
    ) -> None: ...
    def agent_deliver_to_human(
        self, conversation: AgentConversation, *, delivery_access_point: AccessPointKey,
        sender_address: str, text: str,
    ) -> None: ...
    def agent_dispatch_routing(
        self, conversation: AgentConversation, *, sender_address: str,
        reply_text: str, decision: RoutingDispatch,
    ) -> None: ...
    def agent_send_reply(self, conversation: AgentConversation, text: str) -> None: ...
    def agent_send_failure(self, conversation: AgentConversation, error: str) -> None: ...
    def agent_submit_request(self, access_point: AccessPointKey, agent_input: AgentInput) -> None: ...
    def agent_send_notice(self, conversation: AgentConversation, text: str) -> None: ...


@dataclass(eq=False)
class AgentConversation:
    """Owns an agent cycle and its routing/interrupt state."""

    MAX_ROUTING_REPAIR_ATTEMPTS: ClassVar[int] = 1
    access_point: AccessPointKey
    phase: str = "initial"
    routing: RoutingResponseState = field(default_factory=RoutingResponseState)
    routed_handoff: RoutedHandoff | None = None
    routed_reply_to: RoutedReplyTo | None = None
    interrupt_notice: PendingInterruptRequest | None = None
    pending_approval_request: AgentApprovalRequest | None = None
    pending_steer_inputs: list[PendingSteerInput] = field(default_factory=list)
    steer_inputs: list[str] = field(default_factory=list)
    finished: bool = False

    @property
    def source(self) -> str:
        return "agent"

    def handle_result(self, runtime: AgentConversationRuntime, result: InteractiveAgentResult) -> None:
        reply_text = "" if result.failed else result.require_output().text
        provider_error = result.rendered_error if result.failed else ""
        preview = (provider_error if result.failed else reply_text).strip()[:200]
        if self.interrupt_notice is not None:
            runtime.agent_result_suppressed(self, preview)
            self.finish()
            return

        runtime.agent_result_received(self, preview)
        turn_ref = result.turn_ref
        if result.failed:
            runtime.agent_turn_error(self, result.error, result.raw_provider_error)
            handoff = self.routed_handoff
            sender = handoff.sender_address if handoff is not None else runtime.agent_shaman_address(self.access_point)
            if sender:
                if handoff is not None:
                    runtime.agent_routing_event(
                        "routing_target_turn_failed", self.access_point,
                        prefix="target",
                        sender_address=handoff.sender_address,
                        target_address=handoff.target_address,
                        source_turn_ref_sequence=handoff.source_turn_ref.sequence,
                        error=result.error,
                        **raw_provider_error_lifecycle_fields(
                            result.raw_provider_error
                        ),
                    )
                self.finish()
                runtime.agent_escalate_routing_failure(
                    self, sender_address=sender,
                    target_address=handoff.target_address if handoff is not None else "unknown",
                    error_code="agent_backend_failure", details=provider_error,
                    turn_ref=handoff.source_turn_ref if handoff is not None else turn_ref,
                )
                return
            self.phase = "waiting_reply_delivery"
            runtime.agent_send_failure(self, provider_error)
            return

        shaman = runtime.agent_shaman_address(self.access_point)
        if shaman:
            self._handle_shaman_result(runtime, reply_text, turn_ref, shaman)
            return
        grunt = runtime.agent_grunt_address(self.access_point)
        if grunt:
            self._handle_grunt_result(runtime, reply_text, turn_ref, grunt)
            return
        if not preview:
            self.finish()
            return
        self.phase = "waiting_reply_delivery"
        runtime.agent_send_reply(self, reply_text)

    def _handle_shaman_result(
        self, runtime: AgentConversationRuntime, reply_text: str, turn_ref: TurnRef, sender: str,
    ) -> None:
        self.routing.turn_ref = turn_ref
        decision = decide_routing_response(
            sender_address=sender,
            reply_text=reply_text,
            resolve_target=runtime.agent_resolve_target,
        )
        envelope = decision.envelope
        if envelope is not None:
            runtime.agent_routing_event(
                "routing_envelope_parsed", self.access_point,
                sender_address=sender,
                envelope_from=envelope.sender,
                target_address=envelope.target,
                turn_ref_sequence=turn_ref.sequence,
                repair_attempt=self.routing.repair_attempt,
            )
        if isinstance(decision, RoutingReject):
            self._reject_routing_result(runtime, sender, decision.target_address, decision.error)
            return
        if isinstance(decision, RoutingDeliverHuman):
            self._deliver_human(runtime, self.access_point, sender, reply_text)
            return
        if not isinstance(decision, RoutingDispatch):
            raise AssertionError(f"unsupported routing decision: {decision!r}")
        self.finish()
        runtime.agent_dispatch_routing(self, sender_address=sender, reply_text=reply_text, decision=decision)

    def _handle_grunt_result(
        self, runtime: AgentConversationRuntime, reply_text: str, turn_ref: TurnRef, sender: str,
    ) -> None:
        reply_to = self.routed_reply_to
        if reply_to is None:
            runtime.agent_routing_event(
                "routing_grunt_reply_target_missing", self.access_point,
                sender_address=sender, turn_ref_sequence=turn_ref.sequence,
            )
            self.finish()
            runtime.agent_escalate_routing_failure(
                self, sender_address=sender, target_address="unknown",
                error_code="reply_target_missing", details="the Grunt operation has no one-hop reply target",
                turn_ref=turn_ref,
            )
            return
        if not reply_text.strip():
            self.finish()
            return

        self.routing.turn_ref = turn_ref
        if reply_to.address == HUMAN_ADDRESS:
            self._deliver_human(runtime, reply_to.access_point, sender, reply_text)
            return

        caller = runtime.agent_shaman_address(reply_to.access_point)
        if not caller:
            self.finish()
            runtime.agent_escalate_routing_failure(
                self, sender_address=sender, target_address=reply_to.address,
                error_code="reply_target_not_shaman",
                details="the original caller no longer has a Shaman routing identity",
                target_access_point=reply_to.access_point,
                turn_ref=turn_ref,
            )
            return

        envelope = RoutingEnvelope(sender=sender, target=caller, body=reply_text)
        synthetic_reply = format_routing_envelope(envelope.sender, envelope.target, envelope.body)
        runtime.agent_routing_event(
            "routing_grunt_reply_envelope_created", self.access_point,
            target_access_point=reply_to.access_point,
            sender_address=sender, target_address=caller,
            original_target_address=reply_to.address, turn_ref_sequence=turn_ref.sequence,
        )
        self.finish()
        runtime.agent_dispatch_routing(
            self, sender_address=sender, reply_text=synthetic_reply,
            decision=RoutingDispatch(
                envelope=envelope,
                target=ResolvedRoutingIdentity(
                    binding=reply_to.access_point, address=caller, mode=ROUTING_MODE_SHAMAN,
                ),
            ),
        )

    def _deliver_human(
        self, runtime: AgentConversationRuntime, delivery_access_point: AccessPointKey,
        sender_address: str, reply_text: str,
    ) -> None:
        self.phase = "waiting_reply_delivery"
        self.routing.sender_address = sender_address
        self.routing.target_address = HUMAN_ADDRESS
        runtime.agent_deliver_to_human(
            self, delivery_access_point=delivery_access_point,
            sender_address=sender_address, text=reply_text,
        )

    def _reject_routing_result(
        self, runtime: AgentConversationRuntime, sender: str,
        target: str, error: RoutingEnvelopeError,
    ) -> None:
        runtime.agent_routing_event(
            "routing_envelope_rejected", self.access_point,
            sender_address=sender, target_address=target,
            error_code=error.code, error=str(error),
            turn_ref_sequence=self.routing_turn_ref().sequence,
            repair_attempt=self.routing.repair_attempt,
        )
        if self.routing.repair_attempt < self.MAX_ROUTING_REPAIR_ATTEMPTS:
            self.routing.repair_attempt += 1
            self.phase = "routing_repair"
            try:
                runtime.agent_submit_request(
                    self.access_point,
                    AgentInput(
                        text=self._build_routing_repair_prompt(sender, error),
                        origin=AgentInputOrigin.ROUTING,
                        verbatim=True,
                    ),
                )
            except Exception as exc:
                self.finish()
                runtime.agent_escalate_routing_failure(
                    self, sender_address=sender, target_address=target,
                    error_code="repair_submit_failed", details=str(exc),
                    turn_ref=self.routing_turn_ref(),
                )
                return
            runtime.agent_send_notice(
                self, f"Routing response from {sender} was malformed. Requested a corrected response.",
            )
            for event in ("routing_repair_notice_queued", "routing_repair_requested"):
                runtime.agent_routing_event(
                    event, self.access_point,
                    sender_address=sender, target_address=target,
                    error_code=error.code,
                    turn_ref_sequence=self.routing_turn_ref().sequence,
                    repair_attempt=self.routing.repair_attempt,
                )
            return

        self.finish()
        runtime.agent_routing_event(
            "routing_repair_exhausted", self.access_point,
            sender_address=sender, target_address=target,
            error_code=error.code,
            turn_ref_sequence=self.routing_turn_ref().sequence,
            repair_attempt=self.routing.repair_attempt,
        )
        runtime.agent_escalate_routing_failure(
            self, sender_address=sender, target_address=target,
            error_code="repair_exhausted", details=self._format_routing_error(error),
            turn_ref=self.routing_turn_ref(),
        )

    def routing_turn_ref(self) -> TurnRef:
        turn_ref = self.routing.turn_ref
        if turn_ref is None:
            raise RuntimeError("routing response has no TurnRef")
        return turn_ref

    @staticmethod
    def _build_routing_repair_prompt(sender: str, error: RoutingEnvelopeError) -> str:
        return (
            "Routing validation failed for your previous final response.\n"
            f"error: {AgentConversation._format_routing_error(error)}\n"
            f"expected FROM: {sender}\n"
            "Return the complete corrected final response in exactly this format:\n"
            f"FROM: {sender}\n"
            "TO: human-or-known-agent-address\n\n"
            "<non-empty message body>"
        )

    @staticmethod
    def _format_routing_error(error: RoutingEnvelopeError) -> str:
        detail = str(error).strip()
        if not detail or detail == error.code:
            return error.code
        if detail.startswith(f"{error.code}:"):
            return detail
        return f"{error.code}: {detail}"

    def queue_steer(
        self,
        agent_input: AgentInput,
        *,
        source_text: str | None = None,
    ) -> bool:
        if self.finished or self.phase != "initial":
            return False
        self.pending_steer_inputs.append(
            PendingSteerInput(
                agent_input=agent_input,
                source_text=agent_input.text if source_text is None else source_text,
            )
        )
        return True

    def discard_steer(self, agent_input: AgentInput) -> bool:
        for index, pending in enumerate(self.pending_steer_inputs):
            if pending.agent_input == agent_input:
                self.pending_steer_inputs.pop(index)
                return True
        return False

    def confirm_steer(self, agent_input: AgentInput) -> bool:
        for index, pending in enumerate(self.pending_steer_inputs):
            if pending.agent_input != agent_input:
                continue
            self.pending_steer_inputs.pop(index)
            self.steer_inputs.append(pending.source_text)
            return True
        return False

    def routed_envelope_for(
        self,
        target_access_point: AccessPointKey,
        envelope: RoutingEnvelope,
    ) -> RoutingEnvelope:
        if target_access_point == self.access_point or not self.steer_inputs:
            return envelope
        entries: list[str] = []
        for raw_text in self.steer_inputs:
            text = str(raw_text or "").strip()
            if not text:
                continue
            indented_text = text.replace("\n", "\n   ")
            entries.append(f"{len(entries) + 1}. {indented_text}")
        if not entries:
            return envelope
        note = "\n".join(
            (
                "[ORC routing note: steer received during the source turn]",
                *entries,
            )
        )
        return RoutingEnvelope(
            sender=envelope.sender,
            target=envelope.target,
            body=f"{envelope.body.rstrip()}\n\n{note}",
        )

    def interrupt(self, submit: Callable[[AccessPointKey], None]) -> None:
        submit(self.access_point)

    def finish(self) -> None:
        self.finished = True
        self.pending_approval_request = None

    def tick(self, context: InteractionTickContext) -> str | None:
        if self.finished and self.interrupt_notice is None:
            return "completed"
        return None

    def on_completed(self, context: InteractionTickContext, outcome: str) -> None:
        if outcome != "completed":
            raise AssertionError(f"unexpected agent conversation outcome: {outcome}")
