from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from orchestrator.access_point_common import AccessPointKey
from orchestrator.binding_address import ResolvedRoutingIdentity
from orchestrator.routing_envelope import (
    HUMAN_ADDRESS,
    RoutingEnvelope,
    RoutingEnvelopeError,
    parse_routing_envelope,
)


@dataclass(frozen=True)
class RoutingDeliverHuman:
    envelope: RoutingEnvelope


@dataclass(frozen=True)
class RoutingDispatch:
    envelope: RoutingEnvelope
    target: ResolvedRoutingIdentity[AccessPointKey]


@dataclass(frozen=True)
class RoutingReject:
    error: RoutingEnvelopeError
    target_address: str
    envelope: RoutingEnvelope | None = None


RoutingDecision = RoutingDeliverHuman | RoutingDispatch | RoutingReject


def decide_routing_response(
    *,
    sender_address: str,
    reply_text: str,
    resolve_target: Callable[[str], ResolvedRoutingIdentity[AccessPointKey] | None],
) -> RoutingDecision:
    try:
        envelope = parse_routing_envelope(reply_text)
    except RoutingEnvelopeError as exc:
        return RoutingReject(error=exc, target_address="unknown")

    if envelope.sender != sender_address:
        return RoutingReject(
            error=RoutingEnvelopeError(
                "from_mismatch",
                f"from_mismatch: expected {sender_address}, got {envelope.sender}",
            ),
            target_address=envelope.target,
            envelope=envelope,
        )
    if envelope.target == HUMAN_ADDRESS:
        return RoutingDeliverHuman(envelope=envelope)

    target = resolve_target(envelope.target)
    if target is None:
        return RoutingReject(
            error=RoutingEnvelopeError("unknown_to", f"unknown_to: {envelope.target}"),
            target_address=envelope.target,
            envelope=envelope,
        )
    return RoutingDispatch(envelope=envelope, target=target)
