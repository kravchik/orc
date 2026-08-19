from __future__ import annotations

from dataclasses import dataclass

from orchestrator.binding_address import normalize_address


HUMAN_ADDRESS = "human"


class RoutingEnvelopeError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class RoutingEnvelope:
    sender: str
    target: str
    body: str


def format_routing_envelope(sender: str, target: str, body: str) -> str:
    canonical_sender = _validate_endpoint(sender, field="from")
    canonical_target = _validate_endpoint(target, field="to")
    _validate_body(body)
    return f"FROM: {canonical_sender}\nTO: {canonical_target}\n\n{body}"


def format_human_routing_envelope(target: str, body: str) -> str:
    return format_routing_envelope(HUMAN_ADDRESS, target, body)


def parse_routing_envelope(text: str) -> RoutingEnvelope:
    if not isinstance(text, str) or not text.startswith("FROM: "):
        raise RoutingEnvelopeError("missing_envelope", "final response must start with FROM")
    lines = text.split("\n", 3)
    if len(lines) < 2 or not lines[1].startswith("TO: "):
        raise RoutingEnvelopeError("missing_envelope", "TO must be the second header")
    if len(lines) < 3 or lines[2] != "":
        raise RoutingEnvelopeError("missing_separator", "headers must be followed by one empty line")
    body = lines[3] if len(lines) == 4 else ""
    _validate_body(body)

    sender = _validate_endpoint(lines[0][len("FROM: ") :], field="from")
    target = _validate_endpoint(lines[1][len("TO: ") :], field="to")
    return RoutingEnvelope(sender=sender, target=target, body=body)


def _validate_endpoint(raw: object, *, field: str) -> str:
    if not isinstance(raw, str) or not raw:
        raise RoutingEnvelopeError(f"invalid_{field}", f"{field.upper()} address is required")
    if raw == HUMAN_ADDRESS:
        return raw
    try:
        canonical = normalize_address(raw)
    except ValueError as exc:
        raise RoutingEnvelopeError(f"invalid_{field}", str(exc)) from exc
    if canonical != raw:
        raise RoutingEnvelopeError(
            f"non_canonical_{field}",
            f"{field.upper()} address must be canonical: {canonical}",
        )
    return canonical


def _validate_body(body: object) -> None:
    if not isinstance(body, str) or not body.strip():
        raise RoutingEnvelopeError("empty_body", "routing envelope body is required")
    if body.startswith("\n"):
        raise RoutingEnvelopeError("extra_separator", "routing envelope must contain exactly one empty line")
