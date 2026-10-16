"""Provider-neutral agent session effects, results, events, and port."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterator
from dataclasses import dataclass
from enum import StrEnum
import json
import math
from typing import Collection, Generic, Mapping, Protocol, TypeAlias, TypeVar

from orchestrator.agent_capabilities import (
    AgentCapability,
    AgentCapabilitySnapshot,
)
from orchestrator.agent_identity import (
    ApprovalRef,
    ElicitationRef,
    InspectionRef,
    ProviderSessionRef,
    TurnRef,
    WorkStatusRef,
)
from orchestrator.agent_inspection import AgentRuntimeProjection, AgentWorkSnapshot
from orchestrator.agent_io import AgentInput, AgentInputOrigin, AgentOutput


@dataclass(frozen=True)
class RawProviderErrorPayload:
    """Immutable exact JSON object supplied by a provider terminal."""

    _canonical_json: str

    def __post_init__(self) -> None:
        if not isinstance(self._canonical_json, str):
            raise ValueError("raw provider error payload must be canonical JSON")
        try:
            decoded = json.loads(self._canonical_json)
            canonical = _canonical_provider_error_json(decoded)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("raw provider error payload must be a JSON object") from exc
        if canonical != self._canonical_json:
            raise ValueError("raw provider error payload must use canonical JSON")

    @classmethod
    def from_json_object(cls, value: object) -> RawProviderErrorPayload:
        return cls(_canonical_provider_error_json(value))

    def to_json_object(self) -> dict[str, object]:
        decoded = json.loads(self._canonical_json)
        if not isinstance(decoded, dict):
            raise AssertionError("validated provider error payload lost object shape")
        return decoded

    def render(self) -> str:
        return self._canonical_json


def _canonical_provider_error_json(value: object) -> str:
    if not isinstance(value, dict):
        raise ValueError("raw provider error payload must be a JSON object")
    try:
        _validate_provider_error_json_value(value)
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        decoded = json.loads(encoded)
    except (TypeError, ValueError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("raw provider error payload contains a non-JSON value") from exc
    if not isinstance(decoded, dict):
        raise ValueError("raw provider error payload must be a JSON object")
    return encoded


def _validate_provider_error_json_value(value: object) -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("raw provider error payload contains a non-finite number")
        return
    if isinstance(value, list):
        for item in value:
            _validate_provider_error_json_value(item)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("raw provider error payload keys must be strings")
            _validate_provider_error_json_value(item)
        return
    raise ValueError("raw provider error payload contains a non-JSON value")


@dataclass(frozen=True)
class StartTurn:
    turn_ref: TurnRef
    agent_input: AgentInput


@dataclass(frozen=True)
class TrySteerTurn:
    expected_turn_ref: TurnRef
    agent_input: AgentInput


@dataclass(frozen=True)
class InterruptTurn:
    turn_ref: TurnRef


class ApprovalDecisionOrigin(StrEnum):
    HUMAN = "human"
    POLICY_AUTO = "policy_auto"


@dataclass(frozen=True)
class ResolveApproval:
    turn_ref: TurnRef
    approval_ref: ApprovalRef
    decision: str
    origin: ApprovalDecisionOrigin = ApprovalDecisionOrigin.HUMAN
    decision_source: str = "interactive"

    def __post_init__(self) -> None:
        if self.approval_ref.turn != self.turn_ref:
            raise ValueError("approval ref must belong to the resolved turn")
        if not isinstance(self.decision, str) or not self.decision.strip():
            raise ValueError("approval decision must be a non-empty string")
        if not isinstance(self.origin, ApprovalDecisionOrigin):
            raise ValueError("approval decision origin must be ApprovalDecisionOrigin")
        if not isinstance(self.decision_source, str) or not self.decision_source.strip():
            raise ValueError("approval decision source must be a non-empty string")
        object.__setattr__(self, "decision", self.decision.strip())
        object.__setattr__(self, "decision_source", self.decision_source.strip())


class ElicitationAction(StrEnum):
    ACCEPT = "accept"
    DECLINE = "decline"
    CANCEL = "cancel"


ElicitationValue: TypeAlias = str | bool | int | float | tuple[str, ...]


@dataclass(frozen=True)
class AgentFormField:
    name: str
    value_type: str
    required: bool = False
    title: str = ""
    description: str = ""
    enum: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in ("name", "value_type", "title", "description"):
            value = getattr(self, name)
            if not isinstance(value, str):
                raise ValueError(f"form field {name} must be a string")
            object.__setattr__(self, name, value.strip())
        if not self.name or len(self.name) > 128:
            raise ValueError("form field name must be 1..128 characters")
        if self.value_type not in {
            "string",
            "boolean",
            "integer",
            "number",
            "string_array",
        }:
            raise ValueError("form field type is unsupported")
        if len(self.title) > 256 or len(self.description) > 2048:
            raise ValueError("form field presentation is too large")
        if not isinstance(self.required, bool):
            raise ValueError("form field required flag must be boolean")
        if len(self.enum) > 32:
            raise ValueError("form field enum must contain at most 32 values")
        _validate_bounded_strings("form field enum", self.enum, 512)
        if self.enum and self.value_type not in {"string", "string_array"}:
            raise ValueError("form field enum requires string values")


@dataclass(frozen=True)
class AgentFormRequest:
    turn_ref: TurnRef
    elicitation_ref: ElicitationRef
    message: str
    fields: tuple[AgentFormField, ...]

    def __post_init__(self) -> None:
        if self.elicitation_ref.turn != self.turn_ref:
            raise ValueError("elicitation ref must belong to its turn")
        message = str(self.message or "").strip()
        if not message or len(message) > 2048:
            raise ValueError("form message must be 1..2048 characters")
        object.__setattr__(self, "message", message)
        if not isinstance(self.fields, tuple) or not self.fields or len(self.fields) > 16:
            raise ValueError("form request must contain 1..16 fields")
        if any(not isinstance(field, AgentFormField) for field in self.fields):
            raise ValueError("form request fields must be AgentFormField values")
        names = tuple(field.name for field in self.fields)
        if len(names) != len(set(names)):
            raise ValueError("form request field names must be unique")

    def validate_content(
        self,
        content: Mapping[str, object],
    ) -> tuple[tuple[str, ElicitationValue], ...]:
        if not isinstance(content, Mapping):
            raise ValueError("form response content must be an object")
        fields = {field.name: field for field in self.fields}
        if any(not isinstance(key, str) or key not in fields for key in content):
            raise ValueError("form response contains an unknown field")
        if any(field.required and field.name not in content for field in self.fields):
            raise ValueError("form response is missing required fields")
        return tuple(
            (field.name, _validate_form_value(field, content[field.name]))
            for field in self.fields
            if field.name in content
        )


@dataclass(frozen=True)
class ResolveElicitation:
    turn_ref: TurnRef
    elicitation_ref: ElicitationRef
    action: ElicitationAction
    content: tuple[tuple[str, ElicitationValue], ...] = ()

    def __post_init__(self) -> None:
        if self.elicitation_ref.turn != self.turn_ref:
            raise ValueError("elicitation ref must belong to the resolved turn")
        if not isinstance(self.action, ElicitationAction):
            raise ValueError("elicitation action must be ElicitationAction")
        if self.action is not ElicitationAction.ACCEPT and self.content:
            raise ValueError("declined or cancelled elicitation must not contain content")


@dataclass(frozen=True)
class SetModel:
    model: str

    def __post_init__(self) -> None:
        if not isinstance(self.model, str) or not self.model.strip():
            raise ValueError("model must be a non-empty string")
        object.__setattr__(self, "model", self.model.strip())


@dataclass(frozen=True)
class SetEffort:
    effort: str

    def __post_init__(self) -> None:
        if not isinstance(self.effort, str) or not self.effort.strip():
            raise ValueError("effort must be a non-empty string")
        object.__setattr__(self, "effort", self.effort.strip())


@dataclass(frozen=True)
class CloseSession:
    pass


@dataclass(frozen=True)
class RefreshInspection:
    inspection_ref: InspectionRef
    timeout_sec: float

    def __post_init__(self) -> None:
        if not isinstance(self.inspection_ref, InspectionRef):
            raise ValueError("inspection effect requires an InspectionRef")
        if isinstance(self.timeout_sec, bool) or self.timeout_sec <= 0:
            raise ValueError("inspection timeout must be positive")


AgentSessionEffect: TypeAlias = (
    StartTurn
    | TrySteerTurn
    | InterruptTurn
    | ResolveApproval
    | ResolveElicitation
    | SetModel
    | SetEffort
    | CloseSession
    | RefreshInspection
)


class AgentEffectOutcome(StrEnum):
    ACCEPTED = "accepted"
    COMPLETED = "completed"
    REJECTED = "rejected"
    UNSUPPORTED = "unsupported"
    UNCERTAIN = "uncertain"


@dataclass(frozen=True)
class AgentEffectResult:
    outcome: AgentEffectOutcome
    error: str = ""
    required_capability: AgentCapability | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.outcome, AgentEffectOutcome):
            raise ValueError("agent effect outcome must be AgentEffectOutcome")
        if not isinstance(self.error, str):
            raise ValueError("agent effect error must be a string")
        error = self.error.strip()
        object.__setattr__(self, "error", error)
        if self.outcome in {AgentEffectOutcome.ACCEPTED, AgentEffectOutcome.COMPLETED}:
            if error or self.required_capability is not None:
                raise ValueError(
                    f"{self.outcome.value} agent effect result must not include error or capability"
                )
            return
        if self.outcome is AgentEffectOutcome.REJECTED:
            if not error:
                raise ValueError("rejected agent effect requires an error")
            if self.required_capability is not None:
                raise ValueError("rejected agent effect must not include a capability")
            return
        if self.outcome is AgentEffectOutcome.UNCERTAIN:
            if not error:
                raise ValueError("uncertain agent effect requires an error")
            if self.required_capability is not None:
                raise ValueError("uncertain agent effect must not include a capability")
            return
        if not error:
            raise ValueError("unsupported agent effect requires an error")
        if not isinstance(self.required_capability, AgentCapability):
            raise ValueError("unsupported agent effect requires an AgentCapability")

    @classmethod
    def accepted(cls) -> AgentEffectResult:
        return cls(AgentEffectOutcome.ACCEPTED)

    @classmethod
    def completed(cls) -> AgentEffectResult:
        return cls(AgentEffectOutcome.COMPLETED)

    @classmethod
    def rejected(cls, error: str) -> AgentEffectResult:
        return cls(AgentEffectOutcome.REJECTED, error=error)

    @classmethod
    def uncertain(cls, error: str) -> AgentEffectResult:
        return cls(AgentEffectOutcome.UNCERTAIN, error=error)

    @classmethod
    def unsupported(cls, capability: AgentCapability) -> AgentEffectResult:
        if not isinstance(capability, AgentCapability):
            raise ValueError("unsupported agent effect requires an AgentCapability")
        return cls(
            AgentEffectOutcome.UNSUPPORTED,
            error=f"agent provider does not support {capability.value}",
            required_capability=capability,
        )


class ApprovalDecisionPhase(StrEnum):
    PREFLIGHT = "preflight"
    PROVIDER_CALL = "provider_call"


@dataclass(frozen=True)
class ApprovalDecisionResult:
    result: AgentEffectResult
    phase: ApprovalDecisionPhase

    def __post_init__(self) -> None:
        if not isinstance(self.result, AgentEffectResult):
            raise ValueError("approval decision result requires AgentEffectResult")
        if not isinstance(self.phase, ApprovalDecisionPhase):
            raise ValueError("approval decision phase must be ApprovalDecisionPhase")
        if (
            self.phase is ApprovalDecisionPhase.PREFLIGHT
            and self.result.outcome in {AgentEffectOutcome.ACCEPTED, AgentEffectOutcome.COMPLETED}
        ):
            raise ValueError("approval preflight result must not be successful")

    @property
    def delivery_attempted(self) -> bool:
        return self.phase is ApprovalDecisionPhase.PROVIDER_CALL

    @property
    def succeeded(self) -> bool:
        return self.result.outcome in {
            AgentEffectOutcome.ACCEPTED,
            AgentEffectOutcome.COMPLETED,
        }

    @property
    def fatal_delivery_failure(self) -> bool:
        return self.delivery_attempted and not self.succeeded

    @classmethod
    def preflight(cls, result: AgentEffectResult) -> ApprovalDecisionResult:
        return cls(result=result, phase=ApprovalDecisionPhase.PREFLIGHT)

    @classmethod
    def provider_call(cls, result: AgentEffectResult) -> ApprovalDecisionResult:
        return cls(result=result, phase=ApprovalDecisionPhase.PROVIDER_CALL)

    @classmethod
    def provider_call_exception(cls, exc: BaseException) -> ApprovalDecisionResult:
        error = str(exc).strip() or type(exc).__name__
        return cls.provider_call(AgentEffectResult.uncertain(error))


ElicitationDecisionResult: TypeAlias = ApprovalDecisionResult


_EFFECT_CAPABILITIES: dict[type[object], AgentCapability | None] = {
    StartTurn: AgentCapability.START_TURN,
    TrySteerTurn: AgentCapability.STEER_ACTIVE_TURN,
    InterruptTurn: AgentCapability.INTERRUPT_TURN,
    ResolveApproval: None,
    ResolveElicitation: None,
    SetModel: AgentCapability.SET_MODEL,
    SetEffort: AgentCapability.SET_EFFORT,
    CloseSession: None,
    RefreshInspection: None,
}


def required_capability(effect: AgentSessionEffect) -> AgentCapability | None:
    try:
        return _EFFECT_CAPABILITIES[type(effect)]
    except KeyError as exc:
        raise TypeError(f"unknown agent session effect: {type(effect).__name__}") from exc


def unsupported_effect_result(
    effect: AgentSessionEffect,
    snapshot: AgentCapabilitySnapshot,
) -> AgentEffectResult | None:
    capability = required_capability(effect)
    if capability is None:
        return None
    if snapshot.supports(capability):
        return None
    return AgentEffectResult.unsupported(capability)


class AgentApprovalKind(StrEnum):
    COMMAND = "command"
    FILE_CHANGE = "file_change"
    TOOL = "tool"
    USER_INPUT = "user_input"


@dataclass(frozen=True)
class AgentApprovalRequest:
    turn_ref: TurnRef
    approval_ref: ApprovalRef
    kind: AgentApprovalKind
    role: str = "agent"
    command: str = ""
    cwd: str = ""
    affected_paths: tuple[str, ...] = ()
    saved_rule_scope: tuple[str, ...] = ()
    reason: str = ""
    available_decisions: tuple[str, ...] = ()
    prompt_details: tuple[str, ...] = ()
    diagnostics: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if self.approval_ref.turn != self.turn_ref:
            raise ValueError("approval request ref must belong to its turn")
        if not isinstance(self.kind, AgentApprovalKind):
            raise ValueError("approval kind must be AgentApprovalKind")
        for name in ("role", "command", "cwd", "reason"):
            value = getattr(self, name)
            if not isinstance(value, str):
                raise ValueError(f"approval {name} must be a string")
            object.__setattr__(self, name, value.strip())
        if len(self.prompt_details) > 32:
            raise ValueError("approval prompt details must contain at most 32 lines")
        if len(self.diagnostics) > 32:
            raise ValueError("approval diagnostics must contain at most 32 fields")
        _validate_bounded_strings("affected path", self.affected_paths, 4096)
        if len(self.saved_rule_scope) > 32:
            raise ValueError("approval saved rule scope must contain at most 32 entries")
        _validate_bounded_strings("approval saved rule scope", self.saved_rule_scope, 2048)
        _validate_bounded_strings("approval decision", self.available_decisions, 128)
        _validate_bounded_strings("approval prompt detail", self.prompt_details, 2048)
        seen: set[str] = set()
        for key, value in self.diagnostics:
            if not isinstance(key, str) or not key or len(key) > 80:
                raise ValueError("approval diagnostic key must be 1..80 characters")
            if key in seen:
                raise ValueError(f"duplicate approval diagnostic key: {key}")
            seen.add(key)
            if not isinstance(value, str) or not value or len(value) > 2048:
                raise ValueError("approval diagnostic value must be 1..2048 characters")

    def supports_decision(self, decision: str) -> bool:
        return str(decision or "").strip().lower() in self.available_decisions


class ApprovalCorrelationStatus(StrEnum):
    MATCHED = "matched"
    NO_ACTIVE_APPROVAL = "no_active_approval"
    TURN_MISMATCH = "turn_mismatch"
    APPROVAL_MISMATCH = "approval_mismatch"


class NonTerminalEventDisposition(StrEnum):
    ACCEPTED = "accepted"
    STALE = "stale"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"
    UNKNOWN = "unknown"
    EVICTED = "evicted"


_Identity = TypeVar("_Identity")


class BoundedIdentityHistory(Collection[_Identity], Generic[_Identity]):
    """Insertion-ordered dedup history whose capacity bounds memory, not work."""

    def __init__(self, capacity: int) -> None:
        if not isinstance(capacity, int) or isinstance(capacity, bool) or capacity <= 0:
            raise ValueError("identity history capacity must be a positive integer")
        self._capacity = capacity
        self._entries: OrderedDict[_Identity, None] = OrderedDict()
        self._has_evicted = False

    @property
    def has_evicted(self) -> bool:
        return self._has_evicted

    def remember(self, identity: _Identity) -> _Identity | None:
        if identity in self._entries:
            return None
        self._entries[identity] = None
        if len(self._entries) <= self._capacity:
            return None
        evicted, _ = self._entries.popitem(last=False)
        self._has_evicted = True
        return evicted

    def clear(self) -> None:
        self._entries.clear()
        self._has_evicted = False

    def __contains__(self, identity: object) -> bool:
        return identity in self._entries

    def __iter__(self) -> Iterator[_Identity]:
        return iter(self._entries)

    def __len__(self) -> int:
        return len(self._entries)


def turn_started_disposition(
    *,
    active_turn_ref: TurnRef | None,
    event_turn_ref: TurnRef,
    already_started: bool,
) -> NonTerminalEventDisposition:
    if not isinstance(event_turn_ref, TurnRef):
        raise TypeError("turn-started event requires TurnRef")
    if active_turn_ref != event_turn_ref:
        return NonTerminalEventDisposition.STALE
    if already_started:
        return NonTerminalEventDisposition.DUPLICATE
    return NonTerminalEventDisposition.ACCEPTED


def approval_requested_disposition(
    *,
    active_turn_ref: TurnRef | None,
    active_approval_ref: ApprovalRef | None,
    completed_approval_refs: Collection[ApprovalRef],
    event_turn_ref: TurnRef,
    event_approval_ref: ApprovalRef,
) -> NonTerminalEventDisposition:
    if not isinstance(event_turn_ref, TurnRef):
        raise TypeError("approval-requested event requires TurnRef")
    if not isinstance(event_approval_ref, ApprovalRef):
        raise TypeError("approval-requested event requires ApprovalRef")
    if event_approval_ref.turn != event_turn_ref:
        raise ValueError("approval-requested event refs must belong to the same turn")
    if active_turn_ref != event_turn_ref:
        return NonTerminalEventDisposition.STALE
    if event_approval_ref == active_approval_ref:
        return NonTerminalEventDisposition.DUPLICATE
    if event_approval_ref in completed_approval_refs:
        return NonTerminalEventDisposition.DUPLICATE
    if active_approval_ref is not None:
        return NonTerminalEventDisposition.CONFLICT
    return NonTerminalEventDisposition.ACCEPTED


def approval_correlation_status(
    expected_request: AgentApprovalRequest,
    *,
    active_turn_ref: TurnRef | None,
    active_approval_ref: ApprovalRef | None,
) -> ApprovalCorrelationStatus:
    if active_turn_ref is None or active_approval_ref is None:
        return ApprovalCorrelationStatus.NO_ACTIVE_APPROVAL
    if expected_request.turn_ref != active_turn_ref:
        return ApprovalCorrelationStatus.TURN_MISMATCH
    if expected_request.approval_ref != active_approval_ref:
        return ApprovalCorrelationStatus.APPROVAL_MISMATCH
    return ApprovalCorrelationStatus.MATCHED


def approval_decision_preflight(
    expected_request: AgentApprovalRequest,
    *,
    active_turn_ref: TurnRef | None,
    active_approval_ref: ApprovalRef | None,
    decision: str,
) -> AgentEffectResult:
    status = approval_correlation_status(
        expected_request,
        active_turn_ref=active_turn_ref,
        active_approval_ref=active_approval_ref,
    )
    if status is not ApprovalCorrelationStatus.MATCHED:
        return AgentEffectResult.rejected(f"approval request is stale: {status.value}")
    if not expected_request.supports_decision(decision):
        return AgentEffectResult.rejected(
            f"approval decision is not supported: {str(decision or '').strip()}"
        )
    return AgentEffectResult.accepted()


def _validate_bounded_strings(label: str, values: tuple[str, ...], limit: int) -> None:
    if not isinstance(values, tuple):
        raise ValueError(f"{label} values must be a tuple")
    for value in values:
        if not isinstance(value, str) or not value or len(value) > limit:
            raise ValueError(f"{label} must be 1..{limit} characters")


def _validate_form_value(
    field: AgentFormField,
    value: object,
) -> ElicitationValue:
    if field.value_type == "string":
        if not isinstance(value, str) or len(value) > 4096:
            raise ValueError("form string value is invalid")
        if field.enum and value not in field.enum:
            raise ValueError("form string value is not an offered option")
        return value
    if field.value_type == "boolean":
        if not isinstance(value, bool):
            raise ValueError("form boolean value is invalid")
        return value
    if field.value_type == "integer":
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or abs(value) > 9_223_372_036_854_775_807
        ):
            raise ValueError("form integer value is invalid")
        return value
    if field.value_type == "number":
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(float(value))
        ):
            raise ValueError("form number value is invalid")
        return value
    if (
        not isinstance(value, (list, tuple))
        or len(value) > 32
        or any(not isinstance(item, str) or len(item) > 512 for item in value)
    ):
        raise ValueError("form string-array value is invalid")
    normalized = tuple(value)
    if field.enum and any(item not in field.enum for item in normalized):
        raise ValueError("form string-array value contains an unoffered option")
    return normalized


@dataclass(frozen=True)
class SessionStarted:
    session_ref: ProviderSessionRef
    effective_model: str
    effective_backend: str
    capabilities: AgentCapabilitySnapshot


@dataclass(frozen=True)
class SessionFailed:
    error: str


@dataclass(frozen=True)
class ModelSelectionAccepted:
    requested_model: str

    def __post_init__(self) -> None:
        if not isinstance(self.requested_model, str) or not self.requested_model.strip():
            raise ValueError("accepted model selection requires a model")
        object.__setattr__(self, "requested_model", self.requested_model.strip())


@dataclass(frozen=True)
class ModelSelectionFailed:
    requested_model: str
    error: str

    def __post_init__(self) -> None:
        if not isinstance(self.requested_model, str) or not self.requested_model.strip():
            raise ValueError("failed model selection requires a model")
        if not isinstance(self.error, str) or not self.error.strip():
            raise ValueError("failed model selection requires an error")
        object.__setattr__(self, "requested_model", self.requested_model.strip())
        object.__setattr__(self, "error", self.error.strip())


@dataclass(frozen=True)
class ModelSelectionUncertain:
    requested_model: str
    error: str

    def __post_init__(self) -> None:
        if not isinstance(self.requested_model, str) or not self.requested_model.strip():
            raise ValueError("uncertain model selection requires a model")
        if not isinstance(self.error, str) or not self.error.strip():
            raise ValueError("uncertain model selection requires an error")
        object.__setattr__(self, "requested_model", self.requested_model.strip())
        object.__setattr__(self, "error", self.error.strip())


@dataclass(frozen=True)
class EffortSelectionAccepted:
    requested_effort: str

    def __post_init__(self) -> None:
        value = str(self.requested_effort or "").strip()
        if not value:
            raise ValueError("accepted effort selection requires an effort")
        object.__setattr__(self, "requested_effort", value)


@dataclass(frozen=True)
class EffortSelectionFailed:
    requested_effort: str
    error: str

    def __post_init__(self) -> None:
        value = str(self.requested_effort or "").strip()
        error = str(self.error or "").strip()
        if not value or not error:
            raise ValueError("failed effort selection requires effort and error")
        object.__setattr__(self, "requested_effort", value)
        object.__setattr__(self, "error", error)


@dataclass(frozen=True)
class EffortSelectionUncertain:
    requested_effort: str
    error: str

    def __post_init__(self) -> None:
        value = str(self.requested_effort or "").strip()
        error = str(self.error or "").strip()
        if not value or not error:
            raise ValueError("uncertain effort selection requires effort and error")
        object.__setattr__(self, "requested_effort", value)
        object.__setattr__(self, "error", error)


@dataclass(frozen=True)
class TurnStarted:
    turn_ref: TurnRef


@dataclass(frozen=True)
class OutputDelta:
    turn_ref: TurnRef
    text: str


class WorkStatusKind(StrEnum):
    NOTICE = "notice"
    TURN_STARTED = "turn_started"
    TURN_COMPLETED = "turn_completed"
    ITEM_CHANGED = "item_changed"


@dataclass(frozen=True)
class WorkStatusChanged:
    turn_ref: TurnRef
    status_text: str
    status_ref: WorkStatusRef | None = None
    kind: WorkStatusKind = WorkStatusKind.NOTICE
    work: AgentWorkSnapshot | None = None
    item_ref: str = ""
    suppress_status: bool = False
    preserve_work_snapshot: bool = False
    auxiliary: bool = False
    origin: AgentInputOrigin | None = None
    agent_id: str = ""
    tool_use_id: str = ""
    parent_tool_use_id: str = ""
    runtime: str = ""
    model_backend: str = ""


@dataclass(frozen=True)
class ApprovalRequested:
    turn_ref: TurnRef
    approval_ref: ApprovalRef
    request: AgentApprovalRequest

    def __post_init__(self) -> None:
        if self.approval_ref.turn != self.turn_ref:
            raise ValueError("approval event ref must belong to its turn")
        if self.request.turn_ref != self.turn_ref:
            raise ValueError("approval event request must belong to its turn")
        if self.request.approval_ref != self.approval_ref:
            raise ValueError("approval event request ref must match event ref")


@dataclass(frozen=True)
class ElicitationRequested:
    turn_ref: TurnRef
    elicitation_ref: ElicitationRef
    request: AgentFormRequest

    def __post_init__(self) -> None:
        if self.elicitation_ref.turn != self.turn_ref:
            raise ValueError("elicitation event ref must belong to its turn")
        if self.request.turn_ref != self.turn_ref:
            raise ValueError("elicitation event request must belong to its turn")
        if self.request.elicitation_ref != self.elicitation_ref:
            raise ValueError("elicitation event request ref must match event ref")


@dataclass(frozen=True)
class BackgroundTaskChanged:
    turn_ref: TurnRef
    task_ref: str
    state: str
    summary: str = ""
    origin: AgentInputOrigin | None = None
    agent_id: str = ""


@dataclass(frozen=True)
class TurnCompleted:
    turn_ref: TurnRef
    output: AgentOutput


@dataclass(frozen=True)
class TurnInterrupted:
    turn_ref: TurnRef


@dataclass(frozen=True)
class TurnFailed:
    turn_ref: TurnRef
    error: str
    raw_provider_error: RawProviderErrorPayload | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.turn_ref, TurnRef):
            raise ValueError("turn failure requires TurnRef")
        if not isinstance(self.error, str):
            raise ValueError("turn failure error must be a string")
        normalized = self.error.strip()
        if not normalized:
            raise ValueError("turn failure error must not be empty")
        if self.raw_provider_error is not None and not isinstance(
            self.raw_provider_error, RawProviderErrorPayload
        ):
            raise ValueError("turn failure raw provider error must be immutable")
        object.__setattr__(self, "error", normalized)


def raw_provider_error_lifecycle_fields(
    raw_provider_error: RawProviderErrorPayload | None,
) -> dict[str, object]:
    """Project an observed provider payload without synthesizing absence."""
    if raw_provider_error is None:
        return {}
    if not isinstance(raw_provider_error, RawProviderErrorPayload):
        raise ValueError("lifecycle raw provider error must be immutable")
    return {"raw_provider_error": raw_provider_error.to_json_object()}


@dataclass(frozen=True)
class ProviderNotice:
    text: str
    turn_ref: TurnRef | None = None


@dataclass(frozen=True)
class InspectionCompleted:
    inspection_ref: InspectionRef
    projection: AgentRuntimeProjection


AgentSessionEvent: TypeAlias = (
    SessionStarted
    | SessionFailed
    | ModelSelectionAccepted
    | ModelSelectionFailed
    | ModelSelectionUncertain
    | EffortSelectionAccepted
    | EffortSelectionFailed
    | EffortSelectionUncertain
    | TurnStarted
    | OutputDelta
    | WorkStatusChanged
    | ApprovalRequested
    | ElicitationRequested
    | BackgroundTaskChanged
    | TurnCompleted
    | TurnInterrupted
    | TurnFailed
    | ProviderNotice
    | InspectionCompleted
)


class AgentSessionEventKind(StrEnum):
    SESSION_STARTED = "session_started"
    SESSION_FAILED = "session_failed"
    MODEL_SELECTION_ACCEPTED = "model_selection_accepted"
    MODEL_SELECTION_FAILED = "model_selection_failed"
    MODEL_SELECTION_UNCERTAIN = "model_selection_uncertain"
    EFFORT_SELECTION_ACCEPTED = "effort_selection_accepted"
    EFFORT_SELECTION_FAILED = "effort_selection_failed"
    EFFORT_SELECTION_UNCERTAIN = "effort_selection_uncertain"
    TURN_STARTED = "turn_started"
    OUTPUT_DELTA = "output_delta"
    WORK_STATUS_CHANGED = "work_status_changed"
    APPROVAL_REQUESTED = "approval_requested"
    ELICITATION_REQUESTED = "elicitation_requested"
    BACKGROUND_TASK_CHANGED = "background_task_changed"
    TURN_COMPLETED = "turn_completed"
    TURN_INTERRUPTED = "turn_interrupted"
    TURN_FAILED = "turn_failed"
    PROVIDER_NOTICE = "provider_notice"
    INSPECTION_COMPLETED = "inspection_completed"
    UNKNOWN = "unknown"


MAX_AGENT_SESSION_EVENT_TYPE_CHARS = 128


class AgentSessionEventDispatchDisposition(StrEnum):
    HANDLE = "handle"
    DROP_CONTRACT_VIOLATION = "drop_contract_violation"


@dataclass(frozen=True)
class AgentSessionEventDispatch:
    kind: AgentSessionEventKind
    disposition: AgentSessionEventDispatchDisposition
    event_type: str
    reason: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.kind, AgentSessionEventKind):
            raise ValueError("agent session event dispatch requires typed kind")
        if not isinstance(self.disposition, AgentSessionEventDispatchDisposition):
            raise ValueError("agent session event dispatch requires typed disposition")
        if not isinstance(self.event_type, str) or not self.event_type.strip():
            raise ValueError("agent session event dispatch requires event type")
        if not isinstance(self.reason, str):
            raise ValueError("agent session event dispatch reason must be a string")
        event_type = _bounded_agent_session_event_type(self.event_type)
        reason = self.reason.strip()
        object.__setattr__(self, "event_type", event_type)
        object.__setattr__(self, "reason", reason)
        if self.disposition is AgentSessionEventDispatchDisposition.HANDLE:
            if reason:
                raise ValueError("handled agent session event must not include a reason")
            if self.kind is AgentSessionEventKind.UNKNOWN:
                raise ValueError("unknown agent session event cannot be handled")
            return
        if not reason:
            raise ValueError("contract-violation dispatch requires a reason")

    def as_drop_contract_violation(self, reason: str) -> AgentSessionEventDispatch:
        return AgentSessionEventDispatch(
            kind=self.kind,
            disposition=AgentSessionEventDispatchDisposition.DROP_CONTRACT_VIOLATION,
            event_type=self.event_type,
            reason=reason,
        )


def dispatch_agent_session_event(event: object) -> AgentSessionEventDispatch:
    """Classify every provider-neutral session event without touching runtime state."""

    event_type = type(event)
    event_type_name = event_type.__name__
    if event_type is SessionStarted:
        kind = AgentSessionEventKind.SESSION_STARTED
    elif event_type is SessionFailed:
        kind = AgentSessionEventKind.SESSION_FAILED
    elif event_type is ModelSelectionAccepted:
        kind = AgentSessionEventKind.MODEL_SELECTION_ACCEPTED
    elif event_type is ModelSelectionFailed:
        kind = AgentSessionEventKind.MODEL_SELECTION_FAILED
    elif event_type is ModelSelectionUncertain:
        kind = AgentSessionEventKind.MODEL_SELECTION_UNCERTAIN
    elif event_type is EffortSelectionAccepted:
        kind = AgentSessionEventKind.EFFORT_SELECTION_ACCEPTED
    elif event_type is EffortSelectionFailed:
        kind = AgentSessionEventKind.EFFORT_SELECTION_FAILED
    elif event_type is EffortSelectionUncertain:
        kind = AgentSessionEventKind.EFFORT_SELECTION_UNCERTAIN
    elif event_type is TurnStarted:
        kind = AgentSessionEventKind.TURN_STARTED
    elif event_type is OutputDelta:
        kind = AgentSessionEventKind.OUTPUT_DELTA
    elif event_type is WorkStatusChanged:
        kind = AgentSessionEventKind.WORK_STATUS_CHANGED
    elif event_type is ApprovalRequested:
        kind = AgentSessionEventKind.APPROVAL_REQUESTED
    elif event_type is ElicitationRequested:
        kind = AgentSessionEventKind.ELICITATION_REQUESTED
    elif event_type is BackgroundTaskChanged:
        return AgentSessionEventDispatch(
            kind=AgentSessionEventKind.BACKGROUND_TASK_CHANGED,
            disposition=AgentSessionEventDispatchDisposition.DROP_CONTRACT_VIOLATION,
            event_type=event_type_name,
            reason="background task projection is unsupported",
        )
    elif event_type is TurnCompleted:
        kind = AgentSessionEventKind.TURN_COMPLETED
    elif event_type is TurnInterrupted:
        kind = AgentSessionEventKind.TURN_INTERRUPTED
    elif event_type is TurnFailed:
        kind = AgentSessionEventKind.TURN_FAILED
    elif event_type is ProviderNotice:
        kind = AgentSessionEventKind.PROVIDER_NOTICE
    elif event_type is InspectionCompleted:
        kind = AgentSessionEventKind.INSPECTION_COMPLETED
    else:
        return AgentSessionEventDispatch(
            kind=AgentSessionEventKind.UNKNOWN,
            disposition=AgentSessionEventDispatchDisposition.DROP_CONTRACT_VIOLATION,
            event_type=event_type_name,
            reason="unknown agent session event",
        )
    return AgentSessionEventDispatch(
        kind=kind,
        disposition=AgentSessionEventDispatchDisposition.HANDLE,
        event_type=event_type_name,
    )


def _bounded_agent_session_event_type(value: str) -> str:
    text = value.strip()
    if len(text) <= MAX_AGENT_SESSION_EVENT_TYPE_CHARS:
        return text
    return text[: MAX_AGENT_SESSION_EVENT_TYPE_CHARS - 3] + "..."


class AgentSessionPort(Protocol):
    def get_capability_snapshot(self) -> AgentCapabilitySnapshot: ...

    def start_turn(self, effect: StartTurn) -> AgentEffectResult: ...

    def try_steer_turn(self, effect: TrySteerTurn) -> AgentEffectResult: ...

    def interrupt_turn(self, effect: InterruptTurn) -> AgentEffectResult: ...

    def resolve_approval(self, effect: ResolveApproval) -> ApprovalDecisionResult: ...

    def resolve_elicitation(
        self,
        effect: ResolveElicitation,
    ) -> ElicitationDecisionResult: ...

    def set_model(self, effect: SetModel) -> AgentEffectResult: ...

    def set_effort(self, effect: SetEffort) -> AgentEffectResult: ...

    def close_session(self, effect: CloseSession) -> AgentEffectResult: ...

    def refresh_inspection(self, effect: RefreshInspection) -> AgentEffectResult: ...

    def get_local_projection(self) -> AgentRuntimeProjection: ...

    def poll_events(self) -> list[AgentSessionEvent]: ...
