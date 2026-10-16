from __future__ import annotations

from dataclasses import dataclass
import re
from uuid import uuid4


_IDENTIFIER_PATTERN = re.compile(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*\Z")


class _ValidatedIdentifier(str):
    _label = "identifier"

    def __new__(cls, value: object) -> _ValidatedIdentifier:
        if not isinstance(value, str):
            raise ValueError(f"{cls._label} must be a string")
        normalized = value.strip().lower()
        if not _IDENTIFIER_PATTERN.fullmatch(normalized):
            raise ValueError(
                f"{cls._label} must use lowercase letters, digits, and single hyphens"
            )
        return str.__new__(cls, normalized)

    @property
    def value(self) -> str:
        return str(self)

    @classmethod
    def parse(cls, value: object) -> _ValidatedIdentifier:
        return cls(value)


class ProviderKind(_ValidatedIdentifier):
    _label = "provider identifier"


class ProviderProfileId(_ValidatedIdentifier):
    _label = "provider profile identifier"


ProviderKind.CODEX = ProviderKind("codex")  # type: ignore[attr-defined]
ProviderKind.CLAUDE = ProviderKind("claude")  # type: ignore[attr-defined]
ProviderKind.OPENCODE = ProviderKind("opencode")  # type: ignore[attr-defined]
ProviderProfileId.CODEX_NATIVE = ProviderProfileId("codex-native")  # type: ignore[attr-defined]
ProviderProfileId.CLAUDE_NATIVE = ProviderProfileId("claude-native")  # type: ignore[attr-defined]
ProviderProfileId.OPENCODE_NATIVE = ProviderProfileId("opencode-native")  # type: ignore[attr-defined]
ProviderProfileId.OPENCODE_ACP = ProviderProfileId("opencode-acp")  # type: ignore[attr-defined]


@dataclass(frozen=True, order=True)
class ProviderExecutionProfile:
    provider: ProviderKind
    profile: ProviderProfileId

    def __post_init__(self) -> None:
        if not isinstance(self.provider, ProviderKind):
            raise ValueError("execution profile provider must be ProviderKind")
        if not isinstance(self.profile, ProviderProfileId):
            raise ValueError("execution profile ID must be ProviderProfileId")

@dataclass(frozen=True, order=True)
class ProviderSessionRef:
    provider: ProviderKind
    native_id: str
    profile: ProviderProfileId

    def __post_init__(self) -> None:
        if not isinstance(self.provider, ProviderKind):
            raise ValueError("provider session ref provider must be ProviderKind")
        native_id = str(self.native_id or "").strip()
        if not native_id:
            raise ValueError("provider session id must not be empty")
        if not isinstance(self.profile, ProviderProfileId):
            raise ValueError("provider session ref profile must be ProviderProfileId")
        object.__setattr__(self, "native_id", native_id)

    @property
    def execution_profile(self) -> ProviderExecutionProfile:
        return ProviderExecutionProfile(self.provider, self.profile)


@dataclass(frozen=True)
class RuntimeGeneration:
    value: str

    @classmethod
    def new(cls) -> RuntimeGeneration:
        return cls(uuid4().hex)


@dataclass(frozen=True)
class TurnRef:
    runtime_generation: RuntimeGeneration
    sequence: int

    def __post_init__(self) -> None:
        if self.sequence <= 0:
            raise ValueError("turn sequence must be positive")


@dataclass(frozen=True)
class WorkStatusRef:
    runtime_generation: RuntimeGeneration
    sequence: int

    def __post_init__(self) -> None:
        if self.sequence <= 0:
            raise ValueError("work status sequence must be positive")


@dataclass(frozen=True)
class ApprovalRef:
    turn: TurnRef
    value: str

    def __post_init__(self) -> None:
        value = str(self.value or "").strip()
        if not value:
            raise ValueError("approval ref must not be empty")
        object.__setattr__(self, "value", value)

    @classmethod
    def new(cls, turn: TurnRef) -> ApprovalRef:
        return cls(turn=turn, value=uuid4().hex)


@dataclass(frozen=True)
class ElicitationRef:
    turn: TurnRef
    value: str

    def __post_init__(self) -> None:
        value = str(self.value or "").strip()
        if not value:
            raise ValueError("elicitation ref must not be empty")
        object.__setattr__(self, "value", value)

    @classmethod
    def new(cls, turn: TurnRef) -> ElicitationRef:
        return cls(turn=turn, value=uuid4().hex)


@dataclass(frozen=True)
class InspectionRef:
    value: str

    def __post_init__(self) -> None:
        value = str(self.value or "").strip()
        if not value:
            raise ValueError("inspection ref must not be empty")
        object.__setattr__(self, "value", value)

    @classmethod
    def new(cls) -> InspectionRef:
        return cls(uuid4().hex)
