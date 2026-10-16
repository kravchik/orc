"""Provider-neutral host lifecycle and session-open contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import StrEnum
import math
from pathlib import Path
from types import MappingProxyType
from typing import Callable, Mapping, Protocol, TypeAlias, runtime_checkable
from uuid import uuid4

from orchestrator.agent_capabilities import AgentCapability, AgentCapabilitySnapshot
from orchestrator.agent_identity import (
    ProviderExecutionProfile,
    ProviderKind,
    ProviderProfileId,
    ProviderSessionRef,
)
from orchestrator.agent_session import AgentSessionPort, SessionFailed, SessionStarted
from orchestrator.processes import LifecycleLogger


@dataclass(frozen=True)
class OpenSessionCorrelationId:
    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or not self.value.strip():
            raise ValueError("open session correlation id must be a non-empty string")
        object.__setattr__(self, "value", self.value.strip())

    @classmethod
    def new(cls) -> OpenSessionCorrelationId:
        return cls(uuid4().hex)


@dataclass(frozen=True)
class AgentSessionConfig:
    cwd: str = ""
    model: str = ""
    backend: str = ""
    approval_policy: str = ""
    sandbox: str = ""

    def __post_init__(self) -> None:
        for field_name in ("cwd", "model", "backend", "approval_policy", "sandbox"):
            value = getattr(self, field_name)
            if not isinstance(value, str):
                raise ValueError(f"agent session config {field_name} must be a string")
            object.__setattr__(self, field_name, value.strip())


@dataclass(frozen=True)
class StartSessionRequest:
    correlation_id: OpenSessionCorrelationId
    config: AgentSessionConfig

    def __post_init__(self) -> None:
        _validate_open_request(self.correlation_id, self.config)


@dataclass(frozen=True)
class ResumeSessionRequest:
    correlation_id: OpenSessionCorrelationId
    config: AgentSessionConfig
    session_ref: ProviderSessionRef

    def __post_init__(self) -> None:
        _validate_open_request(self.correlation_id, self.config)
        if not isinstance(self.session_ref, ProviderSessionRef):
            raise ValueError("resume session request requires ProviderSessionRef")


OpenSessionRequest: TypeAlias = StartSessionRequest | ResumeSessionRequest


@dataclass(frozen=True)
class ListProviderSessions:
    execution_profile: ProviderExecutionProfile
    cwd: str = ""
    limit: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.execution_profile, ProviderExecutionProfile):
            raise ValueError("session catalog request requires ProviderExecutionProfile")
        if not isinstance(self.cwd, str):
            raise ValueError("session catalog cwd must be a string")
        if self.limit is not None and (
            not isinstance(self.limit, int)
            or isinstance(self.limit, bool)
            or self.limit < 0
        ):
            raise ValueError("session catalog limit must be a non-negative integer or None")
        object.__setattr__(self, "cwd", self.cwd.strip())


@dataclass(frozen=True)
class ProviderSessionCatalogEntry:
    session_ref: ProviderSessionRef
    name: str
    cwd: str
    adapter_name: str
    adapter_version: str
    created_at: str = ""
    updated_at: str = ""
    preview: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.session_ref, ProviderSessionRef):
            raise ValueError("session catalog entry requires ProviderSessionRef")
        for field_name in (
            "name",
            "cwd",
            "adapter_name",
            "adapter_version",
            "created_at",
            "updated_at",
            "preview",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str):
                raise ValueError(f"session catalog {field_name} must be a string")
            object.__setattr__(self, field_name, value.strip())


class ProviderSessionCatalogErrorKind(StrEnum):
    INSTALLATION_UNAVAILABLE = "installation_unavailable"
    UNSUPPORTED = "unsupported"
    INVALID_REQUEST = "invalid_request"
    UNAVAILABLE = "unavailable"
    PROVIDER_FAILED = "provider_failed"


@dataclass(frozen=True)
class ProviderSessionsListed:
    entries: tuple[ProviderSessionCatalogEntry, ...]

    def __post_init__(self) -> None:
        entries = tuple(self.entries)
        if any(not isinstance(item, ProviderSessionCatalogEntry) for item in entries):
            raise ValueError("listed provider sessions must be catalog entries")
        object.__setattr__(self, "entries", entries)


@dataclass(frozen=True)
class ProviderSessionsListFailed:
    kind: ProviderSessionCatalogErrorKind
    error: str

    def __post_init__(self) -> None:
        if not isinstance(self.kind, ProviderSessionCatalogErrorKind):
            raise ValueError("session catalog failure kind must be typed")
        if not isinstance(self.error, str) or not self.error.strip():
            raise ValueError("session catalog failure requires an error")
        object.__setattr__(self, "error", self.error.strip())


ProviderSessionsListResult: TypeAlias = ProviderSessionsListed | ProviderSessionsListFailed
ProviderSessionCatalogLoader: TypeAlias = Callable[
    [ListProviderSessions], ProviderSessionsListResult
]


@runtime_checkable
class ProviderSessionCatalogHost(Protocol):
    def list_sessions(
        self,
        request: ListProviderSessions,
    ) -> ProviderSessionsListResult: ...


@dataclass(frozen=True)
class EffectiveRuntimeSnapshot:
    model: str
    backend: str

    def __post_init__(self) -> None:
        for field_name in ("model", "backend"):
            value = getattr(self, field_name)
            if not isinstance(value, str):
                raise ValueError(f"effective runtime {field_name} must be a string")
            object.__setattr__(self, field_name, value.strip())


class OpenSessionErrorKind(StrEnum):
    INVALID_REQUEST = "invalid_request"
    BUSY = "busy"
    STARTUP_FAILED = "startup_failed"
    STARTUP_UNCERTAIN = "startup_uncertain"
    STARTUP_TIMEOUT = "startup_timeout"
    PROCESS_EXITED = "process_exited"
    CANCELLED = "cancelled"


class ProviderStartupFailureCategory(StrEnum):
    MISSING_EXECUTABLE = "missing_executable"
    VERSION_MISMATCH = "version_mismatch"
    BOOTSTRAP_FAILURE = "bootstrap_failure"


class ProviderStartupFailure(RuntimeError):
    """Typed internal startup failure whose category is safe to expose."""

    def __init__(
        self,
        category: ProviderStartupFailureCategory,
        message: str,
    ) -> None:
        if not isinstance(category, ProviderStartupFailureCategory):
            raise ValueError("provider startup failure requires a typed category")
        if not isinstance(message, str) or not message.strip():
            raise ValueError("provider startup failure message must be non-empty")
        self.category = category
        super().__init__(message.strip())


def classify_provider_startup_failure(
    error: BaseException,
) -> ProviderStartupFailureCategory:
    """Classify startup failures from types only; exception text is never parsed."""

    if not isinstance(error, BaseException):
        raise TypeError("provider startup failure classifier requires an exception")
    if isinstance(error, ProviderStartupFailure):
        return error.category
    if isinstance(error, FileNotFoundError):
        return ProviderStartupFailureCategory.MISSING_EXECUTABLE
    return ProviderStartupFailureCategory.BOOTSTRAP_FAILURE


def publish_provider_startup_failure(
    logger: LifecycleLogger,
    execution_profile: ProviderExecutionProfile,
    category: ProviderStartupFailureCategory,
) -> None:
    """Publish the fixed payload-free diagnostic at the startup owner."""

    if not isinstance(logger, LifecycleLogger):
        raise TypeError("provider startup failure logger must be LifecycleLogger")
    if not isinstance(execution_profile, ProviderExecutionProfile):
        raise TypeError("provider startup failure requires ProviderExecutionProfile")
    if not isinstance(category, ProviderStartupFailureCategory):
        raise TypeError("provider startup failure category must be typed")
    logger.event(
        "provider_startup_failure",
        provider=execution_profile.provider.value,
        profile=execution_profile.profile.value,
        category=category.value,
    )


@dataclass(frozen=True)
class OpenSessionError:
    kind: OpenSessionErrorKind
    message: str
    startup_failure_category: ProviderStartupFailureCategory | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.kind, OpenSessionErrorKind):
            raise ValueError("open session error kind must be OpenSessionErrorKind")
        if not isinstance(self.message, str) or not self.message.strip():
            raise ValueError("open session error message must be a non-empty string")
        category = self.startup_failure_category
        if category is not None:
            if not isinstance(category, ProviderStartupFailureCategory):
                raise ValueError("open session startup failure category must be typed")
            if self.kind is not OpenSessionErrorKind.STARTUP_FAILED:
                raise ValueError(
                    "open session startup failure category requires STARTUP_FAILED"
                )
        object.__setattr__(self, "message", self.message.strip())


@dataclass(frozen=True)
class OpenSessionProgress:
    correlation_id: OpenSessionCorrelationId
    phase: str
    message: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.correlation_id, OpenSessionCorrelationId):
            raise ValueError("open session progress requires OpenSessionCorrelationId")
        if not isinstance(self.phase, str) or not self.phase.strip():
            raise ValueError("open session progress phase must be a non-empty string")
        if not isinstance(self.message, str):
            raise ValueError("open session progress message must be a string")
        object.__setattr__(self, "phase", self.phase.strip())
        object.__setattr__(self, "message", self.message.strip())


@dataclass(frozen=True)
class Opened:
    correlation_id: OpenSessionCorrelationId
    session: AgentSessionPort
    session_ref: ProviderSessionRef
    effective: EffectiveRuntimeSnapshot
    capabilities: AgentCapabilitySnapshot

    def __post_init__(self) -> None:
        if not isinstance(self.correlation_id, OpenSessionCorrelationId):
            raise ValueError("opened session requires OpenSessionCorrelationId")
        if not isinstance(self.session_ref, ProviderSessionRef):
            raise ValueError("opened session requires ProviderSessionRef")
        if not isinstance(self.effective, EffectiveRuntimeSnapshot):
            raise ValueError("opened session requires EffectiveRuntimeSnapshot")
        if not isinstance(self.capabilities, AgentCapabilitySnapshot):
            raise ValueError("opened session requires AgentCapabilitySnapshot")


@dataclass(frozen=True)
class OpenFailed:
    correlation_id: OpenSessionCorrelationId
    error: OpenSessionError

    def __post_init__(self) -> None:
        if not isinstance(self.correlation_id, OpenSessionCorrelationId):
            raise ValueError("failed session open requires OpenSessionCorrelationId")
        if not isinstance(self.error, OpenSessionError):
            raise ValueError("failed session open requires OpenSessionError")


OpenSessionResult: TypeAlias = Opened | OpenFailed
OpenSessionEvent: TypeAlias = OpenSessionProgress | OpenSessionResult


class OpenSessionOperation(ABC):
    """Single-terminal event stream advanced by the provider delivery lane."""

    def __init__(self, request: OpenSessionRequest) -> None:
        if not isinstance(request, (StartSessionRequest, ResumeSessionRequest)):
            raise ValueError("open session operation requires a typed request")
        self.request = request
        self.correlation_id = request.correlation_id
        self._events: list[OpenSessionEvent] = []
        self._terminal_result: OpenSessionResult | None = None

    @property
    def terminal_result(self) -> OpenSessionResult | None:
        return self._terminal_result

    @property
    def completed(self) -> bool:
        return self._terminal_result is not None

    def poll_events(self) -> list[OpenSessionEvent]:
        if not self.completed:
            self._advance()
        events = list(self._events)
        self._events.clear()
        return events

    def _publish_progress(self, *, phase: str, message: str = "") -> None:
        self._require_pending()
        self._events.append(OpenSessionProgress(self.correlation_id, phase, message))

    def _complete(self, result: OpenSessionResult) -> None:
        self._require_pending()
        if not isinstance(result, (Opened, OpenFailed)):
            raise ValueError("open session terminal result must be typed")
        if result.correlation_id != self.correlation_id:
            raise ValueError("open session terminal result correlation does not match request")
        self._terminal_result = result
        self._events.append(result)

    def _require_pending(self) -> None:
        if self.completed:
            raise RuntimeError(
                f"open session operation {self.correlation_id.value} already completed"
            )

    def cancel(self, reason: str) -> None:
        if self.completed:
            return
        self._complete(
            OpenFailed(
                self.correlation_id,
                OpenSessionError(OpenSessionErrorKind.CANCELLED, reason),
            )
        )

    @abstractmethod
    def _advance(self) -> None: ...


@dataclass(frozen=True)
class StopHost:
    pass


@dataclass(frozen=True)
class StopHostResult:
    stopped: bool
    error: str = ""

    def __post_init__(self) -> None:
        if type(self.stopped) is not bool:
            raise ValueError("stop host result stopped must be boolean")
        if not isinstance(self.error, str):
            raise ValueError("stop host result error must be a string")
        error = self.error.strip()
        object.__setattr__(self, "error", error)
        if self.stopped and error:
            raise ValueError("successful stop host result must not include an error")
        if not self.stopped and not error:
            raise ValueError("failed stop host result requires an error")


class ProviderHost(Protocol):
    @property
    def provider(self) -> ProviderKind: ...

    @property
    def profile(self) -> ProviderProfileId: ...

    def open_session(self, request: OpenSessionRequest) -> OpenSessionOperation: ...

    def consume_poll_progress(self) -> bool: ...

    def consume_terminal_error(self) -> str: ...

    def stop_host(self, effect: StopHost) -> StopHostResult: ...


ProviderHostFactory: TypeAlias = Callable[[object], ProviderHost]


class ProviderHostLifetime(StrEnum):
    DEDICATED = "dedicated"
    SHARED_PROCESS = "shared_process"


def validate_timeout_seconds(value: object, *, field_name: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) <= 0
    ):
        raise ValueError(f"{field_name} must be a positive finite number")
    return float(value)


class ProviderHostConfigurationErrorKind(StrEnum):
    MISSING_ROOT = "missing_root"
    CALLER_OVERRIDE = "caller_override"
    CALLER_COMMAND_MISMATCH = "caller_command_mismatch"


class ProviderHostConfigurationError(RuntimeError):
    def __init__(
        self,
        kind: ProviderHostConfigurationErrorKind,
        execution_profile: ProviderExecutionProfile,
    ) -> None:
        if not isinstance(kind, ProviderHostConfigurationErrorKind):
            raise ValueError("provider host configuration error kind must be typed")
        if not isinstance(execution_profile, ProviderExecutionProfile):
            raise ValueError("provider host configuration error requires execution profile")
        self.kind = kind
        self.execution_profile = execution_profile
        message = {
            ProviderHostConfigurationErrorKind.MISSING_ROOT: (
                "shared provider host requires root-owned configuration"
            ),
            ProviderHostConfigurationErrorKind.CALLER_OVERRIDE: (
                "incompatible shared provider host configuration"
            ),
            ProviderHostConfigurationErrorKind.CALLER_COMMAND_MISMATCH: (
                "dedicated provider runtime command differs from configured command"
            ),
        }[kind]
        super().__init__(message)


@dataclass(frozen=True)
class ProviderHostRootSettings:
    """Process-root-owned settings shared by every host execution profile."""

    logger: LifecycleLogger
    startup_timeout_sec: float
    role: str
    rpc_timeout_sec: float
    edge_thread_name: str = "provider-edge-thread"

    def __post_init__(self) -> None:
        if not isinstance(self.logger, LifecycleLogger):
            raise ValueError("provider host root settings require LifecycleLogger")
        startup_timeout_sec = validate_timeout_seconds(
            self.startup_timeout_sec,
            field_name="provider startup timeout",
        )
        if not isinstance(self.role, str) or not self.role.strip():
            raise ValueError("provider role must be a non-empty string")
        rpc_timeout_sec = validate_timeout_seconds(
            self.rpc_timeout_sec,
            field_name="provider RPC timeout",
        )
        if not isinstance(self.edge_thread_name, str) or not self.edge_thread_name.strip():
            raise ValueError("provider edge thread name must be non-empty")
        object.__setattr__(self, "startup_timeout_sec", startup_timeout_sec)
        object.__setattr__(self, "role", self.role.strip())
        object.__setattr__(self, "rpc_timeout_sec", rpc_timeout_sec)
        object.__setattr__(self, "edge_thread_name", self.edge_thread_name.strip())


@dataclass(frozen=True)
class ProviderHostRootConfiguration:
    """Exact immutable process configuration completed by provider registration."""

    settings: ProviderHostRootSettings
    runtime_command: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.settings, ProviderHostRootSettings):
            raise ValueError("provider host root configuration requires root settings")
        command = tuple(self.runtime_command)
        if not command or any(not isinstance(part, str) or not part for part in command):
            raise ValueError("provider runtime command must contain non-empty strings")
        object.__setattr__(self, "runtime_command", command)

    @property
    def logger(self) -> LifecycleLogger:
        return self.settings.logger

    @property
    def startup_timeout_sec(self) -> float:
        return self.settings.startup_timeout_sec

    @property
    def role(self) -> str:
        return self.settings.role

    @property
    def rpc_timeout_sec(self) -> float:
        return self.settings.rpc_timeout_sec

    @property
    def edge_thread_name(self) -> str:
        return self.settings.edge_thread_name


@dataclass(frozen=True)
class ProviderDedicatedHostConfiguration(ProviderHostRootConfiguration):
    """Dedicated-process settings outside the shared-host root contract."""

    rpc_retries: int
    edge_factory: Callable[..., object] | None = None
    protocol_log_include_params: bool | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        if type(self.rpc_retries) is not int or self.rpc_retries < 0:
            raise ValueError("provider RPC retries must be a non-negative integer")
        if self.edge_factory is not None and not callable(self.edge_factory):
            raise ValueError("provider edge factory must be callable or None")
        if self.protocol_log_include_params is not None and type(
            self.protocol_log_include_params
        ) is not bool:
            raise ValueError("provider protocol logging flag must be boolean or None")


@dataclass(frozen=True)
class ComposedProviderHost:
    host: ProviderHost
    initial_capabilities: AgentCapabilitySnapshot

    def __post_init__(self) -> None:
        if not isinstance(self.initial_capabilities, AgentCapabilitySnapshot):
            raise ValueError("composed provider host requires capability snapshot")


ProviderHostComposer: TypeAlias = Callable[
    [ProviderHostRootConfiguration, AgentSessionConfig],
    ComposedProviderHost,
]


@dataclass(frozen=True)
class ProviderRegistrationContext:
    """Immutable provider-specific process inputs owned by registration hooks."""

    cli_paths: Mapping[ProviderKind, Path]
    environments: Mapping[ProviderKind, Mapping[str, str]] = field(repr=False)
    ambient_environment: Mapping[str, str] = field(default_factory=dict, repr=False)
    profile_cli_paths: Mapping[ProviderExecutionProfile, Path] = field(
        default_factory=dict
    )
    session_catalog_roots: Mapping[ProviderExecutionProfile, Path] = field(
        default_factory=dict
    )
    shared_host_cleanup_timeout_sec: float = 20.0

    def __post_init__(self) -> None:
        cli_paths = dict(self.cli_paths)
        profile_cli_paths = dict(self.profile_cli_paths)
        session_catalog_roots = dict(self.session_catalog_roots)
        environments = {
            provider: MappingProxyType(dict(environment))
            for provider, environment in self.environments.items()
        }
        ambient_environment = dict(self.ambient_environment)
        if any(not isinstance(provider, ProviderKind) for provider in cli_paths):
            raise ValueError("provider CLI path keys must be ProviderKind")
        if any(not isinstance(path, Path) for path in cli_paths.values()):
            raise ValueError("provider CLI paths must be Path values")
        if any(
            not isinstance(profile, ProviderExecutionProfile)
            for profile in profile_cli_paths
        ):
            raise ValueError("profile CLI path keys must be ProviderExecutionProfile")
        if any(not isinstance(path, Path) for path in profile_cli_paths.values()):
            raise ValueError("profile CLI paths must be Path values")
        if any(
            not isinstance(profile, ProviderExecutionProfile)
            for profile in session_catalog_roots
        ):
            raise ValueError("session catalog root keys must be ProviderExecutionProfile")
        if any(not isinstance(path, Path) for path in session_catalog_roots.values()):
            raise ValueError("session catalog roots must be Path values")
        if any(not isinstance(provider, ProviderKind) for provider in environments):
            raise ValueError("provider environment keys must be ProviderKind")
        if any(
            not isinstance(key, str) or not isinstance(value, str)
            for environment in environments.values()
            for key, value in environment.items()
        ):
            raise ValueError("provider environments must contain string keys and values")
        if any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in ambient_environment.items()
        ):
            raise ValueError("ambient environment must contain string keys and values")
        shared_host_cleanup_timeout_sec = validate_timeout_seconds(
            self.shared_host_cleanup_timeout_sec,
            field_name="shared provider host cleanup timeout",
        )
        object.__setattr__(self, "cli_paths", MappingProxyType(cli_paths))
        object.__setattr__(
            self,
            "profile_cli_paths",
            MappingProxyType(profile_cli_paths),
        )
        object.__setattr__(
            self,
            "session_catalog_roots",
            MappingProxyType(session_catalog_roots),
        )
        object.__setattr__(self, "environments", MappingProxyType(environments))
        object.__setattr__(
            self,
            "ambient_environment",
            MappingProxyType(ambient_environment),
        )
        object.__setattr__(
            self,
            "shared_host_cleanup_timeout_sec",
            shared_host_cleanup_timeout_sec,
        )

    def cli_path_for(self, provider: ProviderKind) -> Path | None:
        return self.cli_paths.get(provider)

    def environment_for(self, provider: ProviderKind) -> Mapping[str, str]:
        return self.environments.get(provider, MappingProxyType({}))

    def runtime_environment_for(
        self,
        provider: ProviderKind,
        *,
        inherit_parent: bool = True,
    ) -> Mapping[str, str]:
        """Return one immutable environment projection for preflight and spawn."""

        environment = dict(self.ambient_environment) if inherit_parent else {}
        environment.update(self.environment_for(provider))
        return MappingProxyType(environment)

    def cli_path_for_profile(
        self,
        execution_profile: ProviderExecutionProfile,
    ) -> Path | None:
        return self.profile_cli_paths.get(
            execution_profile,
            self.cli_path_for(execution_profile.provider),
        )

    def session_catalog_root_for(
        self,
        execution_profile: ProviderExecutionProfile,
    ) -> Path | None:
        return self.session_catalog_roots.get(execution_profile)


@dataclass(frozen=True)
class _ProviderRegistration:
    config_type: type[object]
    factory: ProviderHostFactory
    composer: ProviderHostComposer | None
    lifetime: ProviderHostLifetime
    session_catalog_loader: ProviderSessionCatalogLoader | None
    shared_runtime_command: tuple[str, ...] | None


class ProviderRegistry:
    def __init__(self, profile_catalog) -> None:
        from orchestrator.runtime_profile_catalog import RuntimeProfileCatalog

        if not isinstance(profile_catalog, RuntimeProfileCatalog):
            raise ValueError("provider registry requires RuntimeProfileCatalog")
        self._profile_catalog = profile_catalog
        self._registrations: dict[ProviderExecutionProfile, _ProviderRegistration] = {}

    def is_registered(self, execution_profile: ProviderExecutionProfile) -> bool:
        self._validate_key(execution_profile)
        return execution_profile in self._registrations

    def require_registered(self, execution_profile: ProviderExecutionProfile) -> None:
        self._validate_key(execution_profile)
        if execution_profile not in self._registrations:
            raise ProviderUnavailableError(execution_profile)

    def register(
        self,
        execution_profile: ProviderExecutionProfile,
        config_type: type[object],
        factory: ProviderHostFactory,
        *,
        composer: ProviderHostComposer | None = None,
        lifetime: ProviderHostLifetime = ProviderHostLifetime.DEDICATED,
        session_catalog_loader: ProviderSessionCatalogLoader | None = None,
        shared_runtime_command: tuple[str, ...] | None = None,
    ) -> None:
        self._validate_key(execution_profile)
        if execution_profile in self._registrations:
            raise ValueError(
                "provider execution profile is already registered: "
                f"{execution_profile.provider.value}/{execution_profile.profile.value}"
            )
        if not isinstance(config_type, type):
            raise ValueError("provider config type must be a type")
        if not callable(factory):
            raise ValueError("provider host factory must be callable")
        if composer is not None and not callable(composer):
            raise ValueError("provider host composer must be callable")
        if not isinstance(lifetime, ProviderHostLifetime):
            raise ValueError("provider host lifetime must be ProviderHostLifetime")
        if session_catalog_loader is not None and not callable(session_catalog_loader):
            raise ValueError("provider session catalog loader must be callable")
        if shared_runtime_command is not None:
            shared_runtime_command = tuple(shared_runtime_command)
            if not shared_runtime_command or any(
                not isinstance(part, str) or not part
                for part in shared_runtime_command
            ):
                raise ValueError(
                    "shared provider runtime command must contain non-empty strings"
                )
        if (
            lifetime is ProviderHostLifetime.SHARED_PROCESS
            and shared_runtime_command is None
        ):
            raise ValueError("shared provider registration requires a runtime command")
        if (
            lifetime is not ProviderHostLifetime.SHARED_PROCESS
            and shared_runtime_command is not None
        ):
            raise ValueError("dedicated provider registration cannot own a shared command")
        self._registrations[execution_profile] = _ProviderRegistration(
            config_type,
            factory,
            composer,
            lifetime,
            session_catalog_loader,
            shared_runtime_command,
        )

    def list_sessions(
        self,
        request: ListProviderSessions,
    ) -> ProviderSessionsListResult:
        if not isinstance(request, ListProviderSessions):
            raise ValueError("provider session listing requires ListProviderSessions")
        self.require_registered(request.execution_profile)
        loader = self._registrations[request.execution_profile].session_catalog_loader
        if loader is None:
            return ProviderSessionsListFailed(
                ProviderSessionCatalogErrorKind.UNSUPPORTED,
                "provider session catalog is unsupported",
            )
        result = loader(request)
        return self.validate_session_listing(request, result)

    def has_session_catalog_loader(
        self,
        execution_profile: ProviderExecutionProfile,
    ) -> bool:
        self.require_registered(execution_profile)
        return self._registrations[execution_profile].session_catalog_loader is not None

    @staticmethod
    def validate_session_listing(
        request: ListProviderSessions,
        result: ProviderSessionsListResult,
    ) -> ProviderSessionsListResult:
        if not isinstance(result, (ProviderSessionsListed, ProviderSessionsListFailed)):
            raise RuntimeError("provider session catalog loader returned an invalid result")
        if isinstance(result, ProviderSessionsListed) and any(
            entry.session_ref.execution_profile != request.execution_profile
            for entry in result.entries
        ):
            return ProviderSessionsListFailed(
                ProviderSessionCatalogErrorKind.INVALID_REQUEST,
                "provider session catalog returned a foreign execution profile",
            )
        return result

    def host_lifetime(
        self,
        execution_profile: ProviderExecutionProfile,
    ) -> ProviderHostLifetime:
        self.require_registered(execution_profile)
        return self._registrations[execution_profile].lifetime

    def shared_runtime_command(
        self,
        execution_profile: ProviderExecutionProfile,
    ) -> tuple[str, ...]:
        self.require_registered(execution_profile)
        registration = self._registrations[execution_profile]
        if registration.lifetime is not ProviderHostLifetime.SHARED_PROCESS:
            raise ValueError("dedicated provider has no shared runtime command")
        command = registration.shared_runtime_command
        if command is None:
            raise RuntimeError("shared provider registration lost its runtime command")
        return command

    def create_host(
        self,
        execution_profile: ProviderExecutionProfile,
        config: object,
    ) -> ProviderHost:
        self.require_registered(execution_profile)
        registration = self._registrations[execution_profile]
        if not isinstance(config, registration.config_type):
            raise TypeError(
                f"provider {execution_profile.provider.value}/"
                f"{execution_profile.profile.value} requires "
                f"{registration.config_type.__name__}; "
                f"got {type(config).__name__}"
            )
        host = registration.factory(config)
        self._validate_host(execution_profile, host)
        return host

    def compose_host(
        self,
        execution_profile: ProviderExecutionProfile,
        configuration: ProviderHostRootConfiguration,
        initial_session_config: AgentSessionConfig,
    ) -> ComposedProviderHost:
        self.require_registered(execution_profile)
        if not isinstance(configuration, ProviderHostRootConfiguration):
            raise ValueError(
                "provider composition requires ProviderHostRootConfiguration"
            )
        if not isinstance(initial_session_config, AgentSessionConfig):
            raise ValueError("provider composition requires AgentSessionConfig")
        registration = self._registrations[execution_profile]
        if registration.composer is None:
            raise RuntimeError(
                f"provider {execution_profile.provider.value}/"
                f"{execution_profile.profile.value} has no production composition"
            )
        composed = registration.composer(configuration, initial_session_config)
        if not isinstance(composed, ComposedProviderHost):
            raise RuntimeError(
                f"provider composer {execution_profile.provider.value}/"
                f"{execution_profile.profile.value} returned {type(composed).__name__}"
            )
        self._validate_host(execution_profile, composed.host)
        declared = self._profile_catalog.supported_capabilities(execution_profile)
        actual = frozenset(
            capability
            for capability in AgentCapability
            if composed.initial_capabilities.supports(capability)
        )
        if actual != declared:
            raise RuntimeError(
                "provider capability snapshot differs from runtime profile catalog: "
                f"{execution_profile.provider.value}/{execution_profile.profile.value}"
            )
        return composed

    def _validate_key(self, execution_profile: ProviderExecutionProfile) -> None:
        if not isinstance(execution_profile, ProviderExecutionProfile):
            raise ValueError("provider registry key must be ProviderExecutionProfile")
        self._profile_catalog.definition(execution_profile)

    @staticmethod
    def _validate_host(
        execution_profile: ProviderExecutionProfile,
        host: ProviderHost,
    ) -> None:
        if (
            host.provider != execution_profile.provider
            or host.profile != execution_profile.profile
        ):
            raise RuntimeError(
                "provider composer returned host for "
                f"{host.provider.value}/{host.profile.value}; expected "
                f"{execution_profile.provider.value}/{execution_profile.profile.value}"
            )


class ProviderUnavailableError(LookupError):
    def __init__(self, execution_profile: ProviderExecutionProfile) -> None:
        if not isinstance(execution_profile, ProviderExecutionProfile):
            raise ValueError("unavailable provider must be ProviderExecutionProfile")
        self.execution_profile = execution_profile
        self.provider = execution_profile.provider
        self.profile = execution_profile.profile
        super().__init__(
            f"provider {self.provider.value} profile {self.profile.value} is unavailable"
        )


def session_event_from_open_result(
    result: OpenSessionResult,
) -> SessionStarted | SessionFailed:
    if isinstance(result, OpenFailed):
        return SessionFailed(result.error.message)
    return SessionStarted(
        session_ref=result.session_ref,
        effective_model=result.effective.model,
        effective_backend=result.effective.backend,
        capabilities=result.capabilities,
    )


def _validate_open_request(
    correlation_id: OpenSessionCorrelationId,
    config: AgentSessionConfig,
) -> None:
    if not isinstance(correlation_id, OpenSessionCorrelationId):
        raise ValueError("open session request requires OpenSessionCorrelationId")
    if not isinstance(config, AgentSessionConfig):
        raise ValueError("open session request requires AgentSessionConfig")
