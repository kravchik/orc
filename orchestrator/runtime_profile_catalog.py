"""Validated runtime-profile identity, defaults, and capability declarations."""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Iterable, Mapping

from orchestrator.agent_capabilities import AgentCapability
from orchestrator.agent_identity import (
    ProviderExecutionProfile,
    ProviderKind,
    ProviderProfileId,
    ProviderSessionRef,
)
from orchestrator.provider_installation import (
    RuntimeBundledExecutable,
    RuntimeInstallationContract,
)


ACP_RUNTIME_PROFILE_CONFIG_SCHEMA_VERSION = 2
ACP_ADAPTER_ID = "acp-v1"
ACP_ADAPTER_COMPONENT = "orc-acp-v1-adapter"
ACP_ADAPTER_VERSION = "1"
RUNTIME_PROFILE_CONFIG_ENV = "ORC_RUNTIME_PROFILE_CONFIG"
BUILTIN_RUNTIME_PROFILE_CONFIG_PATH = (
    Path(__file__).resolve().parents[1] / "config" / "agent_runtime_profiles.v2.json"
)
_PROFILE_CONFIG_MAX_BYTES = 1024 * 1024
_RUNTIME_ARG_MAX_COUNT = 32
_RUNTIME_ARG_MAX_BYTES = 1024
_RUNTIME_ARGS_MAX_BYTES = 8192
_TEXT_TURN_CAPABILITIES = frozenset(
    {
        AgentCapability.START_TURN,
        AgentCapability.TURN_INPUT_CORRELATION,
        AgentCapability.TEXT_STREAM,
    }
)
_ACP_IMPLEMENTED_CAPABILITIES = _TEXT_TURN_CAPABILITIES | frozenset(
    {
        AgentCapability.RESUME_SESSION,
        AgentCapability.LIST_SESSIONS,
        AgentCapability.SESSION_STORE,
        AgentCapability.INTERRUPT_TURN,
        AgentCapability.FORM_ELICITATION,
        AgentCapability.STRUCTURED_USER_INPUT,
        AgentCapability.CLIENT_FILESYSTEM_READ,
        AgentCapability.CLIENT_FILESYSTEM_WRITE,
        AgentCapability.CLIENT_TERMINAL,
        AgentCapability.LIST_MODELS,
        AgentCapability.SET_MODEL,
        AgentCapability.READ_EFFORT,
        AgentCapability.SET_EFFORT,
        AgentCapability.CONTEXT_USAGE,
    }
)
_ACP_CAPABILITY_DEPENDENCIES = {
    AgentCapability.TURN_INPUT_CORRELATION: frozenset(
        {AgentCapability.START_TURN}
    ),
    AgentCapability.TEXT_STREAM: frozenset(
        {
            AgentCapability.START_TURN,
            AgentCapability.TURN_INPUT_CORRELATION,
        }
    ),
    AgentCapability.RESUME_SESSION: frozenset({AgentCapability.SESSION_STORE}),
    AgentCapability.LIST_SESSIONS: frozenset({AgentCapability.SESSION_STORE}),
    AgentCapability.INTERRUPT_TURN: frozenset(
        {
            AgentCapability.START_TURN,
            AgentCapability.TURN_INPUT_CORRELATION,
        }
    ),
    AgentCapability.FORM_ELICITATION: frozenset(
        {
            AgentCapability.START_TURN,
            AgentCapability.TURN_INPUT_CORRELATION,
        }
    ),
    AgentCapability.CLIENT_FILESYSTEM_READ: frozenset(
        {
            AgentCapability.START_TURN,
            AgentCapability.TURN_INPUT_CORRELATION,
        }
    ),
    AgentCapability.CLIENT_FILESYSTEM_WRITE: frozenset(
        {
            AgentCapability.START_TURN,
            AgentCapability.TURN_INPUT_CORRELATION,
        }
    ),
    AgentCapability.CLIENT_TERMINAL: frozenset(
        {
            AgentCapability.START_TURN,
            AgentCapability.TURN_INPUT_CORRELATION,
        }
    ),
}


class RuntimeArgsMode(str, Enum):
    IMMUTABLE = "immutable"
    APPEND = "append"


class RuntimeProfileIntegration(str, Enum):
    NATIVE = "native"
    ACP_V1 = "ACP v1"


@dataclass(frozen=True)
class RuntimeProfileContractProjection:
    """Catalog-owned profile semantics shared by text consumers."""

    integration: RuntimeProfileIntegration
    args_mode: RuntimeArgsMode
    approval_policy_values: tuple[str, ...]
    sandbox_values: tuple[str, ...]

    @property
    def allows_args_suffix(self) -> bool:
        return self.args_mode is RuntimeArgsMode.APPEND


@dataclass(frozen=True)
class RuntimeLaunchConfigurationContract:
    """Profile-owned launch inputs that have an exact provider mapping."""

    args_mode: RuntimeArgsMode
    provider_approval_policy_values: frozenset[str]
    sandbox_values: frozenset[str]

    def __post_init__(self) -> None:
        if not isinstance(self.args_mode, RuntimeArgsMode):
            raise ValueError("runtime args mode must be RuntimeArgsMode")
        for field_name in ("provider_approval_policy_values", "sandbox_values"):
            values = frozenset(getattr(self, field_name))
            if any(
                not isinstance(item, str) or not item.strip() or item != item.strip()
                for item in values
            ):
                raise ValueError(f"{field_name} must contain normalized non-empty strings")
            object.__setattr__(self, field_name, values)


@dataclass(frozen=True)
class NormalizedRuntimeLaunchConfiguration:
    args: tuple[str, ...]
    provider_approval_policy: str
    sandbox: str


@dataclass(frozen=True)
class RuntimeLaunchDefaults:
    """Deployment-owned defaults applied only to supported fresh bindings."""

    provider_approval_policy: str
    sandbox: str

    def __post_init__(self) -> None:
        for field_name in ("provider_approval_policy", "sandbox"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or "\x00" in value:
                raise ValueError(f"runtime launch default {field_name} must be a string")
            object.__setattr__(self, field_name, value.strip())


DEFAULT_RUNTIME_LAUNCH_DEFAULTS = RuntimeLaunchDefaults(
    provider_approval_policy="on-request",
    sandbox="workspace-write",
)


IMMUTABLE_RUNTIME_LAUNCH_CONFIGURATION = RuntimeLaunchConfigurationContract(
    RuntimeArgsMode.IMMUTABLE,
    frozenset(),
    frozenset(),
)

CODEX_RUNTIME_LAUNCH_CONFIGURATION = RuntimeLaunchConfigurationContract(
    RuntimeArgsMode.APPEND,
    frozenset({"never", "on-failure", "on-request", "untrusted"}),
    frozenset({"danger-full-access", "read-only", "workspace-write"}),
)


def _strict_fields(
    value: object,
    *,
    field_name: str,
    required: frozenset[str],
) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be an object")
    missing = sorted(required.difference(value))
    unknown = sorted(set(value).difference(required))
    if missing:
        raise ValueError(f"{field_name} is missing fields: {missing!r}")
    if unknown:
        raise ValueError(f"{field_name} has unknown fields: {unknown!r}")
    return dict(value)


def _non_empty_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _bounded_display_name(value: object, *, field_name: str) -> str:
    display_name = _non_empty_string(value, field_name=field_name)
    if len(display_name) > 64 or any(char in "\r\n" for char in display_name):
        raise ValueError(f"{field_name} must be a single-line string of at most 64 characters")
    return display_name


def _string_tuple(
    value: object,
    *,
    field_name: str,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, list) or (
        not allow_empty and not value
    ) or any(not isinstance(item, str) or not item.strip() or "\x00" in item for item in value):
        qualifier = "a list" if allow_empty else "a non-empty list"
        raise ValueError(f"{field_name} must be {qualifier} of non-empty strings")
    normalized = tuple(item.strip() for item in value)
    if len(set(normalized)) != len(normalized):
        raise ValueError(f"{field_name} must not contain duplicates")
    return normalized


def _positive_int(value: object, *, field_name: str, minimum: int = 1) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise ValueError(f"{field_name} must be an integer >= {minimum}")
    return value


def _finite_timeout(value: object, *, field_name: str) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(float(value))
        or float(value) <= 0
    ):
        raise ValueError(f"{field_name} must be a finite positive number")
    return float(value)


@dataclass(frozen=True)
class AcpRuntimeProfileConfig:
    provider: ProviderKind
    profile: ProviderProfileId
    default: bool
    adapter: str
    command: tuple[str, ...]
    host_lifetime: str
    inherit_parent_environment: bool
    credential_ownership: str
    secret_sources: tuple[str, ...]
    runtime_display_name: str
    runtime_name: str
    backend_name: str
    backend_version: str
    protocol_version: int
    auth_method_ids: tuple[str, ...]
    edge_queue_capacity: int
    port_event_capacity: int
    turn_output_max_chars: int
    frame_max_bytes: int
    process_output_max_bytes: int
    stop_timeout_sec: float
    supported_capabilities: frozenset[AgentCapability]
    launch_configuration: RuntimeLaunchConfigurationContract

    @property
    def execution_profile(self) -> ProviderExecutionProfile:
        return ProviderExecutionProfile(self.provider, self.profile)


def _parse_acp_profile(value: object, *, source: Path, index: int) -> AcpRuntimeProfileConfig:
    prefix = f"{source}: profiles[{index}]"
    profile = _strict_fields(
        value,
        field_name=prefix,
        required=frozenset(
            {
                "provider",
                "profile",
                "default",
                "adapter",
                "command",
                "hostLifetime",
                "environmentPolicy",
                "runtime",
                "backend",
                "protocol",
                "limits",
                "capabilities",
                "launchConfiguration",
            }
        ),
    )
    provider = ProviderKind.parse(profile["provider"])
    profile_id = ProviderProfileId.parse(profile["profile"])
    default = profile["default"]
    if type(default) is not bool:
        raise ValueError(f"{prefix}.default must be boolean")
    adapter = _non_empty_string(profile["adapter"], field_name=f"{prefix}.adapter")
    if adapter != ACP_ADAPTER_ID:
        raise ValueError(f"{prefix}.adapter must be {ACP_ADAPTER_ID!r}")
    command = _string_tuple(profile["command"], field_name=f"{prefix}.command")
    host_lifetime = _non_empty_string(
        profile["hostLifetime"], field_name=f"{prefix}.hostLifetime"
    )
    if host_lifetime not in {"dedicated", "shared-process"}:
        raise ValueError(
            f"{prefix}.hostLifetime must be 'dedicated' or 'shared-process'"
        )

    environment = _strict_fields(
        profile["environmentPolicy"],
        field_name=f"{prefix}.environmentPolicy",
        required=frozenset(
            {"inheritParent", "credentialOwnership", "secretSources"}
        ),
    )
    if type(environment["inheritParent"]) is not bool:
        raise ValueError(f"{prefix}.environmentPolicy.inheritParent must be boolean")
    credential_ownership = _non_empty_string(
        environment["credentialOwnership"],
        field_name=f"{prefix}.environmentPolicy.credentialOwnership",
    )
    if credential_ownership not in {"none", "runtime-local"}:
        raise ValueError(
            f"{prefix}.environmentPolicy.credentialOwnership is unsupported"
        )
    secret_sources = _string_tuple(
        environment["secretSources"],
        field_name=f"{prefix}.environmentPolicy.secretSources",
        allow_empty=True,
    )
    if any(
        not source_ref.replace("-", "").replace("_", "").replace(".", "").isalnum()
        for source_ref in secret_sources
    ):
        raise ValueError(
            f"{prefix}.environmentPolicy.secretSources must contain symbolic references"
        )
    if secret_sources:
        raise ValueError(
            f"{prefix}.environmentPolicy.secretSources requires a configured resolver"
        )

    runtime = _strict_fields(
        profile["runtime"],
        field_name=f"{prefix}.runtime",
        required=frozenset({"displayName", "name"}),
    )
    runtime_display_name = _bounded_display_name(
        runtime["displayName"], field_name=f"{prefix}.runtime.displayName"
    )
    runtime_name = _non_empty_string(runtime["name"], field_name=f"{prefix}.runtime.name")
    backend = _strict_fields(
        profile["backend"],
        field_name=f"{prefix}.backend",
        required=frozenset({"name", "version"}),
    )
    backend_name = _non_empty_string(backend["name"], field_name=f"{prefix}.backend.name")
    backend_version = _non_empty_string(
        backend["version"], field_name=f"{prefix}.backend.version"
    )

    protocol = _strict_fields(
        profile["protocol"],
        field_name=f"{prefix}.protocol",
        required=frozenset({"version", "authMethods"}),
    )
    protocol_version = _positive_int(
        protocol["version"], field_name=f"{prefix}.protocol.version"
    )
    if protocol_version != 1:
        raise ValueError(f"{prefix}.protocol.version must be stable ACP v1")
    auth_method_ids = _string_tuple(
        protocol["authMethods"],
        field_name=f"{prefix}.protocol.authMethods",
        allow_empty=True,
    )

    limits = _strict_fields(
        profile["limits"],
        field_name=f"{prefix}.limits",
        required=frozenset(
            {
                "edgeQueueCapacity",
                "portEventCapacity",
                "turnOutputMaxChars",
                "frameMaxBytes",
                "processOutputMaxBytes",
                "stopTimeoutSec",
            }
        ),
    )
    capabilities_raw = _string_tuple(
        profile["capabilities"], field_name=f"{prefix}.capabilities"
    )
    try:
        capabilities = frozenset(AgentCapability(item) for item in capabilities_raw)
    except ValueError as exc:
        raise ValueError(f"{prefix}.capabilities contains unknown capability") from exc
    unsupported = capabilities.difference(_ACP_IMPLEMENTED_CAPABILITIES)
    if unsupported:
        raise ValueError(
            f"{prefix}.capabilities exceeds implemented ACP contract: "
            f"{sorted(item.value for item in unsupported)!r}"
        )
    for capability, dependencies in _ACP_CAPABILITY_DEPENDENCIES.items():
        if capability not in capabilities:
            continue
        missing = dependencies.difference(capabilities)
        if missing:
            raise ValueError(
                f"{prefix}.capabilities dependency violation: "
                f"{capability.value} requires "
                f"{sorted(item.value for item in missing)!r}"
            )

    launch = _strict_fields(
        profile["launchConfiguration"],
        field_name=f"{prefix}.launchConfiguration",
        required=frozenset(
            {"argsMode", "providerApprovalPolicyValues", "sandboxValues"}
        ),
    )
    try:
        args_mode = RuntimeArgsMode(
            _non_empty_string(
                launch["argsMode"],
                field_name=f"{prefix}.launchConfiguration.argsMode",
            )
        )
    except ValueError as exc:
        raise ValueError(
            f"{prefix}.launchConfiguration.argsMode is unsupported"
        ) from exc
    launch_configuration = RuntimeLaunchConfigurationContract(
        args_mode=args_mode,
        provider_approval_policy_values=frozenset(
            _string_tuple(
                launch["providerApprovalPolicyValues"],
                field_name=(
                    f"{prefix}.launchConfiguration.providerApprovalPolicyValues"
                ),
                allow_empty=True,
            )
        ),
        sandbox_values=frozenset(
            _string_tuple(
                launch["sandboxValues"],
                field_name=f"{prefix}.launchConfiguration.sandboxValues",
                allow_empty=True,
            )
        ),
    )
    if launch_configuration != IMMUTABLE_RUNTIME_LAUNCH_CONFIGURATION:
        raise ValueError(
            f"{prefix}.launchConfiguration must be immutable for ACP v1 profiles"
        )

    return AcpRuntimeProfileConfig(
        provider=provider,
        profile=profile_id,
        default=default,
        adapter=adapter,
        command=command,
        host_lifetime=host_lifetime,
        inherit_parent_environment=environment["inheritParent"],
        credential_ownership=credential_ownership,
        secret_sources=secret_sources,
        runtime_display_name=runtime_display_name,
        runtime_name=runtime_name,
        backend_name=backend_name,
        backend_version=backend_version,
        protocol_version=protocol_version,
        auth_method_ids=auth_method_ids,
        edge_queue_capacity=_positive_int(
            limits["edgeQueueCapacity"], field_name=f"{prefix}.limits.edgeQueueCapacity"
        ),
        port_event_capacity=_positive_int(
            limits["portEventCapacity"],
            field_name=f"{prefix}.limits.portEventCapacity",
            minimum=2,
        ),
        turn_output_max_chars=_positive_int(
            limits["turnOutputMaxChars"],
            field_name=f"{prefix}.limits.turnOutputMaxChars",
        ),
        frame_max_bytes=_positive_int(
            limits["frameMaxBytes"], field_name=f"{prefix}.limits.frameMaxBytes"
        ),
        process_output_max_bytes=_positive_int(
            limits["processOutputMaxBytes"],
            field_name=f"{prefix}.limits.processOutputMaxBytes",
        ),
        stop_timeout_sec=_finite_timeout(
            limits["stopTimeoutSec"], field_name=f"{prefix}.limits.stopTimeoutSec"
        ),
        supported_capabilities=capabilities,
        launch_configuration=launch_configuration,
    )


def _load_acp_profile_configs(path: Path) -> tuple[AcpRuntimeProfileConfig, ...]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ValueError(f"runtime profile config is unavailable: {path}") from exc
    if len(raw) > _PROFILE_CONFIG_MAX_BYTES:
        raise ValueError("runtime profile config exceeds size limit")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"runtime profile config is malformed: {path}") from exc
    root = _strict_fields(
        payload,
        field_name=str(path),
        required=frozenset({"schemaVersion", "profiles"}),
    )
    if root["schemaVersion"] == 1:
        raise ValueError(
            "runtime profile config schema v1 is unsupported; migrate to schema v2"
        )
    if root["schemaVersion"] != ACP_RUNTIME_PROFILE_CONFIG_SCHEMA_VERSION:
        raise ValueError(
            "runtime profile config schemaVersion must be "
            f"{ACP_RUNTIME_PROFILE_CONFIG_SCHEMA_VERSION}"
        )
    profiles = root["profiles"]
    if not isinstance(profiles, list):
        raise ValueError("runtime profile config profiles must be a list")
    return tuple(
        _parse_acp_profile(value, source=path, index=index)
        for index, value in enumerate(profiles)
    )


@dataclass(frozen=True)
class RuntimeProfileDefinition:
    provider: ProviderKind
    profile: ProviderProfileId
    default: bool
    supported_capabilities: frozenset[AgentCapability]
    launch_configuration: RuntimeLaunchConfigurationContract = (
        IMMUTABLE_RUNTIME_LAUNCH_CONFIGURATION
    )
    installation: RuntimeInstallationContract | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.provider, ProviderKind):
            raise ValueError("runtime profile provider must be ProviderKind")
        if not isinstance(self.profile, ProviderProfileId):
            raise ValueError("runtime profile ID must be ProviderProfileId")
        if type(self.default) is not bool:
            raise ValueError("runtime profile default flag must be boolean")
        capabilities = frozenset(self.supported_capabilities)
        if any(not isinstance(item, AgentCapability) for item in capabilities):
            raise ValueError("runtime profile capabilities must be AgentCapability values")
        object.__setattr__(self, "supported_capabilities", capabilities)
        if not isinstance(
            self.launch_configuration, RuntimeLaunchConfigurationContract
        ):
            raise ValueError(
                "runtime profile launch configuration must be "
                "RuntimeLaunchConfigurationContract"
            )
        if not isinstance(self.installation, RuntimeInstallationContract):
            raise ValueError(
                "runtime profile installation must be RuntimeInstallationContract"
            )

    @property
    def execution_profile(self) -> ProviderExecutionProfile:
        return ProviderExecutionProfile(self.provider, self.profile)


class RuntimeProfileCatalog:
    def __init__(
        self,
        definitions: Iterable[RuntimeProfileDefinition],
        *,
        acp_profile_configs: Iterable[AcpRuntimeProfileConfig] = (),
    ) -> None:
        ordered = tuple(definitions)
        if not ordered:
            raise ValueError("runtime profile catalog must not be empty")

        by_execution: dict[ProviderExecutionProfile, RuntimeProfileDefinition] = {}
        profile_providers: dict[ProviderProfileId, ProviderKind] = {}
        defaults: dict[ProviderKind, ProviderProfileId] = {}
        by_provider: dict[ProviderKind, list[RuntimeProfileDefinition]] = {}
        for definition in ordered:
            if not isinstance(definition, RuntimeProfileDefinition):
                raise ValueError(
                    "runtime profile catalog entries must be RuntimeProfileDefinition"
                )
            execution_profile = definition.execution_profile
            if execution_profile in by_execution:
                raise ValueError(
                    "duplicate runtime profile identity: "
                    f"{definition.provider.value}/{definition.profile.value}"
                )
            previous_provider = profile_providers.get(definition.profile)
            if previous_provider is not None and previous_provider != definition.provider:
                raise ValueError(
                    "runtime profile ID belongs to multiple providers: "
                    f"{definition.profile.value}"
                )
            if definition.default and definition.provider in defaults:
                raise ValueError(
                    "multiple default runtime profiles for provider: "
                    f"{definition.provider.value}"
                )
            by_execution[execution_profile] = definition
            profile_providers[definition.profile] = definition.provider
            by_provider.setdefault(definition.provider, []).append(definition)
            if definition.default:
                defaults[definition.provider] = definition.profile

        missing_defaults = [
            provider.value for provider in by_provider if provider not in defaults
        ]
        if missing_defaults:
            raise ValueError(
                "provider has no default runtime profile: "
                + ", ".join(missing_defaults)
            )

        self._definitions = ordered
        self._by_execution: Mapping[
            ProviderExecutionProfile, RuntimeProfileDefinition
        ] = MappingProxyType(by_execution)
        self._profile_providers: Mapping[ProviderProfileId, ProviderKind] = (
            MappingProxyType(profile_providers)
        )
        self._defaults: Mapping[ProviderKind, ProviderProfileId] = MappingProxyType(
            defaults
        )
        self._by_provider: Mapping[
            ProviderKind, tuple[RuntimeProfileDefinition, ...]
        ] = MappingProxyType(
            {provider: tuple(items) for provider, items in by_provider.items()}
        )
        acp_configs = tuple(acp_profile_configs)
        by_acp_execution: dict[ProviderExecutionProfile, AcpRuntimeProfileConfig] = {}
        for config in acp_configs:
            if not isinstance(config, AcpRuntimeProfileConfig):
                raise ValueError("ACP profile config must be AcpRuntimeProfileConfig")
            if config.execution_profile not in by_execution:
                raise ValueError(
                    "ACP profile config has no catalog definition: "
                    f"{config.provider.value}/{config.profile.value}"
                )
            if by_execution[config.execution_profile].default != config.default:
                raise ValueError("ACP profile config default differs from definition")
            if config.execution_profile in by_acp_execution:
                raise ValueError(
                    "duplicate ACP profile config: "
                    f"{config.provider.value}/{config.profile.value}"
                )
            by_acp_execution[config.execution_profile] = config
        self._acp_profile_configs: Mapping[
            ProviderExecutionProfile, AcpRuntimeProfileConfig
        ] = MappingProxyType(by_acp_execution)
        self._session_projector = ProviderSessionProjector(self)

    @property
    def definitions(self) -> tuple[RuntimeProfileDefinition, ...]:
        return self._definitions

    @property
    def execution_profiles(self) -> tuple[ProviderExecutionProfile, ...]:
        return tuple(item.execution_profile for item in self._definitions)

    @property
    def provider_ids(self) -> tuple[ProviderKind, ...]:
        return tuple(self._by_provider)

    @property
    def profile_ids(self) -> tuple[ProviderProfileId, ...]:
        return tuple(item.profile for item in self._definitions)

    @property
    def session_projector(self) -> ProviderSessionProjector:
        return self._session_projector

    def require_provider(self, value: object) -> ProviderKind:
        provider = value if isinstance(value, ProviderKind) else ProviderKind.parse(value)
        if provider not in self._by_provider:
            raise ValueError(f"unknown agent provider: {provider.value!r}")
        return provider

    def default_profile(self, provider: object) -> ProviderProfileId:
        resolved_provider = self.require_provider(provider)
        return self._defaults[resolved_provider]

    def profiles_for_provider(
        self,
        provider: object,
    ) -> tuple[RuntimeProfileDefinition, ...]:
        resolved_provider = self.require_provider(provider)
        return self._by_provider[resolved_provider]

    def provider_for_profile(self, profile: object) -> ProviderKind:
        resolved_profile = (
            profile
            if isinstance(profile, ProviderProfileId)
            else ProviderProfileId.parse(profile)
        )
        try:
            return self._profile_providers[resolved_profile]
        except KeyError as exc:
            raise ValueError(
                f"unknown agent provider profile: {resolved_profile.value!r}"
            ) from exc

    def resolve(
        self,
        provider: object,
        profile: object | None,
    ) -> ProviderExecutionProfile:
        resolved_provider = self.require_provider(provider)
        resolved_profile = (
            self._defaults[resolved_provider]
            if profile is None or (isinstance(profile, str) and not profile.strip())
            else (
                profile
                if isinstance(profile, ProviderProfileId)
                else ProviderProfileId.parse(profile)
            )
        )
        owner = self._profile_providers.get(resolved_profile)
        if owner is None:
            raise ValueError(
                f"unknown agent provider profile: {resolved_profile.value!r}"
            )
        if owner != resolved_provider:
            raise ValueError(
                f"provider profile {resolved_profile.value} does not belong to "
                f"provider {resolved_provider.value}"
            )
        return ProviderExecutionProfile(resolved_provider, resolved_profile)

    def definition(
        self,
        execution_profile: ProviderExecutionProfile,
    ) -> RuntimeProfileDefinition:
        if not isinstance(execution_profile, ProviderExecutionProfile):
            raise ValueError("runtime profile lookup requires ProviderExecutionProfile")
        try:
            return self._by_execution[execution_profile]
        except KeyError as exc:
            raise ValueError(
                "unknown runtime profile identity: "
                f"{execution_profile.provider.value}/{execution_profile.profile.value}"
            ) from exc

    def supported_capabilities(
        self,
        execution_profile: ProviderExecutionProfile,
    ) -> frozenset[AgentCapability]:
        return self.definition(execution_profile).supported_capabilities

    def profile_contract_projection(
        self,
        execution_profile: ProviderExecutionProfile,
    ) -> RuntimeProfileContractProjection:
        definition = self.definition(execution_profile)
        launch = definition.launch_configuration
        return RuntimeProfileContractProjection(
            integration=(
                RuntimeProfileIntegration.ACP_V1
                if self.is_acp_profile(execution_profile)
                else RuntimeProfileIntegration.NATIVE
            ),
            args_mode=launch.args_mode,
            approval_policy_values=tuple(
                sorted(launch.provider_approval_policy_values)
            ),
            sandbox_values=tuple(sorted(launch.sandbox_values)),
        )

    def installation_contract(
        self,
        execution_profile: ProviderExecutionProfile,
    ) -> RuntimeInstallationContract:
        installation = self.definition(execution_profile).installation
        assert isinstance(installation, RuntimeInstallationContract)
        return installation

    def normalize_launch_configuration(
        self,
        execution_profile: ProviderExecutionProfile,
        *,
        args: object = None,
        provider_approval_policy: object = None,
        sandbox: object = None,
        defaults: RuntimeLaunchDefaults | None = None,
    ) -> NormalizedRuntimeLaunchConfiguration:
        if defaults is not None and not isinstance(defaults, RuntimeLaunchDefaults):
            raise ValueError("runtime launch defaults must be RuntimeLaunchDefaults")
        contract = self.definition(execution_profile).launch_configuration
        raw_args = [] if args is None else args
        if not isinstance(raw_args, (list, tuple)):
            raise ValueError("launch setting args must be a list of strings")
        normalized_args = tuple(raw_args)
        if normalized_args and contract.args_mode is RuntimeArgsMode.IMMUTABLE:
            raise ValueError(
                "launch setting args is unsupported for profile "
                f"{execution_profile.profile.value}"
            )
        if len(normalized_args) > _RUNTIME_ARG_MAX_COUNT:
            raise ValueError("launch setting args exceeds item limit")
        args_bytes = 0
        for index, item in enumerate(normalized_args):
            if not isinstance(item, str) or not item.strip() or "\x00" in item:
                raise ValueError(
                    f"launch setting args[{index}] must be a non-empty string"
                )
            item_bytes = len(item.encode("utf-8"))
            if item_bytes > _RUNTIME_ARG_MAX_BYTES:
                raise ValueError(f"launch setting args[{index}] exceeds byte limit")
            args_bytes += item_bytes
        if args_bytes > _RUNTIME_ARGS_MAX_BYTES:
            raise ValueError("launch setting args exceeds aggregate byte limit")

        normalized_policy = self._normalize_launch_enum(
            execution_profile=execution_profile,
            field_name="approval_policy",
            explicit=provider_approval_policy,
            default_value=(defaults.provider_approval_policy if defaults else None),
            supported_values=contract.provider_approval_policy_values,
        )
        normalized_sandbox = self._normalize_launch_enum(
            execution_profile=execution_profile,
            field_name="sandbox",
            explicit=sandbox,
            default_value=(defaults.sandbox if defaults else None),
            supported_values=contract.sandbox_values,
        )
        return NormalizedRuntimeLaunchConfiguration(
            args=normalized_args,
            provider_approval_policy=normalized_policy,
            sandbox=normalized_sandbox,
        )

    @staticmethod
    def _normalize_launch_enum(
        *,
        execution_profile: ProviderExecutionProfile,
        field_name: str,
        explicit: object,
        default_value: str | None,
        supported_values: frozenset[str],
    ) -> str:
        if explicit is None:
            explicit_value = ""
        elif not isinstance(explicit, str):
            raise ValueError(f"launch setting {field_name} must be a string")
        else:
            explicit_value = explicit.strip()
        if explicit_value:
            if not supported_values:
                raise ValueError(
                    f"launch setting {field_name} is unsupported for profile "
                    f"{execution_profile.profile.value}"
                )
            if explicit_value not in supported_values:
                raise ValueError(
                    f"launch setting {field_name} value is unsupported for profile "
                    f"{execution_profile.profile.value}: {explicit_value!r}"
                )
            return explicit_value
        if not supported_values:
            return ""
        if default_value is None:
            return ""
        normalized_default = str(default_value or "").strip()
        if normalized_default not in supported_values:
            raise ValueError(
                f"default launch setting {field_name} value is unsupported for profile "
                f"{execution_profile.profile.value}: {normalized_default!r}"
            )
        return normalized_default

    def acp_profile_config(
        self,
        execution_profile: ProviderExecutionProfile,
    ) -> AcpRuntimeProfileConfig:
        self.definition(execution_profile)
        try:
            return self._acp_profile_configs[execution_profile]
        except KeyError as exc:
            raise ValueError(
                "runtime profile is not configured for adapter=acp-v1: "
                f"{execution_profile.provider.value}/{execution_profile.profile.value}"
            ) from exc

    def is_acp_profile(self, execution_profile: ProviderExecutionProfile) -> bool:
        self.definition(execution_profile)
        return execution_profile in self._acp_profile_configs


class ProviderSessionProjector:
    """Render session identity using one root-owned runtime profile catalog."""

    def __init__(self, catalog: RuntimeProfileCatalog) -> None:
        if not isinstance(catalog, RuntimeProfileCatalog):
            raise ValueError("session projector requires RuntimeProfileCatalog")
        self._catalog = catalog

    def default_profile(self, provider: object) -> ProviderProfileId:
        return self._catalog.default_profile(provider)

    def namespaced_id(self, session_ref: ProviderSessionRef) -> str:
        if not isinstance(session_ref, ProviderSessionRef):
            raise ValueError("session projector requires ProviderSessionRef")
        self._catalog.definition(session_ref.execution_profile)
        default_profile = self._catalog.default_profile(session_ref.provider)
        if session_ref.profile == default_profile:
            return f"{session_ref.provider.value}:{session_ref.native_id}"
        return (
            f"{session_ref.provider.value}/{session_ref.profile.value}:"
            f"{session_ref.native_id}"
        )

    def parse_namespaced_id(self, value: object) -> ProviderSessionRef:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("provider session id must be a non-empty string")
        owner, separator, native_id = value.strip().partition(":")
        if not separator or not native_id:
            raise ValueError("provider session id must be namespaced")
        provider_name, profile_separator, profile_name = owner.partition("/")
        provider = self._catalog.require_provider(provider_name)
        profile = (
            ProviderProfileId.parse(profile_name)
            if profile_separator
            else self._catalog.default_profile(provider)
        )
        execution_profile = ProviderExecutionProfile(provider, profile)
        self._catalog.definition(execution_profile)
        return ProviderSessionRef(provider, native_id, profile)


_CODEX_CAPABILITIES = frozenset(
    {
        AgentCapability.START_TURN,
        AgentCapability.RESUME_SESSION,
        AgentCapability.LIST_SESSIONS,
        AgentCapability.TURN_INPUT_CORRELATION,
        AgentCapability.TEXT_STREAM,
        AgentCapability.LIVE_COMMAND_OUTPUT,
        AgentCapability.SERVER_TOOL_EVENTS,
        AgentCapability.STEER_ACTIVE_TURN,
        AgentCapability.STEER_RECEIPT,
        AgentCapability.INTERRUPT_TURN,
        AgentCapability.INTERRUPT_RECEIPT,
        AgentCapability.APPROVAL_ALWAYS,
        AgentCapability.SET_MODEL,
        AgentCapability.CONTEXT_USAGE,
    }
)

_CLAUDE_CAPABILITIES = _TEXT_TURN_CAPABILITIES | frozenset(
    {
        AgentCapability.RESUME_SESSION,
        AgentCapability.LIST_SESSIONS,
        AgentCapability.SESSION_STORE,
        AgentCapability.INTERRUPT_TURN,
        AgentCapability.STRUCTURED_USER_INPUT,
        AgentCapability.SET_MODEL,
        AgentCapability.CONTEXT_USAGE,
    }
)

_NATIVE_RUNTIME_PROFILE_DEFINITIONS = (
        RuntimeProfileDefinition(
            ProviderKind.CODEX,
            ProviderProfileId.CODEX_NATIVE,
            True,
            _CODEX_CAPABILITIES,
            CODEX_RUNTIME_LAUNCH_CONFIGURATION,
            RuntimeInstallationContract(("codex", "app-server")),
        ),
        RuntimeProfileDefinition(
            ProviderKind.CLAUDE,
            ProviderProfileId.CLAUDE_NATIVE,
            True,
            _CLAUDE_CAPABILITIES,
            IMMUTABLE_RUNTIME_LAUNCH_CONFIGURATION,
            RuntimeInstallationContract(
                ("claude",),
                RuntimeBundledExecutable(
                    "claude_agent_sdk",
                    "_bundled/claude",
                    "_bundled/claude.exe",
                ),
            ),
        ),
        RuntimeProfileDefinition(
            ProviderKind.OPENCODE,
            ProviderProfileId.OPENCODE_NATIVE,
            True,
            frozenset(
                {
                    *_TEXT_TURN_CAPABILITIES,
                    AgentCapability.RESUME_SESSION,
                    AgentCapability.LIST_SESSIONS,
                    AgentCapability.SESSION_STORE,
                    AgentCapability.STEER_ACTIVE_TURN,
                    AgentCapability.STEER_RECEIPT,
                    AgentCapability.INTERRUPT_TURN,
                    AgentCapability.STRUCTURED_USER_INPUT,
                    AgentCapability.APPROVAL_ALWAYS,
                    AgentCapability.SET_MODEL,
                    AgentCapability.CONTEXT_USAGE,
                }
            ),
            IMMUTABLE_RUNTIME_LAUNCH_CONFIGURATION,
            RuntimeInstallationContract(("opencode", "serve")),
        ),
)


def _catalog_from_acp_configs(
    acp_configs: Iterable[AcpRuntimeProfileConfig],
) -> RuntimeProfileCatalog:
    configs = tuple(acp_configs)
    definitions = list(_NATIVE_RUNTIME_PROFILE_DEFINITIONS)
    definitions.extend(
        RuntimeProfileDefinition(
            config.provider,
            config.profile,
            config.default,
            config.supported_capabilities,
            config.launch_configuration,
            RuntimeInstallationContract(config.command),
        )
        for config in configs
    )
    return RuntimeProfileCatalog(definitions, acp_profile_configs=configs)


def load_runtime_profile_catalog(
    trusted_local_config: str | Path | None = None,
) -> RuntimeProfileCatalog:
    builtins = _load_acp_profile_configs(BUILTIN_RUNTIME_PROFILE_CONFIG_PATH)
    local = (
        _load_acp_profile_configs(Path(trusted_local_config).expanduser())
        if trusted_local_config is not None
        else ()
    )
    return _catalog_from_acp_configs((*builtins, *local))


def load_runtime_profile_catalog_from_environment(
    environment: Mapping[str, str] | None = None,
) -> RuntimeProfileCatalog:
    source = os.environ if environment is None else environment
    configured = str(source.get(RUNTIME_PROFILE_CONFIG_ENV, "") or "").strip()
    return load_runtime_profile_catalog(configured or None)


BUILTIN_RUNTIME_PROFILE_CATALOG = load_runtime_profile_catalog()


def render_runtime_profile_prompt_contract(
    catalog: RuntimeProfileCatalog = BUILTIN_RUNTIME_PROFILE_CATALOG,
) -> str:
    lines = ["- Recognized runtime profile catalog:"]
    for provider in catalog.provider_ids:
        default_profile = catalog.default_profile(provider)
        profiles = ", ".join(
            (
                f"`{definition.profile.value}` (default)"
                if definition.profile == default_profile
                else f"`{definition.profile.value}`"
            )
            for definition in catalog.profiles_for_provider(provider)
        )
        lines.append(f"  - provider `{provider.value}` -> profiles {profiles}")
        for definition in catalog.profiles_for_provider(provider):
            projection = catalog.profile_contract_projection(
                definition.execution_profile
            )
            settings: list[str] = []
            if projection.allows_args_suffix:
                settings.append("`args` (bounded command suffix)")
            if projection.approval_policy_values:
                settings.append(
                    "`approval_policy`="
                    + "|".join(projection.approval_policy_values)
                )
            if projection.sandbox_values:
                settings.append(
                    "`sandbox`=" + "|".join(projection.sandbox_values)
                )
            lines.append(
                f"    - `{definition.profile.value}` integration: "
                f"{projection.integration.value}; "
                "launch settings: "
                + (", ".join(settings) if settings else "none (immutable)")
            )
    return "\n".join(lines)


RUNTIME_PROFILE_PROMPT_TOKEN = "{{RUNTIME_PROFILE_CATALOG}}"


def materialize_runtime_profile_prompt(
    prompt: str,
    catalog: RuntimeProfileCatalog = BUILTIN_RUNTIME_PROFILE_CATALOG,
) -> str:
    if not isinstance(prompt, str):
        raise ValueError("Steward prompt must be a string")
    token_count = prompt.count(RUNTIME_PROFILE_PROMPT_TOKEN)
    if token_count > 1:
        raise ValueError("Steward prompt contains duplicate runtime profile catalog tokens")
    contract = render_runtime_profile_prompt_contract(catalog)
    if token_count == 1:
        return prompt.replace(RUNTIME_PROFILE_PROMPT_TOKEN, contract)
    return f"{prompt.rstrip()}\n\n### Runtime profile catalog\n{contract}\n"
