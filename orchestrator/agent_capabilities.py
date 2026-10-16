"""Versioned provider capability snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import re
from types import MappingProxyType
from typing import Iterable, Mapping


CAPABILITY_SNAPSHOT_VERSION = 1
UNKNOWN_COMPONENT_VERSION = "unknown"
OBSERVED_RUNTIME_VERSION_MAX_CHARS = 128
_OBSERVED_RUNTIME_VERSION_PATTERN = re.compile(
    rf"[A-Za-z0-9][A-Za-z0-9._+-]{{0,{OBSERVED_RUNTIME_VERSION_MAX_CHARS - 1}}}\Z"
)


def normalize_observed_runtime_version(value: object, *, field_name: str) -> str:
    """Validate bounded product metadata reported by a live runtime handshake."""

    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    if _OBSERVED_RUNTIME_VERSION_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a bounded safe token")
    return value


class CapabilitySupport(StrEnum):
    SUPPORTED = "supported"
    UNSUPPORTED = "unsupported"


class AgentCapability(StrEnum):
    START_TURN = "start_turn"
    RESUME_SESSION = "resume_session"
    LIST_SESSIONS = "list_sessions"
    SESSION_RESET = "session_reset"
    SESSION_IDLE = "session_idle"
    DURABLE_EVENT_REPLAY = "durable_event_replay"
    TURN_ORIGIN = "turn_origin"
    TURN_INPUT_CORRELATION = "turn_input_correlation"
    TEXT_STREAM = "text_stream"
    LIVE_COMMAND_OUTPUT = "live_command_output"
    SERVER_TOOL_EVENTS = "server_tool_events"
    STEER_ACTIVE_TURN = "steer_active_turn"
    STEER_RECEIPT = "steer_receipt"
    INTERRUPT_TURN = "interrupt_turn"
    INTERRUPT_RECEIPT = "interrupt_receipt"
    STRUCTURED_USER_INPUT = "structured_user_input"
    CLIENT_FILESYSTEM_READ = "client_filesystem_read"
    CLIENT_FILESYSTEM_WRITE = "client_filesystem_write"
    CLIENT_TERMINAL = "client_terminal"
    FORM_ELICITATION = "form_elicitation"
    STRUCTURED_OUTPUT = "structured_output"
    APPROVAL_ALWAYS = "approval_always"
    APPROVAL_MODIFY_INPUT = "approval_modify_input"
    APPROVAL_SAVED_RULES = "approval_saved_rules"
    TOOL_POLICY_INSPECTION = "tool_policy_inspection"
    SET_MODEL = "set_model"
    LIST_MODELS = "list_models"
    SET_EFFORT = "set_effort"
    READ_EFFORT = "read_effort"
    CONTEXT_USAGE = "context_usage"
    MCP_STATUS = "mcp_status"
    SESSION_INSTRUCTION_ENTRIES = "session_instruction_entries"
    BACKGROUND_TASK_EVENTS = "background_task_events"
    BACKGROUND_TASK_INVENTORY = "background_task_inventory"
    STOP_BACKGROUND_TASK = "stop_background_task"
    SHELL_PROCESS_INVENTORY = "shell_process_inventory"
    SHELL_PROCESS_OUTPUT = "shell_process_output"
    STOP_SHELL_PROCESS = "stop_shell_process"
    NATIVE_SUBAGENTS = "native_subagents"
    VERBATIM_INTERNAL_INPUT = "verbatim_internal_input"
    SESSION_STORE = "session_store"


@dataclass(frozen=True)
class VersionedAgentComponent:
    name: str
    version: str = UNKNOWN_COMPONENT_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.name, str):
            raise ValueError("agent component name must be a string")
        if not isinstance(self.version, str):
            raise ValueError("agent component version must be a string")
        name = self.name.strip()
        if not name:
            raise ValueError("agent component name must not be empty")
        version = self.version.strip() or UNKNOWN_COMPONENT_VERSION
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "version", version)


@dataclass(frozen=True)
class AgentCapabilitySnapshot:
    schema_version: int
    adapter: VersionedAgentComponent
    runtime: VersionedAgentComponent
    backend: VersionedAgentComponent
    capabilities: Mapping[AgentCapability, CapabilitySupport]

    def __post_init__(self) -> None:
        if (
            type(self.schema_version) is not int
            or self.schema_version != CAPABILITY_SNAPSHOT_VERSION
        ):
            raise ValueError(
                "capability snapshot schema version must be integer "
                f"{CAPABILITY_SNAPSHOT_VERSION}; got {self.schema_version!r}"
            )
        for field_name in ("adapter", "runtime", "backend"):
            if not isinstance(getattr(self, field_name), VersionedAgentComponent):
                raise ValueError(
                    f"capability snapshot {field_name} must be VersionedAgentComponent"
                )
        values = dict(self.capabilities)
        if any(not isinstance(key, AgentCapability) for key in values):
            raise ValueError("capability snapshot keys must be AgentCapability values")
        if set(values) != set(AgentCapability):
            raise ValueError("capability snapshot must contain exactly one state per capability")
        if any(not isinstance(value, CapabilitySupport) for value in values.values()):
            raise ValueError("capability snapshot values must be binary supported/unsupported states")
        object.__setattr__(self, "capabilities", MappingProxyType(values))

    def supports(self, capability: AgentCapability) -> bool:
        return self.capabilities[capability] is CapabilitySupport.SUPPORTED

    def to_log_fields(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "adapter": self.adapter.name,
            "adapter_version": self.adapter.version,
            "runtime": self.runtime.name,
            "runtime_version": self.runtime.version,
            "backend": self.backend.name,
            "backend_version": self.backend.version,
            "capabilities": {
                capability.value: self.capabilities[capability].value
                for capability in AgentCapability
            },
        }


def _profile_capabilities(provider: str, profile: str) -> frozenset[AgentCapability]:
    from orchestrator.runtime_profile_catalog import BUILTIN_RUNTIME_PROFILE_CATALOG

    execution_profile = BUILTIN_RUNTIME_PROFILE_CATALOG.resolve(provider, profile)
    return BUILTIN_RUNTIME_PROFILE_CATALOG.supported_capabilities(execution_profile)


def build_capability_snapshot(
    *,
    adapter: VersionedAgentComponent,
    runtime: VersionedAgentComponent,
    backend: VersionedAgentComponent,
    supported: Iterable[AgentCapability],
) -> AgentCapabilitySnapshot:
    supported_set = frozenset(supported)
    if any(not isinstance(capability, AgentCapability) for capability in supported_set):
        raise ValueError("supported capabilities must be AgentCapability values")
    return AgentCapabilitySnapshot(
        schema_version=CAPABILITY_SNAPSHOT_VERSION,
        adapter=adapter,
        runtime=runtime,
        backend=backend,
        capabilities={
            capability: (
                CapabilitySupport.SUPPORTED
                if capability in supported_set
                else CapabilitySupport.UNSUPPORTED
            )
            for capability in AgentCapability
        },
    )


def codex_capability_snapshot(
    *,
    runtime_name: str = "codex-app-server",
    runtime_version: str = UNKNOWN_COMPONENT_VERSION,
    backend_name: str = "unknown",
    backend_version: str = UNKNOWN_COMPONENT_VERSION,
) -> AgentCapabilitySnapshot:
    return build_capability_snapshot(
        adapter=VersionedAgentComponent("codex", "1"),
        runtime=VersionedAgentComponent(runtime_name, runtime_version),
        backend=VersionedAgentComponent(backend_name or "unknown", backend_version),
        supported=_profile_capabilities("codex", "codex-native"),
    )


def claude_capability_snapshot(
    *,
    adapter_version: str,
    runtime_version: str = UNKNOWN_COMPONENT_VERSION,
    backend_name: str = "anthropic",
    backend_version: str = UNKNOWN_COMPONENT_VERSION,
) -> AgentCapabilitySnapshot:
    """Truthful capability snapshot for the Claude native session profile."""

    return build_capability_snapshot(
        adapter=VersionedAgentComponent("claude-agent-sdk-python", adapter_version),
        runtime=VersionedAgentComponent("claude-code", runtime_version),
        backend=VersionedAgentComponent(backend_name, backend_version),
        supported=_profile_capabilities("claude", "claude-native"),
    )


def opencode_capability_snapshot(
    *,
    adapter_version: str,
    runtime_version: str = UNKNOWN_COMPONENT_VERSION,
    backend_name: str = "openai",
    backend_version: str = UNKNOWN_COMPONENT_VERSION,
) -> AgentCapabilitySnapshot:
    """Truthful capability snapshot for the OpenCode native session profile."""

    return build_capability_snapshot(
        adapter=VersionedAgentComponent("opencode-api-http", adapter_version),
        runtime=VersionedAgentComponent("opencode", runtime_version),
        backend=VersionedAgentComponent(backend_name, backend_version),
        supported=_profile_capabilities("opencode", "opencode-native"),
    )


def acp_capability_snapshot(
    *,
    adapter_name: str,
    adapter_version: str,
    runtime_name: str,
    runtime_version: str,
    backend_name: str,
    backend_version: str,
    supported: Iterable[AgentCapability],
) -> AgentCapabilitySnapshot:
    """Build a truthful capability snapshot for a configured ACP profile."""

    return build_capability_snapshot(
        adapter=VersionedAgentComponent(adapter_name, adapter_version),
        runtime=VersionedAgentComponent(runtime_name, runtime_version),
        backend=VersionedAgentComponent(backend_name, backend_version),
        supported=supported,
    )


def unsupported_capability_text(capability: AgentCapability) -> str:
    return (
        "unsupported: current agent provider does not support "
        f"{capability.value}."
    )
