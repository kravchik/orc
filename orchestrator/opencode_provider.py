"""OpenCode HTTP provider with one process-root-owned shared host."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
import base64
import hashlib
from io import StringIO
import json
import math
import os
from pathlib import Path
from queue import Empty, Full, Queue
import secrets
import socket
import subprocess
import tempfile
import threading
import time
from types import MappingProxyType
from typing import Any, Mapping, assert_never
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from orchestrator import clock
from orchestrator.agent_capabilities import (
    AgentCapability,
    AgentCapabilitySnapshot,
    normalize_observed_runtime_version,
    opencode_capability_snapshot,
)
from orchestrator.agent_identity import (
    ApprovalRef,
    ProviderExecutionProfile,
    ProviderKind,
    ProviderProfileId,
    ProviderSessionRef,
    TurnRef,
)
from orchestrator.agent_inspection import (
    AgentBackgroundSnapshot,
    AgentContextSnapshot,
    AgentRuntimeProjection,
    AgentSessionSnapshot,
    AgentWorkSnapshot,
    SnapshotAvailability,
)
from orchestrator.agent_provider import (
    AgentSessionConfig,
    ComposedProviderHost,
    EffectiveRuntimeSnapshot,
    OpenFailed,
    OpenSessionCorrelationId,
    OpenSessionError,
    OpenSessionErrorKind,
    OpenSessionOperation,
    OpenSessionRequest,
    ListProviderSessions,
    Opened,
    ProviderHostRootConfiguration,
    ProviderHostLifetime,
    ProviderRegistrationContext,
    ProviderRegistry,
    ProviderSessionCatalogEntry,
    ProviderSessionCatalogErrorKind,
    ProviderSessionsListed,
    ProviderSessionsListFailed,
    ProviderSessionsListResult,
    ResumeSessionRequest,
    StartSessionRequest,
    StopHost,
    StopHostResult,
    validate_timeout_seconds,
)
from orchestrator.agent_session import (
    AgentEffectOutcome,
    AgentEffectResult,
    AgentApprovalKind,
    AgentApprovalRequest,
    AgentSessionEffect,
    AgentSessionEvent,
    BoundedIdentityHistory,
    ApprovalRequested,
    ApprovalDecisionResult,
    CloseSession,
    InterruptTurn,
    InspectionCompleted,
    ModelSelectionAccepted,
    ModelSelectionFailed,
    ModelSelectionUncertain,
    OutputDelta,
    RawProviderErrorPayload,
    RefreshInspection,
    ResolveApproval,
    SessionFailed,
    SetEffort,
    SetModel,
    StartTurn,
    TurnCompleted,
    TurnFailed,
    TurnInterrupted,
    TurnStarted,
    TrySteerTurn,
    approval_decision_preflight,
    unsupported_effect_result,
)
from orchestrator.agent_io import AgentAttachmentRef, AgentInput, AgentOutput
from orchestrator.processes import LifecycleLogger


OPENCODE_API_ADAPTER_COMPONENT = "opencode-api-http"
OPENCODE_API_ADAPTER_VERSION = "2"
OPENCODE_ANONYMOUS_PROVIDER = "opencode"
_EDGE_QUEUE_CAPACITY = 256
_STARTUP_ADMISSION_CAPACITY = 256
_EDGE_STOP_TIMEOUT_SEC = 20.0
_EDGE_POLL_INTERVAL_SEC = 0.01
_PROJECTION_POLL_INTERVAL_SEC = 0.05
_SESSION_CLOSE_RETRY_INTERVAL_SEC = 0.05
_HTTP_ERROR_DETAIL_MAX_CHARS = 256
_TURN_OUTPUT_MAX_CHARS = 1_048_576
_PORT_EVENT_CAPACITY = 256
_MESSAGE_PROJECTION_MAX_BYTES = 2_097_152
_MESSAGE_PROJECTION_MAX_MESSAGES = 256
_MESSAGE_CONTENT_MAX_ITEMS = 256
_NATIVE_MESSAGE_ID_MAX_CHARS = 512
_MODEL_SELECTION_MAX_CHARS = 512
_TOKEN_COUNT_MAX = 9_007_199_254_740_991
_LIVE_RECONNECT_DELAY_SEC = 0.02
_PROCESS_OUTPUT_MAX_BYTES = 8_192
_PROCESS_OUTPUT_READER_STOP_TIMEOUT_SEC = 1.0
_HTTP_REQUEST_TIMEOUT_SEC = 0.2
_PROMPT_ADMISSION_TIMEOUT_SEC = 5.0
_MESSAGE_PROJECTION_MAX_CONSECUTIVE_READ_FAILURES = 4
_PERMISSION_PROJECTION_MAX_REQUESTS = 64
_PERMISSION_DEDUP_HISTORY_CAPACITY = 64
_PERMISSION_RESOURCE_MAX_ITEMS = 32
_PERMISSION_VALUE_MAX_CHARS = 2_048
_ATTACHMENT_MAX_ITEMS = 32
_ATTACHMENT_MAX_BYTES = 20 * 1024 * 1024
_ATTACHMENT_TOTAL_MAX_BYTES = 20 * 1024 * 1024
_ATTACHMENT_MEDIA_TYPE_MAX_CHARS = 128
_ATTACHMENT_NAME_MAX_CHARS = 1_024
_SUPPORTED_ATTACHMENT_IMAGE_TYPES = frozenset(
    {"image/jpeg", "image/png", "image/gif", "image/webp"}
)
_ORC_PERMISSION_CONFIG_CONTENT = json.dumps(
    {
        "default_agent": "build",
        "permissions": [{"action": "*", "resource": "*", "effect": "ask"}],
        "agents": {
            "build": {
                "permissions": [
                    {"action": "*", "resource": "*", "effect": "ask"}
                ]
            }
        },
    },
    separators=(",", ":"),
    sort_keys=True,
)


class _OpenCodeAttachmentError(ValueError):
    pass

class _BoundedOpenCodeProcessOutput:
    """Drain provider output without retaining unbounded or outward-visible text."""

    def __init__(self, capacity: int = _PROCESS_OUTPUT_MAX_BYTES) -> None:
        if not isinstance(capacity, int) or isinstance(capacity, bool) or capacity <= 0:
            raise ValueError("OpenCode process output capacity must be positive")
        self._capacity = capacity
        self._buffer = bytearray()
        self._truncated = False
        self._reader_failed = False
        self._lock = threading.Lock()

    def append(self, chunk: bytes) -> None:
        with self._lock:
            remaining = self._capacity - len(self._buffer)
            if remaining > 0:
                self._buffer.extend(chunk[:remaining])
            if len(chunk) > remaining:
                self._truncated = True

    def mark_reader_failed(self) -> None:
        with self._lock:
            self._reader_failed = True

    def metadata(self) -> tuple[int, bool, bool]:
        with self._lock:
            return len(self._buffer), self._truncated, self._reader_failed


def _drain_opencode_process_output(
    stream: Any,
    output: _BoundedOpenCodeProcessOutput,
) -> None:
    try:
        while True:
            chunk = stream.read(4096)
            if not chunk:
                return
            output.append(chunk)
    except (OSError, ValueError):
        output.mark_reader_failed()


@dataclass(frozen=True)
class OpenCodeProviderConfig:
    logger: LifecycleLogger
    cli_path: Path | None = None
    env: Mapping[str, str] | None = field(default=None, repr=False)
    role: str = "agent"
    startup_timeout_sec: float = 120.0
    rpc_timeout_sec: float = 5.0
    prompt_admission_timeout_sec: float = _PROMPT_ADMISSION_TIMEOUT_SEC
    edge_thread_name: str = "opencode-api-http-thread"
    edge_queue_capacity: int = _EDGE_QUEUE_CAPACITY
    startup_admission_capacity: int = _STARTUP_ADMISSION_CAPACITY
    edge_stop_timeout_sec: float = _EDGE_STOP_TIMEOUT_SEC
    server_username: str = "opencode"
    server_password: str = field(default="", repr=False)
    turn_output_max_chars: int = _TURN_OUTPUT_MAX_CHARS
    port_event_capacity: int = _PORT_EVENT_CAPACITY
    session_store_root: Path | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.logger, LifecycleLogger):
            raise ValueError("OpenCode provider logger must be LifecycleLogger")
        if self.cli_path is not None and not isinstance(self.cli_path, Path):
            raise ValueError("OpenCode provider cli_path must be Path or None")
        if self.session_store_root is not None and not isinstance(
            self.session_store_root, Path
        ):
            raise ValueError("OpenCode provider session_store_root must be Path or None")
        if not isinstance(self.role, str) or not self.role.strip():
            raise ValueError("OpenCode provider role must be a non-empty string")
        startup_timeout_sec = validate_timeout_seconds(
            self.startup_timeout_sec,
            field_name="OpenCode provider startup timeout",
        )
        rpc_timeout_sec = validate_timeout_seconds(
            self.rpc_timeout_sec,
            field_name="OpenCode provider RPC timeout",
        )
        prompt_admission_timeout_sec = validate_timeout_seconds(
            self.prompt_admission_timeout_sec,
            field_name="OpenCode prompt admission timeout",
        )
        if not isinstance(self.edge_thread_name, str) or not self.edge_thread_name.strip():
            raise ValueError("OpenCode provider edge thread name must be non-empty")
        if (
            not isinstance(self.edge_queue_capacity, int)
            or isinstance(self.edge_queue_capacity, bool)
            or self.edge_queue_capacity <= 0
        ):
            raise ValueError("OpenCode provider edge queue capacity must be positive")
        if (
            not isinstance(self.startup_admission_capacity, int)
            or isinstance(self.startup_admission_capacity, bool)
            or self.startup_admission_capacity <= 0
        ):
            raise ValueError("OpenCode startup admission capacity must be positive")
        edge_stop_timeout_sec = validate_timeout_seconds(
            self.edge_stop_timeout_sec,
            field_name="OpenCode provider edge stop timeout",
        )
        if not isinstance(self.server_username, str) or not self.server_username.strip():
            raise ValueError("OpenCode server username must be non-empty")
        if not isinstance(self.server_password, str):
            raise ValueError("OpenCode server password must be a string")
        if (
            not isinstance(self.turn_output_max_chars, int)
            or isinstance(self.turn_output_max_chars, bool)
            or self.turn_output_max_chars <= 0
        ):
            raise ValueError("OpenCode provider turn output limit must be positive")
        if (
            not isinstance(self.port_event_capacity, int)
            or isinstance(self.port_event_capacity, bool)
            or self.port_event_capacity < 2
        ):
            raise ValueError("OpenCode provider port event capacity must be at least 2")
        env = dict(self.env or {})
        if any(not isinstance(key, str) or not isinstance(value, str) for key, value in env.items()):
            raise ValueError("OpenCode provider env must contain string keys and values")
        object.__setattr__(self, "env", MappingProxyType(env))
        object.__setattr__(self, "role", self.role.strip())
        object.__setattr__(self, "startup_timeout_sec", startup_timeout_sec)
        object.__setattr__(self, "rpc_timeout_sec", rpc_timeout_sec)
        object.__setattr__(
            self,
            "prompt_admission_timeout_sec",
            prompt_admission_timeout_sec,
        )
        object.__setattr__(self, "edge_thread_name", self.edge_thread_name.strip())
        object.__setattr__(self, "edge_stop_timeout_sec", edge_stop_timeout_sec)
        object.__setattr__(self, "server_username", self.server_username.strip())
        object.__setattr__(
            self,
            "server_password",
            self.server_password or secrets.token_urlsafe(32),
        )


@dataclass(frozen=True)
class _OpenCommand:
    correlation_id: OpenSessionCorrelationId
    config: AgentSessionConfig
    resume_session_ref: ProviderSessionRef | None = None


@dataclass(frozen=True)
class _ListSessionsCommand:
    request: ListProviderSessions
    reply: Queue[ProviderSessionsListResult]
    cancelled: threading.Event = field(default_factory=threading.Event)


@dataclass(frozen=True)
class _CancelOpenCommand:
    correlation_id: OpenSessionCorrelationId


@dataclass(frozen=True)
class _CloseSessionCommand:
    session_id: str


@dataclass(frozen=True)
class _DetachSessionCommand:
    session_id: str


@dataclass(frozen=True)
class _StartTurnCommand:
    session_id: str
    turn_ref: TurnRef
    message_id: str
    text: str
    files: tuple[dict[str, str], ...] = ()


@dataclass(frozen=True)
class _SetModelCommand:
    session_id: str
    requested_model: str


class _ControlHandoffState(Enum):
    QUEUED = auto()
    DISPATCHING = auto()
    PROVIDER_STARTED = auto()
    ABANDONED = auto()
    COMPLETED = auto()
    CANCELLED = auto()


class _ControlHandoffCompletion(Enum):
    DELIVERED = auto()
    ABANDONED = auto()
    CANCELLED = auto()


class _ControlCommandHandoff:
    """Keep one bounded owner while a control command crosses the edge thread."""

    def __init__(self) -> None:
        self._state = _ControlHandoffState.QUEUED
        self._result: AgentEffectResult | None = None
        self._done = threading.Event()
        self._lock = threading.Lock()

    def claim_dispatch(self) -> bool:
        with self._lock:
            if self._state is _ControlHandoffState.CANCELLED:
                return False
            if self._state is not _ControlHandoffState.QUEUED:
                raise RuntimeError("OpenCode control command was dispatched more than once")
            self._state = _ControlHandoffState.DISPATCHING
            return True

    def begin_provider_call(self) -> bool:
        with self._lock:
            if self._state is _ControlHandoffState.CANCELLED:
                return False
            if self._state is not _ControlHandoffState.DISPATCHING:
                raise RuntimeError("OpenCode control command provider call is out of order")
            self._state = _ControlHandoffState.PROVIDER_STARTED
            return True

    def complete(self, result: AgentEffectResult) -> _ControlHandoffCompletion:
        with self._lock:
            if self._state is _ControlHandoffState.CANCELLED:
                return _ControlHandoffCompletion.CANCELLED
            if self._state not in {
                _ControlHandoffState.DISPATCHING,
                _ControlHandoffState.PROVIDER_STARTED,
                _ControlHandoffState.ABANDONED,
            }:
                raise RuntimeError("OpenCode control command completed out of order")
            abandoned = self._state is _ControlHandoffState.ABANDONED
            self._state = _ControlHandoffState.COMPLETED
            self._result = result
            self._done.set()
            return (
                _ControlHandoffCompletion.ABANDONED
                if abandoned
                else _ControlHandoffCompletion.DELIVERED
            )

    def wait(
        self,
        timeout_sec: float,
        *,
        operation: str,
    ) -> AgentEffectResult:
        if self._done.wait(timeout_sec):
            return self._completed_result()
        with self._lock:
            if self._state is _ControlHandoffState.COMPLETED:
                assert self._result is not None
                return self._result
            if self._state in {
                _ControlHandoffState.QUEUED,
                _ControlHandoffState.DISPATCHING,
            }:
                self._state = _ControlHandoffState.CANCELLED
                return AgentEffectResult.rejected(
                    f"OpenCode {operation} timed out before provider dispatch"
                )
            if self._state is _ControlHandoffState.PROVIDER_STARTED:
                self._state = _ControlHandoffState.ABANDONED
                return AgentEffectResult.uncertain(
                    f"OpenCode {operation} delivery outcome is uncertain"
                )
            raise RuntimeError("OpenCode control command wait state is invalid")

    def _completed_result(self) -> AgentEffectResult:
        with self._lock:
            if (
                self._state is not _ControlHandoffState.COMPLETED
                or self._result is None
            ):
                raise RuntimeError("OpenCode control command completed without a result")
            return self._result


@dataclass(frozen=True)
class _SteerTurnCommand:
    session_id: str
    turn_ref: TurnRef
    message_id: str
    text: str
    files: tuple[dict[str, str], ...]
    handoff: _ControlCommandHandoff


@dataclass(frozen=True)
class _InterruptTurnCommand:
    session_id: str
    turn_ref: TurnRef
    handoff: _ControlCommandHandoff


@dataclass(frozen=True)
class _ApprovalDecisionCommand:
    session_id: str
    turn_ref: TurnRef
    approval_ref: ApprovalRef
    decision: str
    handoff: _ControlCommandHandoff


@dataclass(frozen=True)
class _StopEdgeCommand:
    pass


@dataclass
class _ClosingSession:
    next_retry_at: float = 0.0
    retry_logged: bool = False


_EdgeCommand = (
    _OpenCommand
    | _ListSessionsCommand
    | _CancelOpenCommand
    | _CloseSessionCommand
    | _DetachSessionCommand
    | _StartTurnCommand
    | _SetModelCommand
    | _SteerTurnCommand
    | _InterruptTurnCommand
    | _ApprovalDecisionCommand
    | _StopEdgeCommand
)


@dataclass
class _CorrelatedSteer:
    message_id: str


@dataclass(frozen=True)
class _ControlCommandExecution:
    result: AgentEffectResult
    poll_projection: bool = False


@dataclass
class _CorrelatedEdgeTurn:
    turn_ref: TurnRef
    message_id: str
    baseline_message_ids: frozenset[str]
    admitted_created: float = 0.0
    started: bool = False
    projection_read_failures: int = 0
    observed_auxiliary_items: set[tuple[str, int, str]] = field(default_factory=set)
    steers: list[_CorrelatedSteer] = field(default_factory=list)
    assistant_message_ids: list[str] = field(default_factory=list)
    assistant_text_by_id: dict[str, str] = field(default_factory=dict)
    completed_assistant_ids: set[str] = field(default_factory=set)
    interrupt_requested: bool = False
    interrupt_assistant_message_id: str | None = None
    permission_read_failures: int = 0
    active_read_failures: int = 0
    approval_sequence: int = 0
    active_permission: _PendingNativePermission | None = None
    resolved_permission_ids: BoundedIdentityHistory[str] = field(
        default_factory=lambda: BoundedIdentityHistory(
            _PERMISSION_DEDUP_HISTORY_CAPACITY
        )
    )


@dataclass(frozen=True)
class _ExpectedModelTransition:
    previous: _OpenCodeModelSelection
    model: _OpenCodeModelSelection


@dataclass
class _EdgeSessionState:
    known_message_ids: frozenset[str] = frozenset()
    diagnosed_message_ids: set[str] = field(default_factory=set)
    diagnostic_capacity_exhausted: bool = False
    projection_initialized: bool = False
    pending_model_transitions: list[_ExpectedModelTransition] = field(
        default_factory=list
    )
    correlated_turn: _CorrelatedEdgeTurn | None = None
    model: _OpenCodeModelSelection | None = None
    context_window: int | None = None
    failed: bool = False


@dataclass(frozen=True)
class _NativePermissionRequest:
    native_id: str
    session_id: str
    action: str
    resources: tuple[str, ...]
    save: tuple[str, ...]
    message: str
    command: str
    cwd: str
    source_message_id: str


@dataclass(frozen=True)
class _PendingNativePermission:
    native: _NativePermissionRequest
    request: AgentApprovalRequest


@dataclass(frozen=True)
class _ProjectedContent:
    item_type: str
    text: str = ""


@dataclass(frozen=True)
class _ProjectedMessage:
    native_id: str
    message_type: str
    created: float
    completed: bool
    failed: bool
    error_type: str | None
    raw_error: RawProviderErrorPayload | None
    outcome: str | None
    content: tuple[_ProjectedContent, ...]
    model: _OpenCodeModelSelection | None = None
    previous_model: _OpenCodeModelSelection | None = None
    used_tokens: int | None = None

    @property
    def output_text(self) -> str:
        return "".join(item.text for item in self.content if item.item_type == "text")


@dataclass(frozen=True)
class _OpenCodeModelSelection:
    provider_id: str
    model_id: str
    variant: str = ""

    @property
    def effective_model(self) -> str:
        base = f"{self.provider_id}/{self.model_id}"
        return f"{base}/{self.variant}" if self.variant else base

    def request_payload(self) -> dict[str, str]:
        result = {"providerID": self.provider_id, "id": self.model_id}
        if self.variant:
            result["variant"] = self.variant
        return result


@dataclass(frozen=True)
class _OpenCodeCatalogSnapshot:
    enabled_providers: frozenset[str]
    enabled_models: frozenset[tuple[str, str]]
    enabled_variants: frozenset[tuple[str, str, str]]
    default_model: _OpenCodeModelSelection
    context_windows: tuple[tuple[str, str, int], ...] = ()

    def context_window(self, model: _OpenCodeModelSelection) -> int | None:
        return next(
            (
                window
                for provider_id, model_id, window in self.context_windows
                if provider_id == model.provider_id and model_id == model.model_id
            ),
            None,
        )


@dataclass(frozen=True)
class _CatalogSessionMetadata:
    native_id: str
    cwd: str
    name: str
    created_at: str
    updated_at: str
    model_provider: str
    model_id: str
    model_variant: str


@dataclass(frozen=True)
class _OpenProgress:
    correlation_id: OpenSessionCorrelationId
    phase: str
    message: str


@dataclass(frozen=True)
class _OpenSucceeded:
    correlation_id: OpenSessionCorrelationId
    session_id: str
    model: str
    backend: str
    runtime_version: str
    created_native_session: bool
    session_name: str = ""
    created_at: str = ""
    updated_at: str = ""
    loaded: bool = False
    context_window: int | None = None


@dataclass(frozen=True)
class _OpenFailure:
    correlation_id: OpenSessionCorrelationId
    kind: OpenSessionErrorKind
    error: str


class _AmbiguousSessionCreateError(RuntimeError):
    """The create request was sent, but no native identity was received."""


@dataclass(frozen=True)
class _SessionFailure:
    session_id: str
    error: str


@dataclass(frozen=True)
class _SharedHostFailure:
    session_ids: tuple[str, ...]
    error: str


@dataclass(frozen=True)
class _TurnStartedEvent:
    session_id: str
    turn_ref: TurnRef


@dataclass(frozen=True)
class _TurnOutputEvent:
    session_id: str
    turn_ref: TurnRef
    text: str


@dataclass(frozen=True)
class _TurnCompletedEvent:
    session_id: str
    turn_ref: TurnRef
    model: str = ""
    backend: str = ""
    used_tokens: int | None = None
    context_window: int | None = None


@dataclass(frozen=True)
class _TurnFailedEvent:
    session_id: str
    turn_ref: TurnRef
    error: str
    raw_provider_error: RawProviderErrorPayload | None = None


@dataclass(frozen=True)
class _TurnInterruptedEvent:
    session_id: str
    turn_ref: TurnRef


@dataclass(frozen=True)
class _FatalTurnFailureEvent:
    session_id: str
    turn_ref: TurnRef
    error: str


@dataclass(frozen=True)
class _ApprovalRequestedEvent:
    session_id: str
    turn_ref: TurnRef
    request: AgentApprovalRequest


@dataclass(frozen=True)
class _ModelSelectionAcceptedEvent:
    session_id: str
    requested_model: str
    effective_model: str
    backend: str
    context_window: int | None


@dataclass(frozen=True)
class _ModelSelectionFailedEvent:
    session_id: str
    requested_model: str
    error: str


@dataclass(frozen=True)
class _ModelSelectionUncertainEvent:
    session_id: str
    requested_model: str
    error: str


_EdgeEvent = (
    _OpenProgress
    | _OpenSucceeded
    | _OpenFailure
    | _SessionFailure
    | _SharedHostFailure
    | _TurnStartedEvent
    | _TurnOutputEvent
    | _TurnCompletedEvent
    | _TurnFailedEvent
    | _TurnInterruptedEvent
    | _FatalTurnFailureEvent
    | _ApprovalRequestedEvent
    | _ModelSelectionAcceptedEvent
    | _ModelSelectionFailedEvent
    | _ModelSelectionUncertainEvent
)


class _OpenCodeApiEdge:
    """Own the foreground server, HTTP policy and durable session projections."""

    def __init__(self, config: OpenCodeProviderConfig) -> None:
        self._config = config
        self._commands: Queue[_EdgeCommand] = Queue(maxsize=config.edge_queue_capacity)
        self._events: Queue[_EdgeEvent] = Queue(maxsize=config.edge_queue_capacity)
        self._thread: threading.Thread | None = None
        self._process: subprocess.Popen[bytes] | None = None
        self._process_stdout: _BoundedOpenCodeProcessOutput | None = None
        self._process_stderr: _BoundedOpenCodeProcessOutput | None = None
        self._process_readers: tuple[threading.Thread, ...] = ()
        self._port = 0
        self._base_url = ""
        self._pending: OrderedDict[str, _OpenCommand] = OrderedDict()
        self._pending_catalog: list[_ListSessionsCommand] = []
        self._active_open: _OpenCommand | None = None
        self._sessions: dict[str, _EdgeSessionState] = {}
        self._closing_sessions: dict[str, _ClosingSession] = {}
        self._live_wakeups: Queue[str] = Queue(maxsize=config.edge_queue_capacity)
        self._live_stop = threading.Event()
        self._live_thread: threading.Thread | None = None
        self._http_error_secrets = _sensitive_values(config)
        self._server_ready = False
        self._runtime_version = ""
        self._catalog_snapshot: _OpenCodeCatalogSnapshot | None = None
        self._progress = False
        self._terminal_error = ""
        self._terminal_error_consumed = False
        self._state_lock = threading.Lock()
        self._stop_command_sent = False

    def submit_open(self, command: _OpenCommand) -> None:
        self._ensure_started()
        self._put_command(command)

    def cancel_open(self, correlation_id: OpenSessionCorrelationId) -> None:
        self._put_command(_CancelOpenCommand(correlation_id))

    def close_session(self, session_id: str) -> None:
        self._put_command(_CloseSessionCommand(session_id))

    def detach_session(self, session_id: str) -> None:
        self._put_command(_DetachSessionCommand(session_id))

    def list_sessions(
        self,
        request: ListProviderSessions,
    ) -> ProviderSessionsListResult:
        edge_error = self.terminal_error()
        if edge_error:
            return ProviderSessionsListFailed(
                ProviderSessionCatalogErrorKind.UNAVAILABLE,
                f"OpenCode provider edge is unavailable: {edge_error}",
            )
        if self._config.session_store_root is None:
            return ProviderSessionsListFailed(
                ProviderSessionCatalogErrorKind.UNAVAILABLE,
                "OpenCode session catalog requires an explicit ORC-owned store",
            )
        reply: Queue[ProviderSessionsListResult] = Queue(maxsize=1)
        command = _ListSessionsCommand(request, reply)
        self._ensure_started()
        try:
            self._put_command(command)
            return reply.get(timeout=self._config.startup_timeout_sec)
        except Empty:
            command.cancelled.set()
            return ProviderSessionsListFailed(
                ProviderSessionCatalogErrorKind.UNAVAILABLE,
                "OpenCode session catalog timed out",
            )
        except RuntimeError as exc:
            return ProviderSessionsListFailed(
                ProviderSessionCatalogErrorKind.UNAVAILABLE,
                _error_text(exc),
            )

    def submit_turn(self, command: _StartTurnCommand) -> AgentEffectResult:
        if self.terminal_error():
            return AgentEffectResult.rejected("OpenCode provider edge is unavailable")
        try:
            self._put_command(command)
        except RuntimeError as exc:
            return AgentEffectResult.rejected(_error_text(exc))
        return AgentEffectResult.accepted()

    def submit_model(self, command: _SetModelCommand) -> AgentEffectResult:
        if self.terminal_error():
            return AgentEffectResult.rejected("OpenCode provider edge is unavailable")
        try:
            self._put_command(command)
        except RuntimeError as exc:
            return AgentEffectResult.rejected(_error_text(exc))
        return AgentEffectResult.accepted()

    def submit_steer(self, command: _SteerTurnCommand) -> AgentEffectResult:
        if self.terminal_error():
            return AgentEffectResult.rejected("OpenCode provider edge is unavailable")
        try:
            self._put_command(command)
            return command.handoff.wait(
                self._config.prompt_admission_timeout_sec,
                operation="steer",
            )
        except RuntimeError as exc:
            return AgentEffectResult.rejected(_error_text(exc))

    def submit_interrupt(self, command: _InterruptTurnCommand) -> AgentEffectResult:
        if self.terminal_error():
            return AgentEffectResult.rejected("OpenCode provider edge is unavailable")
        try:
            self._put_command(command)
            return command.handoff.wait(
                self._config.rpc_timeout_sec,
                operation="interrupt",
            )
        except RuntimeError as exc:
            return AgentEffectResult.rejected(_error_text(exc))

    def submit_approval(
        self,
        command: _ApprovalDecisionCommand,
    ) -> AgentEffectResult:
        if self.terminal_error():
            return AgentEffectResult.rejected("OpenCode provider edge is unavailable")
        try:
            self._put_command(command)
            return command.handoff.wait(
                self._config.rpc_timeout_sec,
                operation="approval decision",
            )
        except RuntimeError as exc:
            return AgentEffectResult.rejected(_error_text(exc))

    def poll_events(self) -> list[_EdgeEvent]:
        events: list[_EdgeEvent] = []
        while True:
            try:
                events.append(self._events.get_nowait())
            except Empty:
                return events

    def consume_progress(self) -> bool:
        with self._state_lock:
            progressed = self._progress
            self._progress = False
            return progressed

    def consume_terminal_error(self) -> str:
        with self._state_lock:
            if self._terminal_error_consumed or not self._terminal_error:
                return ""
            self._terminal_error_consumed = True
            return self._terminal_error

    def terminal_error(self) -> str:
        with self._state_lock:
            return self._terminal_error

    def stop(self) -> None:
        thread = self._thread
        if thread is None and self._process is None:
            return
        deadline = time.monotonic() + self._config.edge_stop_timeout_sec
        if thread is not None and thread.is_alive() and not self._stop_command_sent:
            self._put_command(_StopEdgeCommand())
            self._stop_command_sent = True
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=min(0.1, max(0.0, deadline - time.monotonic())))
            if thread.is_alive():
                self._terminate_direct_process()
                thread.join(timeout=max(0.0, deadline - time.monotonic()))
            if thread.is_alive():
                raise RuntimeError("OpenCode API edge did not stop within cleanup deadline")
        if self._process is not None:
            self._stop_process()
        self._thread = None

    def _terminate_direct_process(self) -> None:
        process = self._process
        if process is None or process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=min(2.0, self._config.edge_stop_timeout_sec))
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=min(2.0, self._config.edge_stop_timeout_sec))

    def _ensure_started(self) -> None:
        with self._state_lock:
            if self._thread is not None:
                return
            self._thread = threading.Thread(
                target=self._thread_main,
                name=self._config.edge_thread_name,
                daemon=False,
            )
            self._thread.start()

    def _put_command(self, command: _EdgeCommand) -> None:
        try:
            self._commands.put_nowait(command)
        except Full as exc:
            raise RuntimeError("OpenCode provider edge command queue capacity exceeded") from exc
        self._mark_progress()

    def _thread_main(self) -> None:
        try:
            self._run()
        except _AmbiguousSessionCreateError:
            failure_kind = "session_create_uncertain"
            error = "OpenCode provider edge failed: session_create_uncertain"
            self._config.logger.event(
                "opencode_provider_edge_failed",
                provider=ProviderKind.OPENCODE.value,
                failure_kind=failure_kind,
            )
            with self._state_lock:
                self._terminal_error = error
            active = self._active_open
            self._active_open = None
            if active is not None:
                self._publish(
                    _OpenFailure(
                        active.correlation_id,
                        OpenSessionErrorKind.STARTUP_UNCERTAIN,
                        "OpenCode session create delivery outcome is uncertain",
                    )
                )
            self._publish_failures_for_all(OpenSessionErrorKind.STARTUP_FAILED, error)
            self._publish_session_failures(error)
        except BaseException as exc:
            failure_kind = _edge_failure_kind(exc)
            error = f"OpenCode provider edge failed: {failure_kind}"
            self._config.logger.event(
                "opencode_provider_edge_failed",
                provider=ProviderKind.OPENCODE.value,
                failure_kind=failure_kind,
            )
            with self._state_lock:
                self._terminal_error = error
            self._publish_failures_for_all(OpenSessionErrorKind.STARTUP_FAILED, error)
            self._publish_session_failures(error)
        finally:
            try:
                self._stop_process()
            except Exception as exc:
                with self._state_lock:
                    self._terminal_error = _error_text(exc)
            self._mark_progress()

    def _run(self) -> None:
        stopping = False
        last_projection_poll = 0.0
        while not stopping:
            while True:
                try:
                    command = self._commands.get_nowait()
                except Empty:
                    break
                if isinstance(command, _OpenCommand):
                    self._pending[command.correlation_id.value] = command
                    self._publish(
                        _OpenProgress(
                            command.correlation_id,
                            "server_starting",
                    "starting OpenCode foreground server",
                        )
                    )
                elif isinstance(command, _ListSessionsCommand):
                    if command.cancelled.is_set():
                        continue
                    if (
                        len(self._pending_catalog)
                        >= self._config.startup_admission_capacity
                    ):
                        command.reply.put_nowait(
                            ProviderSessionsListFailed(
                                ProviderSessionCatalogErrorKind.UNAVAILABLE,
                                "OpenCode session catalog admission capacity exceeded",
                            )
                        )
                    else:
                        self._pending_catalog.append(command)
                elif isinstance(command, _CancelOpenCommand):
                    cancelled = self._pending.pop(command.correlation_id.value, None)
                    if cancelled is not None:
                        self._publish(
                            _OpenFailure(
                                command.correlation_id,
                                OpenSessionErrorKind.CANCELLED,
                                "OpenCode session open cancelled",
                            )
                        )
                elif isinstance(command, _CloseSessionCommand):
                    self._schedule_session_close(command.session_id)
                elif isinstance(command, _DetachSessionCommand):
                    self._detach_session(command.session_id)
                elif isinstance(command, _StartTurnCommand):
                    self._start_turn(command)
                elif isinstance(command, _SetModelCommand):
                    self._set_model(command)
                elif isinstance(command, _SteerTurnCommand):
                    self._execute_control_command(command)
                elif isinstance(command, _InterruptTurnCommand):
                    self._execute_control_command(command)
                elif isinstance(command, _ApprovalDecisionCommand):
                    self._execute_control_command(command)
                else:
                    stopping = True
            if stopping:
                self._publish_failures_for_all(
                    OpenSessionErrorKind.CANCELLED,
                    "OpenCode provider host stopped during pending operation",
                )
                break
            self._observe_process_exit()
            if (self._pending or self._pending_catalog) and self._process is None:
                self._start_process()
            if (
                (self._pending or self._pending_catalog)
                and self._process is not None
                and (
                    not self._server_ready
                    or self._pending_waits_for_provider_catalog()
                )
            ):
                self._poll_server_info()
            if self._server_ready and self._pending_catalog:
                command = self._pending_catalog.pop(0)
                if not command.cancelled.is_set():
                    command.reply.put_nowait(self._list_sessions(command.request))
            if self._server_ready and self._pending:
                _key, command = next(iter(self._pending.items()))
                if not self._command_waits_for_provider_catalog(command):
                    self._pending.pop(_key)
                    self._active_open = command
                    try:
                        self._create_session(command)
                    except BaseException:
                        raise
                    else:
                        self._active_open = None
            now = time.monotonic()
            for session_id in self._consume_live_wakeups():
                if session_id in self._closing_sessions:
                    continue
                if not self._poll_active_correlated_projection(session_id):
                    state = self._sessions.get(session_id)
                    if (
                        state is not None
                        and not state.failed
                        and state.correlated_turn is None
                    ):
                        self._config.logger.event(
                            "opencode_stale_live_wakeup_ignored",
                            provider=ProviderKind.OPENCODE.value,
                        )
                    continue
            for session_id, closing in tuple(self._closing_sessions.items()):
                if now >= closing.next_retry_at:
                    self._close_remote_session(session_id)
            if (
                self._server_ready
                and self._sessions
                and now - last_projection_poll >= _PROJECTION_POLL_INTERVAL_SEC
            ):
                last_projection_poll = now
                for session_id in tuple(self._sessions):
                    if session_id in self._closing_sessions:
                        continue
                    self._poll_active_correlated_projection(session_id)
            time.sleep(_EDGE_POLL_INTERVAL_SEC)
        self._sessions.clear()
        self._closing_sessions.clear()

    def _command_waits_for_provider_catalog(self, command: _OpenCommand) -> bool:
        if command.resume_session_ref is not None or not command.config.model:
            return False
        catalog = self._catalog_snapshot
        if catalog is None:
            return True
        requested = _parse_startup_model(command.config.model)
        return requested.provider_id not in catalog.enabled_providers or (
            requested.provider_id != OPENCODE_ANONYMOUS_PROVIDER
            and (requested.provider_id, requested.model_id)
            not in catalog.enabled_models
        )

    def _pending_waits_for_provider_catalog(self) -> bool:
        return any(
            self._command_waits_for_provider_catalog(command)
            for command in self._pending.values()
        )

    def _start_process(self) -> None:
        port = _reserve_local_port()
        state_root = (
            tempfile.TemporaryDirectory(prefix="orc-opencode-state-")
            if self._config.session_store_root is None
            else None
        )
        profile_root = (
            Path(state_root.name)
            if state_root is not None
            else self._config.session_store_root
        )
        assert profile_root is not None
        profile_root.mkdir(parents=True, exist_ok=True)
        command = [
            str(self._config.cli_path or "opencode"),
            "serve",
            "--hostname",
            "127.0.0.1",
            "--port",
            str(port),
        ]
        env = dict(self._config.env or {})
        env.update(
            {
                "OPENCODE_SERVER_USERNAME": self._config.server_username,
                "OPENCODE_SERVER_PASSWORD": self._config.server_password,
                # Loaded after ambient/project config; the build rule prevents
                # a narrower provider allow from bypassing ORC approval policy.
                "OPENCODE_CONFIG_CONTENT": _ORC_PERMISSION_CONFIG_CONTENT,
                "XDG_CONFIG_HOME": str(profile_root / "config"),
                "XDG_DATA_HOME": str(profile_root / "data"),
                "XDG_STATE_HOME": str(profile_root / "state"),
            }
        )
        try:
            process = subprocess.Popen(
                command,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except Exception:
            if state_root is not None:
                state_root.cleanup()
            raise
        self._process = process
        assert process.stdout is not None
        assert process.stderr is not None
        self._process_stdout = _BoundedOpenCodeProcessOutput()
        self._process_stderr = _BoundedOpenCodeProcessOutput()
        self._process_readers = (
            threading.Thread(
                target=_drain_opencode_process_output,
                args=(process.stdout, self._process_stdout),
                name=f"{self._config.edge_thread_name}-stdout",
                daemon=False,
            ),
            threading.Thread(
                target=_drain_opencode_process_output,
                args=(process.stderr, self._process_stderr),
                name=f"{self._config.edge_thread_name}-stderr",
                daemon=False,
            ),
        )
        for reader in self._process_readers:
            reader.start()
        self._state_root = state_root
        self._port = port
        self._base_url = f"http://127.0.0.1:{port}"
        self._server_ready = False
        self._runtime_version = ""
        self._config.logger.event(
            "process_started",
            provider=ProviderKind.OPENCODE.value,
            role=self._config.role,
            pid=process.pid,
        )
        self._mark_progress()

    def _observe_process_exit(self) -> None:
        process = self._process
        if process is None:
            return
        returncode = process.poll()
        if returncode is None:
            return
        error = f"OpenCode server exited unexpectedly (returncode={returncode})"
        self._stop_live_reader()
        self._record_process_stopped(process)
        self._process = None
        self._server_ready = False
        self._runtime_version = ""
        self._catalog_snapshot = None
        self._cleanup_state_root()
        self._publish_failures_for_all(OpenSessionErrorKind.PROCESS_EXITED, error)
        self._publish_session_failures(error)

    def _poll_server_info(self) -> None:
        try:
            status, payload = self._request("GET", "/api/info")
        except (OSError, URLError):
            return
        if status == 503:
            return
        if status != 200 or not isinstance(payload, dict):
            self._publish_failures_for_all(
                OpenSessionErrorKind.STARTUP_FAILED,
                _http_error(
                    "OpenCode server info",
                    status,
                    payload,
                    secrets=self._http_error_secrets,
                ),
            )
            return
        try:
            version = normalize_observed_runtime_version(
                payload.get("version"),
                field_name="OpenCode API version",
            )
        except ValueError:
            self._publish_failures_for_all(
                OpenSessionErrorKind.STARTUP_FAILED,
                "OpenCode API info version is invalid",
            )
            return
        try:
            provider_status, provider_payload = self._request("GET", "/api/provider")
            model_status, model_payload = self._request("GET", "/api/model")
            default_status, default_payload = self._request("GET", "/api/model/default")
            credential_status, credential_payload = self._request("GET", "/api/credential")
        except (OSError, URLError):
            return
        responses = (
            ("provider catalog", provider_status, provider_payload),
            ("model catalog", model_status, model_payload),
            ("default model", default_status, default_payload),
            ("credential inventory", credential_status, credential_payload),
        )
        if any(status == 503 for _name, status, _payload in responses):
            return
        for name, response_status, response_payload in responses:
            if response_status != 200:
                self._publish_failures_for_all(
                    OpenSessionErrorKind.STARTUP_FAILED,
                    _http_error(f"OpenCode {name}", response_status, response_payload),
                )
                return
        try:
            catalog_snapshot = _validate_runtime_catalog(
                provider_payload,
                model_payload,
                default_payload,
                credential_payload,
            )
        except ValueError as exc:
            if _catalog_is_settling(provider_payload, model_payload, default_payload):
                return
            self._publish_failures_for_all(
                OpenSessionErrorKind.STARTUP_FAILED,
                f"AnonymousModelUnavailable: {_error_text(exc)}",
            )
            return
        self._runtime_version = version
        self._catalog_snapshot = catalog_snapshot
        self._server_ready = True
        default_model = catalog_snapshot.default_model
        for command in self._pending.values():
            self._publish(
                _OpenProgress(
                    command.correlation_id,
                    "server_ready",
                    f"OpenCode server {version} is ready with "
                    f"{default_model.effective_model}",
                )
            )

    def _create_session(self, command: _OpenCommand) -> None:
        if command.resume_session_ref is not None:
            self._resume_session(command)
            return
        catalog_snapshot = self._catalog_snapshot
        assert catalog_snapshot is not None
        try:
            selected_model = (
                _require_requested_startup_model(
                    catalog_snapshot,
                    command.config.model,
                )
                if command.config.model
                else _require_anonymous_fresh_model(catalog_snapshot)
            )
        except ValueError as exc:
            self._publish(
                _OpenFailure(
                    command.correlation_id,
                    (
                        OpenSessionErrorKind.INVALID_REQUEST
                        if command.config.model
                        else OpenSessionErrorKind.STARTUP_FAILED
                    ),
                    (
                        f"RequestedModelUnavailable: {_error_text(exc)}"
                        if command.config.model
                        else f"AnonymousModelUnavailable: {_error_text(exc)}"
                    ),
                )
            )
            return
        try:
            status, payload = self._request(
                "POST",
                "/api/session",
                {
                    "location": {"directory": command.config.cwd},
                    "model": selected_model.request_payload(),
                },
                timeout_sec=self._config.rpc_timeout_sec,
            )
        except TimeoutError as exc:
            raise _AmbiguousSessionCreateError from exc
        data = _response_data(payload)
        if status not in {200, 201} or not isinstance(data, dict):
            self._publish(
                _OpenFailure(
                    command.correlation_id,
                    OpenSessionErrorKind.STARTUP_FAILED,
                    _http_error(
                        "OpenCode session.create",
                        status,
                        payload,
                        secrets=self._http_error_secrets,
                    ),
                )
            )
            return
        session_id = str(data.get("id") or "").strip()
        if not session_id:
            self._publish(
                _OpenFailure(
                    command.correlation_id,
                    OpenSessionErrorKind.STARTUP_FAILED,
                    "OpenCode session.create omitted stable session ID",
                )
            )
            return
        if session_id in self._sessions:
            self._publish(
                _OpenFailure(
                    command.correlation_id,
                    OpenSessionErrorKind.STARTUP_FAILED,
                    "OpenCode session.create reused an active session ID",
                )
            )
            return
        try:
            created_model = _validate_created_session_model(
                data.get("model"),
                selected_model,
            )
        except ValueError:
            self._sessions[session_id] = _EdgeSessionState()
            self._schedule_session_close(session_id)
            self._publish(
                _OpenFailure(
                    command.correlation_id,
                    OpenSessionErrorKind.STARTUP_FAILED,
                    "OpenCode session.create returned a different model",
                )
            )
            return
        self._sessions[session_id] = _EdgeSessionState(
            model=created_model,
            context_window=catalog_snapshot.context_window(created_model),
        )
        self._poll_session_projection(session_id)
        if self._sessions[session_id].failed:
            self._sessions.pop(session_id, None)
            self._publish(
                _OpenFailure(
                    command.correlation_id,
                    OpenSessionErrorKind.STARTUP_FAILED,
                    "OpenCode message projection initialization failed",
                )
            )
            return
        self._publish(
            _OpenSucceeded(
                command.correlation_id,
                session_id,
                created_model.effective_model,
                created_model.provider_id,
                self._runtime_version,
                True,
                context_window=catalog_snapshot.context_window(created_model),
            )
        )

    def _resume_session(self, command: _OpenCommand) -> None:
        session_ref = command.resume_session_ref
        assert session_ref is not None
        session_id = session_ref.native_id
        status, payload = self._request("GET", _session_path(session_id))
        if status == 503:
            self._pending[command.correlation_id.value] = command
            return
        if status == 404:
            self._publish(
                _OpenFailure(
                    command.correlation_id,
                    OpenSessionErrorKind.INVALID_REQUEST,
                    "OpenCode session is unavailable",
                )
            )
            return
        data = _response_data(payload)
        try:
            metadata = _validate_catalog_session(data)
        except ValueError:
            self._publish(
                _OpenFailure(
                    command.correlation_id,
                    OpenSessionErrorKind.STARTUP_FAILED,
                    "OpenCode session.read returned invalid session metadata",
                )
            )
            return
        if status != 200 or metadata.native_id != session_id or (
            command.config.cwd and metadata.cwd != command.config.cwd
        ):
            self._publish(
                _OpenFailure(
                    command.correlation_id,
                    OpenSessionErrorKind.INVALID_REQUEST,
                    "OpenCode session is unavailable",
                )
            )
            return
        catalog_snapshot = self._catalog_snapshot
        assert catalog_snapshot is not None
        persisted_model = _OpenCodeModelSelection(
            metadata.model_provider,
            metadata.model_id,
            metadata.model_variant,
        )
        if (
            persisted_model.provider_id,
            persisted_model.model_id,
        ) not in catalog_snapshot.enabled_models:
            self._publish(
                _OpenFailure(
                    command.correlation_id,
                    OpenSessionErrorKind.INVALID_REQUEST,
                    "OpenCode persisted session model is unavailable",
                )
            )
            return
        if session_id in self._sessions:
            self._publish(
                _OpenFailure(
                    command.correlation_id,
                    OpenSessionErrorKind.BUSY,
                    "OpenCode session is already attached",
                )
            )
            return
        self._sessions[session_id] = _EdgeSessionState(
            model=persisted_model,
            context_window=catalog_snapshot.context_window(persisted_model),
        )
        self._poll_session_projection(session_id)
        if self._sessions[session_id].failed:
            self._sessions.pop(session_id, None)
            self._publish(
                _OpenFailure(
                    command.correlation_id,
                    OpenSessionErrorKind.STARTUP_FAILED,
                    "OpenCode message projection initialization failed",
                )
            )
            return
        self._publish(
            _OpenSucceeded(
                command.correlation_id,
                session_id,
                persisted_model.effective_model,
                metadata.model_provider,
                self._runtime_version,
                False,
                metadata.name,
                metadata.created_at,
                metadata.updated_at,
                True,
                catalog_snapshot.context_window(persisted_model),
            )
        )

    def _list_sessions(self, request: ListProviderSessions) -> ProviderSessionsListResult:
        try:
            status, payload = self._request("GET", "/api/session")
            if status != 200:
                return ProviderSessionsListFailed(
                    ProviderSessionCatalogErrorKind.PROVIDER_FAILED,
                    _http_error(
                        "OpenCode session.list",
                        status,
                        payload,
                        secrets=self._http_error_secrets,
                    ),
                )
            sessions = _validate_session_catalog(payload)
        except ValueError:
            return ProviderSessionsListFailed(
                ProviderSessionCatalogErrorKind.PROVIDER_FAILED,
                "OpenCode session catalog response is invalid",
            )
        except (OSError, URLError):
            return ProviderSessionsListFailed(
                ProviderSessionCatalogErrorKind.PROVIDER_FAILED,
                "OpenCode session catalog transport failed",
            )
        entries = tuple(
            ProviderSessionCatalogEntry(
                session_ref=ProviderSessionRef(
                    ProviderKind.OPENCODE,
                    item.native_id,
                    ProviderProfileId.OPENCODE_NATIVE,
                ),
                name=item.name,
                cwd=item.cwd,
                adapter_name=OPENCODE_API_ADAPTER_COMPONENT,
                adapter_version=OPENCODE_API_ADAPTER_VERSION,
                created_at=item.created_at,
                updated_at=item.updated_at,
            )
            for item in sessions
            if not request.cwd or item.cwd == request.cwd
        )
        return ProviderSessionsListed(entries[: request.limit] if request.limit is not None else entries)

    def _detach_session(self, session_id: str) -> None:
        self._sessions.pop(session_id, None)
        self._closing_sessions.pop(session_id, None)

    def _start_turn(self, command: _StartTurnCommand) -> None:
        state = self._sessions.get(command.session_id)
        if state is None or command.session_id in self._closing_sessions or state.failed:
            self._publish(
                _TurnFailedEvent(
                    command.session_id,
                    command.turn_ref,
                    "OpenCode provider session is unavailable",
                )
            )
            return
        if state.correlated_turn is not None:
            self._publish(
                _TurnFailedEvent(
                    command.session_id,
                    command.turn_ref,
                    "OpenCode provider session is busy",
                )
            )
            return
        state.correlated_turn = _CorrelatedEdgeTurn(
            command.turn_ref,
            command.message_id,
            state.known_message_ids,
        )
        self._ensure_live_reader()
        try:
            status, payload = self._request(
                "POST",
                f"{_session_path(command.session_id)}/prompt",
                {
                    "id": command.message_id,
                    "text": command.text,
                    "delivery": "queue",
                    "resume": True,
                    **({"files": list(command.files)} if command.files else {}),
                },
                timeout_sec=self._config.prompt_admission_timeout_sec,
            )
        except Exception as exc:
            detail = _sanitize_opencode_error(
                exc,
                secrets=self._http_error_secrets,
                native_ids=(command.session_id, command.message_id, command.text),
                default="transport failure",
            )
            self._fail_edge_turn_and_session(
                state,
                command.session_id,
                command.turn_ref,
                f"OpenCode session.prompt delivery is uncertain: {detail}",
            )
            return
        if status != 200:
            self._fail_edge_turn(
                state,
                command.session_id,
                command.turn_ref,
                _http_error(
                    "OpenCode session.prompt",
                    status,
                    payload,
                    secrets=self._http_error_secrets,
                    native_ids=(command.session_id, command.message_id, command.text),
                ),
            )
            return
        try:
            admitted_created = _validate_session_input_admitted(
                payload,
                session_id=command.session_id,
                message_id=command.message_id,
                text=command.text,
                delivery="queue",
                files=command.files,
            )
        except ValueError:
            self._fail_edge_turn_and_session(
                state,
                command.session_id,
                command.turn_ref,
                "OpenCode session.prompt returned mismatched correlation",
            )
            return
        assert state.correlated_turn is not None
        state.correlated_turn.admitted_created = admitted_created
        state.correlated_turn.started = True
        self._publish(_TurnStartedEvent(command.session_id, command.turn_ref))
        self._poll_session_projection(command.session_id)

    def _execute_control_command(
        self,
        command: _SteerTurnCommand | _InterruptTurnCommand | _ApprovalDecisionCommand,
    ) -> None:
        if not command.handoff.claim_dispatch():
            return
        if isinstance(command, _SteerTurnCommand):
            execution = self._steer_turn(command)
            operation = "steer"
        elif isinstance(command, _InterruptTurnCommand):
            execution = self._interrupt_turn(command)
            operation = "interrupt"
        elif isinstance(command, _ApprovalDecisionCommand):
            execution = self._resolve_approval(command)
            operation = "approval decision"
        else:
            assert_never(command)
        if execution is None:
            return
        completion = command.handoff.complete(execution.result)
        if completion is _ControlHandoffCompletion.CANCELLED:
            return
        if completion is _ControlHandoffCompletion.ABANDONED:
            state = self._sessions.get(command.session_id)
            if state is not None and not state.failed:
                correlated = state.correlated_turn
                if correlated is not None and correlated.turn_ref == command.turn_ref:
                    self._fail_edge_turn_and_session(
                        state,
                        command.session_id,
                        command.turn_ref,
                        f"OpenCode {operation} delivery ownership timed out",
                    )
            return
        if execution.poll_projection:
            self._poll_session_projection(command.session_id)

    def _set_model(self, command: _SetModelCommand) -> None:
        state = self._sessions.get(command.session_id)
        if (
            state is None
            or state.failed
            or command.session_id in self._closing_sessions
        ):
            self._publish(
                _ModelSelectionFailedEvent(
                    command.session_id,
                    command.requested_model,
                    "OpenCode provider session is unavailable",
                )
            )
            return
        if state.correlated_turn is not None:
            self._publish(
                _ModelSelectionFailedEvent(
                    command.session_id,
                    command.requested_model,
                    "OpenCode provider session is busy",
                )
            )
            return
        if (
            state.model is None
            or len(state.pending_model_transitions)
            >= _MESSAGE_PROJECTION_MAX_MESSAGES
        ):
            self._publish(
                _ModelSelectionFailedEvent(
                    command.session_id,
                    command.requested_model,
                    "OpenCode model transition capacity is unavailable",
                )
            )
            return
        previous_model = state.model
        try:
            provider_status, provider_payload = self._request("GET", "/api/provider")
            model_status, model_payload = self._request("GET", "/api/model")
            default_status, default_payload = self._request("GET", "/api/model/default")
            credential_status, credential_payload = self._request("GET", "/api/credential")
        except Exception as exc:
            self._publish(
                _ModelSelectionFailedEvent(
                    command.session_id,
                    command.requested_model,
                    "OpenCode model catalog refresh failed: "
                    + _sanitize_opencode_error(
                        exc,
                        secrets=self._http_error_secrets,
                        native_ids=(command.session_id, command.requested_model),
                        default="transport failure",
                    ),
                )
            )
            return
        responses = (
            ("provider catalog", provider_status, provider_payload),
            ("model catalog", model_status, model_payload),
            ("default model", default_status, default_payload),
            ("credential inventory", credential_status, credential_payload),
        )
        failed_response = next(
            (
                (name, status, payload)
                for name, status, payload in responses
                if status != 200
            ),
            None,
        )
        if failed_response is not None:
            name, status, payload = failed_response
            self._publish(
                _ModelSelectionFailedEvent(
                    command.session_id,
                    command.requested_model,
                    _http_error(
                        f"OpenCode {name}",
                        status,
                        payload,
                        secrets=self._http_error_secrets,
                        native_ids=(command.session_id, command.requested_model),
                    ),
                )
            )
            return
        try:
            catalog = _validate_runtime_catalog(
                provider_payload,
                model_payload,
                default_payload,
                credential_payload,
            )
            selected = _require_requested_runtime_model(
                catalog,
                command.requested_model,
            )
        except ValueError as exc:
            self._publish(
                _ModelSelectionFailedEvent(
                    command.session_id,
                    command.requested_model,
                    f"RequestedModelUnavailable: {_error_text(exc)}",
                )
            )
            return
        try:
            status, payload = self._request(
                "POST",
                f"{_session_path(command.session_id)}/model",
                {"model": selected.request_payload()},
                timeout_sec=self._config.rpc_timeout_sec,
            )
        except Exception as exc:
            error = "OpenCode model selection delivery outcome is uncertain"
            self._publish(
                _ModelSelectionUncertainEvent(
                    command.session_id,
                    command.requested_model,
                    error,
                )
            )
            state.failed = True
            self._publish(_SessionFailure(command.session_id, error))
            self._config.logger.event(
                "opencode_model_selection_uncertain",
                provider=ProviderKind.OPENCODE.value,
                error=_sanitize_opencode_error(
                    exc,
                    secrets=self._http_error_secrets,
                    native_ids=(command.session_id, command.requested_model),
                    default="transport failure",
                ),
            )
            return
        if status != 204:
            self._publish(
                _ModelSelectionFailedEvent(
                    command.session_id,
                    command.requested_model,
                    _http_error(
                        "OpenCode session.switchModel",
                        status,
                        payload,
                        secrets=self._http_error_secrets,
                        native_ids=(command.session_id, command.requested_model),
                    ),
                )
            )
            return
        try:
            read_status, read_payload = self._request(
                "GET",
                _session_path(command.session_id),
                timeout_sec=self._config.rpc_timeout_sec,
            )
            if read_status != 200:
                raise ValueError("session model projection is unavailable")
            metadata = _validate_catalog_session(_response_data(read_payload))
            if metadata.native_id != command.session_id:
                raise ValueError("session model projection identity is invalid")
            effective = _validate_created_session_model(
                _OpenCodeModelSelection(
                    metadata.model_provider,
                    metadata.model_id,
                    metadata.model_variant,
                ).request_payload(),
                selected,
            )
        except Exception as exc:
            error = "OpenCode model selection projection outcome is uncertain"
            self._publish(
                _ModelSelectionUncertainEvent(
                    command.session_id,
                    command.requested_model,
                    error,
                )
            )
            state.failed = True
            self._publish(_SessionFailure(command.session_id, error))
            self._config.logger.event(
                "opencode_model_projection_uncertain",
                provider=ProviderKind.OPENCODE.value,
                error=_sanitize_opencode_error(
                    exc,
                    secrets=self._http_error_secrets,
                    native_ids=(command.session_id, command.requested_model),
                    default="invalid projection",
                ),
            )
            return
        self._catalog_snapshot = catalog
        state.model = effective
        state.context_window = catalog.context_window(effective)
        if not _same_model_ref(previous_model, effective):
            state.pending_model_transitions.append(
                _ExpectedModelTransition(previous_model, effective)
            )
        self._publish(
            _ModelSelectionAcceptedEvent(
                command.session_id,
                command.requested_model,
                effective.effective_model,
                effective.provider_id,
                state.context_window,
            )
        )

    def _steer_turn(
        self,
        command: _SteerTurnCommand,
    ) -> _ControlCommandExecution | None:
        state = self._sessions.get(command.session_id)
        if state is None or command.session_id in self._closing_sessions or state.failed:
            return _ControlCommandExecution(
                AgentEffectResult.rejected("OpenCode provider session is unavailable")
            )
        correlated = state.correlated_turn
        if correlated is None:
            return _ControlCommandExecution(
                AgentEffectResult.rejected("OpenCode provider session has no active turn")
            )
        if correlated.turn_ref != command.turn_ref:
            return _ControlCommandExecution(
                AgentEffectResult.rejected("OpenCode active turn no longer matches")
            )
        if correlated.interrupt_requested:
            return _ControlCommandExecution(
                AgentEffectResult.rejected(
                    "OpenCode turn interrupt is already requested"
                )
            )
        if len(correlated.steers) >= _MESSAGE_PROJECTION_MAX_MESSAGES - 1:
            return _ControlCommandExecution(
                AgentEffectResult.rejected(
                    "OpenCode steer admission capacity exceeded"
                )
            )
        if not command.handoff.begin_provider_call():
            return None
        try:
            status, payload = self._request(
                "POST",
                f"{_session_path(command.session_id)}/prompt",
                {
                    "id": command.message_id,
                    "text": command.text,
                    "delivery": "steer",
                    "resume": True,
                    **({"files": list(command.files)} if command.files else {}),
                },
                timeout_sec=self._config.prompt_admission_timeout_sec,
            )
        except Exception as exc:
            detail = _sanitize_opencode_error(
                exc,
                secrets=self._http_error_secrets,
                native_ids=(command.session_id, command.message_id, command.text),
                default="transport failure",
            )
            self._fail_edge_turn_and_session(
                state,
                command.session_id,
                command.turn_ref,
                f"OpenCode session.prompt steer delivery is uncertain: {detail}",
            )
            return _ControlCommandExecution(
                AgentEffectResult.uncertain(
                    "OpenCode steer delivery outcome is uncertain"
                )
            )
        if status != 200:
            return _ControlCommandExecution(
                AgentEffectResult.rejected(
                    _http_error(
                        "OpenCode session.prompt steer",
                        status,
                        payload,
                        secrets=self._http_error_secrets,
                        native_ids=(command.session_id, command.message_id, command.text),
                    )
                )
            )
        try:
            _validate_session_input_admitted(
                payload,
                session_id=command.session_id,
                message_id=command.message_id,
                text=command.text,
                delivery="steer",
                files=command.files,
            )
        except ValueError:
            self._fail_edge_turn_and_session(
                state,
                command.session_id,
                command.turn_ref,
                "OpenCode session.prompt steer returned mismatched correlation",
            )
            return _ControlCommandExecution(
                AgentEffectResult.uncertain(
                    "OpenCode steer delivery correlation is uncertain"
                )
            )
        correlated.steers.append(_CorrelatedSteer(command.message_id))
        return _ControlCommandExecution(
            AgentEffectResult.accepted(),
            poll_projection=True,
        )

    def _resolve_approval(
        self,
        command: _ApprovalDecisionCommand,
    ) -> _ControlCommandExecution | None:
        state = self._sessions.get(command.session_id)
        if state is None or command.session_id in self._closing_sessions or state.failed:
            return _ControlCommandExecution(
                AgentEffectResult.rejected("OpenCode provider session is unavailable")
            )
        correlated = state.correlated_turn
        if correlated is None or correlated.turn_ref != command.turn_ref:
            return _ControlCommandExecution(
                AgentEffectResult.rejected("OpenCode approval turn no longer matches")
            )
        pending = correlated.active_permission
        if pending is None or pending.request.approval_ref != command.approval_ref:
            return _ControlCommandExecution(
                AgentEffectResult.rejected("OpenCode approval request no longer matches")
            )
        decisions = {
            "accept": "once",
            "decline": "reject",
            "always_allow": "always",
        }
        native_decision = decisions.get(command.decision)
        if native_decision is None or not pending.request.supports_decision(command.decision):
            return _ControlCommandExecution(
                AgentEffectResult.rejected(
                    f"OpenCode approval decision is not supported: {command.decision}"
                )
            )
        if native_decision == "always" and not pending.native.save:
            return _ControlCommandExecution(
                AgentEffectResult.rejected(
                    "OpenCode always approval requires explicit provider save scope"
                )
            )
        if not command.handoff.begin_provider_call():
            return None
        permission_path = quote(pending.native.native_id, safe="")
        try:
            status, payload = self._request(
                "POST",
                f"{_session_path(command.session_id)}/permission/{permission_path}/reply",
                {"decision": native_decision},
                timeout_sec=self._config.rpc_timeout_sec,
            )
        except Exception as exc:
            detail = _sanitize_opencode_error(
                exc,
                secrets=self._http_error_secrets,
                native_ids=(command.session_id, pending.native.native_id),
                default="transport failure",
            )
            self._fail_edge_turn_and_session(
                state,
                command.session_id,
                command.turn_ref,
                f"OpenCode permission reply delivery is uncertain: {detail}",
            )
            return _ControlCommandExecution(
                AgentEffectResult.uncertain(
                    "OpenCode approval decision delivery outcome is uncertain"
                )
            )
        if status not in {200, 204}:
            return _ControlCommandExecution(
                AgentEffectResult.rejected(
                    _http_error(
                        "OpenCode permission.reply",
                        status,
                        payload,
                        secrets=self._http_error_secrets,
                        native_ids=(command.session_id, pending.native.native_id),
                    )
                )
            )
        self._remember_resolved_permission(
            correlated,
            session_id=command.session_id,
            permission_id=pending.native.native_id,
        )
        correlated.active_permission = None
        return _ControlCommandExecution(
            AgentEffectResult.accepted(),
            poll_projection=True,
        )

    def _interrupt_turn(
        self,
        command: _InterruptTurnCommand,
    ) -> _ControlCommandExecution | None:
        state = self._sessions.get(command.session_id)
        if state is None or command.session_id in self._closing_sessions or state.failed:
            return _ControlCommandExecution(
                AgentEffectResult.rejected("OpenCode provider session is unavailable")
            )
        correlated = state.correlated_turn
        if correlated is None:
            return _ControlCommandExecution(
                AgentEffectResult.rejected("OpenCode provider session has no active turn")
            )
        if correlated.turn_ref != command.turn_ref:
            return _ControlCommandExecution(
                AgentEffectResult.rejected("OpenCode active turn no longer matches")
            )
        if correlated.interrupt_requested:
            return _ControlCommandExecution(
                AgentEffectResult.rejected(
                    "OpenCode turn interrupt is already requested"
                )
            )
        if not command.handoff.begin_provider_call():
            return None
        try:
            status, payload = self._request(
                "POST",
                f"{_session_path(command.session_id)}/interrupt",
                timeout_sec=self._config.rpc_timeout_sec,
            )
        except Exception as exc:
            detail = _sanitize_opencode_error(
                exc,
                secrets=self._http_error_secrets,
                native_ids=(command.session_id,),
                default="transport failure",
            )
            self._fail_edge_turn_and_session(
                state,
                command.session_id,
                command.turn_ref,
                f"OpenCode session.interrupt delivery is uncertain: {detail}",
            )
            return _ControlCommandExecution(
                AgentEffectResult.uncertain(
                    "OpenCode interrupt delivery outcome is uncertain"
                )
            )
        if status != 200:
            return _ControlCommandExecution(
                AgentEffectResult.rejected(
                    _http_error(
                        "OpenCode session.interrupt",
                        status,
                        payload,
                        secrets=self._http_error_secrets,
                        native_ids=(command.session_id,),
                    )
                )
            )
        try:
            interrupted = _validate_interrupt_result(payload)
        except ValueError:
            self._fail_edge_turn_and_session(
                state,
                command.session_id,
                command.turn_ref,
                "OpenCode session.interrupt returned invalid correlation",
            )
            return _ControlCommandExecution(
                AgentEffectResult.uncertain(
                    "OpenCode interrupt delivery correlation is uncertain"
                )
            )
        if not interrupted:
            return _ControlCommandExecution(
                AgentEffectResult.rejected("OpenCode active turn already settled"),
                poll_projection=True,
            )
        correlated.interrupt_requested = True
        if (
            correlated.assistant_message_ids
            and correlated.assistant_message_ids[-1]
            not in correlated.completed_assistant_ids
        ):
            correlated.interrupt_assistant_message_id = (
                correlated.assistant_message_ids[-1]
            )
        return _ControlCommandExecution(
            AgentEffectResult.accepted(),
            poll_projection=True,
        )

    def _poll_active_correlated_projection(self, session_id: str) -> bool:
        state = self._sessions.get(session_id)
        if (
            state is None
            or state.failed
            or state.correlated_turn is None
            or self._events.full()
        ):
            return False
        self._poll_session_projection(session_id)
        return True

    def _poll_session_projection(self, session_id: str) -> None:
        state = self._sessions.get(session_id)
        if state is None or state.failed or self._events.full():
            return
        correlated = state.correlated_turn
        approval_drain_active: bool | None = None
        if correlated is not None and correlated.resolved_permission_ids:
            try:
                active_status, active_payload = self._request(
                    "GET",
                    "/api/session/active",
                )
            except (OSError, URLError):
                correlated.active_read_failures += 1
                if (
                    correlated.active_read_failures
                    >= _MESSAGE_PROJECTION_MAX_CONSECUTIVE_READ_FAILURES
                ):
                    self._fail_projection(
                        state,
                        session_id,
                        "active-session projection transport failed after bounded read retries",
                    )
                return
            except ValueError as exc:
                self._fail_projection(state, session_id, _error_text(exc))
                return
            correlated.active_read_failures = 0
            if active_status != 200:
                self._fail_projection(
                    state,
                    session_id,
                    _http_error(
                        "OpenCode session.active",
                        active_status,
                        active_payload,
                        secrets=self._http_error_secrets,
                        native_ids=(session_id,),
                    ),
                )
                return
            try:
                approval_drain_active = _validate_active_session_projection(
                    active_payload,
                    session_id=session_id,
                )
            except ValueError as exc:
                self._fail_projection(state, session_id, _error_text(exc))
                return
        try:
            status, payload = self._request("GET", f"{_session_path(session_id)}/message")
        except (OSError, URLError):
            correlated = state.correlated_turn
            if correlated is None:
                return
            correlated.projection_read_failures += 1
            if (
                correlated.projection_read_failures
                >= _MESSAGE_PROJECTION_MAX_CONSECUTIVE_READ_FAILURES
            ):
                self._fail_projection(
                    state,
                    session_id,
                    "transport failed after bounded read retries",
                )
            return
        except ValueError as exc:
            self._fail_projection(state, session_id, _error_text(exc))
            return
        correlated = state.correlated_turn
        if correlated is not None:
            correlated.projection_read_failures = 0
        if status != 200:
            self._fail_projection(
                state,
                session_id,
                _http_error(
                    "OpenCode session.message",
                    status,
                    payload,
                    secrets=self._http_error_secrets,
                    native_ids=(session_id,),
                ),
            )
            return
        try:
            messages = _validate_message_projection(payload)
        except ValueError as exc:
            self._fail_projection(state, session_id, _error_text(exc))
            return
        try:
            permission_status, permission_payload = self._request(
                "GET",
                f"{_session_path(session_id)}/permission",
            )
        except (OSError, URLError):
            correlated = state.correlated_turn
            if correlated is None:
                return
            correlated.permission_read_failures += 1
            if (
                correlated.permission_read_failures
                >= _MESSAGE_PROJECTION_MAX_CONSECUTIVE_READ_FAILURES
            ):
                self._fail_projection(
                    state,
                    session_id,
                    "permission projection transport failed after bounded read retries",
                )
            return
        except ValueError as exc:
            self._fail_projection(state, session_id, _error_text(exc))
            return
        correlated = state.correlated_turn
        if correlated is not None:
            correlated.permission_read_failures = 0
        if permission_status != 200:
            self._fail_projection(
                state,
                session_id,
                _http_error(
                    "OpenCode session.permission",
                    permission_status,
                    permission_payload,
                    secrets=self._http_error_secrets,
                    native_ids=(session_id,),
                ),
            )
            return
        try:
            permissions = _validate_permission_projection(
                permission_payload,
                session_id=session_id,
            )
        except ValueError as exc:
            self._fail_projection(state, session_id, _error_text(exc))
            return
        if not self._reconcile_permission_projection(
            state,
            session_id,
            messages,
            permissions,
        ):
            return
        self._reconcile_message_projection(
            state,
            session_id,
            messages,
            approval_drain_active=approval_drain_active,
        )

    def _reconcile_permission_projection(
        self,
        state: _EdgeSessionState,
        session_id: str,
        messages: tuple[_ProjectedMessage, ...],
        permissions: tuple[_NativePermissionRequest, ...],
    ) -> bool:
        correlated = state.correlated_turn
        if correlated is None:
            if permissions:
                self._fail_session_protocol(
                    state,
                    session_id,
                    "pending permission has no active ORC turn",
                )
                return False
            return True
        active = correlated.active_permission
        by_id = {permission.native_id: permission for permission in permissions}
        if active is not None:
            current = by_id.get(active.native.native_id)
            if current is None:
                if correlated.interrupt_requested:
                    self._remember_resolved_permission(
                        correlated,
                        session_id=session_id,
                        permission_id=active.native.native_id,
                    )
                    correlated.active_permission = None
                    return True
                self._fail_edge_turn_and_session(
                    state,
                    session_id,
                    correlated.turn_ref,
                    "OpenCode pending permission disappeared before ORC decision",
                )
                return False
            if current != active.native:
                self._fail_edge_turn_and_session(
                    state,
                    session_id,
                    correlated.turn_ref,
                    "OpenCode pending permission mutated during ORC decision",
                )
                return False
            return True

        candidates = tuple(
            permission
            for permission in permissions
            if permission.native_id not in correlated.resolved_permission_ids
        )
        if not candidates:
            return True
        native = candidates[0]
        if native.source_message_id:
            matching = tuple(
                message
                for message in messages
                if message.native_id == native.source_message_id
            )
            if not matching:
                return True
            if (
                len(matching) != 1
                or matching[0].message_type != "assistant"
                or native.source_message_id in correlated.baseline_message_ids
                or matching[0].created < correlated.admitted_created
            ):
                self._fail_edge_turn_and_session(
                    state,
                    session_id,
                    correlated.turn_ref,
                    "OpenCode permission source correlation mismatch",
                )
                return False
        correlated.approval_sequence += 1
        approval_ref = ApprovalRef(
            correlated.turn_ref,
            f"opencode-approval-{correlated.approval_sequence}",
        )
        request = _normalize_opencode_permission_request(
            turn_ref=correlated.turn_ref,
            approval_ref=approval_ref,
            native=native,
        )
        correlated.active_permission = _PendingNativePermission(native, request)
        self._publish(
            _ApprovalRequestedEvent(
                session_id,
                correlated.turn_ref,
                request,
            )
        )
        return True

    def _remember_resolved_permission(
        self,
        correlated: _CorrelatedEdgeTurn,
        *,
        session_id: str,
        permission_id: str,
    ) -> None:
        evicted = correlated.resolved_permission_ids.remember(permission_id)
        if evicted is not None:
            self._config.logger.event(
                "opencode_permission_dedup_history_evicted",
                session_id=session_id,
                turn_ref_sequence=correlated.turn_ref.sequence,
                evicted_permission_id=evicted,
            )

    def _reconcile_message_projection(
        self,
        state: _EdgeSessionState,
        session_id: str,
        messages: tuple[_ProjectedMessage, ...],
        *,
        approval_drain_active: bool | None = None,
    ) -> None:
        if not self._reconcile_model_transition_projection(
            state,
            session_id,
            messages,
        ):
            return
        known_types = {"user", "assistant", "idle", "model-switched"}
        for message in messages:
            if (
                message.message_type not in known_types
                and message.native_id not in state.diagnosed_message_ids
            ):
                if len(state.diagnosed_message_ids) < _MESSAGE_PROJECTION_MAX_MESSAGES:
                    state.diagnosed_message_ids.add(message.native_id)
                    self._config.logger.event(
                        "opencode_unknown_event_ignored",
                        provider=ProviderKind.OPENCODE.value,
                        event_type=message.message_type[:128],
                    )
                elif not state.diagnostic_capacity_exhausted:
                    state.diagnostic_capacity_exhausted = True
                    self._config.logger.event(
                        "opencode_projection_diagnostic_capacity_exhausted",
                        provider=ProviderKind.OPENCODE.value,
                    )
        state.known_message_ids = frozenset(message.native_id for message in messages)
        correlated = state.correlated_turn
        if correlated is None:
            return

        steer_by_id = {steer.message_id: steer for steer in correlated.steers}
        expected_user_ids = {correlated.message_id, *steer_by_id}
        matching_users = [
            message
            for message in messages
            if message.message_type == "user"
            and message.native_id in expected_user_ids
        ]
        new_users = {
            message.native_id
            for message in messages
            if message.message_type == "user"
            and message.native_id not in correlated.baseline_message_ids
        }
        if not new_users.issubset(expected_user_ids):
            self._fail_projection(state, session_id, "admitted user correlation mismatch")
            return
        if any(
            sum(
                1
                for message in matching_users
                if message.native_id == expected_user_id
            )
            > 1
            for expected_user_id in expected_user_ids
        ):
            self._fail_projection(state, session_id, "admitted user correlation mismatch")
            return
        if correlated.message_id not in new_users:
            return
        promoted_steers = [
            steer for steer in correlated.steers if steer.message_id in new_users
        ]
        if promoted_steers != correlated.steers[: len(promoted_steers)]:
            self._fail_projection(state, session_id, "steer promotion order mismatch")
            return

        assistants = sorted(
            (
            message
            for message in messages
            if message.message_type == "assistant"
            and message.native_id not in correlated.baseline_message_ids
            and message.created >= correlated.admitted_created
            ),
            key=lambda item: item.created,
        )
        maximum_assistant_count = (
            1 + len(promoted_steers) + correlated.approval_sequence
        )
        if len(assistants) > maximum_assistant_count:
            self._fail_projection(
                state,
                session_id,
                "active turn assistant count exceeds promoted input count",
            )
            return
        assistant_ids = [assistant.native_id for assistant in assistants]
        if assistant_ids[: len(correlated.assistant_message_ids)] != (
            correlated.assistant_message_ids
        ):
            self._fail_projection(state, session_id, "assistant identity changed during active turn")
            return
        correlated.assistant_message_ids.extend(
            assistant_ids[len(correlated.assistant_message_ids) :]
        )

        for assistant in assistants:
            if assistant.model is not None:
                catalog = self._catalog_snapshot
                selected = state.model
                if catalog is None or selected is None:
                    self._fail_projection(
                        state,
                        session_id,
                        "assistant metadata has no catalog-owned model",
                    )
                    return
                try:
                    canonical = _validate_created_session_model(
                        assistant.model.request_payload(),
                        selected,
                    )
                except ValueError:
                    self._fail_projection(
                        state,
                        session_id,
                        "assistant model differs from the owned session model",
                    )
                    return
                state.model = canonical
                state.context_window = catalog.context_window(canonical)
            if (
                assistant.native_id in correlated.completed_assistant_ids
                and assistant.output_text
                != correlated.assistant_text_by_id.get(assistant.native_id, "")
            ):
                self._fail_projection(
                    state,
                    session_id,
                    "completed assistant text mutated",
                )
                return

            for index, item in enumerate(assistant.content):
                key = (assistant.native_id, index, item.item_type)
                if key in correlated.observed_auxiliary_items or item.item_type in {
                    "text",
                    "reasoning",
                }:
                    continue
                correlated.observed_auxiliary_items.add(key)
                if item.item_type in {"permission", "question", "form", "callback"}:
                    self._fail_edge_turn_and_session(
                        state,
                        session_id,
                        correlated.turn_ref,
                        f"OpenCode provider callback is unsupported: {item.item_type}",
                    )
                    return
                if item.item_type.startswith("tool"):
                    self._config.logger.event(
                        "opencode_autonomous_tool_event_observed",
                        provider=ProviderKind.OPENCODE.value,
                        turn_ref_sequence=correlated.turn_ref.sequence,
                        event_type=item.item_type[:128],
                    )
                    continue
                self._config.logger.event(
                    "opencode_unknown_event_ignored",
                    provider=ProviderKind.OPENCODE.value,
                    event_type=item.item_type[:128],
                )

            current_text = assistant.output_text
            previous_text = correlated.assistant_text_by_id.get(assistant.native_id, "")
            if not current_text.startswith(previous_text):
                self._fail_projection(state, session_id, "assistant text mutated non-monotonically")
                return
            total_output_chars = sum(
                len(value)
                for message_id, value in correlated.assistant_text_by_id.items()
                if message_id != assistant.native_id
            ) + len(current_text)
            if total_output_chars > self._config.turn_output_max_chars:
                self._fail_edge_turn_and_session(
                    state,
                    session_id,
                    correlated.turn_ref,
                    "OpenCode turn output limit exceeded "
                    f"({self._config.turn_output_max_chars} characters)",
                )
                return
            suffix = current_text[len(previous_text) :]
            correlated.assistant_text_by_id[assistant.native_id] = current_text
            if suffix:
                self._publish(_TurnOutputEvent(session_id, correlated.turn_ref, suffix))
            if assistant.completed:
                correlated.completed_assistant_ids.add(assistant.native_id)

        if correlated.interrupt_requested:
            if correlated.interrupt_assistant_message_id is None:
                interrupt_candidates = [
                    assistant
                    for assistant in assistants
                    if assistant.error_type == "aborted"
                ]
                if not interrupt_candidates:
                    interrupt_candidates = [
                        assistant for assistant in assistants if not assistant.completed
                    ]
                if interrupt_candidates:
                    correlated.interrupt_assistant_message_id = (
                        interrupt_candidates[-1].native_id
                    )

        if correlated.interrupt_requested:
            interrupted_assistant = next(
                (
                    assistant
                    for assistant in assistants
                    if assistant.native_id
                    == correlated.interrupt_assistant_message_id
                ),
                None,
            )
            interrupted_idle = any(
                message.message_type == "idle"
                and message.native_id not in correlated.baseline_message_ids
                and message.outcome == "interrupted"
                for message in messages
            )
            if (
                interrupted_assistant is None
                or not interrupted_assistant.completed
                or interrupted_assistant.error_type != "aborted"
                or not interrupted_idle
            ):
                return
            state.correlated_turn = None
            self._publish(_TurnInterruptedEvent(session_id, correlated.turn_ref))
            return

        if correlated.active_permission is not None:
            return

        failed_assistant = next(
            (assistant for assistant in assistants if assistant.completed and assistant.failed),
            None,
        )
        if failed_assistant is not None:
            state.correlated_turn = None
            self._publish(
                _TurnFailedEvent(
                    session_id,
                    correlated.turn_ref,
                    "OpenCode execution failed",
                    failed_assistant.raw_error,
                )
            )
            return

        if len(promoted_steers) != len(correlated.steers):
            return
        if correlated.approval_sequence:
            if approval_drain_active is not False:
                return
            if not assistants or not assistants[-1].completed:
                return
        else:
            expected_assistant_count = 1 + len(correlated.steers)
            if (
                len(assistants) != expected_assistant_count
                or not assistants[-1].completed
            ):
                return
        if correlated.active_permission is not None:
            return
        state.correlated_turn = None
        terminal_assistant = assistants[-1]
        effective_model = state.model
        self._publish(
            _TurnCompletedEvent(
                session_id,
                correlated.turn_ref,
                model=(
                    effective_model.effective_model
                    if effective_model is not None
                    else ""
                ),
                backend=(effective_model.provider_id if effective_model is not None else ""),
                used_tokens=terminal_assistant.used_tokens,
                context_window=state.context_window,
            )
        )

    def _reconcile_model_transition_projection(
        self,
        state: _EdgeSessionState,
        session_id: str,
        messages: tuple[_ProjectedMessage, ...],
    ) -> bool:
        try:
            transitions = _validate_model_transition_chain(messages, state.model)
            if not state.projection_initialized:
                if state.pending_model_transitions:
                    raise ValueError(
                        "initial projection contains an owned pending model transition"
                    )
                state.projection_initialized = True
                return True

            new_transitions = tuple(
                transition
                for transition in transitions
                if transition.native_id not in state.known_message_ids
            )
            expected = tuple(state.pending_model_transitions)
            if len(new_transitions) != len(expected):
                raise ValueError("model transition count differs from confirmed changes")
            for transition, owned in zip(new_transitions, expected, strict=True):
                if (
                    transition.previous_model is None
                    or not _same_model_ref(transition.previous_model, owned.previous)
                    or transition.model is None
                    or not _same_model_ref(transition.model, owned.model)
                ):
                    raise ValueError(
                        "model transition differs from the confirmed model change"
                    )
        except ValueError as exc:
            self._fail_projection(state, session_id, _error_text(exc))
            return False
        state.pending_model_transitions.clear()
        return True

    def _fail_projection(
        self,
        state: _EdgeSessionState,
        session_id: str,
        error: str,
    ) -> None:
        correlated = state.correlated_turn
        if correlated is None:
            self._fail_session_protocol(state, session_id, error)
            return
        self._fail_edge_turn_and_session(
            state,
            session_id,
            correlated.turn_ref,
            f"OpenCode message projection error: {error}",
        )

    def _fail_edge_turn(
        self,
        state: _EdgeSessionState,
        session_id: str,
        turn_ref: TurnRef,
        error: str,
    ) -> None:
        correlated = state.correlated_turn
        if correlated is not None and correlated.turn_ref == turn_ref:
            state.correlated_turn = None
        self._publish(_TurnFailedEvent(session_id, turn_ref, error))

    def _fail_session_protocol(
        self,
        state: _EdgeSessionState,
        session_id: str,
        error: str,
    ) -> None:
        state.failed = True
        self._publish(_SessionFailure(session_id, f"OpenCode protocol error: {error}"))

    def _fail_edge_turn_and_session(
        self,
        state: _EdgeSessionState,
        session_id: str,
        turn_ref: TurnRef,
        error: str,
    ) -> None:
        state.failed = True
        correlated = state.correlated_turn
        if correlated is not None and correlated.turn_ref == turn_ref:
            state.correlated_turn = None
        self._publish(_FatalTurnFailureEvent(session_id, turn_ref, error))

    def _schedule_session_close(self, session_id: str) -> None:
        if session_id not in self._sessions:
            return
        self._closing_sessions.setdefault(session_id, _ClosingSession())
        self._close_remote_session(session_id)

    def _close_remote_session(self, session_id: str) -> bool:
        if session_id not in self._sessions:
            self._closing_sessions.pop(session_id, None)
            return True
        closing = self._closing_sessions.setdefault(session_id, _ClosingSession())
        error = ""
        if self._server_ready and self._process is not None:
            try:
                status, payload = self._request("DELETE", _session_path(session_id))
                if status not in {200, 204, 404}:
                    error = _http_error(
                        "OpenCode session.remove",
                        status,
                        payload,
                        secrets=self._http_error_secrets,
                    )
            except Exception as exc:
                error = _error_text(exc)
        elif self._process is not None:
            error = "OpenCode session.remove deferred until server is ready"
        if error:
            closing.next_retry_at = time.monotonic() + _SESSION_CLOSE_RETRY_INTERVAL_SEC
            if not closing.retry_logged:
                closing.retry_logged = True
                self._config.logger.event(
                    "opencode_session_close_retry",
                    provider=ProviderKind.OPENCODE.value,
                    error=error,
                )
            return False
        self._sessions.pop(session_id, None)
        self._closing_sessions.pop(session_id, None)
        self._config.logger.event(
            "opencode_session_closed",
            provider=ProviderKind.OPENCODE.value,
        )
        return True

    def _publish_failures_for_all(
        self,
        kind: OpenSessionErrorKind,
        error: str,
    ) -> None:
        active = self._active_open
        self._active_open = None
        pending = tuple(self._pending.values())
        self._pending.clear()
        pending_catalog = tuple(self._pending_catalog)
        self._pending_catalog.clear()
        for command in pending_catalog:
            if command.cancelled.is_set():
                continue
            try:
                command.reply.put_nowait(
                    ProviderSessionsListFailed(
                        ProviderSessionCatalogErrorKind.PROVIDER_FAILED,
                        error,
                    )
                )
            except Full:
                pass
        commands = ((active,) if active is not None else ()) + pending
        for command in commands:
            self._publish(_OpenFailure(command.correlation_id, kind, error))

    def _publish_session_failures(self, error: str) -> None:
        session_ids = tuple(self._sessions)
        self._sessions.clear()
        self._closing_sessions.clear()
        if session_ids:
            self._publish(_SharedHostFailure(session_ids, error))

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        timeout_sec: float = _HTTP_REQUEST_TIMEOUT_SEC,
    ) -> tuple[int, object]:
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        authorization = base64.b64encode(
            f"{self._config.server_username}:{self._config.server_password}".encode()
        ).decode()
        request = Request(
            self._base_url + path,
            data=data,
            method=method,
            headers={
                "Authorization": f"Basic {authorization}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urlopen(request, timeout=timeout_sec) as response:
                raw = response.read(_MESSAGE_PROJECTION_MAX_BYTES + 1)
                if len(raw) > _MESSAGE_PROJECTION_MAX_BYTES:
                    raise ValueError("OpenCode JSON response exceeded size limit")
                return response.status, json.loads(raw) if raw else None
        except HTTPError as exc:
            try:
                raw = exc.read(_HTTP_ERROR_DETAIL_MAX_CHARS + 1)
                payload_value: object
                try:
                    payload_value = json.loads(raw) if raw else None
                except json.JSONDecodeError:
                    payload_value = {"error": raw.decode("utf-8", errors="replace")[:256]}
                return exc.code, payload_value
            finally:
                exc.close()

    def _ensure_live_reader(self) -> None:
        thread = self._live_thread
        if thread is not None and thread.is_alive():
            return
        self._live_stop.clear()
        self._live_thread = threading.Thread(
            target=self._live_reader_main,
            name=f"{self._config.edge_thread_name}-live",
            daemon=False,
        )
        self._live_thread.start()

    def _live_reader_main(self) -> None:
        authorization = base64.b64encode(
            f"{self._config.server_username}:{self._config.server_password}".encode()
        ).decode()
        while not self._live_stop.is_set():
            request = Request(
                self._base_url + "/api/event",
                method="GET",
                headers={"Authorization": f"Basic {authorization}"},
            )
            try:
                with urlopen(request, timeout=0.2) as response:
                    for raw_line in response:
                        if self._live_stop.is_set():
                            return
                        line = raw_line.decode("utf-8", errors="replace").strip()
                        if not line.startswith("data:"):
                            continue
                        payload = json.loads(line[5:].strip())
                        if not isinstance(payload, dict):
                            continue
                        event_data = payload.get("data")
                        session_id = (
                            str(event_data.get("sessionID") or "").strip()
                            if isinstance(event_data, dict)
                            else ""
                        )
                        if not session_id:
                            continue
                        try:
                            self._live_wakeups.put_nowait(session_id)
                        except Full:
                            self._config.logger.event(
                                "opencode_live_wakeup_dropped",
                                provider=ProviderKind.OPENCODE.value,
                            )
                        self._mark_progress()
            except (OSError, URLError, TimeoutError, json.JSONDecodeError):
                if not self._live_stop.is_set():
                    time.sleep(_LIVE_RECONNECT_DELAY_SEC)

    def _consume_live_wakeups(self) -> tuple[str, ...]:
        session_ids: set[str] = set()
        while True:
            try:
                session_ids.add(self._live_wakeups.get_nowait())
            except Empty:
                return tuple(session_ids)

    def _stop_live_reader(self) -> None:
        self._live_stop.set()
        thread = self._live_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=1.0)
            if thread.is_alive():
                raise RuntimeError("OpenCode live event reader did not stop")
        self._live_thread = None

    def _publish(self, event: _EdgeEvent) -> None:
        try:
            self._events.put_nowait(event)
        except Full as exc:
            raise RuntimeError("OpenCode provider edge event queue capacity exceeded") from exc
        self._mark_progress()

    def _mark_progress(self) -> None:
        with self._state_lock:
            self._progress = True

    def _stop_process(self) -> None:
        self._stop_live_reader()
        process = self._process
        if process is None:
            self._cleanup_state_root()
            return
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2.0)
        self._record_process_stopped(process)
        self._process = None
        self._server_ready = False
        self._runtime_version = ""
        self._catalog_snapshot = None
        self._sessions.clear()
        self._closing_sessions.clear()
        self._cleanup_state_root()

    def _record_process_stopped(self, process: subprocess.Popen[bytes]) -> None:
        output_fields = self._finish_process_output(process)
        self._config.logger.event(
            "process_stopped",
            provider=ProviderKind.OPENCODE.value,
            role=self._config.role,
            pid=process.pid,
            returncode=process.returncode,
            **output_fields,
        )

    def _finish_process_output(self, process: subprocess.Popen[bytes]) -> dict[str, object]:
        deadline = time.monotonic() + _PROCESS_OUTPUT_READER_STOP_TIMEOUT_SEC
        for reader in self._process_readers:
            reader.join(timeout=max(0.0, deadline - time.monotonic()))
        if any(reader.is_alive() for reader in self._process_readers):
            for stream in (process.stdout, process.stderr):
                if stream is not None:
                    stream.close()
            deadline = time.monotonic() + _PROCESS_OUTPUT_READER_STOP_TIMEOUT_SEC
            for reader in self._process_readers:
                reader.join(timeout=max(0.0, deadline - time.monotonic()))
        if any(reader.is_alive() for reader in self._process_readers):
            raise RuntimeError("OpenCode process output reader did not stop")
        for stream in (process.stdout, process.stderr):
            if stream is not None:
                stream.close()
        stdout = self._process_stdout.metadata() if self._process_stdout is not None else (0, False, False)
        stderr = self._process_stderr.metadata() if self._process_stderr is not None else (0, False, False)
        self._process_stdout = None
        self._process_stderr = None
        self._process_readers = ()
        return {
            "stdout_bytes": stdout[0],
            "stdout_truncated": stdout[1],
            "stdout_reader_failed": stdout[2],
            "stderr_bytes": stderr[0],
            "stderr_truncated": stderr[1],
            "stderr_reader_failed": stderr[2],
        }

    def _cleanup_state_root(self) -> None:
        state_root = getattr(self, "_state_root", None)
        if state_root is not None:
            state_root.cleanup()
            self._state_root = None


class OpenCodeProviderHost:
    """Process-root-owned host that serves independent OpenCode session ports."""

    provider = ProviderKind.OPENCODE
    profile = ProviderProfileId.OPENCODE_NATIVE

    def __init__(self, config: OpenCodeProviderConfig) -> None:
        self._config = config
        self._edge = _OpenCodeApiEdge(config)
        self._operations: dict[str, _OpenCodeOpenSessionOperation] = {}
        self._startup_reservations: set[str] = set()
        self._ports: dict[str, OpenCodeAgentSessionAdapter] = {}
        self._backend_lifecycle_started = False
        self._closing = False
        self._stopped = False

    def open_session(self, request: OpenSessionRequest) -> OpenSessionOperation:
        if not isinstance(request, (StartSessionRequest, ResumeSessionRequest)):
            raise ValueError("OpenCode host requires a typed open session request")
        if self._stopped:
            return _FailedOpenCodeOpenSessionOperation(
                request,
                OpenSessionError(OpenSessionErrorKind.CANCELLED, "OpenCode provider host is stopped"),
            )
        if self._closing:
            return _FailedOpenCodeOpenSessionOperation(
                request,
                OpenSessionError(
                    OpenSessionErrorKind.CANCELLED,
                    "OpenCode provider host cleanup must be retried before opening a session",
                ),
            )
        edge_error = self._edge.terminal_error()
        if edge_error:
            return self._failed_operation(
                request,
                OpenSessionError(
                    OpenSessionErrorKind.STARTUP_FAILED,
                    f"OpenCode provider edge is unavailable: {edge_error}",
                ),
            )
        if isinstance(request, ResumeSessionRequest):
            if request.session_ref.execution_profile != ProviderExecutionProfile(
                self.provider,
                self.profile,
            ):
                return self._failed_operation(
                    request,
                    OpenSessionError(
                        OpenSessionErrorKind.INVALID_REQUEST,
                        "OpenCode resume session belongs to a different provider profile",
                    ),
                )
            if self._config.session_store_root is None:
                return self._failed_operation(
                    request,
                    OpenSessionError(
                        OpenSessionErrorKind.INVALID_REQUEST,
                        "OpenCode session resume requires an explicit ORC-owned store",
                    ),
                )
        validation_error = self._validate_start_request(request)
        if validation_error is not None:
            return self._failed_operation(request, validation_error)
        if len(self._startup_reservations) >= self._config.startup_admission_capacity:
            return self._failed_operation(
                request,
                OpenSessionError(
                    OpenSessionErrorKind.BUSY,
                    "OpenCode shared startup admission capacity exceeded",
                ),
            )
        operation = _OpenCodeOpenSessionOperation(self, request)
        self._startup_reservations.add(request.correlation_id.value)
        self._operations[request.correlation_id.value] = operation
        try:
            operation.dispatch()
        except Exception as exc:
            self._complete_failure(
                operation,
                OpenSessionError(
                    OpenSessionErrorKind.BUSY,
                    f"OpenCode shared startup admission failed: {_error_text(exc)}",
                ),
            )
            self._startup_reservations.discard(request.correlation_id.value)
        return operation

    def list_sessions(
        self,
        request: ListProviderSessions,
    ) -> ProviderSessionsListResult:
        if request.execution_profile != ProviderExecutionProfile(self.provider, self.profile):
            return ProviderSessionsListFailed(
                ProviderSessionCatalogErrorKind.INVALID_REQUEST,
                "OpenCode session catalog requested a foreign provider profile",
            )
        if self._stopped:
            return ProviderSessionsListFailed(
                ProviderSessionCatalogErrorKind.UNAVAILABLE,
                "OpenCode provider host is stopped",
            )
        if self._closing:
            return ProviderSessionsListFailed(
                ProviderSessionCatalogErrorKind.UNAVAILABLE,
                "OpenCode provider host cleanup must be retried",
            )
        edge_error = self._edge.terminal_error()
        if edge_error:
            return ProviderSessionsListFailed(
                ProviderSessionCatalogErrorKind.UNAVAILABLE,
                f"OpenCode provider edge is unavailable: {edge_error}",
            )
        return self._edge.list_sessions(request)

    def consume_poll_progress(self) -> bool:
        self._drain_edge_events()
        return self._edge.consume_progress()

    def consume_terminal_error(self) -> str:
        self._drain_edge_events()
        return self._edge.consume_terminal_error()

    def stop_host(self, effect: StopHost) -> StopHostResult:
        if not isinstance(effect, StopHost):
            raise ValueError("OpenCode provider host stop requires StopHost")
        if self._stopped:
            return StopHostResult(stopped=True)
        self._closing = True
        cleanup_errors: list[str] = []
        for operation in tuple(self._operations.values()):
            try:
                operation.cancel("OpenCode provider host stopped during session open")
            except Exception as exc:
                cleanup_errors.append(_error_text(exc))
        for port in tuple(self._ports.values()):
            try:
                port.close_session(CloseSession())
            except Exception as exc:
                cleanup_errors.append(_error_text(exc))
        try:
            self._edge.stop()
        except Exception as exc:
            cleanup_errors.append(_error_text(exc))
            return StopHostResult(
                stopped=False,
                error="; ".join(cleanup_errors),
            )
        self._drain_edge_events()
        self._operations.clear()
        self._startup_reservations.clear()
        self._ports.clear()
        self._stopped = True
        if self._backend_lifecycle_started:
            self._config.logger.event(
                "agent_backend_stopped",
                provider=self.provider.value,
                role=self._config.role,
            )
        return StopHostResult(stopped=True)

    def _validate_start_request(self, request: OpenSessionRequest) -> OpenSessionError | None:
        backend = request.config.backend.strip()
        if backend and backend != OPENCODE_ANONYMOUS_PROVIDER:
            return OpenSessionError(
                OpenSessionErrorKind.INVALID_REQUEST,
                "OpenCode native supports only provider backend "
                f"{OPENCODE_ANONYMOUS_PROVIDER}; got {backend}",
            )
        model = request.config.model.strip()
        if isinstance(request, ResumeSessionRequest) and model:
            return OpenSessionError(
                OpenSessionErrorKind.INVALID_REQUEST,
                "OpenCode resume model is selected by persisted session metadata",
            )
        if isinstance(request, StartSessionRequest) and model:
            try:
                parsed = _parse_startup_model(model)
            except ValueError as exc:
                return OpenSessionError(
                    OpenSessionErrorKind.INVALID_REQUEST,
                    f"OpenCode startup model is invalid: {_error_text(exc)}",
                )
        return None

    def _failed_operation(
        self,
        request: OpenSessionRequest,
        error: OpenSessionError,
    ) -> _FailedOpenCodeOpenSessionOperation:
        self._config.logger.event(
            "provider_session_open_failed",
            provider=self.provider.value,
            correlation_id=request.correlation_id.value,
            error_kind=error.kind.value,
            error=error.message,
        )
        return _FailedOpenCodeOpenSessionOperation(request, error)

    def _cancel_open(self, operation: _OpenCodeOpenSessionOperation, reason: str) -> None:
        self._cancel_open_with_error(
            operation,
            OpenSessionError(OpenSessionErrorKind.CANCELLED, reason),
        )

    def _cancel_open_with_error(
        self,
        operation: _OpenCodeOpenSessionOperation,
        error: OpenSessionError,
    ) -> None:
        if operation.completed:
            return
        self._edge.cancel_open(operation.correlation_id)
        self._complete_failure(operation, error)

    def _complete_failure(
        self,
        operation: _OpenCodeOpenSessionOperation,
        error: OpenSessionError,
    ) -> None:
        if operation.completed:
            return
        self._operations.pop(operation.correlation_id.value, None)
        self._config.logger.event(
            "provider_session_open_failed",
            provider=self.provider.value,
            correlation_id=operation.correlation_id.value,
            error_kind=error.kind.value,
            error=error.message,
        )
        operation.receive_failure(error)

    def _drain_edge_events(self) -> None:
        for event in self._edge.poll_events():
            if isinstance(event, _OpenProgress):
                operation = self._operations.get(event.correlation_id.value)
                if operation is not None and not operation.completed:
                    operation.receive_progress(event.phase, event.message)
                continue
            if isinstance(event, _OpenFailure):
                self._startup_reservations.discard(event.correlation_id.value)
                operation = self._operations.get(event.correlation_id.value)
                if operation is not None:
                    self._complete_failure(
                        operation,
                        OpenSessionError(event.kind, event.error),
                    )
                continue
            if isinstance(event, _OpenSucceeded):
                self._startup_reservations.discard(event.correlation_id.value)
                operation = self._operations.pop(event.correlation_id.value, None)
                if operation is None or operation.completed:
                    if event.created_native_session:
                        self._edge.close_session(event.session_id)
                    else:
                        self._edge.detach_session(event.session_id)
                        self._log_session_detached()
                    continue
                session_ref = ProviderSessionRef(
                    self.provider,
                    event.session_id,
                    self.profile,
                )
                capabilities = opencode_capability_snapshot(
                    adapter_version=OPENCODE_API_ADAPTER_VERSION,
                    runtime_version=event.runtime_version,
                    backend_name=event.backend,
                )
                port = OpenCodeAgentSessionAdapter(
                    host=self,
                    session_ref=session_ref,
                    capabilities=capabilities,
                    model=event.model,
                    backend=event.backend,
                    context_window=event.context_window,
                    logger=self._config.logger,
                    cwd=Path(operation.request.config.cwd or os.getcwd()).resolve(),
                    turn_output_max_chars=self._config.turn_output_max_chars,
                    event_capacity=self._config.port_event_capacity,
                    session_name=event.session_name,
                    created_at=event.created_at,
                    updated_at=event.updated_at,
                    loaded=event.loaded,
                )
                self._ports[event.session_id] = port
                self._config.logger.event(
                    "agent_capability_snapshot",
                    provider=self.provider.value,
                    **capabilities.to_log_fields(),
                )
                if not self._backend_lifecycle_started:
                    self._config.logger.event(
                        "agent_backend_started",
                        provider=self.provider.value,
                        role=self._config.role,
                        runtime_version=event.runtime_version,
                    )
                    self._backend_lifecycle_started = True
                self._config.logger.event(
                    "provider_session_opened",
                    provider=self.provider.value,
                    correlation_id=event.correlation_id.value,
                    provider_session_id=event.session_id,
                    effective_model=event.model,
                    effective_backend=event.backend,
                )
                operation.receive_success(
                    Opened(
                        correlation_id=event.correlation_id,
                        session=port,
                        session_ref=session_ref,
                        effective=EffectiveRuntimeSnapshot(event.model, event.backend),
                        capabilities=capabilities,
                    )
                )
                continue
            if isinstance(event, _SessionFailure):
                port = self._ports.get(event.session_id)
                if port is not None:
                    port.receive_failure(event.error)
                continue
            if isinstance(event, _SharedHostFailure):
                for session_id in event.session_ids:
                    port = self._ports.get(session_id)
                    if port is not None:
                        port.receive_failure(event.error)
                continue
            if isinstance(event, _TurnStartedEvent):
                port = self._ports.get(event.session_id)
                if port is not None:
                    port.receive_turn_started(event.turn_ref)
                continue
            if isinstance(event, _TurnOutputEvent):
                port = self._ports.get(event.session_id)
                if port is not None:
                    port.receive_turn_output(event.turn_ref, event.text)
                continue
            if isinstance(event, _TurnCompletedEvent):
                port = self._ports.get(event.session_id)
                if port is not None:
                    port.receive_turn_completed(
                        event.turn_ref,
                        model=event.model,
                        backend=event.backend,
                        used_tokens=event.used_tokens,
                        context_window=event.context_window,
                    )
                continue
            if isinstance(event, _TurnFailedEvent):
                port = self._ports.get(event.session_id)
                if port is not None:
                    port.receive_turn_failed(
                        event.turn_ref,
                        event.error,
                        raw_provider_error=event.raw_provider_error,
                    )
                continue
            if isinstance(event, _TurnInterruptedEvent):
                port = self._ports.get(event.session_id)
                if port is not None:
                    port.receive_turn_interrupted(event.turn_ref)
                continue
            if isinstance(event, _FatalTurnFailureEvent):
                port = self._ports.get(event.session_id)
                if port is not None:
                    port.receive_fatal_turn_failure(event.turn_ref, event.error)
                continue
            if isinstance(event, _ApprovalRequestedEvent):
                port = self._ports.get(event.session_id)
                if port is not None:
                    port.receive_approval_requested(event.turn_ref, event.request)
                continue
            if isinstance(event, _ModelSelectionAcceptedEvent):
                port = self._ports.get(event.session_id)
                if port is not None:
                    port.receive_model_selection_accepted(
                        event.requested_model,
                        effective_model=event.effective_model,
                        backend=event.backend,
                        context_window=event.context_window,
                    )
                continue
            if isinstance(event, _ModelSelectionFailedEvent):
                port = self._ports.get(event.session_id)
                if port is not None:
                    port.receive_model_selection_failed(
                        event.requested_model,
                        event.error,
                    )
                continue
            if isinstance(event, _ModelSelectionUncertainEvent):
                port = self._ports.get(event.session_id)
                if port is not None:
                    port.receive_model_selection_uncertain(
                        event.requested_model,
                        event.error,
                    )
                continue
            assert_never(event)

    def _detach_port(self, port: OpenCodeAgentSessionAdapter) -> None:
        session_id = port.session_ref.native_id
        if self._ports.get(session_id) is not port:
            return
        self._edge.detach_session(session_id)
        self._ports.pop(session_id, None)
        self._log_session_detached()

    def _log_session_detached(self) -> None:
        self._config.logger.event(
            "opencode_session_detached",
            provider=self.provider.value,
        )

    def _start_turn(
        self,
        port: OpenCodeAgentSessionAdapter,
        effect: StartTurn,
    ) -> AgentEffectResult:
        session_id = port.session_ref.native_id
        if self._ports.get(session_id) is not port:
            return AgentEffectResult.rejected("OpenCode provider session is unavailable")
        return self._edge.submit_turn(
            _StartTurnCommand(
                session_id=session_id,
                turn_ref=effect.turn_ref,
                message_id=_turn_message_id(effect.turn_ref),
                text=effect.agent_input.text,
                files=port.prepare_prompt_files(effect.agent_input),
            )
        )

    def _set_model(
        self,
        port: OpenCodeAgentSessionAdapter,
        effect: SetModel,
    ) -> AgentEffectResult:
        session_id = port.session_ref.native_id
        if self._ports.get(session_id) is not port:
            return AgentEffectResult.rejected("OpenCode provider session is unavailable")
        return self._edge.submit_model(
            _SetModelCommand(
                session_id=session_id,
                requested_model=effect.model,
            )
        )

    def _steer_turn(
        self,
        port: OpenCodeAgentSessionAdapter,
        effect: TrySteerTurn,
        message_id: str,
        files: tuple[dict[str, str], ...],
    ) -> AgentEffectResult:
        session_id = port.session_ref.native_id
        if self._ports.get(session_id) is not port:
            return AgentEffectResult.rejected("OpenCode provider session is unavailable")
        return self._edge.submit_steer(
            _SteerTurnCommand(
                session_id=session_id,
                turn_ref=effect.expected_turn_ref,
                message_id=message_id,
                text=effect.agent_input.text,
                files=files,
                handoff=_ControlCommandHandoff(),
            )
        )

    def _interrupt_turn(
        self,
        port: OpenCodeAgentSessionAdapter,
        effect: InterruptTurn,
    ) -> AgentEffectResult:
        session_id = port.session_ref.native_id
        if self._ports.get(session_id) is not port:
            return AgentEffectResult.rejected("OpenCode provider session is unavailable")
        return self._edge.submit_interrupt(
            _InterruptTurnCommand(
                session_id=session_id,
                turn_ref=effect.turn_ref,
                handoff=_ControlCommandHandoff(),
            )
        )

    def _resolve_approval(
        self,
        port: OpenCodeAgentSessionAdapter,
        effect: ResolveApproval,
    ) -> AgentEffectResult:
        session_id = port.session_ref.native_id
        if self._ports.get(session_id) is not port:
            return AgentEffectResult.rejected("OpenCode provider session is unavailable")
        return self._edge.submit_approval(
            _ApprovalDecisionCommand(
                session_id=session_id,
                turn_ref=effect.turn_ref,
                approval_ref=effect.approval_ref,
                decision=effect.decision,
                handoff=_ControlCommandHandoff(),
            )
        )

class _OpenCodeOpenSessionOperation(OpenSessionOperation):
    def __init__(self, host: OpenCodeProviderHost, request: OpenSessionRequest) -> None:
        super().__init__(request)
        self._host = host
        self._deadline = clock.monotonic() + host._config.startup_timeout_sec
        self._dispatched = False

    def dispatch(self) -> None:
        if self._dispatched:
            raise RuntimeError("OpenCode session open was already dispatched")
        self._dispatched = True
        self._host._edge.submit_open(
            _OpenCommand(
                self.correlation_id,
                self.request.config,
                self.request.session_ref
                if isinstance(self.request, ResumeSessionRequest)
                else None,
            )
        )

    def cancel(self, reason: str) -> None:
        self._host._cancel_open(self, reason)

    def receive_progress(self, phase: str, message: str) -> None:
        if not self.completed:
            self._publish_progress(phase=phase, message=message)

    def receive_success(self, result: Opened) -> None:
        if not self.completed:
            self._complete(result)

    def receive_failure(self, error: OpenSessionError) -> None:
        if not self.completed:
            self._complete(OpenFailed(self.correlation_id, error))

    def _advance(self) -> None:
        self._host._drain_edge_events()
        if not self.completed and clock.monotonic() >= self._deadline:
            self._host._cancel_open_with_error(
                self,
                OpenSessionError(
                    OpenSessionErrorKind.STARTUP_TIMEOUT,
                    "OpenCode provider startup timed out",
                ),
            )


class _FailedOpenCodeOpenSessionOperation(OpenSessionOperation):
    def __init__(self, request: OpenSessionRequest, error: OpenSessionError) -> None:
        super().__init__(request)
        self._complete(OpenFailed(self.correlation_id, error))

    def _advance(self) -> None:
        return


class OpenCodeAgentSessionAdapter:
    """Provider-neutral text session port backed by a shared-host lease."""

    def __init__(
        self,
        *,
        host: OpenCodeProviderHost,
        session_ref: ProviderSessionRef,
        capabilities: AgentCapabilitySnapshot,
        model: str,
        backend: str,
        context_window: int | None,
        logger: LifecycleLogger,
        cwd: Path,
        turn_output_max_chars: int,
        event_capacity: int,
        session_name: str = "",
        created_at: str = "",
        updated_at: str = "",
        loaded: bool = False,
    ) -> None:
        self._host = host
        self.session_ref = session_ref
        self._capabilities = capabilities
        self._model = model
        self._backend = backend
        self._used_tokens: int | None = None
        self._context_window = context_window
        self._pending_model: str | None = None
        self._logger = logger
        self._cwd = cwd
        self._turn_output_max_chars = turn_output_max_chars
        self._event_capacity = event_capacity
        self._closed = False
        self._failure_emitted = False
        self._active_turn_ref: TurnRef | None = None
        self._active_approval_request: AgentApprovalRequest | None = None
        self._steer_occurrence = 0
        self._output_text = StringIO()
        self._events: list[AgentSessionEvent] = []
        self._session_name = session_name
        self._created_at = created_at
        self._updated_at = updated_at
        self._loaded = loaded

    def get_capability_snapshot(self) -> AgentCapabilitySnapshot:
        return self._capabilities

    def start_turn(self, effect: StartTurn) -> AgentEffectResult:
        if not isinstance(effect, StartTurn):
            raise ValueError("OpenCode turn start requires StartTurn")
        unsupported = unsupported_effect_result(effect, self._capabilities)
        if unsupported is not None:
            return unsupported
        if self._closed:
            return AgentEffectResult.rejected("OpenCode provider session is closed")
        if self._failure_emitted:
            return AgentEffectResult.rejected("OpenCode provider session is unavailable")
        if self._active_turn_ref is not None:
            return AgentEffectResult.rejected("OpenCode provider session is busy")
        if self._pending_model is not None:
            return AgentEffectResult.rejected("OpenCode model selection is pending")
        try:
            result = self._host._start_turn(self, effect)
        except _OpenCodeAttachmentError as exc:
            return AgentEffectResult.rejected(str(exc))
        if result.outcome is AgentEffectOutcome.ACCEPTED:
            self._active_turn_ref = effect.turn_ref
            self._steer_occurrence = 0
            self._output_text = StringIO()
            self._used_tokens = None
        return result

    def try_steer_turn(self, effect: TrySteerTurn) -> AgentEffectResult:
        if not isinstance(effect, TrySteerTurn):
            raise ValueError("OpenCode turn steer requires TrySteerTurn")
        unsupported = unsupported_effect_result(effect, self._capabilities)
        if unsupported is not None:
            return unsupported
        if self._closed or self._failure_emitted:
            return AgentEffectResult.rejected("OpenCode provider session is unavailable")
        if self._active_turn_ref is None:
            return AgentEffectResult.rejected("OpenCode provider session has no active turn")
        if self._active_turn_ref != effect.expected_turn_ref:
            return AgentEffectResult.rejected("OpenCode active turn no longer matches")
        try:
            files = self.prepare_prompt_files(effect.agent_input)
        except _OpenCodeAttachmentError as exc:
            return AgentEffectResult.rejected(str(exc))
        self._steer_occurrence += 1
        return self._host._steer_turn(
            self,
            effect,
            _steer_message_id(effect.expected_turn_ref, self._steer_occurrence),
            files,
        )

    def interrupt_turn(self, effect: InterruptTurn) -> AgentEffectResult:
        if not isinstance(effect, InterruptTurn):
            raise ValueError("OpenCode turn interrupt requires InterruptTurn")
        unsupported = unsupported_effect_result(effect, self._capabilities)
        if unsupported is not None:
            return unsupported
        if self._closed or self._failure_emitted:
            return AgentEffectResult.rejected("OpenCode provider session is unavailable")
        if self._active_turn_ref is None:
            return AgentEffectResult.rejected("OpenCode provider session has no active turn")
        if self._active_turn_ref != effect.turn_ref:
            return AgentEffectResult.rejected("OpenCode active turn no longer matches")
        return self._host._interrupt_turn(self, effect)

    def resolve_approval(self, effect: ResolveApproval) -> ApprovalDecisionResult:
        request = self._active_approval_request
        if request is None:
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected("OpenCode approval request is no longer active")
            )
        preflight = approval_decision_preflight(
            request,
            active_turn_ref=self._active_turn_ref,
            active_approval_ref=request.approval_ref,
            decision=effect.decision,
        )
        if preflight.outcome is not AgentEffectOutcome.ACCEPTED:
            return ApprovalDecisionResult.preflight(preflight)
        if effect.turn_ref != request.turn_ref or effect.approval_ref != request.approval_ref:
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected("OpenCode approval request no longer matches")
            )
        try:
            result = self._host._resolve_approval(self, effect)
        except Exception as exc:
            return ApprovalDecisionResult.provider_call_exception(exc)
        if result.outcome is AgentEffectOutcome.ACCEPTED:
            self._active_approval_request = None
        return ApprovalDecisionResult.provider_call(result)

    def prepare_prompt_files(self, agent_input: AgentInput) -> tuple[dict[str, str], ...]:
        return _opencode_prompt_files(self._cwd, agent_input)

    def set_model(self, effect: SetModel) -> AgentEffectResult:
        if not isinstance(effect, SetModel):
            raise ValueError("OpenCode model selection requires SetModel")
        unsupported = unsupported_effect_result(effect, self._capabilities)
        if unsupported is not None:
            return unsupported
        if self._closed:
            return AgentEffectResult.rejected("OpenCode provider session is closed")
        if self._failure_emitted:
            return AgentEffectResult.rejected("OpenCode provider session is unavailable")
        if self._active_turn_ref is not None:
            return AgentEffectResult.rejected("OpenCode provider session is busy")
        if self._pending_model is not None:
            return AgentEffectResult.rejected("OpenCode model selection is already pending")
        try:
            _parse_startup_model(effect.model)
        except ValueError as exc:
            return AgentEffectResult.rejected(
                f"OpenCode model selection is invalid: {_error_text(exc)}"
            )
        result = self._host._set_model(self, effect)
        if result.outcome is AgentEffectOutcome.ACCEPTED:
            self._pending_model = effect.model
        return result

    def set_effort(self, effect: SetEffort) -> AgentEffectResult:
        return self._unsupported(effect)

    def close_session(self, effect: CloseSession) -> AgentEffectResult:
        if not isinstance(effect, CloseSession):
            raise ValueError("OpenCode session close requires CloseSession")
        self._host._detach_port(self)
        self._events.clear()
        self._closed = True
        self._active_turn_ref = None
        self._active_approval_request = None
        self._pending_model = None
        self._steer_occurrence = 0
        self._output_text = StringIO()
        return AgentEffectResult.completed()

    def refresh_inspection(self, effect: RefreshInspection) -> AgentEffectResult:
        if not isinstance(effect, RefreshInspection):
            raise ValueError("OpenCode inspection requires RefreshInspection")
        if self._closed:
            return AgentEffectResult.rejected("OpenCode provider session is closed")
        if len(self._events) >= self._event_capacity:
            return AgentEffectResult.rejected("OpenCode session event capacity exceeded")
        self._events.append(
            InspectionCompleted(effect.inspection_ref, self.get_local_projection())
        )
        return AgentEffectResult.accepted()

    def get_local_projection(self) -> AgentRuntimeProjection:
        return AgentRuntimeProjection(
            capabilities=self._capabilities,
            session=AgentSessionSnapshot(
                SnapshotAvailability.AVAILABLE,
                session_ref=self.session_ref,
                status=(
                    "failed"
                    if self._failure_emitted
                    else "closed"
                    if self._closed
                    else "running"
                    if self._active_turn_ref is not None
                    else "idle"
                ),
                backend=self._backend,
                model=self._model,
                name=self._session_name,
                loaded=self._loaded,
                created_at=self._created_at,
                updated_at=self._updated_at,
            ),
            work=AgentWorkSnapshot(SnapshotAvailability.UNSUPPORTED),
            context=(
                AgentContextSnapshot(
                    SnapshotAvailability.AVAILABLE,
                    used_tokens=self._used_tokens,
                    model_context_window=self._context_window,
                )
                if self._used_tokens is not None and self._context_window is not None
                else AgentContextSnapshot(SnapshotAvailability.UNKNOWN)
            ),
            background=AgentBackgroundSnapshot(SnapshotAvailability.UNSUPPORTED),
        )

    def poll_events(self) -> list[AgentSessionEvent]:
        self._host._drain_edge_events()
        events = list(self._events)
        self._events.clear()
        return events

    def receive_failure(self, error: str) -> None:
        if not self._closed and not self._failure_emitted:
            self._fail_closed(self._active_turn_ref, error)

    def receive_turn_started(self, turn_ref: TurnRef) -> None:
        if self._is_active(turn_ref):
            self._queue_turn_event(TurnStarted(turn_ref), turn_ref)
        else:
            self._log_stale_turn_event(turn_ref, "turn_started")

    def receive_turn_output(self, turn_ref: TurnRef, text: str) -> None:
        if not self._is_active(turn_ref):
            self._log_stale_turn_event(turn_ref, "output_delta")
            return
        if self._output_text.tell() + len(text) > self._turn_output_max_chars:
            error = (
                "OpenCode turn output limit exceeded "
                f"({self._turn_output_max_chars} characters)"
            )
            self._fail_closed(turn_ref, error)
            return
        if not self._queue_turn_event(OutputDelta(turn_ref, text), turn_ref):
            return
        self._output_text.write(text)

    def receive_turn_completed(
        self,
        turn_ref: TurnRef,
        *,
        model: str,
        backend: str,
        used_tokens: int | None,
        context_window: int | None,
    ) -> None:
        if not self._is_active(turn_ref):
            self._log_stale_turn_event(turn_ref, "turn_completed")
            return
        output = AgentOutput(self._output_text.getvalue())
        if not self._queue_turn_event(TurnCompleted(turn_ref, output), turn_ref):
            return
        if model and backend:
            self._model = model
            self._backend = backend
            self._used_tokens = used_tokens
            self._context_window = context_window
        self._active_turn_ref = None
        self._active_approval_request = None
        self._steer_occurrence = 0
        self._output_text = StringIO()

    def receive_model_selection_accepted(
        self,
        requested_model: str,
        *,
        effective_model: str,
        backend: str,
        context_window: int | None,
    ) -> None:
        if self._pending_model != requested_model or self._closed:
            self._logger.event(
                "opencode_stale_model_selection_result_ignored",
                provider=ProviderKind.OPENCODE.value,
                result_type="accepted",
            )
            return
        if not self._queue_control_event(ModelSelectionAccepted(requested_model)):
            return
        self._pending_model = None
        self._model = effective_model
        self._backend = backend
        self._used_tokens = None
        self._context_window = context_window

    def receive_model_selection_failed(self, requested_model: str, error: str) -> None:
        if self._pending_model != requested_model or self._closed:
            self._logger.event(
                "opencode_stale_model_selection_result_ignored",
                provider=ProviderKind.OPENCODE.value,
                result_type="failed",
            )
            return
        if not self._queue_control_event(ModelSelectionFailed(requested_model, error)):
            return
        self._pending_model = None

    def receive_model_selection_uncertain(
        self,
        requested_model: str,
        error: str,
    ) -> None:
        if self._pending_model != requested_model or self._closed:
            self._logger.event(
                "opencode_stale_model_selection_result_ignored",
                provider=ProviderKind.OPENCODE.value,
                result_type="uncertain",
            )
            return
        if not self._queue_control_event(
            ModelSelectionUncertain(requested_model, error)
        ):
            return
        self._pending_model = None

    def _queue_control_event(self, event: AgentSessionEvent) -> bool:
        if len(self._events) < self._event_capacity:
            self._events.append(event)
            return True
        self._fail_closed(
            None,
            f"OpenCode session event backlog limit exceeded ({self._event_capacity} events)",
            replace_pending=True,
        )
        return False

    def receive_turn_failed(
        self,
        turn_ref: TurnRef,
        error: str,
        *,
        raw_provider_error: RawProviderErrorPayload | None = None,
    ) -> None:
        if not self._is_active(turn_ref):
            self._log_stale_turn_event(turn_ref, "turn_failed")
            return
        if not self._queue_turn_event(
            TurnFailed(turn_ref, error, raw_provider_error),
            turn_ref,
        ):
            return
        self._active_turn_ref = None
        self._active_approval_request = None
        self._steer_occurrence = 0
        self._output_text = StringIO()

    def receive_turn_interrupted(self, turn_ref: TurnRef) -> None:
        if not self._is_active(turn_ref):
            self._log_stale_turn_event(turn_ref, "turn_interrupted")
            return
        if not self._queue_turn_event(TurnInterrupted(turn_ref), turn_ref):
            return
        self._active_turn_ref = None
        self._active_approval_request = None
        self._steer_occurrence = 0
        self._output_text = StringIO()

    def receive_fatal_turn_failure(self, turn_ref: TurnRef, error: str) -> None:
        if not self._is_active(turn_ref):
            self._log_stale_turn_event(turn_ref, "fatal_turn_failure")
            return
        self._fail_closed(turn_ref, error)

    def receive_approval_requested(
        self,
        turn_ref: TurnRef,
        request: AgentApprovalRequest,
    ) -> None:
        if not self._is_active(turn_ref):
            self._log_stale_turn_event(turn_ref, "approval_requested")
            return
        if self._active_approval_request is not None:
            self._fail_closed(turn_ref, "OpenCode concurrent approval request is unsupported")
            return
        if not self._queue_turn_event(
            ApprovalRequested(turn_ref, request.approval_ref, request),
            turn_ref,
        ):
            return
        self._active_approval_request = request

    def _queue_turn_event(self, event: AgentSessionEvent, turn_ref: TurnRef) -> bool:
        if len(self._events) < self._event_capacity:
            self._events.append(event)
            return True
        self._fail_closed(
            turn_ref,
            f"OpenCode session event backlog limit exceeded ({self._event_capacity} events)",
            replace_pending=True,
        )
        return False

    def _fail_closed(
        self,
        turn_ref: TurnRef | None,
        error: str,
        *,
        replace_pending: bool = False,
    ) -> None:
        if self._failure_emitted:
            return
        terminal_events: list[AgentSessionEvent] = []
        if turn_ref is not None:
            terminal_events.append(TurnFailed(turn_ref, error))
        terminal_events.append(SessionFailed(error))
        if replace_pending or len(self._events) + len(terminal_events) > self._event_capacity:
            self._events.clear()
        self._events.extend(terminal_events)
        self._failure_emitted = True
        self._closed = True
        self._active_turn_ref = None
        self._active_approval_request = None
        self._pending_model = None
        self._steer_occurrence = 0
        self._output_text = StringIO()
        self._host._detach_port(self)

    def _is_active(self, turn_ref: TurnRef) -> bool:
        return self._active_turn_ref == turn_ref and not self._closed

    def _log_stale_turn_event(self, turn_ref: TurnRef, event_type: str) -> None:
        self._logger.event(
            "opencode_stale_turn_event_ignored",
            provider=ProviderKind.OPENCODE.value,
            turn_ref_sequence=turn_ref.sequence,
            event_type=event_type[:128],
        )

    def _unsupported(self, effect: AgentSessionEffect) -> AgentEffectResult:
        result = unsupported_effect_result(effect, self._capabilities)
        if result is None:
            raise AssertionError(
                f"OpenCode text-turn adapter unexpectedly supports {type(effect).__name__}"
            )
        return result


def register_opencode_provider(
    registry: ProviderRegistry,
    *,
    cli_path: Path | None = None,
    env: Mapping[str, str] | None = None,
    ambient_environment: Mapping[str, str] | None = None,
    edge_stop_timeout_sec: float = _EDGE_STOP_TIMEOUT_SEC,
    session_store_root: Path | None = None,
) -> None:
    registered_env = MappingProxyType(
        {
            **dict(os.environ if ambient_environment is None else ambient_environment),
            **dict(env or {}),
        }
    )

    resolved_cli_path = Path(cli_path or "opencode")
    runtime_command = (str(resolved_cli_path), "serve")

    def compose(
        context: ProviderHostRootConfiguration,
        _initial_session_config: AgentSessionConfig,
    ) -> ComposedProviderHost:
        if context.runtime_command != runtime_command:
            raise RuntimeError("OpenCode shared host command differs from registration")
        config = OpenCodeProviderConfig(
            logger=context.logger,
            cli_path=Path(context.runtime_command[0]),
            env=registered_env,
            role=context.role,
            startup_timeout_sec=context.startup_timeout_sec,
            rpc_timeout_sec=context.rpc_timeout_sec,
            edge_thread_name=context.edge_thread_name.replace(
                "jsonrpc",
                OPENCODE_API_ADAPTER_COMPONENT,
            ),
            edge_stop_timeout_sec=edge_stop_timeout_sec,
            session_store_root=session_store_root,
        )
        return ComposedProviderHost(
            host=OpenCodeProviderHost(config),
            initial_capabilities=opencode_capability_snapshot(
                adapter_version=OPENCODE_API_ADAPTER_VERSION,
                backend_name=OPENCODE_ANONYMOUS_PROVIDER,
            ),
        )

    registry.register(
        ProviderExecutionProfile(ProviderKind.OPENCODE, ProviderProfileId.OPENCODE_NATIVE),
        OpenCodeProviderConfig,
        OpenCodeProviderHost,
        composer=compose,
        lifetime=ProviderHostLifetime.SHARED_PROCESS,
        shared_runtime_command=runtime_command,
    )


def register_provider(
    registry: ProviderRegistry,
    context: ProviderRegistrationContext,
) -> None:
    execution_profile = ProviderExecutionProfile(
        ProviderKind.OPENCODE,
        ProviderProfileId.OPENCODE_NATIVE,
    )
    register_opencode_provider(
        registry,
        cli_path=context.cli_path_for(ProviderKind.OPENCODE),
        env=context.runtime_environment_for(ProviderKind.OPENCODE),
        ambient_environment={},
        edge_stop_timeout_sec=context.shared_host_cleanup_timeout_sec,
        session_store_root=context.session_catalog_root_for(execution_profile),
    )


def _reserve_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _bounded_permission_string(value: object, *, label: str, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise ValueError(f"permission {label} must be a string")
    normalized = value.strip()
    if (not normalized and not allow_empty) or len(normalized) > _PERMISSION_VALUE_MAX_CHARS:
        raise ValueError(
            f"permission {label} must contain 1..{_PERMISSION_VALUE_MAX_CHARS} characters"
        )
    return normalized


def _validate_permission_projection(
    payload: object,
    *,
    session_id: str,
) -> tuple[_NativePermissionRequest, ...]:
    data = _response_data(payload)
    if not isinstance(data, list):
        raise ValueError("permission projection data must be a list")
    if len(data) > _PERMISSION_PROJECTION_MAX_REQUESTS:
        raise ValueError("permission projection exceeded request capacity")
    requests: list[_NativePermissionRequest] = []
    seen_ids: set[str] = set()
    allowed_fields = {
        "id",
        "sessionID",
        "action",
        "resources",
        "save",
        "metadata",
        "source",
        "message",
    }
    for value in data:
        if not isinstance(value, dict) or not set(value).issubset(allowed_fields):
            raise ValueError("permission projection contains invalid request fields")
        native_id = _bounded_permission_string(value.get("id"), label="identity")
        owner = _bounded_permission_string(value.get("sessionID"), label="session identity")
        action = _bounded_permission_string(value.get("action"), label="action")
        if owner != session_id:
            raise ValueError("permission projection session correlation mismatch")
        if native_id in seen_ids:
            raise ValueError("permission projection contains duplicate identities")
        seen_ids.add(native_id)
        raw_resources = value.get("resources")
        raw_save = value.get("save", [])
        if (
            not isinstance(raw_resources, list)
            or not isinstance(raw_save, list)
            or len(raw_resources) > _PERMISSION_RESOURCE_MAX_ITEMS
            or len(raw_save) > _PERMISSION_RESOURCE_MAX_ITEMS
        ):
            raise ValueError("permission resources are invalid")
        resources = tuple(
            _bounded_permission_string(item, label="resource")
            for item in raw_resources
        )
        save = tuple(
            _bounded_permission_string(item, label="save scope")
            for item in raw_save
        )
        metadata = value.get("metadata", {})
        if not isinstance(metadata, dict) or len(metadata) > _PERMISSION_RESOURCE_MAX_ITEMS:
            raise ValueError("permission metadata is invalid")
        command = ""
        cwd = ""
        for key, target in (("command", "command"), ("cwd", "cwd")):
            if key not in metadata:
                continue
            normalized = _bounded_permission_string(
                metadata[key],
                label=f"metadata {key}",
                allow_empty=True,
            )
            if target == "command":
                command = normalized
            else:
                cwd = normalized
        source = value.get("source")
        if not isinstance(source, dict) or set(source) != {"type", "messageID", "id"}:
            raise ValueError("permission source is required for ORC correlation")
        if source.get("type") != "tool":
            raise ValueError("permission source type is invalid")
        source_message_id = _bounded_permission_string(
            source.get("messageID"),
            label="source message identity",
        )
        _bounded_permission_string(source.get("id"), label="source tool identity")
        message = _bounded_permission_string(
            value.get("message", ""),
            label="message",
            allow_empty=True,
        )
        requests.append(
            _NativePermissionRequest(
                native_id=native_id,
                session_id=owner,
                action=action,
                resources=resources,
                save=save,
                message=message,
                command=command,
                cwd=cwd,
                source_message_id=source_message_id,
            )
        )
    return tuple(requests)


def _normalize_opencode_permission_request(
    *,
    turn_ref: TurnRef,
    approval_ref: ApprovalRef,
    native: _NativePermissionRequest,
) -> AgentApprovalRequest:
    action = native.action.strip()
    action_key = action.lower()
    reason = native.message or f"OpenCode requested permission for {action}"
    unknown_resources = () if native.resources else ("unavailable",)
    decisions = ["accept", "decline"]
    if native.save and not unknown_resources:
        decisions.append("always_allow")
    diagnostics = tuple(
        item
        for item in (
            ("provider_action", action[:128]),
            (
                "provider_scope",
                "explicit_saved_rule" if native.save else "one_time",
            ),
            (
                "provider_resources",
                "\n".join(native.resources)[:_PERMISSION_VALUE_MAX_CHARS],
            )
            if native.resources
            else None,
            ("provider_cwd", native.cwd[:_PERMISSION_VALUE_MAX_CHARS])
            if native.cwd
            else None,
        )
        if item is not None
    )
    command_actions = {"bash", "command", "exec", "shell"}
    file_actions = {"edit", "file", "patch", "read", "write"}
    if action_key in command_actions:
        command = native.command or (native.resources[0] if native.resources else action)
        details = tuple(
            item
            for item in (
                f"command: {command}",
                f"cwd: {native.cwd}" if native.cwd else "",
                *(f"resource: {resource}" for resource in native.resources),
            )
            if item
        )
        return AgentApprovalRequest(
            turn_ref=turn_ref,
            approval_ref=approval_ref,
            kind=AgentApprovalKind.COMMAND,
            command=command,
            cwd=native.cwd,
            saved_rule_scope=native.save,
            reason=reason,
            available_decisions=tuple(decisions),
            prompt_details=details,
            diagnostics=diagnostics,
        )
    if action_key in file_actions:
        details = tuple(f"file: {resource}" for resource in native.resources)
        return AgentApprovalRequest(
            turn_ref=turn_ref,
            approval_ref=approval_ref,
            kind=AgentApprovalKind.FILE_CHANGE,
            command=action,
            cwd=native.cwd,
            affected_paths=native.resources,
            saved_rule_scope=native.save,
            reason=reason,
            available_decisions=tuple(decisions),
            prompt_details=details or (f"action: {action}",),
            diagnostics=diagnostics,
        )
    details = [f"tool: {action}"]
    if unknown_resources:
        details.extend(f"unknown resource: {resource}" for resource in unknown_resources)
    else:
        details.extend(f"resource: {resource}" for resource in native.resources)
    if unknown_resources:
        decisions = ["decline"]
    return AgentApprovalRequest(
        turn_ref=turn_ref,
        approval_ref=approval_ref,
        kind=AgentApprovalKind.TOOL,
        command=action,
        cwd=native.cwd,
        saved_rule_scope=native.save,
        reason=reason,
        available_decisions=tuple(decisions),
        prompt_details=tuple(details),
        diagnostics=diagnostics,
    )


def _resolve_opencode_attachment_path(cwd: Path, attachment: AgentAttachmentRef) -> Path:
    root = cwd.resolve()
    try:
        path = (root / attachment.relative_path).resolve(strict=True)
    except OSError as exc:
        raise _OpenCodeAttachmentError(
            f"OpenCode attachment is unavailable: {attachment.display_name}"
        ) from exc
    if not path.is_relative_to(root) or not path.is_file():
        raise _OpenCodeAttachmentError(
            f"OpenCode attachment is unavailable: {attachment.display_name}"
        )
    return path


def _opencode_prompt_file(cwd: Path, attachment: AgentAttachmentRef) -> dict[str, str]:
    media_type = str(attachment.media_type or "").strip().lower()
    display_name = str(attachment.display_name or "").strip()
    if len(media_type) > _ATTACHMENT_MEDIA_TYPE_MAX_CHARS:
        raise _OpenCodeAttachmentError("OpenCode attachment media type is too long")
    if not display_name or len(display_name) > _ATTACHMENT_NAME_MAX_CHARS:
        raise _OpenCodeAttachmentError("OpenCode attachment name is invalid")
    if (
        media_type not in _SUPPORTED_ATTACHMENT_IMAGE_TYPES
        and not media_type.startswith("text/")
    ):
        raise _OpenCodeAttachmentError(
            f"unsupported OpenCode attachment media type: {media_type or 'unknown'}"
        )
    if attachment.size_bytes > _ATTACHMENT_MAX_BYTES:
        raise _OpenCodeAttachmentError(
            f"OpenCode attachment exceeds {_ATTACHMENT_MAX_BYTES} byte limit"
        )
    path = _resolve_opencode_attachment_path(cwd, attachment)
    try:
        with path.open("rb") as stream:
            payload = stream.read(_ATTACHMENT_MAX_BYTES + 1)
    except OSError as exc:
        raise _OpenCodeAttachmentError(
            f"OpenCode attachment is unavailable: {attachment.display_name}"
        ) from exc
    if len(payload) != attachment.size_bytes:
        raise _OpenCodeAttachmentError(
            f"OpenCode attachment size changed: {attachment.display_name}"
        )
    if len(payload) > _ATTACHMENT_MAX_BYTES:
        raise _OpenCodeAttachmentError(
            f"OpenCode attachment exceeds {_ATTACHMENT_MAX_BYTES} byte limit"
        )
    if media_type.startswith("text/"):
        try:
            payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise _OpenCodeAttachmentError(
                f"OpenCode text attachment is not valid UTF-8: {attachment.display_name}"
            ) from exc
    encoded = base64.b64encode(payload).decode("ascii")
    return {
        "uri": f"data:{media_type};base64,{encoded}",
        "name": display_name,
    }


def _opencode_prompt_files(cwd: Path, agent_input: AgentInput) -> tuple[dict[str, str], ...]:
    if len(agent_input.attachments) > _ATTACHMENT_MAX_ITEMS:
        raise _OpenCodeAttachmentError(
            f"OpenCode attachment count exceeds {_ATTACHMENT_MAX_ITEMS} file limit"
        )
    total_bytes = sum(attachment.size_bytes for attachment in agent_input.attachments)
    if total_bytes > _ATTACHMENT_TOTAL_MAX_BYTES:
        raise _OpenCodeAttachmentError(
            "OpenCode attachments exceed "
            f"{_ATTACHMENT_TOTAL_MAX_BYTES} byte aggregate limit"
        )
    return tuple(
        _opencode_prompt_file(cwd, attachment)
        for attachment in agent_input.attachments
    )


def _projected_token_count(value: object, field: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
        or value > _TOKEN_COUNT_MAX
    ):
        raise ValueError(f"message projection {field} token count is invalid")
    return value


def _projected_context_usage(value: object) -> int | None:
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != {
        "input",
        "output",
        "reasoning",
        "cache",
    }:
        raise ValueError("message projection token usage is invalid")
    cache = value.get("cache")
    if not isinstance(cache, dict) or set(cache) != {"read", "write"}:
        raise ValueError("message projection cache token usage is invalid")
    input_tokens = _projected_token_count(value.get("input"), "input")
    _projected_token_count(value.get("output"), "output")
    _projected_token_count(value.get("reasoning"), "reasoning")
    cache_read = _projected_token_count(cache.get("read"), "cache read")
    cache_write = _projected_token_count(cache.get("write"), "cache write")
    used = input_tokens + cache_read + cache_write
    if used > _TOKEN_COUNT_MAX:
        raise ValueError("message projection context usage exceeds its bound")
    return used


def _validate_message_projection(payload: object) -> tuple[_ProjectedMessage, ...]:
    data = _response_data(payload)
    if not isinstance(data, list):
        raise ValueError("message projection data must be a list")
    if len(data) > _MESSAGE_PROJECTION_MAX_MESSAGES:
        raise ValueError("message projection exceeded message capacity")
    projected: list[_ProjectedMessage] = []
    seen_ids: set[str] = set()
    previous_created: float | None = None
    for raw_message in data:
        if not isinstance(raw_message, dict):
            raise ValueError("message projection contains a non-object message")
        native_id = raw_message.get("id")
        message_type = raw_message.get("type")
        if (
            not isinstance(native_id, str)
            or not native_id
            or len(native_id) > _NATIVE_MESSAGE_ID_MAX_CHARS
        ):
            raise ValueError("message projection identity is invalid")
        if native_id in seen_ids:
            raise ValueError("message projection contains duplicate identities")
        seen_ids.add(native_id)
        if (
            not isinstance(message_type, str)
            or not message_type
            or len(message_type) > 128
        ):
            raise ValueError("message projection type is invalid")
        timing = raw_message.get("time")
        created = timing.get("created") if isinstance(timing, dict) else None
        if (
            not isinstance(created, (int, float))
            or isinstance(created, bool)
            or not math.isfinite(float(created))
        ):
            raise ValueError("message projection created time is invalid")
        created_value = float(created)
        if previous_created is not None and created_value > previous_created:
            raise ValueError("message projection order is not descending")
        previous_created = created_value
        if message_type == "model-switched":
            allowed_fields = {"id", "metadata", "time", "type", "model", "previous"}
            if (
                not {"id", "time", "type", "model"}.issubset(raw_message)
                or not set(raw_message).issubset(allowed_fields)
                or not native_id.startswith("msg_")
                or not isinstance(timing, dict)
                or set(timing) != {"created"}
            ):
                raise ValueError("model-switched message shape is invalid")
            metadata = raw_message.get("metadata")
            if metadata is not None and (
                not isinstance(metadata, dict)
                or len(metadata) > _MESSAGE_CONTENT_MAX_ITEMS
                or any(
                    not isinstance(key, str) or len(key) > 128
                    for key in metadata
                )
            ):
                raise ValueError("model-switched metadata is invalid")
            model = _validate_persisted_session_model(raw_message.get("model"))
            raw_previous = raw_message.get("previous")
            previous_model = (
                _validate_persisted_session_model(raw_previous)
                if "previous" in raw_message
                else None
            )
            projected.append(
                _ProjectedMessage(
                    native_id=native_id,
                    message_type=message_type,
                    created=created_value,
                    completed=False,
                    failed=False,
                    error_type=None,
                    raw_error=None,
                    outcome=None,
                    content=(),
                    model=model,
                    previous_model=previous_model,
                )
            )
            continue
        is_completed = isinstance(timing, dict) and "completed" in timing
        if is_completed:
            completed_at = timing.get("completed")
            if (
                not isinstance(completed_at, (int, float))
                or isinstance(completed_at, bool)
                or not math.isfinite(float(completed_at))
            ):
                raise ValueError("message projection completed time is invalid")
        raw_content = raw_message.get("content", [])
        if not isinstance(raw_content, list) or len(raw_content) > _MESSAGE_CONTENT_MAX_ITEMS:
            raise ValueError("message projection content is invalid")
        normalized_content: list[_ProjectedContent] = []
        for raw_item in raw_content:
            if not isinstance(raw_item, dict):
                raise ValueError("message projection contains a non-object content item")
            item_type = raw_item.get("type")
            if not isinstance(item_type, str) or not item_type or len(item_type) > 128:
                raise ValueError("message projection content type is invalid")
            item_text = raw_item.get("text", "")
            if item_type == "text" and not isinstance(item_text, str):
                raise ValueError("message projection text content is invalid")
            normalized_content.append(
                _ProjectedContent(item_type, item_text if item_type == "text" else "")
            )
        raw_error = raw_message.get("error")
        if raw_error is not None and not isinstance(raw_error, dict):
            raise ValueError("message projection error is invalid")
        error_type = raw_error.get("type") if isinstance(raw_error, dict) else None
        if error_type is not None and (
            not isinstance(error_type, str) or not error_type or len(error_type) > 128
        ):
            raise ValueError("message projection error type is invalid")
        normalized_error = (
            RawProviderErrorPayload.from_json_object(raw_error)
            if isinstance(raw_error, dict)
            else None
        )
        outcome = raw_message.get("outcome")
        if outcome is not None and (
            not isinstance(outcome, str) or not outcome or len(outcome) > 128
        ):
            raise ValueError("message projection outcome is invalid")
        raw_model = raw_message.get("model")
        model = (
            _validate_persisted_session_model(raw_model)
            if raw_model is not None
            else None
        )
        used_tokens = _projected_context_usage(raw_message.get("tokens"))
        if message_type != "assistant" and (
            model is not None or used_tokens is not None
        ):
            raise ValueError("non-assistant message contains runtime metadata")
        projected.append(
            _ProjectedMessage(
                native_id=native_id,
                message_type=message_type,
                created=created_value,
                completed=is_completed,
                failed=raw_error is not None,
                error_type=error_type,
                raw_error=normalized_error,
                outcome=outcome,
                content=tuple(normalized_content),
                model=model,
                used_tokens=used_tokens,
            )
        )
    return tuple(projected)


def _canonical_model_ref(
    model: _OpenCodeModelSelection,
) -> _OpenCodeModelSelection:
    return _OpenCodeModelSelection(
        model.provider_id,
        model.model_id,
        model.variant or "default",
    )


def _same_model_ref(
    left: _OpenCodeModelSelection,
    right: _OpenCodeModelSelection,
) -> bool:
    return _canonical_model_ref(left) == _canonical_model_ref(right)


def _validate_model_transition_chain(
    messages: tuple[_ProjectedMessage, ...],
    current_model: _OpenCodeModelSelection | None,
) -> tuple[_ProjectedMessage, ...]:
    transitions = tuple(
        reversed(
            tuple(
                message
                for message in messages
                if message.message_type == "model-switched"
            )
        )
    )
    prior_target: _OpenCodeModelSelection | None = None
    for transition in transitions:
        target = transition.model
        previous = transition.previous_model
        if target is None:
            raise ValueError("model transition omitted its target model")
        if previous is not None and _same_model_ref(previous, target):
            raise ValueError("model transition does not change the effective model")
        if prior_target is not None and (
            previous is None or not _same_model_ref(previous, prior_target)
        ):
            raise ValueError("model transition history is discontinuous")
        prior_target = target
    if transitions and (
        current_model is None
        or transitions[-1].model is None
        or not _same_model_ref(transitions[-1].model, current_model)
    ):
        raise ValueError(
            "model transition history differs from the provider-confirmed session model"
        )
    return transitions


def _validate_catalog_session(value: object) -> _CatalogSessionMetadata:
    if not isinstance(value, dict):
        raise ValueError("session metadata must be an object")
    native_id = str(value.get("id") or "").strip()
    if not native_id or len(native_id) > _NATIVE_MESSAGE_ID_MAX_CHARS:
        raise ValueError("session metadata requires a bounded stable ID")
    location = value.get("location")
    cwd = (
        str(location.get("directory") or "").strip()
        if isinstance(location, dict)
        else ""
    )
    if not cwd:
        raise ValueError("session metadata requires a directory")
    name = str(value.get("title") or value.get("name") or "OpenCode session").strip()
    timing = value.get("time")
    created = _format_opencode_timestamp(
        timing.get("created") if isinstance(timing, dict) else None
    )
    updated = _format_opencode_timestamp(
        timing.get("updated") if isinstance(timing, dict) else None
    )
    model = _validate_persisted_session_model(value.get("model"))
    return _CatalogSessionMetadata(
        native_id=native_id,
        cwd=cwd,
        name=(name or "OpenCode session")[:256],
        created_at=created,
        updated_at=updated,
        model_provider=model.provider_id,
        model_id=model.model_id,
        model_variant=model.variant,
    )


def _validate_session_catalog(payload: object) -> tuple[_CatalogSessionMetadata, ...]:
    data = _response_data(payload)
    if not isinstance(data, list):
        raise ValueError("session catalog data must be a list")
    if len(data) > _MESSAGE_PROJECTION_MAX_MESSAGES:
        raise ValueError("session catalog exceeded entry capacity")
    entries = tuple(_validate_catalog_session(item) for item in data)
    if len({entry.native_id for entry in entries}) != len(entries):
        raise ValueError("session catalog contains duplicate session IDs")
    return tuple(
        sorted(
            entries,
            key=lambda item: (item.updated_at, item.created_at),
            reverse=True,
        )
    )


def _validate_persisted_session_model(value: object) -> _OpenCodeModelSelection:
    if not isinstance(value, dict):
        raise ValueError("session metadata requires persisted model")
    allowed_fields = {"providerID", "id", "variant"}
    if not {"providerID", "id"}.issubset(value) or not set(value).issubset(
        allowed_fields
    ):
        raise ValueError("session metadata persisted model is invalid")
    provider_id = value.get("providerID")
    model_id = value.get("id")
    variant = value.get("variant")
    if (
        not isinstance(provider_id, str)
        or not provider_id.strip()
        or len(provider_id.strip()) > _MODEL_SELECTION_MAX_CHARS
        or not isinstance(model_id, str)
        or not model_id.strip()
        or len(model_id.strip()) > _MODEL_SELECTION_MAX_CHARS
        or (
            variant is not None
            and (
                not isinstance(variant, str)
                or not variant.strip()
                or len(variant.strip()) > _MODEL_SELECTION_MAX_CHARS
            )
        )
    ):
        raise ValueError("session metadata persisted model is invalid")
    return _OpenCodeModelSelection(
        provider_id.strip(),
        model_id.strip(),
        variant.strip() if isinstance(variant, str) else "",
    )


def _validate_created_session_model(
    value: object,
    selected: _OpenCodeModelSelection,
) -> _OpenCodeModelSelection:
    created = _validate_persisted_session_model(value)
    if created.provider_id != selected.provider_id or created.model_id != selected.model_id:
        raise ValueError("created session model differs from selected startup model")
    if selected.variant:
        if created.variant != selected.variant:
            raise ValueError("created session model differs from selected startup model")
        return created
    # OpenCode projects an omitted base variant as `default`; named variants are overlays.
    if created.variant != "default":
        raise ValueError("created session model returned an unexpected variant")
    return created


def _format_opencode_timestamp(value: object) -> str:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value < 0
    ):
        return ""
    return datetime.fromtimestamp(float(value) / 1000, timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def _validate_session_input_admitted(
    payload: object,
    *,
    session_id: str,
    message_id: str,
    text: str,
    delivery: str,
    files: tuple[dict[str, str], ...] = (),
) -> float:
    if delivery not in {"queue", "steer"}:
        raise ValueError("session input admission delivery is invalid")
    admitted = _response_data(payload)
    expected_fields = {"delivery", "id", "payload", "sessionID", "time", "type"}
    if not isinstance(admitted, dict) or set(admitted) != expected_fields:
        raise ValueError("session input admission has invalid fields")
    admitted_payload = admitted.get("payload")
    expected_payload: dict[str, object] = {"text": text}
    if files:
        expected_files: list[dict[str, object]] = []
        for item in files:
            if not isinstance(item, dict) or set(item) != {"name", "uri"}:
                raise ValueError("session input admission request file is invalid")
            name = item.get("name")
            uri = item.get("uri")
            if not isinstance(name, str) or not name or not isinstance(uri, str):
                raise ValueError("session input admission request file is invalid")
            prefix, separator, encoded = uri.partition(";base64,")
            if (
                separator != ";base64,"
                or not prefix.startswith("data:")
                or not prefix.removeprefix("data:")
            ):
                raise ValueError("session input admission request file is invalid")
            try:
                base64.b64decode(encoded, validate=True)
            except ValueError as exc:
                raise ValueError("session input admission request file is invalid") from exc
            expected_files.append(
                {
                    "data": encoded,
                    "mime": prefix.removeprefix("data:"),
                    "name": name,
                    "source": {"type": "inline"},
                }
            )
        expected_payload["files"] = expected_files
    if (
        admitted.get("id") != message_id
        or admitted.get("sessionID") != session_id
        or admitted.get("type") != "user"
        or admitted.get("delivery") != delivery
        or admitted_payload != expected_payload
    ):
        raise ValueError("session input admission correlation mismatch")
    time_payload = admitted.get("time")
    if not isinstance(time_payload, dict) or set(time_payload) != {"created"}:
        raise ValueError("session input admission time is invalid")
    created = time_payload.get("created")
    if (
        not isinstance(created, (int, float))
        or isinstance(created, bool)
        or not math.isfinite(float(created))
    ):
        raise ValueError("session input admission created time is invalid")
    return float(created)


def _validate_active_session_projection(
    payload: object,
    *,
    session_id: str,
) -> bool:
    data = _response_data(payload)
    if not isinstance(data, dict) or len(data) > _MESSAGE_PROJECTION_MAX_MESSAGES:
        raise ValueError("active-session projection must be a bounded object")
    for native_id, state in data.items():
        if (
            not isinstance(native_id, str)
            or not native_id
            or not isinstance(state, dict)
            or state != {"type": "running"}
        ):
            raise ValueError("active-session projection contains an invalid entry")
    return session_id in data


def _validate_interrupt_result(payload: object) -> bool:
    if not isinstance(payload, dict) or set(payload) != {"interrupted"}:
        raise ValueError("session interrupt result has invalid fields")
    interrupted = payload.get("interrupted")
    if not isinstance(interrupted, bool):
        raise ValueError("session interrupt result is invalid")
    return interrupted


def _select_anonymous_model(
    provider_payload: object,
    model_payload: object,
    default_payload: object,
    credential_payload: object,
) -> tuple[str, str]:
    selection = _require_anonymous_fresh_model(
        _validate_runtime_catalog(
            provider_payload,
            model_payload,
            default_payload,
            credential_payload,
        )
    )
    return selection.provider_id, selection.effective_model


def _validate_runtime_catalog(
    provider_payload: object,
    model_payload: object,
    default_payload: object,
    credential_payload: object,
) -> _OpenCodeCatalogSnapshot:
    providers = _catalog_data(provider_payload, "provider catalog")
    models = _catalog_data(model_payload, "model catalog")
    credentials = _catalog_data(credential_payload, "credential inventory")
    if any(not isinstance(item, dict) for item in credentials):
        raise ValueError("credential inventory contains a non-object entry")
    enabled_providers = {
        str(item.get("id") or "").strip()
        for item in providers
        if isinstance(item, dict)
        and item.get("activation") in {"auto", "enabled"}
    }
    if any(not isinstance(item, dict) for item in providers) or "" in enabled_providers:
        raise ValueError("provider catalog contains an invalid entry")
    enabled_models = frozenset(
        (
            str(item.get("providerID") or "").strip(),
            str(item.get("modelID") or "").strip(),
        )
        for item in models
        if isinstance(item, dict)
        and item.get("status") == "active"
        and item.get("enabled", True) is True
        and str(item.get("providerID") or "").strip() in enabled_providers
    )
    if any(not isinstance(item, dict) for item in models) or any(
        not provider_id or not model_id for provider_id, model_id in enabled_models
    ):
        raise ValueError("model catalog contains an invalid entry")
    enabled_variants: set[tuple[str, str, str]] = set()
    context_windows: list[tuple[str, str, int]] = []
    for item in models:
        assert isinstance(item, dict)
        provider_id = str(item.get("providerID") or "").strip()
        model_id = str(item.get("modelID") or "").strip()
        if (provider_id, model_id) not in enabled_models:
            continue
        limit = item.get("limit")
        if limit is not None:
            if not isinstance(limit, dict):
                raise ValueError("model catalog limit must be an object")
            context_window = limit.get("context")
            if (
                not isinstance(context_window, int)
                or isinstance(context_window, bool)
                or context_window <= 0
                or context_window > _TOKEN_COUNT_MAX
            ):
                raise ValueError("model catalog context limit is invalid")
            context_windows.append((provider_id, model_id, context_window))
        variants = item.get("variants", [])
        if not isinstance(variants, list):
            raise ValueError("model catalog variants must be a list")
        for variant in variants:
            variant_id = (
                str(variant.get("id") or "").strip()
                if isinstance(variant, dict)
                else ""
            )
            if not variant_id or len(variant_id) > _MODEL_SELECTION_MAX_CHARS:
                raise ValueError("model catalog contains an invalid variant")
            enabled_variants.add((provider_id, model_id, variant_id))
    default_data = _response_data(default_payload)
    if not isinstance(default_data, dict):
        raise ValueError("server default model is unavailable")
    default_provider = str(default_data.get("providerID") or "").strip()
    default_model = str(default_data.get("modelID") or "").strip()
    default_variant = str(default_data.get("variant") or "").strip()
    if (default_provider, default_model) not in enabled_models:
        raise ValueError("server default model is unavailable")
    return _OpenCodeCatalogSnapshot(
        frozenset(enabled_providers),
        enabled_models,
        frozenset(enabled_variants),
        _OpenCodeModelSelection(default_provider, default_model, default_variant),
        tuple(sorted(context_windows)),
    )


def _require_anonymous_fresh_model(
    catalog: _OpenCodeCatalogSnapshot,
) -> _OpenCodeModelSelection:
    selected = catalog.default_model
    if selected.provider_id != OPENCODE_ANONYMOUS_PROVIDER:
        raise ValueError("server default is not an anonymous OpenCode model")
    if (selected.provider_id, selected.model_id) not in catalog.enabled_models:
        raise ValueError("server default model is unavailable")
    return selected


def _parse_startup_model(value: str) -> _OpenCodeModelSelection:
    if not isinstance(value, str):
        raise ValueError("model selection must be a string")
    normalized = value.strip()
    if not normalized or len(normalized) > _MODEL_SELECTION_MAX_CHARS:
        raise ValueError("model selection is empty or exceeds its size limit")
    parts = normalized.split("/")
    if len(parts) not in {2, 3} or any(
        not part or part != part.strip() for part in parts
    ):
        raise ValueError("model selection must use provider/model[/variant]")
    return _OpenCodeModelSelection(
        provider_id=parts[0],
        model_id=parts[1],
        variant=parts[2] if len(parts) == 3 else "",
    )


def _require_requested_startup_model(
    catalog: _OpenCodeCatalogSnapshot,
    value: str,
) -> _OpenCodeModelSelection:
    try:
        return _require_requested_runtime_model(catalog, value)
    except ValueError as exc:
        message = str(exc).replace("requested model", "requested startup model", 1)
        raise ValueError(message) from exc


def _require_requested_runtime_model(
    catalog: _OpenCodeCatalogSnapshot,
    value: str,
) -> _OpenCodeModelSelection:
    selected = _parse_startup_model(value)
    if (
        selected.provider_id,
        selected.model_id,
    ) not in catalog.enabled_models:
        raise ValueError("requested model is unavailable")
    if selected.variant and (
        selected.provider_id,
        selected.model_id,
        selected.variant,
    ) not in catalog.enabled_variants:
        raise ValueError("requested model variant is unavailable")
    return selected


def _catalog_data(payload: object, name: str) -> list[object]:
    data = _response_data(payload)
    if not isinstance(data, list):
        raise ValueError(f"{name} data must be a list")
    return data


def _response_data(payload: object) -> object:
    return payload.get("data") if isinstance(payload, dict) else None


def _catalog_is_settling(
    provider_payload: object,
    model_payload: object,
    default_payload: object,
) -> bool:
    return (
        _response_data(provider_payload) == []
        or _response_data(model_payload) == []
        or _response_data(default_payload) is None
    )


def _http_error(
    operation: str,
    status: int,
    payload: object,
    *,
    secrets: tuple[str, ...] = (),
    native_ids: tuple[str, ...] = (),
) -> str:
    del payload, secrets, native_ids
    return f"{operation} failed with HTTP {status}"


def _sanitize_opencode_error(
    value: object,
    *,
    secrets: tuple[str, ...] = (),
    native_ids: tuple[str, ...] = (),
    default: str,
) -> str:
    detail = " ".join(str(value or "").split()) or default
    sensitive_values = tuple(
        sorted(
            {item for item in (*secrets, *native_ids) if item},
            key=len,
            reverse=True,
        )
    )
    for sensitive in sensitive_values:
        detail = detail.replace(sensitive, "<redacted>")
    return detail[:_HTTP_ERROR_DETAIL_MAX_CHARS]


def _sensitive_values(config: OpenCodeProviderConfig) -> tuple[str, ...]:
    markers = ("KEY", "TOKEN", "PASSWORD", "SECRET")
    values = {
        value
        for key, value in dict(config.env or {}).items()
        if value and any(marker in key.upper() for marker in markers)
    }
    if config.server_password:
        values.add(config.server_password)
    return tuple(sorted(values, key=len, reverse=True))


def _session_path(session_id: str) -> str:
    return f"/api/session/{quote(session_id, safe='')}"


def _turn_message_id(turn_ref: TurnRef) -> str:
    digest = hashlib.sha256(
        f"{turn_ref.runtime_generation.value}:{turn_ref.sequence}".encode("utf-8")
    ).hexdigest()
    return f"msg_{digest[:32]}"


def _steer_message_id(turn_ref: TurnRef, occurrence: int) -> str:
    if not isinstance(occurrence, int) or isinstance(occurrence, bool) or occurrence <= 0:
        raise ValueError("OpenCode steer occurrence must be positive")
    digest = hashlib.sha256(
        (
            f"{turn_ref.runtime_generation.value}:{turn_ref.sequence}:"
            f"steer:{occurrence}"
        ).encode("utf-8")
    ).hexdigest()
    return f"msg_{digest[:32]}"


def _error_text(exc: BaseException) -> str:
    return str(exc).strip() or type(exc).__name__


def _edge_failure_kind(exc: BaseException) -> str:
    if isinstance(exc, Full):
        return "queue_capacity"
    if isinstance(exc, AssertionError):
        return "assertion_error"
    if isinstance(exc, KeyError):
        return "key_error"
    if isinstance(exc, TypeError):
        return "type_error"
    if isinstance(exc, ValueError):
        return "value_error"
    if isinstance(exc, (OSError, URLError)):
        return "transport_error"
    return "internal_error"
