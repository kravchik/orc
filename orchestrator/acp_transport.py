"""ACP serialized stdio transport, callback registry, and host edge."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
from queue import Empty, Full, Queue
import re
import selectors
import shlex
import signal
import subprocess
import sys
import threading
import time
from typing import Mapping

from orchestrator.agent_capabilities import (
    AgentCapability,
    UNKNOWN_COMPONENT_VERSION,
    normalize_observed_runtime_version,
)
from orchestrator.agent_identity import (
    ApprovalRef,
    ElicitationRef,
    ProviderExecutionProfile,
    TurnRef,
)
from orchestrator.agent_provider import (
    OpenSessionCorrelationId,
    OpenSessionErrorKind,
    ProviderStartupFailure,
    ProviderStartupFailureCategory,
    ProviderSessionCatalogErrorKind,
    classify_provider_startup_failure,
    publish_provider_startup_failure,
)
from orchestrator.agent_session import (
    AgentApprovalKind,
    AgentApprovalRequest,
    AgentFormRequest,
    BoundedIdentityHistory,
    ElicitationAction,
    RawProviderErrorPayload,
)

from orchestrator.acp_protocol import (
    AcpProviderConfig,
    _APPROVAL_REVIEW_DECLINE_NOTICE,
    _AcpConnectionError,
    _AcpContractError,
    _AcpFailureDiagnostic,
    _ClientServiceEffect,
    _ConfigSnapshot,
    _FILESYSTEM_LINE_MAX,
    _FILESYSTEM_TEXT_MAX_BYTES,
    _FilesystemReadEffect,
    _FilesystemWriteEffect,
    _NegotiatedPromptCapabilities,
    _OwnedTerminal,
    _PermissionReview,
    _SessionUsage,
    _TERMINAL_ARG_MAX_COUNT,
    _TERMINAL_ENV_MAX_COUNT,
    _TERMINAL_ENV_TOTAL_MAX_BYTES,
    _TERMINAL_ENV_VALUE_MAX_CHARS,
    _TERMINAL_OUTPUT_MAX_BYTES,
    _TERMINAL_TEXT_MAX_CHARS,
    _TerminalCreateEffect,
    _acp_failure_diagnostic,
    _bounded_string,
    _classify_inbound_frame,
    _normalize_form_request,
    _normalize_permission_request,
    _open_workspace_directory,
    _open_workspace_parent,
    _parse_config_snapshot,
    _parse_effective_runtime,
    _parse_usage_update,
    _read_workspace_text,
    _validate_workspace_directory,
    _write_workspace_text,
)

_POLL_INTERVAL_SEC = 0.01
_OPEN_CLEANUP_RETRY_INTERVAL_SEC = 0.05
_CATALOG_MAX_PAGES = 64
_CATALOG_MAX_ENTRIES = 1024
_ACP_AGENT_INFO_TEXT_MAX_CHARS = 128


def _normalize_agent_info_text(value: object, *, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > _ACP_AGENT_INFO_TEXT_MAX_CHARS
        or not value.isprintable()
    ):
        raise ValueError(f"{field_name} must be bounded printable text")
    return value


@dataclass(frozen=True)
class _OpenCommand:
    correlation_id: OpenSessionCorrelationId
    cwd: str
    resume_native_session_id: str = ""
    requested_model: str = ""


@dataclass(frozen=True)
class _CatalogEntry:
    native_session_id: str
    name: str
    cwd: str
    created_at: str = ""
    updated_at: str = ""
    preview: str = ""


@dataclass(frozen=True)
class _CatalogSucceeded:
    entries: tuple[_CatalogEntry, ...]


@dataclass(frozen=True)
class _CatalogFailed:
    kind: ProviderSessionCatalogErrorKind
    error: str


_CatalogResult = _CatalogSucceeded | _CatalogFailed


@dataclass(frozen=True)
class _ListCommand:
    cwd: str
    limit: int | None
    result_queue: Queue[_CatalogResult] = field(repr=False, compare=False)


@dataclass(frozen=True)
class _CloseCommand:
    native_session_id: str


@dataclass(frozen=True)
class _PromptCommand:
    native_session_id: str
    turn_ref: TurnRef
    prompt: tuple[dict[str, object], ...]
    workspace: str


@dataclass(frozen=True)
class _CancelCommand:
    native_session_id: str
    turn_ref: TurnRef


@dataclass(frozen=True)
class _SetConfigCommand:
    native_session_id: str
    category: str
    config_id: str
    value: str


_CALLBACK_HANDOFF_WRITTEN = "written"
_CALLBACK_HANDOFF_STALE = "stale"
_CALLBACK_HANDOFF_UNCERTAIN = "uncertain"


class _CallbackResponseHandoff:
    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._state = "pending"

    def begin_write(self) -> bool:
        with self._condition:
            if self._state != "pending":
                return False
            self._state = "writing"
            return True

    def complete_written(self) -> None:
        self._complete(_CALLBACK_HANDOFF_WRITTEN)

    def complete_stale(self) -> None:
        self._complete(_CALLBACK_HANDOFF_STALE)

    def complete_uncertain(self) -> None:
        self._complete(_CALLBACK_HANDOFF_UNCERTAIN)

    def _complete(self, status: str) -> None:
        with self._condition:
            if self._state in {
                _CALLBACK_HANDOFF_WRITTEN,
                _CALLBACK_HANDOFF_STALE,
                _CALLBACK_HANDOFF_UNCERTAIN,
            }:
                return
            self._state = status
            self._condition.notify_all()

    def wait(self, timeout_sec: float) -> str:
        deadline = time.monotonic() + timeout_sec
        with self._condition:
            while self._state in {"pending", "writing"}:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._state = (
                        _CALLBACK_HANDOFF_STALE
                        if self._state == "pending"
                        else _CALLBACK_HANDOFF_UNCERTAIN
                    )
                    break
                self._condition.wait(timeout=remaining)
            return self._state


@dataclass(frozen=True)
class _ResolvePermissionCommand:
    native_session_id: str
    turn_ref: TurnRef
    approval_ref: ApprovalRef
    decision: str
    handoff: _CallbackResponseHandoff = field(repr=False, compare=False)


@dataclass(frozen=True)
class _ResolveFormCommand:
    native_session_id: str
    turn_ref: TurnRef
    elicitation_ref: ElicitationRef
    action: ElicitationAction
    content: tuple[tuple[str, object], ...]
    handoff: _CallbackResponseHandoff = field(repr=False, compare=False)


@dataclass(frozen=True)
class _StopCommand:
    pass


_EdgeCommand = (
    _OpenCommand
    | _ListCommand
    | _CloseCommand
    | _PromptCommand
    | _CancelCommand
    | _SetConfigCommand
    | _ResolvePermissionCommand
    | _ResolveFormCommand
    | _StopCommand
)


@dataclass(frozen=True)
class _OpenMetadata:
    model: str = "unknown"
    backend: str = "unknown"
    loaded: bool = False
    replay_update_count: int = 0
    config_snapshot: _ConfigSnapshot = _ConfigSnapshot()
    usage: _SessionUsage | None = None
    runtime_version: str = UNKNOWN_COMPONENT_VERSION


@dataclass(frozen=True)
class _OpenSucceeded:
    correlation_id: OpenSessionCorrelationId
    native_session_id: str
    metadata: _OpenMetadata = _OpenMetadata()


@dataclass(frozen=True)
class _OpenFailedEvent:
    correlation_id: OpenSessionCorrelationId
    kind: OpenSessionErrorKind
    error: str


@dataclass(frozen=True)
class _CloseSucceeded:
    native_session_id: str


@dataclass(frozen=True)
class _CloseFailed:
    native_session_id: str
    error: str


@dataclass(frozen=True)
class _TurnOutputEvent:
    native_session_id: str
    turn_ref: TurnRef
    text: str


@dataclass(frozen=True)
class _TurnCompletedEvent:
    native_session_id: str
    turn_ref: TurnRef


@dataclass(frozen=True)
class _TurnInterruptedEvent:
    native_session_id: str
    turn_ref: TurnRef


@dataclass(frozen=True)
class _TurnFailedEvent:
    native_session_id: str
    turn_ref: TurnRef
    error: str
    raw_provider_error: RawProviderErrorPayload | None = None
    fatal: bool = False


@dataclass(frozen=True)
class _ApprovalRequestedEvent:
    native_session_id: str
    turn_ref: TurnRef
    request: AgentApprovalRequest


@dataclass(frozen=True)
class _ProviderNoticeEvent:
    native_session_id: str
    turn_ref: TurnRef
    text: str


@dataclass(frozen=True)
class _ElicitationRequestedEvent:
    native_session_id: str
    turn_ref: TurnRef
    request: AgentFormRequest


@dataclass(frozen=True)
class _ConfigSnapshotEvent:
    native_session_id: str
    snapshot: _ConfigSnapshot


@dataclass(frozen=True)
class _UsageUpdatedEvent:
    native_session_id: str
    usage: _SessionUsage


@dataclass(frozen=True)
class _ConfigSelectionAcceptedEvent:
    native_session_id: str
    category: str
    requested_value: str
    snapshot: _ConfigSnapshot


@dataclass(frozen=True)
class _ConfigSelectionFailedEvent:
    native_session_id: str
    category: str
    requested_value: str
    error: str


@dataclass(frozen=True)
class _ConfigSelectionUncertainEvent:
    native_session_id: str
    category: str
    requested_value: str
    error: str


@dataclass(frozen=True)
class _SessionMetadataFailedEvent:
    native_session_id: str
    error: str


@dataclass(frozen=True)
class _HostFailed:
    error: str
    startup_failure_category: ProviderStartupFailureCategory | None = None


_EdgeEvent = (
    _OpenSucceeded
    | _OpenFailedEvent
    | _CloseSucceeded
    | _CloseFailed
    | _TurnOutputEvent
    | _TurnCompletedEvent
    | _TurnInterruptedEvent
    | _TurnFailedEvent
    | _ApprovalRequestedEvent
    | _ProviderNoticeEvent
    | _ElicitationRequestedEvent
    | _ConfigSnapshotEvent
    | _UsageUpdatedEvent
    | _ConfigSelectionAcceptedEvent
    | _ConfigSelectionFailedEvent
    | _ConfigSelectionUncertainEvent
    | _SessionMetadataFailedEvent
    | _HostFailed
)

_LifecycleTerminalEvent = (
    _OpenSucceeded | _OpenFailedEvent | _CloseSucceeded | _CloseFailed
)


@dataclass(frozen=True)
class _LifecycleTerminalHandoff:
    event: _LifecycleTerminalEvent
    release_admission: bool


@dataclass(frozen=True)
class _PendingOpen:
    command: _OpenCommand
    deadline: float


@dataclass
class _PendingReplayOpen:
    command: _OpenCommand
    native_session_id: str
    model: str
    backend: str
    config_snapshot: _ConfigSnapshot
    deadline: float
    update_count: int = 0
    replay_chars: int = 0
    usage: _SessionUsage | None = None


@dataclass(frozen=True)
class _PendingModelSelection:
    command: _OpenCommand
    native_session_id: str
    config_id: str
    deadline: float


@dataclass
class _PendingConfigSelection:
    command: _SetConfigCommand
    deadline: float
    requested_effect_observed: bool = False


@dataclass
class _PendingList:
    command: _ListCommand
    deadline: float
    entries: list[_CatalogEntry] = field(default_factory=list)
    cursors: set[str] = field(default_factory=set)
    page_count: int = 1


@dataclass(frozen=True)
class _PendingClose:
    native_session_id: str
    deadline: float


@dataclass
class _PendingOpenCleanup:
    correlation_id: OpenSessionCorrelationId
    native_session_id: str
    delete_after_close: bool
    deleting: bool
    deadline: float
    terminal: _OpenFailedEvent | None = None
    terminal_delivered: bool = False
    retry_logged: bool = False
    next_retry_at: float = 0.0


@dataclass
class _PendingPrompt:
    native_session_id: str
    turn_ref: TurnRef
    workspace: str
    completed_tool_call_ids: BoundedIdentityHistory[str]
    cancel_requested: bool = False
    active_tool_call_ids: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class _PendingPermissionCallback:
    request_id: int
    native_session_id: str
    turn_ref: TurnRef
    approval_ref: ApprovalRef
    tool_call_id: str
    option_ids: Mapping[str, str]


@dataclass(frozen=True)
class _PendingFormCallback:
    request_id: int
    native_session_id: str
    turn_ref: TurnRef
    elicitation_ref: ElicitationRef
    request: AgentFormRequest


@dataclass(frozen=True)
class _PendingClientServiceCallback:
    request_id: int
    native_session_id: str
    turn_ref: TurnRef
    approval_ref: ApprovalRef
    request: AgentApprovalRequest
    effect: _ClientServiceEffect


@dataclass(frozen=True)
class _PendingTerminalWaitCallback:
    request_id: int
    native_session_id: str
    turn_ref: TurnRef
    terminal_id: str


_PendingCallback = (
    _PendingPermissionCallback
    | _PendingFormCallback
    | _PendingClientServiceCallback
    | _PendingTerminalWaitCallback
)


def _callback_owner(callback: _PendingCallback) -> tuple[str, TurnRef]:
    return callback.native_session_id, callback.turn_ref


class _PendingCallbackRegistry:
    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._callbacks: dict[int, _PendingCallback] = {}

    def register(self, callback: _PendingCallback) -> str:
        existing = self._callbacks.get(callback.request_id)
        if existing is not None:
            return (
                "duplicate_same_owner"
                if _callback_owner(existing) == _callback_owner(callback)
                else "duplicate_cross_owner"
            )
        if any(
            _callback_owner(existing) == _callback_owner(callback)
            for existing in self._callbacks.values()
        ):
            return "owner_conflict"
        if len(self._callbacks) >= self._capacity:
            return "capacity"
        self._callbacks[callback.request_id] = callback
        return "registered"

    def duplicate_status(
        self,
        request_id: int,
        *,
        native_session_id: str,
        turn_ref: TurnRef,
    ) -> str | None:
        existing = self._callbacks.get(request_id)
        if existing is None:
            return None
        return (
            "duplicate_same_owner"
            if _callback_owner(existing) == (native_session_id, turn_ref)
            else "duplicate_cross_owner"
        )

    def pop_request_id(self, request_id: int) -> _PendingCallback | None:
        return self._callbacks.pop(request_id, None)

    def match_permission(
        self,
        command: _ResolvePermissionCommand,
    ) -> _PendingPermissionCallback | _PendingClientServiceCallback | None:
        matches = tuple(
            callback
            for callback in self._callbacks.values()
            if isinstance(
                callback,
                (_PendingPermissionCallback, _PendingClientServiceCallback),
            )
            and callback.native_session_id == command.native_session_id
            and callback.turn_ref == command.turn_ref
            and callback.approval_ref == command.approval_ref
        )
        return matches[0] if len(matches) == 1 else None

    def match_form(
        self,
        command: _ResolveFormCommand,
    ) -> _PendingFormCallback | None:
        matches = tuple(
            callback
            for callback in self._callbacks.values()
            if isinstance(callback, _PendingFormCallback)
            and callback.native_session_id == command.native_session_id
            and callback.turn_ref == command.turn_ref
            and callback.elicitation_ref == command.elicitation_ref
        )
        return matches[0] if len(matches) == 1 else None

    def pending_for_owner(
        self,
        *,
        native_session_id: str,
        turn_ref: TurnRef | None,
    ) -> tuple[_PendingCallback, ...]:
        return tuple(
            callback
            for callback in self._callbacks.values()
            if callback.native_session_id == native_session_id
            and (turn_ref is None or callback.turn_ref == turn_ref)
        )

    def pending_all(self) -> tuple[_PendingCallback, ...]:
        return tuple(self._callbacks.values())


_PendingRequest = (
    _PendingOpen
    | _PendingReplayOpen
    | _PendingModelSelection
    | _PendingList
    | _PendingClose
    | _PendingOpenCleanup
    | _PendingPrompt
    | _PendingConfigSelection
)

def _normalize_client_service_request(
    method: str,
    params: object,
    *,
    native_session_id: str,
    turn_ref: TurnRef,
    workspace: str,
) -> _PendingClientServiceCallback:
    if not isinstance(params, dict) or params.get("sessionId") != native_session_id:
        raise _AcpContractError("ACP client service correlation is invalid")
    request_id = params.pop("__request_id", None)
    if type(request_id) is not int:
        raise _AcpContractError("ACP client service request ID is invalid")
    approval_ref = ApprovalRef.new(turn_ref)
    if method in {"fs/read_text_file", "fs/write_text_file"}:
        allowed = (
            {"sessionId", "path", "line", "limit"}
            if method == "fs/read_text_file"
            else {"sessionId", "path", "content"}
        )
        if set(params).difference(allowed):
            raise _AcpContractError("ACP filesystem params are invalid")
        path = _bounded_string(
            params.get("path"), field_name="filesystem path", limit=4096
        )
        parent_fd, _ = _open_workspace_parent(workspace, path)
        os.close(parent_fd)
        if method == "fs/read_text_file":
            line = params.get("line")
            limit = params.get("limit")
            if line is not None and (
                type(line) is not int or not 1 <= line <= _FILESYSTEM_LINE_MAX
            ):
                raise _AcpContractError("ACP filesystem line is invalid")
            if limit is not None and (
                type(limit) is not int or not 1 <= limit <= _FILESYSTEM_LINE_MAX
            ):
                raise _AcpContractError("ACP filesystem limit is invalid")
            effect: _ClientServiceEffect = _FilesystemReadEffect(
                workspace, path, line, limit
            )
            title = "Read workspace file"
            kind = AgentApprovalKind.TOOL
        else:
            content = params.get("content")
            if not isinstance(content, str) or len(content.encode("utf-8")) > _FILESYSTEM_TEXT_MAX_BYTES:
                raise _AcpContractError("ACP filesystem content is invalid")
            effect = _FilesystemWriteEffect(workspace, path, content)
            title = "Write workspace file"
            kind = AgentApprovalKind.FILE_CHANGE
        request = AgentApprovalRequest(
            turn_ref=turn_ref,
            approval_ref=approval_ref,
            kind=kind,
            command=title,
            cwd=workspace,
            affected_paths=(path,),
            reason=f"ACP requested permission to {title.casefold()}",
            available_decisions=("accept", "decline"),
            prompt_details=(f"file: {path}",),
            diagnostics=(("client_service", method),),
        )
        return _PendingClientServiceCallback(
            request_id,
            native_session_id,
            turn_ref,
            approval_ref,
            request,
            effect,
        )
    if method != "terminal/create":
        raise _AcpContractError("ACP client service is unsupported")
    if set(params).difference(
        {"sessionId", "command", "args", "env", "cwd", "outputByteLimit"}
    ):
        raise _AcpContractError("ACP terminal params are invalid")
    command = _bounded_string(
        params.get("command"), field_name="terminal command", limit=_TERMINAL_TEXT_MAX_CHARS
    )
    args = params.get("args", [])
    if (
        not isinstance(args, list)
        or len(args) > _TERMINAL_ARG_MAX_COUNT
        or any(not isinstance(item, str) or len(item) > _TERMINAL_TEXT_MAX_CHARS for item in args)
    ):
        raise _AcpContractError("ACP terminal args are invalid")
    cwd = _validate_workspace_directory(
        workspace,
        _bounded_string(
            params.get("cwd"), field_name="terminal cwd", limit=4096
        ),
    )
    raw_env = params.get("env", [])
    if not isinstance(raw_env, list) or len(raw_env) > _TERMINAL_ENV_MAX_COUNT:
        raise _AcpContractError("ACP terminal env is invalid")
    env: list[tuple[str, str]] = []
    env_names: set[str] = set()
    env_total_bytes = 0
    for item in raw_env:
        if not isinstance(item, dict) or set(item) != {"name", "value"}:
            raise _AcpContractError("ACP terminal env entry is invalid")
        name = _bounded_string(
            item.get("name"), field_name="terminal env name", limit=128
        )
        value = item.get("value")
        if (
            re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) is None
            or not isinstance(value, str)
            or len(value) > _TERMINAL_ENV_VALUE_MAX_CHARS
            or any(character in value for character in ("\x00", "\r", "\n"))
            or name.startswith(("DYLD_", "LD_"))
            or name in {"PYTHONPATH", "PYTHONHOME"}
            or name in env_names
        ):
            raise _AcpContractError("ACP terminal env entry is invalid")
        env_total_bytes += len(name.encode("utf-8")) + len(value.encode("utf-8"))
        if env_total_bytes > _TERMINAL_ENV_TOTAL_MAX_BYTES:
            raise _AcpContractError("ACP terminal env exceeds capacity")
        env_names.add(name)
        env.append((name, value))
    output_limit = params.get("outputByteLimit")
    if (
        type(output_limit) is not int
        or not 1 <= output_limit <= _TERMINAL_OUTPUT_MAX_BYTES
    ):
        raise _AcpContractError("ACP terminal output limit is invalid")
    effect = _TerminalCreateEffect(
        workspace,
        command,
        tuple(args),
        cwd,
        tuple(env),
        output_limit,
    )
    display = shlex.join((command, *args))
    request = AgentApprovalRequest(
        turn_ref=turn_ref,
        approval_ref=approval_ref,
        kind=AgentApprovalKind.COMMAND,
        command=display,
        cwd=cwd,
        reason="ACP requested permission to start a client terminal",
        available_decisions=("accept", "decline"),
        prompt_details=(
            f"command: {display}",
            f"cwd: {cwd}",
            *(f"env: {name}={value}" for name, value in env),
        ),
        diagnostics=(("client_service", method),),
    )
    return _PendingClientServiceCallback(
        request_id,
        native_session_id,
        turn_ref,
        approval_ref,
        request,
        effect,
    )

def _parse_catalog_page(
    result: object,
    *,
    expected_cwd: str,
) -> tuple[tuple[_CatalogEntry, ...], str | None]:
    if not isinstance(result, dict):
        raise _AcpContractError("ACP session catalog response is invalid")
    raw_entries = result.get("sessions")
    if not isinstance(raw_entries, list):
        raise _AcpContractError("ACP session catalog entries are invalid")
    entries: list[_CatalogEntry] = []
    for raw in raw_entries:
        if not isinstance(raw, dict):
            raise _AcpContractError("ACP session catalog entry is invalid")
        native_session_id = _bounded_string(
            raw.get("sessionId"),
            field_name="session catalog identity",
            limit=1024,
        )
        cwd = _bounded_string(
            raw.get("cwd"),
            field_name="session catalog cwd",
            limit=4096,
        )
        if expected_cwd and cwd != expected_cwd:
            raise _AcpContractError("ACP session catalog returned a foreign cwd")
        title = raw.get("title", "")
        created_at = raw.get("createdAt", "")
        updated_at = raw.get("updatedAt", "")
        preview = raw.get("preview", "")
        for field_name, value, limit in (
            ("title", title, 1024),
            ("created timestamp", created_at, 128),
            ("updated timestamp", updated_at, 128),
            ("preview", preview, 4096),
        ):
            if not isinstance(value, str) or len(value) > limit:
                raise _AcpContractError(f"ACP session catalog {field_name} is invalid")
        entries.append(
            _CatalogEntry(
                native_session_id=native_session_id,
                name=title.strip(),
                cwd=cwd,
                created_at=created_at.strip(),
                updated_at=updated_at.strip(),
                preview=preview.strip(),
            )
        )
    next_cursor = result.get("nextCursor")
    if next_cursor is not None:
        next_cursor = _bounded_string(
            next_cursor,
            field_name="session catalog cursor",
            limit=4096,
        )
    return tuple(entries), next_cursor

class _AcpEdge:
    def __init__(self, config: AcpProviderConfig) -> None:
        self._config = config
        self._commands: Queue[_EdgeCommand] = Queue(maxsize=config.edge_queue_capacity)
        self._events: Queue[_EdgeEvent] = Queue(maxsize=config.edge_queue_capacity)
        self._lifecycle_terminals: deque[_LifecycleTerminalHandoff] = deque()
        self._lifecycle_terminal_lock = threading.Lock()
        self._turn_overflows: dict[tuple[str, TurnRef], _TurnFailedEvent] = {}
        self._turn_overflow_lock = threading.Lock()
        self._host_failure: _HostFailed | None = None
        self._host_failure_lock = threading.Lock()
        self._abandoned_turns: set[tuple[str, TurnRef]] = set()
        self._abandoned_turns_lock = threading.Lock()
        self._cancelled: set[OpenSessionCorrelationId] = set()
        self._cancel_lock = threading.Lock()
        self._catalog_waiters: set[Queue[_CatalogResult]] = set()
        self._catalog_waiters_lock = threading.Lock()
        self._open_cleanup_owners: dict[str, OpenSessionCorrelationId] = {}
        self._open_cleanup_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._process: subprocess.Popen[bytes] | None = None
        self._stderr_thread: threading.Thread | None = None
        self._progress = False
        self._progress_lock = threading.Lock()
        self._terminal_error = ""
        self._stopped = False
        self._process_started_logged = False
        self._process_stopped_logged = False
        self._request_sequence = 0
        self._admission_count = 0
        self._admission_lock = threading.Lock()
        self._negotiated_prompt_capabilities = _NegotiatedPromptCapabilities()
        self._runtime_version = UNKNOWN_COMPONENT_VERSION
        self._owned_terminals: dict[str, _OwnedTerminal] = {}
        self._released_terminal_owners: dict[str, tuple[str, TurnRef]] = {}
        self._terminal_sequence = 0

    def start(self) -> None:
        if self._thread is not None:
            return
        thread = threading.Thread(
            target=self._run,
            name=self._config.edge_thread_name,
            daemon=True,
        )
        self._thread = thread
        thread.start()

    def submit_open(self, command: _OpenCommand) -> None:
        self._submit(command)

    def list_sessions(self, cwd: str, limit: int | None) -> _CatalogResult:
        result_queue: Queue[_CatalogResult] = Queue(maxsize=1)
        with self._catalog_waiters_lock:
            self._catalog_waiters.add(result_queue)
        command = _ListCommand(cwd, limit, result_queue)
        try:
            self._submit(command)
        except RuntimeError:
            with self._catalog_waiters_lock:
                self._catalog_waiters.discard(result_queue)
            return _CatalogFailed(
                ProviderSessionCatalogErrorKind.UNAVAILABLE,
                "ACP session catalog admission is unavailable",
            )
        try:
            return result_queue.get(
                timeout=self._config.startup_timeout_sec + self._config.rpc_timeout_sec
            )
        except Empty:
            with self._catalog_waiters_lock:
                self._catalog_waiters.discard(result_queue)
            return _CatalogFailed(
                ProviderSessionCatalogErrorKind.PROVIDER_FAILED,
                "ACP session catalog timed out",
            )

    def cancel_open(self, correlation_id: OpenSessionCorrelationId) -> None:
        with self._cancel_lock:
            self._cancelled.add(correlation_id)
        self._mark_progress()

    def acknowledge_open(self, correlation_id: OpenSessionCorrelationId) -> None:
        with self._cancel_lock:
            self._cancelled.discard(correlation_id)

    def close_session(self, native_session_id: str) -> None:
        self._submit(_CloseCommand(native_session_id))

    def submit_turn(self, command: _PromptCommand) -> None:
        self._submit(command)

    def prompt_capabilities(self) -> _NegotiatedPromptCapabilities:
        return self._negotiated_prompt_capabilities

    def submit_interrupt(self, command: _CancelCommand) -> None:
        self._submit(command)

    def submit_config_selection(self, command: _SetConfigCommand) -> None:
        self._submit(command)

    def submit_permission(self, command: _ResolvePermissionCommand) -> None:
        self._submit(command)

    def submit_form(self, command: _ResolveFormCommand) -> None:
        self._submit(command)

    def abandon_turn(self, native_session_id: str, turn_ref: TurnRef) -> None:
        with self._abandoned_turns_lock:
            self._abandoned_turns.add((native_session_id, turn_ref))
        self._mark_progress()

    def poll_events(self, limit: int = 64) -> list[_EdgeEvent]:
        with self._host_failure_lock:
            host_failure = self._host_failure
            self._host_failure = None
        if host_failure is not None:
            self._discard_events_after_host_failure()
            return [host_failure]

        events: list[_EdgeEvent] = []
        lifecycle_handoffs: list[_LifecycleTerminalHandoff] = []
        with self._lifecycle_terminal_lock:
            while self._lifecycle_terminals and len(events) < limit:
                handoff = self._lifecycle_terminals.popleft()
                lifecycle_handoffs.append(handoff)
                events.append(handoff.event)
        for handoff in lifecycle_handoffs:
            if handoff.release_admission:
                self._release_admission()
        with self._turn_overflow_lock:
            overflow_keys = tuple(self._turn_overflows)[: max(0, limit - len(events))]
            for key in overflow_keys:
                events.append(self._turn_overflows.pop(key))
        for _ in overflow_keys:
            self._release_admission()
        for _ in range(max(0, limit - len(events))):
            try:
                events.append(self._events.get_nowait())
            except Empty:
                break
        return events

    def _discard_events_after_host_failure(self) -> None:
        with self._lifecycle_terminal_lock:
            self._lifecycle_terminals.clear()
        with self._turn_overflow_lock:
            self._turn_overflows.clear()
        while True:
            try:
                self._events.get_nowait()
            except Empty:
                break
        with self._admission_lock:
            self._admission_count = 0

    def consume_progress(self) -> bool:
        with self._progress_lock:
            value = self._progress
            self._progress = False
            return value

    def consume_terminal_error(self) -> str:
        error = self._terminal_error
        self._terminal_error = ""
        return error

    def stop(self) -> None:
        if self._stopped:
            return
        thread = self._thread
        if thread is None:
            self._stopped = True
            return
        if thread.is_alive():
            self._submit(_StopCommand(), reserve=False)
            thread.join(timeout=self._config.edge_stop_timeout_sec)
        if thread.is_alive():
            self._stop_process()
            thread.join(timeout=self._config.edge_stop_timeout_sec)
        if thread.is_alive():
            raise RuntimeError("ACP edge thread did not stop")
        if self._process is not None:
            try:
                self._stop_process()
            except Exception:
                raise RuntimeError("ACP host cleanup failed") from None
            self._log_process_stopped()
            self._process = None
        if self._owned_terminals:
            try:
                self._release_all_terminals()
            except _AcpConnectionError:
                raise RuntimeError("ACP client terminal cleanup failed") from None
        self._thread = None
        self._stopped = True

    def _submit(self, command: _EdgeCommand, *, reserve: bool = True) -> None:
        if self._stopped:
            raise RuntimeError("ACP edge is stopped")
        if reserve:
            with self._admission_lock:
                if self._admission_count >= self._config.edge_queue_capacity + 1:
                    raise RuntimeError(
                        "ACP edge command queue capacity exceeded"
                    )
                self._admission_count += 1
        try:
            self._commands.put_nowait(command)
        except Full:
            if reserve:
                self._release_admission()
            raise RuntimeError("ACP edge command queue capacity exceeded") from None
        self._mark_progress()

    def _release_admission(self) -> None:
        with self._admission_lock:
            if self._admission_count > 0:
                self._admission_count -= 1

    def _run(self) -> None:
        bootstrap_complete = False
        try:
            process = subprocess.Popen(
                list(self._config.command),
                cwd=None,
                env=dict(self._config.env or {}),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
            )
            self._process = process
            self._config.logger.event(
                "process_started",
                provider=self._config.runtime_profile.provider.value,
                profile=self._config.runtime_profile.profile.value,
                role=self._config.role,
                pid=process.pid,
                ownership_scope="direct_process",
            )
            self._process_started_logged = True
            self._stderr_thread = threading.Thread(
                target=self._drain_stderr,
                args=(process,),
                name=f"{self._config.edge_thread_name}-stderr",
                daemon=True,
            )
            self._stderr_thread.start()
            self._initialize(process)
            bootstrap_complete = True
            self._serve(process)
        except BaseException as exc:
            self._fail_catalog_waiters("ACP session catalog is unavailable")
            startup_failure_category = (
                None if bootstrap_complete else classify_provider_startup_failure(exc)
            )
            if startup_failure_category is not None:
                publish_provider_startup_failure(
                    self._config.logger,
                    ProviderExecutionProfile(
                        self._config.runtime_profile.provider,
                        self._config.runtime_profile.profile,
                    ),
                    startup_failure_category,
                )
            self._publish(
                _HostFailed(
                    "ACP host failed",
                    startup_failure_category,
                )
            )
        finally:
            self._fail_catalog_waiters("ACP session catalog is unavailable")
            try:
                self._stop_process()
            except Exception:
                self._terminal_error = "ACP host cleanup failed"
            else:
                self._log_process_stopped()
                self._process = None
            self._mark_progress()

    def _serve(self, process: subprocess.Popen[bytes]) -> None:
        pending: dict[int, _PendingRequest] = {}
        callbacks = _PendingCallbackRegistry(self._config.edge_queue_capacity)
        prompt_requests: dict[str, int] = {}
        replay_requests: dict[str, int] = {}
        cleanup_retries: dict[OpenSessionCorrelationId, _PendingOpenCleanup] = {}
        stop_requested = False
        try:
            while True:
                self._detach_abandoned_turns(process, pending, prompt_requests, callbacks)
                self._service_terminal_waiters(process, callbacks)
                if process.poll() is not None:
                    raise _AcpContractError("ACP process exited")
                while not stop_requested:
                    try:
                        command = self._commands.get_nowait()
                    except Empty:
                        break
                    if isinstance(command, _StopCommand):
                        self._cancel_all_callbacks(process, callbacks)
                        stop_requested = True
                        break
                    self._dispatch_command(
                        process,
                        command,
                        pending,
                        prompt_requests,
                        callbacks,
                    )

                self._retry_open_cleanups(process, pending, cleanup_retries)
                if stop_requested and not cleanup_retries and not any(
                    isinstance(
                        request,
                        (
                            _PendingOpen,
                            _PendingReplayOpen,
                            _PendingModelSelection,
                            _PendingConfigSelection,
                            _PendingList,
                            _PendingOpenCleanup,
                        ),
                    )
                    for request in pending.values()
                ):
                    return

                frame = self._read_frame_if_available(
                    process,
                    timeout_sec=_POLL_INTERVAL_SEC,
                )
                if frame is not None:
                    self._handle_frame(
                        process,
                        frame,
                        pending,
                        prompt_requests,
                        replay_requests,
                        cleanup_retries,
                        callbacks,
                    )
                self._expire_requests(
                    process,
                    pending,
                    replay_requests,
                    cleanup_retries,
                )
        finally:
            self._release_all_terminals()

    def _detach_abandoned_turns(
        self,
        process: subprocess.Popen[bytes],
        pending: dict[int, _PendingRequest],
        prompt_requests: dict[str, int],
        callbacks: _PendingCallbackRegistry,
    ) -> None:
        with self._abandoned_turns_lock:
            abandoned = tuple(self._abandoned_turns)
            self._abandoned_turns.clear()
        for native_session_id, turn_ref in abandoned:
            request_id = prompt_requests.get(native_session_id)
            request = pending.get(request_id) if request_id is not None else None
            if isinstance(request, _PendingPrompt) and request.turn_ref == turn_ref:
                self._cancel_callbacks(
                    process,
                    callbacks,
                    native_session_id=native_session_id,
                    turn_ref=turn_ref,
                )
                prompt_requests.pop(native_session_id, None)
                pending.pop(request_id, None)
                self._release_owned_terminals(
                    native_session_id=native_session_id,
                    turn_ref=turn_ref,
                )
                self._release_admission()

    def _dispatch_command(
        self,
        process: subprocess.Popen[bytes],
        command: _OpenCommand
        | _ListCommand
        | _CloseCommand
        | _PromptCommand
        | _CancelCommand
        | _SetConfigCommand
        | _ResolvePermissionCommand
        | _ResolveFormCommand,
        pending: dict[int, _PendingRequest],
        prompt_requests: dict[str, int],
        callbacks: _PendingCallbackRegistry,
    ) -> None:
        deadline = time.monotonic() + self._config.rpc_timeout_sec
        if isinstance(command, _OpenCommand):
            if command.resume_native_session_id and self._owns_open_cleanup(
                command.resume_native_session_id
            ):
                self._publish_open_failure(
                    command.correlation_id,
                    release_admission=True,
                    error="ACP session cleanup is pending",
                )
                return
            method = "session/resume" if command.resume_native_session_id else "session/new"
            params: dict[str, object] = {"cwd": command.cwd, "mcpServers": []}
            if command.resume_native_session_id:
                params["sessionId"] = command.resume_native_session_id
            request_id = self._send_request(
                process,
                method,
                params,
            )
            pending[request_id] = _PendingOpen(command, deadline)
            return
        if isinstance(command, _ListCommand):
            self._dispatch_list_page(process, command, pending, cursor=None)
            return
        if isinstance(command, _CloseCommand):
            self._cancel_callbacks(
                process,
                callbacks,
                native_session_id=command.native_session_id,
                turn_ref=None,
            )
            self._release_owned_terminals(native_session_id=command.native_session_id)
            request_id = self._send_request(
                process,
                "session/close",
                {"sessionId": command.native_session_id},
            )
            pending[request_id] = _PendingClose(command.native_session_id, deadline)
            return
        if isinstance(command, _CancelCommand):
            request_id = prompt_requests.get(command.native_session_id)
            request = pending.get(request_id) if request_id is not None else None
            if isinstance(request, _PendingPrompt) and request.turn_ref == command.turn_ref:
                request.cancel_requested = True
                self._cancel_callbacks(
                    process,
                    callbacks,
                    native_session_id=command.native_session_id,
                    turn_ref=command.turn_ref,
                )
                self._release_owned_terminals(
                    native_session_id=command.native_session_id,
                    turn_ref=command.turn_ref,
                )
                self._write_frame(
                    process,
                    {
                        "jsonrpc": "2.0",
                        "method": "session/cancel",
                        "params": {"sessionId": command.native_session_id},
                    },
                )
            self._release_admission()
            return
        if isinstance(command, _SetConfigCommand):
            try:
                request_id = self._send_request(
                    process,
                    "session/set_config_option",
                    {
                        "sessionId": command.native_session_id,
                        "configId": command.config_id,
                        "value": command.value,
                    },
                )
            except _AcpContractError:
                if not self._publish(
                    _ConfigSelectionUncertainEvent(
                        command.native_session_id,
                        command.category,
                        command.value,
                        "ACP configuration selection outcome is uncertain",
                    )
                ):
                    raise
                self._release_admission()
                return
            pending[request_id] = _PendingConfigSelection(command, deadline)
            return
        if isinstance(command, _ResolvePermissionCommand):
            self._resolve_permission_callback(
                process,
                command,
                callbacks,
                pending,
                prompt_requests,
            )
            self._release_admission()
            return
        if isinstance(command, _ResolveFormCommand):
            self._resolve_form_callback(process, command, callbacks)
            self._release_admission()
            return
        if command.native_session_id in prompt_requests:
            published = self._publish(
                _TurnFailedEvent(
                    command.native_session_id,
                    command.turn_ref,
                    "ACP session is busy",
                )
            )
            if published:
                self._release_admission()
            return
        request_id = self._send_request(
            process,
            "session/prompt",
            {
                "sessionId": command.native_session_id,
                "prompt": list(command.prompt),
            },
        )
        pending[request_id] = _PendingPrompt(
            command.native_session_id,
            command.turn_ref,
            command.workspace,
            BoundedIdentityHistory(self._config.edge_queue_capacity),
        )
        prompt_requests[command.native_session_id] = request_id

    def _dispatch_list_page(
        self,
        process: subprocess.Popen[bytes],
        command: _ListCommand,
        pending: dict[int, _PendingRequest],
        *,
        cursor: str | None,
        state: _PendingList | None = None,
    ) -> None:
        params: dict[str, object] = {"cwd": command.cwd}
        if cursor is not None:
            params["cursor"] = cursor
        request_id = self._send_request(process, "session/list", params)
        pending[request_id] = state or _PendingList(
            command=command,
            deadline=time.monotonic() + self._config.rpc_timeout_sec,
        )

    def _complete_catalog(
        self,
        command: _ListCommand,
        result: _CatalogResult,
    ) -> None:
        with self._catalog_waiters_lock:
            owned = command.result_queue in self._catalog_waiters
            self._catalog_waiters.discard(command.result_queue)
        if owned:
            try:
                command.result_queue.put_nowait(result)
            except Full:
                pass
        self._release_admission()

    def _fail_catalog_waiters(self, error: str) -> None:
        with self._catalog_waiters_lock:
            waiters = tuple(self._catalog_waiters)
            self._catalog_waiters.clear()
        for waiter in waiters:
            try:
                waiter.put_nowait(
                    _CatalogFailed(ProviderSessionCatalogErrorKind.UNAVAILABLE, error)
                )
            except Full:
                continue

    def _send_request(
        self,
        process: subprocess.Popen[bytes],
        method: str,
        params: dict[str, object],
    ) -> int:
        request_id = self._next_request_id()
        self._write_frame(
            process,
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
        )
        return request_id

    def _handle_frame(
        self,
        process: subprocess.Popen[bytes],
        frame: dict[str, object],
        pending: dict[int, _PendingRequest],
        prompt_requests: dict[str, int],
        replay_requests: dict[str, int],
        cleanup_retries: dict[OpenSessionCorrelationId, _PendingOpenCleanup],
        callbacks: _PendingCallbackRegistry,
    ) -> None:
        classified = _classify_inbound_frame(frame)
        if classified.kind == "response":
            assert classified.request_id is not None
            request = pending.pop(classified.request_id, None)
            if request is None:
                self._config.logger.event(
                    "acp_stale_response_ignored",
                    provider=self._config.runtime_profile.provider.value,
                    profile=self._config.runtime_profile.profile.value,
                )
                return
            self._handle_response(
                process,
                request,
                frame,
                pending,
                prompt_requests,
                replay_requests,
                cleanup_retries,
                callbacks,
            )
            return
        if classified.kind == "notification":
            if classified.method != "session/update":
                return
            self._handle_session_update(
                process,
                frame,
                pending,
                prompt_requests,
                replay_requests,
            )
            return
        if classified.kind == "request":
            self._handle_callback(
                process,
                frame,
                pending,
                prompt_requests,
                replay_requests,
                callbacks,
            )
            return
        raise AssertionError(f"unhandled ACP frame classification: {classified.kind}")

    def _handle_response(
        self,
        process: subprocess.Popen[bytes],
        request: _PendingRequest,
        frame: dict[str, object],
        pending: dict[int, _PendingRequest],
        prompt_requests: dict[str, int],
        replay_requests: dict[str, int],
        cleanup_retries: dict[OpenSessionCorrelationId, _PendingOpenCleanup],
        callbacks: _PendingCallbackRegistry,
    ) -> None:
        if isinstance(request, _PendingPrompt):
            prompt_requests.pop(request.native_session_id, None)
            self._cancel_callbacks(
                process,
                callbacks,
                native_session_id=request.native_session_id,
                turn_ref=request.turn_ref,
            )
            self._release_owned_terminals(
                native_session_id=request.native_session_id,
                turn_ref=request.turn_ref,
                clear_released=True,
            )
            if "error" in frame:
                self._log_prompt_failure(_acp_failure_diagnostic(frame))
                raw_provider_error = RawProviderErrorPayload.from_json_object(
                    frame["error"]
                )
                self._publish_prompt_terminal(
                    _TurnFailedEvent(
                        request.native_session_id,
                        request.turn_ref,
                        f"{self._config.runtime_profile.runtime_display_name} execution failed",
                        raw_provider_error,
                    )
                )
                return
            result = frame.get("result")
            if not isinstance(result, dict) or not isinstance(result.get("stopReason"), str):
                self._publish_prompt_terminal(
                    _TurnFailedEvent(
                        request.native_session_id,
                        request.turn_ref,
                        "ACP prompt result is invalid",
                        fatal=True,
                    )
                )
                return
            stop_reason = result["stopReason"]
            if stop_reason == "end_turn":
                self._publish_prompt_terminal(
                    _TurnCompletedEvent(request.native_session_id, request.turn_ref)
                )
            elif stop_reason == "cancelled" and request.cancel_requested:
                self._publish_prompt_terminal(
                    _TurnInterruptedEvent(
                        request.native_session_id,
                        request.turn_ref,
                    )
                )
            elif stop_reason in {
                "cancelled",
                "max_tokens",
                "max_turn_requests",
                "refusal",
            }:
                self._log_prompt_failure(
                    _AcpFailureDiagnostic(
                        error_code=None,
                        error_category=f"stop_{stop_reason}",
                        service="missing",
                        error_name="missing",
                    )
                )
                self._publish_prompt_terminal(
                    _TurnFailedEvent(
                        request.native_session_id,
                        request.turn_ref,
                        f"ACP turn stopped: {stop_reason}",
                        RawProviderErrorPayload.from_json_object(result),
                    )
                )
            else:
                self._log_prompt_failure(
                    _AcpFailureDiagnostic(
                        error_code=None,
                        error_category="invalid_stop_reason",
                        service="missing",
                        error_name="missing",
                    )
                )
                self._publish_prompt_terminal(
                    _TurnFailedEvent(
                        request.native_session_id,
                        request.turn_ref,
                        "ACP prompt stop reason is invalid",
                        fatal=True,
                    )
                )
            return

        if isinstance(request, _PendingList):
            if "error" in frame:
                self._complete_catalog(
                    request.command,
                    _CatalogFailed(
                        ProviderSessionCatalogErrorKind.PROVIDER_FAILED,
                        "ACP session catalog request failed",
                    ),
                )
                return
            try:
                page_entries, next_cursor = _parse_catalog_page(
                    frame.get("result"),
                    expected_cwd=request.command.cwd,
                )
                known_ids = {item.native_session_id for item in request.entries}
                if any(item.native_session_id in known_ids for item in page_entries):
                    raise _AcpContractError(
                        "ACP session catalog repeated a session identity"
                    )
                request.entries.extend(page_entries)
                if len(request.entries) > _CATALOG_MAX_ENTRIES:
                    raise _AcpContractError(
                        "ACP session catalog entry capacity exceeded"
                    )
                requested_limit = request.command.limit
                if requested_limit is not None and len(request.entries) >= requested_limit:
                    self._complete_catalog(
                        request.command,
                        _CatalogSucceeded(tuple(request.entries[:requested_limit])),
                    )
                    return
                if next_cursor is None:
                    self._complete_catalog(
                        request.command,
                        _CatalogSucceeded(tuple(request.entries)),
                    )
                    return
                if (
                    request.page_count >= _CATALOG_MAX_PAGES
                    or next_cursor in request.cursors
                ):
                    raise _AcpContractError("ACP session catalog cursor is invalid")
                request.cursors.add(next_cursor)
                request.page_count += 1
                request.deadline = time.monotonic() + self._config.rpc_timeout_sec
                self._dispatch_list_page(
                    process,
                    request.command,
                    pending,
                    cursor=next_cursor,
                    state=request,
                )
            except _AcpContractError:
                self._complete_catalog(
                    request.command,
                    _CatalogFailed(
                        ProviderSessionCatalogErrorKind.PROVIDER_FAILED,
                        "ACP session catalog response is invalid",
                    ),
                )
            return

        if isinstance(request, _PendingConfigSelection):
            command = request.command
            if "error" in frame:
                event: _EdgeEvent = (
                    _ConfigSelectionUncertainEvent(
                        command.native_session_id,
                        command.category,
                        command.value,
                        "ACP configuration selection outcome is uncertain",
                    )
                    if request.requested_effect_observed
                    else _ConfigSelectionFailedEvent(
                        command.native_session_id,
                        command.category,
                        command.value,
                        "ACP configuration selection was rejected",
                    )
                )
            else:
                try:
                    snapshot = _parse_config_snapshot(frame.get("result"))
                    option = snapshot.by_category(command.category)
                    if (
                        option is None
                        or option.config_id != command.config_id
                        or option.current_value != command.value
                    ):
                        raise _AcpContractError(
                            "ACP configuration selection was not confirmed"
                        )
                except _AcpContractError:
                    event = _ConfigSelectionUncertainEvent(
                        command.native_session_id,
                        command.category,
                        command.value,
                        "ACP configuration selection outcome is uncertain",
                    )
                else:
                    event = _ConfigSelectionAcceptedEvent(
                        command.native_session_id,
                        command.category,
                        command.value,
                        snapshot,
                    )
            if not self._publish(event):
                raise _AcpContractError("ACP configuration result delivery failed")
            self._release_admission()
            return

        if isinstance(request, _PendingReplayOpen):
            replay_requests.pop(request.native_session_id, None)
            if "error" in frame:
                terminal = self._resume_failure_event(
                    request.command.correlation_id, frame
                )
                self._begin_failed_open_cleanup(
                    process,
                    pending,
                    request.command,
                    request.native_session_id,
                    error=terminal.error,
                    kind=terminal.kind,
                )
                return
            result = frame.get("result")
            returned_id = result.get("sessionId") if isinstance(result, dict) else None
            try:
                loaded = _parse_effective_runtime(
                    result,
                    fallback_backend=self._config.runtime_profile.backend_name,
                )
                if (
                    not isinstance(result, dict)
                    or returned_id not in (None, request.native_session_id)
                    or loaded.model != request.model
                ):
                    raise _AcpContractError("ACP session replay response is invalid")
            except _AcpContractError:
                self._begin_failed_open_cleanup(
                    process,
                    pending,
                    request.command,
                    request.native_session_id,
                    error="ACP session replay response is invalid",
                )
                return
            if self._is_cancelled(request.command.correlation_id):
                self._begin_cancelled_open_cleanup(
                    process,
                    pending,
                    request.command,
                    request.native_session_id,
                )
                return
            self._publish_lifecycle_terminal(
                _OpenSucceeded(
                    request.command.correlation_id,
                    request.native_session_id,
                    _OpenMetadata(
                        model=loaded.model,
                        backend=loaded.backend,
                        loaded=True,
                        replay_update_count=request.update_count,
                        config_snapshot=loaded.config_snapshot,
                        runtime_version=self._runtime_version,
                    ),
                ),
                release_admission=True,
            )
            return

        if isinstance(request, _PendingModelSelection):
            if "error" in frame:
                self._begin_failed_open_cleanup(
                    process,
                    pending,
                    request.command,
                    request.native_session_id,
                    error="ACP requested startup model is unavailable",
                )
                return
            try:
                selection = _parse_effective_runtime(
                    frame.get("result"),
                    fallback_backend=self._config.runtime_profile.backend_name,
                )
                if (
                    selection.model_config_id != request.config_id
                    or selection.model != request.command.requested_model
                ):
                    raise _AcpContractError("ACP selected model did not match request")
            except _AcpContractError:
                self._begin_failed_open_cleanup(
                    process,
                    pending,
                    request.command,
                    request.native_session_id,
                    error="ACP requested startup model was not confirmed",
                )
                return
            if self._is_cancelled(request.command.correlation_id):
                self._begin_cancelled_open_cleanup(
                    process,
                    pending,
                    request.command,
                    request.native_session_id,
                )
                return
            self._publish_lifecycle_terminal(
                _OpenSucceeded(
                    request.command.correlation_id,
                    request.native_session_id,
                    _OpenMetadata(
                        model=selection.model,
                        backend=selection.backend,
                        config_snapshot=selection.config_snapshot,
                        runtime_version=self._runtime_version,
                    ),
                ),
                release_admission=True,
            )
            return

        if isinstance(request, _PendingOpen):
            if "error" in frame:
                if request.command.resume_native_session_id:
                    self._publish_resume_failure(
                        request.command.correlation_id,
                        frame,
                        release_admission=True,
                    )
                else:
                    self._publish_open_failure(
                        request.command.correlation_id,
                        release_admission=True,
                    )
                return
            result = frame.get("result")
            returned_id = result.get("sessionId") if isinstance(result, dict) else None
            expected_id = request.command.resume_native_session_id
            native_session_id = expected_id or returned_id
            if (
                not isinstance(native_session_id, str)
                or not native_session_id.strip()
                or (expected_id and returned_id not in (None, expected_id))
            ):
                self._publish_open_failure(
                    request.command.correlation_id,
                    release_admission=True,
                    error="ACP session identity correlation failed",
                )
                return
            try:
                selection = _parse_effective_runtime(
                    result,
                    fallback_backend=self._config.runtime_profile.backend_name,
                )
            except _AcpContractError:
                self._begin_failed_open_cleanup(
                    process,
                    pending,
                    request.command,
                    native_session_id,
                    error="ACP session configuration is invalid",
                )
                return
            if self._is_cancelled(request.command.correlation_id):
                self._begin_cancelled_open_cleanup(
                    process,
                    pending,
                    request.command,
                    native_session_id,
                )
                return
            if expected_id:
                if (
                    request.command.requested_model
                    and selection.model != request.command.requested_model
                ):
                    self._begin_failed_open_cleanup(
                        process,
                        pending,
                        request.command,
                        native_session_id,
                        error="ACP resumed session model does not match requested model",
                    )
                    return
                load_id = self._send_request(
                    process,
                    "session/load",
                    {
                        "sessionId": native_session_id,
                        "cwd": request.command.cwd,
                        "mcpServers": [],
                    },
                )
                pending[load_id] = _PendingReplayOpen(
                    command=request.command,
                    native_session_id=native_session_id,
                    model=selection.model,
                    backend=selection.backend,
                    config_snapshot=selection.config_snapshot,
                    deadline=time.monotonic() + self._config.rpc_timeout_sec,
                )
                replay_requests[native_session_id] = load_id
                return
            if request.command.requested_model:
                if (
                    not selection.model_config_id
                    or request.command.requested_model not in selection.enabled_models
                ):
                    self._begin_failed_open_cleanup(
                        process,
                        pending,
                        request.command,
                        native_session_id,
                        error="ACP requested startup model is unavailable",
                    )
                    return
                selection_id = self._send_request(
                    process,
                    "session/set_config_option",
                    {
                        "sessionId": native_session_id,
                        "configId": selection.model_config_id,
                        "value": request.command.requested_model,
                    },
                )
                pending[selection_id] = _PendingModelSelection(
                    command=request.command,
                    native_session_id=native_session_id,
                    config_id=selection.model_config_id,
                    deadline=time.monotonic() + self._config.rpc_timeout_sec,
                )
                return
            self._publish_lifecycle_terminal(
                _OpenSucceeded(
                    request.command.correlation_id,
                    native_session_id,
                    _OpenMetadata(
                        model=selection.model,
                        backend=selection.backend,
                        config_snapshot=selection.config_snapshot,
                        runtime_version=self._runtime_version,
                    ),
                ),
                release_admission=True,
            )
            return

        if isinstance(request, _PendingOpenCleanup):
            if "error" in frame or not isinstance(frame.get("result"), dict):
                self._defer_open_cleanup_retry(
                    request,
                    cleanup_retries,
                )
                return
            if request.delete_after_close and not request.deleting:
                delete_id = self._send_request(
                    process,
                    "session/delete",
                    {"sessionId": request.native_session_id},
                )
                request.deleting = True
                request.deadline = time.monotonic() + self._config.rpc_timeout_sec
                pending[delete_id] = request
                return
            self._finish_open_cleanup(request)
            return
        if "error" in frame or not isinstance(frame.get("result"), dict):
            self._publish_lifecycle_terminal(
                _CloseFailed(
                    request.native_session_id,
                    "ACP session cleanup failed",
                ),
                release_admission=True,
            )
        else:
            self._publish_lifecycle_terminal(
                _CloseSucceeded(request.native_session_id),
                release_admission=True,
            )

    def _publish_open_failure(
        self,
        correlation_id: OpenSessionCorrelationId,
        *,
        release_admission: bool,
        error: str = "",
    ) -> None:
        cancelled = self._is_cancelled(correlation_id)
        self._publish_lifecycle_terminal(
            _OpenFailedEvent(
                correlation_id,
                (
                    OpenSessionErrorKind.CANCELLED
                    if cancelled
                    else OpenSessionErrorKind.STARTUP_FAILED
                ),
                (
                    "ACP session open was cancelled"
                    if cancelled
                    else error or "ACP session startup failed"
                ),
            ),
            release_admission=release_admission,
        )

    def _publish_resume_failure(
        self,
        correlation_id: OpenSessionCorrelationId,
        frame: dict[str, object],
        *,
        release_admission: bool,
    ) -> None:
        self._publish_lifecycle_terminal(
            self._resume_failure_event(correlation_id, frame),
            release_admission=release_admission,
        )

    @staticmethod
    def _resume_failure_event(
        correlation_id: OpenSessionCorrelationId,
        frame: dict[str, object],
    ) -> _OpenFailedEvent:
        error = frame.get("error")
        code = error.get("code") if isinstance(error, dict) else None
        return _OpenFailedEvent(
            correlation_id,
            (
                OpenSessionErrorKind.INVALID_REQUEST
                if code == -32002
                else OpenSessionErrorKind.STARTUP_FAILED
            ),
            (
                "ACP session is unavailable"
                if code == -32002
                else "ACP session resume failed"
            ),
        )

    def _begin_cancelled_open_cleanup(
        self,
        process: subprocess.Popen[bytes],
        pending: dict[int, _PendingRequest],
        command: _OpenCommand,
        native_session_id: str,
    ) -> None:
        self._claim_open_cleanup(command.correlation_id, native_session_id)
        self._publish_lifecycle_terminal(
            _OpenFailedEvent(
                command.correlation_id,
                OpenSessionErrorKind.CANCELLED,
                "ACP session open was cancelled",
            ),
            release_admission=False,
        )
        close_id = self._send_request(
            process,
            "session/close",
            {"sessionId": native_session_id},
        )
        pending[close_id] = _PendingOpenCleanup(
            correlation_id=command.correlation_id,
            native_session_id=native_session_id,
            delete_after_close=not bool(command.resume_native_session_id),
            deleting=False,
            deadline=time.monotonic() + self._config.rpc_timeout_sec,
        )

    def _begin_failed_open_cleanup(
        self,
        process: subprocess.Popen[bytes],
        pending: dict[int, _PendingRequest],
        command: _OpenCommand,
        native_session_id: str,
        *,
        error: str,
        kind: OpenSessionErrorKind = OpenSessionErrorKind.STARTUP_FAILED,
    ) -> None:
        self._claim_open_cleanup(command.correlation_id, native_session_id)
        close_id = self._send_request(
            process,
            "session/close",
            {"sessionId": native_session_id},
        )
        pending[close_id] = _PendingOpenCleanup(
            correlation_id=command.correlation_id,
            native_session_id=native_session_id,
            delete_after_close=not bool(command.resume_native_session_id),
            deleting=False,
            deadline=time.monotonic() + self._config.rpc_timeout_sec,
            terminal=_OpenFailedEvent(
                command.correlation_id,
                kind,
                error,
            ),
        )

    def _finish_open_cleanup(
        self,
        request: _PendingOpenCleanup,
    ) -> None:
        self._release_open_cleanup(request)
        self._config.logger.event(
            "acp_session_cleanup_completed",
            provider=self._config.runtime_profile.provider.value,
            profile=self._config.runtime_profile.profile.value,
        )
        if request.terminal is not None and not request.terminal_delivered:
            self._publish_lifecycle_terminal(
                request.terminal,
                release_admission=True,
            )
        else:
            self._release_admission()

    def _claim_open_cleanup(
        self,
        correlation_id: OpenSessionCorrelationId,
        native_session_id: str,
    ) -> None:
        with self._open_cleanup_lock:
            self._open_cleanup_owners[native_session_id] = correlation_id

    def _owns_open_cleanup(self, native_session_id: str) -> bool:
        with self._open_cleanup_lock:
            return native_session_id in self._open_cleanup_owners

    def _release_open_cleanup(self, request: _PendingOpenCleanup) -> None:
        with self._open_cleanup_lock:
            if (
                self._open_cleanup_owners.get(request.native_session_id)
                == request.correlation_id
            ):
                self._open_cleanup_owners.pop(request.native_session_id, None)

    def _defer_open_cleanup_retry(
        self,
        request: _PendingOpenCleanup,
        cleanup_retries: dict[OpenSessionCorrelationId, _PendingOpenCleanup],
    ) -> None:
        if request.terminal is not None and not request.terminal_delivered:
            self._publish_lifecycle_terminal(
                request.terminal,
                release_admission=False,
            )
            request.terminal_delivered = True
        if not request.retry_logged:
            request.retry_logged = True
            self._config.logger.event(
                "acp_session_cleanup_retry",
                provider=self._config.runtime_profile.provider.value,
                profile=self._config.runtime_profile.profile.value,
            )
        request.next_retry_at = (
            time.monotonic() + _OPEN_CLEANUP_RETRY_INTERVAL_SEC
        )
        cleanup_retries[request.correlation_id] = request

    def _retry_open_cleanups(
        self,
        process: subprocess.Popen[bytes],
        pending: dict[int, _PendingRequest],
        cleanup_retries: dict[OpenSessionCorrelationId, _PendingOpenCleanup],
    ) -> None:
        now = time.monotonic()
        for correlation_id, request in tuple(cleanup_retries.items()):
            if request.next_retry_at > now:
                continue
            cleanup_retries.pop(correlation_id, None)
            method = "session/delete" if request.deleting else "session/close"
            request_id = self._send_request(
                process,
                method,
                {"sessionId": request.native_session_id},
            )
            request.deadline = now + self._config.rpc_timeout_sec
            pending[request_id] = request

    def _handle_session_update(
        self,
        process: subprocess.Popen[bytes],
        frame: dict[str, object],
        pending: dict[int, _PendingRequest],
        prompt_requests: dict[str, int],
        replay_requests: dict[str, int],
    ) -> None:
        params = frame.get("params")
        native_session_id = (
            params.get("sessionId") if isinstance(params, dict) else None
        )
        if not isinstance(native_session_id, str) or not native_session_id:
            raise _AcpContractError("ACP session update has no sessionId")
        replay_id = replay_requests.get(native_session_id)
        replay = pending.get(replay_id) if replay_id is not None else None
        if isinstance(replay, _PendingReplayOpen):
            self._handle_replay_update(
                process,
                replay,
                params.get("update") if isinstance(params, dict) else None,
                pending,
                replay_requests,
            )
            return
        update = params.get("update") if isinstance(params, dict) else None
        if isinstance(update, dict) and update.get("sessionUpdate") == "config_option_update":
            try:
                snapshot = _parse_config_snapshot(
                    {"configOptions": update.get("configOptions")}
                )
            except _AcpContractError:
                if not self._publish(
                    _SessionMetadataFailedEvent(
                        native_session_id, "ACP configuration update is invalid"
                    )
                ):
                    raise _AcpContractError("ACP metadata failure delivery failed")
            else:
                for pending_request in pending.values():
                    if (
                        isinstance(pending_request, _PendingConfigSelection)
                        and pending_request.command.native_session_id == native_session_id
                    ):
                        option = snapshot.by_category(pending_request.command.category)
                        if (
                            option is not None
                            and option.config_id == pending_request.command.config_id
                            and option.current_value == pending_request.command.value
                        ):
                            pending_request.requested_effect_observed = True
                if not self._publish(_ConfigSnapshotEvent(native_session_id, snapshot)):
                    raise _AcpContractError("ACP config update delivery failed")
            return
        if isinstance(update, dict) and update.get("sessionUpdate") == "usage_update":
            try:
                usage = _parse_usage_update(update)
            except _AcpContractError:
                if not self._publish(
                    _SessionMetadataFailedEvent(
                        native_session_id, "ACP usage update is invalid"
                    )
                ):
                    raise _AcpContractError("ACP metadata failure delivery failed")
            else:
                if not self._publish(_UsageUpdatedEvent(native_session_id, usage)):
                    raise _AcpContractError("ACP usage update delivery failed")
            return
        prompt_id = prompt_requests.get(native_session_id)
        request = pending.get(prompt_id) if prompt_id is not None else None
        if not isinstance(request, _PendingPrompt):
            self._config.logger.event(
                "acp_stale_update_ignored",
                provider=self._config.runtime_profile.provider.value,
                profile=self._config.runtime_profile.profile.value,
            )
            return
        if not isinstance(update, dict):
            self._fail_prompt(
                request,
                "ACP session update is malformed",
                pending,
                prompt_requests,
            )
            return
        update_kind = update.get("sessionUpdate")
        if update_kind == "agent_message_chunk":
            content = update.get("content")
            text = content.get("text") if isinstance(content, dict) else None
            if (
                not isinstance(content, dict)
                or content.get("type") != "text"
                or not isinstance(text, str)
            ):
                self._fail_prompt(
                    request,
                    "ACP text update is malformed",
                    pending,
                    prompt_requests,
                )
                return
            if text:
                if not self._publish(
                    _TurnOutputEvent(native_session_id, request.turn_ref, text)
                ):
                    self._remove_prompt(
                        request,
                        pending,
                        prompt_requests,
                        release_admission=False,
                    )
            return
        if update_kind == "tool_call":
            tool_call_id = update.get("toolCallId")
            if (
                not isinstance(tool_call_id, str)
                or not tool_call_id
                or len(tool_call_id) > 256
                or tool_call_id in request.active_tool_call_ids
                or tool_call_id in request.completed_tool_call_ids
                or len(request.active_tool_call_ids)
                >= self._config.edge_queue_capacity
            ):
                self._fail_prompt(
                    request,
                    "ACP tool update correlation is invalid",
                    pending,
                    prompt_requests,
                )
                return
            request.active_tool_call_ids.add(tool_call_id)
            return
        if update_kind == "tool_call_update":
            tool_call_id = update.get("toolCallId")
            if (
                not isinstance(tool_call_id, str)
                or not tool_call_id
                or len(tool_call_id) > 256
            ):
                self._fail_prompt(
                    request,
                    "ACP tool update correlation is invalid",
                    pending,
                    prompt_requests,
                )
                return
            if (
                tool_call_id not in request.active_tool_call_ids
                and tool_call_id not in request.completed_tool_call_ids
            ):
                if request.completed_tool_call_ids.has_evicted:
                    self._config.logger.event(
                        "acp_evicted_tool_update_ignored",
                        provider=self._config.runtime_profile.provider.value,
                        profile=self._config.runtime_profile.profile.value,
                        runtime_generation=request.turn_ref.runtime_generation.value,
                        turn_ref_sequence=request.turn_ref.sequence,
                        tool_call_id=tool_call_id,
                    )
                    return
                self._fail_prompt(
                    request,
                    "ACP tool update correlation is invalid",
                    pending,
                    prompt_requests,
                )
                return
            if update.get("status") in {"completed", "failed"}:
                self._remember_completed_tool_call(request, tool_call_id)
            return
        if update_kind in {
            "user_message_chunk",
            "agent_thought_chunk",
            "plan",
            "available_commands_update",
            "current_mode_update",
            "session_info_update",
        }:
            return
        self._fail_prompt(
            request,
            "ACP session update is unsupported",
            pending,
            prompt_requests,
        )

    def _handle_replay_update(
        self,
        process: subprocess.Popen[bytes],
        request: _PendingReplayOpen,
        update: object,
        pending: dict[int, _PendingRequest],
        replay_requests: dict[str, int],
    ) -> None:
        request_id = replay_requests.get(request.native_session_id)
        if request_id is None or pending.get(request_id) is not request:
            return
        if not isinstance(update, dict):
            self._fail_replay_open(process, request, pending, replay_requests)
            return
        update_kind = update.get("sessionUpdate")
        allowed = {
            "user_message_chunk",
            "agent_message_chunk",
            "agent_thought_chunk",
            "tool_call",
            "tool_call_update",
            "plan",
            "available_commands_update",
            "current_mode_update",
            "config_option_update",
            "session_info_update",
            "usage_update",
        }
        if update_kind not in allowed:
            self._fail_replay_open(process, request, pending, replay_requests)
            return
        request.update_count += 1
        if request.update_count > self._config.edge_queue_capacity:
            self._fail_replay_open(process, request, pending, replay_requests)
            return
        if update_kind == "config_option_update":
            try:
                request.config_snapshot = _parse_config_snapshot(
                    {"configOptions": update.get("configOptions")}
                )
            except _AcpContractError:
                self._fail_replay_open(process, request, pending, replay_requests)
            return
        if update_kind == "usage_update":
            try:
                request.usage = _parse_usage_update(update)
            except _AcpContractError:
                self._fail_replay_open(process, request, pending, replay_requests)
            return
        if update_kind == "agent_message_chunk":
            content = update.get("content")
            text = content.get("text") if isinstance(content, dict) else None
            if (
                not isinstance(content, dict)
                or content.get("type") != "text"
                or not isinstance(text, str)
            ):
                self._fail_replay_open(process, request, pending, replay_requests)
                return
            request.replay_chars += len(text)
            if request.replay_chars > self._config.turn_output_max_chars:
                self._fail_replay_open(process, request, pending, replay_requests)
                return

    def _fail_replay_open(
        self,
        process: subprocess.Popen[bytes],
        request: _PendingReplayOpen,
        pending: dict[int, _PendingRequest],
        replay_requests: dict[str, int],
    ) -> None:
        request_id = replay_requests.pop(request.native_session_id, None)
        if request_id is not None:
            pending.pop(request_id, None)
        self._begin_failed_open_cleanup(
            process,
            pending,
            request.command,
            request.native_session_id,
            error="ACP session replay is invalid",
        )

    def _terminal_reader(self, terminal: _OwnedTerminal) -> None:
        stream = terminal.process.stdout
        if stream is None:
            return
        try:
            while True:
                chunk = stream.read(4096)
                if not chunk:
                    return
                terminal.append_output(chunk)
        except (OSError, ValueError):
            return

    def _start_owned_terminal(
        self,
        native_session_id: str,
        turn_ref: TurnRef,
        effect: _TerminalCreateEffect,
    ) -> str:
        if (
            len(self._owned_terminals) + len(self._released_terminal_owners)
            >= self._config.edge_queue_capacity
        ):
            raise _AcpContractError("ACP terminal capacity exceeded")
        environment = {
            key: value
            for key, value in dict(self._config.env or {}).items()
            if key in {"PATH", "TMPDIR", "LANG", "LC_ALL"}
        }
        environment.update(dict(effect.env))
        directory_fd, _ = _open_workspace_directory(effect.workspace, effect.cwd)
        launcher = Path(__file__).with_name("acp_terminal_launcher.py")
        try:
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-I",
                    "-S",
                    str(launcher),
                    str(directory_fd),
                    effect.command,
                    *effect.args,
                ],
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=0,
                start_new_session=True,
                pass_fds=(directory_fd,),
            )
        except (OSError, ValueError):
            raise _AcpContractError("ACP terminal process failed to start") from None
        finally:
            os.close(directory_fd)
        self._terminal_sequence += 1
        terminal_id = f"orc-terminal-{self._terminal_sequence}"
        terminal = _OwnedTerminal(
            terminal_id,
            native_session_id,
            turn_ref,
            process,
            effect.output_byte_limit,
        )
        reader = threading.Thread(
            target=self._terminal_reader,
            args=(terminal,),
            name=f"{self._config.edge_thread_name}-terminal-{self._terminal_sequence}",
            daemon=True,
        )
        terminal.reader = reader
        self._owned_terminals[terminal_id] = terminal
        reader.start()
        return terminal_id

    @staticmethod
    def _terminal_exit_status(terminal: _OwnedTerminal) -> dict[str, object] | None:
        return_code = terminal.process.poll()
        if return_code is None:
            return None
        if return_code < 0:
            try:
                signal_name: str | None = str(signal.Signals(-return_code).name)
            except ValueError:
                signal_name = "other"
            return {"exitCode": None, "signal": signal_name}
        return {"exitCode": return_code, "signal": None}

    def _release_terminal(self, terminal_id: str) -> bool:
        terminal = self._owned_terminals.get(terminal_id)
        if terminal is None:
            return False
        process = terminal.process
        process_group_id = process.pid
        try:
            os.killpg(process_group_id, signal.SIGTERM)
        except ProcessLookupError:
            pass
        except (PermissionError, OSError) as exc:
            raise _AcpConnectionError(
                "ACP owned terminal cleanup signal failed"
            ) from exc
        if process.poll() is None:
            try:
                process.wait(timeout=self._config.edge_stop_timeout_sec)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process_group_id, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except (PermissionError, OSError) as exc:
                    raise _AcpConnectionError(
                        "ACP owned terminal cleanup signal failed"
                    ) from exc
                try:
                    process.wait(timeout=self._config.edge_stop_timeout_sec)
                except subprocess.TimeoutExpired:
                    raise _AcpConnectionError(
                        "ACP owned terminal leader cleanup did not complete"
                    ) from None
        # The leader may have exited while descendants still own the process group.
        try:
            os.killpg(process_group_id, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except (PermissionError, OSError) as exc:
            raise _AcpConnectionError(
                "ACP owned terminal cleanup signal failed"
            ) from exc
        reader = terminal.reader
        if reader is not None:
            reader.join(timeout=self._config.edge_stop_timeout_sec)
            if reader.is_alive():
                raise _AcpConnectionError(
                    "ACP owned terminal reader cleanup did not complete"
                )
        if process.stdout is not None and not process.stdout.closed:
            process.stdout.close()
        self._owned_terminals.pop(terminal_id, None)
        self._released_terminal_owners[terminal_id] = (
            terminal.native_session_id,
            terminal.turn_ref,
        )
        return True

    def _release_owned_terminals(
        self,
        *,
        native_session_id: str,
        turn_ref: TurnRef | None = None,
        clear_released: bool = True,
    ) -> None:
        for terminal_id, terminal in tuple(self._owned_terminals.items()):
            if terminal.native_session_id == native_session_id and (
                turn_ref is None or terminal.turn_ref == turn_ref
            ):
                self._release_terminal(terminal_id)
        if clear_released:
            for terminal_id, owner in tuple(self._released_terminal_owners.items()):
                if owner[0] == native_session_id and (
                    turn_ref is None or owner[1] == turn_ref
                ):
                    self._released_terminal_owners.pop(terminal_id, None)

    def _release_all_terminals(self) -> None:
        for terminal_id in tuple(self._owned_terminals):
            self._release_terminal(terminal_id)
        self._released_terminal_owners.clear()

    def _service_terminal_waiters(
        self,
        process: subprocess.Popen[bytes],
        callbacks: _PendingCallbackRegistry,
    ) -> None:
        for callback in callbacks.pending_all():
            if not isinstance(callback, _PendingTerminalWaitCallback):
                continue
            terminal = self._owned_terminals.get(callback.terminal_id)
            status = self._terminal_exit_status(terminal) if terminal is not None else None
            if status is None:
                continue
            self._write_frame(
                process,
                {"jsonrpc": "2.0", "id": callback.request_id, "result": status},
            )
            callbacks.pop_request_id(callback.request_id)

    def _handle_terminal_operation(
        self,
        process: subprocess.Popen[bytes],
        frame: dict[str, object],
        request: _PendingPrompt,
        callbacks: _PendingCallbackRegistry,
    ) -> None:
        params = frame.get("params")
        method = frame.get("method")
        if (
            not isinstance(params, dict)
            or set(params) != {"sessionId", "terminalId"}
            or params.get("sessionId") != request.native_session_id
            or not isinstance(params.get("terminalId"), str)
        ):
            raise _AcpContractError("ACP terminal operation params are invalid")
        terminal_id = params["terminalId"]
        terminal = self._owned_terminals.get(terminal_id)
        released_owner = self._released_terminal_owners.get(terminal_id)
        if terminal is None and method == "terminal/release" and released_owner == (
            request.native_session_id,
            request.turn_ref,
        ):
            callback = _PendingTerminalWaitCallback(
                int(frame["id"]),
                request.native_session_id,
                request.turn_ref,
                terminal_id,
            )
            if callbacks.register(callback) != "registered":
                raise _AcpContractError("ACP terminal callback state is invalid")
            self._write_frame(
                process,
                {"jsonrpc": "2.0", "id": callback.request_id, "result": {}},
            )
            callbacks.pop_request_id(callback.request_id)
            return
        if (
            terminal is None
            or terminal.native_session_id != request.native_session_id
            or terminal.turn_ref != request.turn_ref
        ):
            raise _AcpContractError("ACP terminal ownership is invalid")
        callback = _PendingTerminalWaitCallback(
            int(frame["id"]),
            request.native_session_id,
            request.turn_ref,
            terminal_id,
        )
        if callbacks.register(callback) != "registered":
            raise _AcpContractError("ACP terminal callback state is invalid")
        if method == "terminal/wait_for_exit":
            return
        if method == "terminal/output":
            output, truncated = terminal.snapshot()
            result: dict[str, object] = {
                "output": output,
                "truncated": truncated,
            }
            status = self._terminal_exit_status(terminal)
            if status is not None:
                result["exitStatus"] = status
        elif method == "terminal/kill":
            if terminal.process.poll() is None:
                try:
                    os.killpg(terminal.process.pid, signal.SIGTERM)
                except (ProcessLookupError, PermissionError, OSError):
                    pass
            result = {}
        elif method == "terminal/release":
            self._release_terminal(terminal_id)
            result = {}
        else:
            callbacks.pop_request_id(callback.request_id)
            raise _AcpContractError("ACP terminal method is unsupported")
        self._write_frame(
            process,
            {"jsonrpc": "2.0", "id": callback.request_id, "result": result},
        )
        callbacks.pop_request_id(callback.request_id)

    def _handle_callback(
        self,
        process: subprocess.Popen[bytes],
        frame: dict[str, object],
        pending: dict[int, _PendingRequest],
        prompt_requests: dict[str, int],
        replay_requests: dict[str, int],
        callbacks: _PendingCallbackRegistry,
    ) -> None:
        params = frame.get("params")
        native_session_id = (
            params.get("sessionId") if isinstance(params, dict) else None
        )
        prompt_id = (
            prompt_requests.get(native_session_id)
            if isinstance(native_session_id, str)
            else None
        )
        request = pending.get(prompt_id) if prompt_id is not None else None
        method = frame.get("method")
        if (
            isinstance(request, _PendingPrompt)
            and request.cancel_requested
            and method in {
                "session/request_permission",
                "elicitation/create",
                "fs/read_text_file",
                "fs/write_text_file",
                "terminal/create",
                "terminal/output",
                "terminal/wait_for_exit",
                "terminal/kill",
                "terminal/release",
            }
        ):
            self._write_frame(
                process,
                {
                    "jsonrpc": "2.0",
                    "id": frame.get("id"),
                    "error": {"code": -32800, "message": "request cancelled"},
                },
            )
            return
        if not isinstance(request, _PendingPrompt):
            self._write_callback_error(
                process,
                frame.get("id"),
                -32800,
                "request cancelled",
            )
            return
        duplicate = callbacks.duplicate_status(
            int(frame["id"]),
            native_session_id=request.native_session_id,
            turn_ref=request.turn_ref,
        )
        if duplicate is not None:
            self._handle_callback_registration_failure(
                process,
                callbacks,
                int(frame["id"]),
                duplicate,
                request,
                pending,
                prompt_requests,
            )
            return
        try:
            if method == "session/request_permission":
                normalized = _normalize_permission_request(
                    params,
                    turn_ref=request.turn_ref,
                )
                if normalized.tool_call_id in request.completed_tool_call_ids or (
                    request.completed_tool_call_ids.has_evicted
                    and normalized.tool_call_id not in request.active_tool_call_ids
                ):
                    self._write_callback_error(
                        process,
                        frame.get("id"),
                        -32800,
                        "approval already resolved",
                    )
                    self._config.logger.event(
                        "acp_completed_permission_duplicate_ignored",
                        provider=self._config.runtime_profile.provider.value,
                        profile=self._config.runtime_profile.profile.value,
                        runtime_generation=request.turn_ref.runtime_generation.value,
                        turn_ref_sequence=request.turn_ref.sequence,
                        history_evicted=request.completed_tool_call_ids.has_evicted,
                    )
                    return
                if normalized.tool_call_id not in request.active_tool_call_ids:
                    raise _AcpContractError(
                        "ACP permission tool correlation is invalid"
                    )
                callback = _PendingPermissionCallback(
                    request_id=int(frame["id"]),
                    native_session_id=request.native_session_id,
                    turn_ref=request.turn_ref,
                    approval_ref=normalized.approval_ref,
                    tool_call_id=normalized.tool_call_id,
                    option_ids=normalized.option_ids,
                )
                registration = callbacks.register(callback)
                if registration != "registered":
                    self._handle_callback_registration_failure(
                        process,
                        callbacks,
                        callback.request_id,
                        registration,
                        request,
                        pending,
                        prompt_requests,
                    )
                    return
                if not normalized.review.reviewable:
                    self._settle_unreviewable_permission(
                        process,
                        callbacks,
                        callback,
                        normalized.review,
                    )
                    self._remember_completed_tool_call(
                        request,
                        callback.tool_call_id,
                    )
                    return
                if normalized.request is None:
                    raise AssertionError("reviewable ACP permission has no request")
                if self._publish(
                    _ApprovalRequestedEvent(
                        request.native_session_id,
                        request.turn_ref,
                        normalized.request,
                    )
                ):
                    return
                self._write_callback_error(
                    process,
                    callback.request_id,
                    -32602,
                    "invalid callback params",
                )
                callbacks.pop_request_id(callback.request_id)
                self._fail_prompt(
                    request,
                    "ACP provider callback is invalid",
                    pending,
                    prompt_requests,
                )
                return
            if (
                method == "elicitation/create"
                and AgentCapability.FORM_ELICITATION
                in self._config.runtime_profile.supported_capabilities
            ):
                normalized_form = _normalize_form_request(
                    params,
                    turn_ref=request.turn_ref,
                )
                callback = _PendingFormCallback(
                    request_id=int(frame["id"]),
                    native_session_id=request.native_session_id,
                    turn_ref=request.turn_ref,
                    elicitation_ref=normalized_form.request.elicitation_ref,
                    request=normalized_form.request,
                )
                registration = callbacks.register(callback)
                if registration != "registered":
                    self._handle_callback_registration_failure(
                        process,
                        callbacks,
                        callback.request_id,
                        registration,
                        request,
                        pending,
                        prompt_requests,
                    )
                    return
                if self._publish(
                    _ElicitationRequestedEvent(
                        request.native_session_id,
                        request.turn_ref,
                        normalized_form.request,
                    )
                ):
                    return
                self._write_callback_error(
                    process,
                    callback.request_id,
                    -32602,
                    "invalid callback params",
                )
                callbacks.pop_request_id(callback.request_id)
                self._fail_prompt(
                    request,
                    "ACP provider callback is invalid",
                    pending,
                    prompt_requests,
                )
                return
            service_capability = {
                "fs/read_text_file": AgentCapability.CLIENT_FILESYSTEM_READ,
                "fs/write_text_file": AgentCapability.CLIENT_FILESYSTEM_WRITE,
                "terminal/create": AgentCapability.CLIENT_TERMINAL,
            }.get(str(method))
            if (
                service_capability is not None
                and service_capability
                in self._config.runtime_profile.supported_capabilities
            ):
                service_params = dict(params) if isinstance(params, dict) else {}
                service_params["__request_id"] = int(frame["id"])
                callback = _normalize_client_service_request(
                    str(method),
                    service_params,
                    native_session_id=request.native_session_id,
                    turn_ref=request.turn_ref,
                    workspace=request.workspace,
                )
                registration = callbacks.register(callback)
                if registration != "registered":
                    self._handle_callback_registration_failure(
                        process,
                        callbacks,
                        callback.request_id,
                        registration,
                        request,
                        pending,
                        prompt_requests,
                    )
                    return
                if self._publish(
                    _ApprovalRequestedEvent(
                        request.native_session_id,
                        request.turn_ref,
                        callback.request,
                    )
                ):
                    return
                self._write_callback_error(
                    process,
                    callback.request_id,
                    -32602,
                    "invalid callback params",
                )
                callbacks.pop_request_id(callback.request_id)
                self._fail_prompt(
                    request,
                    "ACP provider callback is invalid",
                    pending,
                    prompt_requests,
                )
                return
            if (
                method
                in {
                    "terminal/output",
                    "terminal/wait_for_exit",
                    "terminal/kill",
                    "terminal/release",
                }
                and AgentCapability.CLIENT_TERMINAL
                in self._config.runtime_profile.supported_capabilities
            ):
                self._handle_terminal_operation(
                    process,
                    frame,
                    request,
                    callbacks,
                )
                return
        except _AcpContractError:
            self._write_callback_error(
                process,
                frame.get("id"),
                -32602,
                "invalid callback params",
            )
            self._fail_prompt(
                request,
                "ACP provider callback is invalid",
                pending,
                prompt_requests,
            )
            return
        self._write_frame(
            process,
            {
                "jsonrpc": "2.0",
                "id": frame.get("id"),
                "error": {"code": -32601, "message": "unsupported client callback"},
            },
        )
        if isinstance(request, _PendingPrompt):
            self._fail_prompt(
                request,
                "ACP provider callback is unsupported",
                pending,
                prompt_requests,
            )
            return
        open_requests = [
            (request_id, item)
            for request_id, item in pending.items()
            if isinstance(
                item,
                (_PendingOpen, _PendingReplayOpen, _PendingModelSelection),
            )
        ]
        if len(open_requests) == 1:
            request_id, open_request = open_requests[0]
            pending.pop(request_id, None)
            if isinstance(open_request, _PendingReplayOpen):
                replay_requests.pop(open_request.native_session_id, None)
            if isinstance(open_request, _PendingModelSelection):
                self._begin_failed_open_cleanup(
                    process,
                    pending,
                    open_request.command,
                    open_request.native_session_id,
                    error="ACP startup model selection was interrupted",
                )
                return
            self._publish_open_failure(
                open_request.command.correlation_id,
                release_admission=True,
            )

    def _handle_callback_registration_failure(
        self,
        process: subprocess.Popen[bytes],
        callbacks: _PendingCallbackRegistry,
        request_id: int,
        registration: str,
        request: _PendingPrompt,
        pending: dict[int, _PendingRequest],
        prompt_requests: dict[str, int],
    ) -> None:
        self._write_callback_error(
            process,
            request_id,
            -32602,
            "invalid callback state",
        )
        if registration in {"duplicate_same_owner", "duplicate_cross_owner"}:
            callbacks.pop_request_id(request_id)
        if registration == "duplicate_cross_owner":
            raise _AcpConnectionError("ACP callback request ID is ambiguous")
        if registration == "owner_conflict":
            self._cancel_callbacks(
                process,
                callbacks,
                native_session_id=request.native_session_id,
                turn_ref=request.turn_ref,
            )
        self._fail_prompt(
            request,
            "ACP callback responder ownership conflict",
            pending,
            prompt_requests,
        )

    def _settle_unreviewable_permission(
        self,
        process: subprocess.Popen[bytes],
        callbacks: _PendingCallbackRegistry,
        callback: _PendingPermissionCallback,
        review: _PermissionReview,
    ) -> None:
        option_id = callback.option_ids.get("decline")
        outcome: dict[str, object] = (
            {"outcome": "selected", "optionId": option_id}
            if option_id is not None
            else {"outcome": "cancelled"}
        )
        try:
            self._write_frame(
                process,
                {
                    "jsonrpc": "2.0",
                    "id": callback.request_id,
                    "result": {"outcome": outcome},
                },
            )
        except _AcpContractError as exc:
            # A callback response is never retried after the write handoff starts.
            raise _AcpConnectionError(
                "ACP permission review settlement is uncertain"
            ) from exc
        callbacks.pop_request_id(callback.request_id)
        self._config.logger.event(
            "acp_permission_review_auto_declined",
            category="effect_payload_too_large",
            observed_size=review.observed_size,
            limit=review.limit,
            runtime_generation=callback.turn_ref.runtime_generation.value,
            turn_ref_sequence=callback.turn_ref.sequence,
        )
        self._publish(
            _ProviderNoticeEvent(
                callback.native_session_id,
                callback.turn_ref,
                _APPROVAL_REVIEW_DECLINE_NOTICE,
            )
        )

    def _resolve_permission_callback(
        self,
        process: subprocess.Popen[bytes],
        command: _ResolvePermissionCommand,
        callbacks: _PendingCallbackRegistry,
        pending: dict[int, _PendingRequest],
        prompt_requests: dict[str, int],
    ) -> None:
        callback = callbacks.match_permission(command)
        if callback is None:
            command.handoff.complete_stale()
            return
        if isinstance(callback, _PendingClientServiceCallback):
            if command.decision not in {"accept", "decline"}:
                command.handoff.complete_stale()
                return
            if not command.handoff.begin_write():
                return
            try:
                if command.decision == "decline":
                    response: dict[str, object] = {
                        "jsonrpc": "2.0",
                        "id": callback.request_id,
                        "error": {"code": -32800, "message": "request cancelled"},
                    }
                else:
                    try:
                        if isinstance(callback.effect, _FilesystemReadEffect):
                            result: dict[str, object] = {
                                "content": _read_workspace_text(
                                    callback.effect.workspace,
                                    callback.effect.path,
                                    line=callback.effect.line,
                                    limit=callback.effect.limit,
                                )
                            }
                        elif isinstance(callback.effect, _FilesystemWriteEffect):
                            _write_workspace_text(
                                callback.effect.workspace,
                                callback.effect.path,
                                callback.effect.content,
                            )
                            result = {}
                        else:
                            result = {
                                "terminalId": self._start_owned_terminal(
                                    callback.native_session_id,
                                    callback.turn_ref,
                                    callback.effect,
                                )
                            }
                        response = {
                            "jsonrpc": "2.0",
                            "id": callback.request_id,
                            "result": result,
                        }
                    except _AcpContractError:
                        response = {
                            "jsonrpc": "2.0",
                            "id": callback.request_id,
                            "error": {
                                "code": -32603,
                                "message": "client service failed",
                            },
                        }
                self._write_frame(process, response)
            except BaseException:
                command.handoff.complete_uncertain()
                raise
            callbacks.pop_request_id(callback.request_id)
            command.handoff.complete_written()
            return
        option_id = callback.option_ids.get(command.decision)
        if option_id is None:
            command.handoff.complete_stale()
            return
        if not command.handoff.begin_write():
            return
        try:
            self._write_frame(
                process,
                {
                    "jsonrpc": "2.0",
                    "id": callback.request_id,
                    "result": {
                        "outcome": {
                            "outcome": "selected",
                            "optionId": option_id,
                        }
                    },
                },
            )
        except BaseException:
            command.handoff.complete_uncertain()
            raise
        callbacks.pop_request_id(callback.request_id)
        prompt_request_id = prompt_requests.get(callback.native_session_id)
        prompt = (
            pending.get(prompt_request_id)
            if prompt_request_id is not None
            else None
        )
        if isinstance(prompt, _PendingPrompt) and prompt.turn_ref == callback.turn_ref:
            self._remember_completed_tool_call(prompt, callback.tool_call_id)
        command.handoff.complete_written()

    def _remember_completed_tool_call(
        self,
        request: _PendingPrompt,
        tool_call_id: str,
    ) -> None:
        request.active_tool_call_ids.discard(tool_call_id)
        evicted = request.completed_tool_call_ids.remember(tool_call_id)
        if evicted is not None:
            self._config.logger.event(
                "acp_tool_call_dedup_history_evicted",
                provider=self._config.runtime_profile.provider.value,
                profile=self._config.runtime_profile.profile.value,
                runtime_generation=request.turn_ref.runtime_generation.value,
                turn_ref_sequence=request.turn_ref.sequence,
                evicted_tool_call_id=evicted,
            )

    def _resolve_form_callback(
        self,
        process: subprocess.Popen[bytes],
        command: _ResolveFormCommand,
        callbacks: _PendingCallbackRegistry,
    ) -> None:
        callback = callbacks.match_form(command)
        if callback is None:
            command.handoff.complete_stale()
            return
        if command.action is ElicitationAction.ACCEPT:
            try:
                content = dict(callback.request.validate_content(dict(command.content)))
            except ValueError:
                command.handoff.complete_stale()
                return
            result: dict[str, object] = {"action": "accept", "content": content}
        else:
            result = {"action": command.action.value}
        if not command.handoff.begin_write():
            return
        try:
            self._write_frame(
                process,
                {"jsonrpc": "2.0", "id": callback.request_id, "result": result},
            )
        except BaseException:
            command.handoff.complete_uncertain()
            raise
        callbacks.pop_request_id(callback.request_id)
        command.handoff.complete_written()

    def _cancel_callbacks(
        self,
        process: subprocess.Popen[bytes],
        callbacks: _PendingCallbackRegistry,
        *,
        native_session_id: str,
        turn_ref: TurnRef | None,
    ) -> None:
        owned = callbacks.pending_for_owner(
            native_session_id=native_session_id,
            turn_ref=turn_ref,
        )
        for callback in owned:
            if isinstance(callback, _PendingPermissionCallback):
                frame: dict[str, object] = {
                    "jsonrpc": "2.0",
                    "id": callback.request_id,
                    "result": {"outcome": {"outcome": "cancelled"}},
                }
            elif isinstance(callback, _PendingFormCallback):
                frame = {
                    "jsonrpc": "2.0",
                    "id": callback.request_id,
                    "result": {"action": "cancel"},
                }
            else:
                frame = {
                    "jsonrpc": "2.0",
                    "id": callback.request_id,
                    "error": {"code": -32800, "message": "request cancelled"},
                }
            self._write_frame(process, frame)
            callbacks.pop_request_id(callback.request_id)

    def _cancel_all_callbacks(
        self,
        process: subprocess.Popen[bytes],
        callbacks: _PendingCallbackRegistry,
    ) -> None:
        for callback in callbacks.pending_all():
            if isinstance(callback, _PendingPermissionCallback):
                frame: dict[str, object] = {
                    "jsonrpc": "2.0",
                    "id": callback.request_id,
                    "result": {"outcome": {"outcome": "cancelled"}},
                }
            elif isinstance(callback, _PendingFormCallback):
                frame = {
                    "jsonrpc": "2.0",
                    "id": callback.request_id,
                    "result": {"action": "cancel"},
                }
            else:
                frame = {
                    "jsonrpc": "2.0",
                    "id": callback.request_id,
                    "error": {"code": -32800, "message": "request cancelled"},
                }
            self._write_frame(process, frame)
            callbacks.pop_request_id(callback.request_id)

    def _write_callback_error(
        self,
        process: subprocess.Popen[bytes],
        request_id: object,
        code: int,
        message: str,
    ) -> None:
        self._write_frame(
            process,
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": code, "message": message},
            },
        )

    def _fail_prompt(
        self,
        request: _PendingPrompt,
        error: str,
        pending: dict[int, _PendingRequest],
        prompt_requests: dict[str, int],
    ) -> None:
        if not self._remove_prompt(
            request,
            pending,
            prompt_requests,
            release_admission=False,
        ):
            return
        self._publish_prompt_terminal(
            _TurnFailedEvent(
                request.native_session_id,
                request.turn_ref,
                error,
                fatal=True,
            )
        )

    def _remove_prompt(
        self,
        request: _PendingPrompt,
        pending: dict[int, _PendingRequest],
        prompt_requests: dict[str, int],
        *,
        release_admission: bool = True,
    ) -> bool:
        request_id = prompt_requests.get(request.native_session_id)
        if request_id is None or pending.get(request_id) is not request:
            return False
        prompt_requests.pop(request.native_session_id, None)
        pending.pop(request_id, None)
        self._release_owned_terminals(
            native_session_id=request.native_session_id,
            turn_ref=request.turn_ref,
            clear_released=True,
        )
        if release_admission:
            self._release_admission()
        return True

    def _publish_prompt_terminal(
        self,
        event: _TurnCompletedEvent | _TurnInterruptedEvent | _TurnFailedEvent,
    ) -> None:
        if self._publish(event):
            self._release_admission()

    def _log_prompt_failure(self, diagnostic: _AcpFailureDiagnostic) -> None:
        self._config.logger.event(
            "acp_prompt_failed",
            provider=self._config.runtime_profile.provider.value,
            profile=self._config.runtime_profile.profile.value,
            error_code=diagnostic.error_code,
            error_category=diagnostic.error_category,
            service=diagnostic.service,
            error_name=diagnostic.error_name,
        )

    def _expire_requests(
        self,
        process: subprocess.Popen[bytes],
        pending: dict[int, _PendingRequest],
        replay_requests: dict[str, int],
        cleanup_retries: dict[OpenSessionCorrelationId, _PendingOpenCleanup],
    ) -> None:
        now = time.monotonic()
        expired = [
            request_id
            for request_id, request in pending.items()
            if not isinstance(request, _PendingPrompt) and request.deadline <= now
        ]
        for request_id in expired:
            request = pending.pop(request_id)
            if isinstance(request, _PendingOpen):
                self._publish_open_failure(
                    request.command.correlation_id,
                    release_admission=True,
                )
            elif isinstance(request, _PendingReplayOpen):
                replay_requests.pop(request.native_session_id, None)
                self._begin_failed_open_cleanup(
                    process,
                    pending,
                    request.command,
                    request.native_session_id,
                    error="ACP session replay timed out",
                )
            elif isinstance(request, _PendingModelSelection):
                self._begin_failed_open_cleanup(
                    process,
                    pending,
                    request.command,
                    request.native_session_id,
                    error="ACP startup model selection timed out",
                )
            elif isinstance(request, _PendingList):
                self._complete_catalog(
                    request.command,
                    _CatalogFailed(
                        ProviderSessionCatalogErrorKind.PROVIDER_FAILED,
                        "ACP session catalog timed out",
                    ),
                )
            elif isinstance(request, _PendingOpenCleanup):
                self._defer_open_cleanup_retry(
                    request,
                    cleanup_retries,
                )
            elif isinstance(request, _PendingConfigSelection):
                if not self._publish(
                    _ConfigSelectionUncertainEvent(
                        request.command.native_session_id,
                        request.command.category,
                        request.command.value,
                        "ACP configuration selection outcome is uncertain",
                    )
                ):
                    raise _AcpContractError(
                        "ACP configuration timeout delivery failed"
                    )
                self._release_admission()
            else:
                self._publish_lifecycle_terminal(
                    _CloseFailed(
                        request.native_session_id,
                        "ACP session cleanup failed",
                    ),
                    release_admission=True,
                )

    def _initialize(self, process: subprocess.Popen[bytes]) -> None:
        client_capabilities: dict[str, object] = {
            "auth": {"terminal": False},
        }
        if (
            AgentCapability.FORM_ELICITATION
            in self._config.runtime_profile.supported_capabilities
        ):
            client_capabilities["elicitation"] = {"form": {}}
        declared = self._config.runtime_profile.supported_capabilities
        if (
            AgentCapability.CLIENT_FILESYSTEM_READ in declared
            or AgentCapability.CLIENT_FILESYSTEM_WRITE in declared
        ):
            client_capabilities["fs"] = {
                "readTextFile": AgentCapability.CLIENT_FILESYSTEM_READ in declared,
                "writeTextFile": AgentCapability.CLIENT_FILESYSTEM_WRITE in declared,
            }
        if AgentCapability.CLIENT_TERMINAL in declared:
            client_capabilities["terminal"] = True
        result = self._request(
            process,
            "initialize",
            {
                "protocolVersion": self._config.runtime_profile.protocol_version,
                "clientCapabilities": client_capabilities,
                "clientInfo": {"name": "orc", "version": "1"},
            },
            timeout_sec=self._config.rpc_timeout_sec,
        )
        if result.get("protocolVersion") != self._config.runtime_profile.protocol_version:
            raise ProviderStartupFailure(
                ProviderStartupFailureCategory.VERSION_MISMATCH,
                "ACP protocol version mismatch",
            )
        if "agentInfo" not in result:
            self._runtime_version = UNKNOWN_COMPONENT_VERSION
        else:
            agent_info = result["agentInfo"]
            if not isinstance(agent_info, dict):
                raise _AcpContractError("ACP agent info is invalid")
            if "name" not in agent_info or "version" not in agent_info:
                raise _AcpContractError("ACP agent info is incomplete")
            try:
                _normalize_agent_info_text(
                    agent_info["name"],
                    field_name="ACP agent name",
                )
                self._runtime_version = normalize_observed_runtime_version(
                    agent_info["version"],
                    field_name="ACP agent version",
                )
                if "title" in agent_info:
                    _normalize_agent_info_text(
                        agent_info["title"],
                        field_name="ACP agent title",
                    )
            except ValueError as exc:
                raise _AcpContractError("ACP agent info is invalid") from exc
        if not isinstance(result.get("agentCapabilities"), dict):
            raise _AcpContractError("ACP capabilities are missing")
        agent_capabilities = result["agentCapabilities"]
        prompt_capabilities = agent_capabilities.get("promptCapabilities", {})
        if not isinstance(prompt_capabilities, dict):
            raise _AcpContractError("ACP prompt capabilities are invalid")
        negotiated = _NegotiatedPromptCapabilities(
            image=prompt_capabilities.get("image", False) is True,
            audio=prompt_capabilities.get("audio", False) is True,
            embedded_context=(
                prompt_capabilities.get("embeddedContext", False) is True
            ),
        )
        if AgentCapability.STRUCTURED_USER_INPUT in declared and not (
            negotiated.image and negotiated.embedded_context
        ):
            raise _AcpContractError("ACP declared prompt capability was not negotiated")
        self._negotiated_prompt_capabilities = negotiated
        auth_methods = result.get("authMethods")
        if not isinstance(auth_methods, list):
            raise _AcpContractError("ACP auth methods are invalid")
        auth_ids = tuple(
            item.get("id")
            for item in auth_methods
            if isinstance(item, dict) and isinstance(item.get("id"), str)
        )
        if len(auth_ids) != len(auth_methods) or set(auth_ids) != set(
            self._config.runtime_profile.auth_method_ids
        ):
            raise _AcpContractError("ACP auth method is unsupported")

    def _request(
        self,
        process: subprocess.Popen[bytes],
        method: str,
        params: dict[str, object],
        *,
        timeout_sec: float,
    ) -> dict[str, object]:
        request_id = self._next_request_id()
        self._write_frame(
            process,
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
        )
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            frame = self._read_frame(process, deadline)
            classified = _classify_inbound_frame(frame)
            if classified.kind == "response":
                if classified.request_id != request_id:
                    raise _AcpContractError("ACP response correlation failed")
                if "error" in frame:
                    raise _AcpContractError(f"ACP {method} was rejected")
                result = frame.get("result")
                if not isinstance(result, dict):
                    raise _AcpContractError(f"ACP {method} result must be an object")
                return result
            if classified.kind == "request":
                assert classified.request_id is not None
                self._write_frame(
                    process,
                    {
                        "jsonrpc": "2.0",
                        "id": classified.request_id,
                        "error": {"code": -32601, "message": "unsupported client callback"},
                    },
                )
                raise _AcpContractError("ACP requested an unsupported callback")
            if classified.kind == "notification":
                if classified.method == "session/update" and not isinstance(
                    frame.get("params"), dict
                ):
                    raise _AcpContractError("ACP session update params are invalid")
                continue
            raise AssertionError(
                f"unhandled ACP frame classification: {classified.kind}"
            )
        raise _AcpContractError(f"ACP {method} timed out")

    def _next_request_id(self) -> int:
        self._request_sequence += 1
        return self._request_sequence

    def _write_frame(
        self,
        process: subprocess.Popen[bytes],
        frame: dict[str, object],
    ) -> None:
        stream = process.stdin
        if stream is None:
            raise _AcpContractError("ACP stdin is unavailable")
        encoded = json.dumps(frame, separators=(",", ":")).encode("utf-8") + b"\n"
        if len(encoded) > self._config.runtime_profile.frame_max_bytes:
            raise _AcpContractError("ACP outbound frame exceeded capacity")
        try:
            stream.write(encoded)
            stream.flush()
        except (BrokenPipeError, OSError, ValueError):
            raise _AcpContractError("ACP write failed") from None

    def _read_frame(
        self,
        process: subprocess.Popen[bytes],
        deadline: float,
    ) -> dict[str, object]:
        stream = process.stdout
        if stream is None:
            raise _AcpContractError("ACP stdout is unavailable")
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise _AcpContractError("ACP process exited")
            selector = selectors.DefaultSelector()
            try:
                selector.register(stream, selectors.EVENT_READ)
                events = selector.select(min(0.1, max(0.0, deadline - time.monotonic())))
            finally:
                selector.close()
            if not events:
                continue
            frame_max_bytes = self._config.runtime_profile.frame_max_bytes
            raw = stream.readline(frame_max_bytes + 1)
            if not raw:
                raise _AcpContractError("ACP stdout closed")
            if len(raw) > frame_max_bytes or not raw.endswith(b"\n"):
                raise _AcpContractError("ACP frame exceeded capacity")
            try:
                frame = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError):
                raise _AcpContractError("ACP frame is malformed") from None
            if not isinstance(frame, dict):
                raise _AcpContractError("ACP frame must be an object")
            return frame
        raise _AcpContractError("ACP response timed out")

    def _read_frame_if_available(
        self,
        process: subprocess.Popen[bytes],
        *,
        timeout_sec: float,
    ) -> dict[str, object] | None:
        stream = process.stdout
        if stream is None:
            raise _AcpContractError("ACP stdout is unavailable")
        if process.poll() is not None:
            raise _AcpContractError("ACP process exited")
        selector = selectors.DefaultSelector()
        try:
            selector.register(stream, selectors.EVENT_READ)
            if not selector.select(timeout_sec):
                return None
        finally:
            selector.close()
        return self._read_frame(process, time.monotonic() + timeout_sec)

    def _drain_stderr(self, process: subprocess.Popen[bytes]) -> None:
        stream = process.stderr
        if stream is None:
            return
        try:
            while True:
                chunk = stream.read(4096)
                if not chunk:
                    return
        except (OSError, ValueError):
            return

    def _is_cancelled(self, correlation_id: OpenSessionCorrelationId) -> bool:
        with self._cancel_lock:
            return correlation_id in self._cancelled

    def _publish(self, event: _EdgeEvent) -> bool:
        if isinstance(event, _HostFailed):
            with self._host_failure_lock:
                self._host_failure = event
            self._mark_progress()
            return True
        try:
            self._events.put_nowait(event)
        except Full:
            if isinstance(
                event,
                (
                    _TurnOutputEvent,
                    _TurnCompletedEvent,
                    _TurnInterruptedEvent,
                    _TurnFailedEvent,
                    _ApprovalRequestedEvent,
                    _ProviderNoticeEvent,
                    _ElicitationRequestedEvent,
                ),
            ):
                key = (event.native_session_id, event.turn_ref)
                with self._turn_overflow_lock:
                    self._turn_overflows.setdefault(
                        key,
                        _TurnFailedEvent(
                            event.native_session_id,
                            event.turn_ref,
                            "ACP session event delivery capacity exceeded",
                            fatal=True,
                        ),
                    )
            else:
                self._terminal_error = "ACP edge event queue capacity exceeded"
            self._mark_progress()
            return False
        self._mark_progress()
        return True

    def _publish_lifecycle_terminal(
        self,
        event: _LifecycleTerminalEvent,
        *,
        release_admission: bool,
    ) -> None:
        try:
            self._events.put_nowait(event)
        except Full:
            with self._lifecycle_terminal_lock:
                if len(self._lifecycle_terminals) >= self._config.edge_queue_capacity + 1:
                    raise _AcpContractError(
                        "ACP lifecycle handoff capacity exceeded"
                    )
                self._lifecycle_terminals.append(
                    _LifecycleTerminalHandoff(event, release_admission)
                )
        else:
            if release_admission:
                self._release_admission()
        self._mark_progress()

    def _log_process_stopped(self) -> None:
        if not self._process_started_logged or self._process_stopped_logged:
            return
        process = self._process
        self._config.logger.event(
            "process_stopped",
            provider=self._config.runtime_profile.provider.value,
            profile=self._config.runtime_profile.profile.value,
            role=self._config.role,
            pid=process.pid if process is not None else None,
            ownership_scope="direct_process",
        )
        self._process_stopped_logged = True

    def _mark_progress(self) -> None:
        with self._progress_lock:
            self._progress = True

    def _stop_process(self) -> None:
        process = self._process
        if process is None:
            return
        if process.stdin is not None and not process.stdin.closed:
            process.stdin.close()
        if process.poll() is None:
            try:
                process.wait(timeout=self._config.edge_stop_timeout_sec)
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    process.wait(timeout=self._config.edge_stop_timeout_sec)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=self._config.edge_stop_timeout_sec)
        for stream in (process.stdout, process.stderr):
            if stream is not None and not stream.closed:
                stream.close()
        stderr_thread = self._stderr_thread
        if stderr_thread is not None:
            stderr_thread.join(timeout=self._config.edge_stop_timeout_sec)
            if stderr_thread.is_alive():
                raise RuntimeError("ACP stderr reader did not stop")
        self._stderr_thread = None
