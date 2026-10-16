"""Claude Agent SDK provider host with dedicated process/session ownership."""

from __future__ import annotations

import asyncio
import base64
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from io import StringIO
import json
import os
from pathlib import Path
from queue import Empty, Full, Queue
import re
import subprocess
import threading
import time
from types import MappingProxyType
from typing import Any, Callable, Mapping
from uuid import uuid4

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    ProcessError,
    PermissionResultAllow,
    PermissionResultDeny,
    ResultMessage,
    SDKSessionInfo,
    SystemMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
    __version__ as CLAUDE_AGENT_SDK_VERSION,
)
from claude_agent_sdk.types import ToolPermissionContext
from claude_agent_sdk._internal.sessions import (
    _parse_session_info_from_lite,
    _read_session_lite,
    _read_sessions_from_dir,
    _validate_uuid,
    project_key_for_directory,
)

from orchestrator import clock
from orchestrator.agent_capabilities import (
    AgentCapability,
    AgentCapabilitySnapshot,
    claude_capability_snapshot,
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
    OpenSessionError,
    OpenSessionErrorKind,
    OpenSessionOperation,
    OpenSessionRequest,
    Opened,
    ListProviderSessions,
    ProviderHostRootConfiguration,
    ProviderRegistrationContext,
    ProviderRegistry,
    ProviderSessionCatalogEntry,
    ProviderSessionCatalogErrorKind,
    ProviderSessionsListFailed,
    ProviderSessionsListed,
    ResumeSessionRequest,
    StartSessionRequest,
    StopHost,
    StopHostResult,
    validate_timeout_seconds,
)
from orchestrator.agent_session import (
    AgentApprovalKind,
    AgentApprovalRequest,
    AgentEffectOutcome,
    AgentEffectResult,
    AgentSessionEffect,
    AgentSessionEvent,
    ApprovalDecisionResult,
    ApprovalRequested,
    CloseSession,
    InspectionCompleted,
    InterruptTurn,
    ModelSelectionAccepted,
    ModelSelectionFailed,
    ModelSelectionUncertain,
    RefreshInspection,
    RawProviderErrorPayload,
    ResolveApproval,
    SessionFailed,
    SetEffort,
    SetModel,
    StartTurn,
    OutputDelta,
    TurnCompleted,
    TurnFailed,
    TurnInterrupted,
    TurnStarted,
    TrySteerTurn,
    unsupported_effect_result,
    approval_decision_preflight,
)
from orchestrator.agent_io import AgentAttachmentRef, AgentInput, AgentOutput
from orchestrator.processes import LifecycleLogger


_CLAUDE_EDGE_QUEUE_CAPACITY = 256
_CLAUDE_RESULT_UUID_CAPACITY = 256
_CLAUDE_EDGE_STOP_TIMEOUT_SEC = 20.0
_CLAUDE_TURN_OUTPUT_MAX_CHARS = 1_048_576
_CLAUDE_DIAGNOSTIC_MAX_CHARS = 512
_CLAUDE_STDERR_MAX_CHARS = 2_048
_CLAUDE_COMMAND_QUEUE_CAPACITY = 2
_CLAUDE_PERMISSION_SETTINGS = json.dumps(
    {"permissions": {"ask": ["Bash"]}},
    ensure_ascii=True,
    separators=(",", ":"),
    sort_keys=True,
)
_CLAUDE_ATTACHMENT_MAX_BYTES = 20 * 1024 * 1024
_CLAUDE_APPROVAL_DIAGNOSTIC_MAX_CHARS = 2_048
_CLAUDE_AUTH_STATUS_TIMEOUT_SEC = 10.0
_CLAUDE_AUTH_STATUS_MAX_BYTES = 16_384
_CLAUDE_AUTH_PROCESS_STOP_TIMEOUT_SEC = 1.0
_CLAUDE_SUBSCRIPTION_TYPES = frozenset({"max", "pro"})
_CLAUDE_AUTH_UNAVAILABLE_ERROR = (
    "Claude App subscription login is unavailable; interactive login is disabled"
)
_CLAUDE_SDK_PROCESS_FAILURE_ERROR = "Claude SDK process exited unexpectedly"
_CLAUDE_SDK_SESSION_FAILURE_ERROR = "Claude SDK session failed"
_CLAUDE_MODEL_SELECTION_UNCERTAIN_ERROR = (
    "Claude model selection outcome is uncertain; provider session failed closed"
)
_CLAUDE_INTERRUPTED_TERMINAL_REASONS = frozenset(
    {"aborted_streaming", "aborted_tools"}
)
_CLAUDE_AUTH_OVERRIDE_EXACT_NAMES = frozenset(
    {
        "ANTHROPIC_API_HOST",
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_AUTH_TOKEN",
        "ANTHROPIC_BASE_URL",
        "ANTHROPIC_CUSTOM_HEADERS",
        "ANTHROPIC_ENVIRONMENT_ID",
        "ANTHROPIC_ENVIRONMENT_KEY",
        "ANTHROPIC_IDENTITY_TOKEN",
        "ANTHROPIC_IDENTITY_TOKEN_FILE",
        "ANTHROPIC_ORGANIZATION_ID",
        "ANTHROPIC_PROFILE",
        "ANTHROPIC_SERVICE_ACCOUNT_ID",
        "ANTHROPIC_UNIX_SOCKET",
        "ANTHROPIC_WORKSPACE_ID",
        "CLAUDE_CODE_API_BASE_URL",
        "CLAUDE_CODE_API_KEY_FILE_DESCRIPTOR",
        "CLAUDE_CODE_CUSTOM_OAUTH_URL",
        "CLAUDE_CODE_ENABLE_PROXY_AUTH_HELPER",
        "CLAUDE_CODE_OAUTH_TOKEN",
        "CLAUDE_CODE_OAUTH_TOKEN_FILE_DESCRIPTOR",
        "CLAUDE_CODE_USE_ANTHROPIC_AWS",
        "CLAUDE_CODE_USE_ANTHROPIC_GOOGLE_CLOUD",
        "CLAUDE_CODE_USE_BEDROCK",
        "CLAUDE_CODE_USE_FOUNDRY",
        "CLAUDE_CODE_USE_GATEWAY",
        "CLAUDE_CODE_USE_MANTLE",
        "CLAUDE_CODE_USE_VERTEX",
    }
)
_CLAUDE_AUTH_OVERRIDE_PREFIXES = (
    "ANTHROPIC_AWS_",
    "ANTHROPIC_BEDROCK_",
    "ANTHROPIC_FOUNDRY_",
    "ANTHROPIC_GOOGLE_CLOUD_",
    "ANTHROPIC_VERTEX_",
    "CLAUDE_CODE_OAUTH_",
)
_CLAUDE_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(?:[a-z0-9_-]*(?:api[_-]?key|token|secret|password)|authorization|bearer)"
    r"\b\s*[:=]\s*[^\s,;]+"
)
_CLAUDE_BEARER_SECRET = re.compile(r"(?i)\bbearer\s+[^\s,;]+")
_CLAUDE_SECRET_LITERAL = re.compile(r"\bsk-ant-[A-Za-z0-9_-]+\b")
_CLAUDE_MODEL_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$")
_CLAUDE_MAX_TOKEN_COUNT = (1 << 63) - 1
_CLAUDE_USAGE_FIELDS = (
    "input_tokens",
    "output_tokens",
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
)
_CLAUDE_MODEL_USAGE_FIELDS = (
    "inputTokens",
    "outputTokens",
    "cacheReadInputTokens",
    "cacheCreationInputTokens",
)
_CLAUDE_BACKENDS = {
    "firstParty": "anthropic",
    "anthropic": "anthropic",
    "bedrock": "bedrock",
    "anthropicAws": "bedrock",
    "vertex": "vertex",
    "anthropicGoogleCloud": "vertex",
    "foundry": "foundry",
    "gateway": "gateway",
    "mantle": "mantle",
}


def _bounded_claude_diagnostic(
    value: object,
    *,
    max_chars: int = _CLAUDE_DIAGNOSTIC_MAX_CHARS,
) -> str:
    if not isinstance(max_chars, int) or isinstance(max_chars, bool) or max_chars <= 0:
        raise ValueError("Claude diagnostic max_chars must be positive")
    text = str(value or "").strip()
    text = _CLAUDE_SECRET_ASSIGNMENT.sub("credential=[REDACTED]", text)
    text = _CLAUDE_BEARER_SECRET.sub("Bearer [REDACTED]", text)
    text = _CLAUDE_SECRET_LITERAL.sub("[REDACTED]", text)
    if len(text) <= max_chars:
        return text
    suffix = "... [truncated]"
    if max_chars <= len(suffix):
        return suffix[:max_chars]
    return text[: max_chars - len(suffix)] + suffix


def _bounded_approval_value(value: object) -> str:
    try:
        rendered = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):
        rendered = repr(value)
    return _bounded_claude_diagnostic(
        rendered,
        max_chars=_CLAUDE_APPROVAL_DIAGNOSTIC_MAX_CHARS,
    ) or "unavailable"


def _normalized_claude_model(value: object) -> str:
    if not isinstance(value, str):
        return ""
    model = value.strip()
    return model if _CLAUDE_MODEL_ID.fullmatch(model) else ""


def _bounded_usage_integer(value: object, *, field_name: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 0
        or value > _CLAUDE_MAX_TOKEN_COUNT
    ):
        raise ValueError(f"Claude {field_name} must be a bounded non-negative integer")
    return value


def _usage_total_from_fields(
    usage: object,
    *,
    field_names: tuple[str, ...],
) -> int | None:
    if usage is None:
        return None
    if not isinstance(usage, dict):
        raise ValueError("Claude usage must be an object")
    present = [field for field in field_names if field in usage]
    if not present:
        return None
    total = sum(
        _bounded_usage_integer(usage[field], field_name=field)
        for field in present
    )
    if total > _CLAUDE_MAX_TOKEN_COUNT:
        raise ValueError("Claude usage total exceeds the bounded token range")
    return total


def _claude_usage_total(usage: object) -> int | None:
    return _usage_total_from_fields(usage, field_names=_CLAUDE_USAGE_FIELDS)


def _claude_result_usage_metadata(
    message: ResultMessage,
    *,
    effective_model: str,
) -> tuple[str, str, int | None, int | None] | None:
    model_usage = message.model_usage
    result_total = _claude_usage_total(message.usage)
    if model_usage is None:
        return (effective_model, "", result_total, None) if result_total is not None else None
    if not isinstance(model_usage, dict) or not model_usage:
        raise ValueError("Claude model usage must be a non-empty object")
    if len(model_usage) > 32:
        raise ValueError("Claude model usage exceeds entry capacity")

    candidates: list[tuple[str, dict[str, Any]]] = []
    for raw_model, raw_usage in model_usage.items():
        model = _normalized_claude_model(raw_model)
        if not model or not isinstance(raw_usage, dict):
            raise ValueError("Claude model usage entry is invalid")
        canonical = _normalized_claude_model(raw_usage.get("canonicalModel"))
        if effective_model and effective_model not in {model, canonical}:
            continue
        candidates.append((canonical or model, raw_usage))
    if not candidates and not effective_model and len(model_usage) == 1:
        raw_model, raw_usage = next(iter(model_usage.items()))
        assert isinstance(raw_usage, dict)
        candidates.append(
            (
                _normalized_claude_model(raw_usage.get("canonicalModel"))
                or str(raw_model),
                raw_usage,
            )
        )
    if len(candidates) != 1:
        raise ValueError("Claude model usage does not identify one effective model")

    model, usage = candidates[0]
    provider = usage.get("provider")
    if provider is not None and not isinstance(provider, str):
        raise ValueError("Claude model usage provider must be a string")
    backend = _CLAUDE_BACKENDS.get(str(provider or ""), "")
    context_window = (
        _bounded_usage_integer(usage["contextWindow"], field_name="contextWindow")
        if "contextWindow" in usage
        else None
    )
    if context_window == 0:
        raise ValueError("Claude context window must be positive")
    model_total = _usage_total_from_fields(
        usage,
        field_names=_CLAUDE_MODEL_USAGE_FIELDS,
    )
    return (
        model,
        backend,
        model_total if model_total is not None else result_total,
        context_window,
    )


def _normalize_claude_approval_request(
    *,
    turn_ref: TurnRef,
    approval_ref: ApprovalRef,
    tool_name: str,
    tool_input: Mapping[str, Any],
    context: ToolPermissionContext,
) -> AgentApprovalRequest:
    name = str(tool_name or "").strip() or "unknown"
    reason = next(
        (
            value.strip()
            for value in (
                context.title,
                context.description,
                context.decision_reason,
            )
            if isinstance(value, str) and value.strip()
        ),
        f"Claude requested permission for {name}",
    )
    diagnostics = (
        ("provider_tool", _bounded_claude_diagnostic(name, max_chars=128)),
        ("provider_input", _bounded_approval_value(dict(tool_input))),
    )
    if name == "Bash":
        command = str(tool_input.get("command") or "").strip()
        cwd = str(tool_input.get("cwd") or "").strip()
        details = tuple(
            item
            for item in (
                f"command: {command}" if command else "command: unavailable",
                f"cwd: {cwd}" if cwd else "",
            )
            if item
        )
        return AgentApprovalRequest(
            turn_ref=turn_ref,
            approval_ref=approval_ref,
            kind=AgentApprovalKind.COMMAND,
            command=command or name,
            cwd=cwd,
            reason=reason,
            available_decisions=("accept", "decline"),
            prompt_details=details,
            diagnostics=diagnostics,
        )
    path = str(
        tool_input.get("file_path")
        or tool_input.get("path")
        or context.blocked_path
        or ""
    ).strip()
    if name in {"Write", "Edit", "MultiEdit", "NotebookEdit"}:
        return AgentApprovalRequest(
            turn_ref=turn_ref,
            approval_ref=approval_ref,
            kind=AgentApprovalKind.FILE_CHANGE,
            command=name,
            affected_paths=(path,) if path else (),
            reason=reason,
            available_decisions=("accept", "decline"),
            prompt_details=(f"file: {path}",) if path else (f"tool: {name}",),
            diagnostics=diagnostics,
        )
    details = [f"tool: {name}"]
    for key in ("url", "query", "prompt"):
        value = str(tool_input.get(key) or "").strip()
        if value:
            details.append(f"{key}: {_bounded_claude_diagnostic(value, max_chars=512)}")
    return AgentApprovalRequest(
        turn_ref=turn_ref,
        approval_ref=approval_ref,
        kind=AgentApprovalKind.TOOL,
        command=name,
        reason=reason,
        available_decisions=("accept", "decline"),
        prompt_details=tuple(details),
        diagnostics=diagnostics,
    )


def _resolve_claude_attachment_path(cwd: Path, attachment: AgentAttachmentRef) -> Path:
    root = cwd.resolve()
    try:
        path = (root / attachment.relative_path).resolve(strict=True)
    except OSError as exc:
        raise _ClaudeAttachmentError(
            f"Claude attachment is unavailable: {attachment.display_name}"
        ) from exc
    if not path.is_relative_to(root) or not path.is_file():
        raise _ClaudeAttachmentError(
            f"Claude attachment is unavailable: {attachment.display_name}"
        )
    return path


def _claude_attachment_block(cwd: Path, attachment: AgentAttachmentRef) -> dict[str, Any]:
    media_type = str(attachment.media_type or "").strip().lower()
    supported_image_types = {"image/jpeg", "image/png", "image/gif", "image/webp"}
    if media_type not in supported_image_types and not media_type.startswith("text/"):
        raise _ClaudeAttachmentError(
            f"unsupported Claude attachment media type: {media_type or 'unknown'}"
        )
    if attachment.size_bytes > _CLAUDE_ATTACHMENT_MAX_BYTES:
        raise _ClaudeAttachmentError(
            f"Claude attachment exceeds {_CLAUDE_ATTACHMENT_MAX_BYTES} byte limit"
        )
    path = _resolve_claude_attachment_path(cwd, attachment)
    try:
        with path.open("rb") as stream:
            payload = stream.read(_CLAUDE_ATTACHMENT_MAX_BYTES + 1)
    except OSError as exc:
        raise _ClaudeAttachmentError(
            f"Claude attachment is unavailable: {attachment.display_name}"
        ) from exc
    if len(payload) != attachment.size_bytes:
        raise _ClaudeAttachmentError(
            f"Claude attachment size changed: {attachment.display_name}"
        )
    if len(payload) > _CLAUDE_ATTACHMENT_MAX_BYTES:
        raise _ClaudeAttachmentError(
            f"Claude attachment exceeds {_CLAUDE_ATTACHMENT_MAX_BYTES} byte limit"
        )
    if media_type in supported_image_types:
        return {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": media_type,
                "data": base64.b64encode(payload).decode("ascii"),
            },
        }
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _ClaudeAttachmentError(
            f"Claude text attachment is not valid UTF-8: {attachment.display_name}"
        ) from exc
    return {
        "type": "text",
        "text": f"[Attachment: {attachment.display_name}]\n{text}",
    }


def _claude_query_content(cwd: Path, agent_input: AgentInput) -> tuple[dict[str, Any], ...]:
    blocks: list[dict[str, Any]] = []
    if agent_input.text:
        blocks.append({"type": "text", "text": agent_input.text})
    blocks.extend(
        _claude_attachment_block(cwd, attachment)
        for attachment in agent_input.attachments
    )
    if not blocks:
        blocks.append({"type": "text", "text": ""})
    return tuple(blocks)


async def _claude_query_messages(
    content: tuple[dict[str, Any], ...],
):
    yield {
        "type": "user",
        "message": {"role": "user", "content": [dict(block) for block in content]},
        "parent_tool_use_id": None,
    }


class _BoundedClaudeStderr:
    def __init__(self, capacity: int = _CLAUDE_STDERR_MAX_CHARS) -> None:
        if not isinstance(capacity, int) or isinstance(capacity, bool) or capacity <= 0:
            raise ValueError("Claude stderr capacity must be positive")
        self._capacity = capacity
        self._lock = threading.Lock()
        self._text = ""

    def append(self, line: str) -> None:
        diagnostic = _bounded_claude_diagnostic(line)
        if not diagnostic:
            return
        with self._lock:
            combined = f"{self._text}\n{diagnostic}" if self._text else diagnostic
            self._text = combined[-self._capacity :]

    def snapshot(self) -> str:
        with self._lock:
            return self._text


class _ClaudeAuthPreflightError(RuntimeError):
    pass


class _ClaudeAuthCleanupError(RuntimeError):
    pass


class _ClaudeProtocolContractError(RuntimeError):
    pass


class _ClaudeModelSelectionUncertainError(RuntimeError):
    pass


class _ClaudeAttachmentError(ValueError):
    pass


def _is_explicit_claude_model_rejection(exc: BaseException) -> bool:
    # The pinned SDK wraps a provider control-response error as a plain Exception.
    # Timeout and transport failures retain either a cause or a typed exception.
    return type(exc) is Exception and exc.__cause__ is None


def _is_claude_auth_override(name: str) -> bool:
    return name in _CLAUDE_AUTH_OVERRIDE_EXACT_NAMES or name.startswith(
        _CLAUDE_AUTH_OVERRIDE_PREFIXES
    )


def _has_claude_auth_override(*environments: Mapping[str, str]) -> bool:
    return any(
        _is_claude_auth_override(name)
        for environment in environments
        for name in environment
    )


class _BoundedClaudeProcessOutput:
    def __init__(self, capacity: int = _CLAUDE_AUTH_STATUS_MAX_BYTES) -> None:
        self._capacity = capacity
        self._buffer = bytearray()
        self._lock = threading.Lock()
        self._overflow = threading.Event()
        self._reader_failed = threading.Event()

    def append(self, chunk: bytes) -> None:
        with self._lock:
            remaining = self._capacity - len(self._buffer)
            if remaining > 0:
                self._buffer.extend(chunk[:remaining])
            if len(chunk) > remaining:
                self._overflow.set()

    def mark_reader_failed(self) -> None:
        self._reader_failed.set()

    @property
    def failed(self) -> bool:
        return self._overflow.is_set() or self._reader_failed.is_set()

    def snapshot(self) -> bytes:
        with self._lock:
            return bytes(self._buffer)


def _drain_claude_process_output(
    stream: Any,
    output: _BoundedClaudeProcessOutput,
) -> None:
    try:
        while True:
            chunk = stream.read(4_096)
            if not chunk:
                return
            output.append(chunk)
    except (OSError, ValueError):
        output.mark_reader_failed()


class _ClaudeAuthProcessOwner:
    """Owns one auth subprocess and both bounded output readers."""

    def __init__(self, process: subprocess.Popen[bytes]) -> None:
        if process.stdout is None or process.stderr is None:
            raise ValueError("Claude auth process requires stdout and stderr pipes")
        self._process = process
        self.stdout = _BoundedClaudeProcessOutput()
        self.stderr = _BoundedClaudeProcessOutput()
        self._readers = (
            threading.Thread(
                target=_drain_claude_process_output,
                args=(process.stdout, self.stdout),
                name="claude-auth-stdout-reader",
                daemon=False,
            ),
            threading.Thread(
                target=_drain_claude_process_output,
                args=(process.stderr, self.stderr),
                name="claude-auth-stderr-reader",
                daemon=False,
            ),
        )
        self._started_readers: list[threading.Thread] = []
        self._lock = threading.RLock()
        self._pipes_closed = False
        self._cleaned = False

    @property
    def cleaned(self) -> bool:
        with self._lock:
            return self._cleaned

    @property
    def output_failed(self) -> bool:
        return self.stdout.failed or self.stderr.failed

    @property
    def returncode(self) -> int | None:
        return self._process.poll()

    def start_readers(self) -> None:
        with self._lock:
            if self._cleaned:
                return
            try:
                for reader in self._readers:
                    reader.start()
                    self._started_readers.append(reader)
            except BaseException:
                try:
                    self._cleanup(terminate=True)
                except _ClaudeAuthCleanupError:
                    pass
                raise

    def stop(self) -> None:
        with self._lock:
            self._cleanup(terminate=True)

    def finish(self) -> None:
        with self._lock:
            self._cleanup(terminate=False)

    def _cleanup(self, *, terminate: bool) -> None:
        if self._cleaned:
            return
        try:
            if terminate:
                self._stop_process()
            elif self._process.poll() is None:
                raise RuntimeError("Claude auth process is still running")
            self._close_pipes_and_join_readers()
        except BaseException as exc:
            raise _ClaudeAuthCleanupError(
                "Claude auth process cleanup did not complete"
            ) from exc
        self._cleaned = True

    def _stop_process(self) -> None:
        if self._process.poll() is not None:
            return
        try:
            self._process.terminate()
        except ProcessLookupError:
            return
        try:
            self._process.wait(timeout=_CLAUDE_AUTH_PROCESS_STOP_TIMEOUT_SEC)
            return
        except subprocess.TimeoutExpired:
            pass
        try:
            self._process.kill()
        except ProcessLookupError:
            return
        self._process.wait(timeout=_CLAUDE_AUTH_PROCESS_STOP_TIMEOUT_SEC)

    def _close_pipes_and_join_readers(self) -> None:
        deadline = time.monotonic() + _CLAUDE_AUTH_PROCESS_STOP_TIMEOUT_SEC
        for reader in self._started_readers:
            reader.join(timeout=max(0.0, deadline - time.monotonic()))
        if any(reader.is_alive() for reader in self._started_readers):
            self._close_pipes()
            deadline = time.monotonic() + _CLAUDE_AUTH_PROCESS_STOP_TIMEOUT_SEC
            for reader in self._started_readers:
                reader.join(timeout=max(0.0, deadline - time.monotonic()))
        else:
            self._close_pipes()
        if any(reader.is_alive() for reader in self._started_readers):
            raise RuntimeError("Claude auth output reader did not stop")

    def _close_pipes(self) -> None:
        if self._pipes_closed:
            return
        assert self._process.stdout is not None
        assert self._process.stderr is not None
        self._process.stdout.close()
        self._process.stderr.close()
        self._pipes_closed = True


@dataclass(frozen=True)
class _ClaudeRuntimeContext:
    """One immutable environment/store view shared by all Claude operations."""

    environment: Mapping[str, str] = field(repr=False)
    projects_root: Path

    @classmethod
    def capture(
        cls,
        *,
        ambient_environment: Mapping[str, str],
        overrides: Mapping[str, str] | None,
    ) -> _ClaudeRuntimeContext:
        environment = {**dict(ambient_environment), **dict(overrides or {})}
        if any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in environment.items()
        ):
            raise ValueError("Claude provider env must contain string keys and values")
        config_dir = environment.get("CLAUDE_CONFIG_DIR", "").strip()
        if config_dir:
            config_root = Path(config_dir).expanduser()
        else:
            home = environment.get("HOME", "").strip()
            config_root = (Path(home).expanduser() if home else Path.home()) / ".claude"
        return cls(
            environment=MappingProxyType(environment),
            projects_root=config_root / "projects",
        )

    def list_sessions(
        self,
        *,
        directory: str,
        limit: int | None,
    ) -> list[SDKSessionInfo]:
        if directory:
            resolved = str(Path(directory).expanduser().resolve())
            sessions = _read_sessions_from_dir(
                self.projects_root / project_key_for_directory(resolved),
                resolved,
            )
        else:
            sessions = []
            try:
                project_dirs = [item for item in self.projects_root.iterdir() if item.is_dir()]
            except OSError:
                project_dirs = []
            for project_dir in project_dirs:
                sessions.extend(_read_sessions_from_dir(project_dir))
            newest_by_id: dict[str, SDKSessionInfo] = {}
            for session in sessions:
                previous = newest_by_id.get(session.session_id)
                if previous is None or session.last_modified > previous.last_modified:
                    newest_by_id[session.session_id] = session
            sessions = list(newest_by_id.values())
        sessions.sort(key=lambda session: session.last_modified, reverse=True)
        if limit is not None and limit > 0:
            return sessions[:limit]
        return sessions

    def get_session_info(
        self,
        session_id: str,
        *,
        directory: str,
    ) -> SDKSessionInfo | None:
        native_id = _validate_uuid(session_id)
        if native_id is None:
            return None
        file_name = f"{native_id}.jsonl"
        if directory:
            resolved = str(Path(directory).expanduser().resolve())
            lite = _read_session_lite(
                self.projects_root / project_key_for_directory(resolved) / file_name
            )
            return (
                _parse_session_info_from_lite(native_id, lite, resolved)
                if lite is not None
                else None
            )
        try:
            project_dirs = [item for item in self.projects_root.iterdir() if item.is_dir()]
        except OSError:
            return None
        for project_dir in project_dirs:
            lite = _read_session_lite(project_dir / file_name)
            if lite is not None:
                return _parse_session_info_from_lite(native_id, lite)
        return None


@dataclass(frozen=True)
class ClaudeProviderConfig:
    logger: LifecycleLogger
    cli_path: Path | None = None
    env: Mapping[str, str] | None = field(default=None, repr=False)
    role: str = "agent"
    startup_timeout_sec: float = 120.0
    rpc_timeout_sec: float = 60.0
    auth_status_timeout_sec: float = _CLAUDE_AUTH_STATUS_TIMEOUT_SEC
    edge_thread_name: str = "claude-sdk-client-thread"
    edge_queue_capacity: int = _CLAUDE_EDGE_QUEUE_CAPACITY
    result_uuid_capacity: int = _CLAUDE_RESULT_UUID_CAPACITY
    edge_stop_timeout_sec: float = _CLAUDE_EDGE_STOP_TIMEOUT_SEC
    turn_output_max_chars: int = _CLAUDE_TURN_OUTPUT_MAX_CHARS
    runtime_context: _ClaudeRuntimeContext | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.logger, LifecycleLogger):
            raise ValueError("Claude provider logger must be LifecycleLogger")
        if self.cli_path is not None and not isinstance(self.cli_path, Path):
            raise ValueError("Claude provider cli_path must be Path or None")
        if not isinstance(self.role, str) or not self.role.strip():
            raise ValueError("Claude provider role must be a non-empty string")
        if self.startup_timeout_sec <= 0:
            raise ValueError("Claude provider startup timeout must be positive")
        rpc_timeout_sec = validate_timeout_seconds(
            self.rpc_timeout_sec,
            field_name="Claude provider RPC timeout",
        )
        auth_status_timeout_sec = validate_timeout_seconds(
            self.auth_status_timeout_sec,
            field_name="Claude provider auth status timeout",
        )
        if not isinstance(self.edge_thread_name, str) or not self.edge_thread_name.strip():
            raise ValueError("Claude provider edge thread name must be non-empty")
        if (
            not isinstance(self.edge_queue_capacity, int)
            or isinstance(self.edge_queue_capacity, bool)
            or self.edge_queue_capacity <= 0
        ):
            raise ValueError("Claude provider edge queue capacity must be positive")
        if (
            not isinstance(self.result_uuid_capacity, int)
            or isinstance(self.result_uuid_capacity, bool)
            or self.result_uuid_capacity <= 0
        ):
            raise ValueError("Claude provider result UUID capacity must be positive")
        if self.edge_stop_timeout_sec <= 0:
            raise ValueError("Claude provider edge stop timeout must be positive")
        if (
            not isinstance(self.turn_output_max_chars, int)
            or isinstance(self.turn_output_max_chars, bool)
            or self.turn_output_max_chars <= 0
        ):
            raise ValueError("Claude provider turn output limit must be positive")
        runtime_context = self.runtime_context or _ClaudeRuntimeContext.capture(
            ambient_environment=os.environ,
            overrides=self.env,
        )
        if not isinstance(runtime_context, _ClaudeRuntimeContext):
            raise ValueError("Claude provider runtime context must be adapter-owned")
        object.__setattr__(self, "runtime_context", runtime_context)
        object.__setattr__(self, "env", runtime_context.environment)
        object.__setattr__(self, "role", self.role.strip())
        object.__setattr__(self, "edge_thread_name", self.edge_thread_name.strip())
        object.__setattr__(self, "auth_status_timeout_sec", auth_status_timeout_sec)
        object.__setattr__(self, "rpc_timeout_sec", rpc_timeout_sec)


@dataclass(frozen=True)
class _ClaudeEdgeConnected:
    session_id: str
    model: str
    runtime_version: str


@dataclass(frozen=True)
class _ClaudeEdgeMessage:
    message: object


@dataclass(frozen=True)
class _ClaudeEdgeTurnStarted:
    turn_ref: TurnRef


@dataclass(frozen=True)
class _ClaudeEdgeTurnMessage:
    turn_ref: TurnRef
    message: object
    interrupt_requested: bool = False


@dataclass(frozen=True)
class _ClaudeEdgeTurnFailed:
    turn_ref: TurnRef
    error: str


@dataclass(frozen=True)
class _ClaudeEdgeApprovalRequested:
    turn_ref: TurnRef
    request: AgentApprovalRequest


@dataclass(frozen=True)
class _ClaudeEdgeModelSelectionAccepted:
    requested_model: str


@dataclass(frozen=True)
class _ClaudeEdgeModelSelectionFailed:
    requested_model: str
    error: str


@dataclass(frozen=True)
class _ClaudeEdgeModelSelectionUncertain:
    requested_model: str
    error: str


@dataclass(frozen=True)
class _ClaudeMetadataEpoch:
    model: str = ""
    backend: str = ""
    used_tokens: int | None = None
    context_window: int | None = None
    context_error: str = ""


@dataclass
class _ClaudePendingMetadata:
    model: str = ""
    used_tokens: int | None = None
    context_error: str = ""


@dataclass(frozen=True)
class _ClaudeTurnCommand:
    turn_ref: TurnRef
    text: str
    content: tuple[dict[str, Any], ...] | None = None


@dataclass(frozen=True)
class _ClaudePendingApproval:
    request: AgentApprovalRequest
    future: asyncio.Future[PermissionResultAllow | PermissionResultDeny]


@dataclass(frozen=True)
class _ClaudeInterruptCommand:
    turn_ref: TurnRef


@dataclass(frozen=True)
class _ClaudeSetModelCommand:
    model: str


@dataclass(frozen=True)
class _ClaudeEdgeDuplicateInitialization:
    pass


@dataclass(frozen=True)
class _ClaudeEdgeFailure:
    error: str
    error_kind: OpenSessionErrorKind


_ClaudeEdgeEvent = (
    _ClaudeEdgeConnected
    | _ClaudeEdgeDuplicateInitialization
    | _ClaudeEdgeMessage
    | _ClaudeEdgeTurnStarted
    | _ClaudeEdgeTurnMessage
    | _ClaudeEdgeTurnFailed
    | _ClaudeEdgeApprovalRequested
    | _ClaudeEdgeModelSelectionAccepted
    | _ClaudeEdgeModelSelectionFailed
    | _ClaudeEdgeModelSelectionUncertain
    | _ClaudeEdgeFailure
)

_ClaudeEdgeCommand = _ClaudeTurnCommand | _ClaudeInterruptCommand | _ClaudeSetModelCommand


class _ClaudeSdkEdge:
    """Owns the SDK async runtime, client and its single lifetime reader."""

    def __init__(
        self,
        config: ClaudeProviderConfig,
        options: ClaudeAgentOptions,
        stderr: _BoundedClaudeStderr,
    ) -> None:
        self._config = config
        self._options = options
        self._stderr = stderr
        self._options.can_use_tool = self._handle_provider_callback
        self._events: Queue[_ClaudeEdgeEvent] = Queue(maxsize=config.edge_queue_capacity)
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_event: asyncio.Event | None = None
        self._commands: asyncio.Queue[_ClaudeEdgeCommand] | None = None
        self._auth_owner: _ClaudeAuthProcessOwner | None = None
        self._correlated_turn_ref: TurnRef | None = None
        self._interrupt_turn_ref: TurnRef | None = None
        self._pending_model: str | None = None
        self._pending_approval: _ClaudePendingApproval | None = None
        self._approval_sequence = 0
        self._session_id = str(options.session_id or options.resume or "").strip()
        if not self._session_id:
            raise ValueError("Claude SDK edge requires a correlated session ID")
        self._stop_requested = threading.Event()
        self._state_lock = threading.Lock()
        self._progress = False
        self._terminal_error = ""
        self._terminal_error_consumed = False
        self._overflow_error = ""
        self._cleanup_error = ""
        self._cleanup_error_is_auth = False
        self._result_uuid_order: deque[str] = deque()
        self._result_uuids: set[str] = set()

    def start(self) -> None:
        with self._state_lock:
            if self._thread is not None:
                raise RuntimeError("Claude SDK edge is already started")
            self._thread = threading.Thread(
                target=self._thread_main,
                name=self._config.edge_thread_name,
                daemon=False,
            )
            self._thread.start()

    def poll_events(self, *, limit: int | None = None) -> list[_ClaudeEdgeEvent]:
        events: list[_ClaudeEdgeEvent] = []
        while limit is None or len(events) < limit:
            try:
                event = self._events.get_nowait()
            except Empty:
                break
            events.append(event)
            if isinstance(event, _ClaudeEdgeFailure):
                self._terminal_error_consumed = True
        return events

    def submit_turn(self, command: _ClaudeTurnCommand) -> AgentEffectResult:
        with self._state_lock:
            loop = self._loop
            commands = self._commands
            if self._stop_requested.is_set() or self._terminal_error or self._overflow_error:
                return AgentEffectResult.rejected("Claude provider session is unavailable")
            if loop is None or commands is None or not self._session_id:
                return AgentEffectResult.rejected("Claude provider session is not ready")
            if self._correlated_turn_ref is not None:
                return AgentEffectResult.rejected("Claude provider session is busy")
            self._correlated_turn_ref = command.turn_ref
        try:
            loop.call_soon_threadsafe(self._handoff_command, commands, command)
        except RuntimeError as exc:
            with self._state_lock:
                if self._correlated_turn_ref == command.turn_ref:
                    self._correlated_turn_ref = None
            return AgentEffectResult.rejected(_error_text(exc))
        return AgentEffectResult.accepted()

    def interrupt_turn(self, command: _ClaudeInterruptCommand) -> AgentEffectResult:
        with self._state_lock:
            loop = self._loop
            commands = self._commands
            if self._stop_requested.is_set() or self._terminal_error or self._overflow_error:
                return AgentEffectResult.rejected("Claude provider session is unavailable")
            if loop is None or commands is None or not self._session_id:
                return AgentEffectResult.rejected("Claude provider session is not ready")
            if self._correlated_turn_ref != command.turn_ref:
                return AgentEffectResult.rejected("Claude active turn no longer matches")
            if self._interrupt_turn_ref is not None:
                return AgentEffectResult.rejected("Claude turn interrupt was already requested")
            self._interrupt_turn_ref = command.turn_ref
        try:
            loop.call_soon_threadsafe(self._handoff_command, commands, command)
        except RuntimeError as exc:
            with self._state_lock:
                if self._interrupt_turn_ref == command.turn_ref:
                    self._interrupt_turn_ref = None
            return AgentEffectResult.rejected(_error_text(exc))
        return AgentEffectResult.accepted()

    def set_model(self, command: _ClaudeSetModelCommand) -> AgentEffectResult:
        with self._state_lock:
            loop = self._loop
            commands = self._commands
            if self._stop_requested.is_set() or self._terminal_error or self._overflow_error:
                return AgentEffectResult.rejected("Claude provider session is unavailable")
            if loop is None or commands is None or not self._session_id:
                return AgentEffectResult.rejected("Claude provider session is not ready")
            if self._correlated_turn_ref is not None:
                return AgentEffectResult.rejected("Claude provider session is busy")
            if self._pending_model is not None:
                return AgentEffectResult.rejected("Claude model selection is already pending")
            self._pending_model = command.model
        try:
            loop.call_soon_threadsafe(self._handoff_command, commands, command)
        except RuntimeError as exc:
            with self._state_lock:
                if self._pending_model == command.model:
                    self._pending_model = None
            return AgentEffectResult.rejected(_error_text(exc))
        return AgentEffectResult.accepted()

    def resolve_approval(
        self,
        effect: ResolveApproval,
        *,
        expected_request: AgentApprovalRequest,
    ) -> AgentEffectResult:
        with self._state_lock:
            loop = self._loop
            pending = self._pending_approval
            if self._stop_requested.is_set() or self._terminal_error or self._overflow_error:
                return AgentEffectResult.rejected("Claude provider session is unavailable")
            if loop is None:
                return AgentEffectResult.rejected("Claude provider session is not ready")
            if pending is None or pending.request != expected_request:
                return AgentEffectResult.rejected("Claude approval request no longer matches")
        result: PermissionResultAllow | PermissionResultDeny
        if effect.decision == "accept":
            result = PermissionResultAllow()
        elif effect.decision == "decline":
            result = PermissionResultDeny(message="ORC approval declined")
        else:
            return AgentEffectResult.rejected(
                f"Claude approval decision is not supported: {effect.decision}"
            )
        try:
            loop.call_soon_threadsafe(self._complete_approval, pending, result)
        except RuntimeError as exc:
            raise RuntimeError(_error_text(exc)) from exc
        return AgentEffectResult.accepted()

    def consume_overflow_error(self) -> str:
        with self._state_lock:
            error = self._overflow_error
            self._overflow_error = ""
            return error

    def consume_session_overflow_error(self) -> str:
        with self._state_lock:
            error = self._overflow_error
            self._overflow_error = ""
            if error:
                self._terminal_error_consumed = True
            return error

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

    def stop(self) -> None:
        self._stop_requested.set()
        with self._state_lock:
            loop = self._loop
            stop_event = self._stop_event
            auth_owner = self._auth_owner
            thread = self._thread
        auth_cleanup_error = ""
        if auth_owner is not None:
            try:
                auth_owner.stop()
            except _ClaudeAuthCleanupError as exc:
                auth_cleanup_error = _error_text(exc)
        if loop is not None and stop_event is not None:
            try:
                loop.call_soon_threadsafe(
                    self._cancel_active_approval,
                    "Claude session is stopping",
                )
                loop.call_soon_threadsafe(stop_event.set)
            except RuntimeError:
                # The edge thread is already closing or has closed its loop.
                pass
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=self._config.edge_stop_timeout_sec)
            if thread.is_alive():
                raise RuntimeError("Claude SDK edge did not stop within cleanup deadline")
        with self._state_lock:
            if auth_owner is not None and auth_owner.cleaned:
                if self._auth_owner is auth_owner:
                    self._auth_owner = None
                if self._cleanup_error_is_auth:
                    self._cleanup_error = ""
                    self._cleanup_error_is_auth = False
            cleanup_error = self._cleanup_error
        if auth_cleanup_error and not auth_owner.cleaned:
            raise RuntimeError(auth_cleanup_error)
        if cleanup_error:
            raise RuntimeError(f"Claude SDK edge cleanup failed: {cleanup_error}")

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._run())
        except BaseException as exc:
            if self._stop_requested.is_set():
                with self._state_lock:
                    self._cleanup_error = _error_text(exc)
                    self._cleanup_error_is_auth = isinstance(
                        exc,
                        _ClaudeAuthCleanupError,
                    )
            else:
                self._publish_failure(exc)
        finally:
            with self._state_lock:
                self._loop = None
                self._stop_event = None
                self._progress = True

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()
        commands: asyncio.Queue[_ClaudeEdgeCommand] = (
            asyncio.Queue(maxsize=_CLAUDE_COMMAND_QUEUE_CAPACITY)
        )
        with self._state_lock:
            self._loop = loop
            self._stop_event = stop_event
            self._commands = commands
            self._progress = True
        if self._stop_requested.is_set():
            stop_event.set()
            return
        if not self._require_local_subscription():
            return
        if self._stop_requested.is_set():
            return
        client = ClaudeSDKClient(options=self._options)
        connect_task: asyncio.Task[None] | None = None
        reader: asyncio.Task[None] | None = None
        command_worker: asyncio.Task[None] | None = None
        stop_waiter: asyncio.Task[bool] | None = None
        try:
            connect_task = asyncio.create_task(client.connect())
            stop_waiter = asyncio.create_task(stop_event.wait())
            done, _pending = await asyncio.wait(
                {connect_task, stop_waiter},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_waiter in done:
                connect_task.cancel()
                try:
                    await connect_task
                except asyncio.CancelledError:
                    pass
                return
            await connect_task
            server_info = await client.get_server_info() or {}
            reader = asyncio.create_task(self._read_lifetime(client, server_info))
            command_worker = asyncio.create_task(self._run_commands(client, commands))
            self._publish(
                _ClaudeEdgeConnected(
                    session_id=self._session_id,
                    model=str(self._options.model or "").strip(),
                    runtime_version=str(
                        server_info.get("server_version") or "unknown"
                    ).strip()
                    or "unknown",
                )
            )
            done, _pending = await asyncio.wait(
                {reader, command_worker, stop_waiter},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if reader in done:
                await reader
                if not self._stop_requested.is_set():
                    raise _ClaudeProtocolContractError(
                        "Claude SDK message stream ended unexpectedly"
                    )
            if command_worker in done:
                await command_worker
                if not self._stop_requested.is_set():
                    raise _ClaudeProtocolContractError(
                        "Claude SDK command loop ended unexpectedly"
                    )
        finally:
            if stop_waiter is not None and not stop_waiter.done():
                stop_waiter.cancel()
            if stop_waiter is not None:
                try:
                    await stop_waiter
                except asyncio.CancelledError:
                    pass
            await client.disconnect()
            if command_worker is not None and not command_worker.done():
                command_worker.cancel()
            if command_worker is not None:
                try:
                    await command_worker
                except asyncio.CancelledError:
                    pass
            if reader is not None and not reader.done():
                reader.cancel()
            if reader is not None:
                try:
                    await reader
                except asyncio.CancelledError:
                    pass
            with self._state_lock:
                self._commands = None
                self._session_id = ""

    def _require_local_subscription(self) -> bool:
        command = [str(self._config.cli_path or "claude"), "auth", "status", "--json"]
        environment = dict(self._config.env or {})
        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=environment,
                bufsize=0,
            )
        except OSError:
            self._log_auth_preflight(available=False)
            raise _ClaudeAuthPreflightError(_CLAUDE_AUTH_UNAVAILABLE_ERROR) from None
        owner = _ClaudeAuthProcessOwner(process)
        with self._state_lock:
            self._auth_owner = owner
        deadline = time.monotonic() + self._config.auth_status_timeout_sec
        cancelled = False
        try:
            owner.start_readers()
            while owner.returncode is None:
                if self._stop_requested.is_set():
                    cancelled = True
                    owner.stop()
                    break
                if owner.output_failed or time.monotonic() >= deadline:
                    owner.stop()
                    break
                time.sleep(0.005)
            if owner.returncode is None:
                owner.stop()
        finally:
            if owner.returncode is None:
                owner.stop()
            else:
                owner.finish()
            with self._state_lock:
                if owner.cleaned and self._auth_owner is owner:
                    self._auth_owner = None

        if cancelled or self._stop_requested.is_set():
            self._log_auth_preflight(available=False)
            return False
        available = False
        if owner.returncode == 0 and not owner.output_failed:
            try:
                payload = json.loads(
                    owner.stdout.snapshot().decode("utf-8", errors="replace")
                )
            except (TypeError, ValueError):
                pass
            else:
                available = (
                    isinstance(payload, dict)
                    and payload.get("loggedIn") is True
                    and payload.get("authMethod") == "claude.ai"
                    and isinstance(payload.get("subscriptionType"), str)
                    and payload["subscriptionType"].strip().lower()
                    in _CLAUDE_SUBSCRIPTION_TYPES
                )
        self._log_auth_preflight(available=available)
        if not available:
            raise _ClaudeAuthPreflightError(_CLAUDE_AUTH_UNAVAILABLE_ERROR)
        return True

    def _log_auth_preflight(self, *, available: bool) -> None:
        self._config.logger.event(
            "claude_auth_preflight",
            provider=ProviderKind.CLAUDE.value,
            available=available,
            source="claude_app_subscription",
        )

    async def _run_commands(
        self,
        client: ClaudeSDKClient,
        commands: asyncio.Queue[_ClaudeEdgeCommand],
    ) -> None:
        while True:
            command = await commands.get()
            if isinstance(command, _ClaudeSetModelCommand):
                try:
                    async with asyncio.timeout(self._config.rpc_timeout_sec):
                        await client.set_model(command.model)
                except TimeoutError as exc:
                    self._publish_uncertain_model_selection(command.model)
                    raise _ClaudeModelSelectionUncertainError(
                        _CLAUDE_MODEL_SELECTION_UNCERTAIN_ERROR
                    ) from exc
                except Exception as exc:
                    if not _is_explicit_claude_model_rejection(exc):
                        self._publish_uncertain_model_selection(command.model)
                        raise _ClaudeModelSelectionUncertainError(
                            _CLAUDE_MODEL_SELECTION_UNCERTAIN_ERROR
                        ) from exc
                    error = _claude_edge_failure_error(exc)
                    with self._state_lock:
                        if self._pending_model == command.model:
                            self._pending_model = None
                    self._publish(
                        _ClaudeEdgeModelSelectionFailed(command.model, error)
                    )
                else:
                    with self._state_lock:
                        if self._pending_model == command.model:
                            self._pending_model = None
                    self._publish(_ClaudeEdgeModelSelectionAccepted(command.model))
                continue
            if isinstance(command, _ClaudeInterruptCommand):
                with self._state_lock:
                    is_current = self._correlated_turn_ref == command.turn_ref
                if not is_current:
                    with self._state_lock:
                        if self._interrupt_turn_ref == command.turn_ref:
                            self._interrupt_turn_ref = None
                    self._config.logger.event(
                        "claude_stale_interrupt_request_dropped",
                        provider=ProviderKind.CLAUDE.value,
                        turn_ref_sequence=command.turn_ref.sequence,
                    )
                    continue
                self._cancel_pending_approval(
                    command.turn_ref,
                    message="ORC approval cancelled by turn interrupt",
                )
                await asyncio.sleep(0)
                await client.interrupt()
                continue
            self._publish(_ClaudeEdgeTurnStarted(command.turn_ref))
            try:
                prompt = (
                    command.text
                    if command.content is None
                    else _claude_query_messages(command.content)
                )
                await client.query(prompt, session_id=self._session_id)
            except Exception as exc:
                error = _claude_edge_failure_error(exc)
                with self._state_lock:
                    if self._correlated_turn_ref == command.turn_ref:
                        self._correlated_turn_ref = None
                    if self._interrupt_turn_ref == command.turn_ref:
                        self._interrupt_turn_ref = None
                self._publish(_ClaudeEdgeTurnFailed(command.turn_ref, error))
                raise

    def _handoff_command(
        self,
        commands: asyncio.Queue[_ClaudeEdgeCommand],
        command: _ClaudeEdgeCommand,
    ) -> None:
        try:
            commands.put_nowait(command)
        except asyncio.QueueFull:
            self._config.logger.event(
                "claude_command_handoff_failed",
                provider=ProviderKind.CLAUDE.value,
                command_kind=(
                    "interrupt"
                    if isinstance(command, _ClaudeInterruptCommand)
                    else "model"
                    if isinstance(command, _ClaudeSetModelCommand)
                    else "turn"
                ),
                turn_ref_sequence=(
                    command.turn_ref.sequence
                    if isinstance(command, (_ClaudeTurnCommand, _ClaudeInterruptCommand))
                    else None
                ),
            )
            self._stop_requested.set()
            self._publish_failure(
                _ClaudeProtocolContractError(
                    "Claude provider command handoff capacity exceeded"
                )
            )
            stop_event = self._stop_event
            if stop_event is not None:
                stop_event.set()

    def _publish_uncertain_model_selection(self, requested_model: str) -> None:
        with self._state_lock:
            if self._pending_model != requested_model:
                return
            self._pending_model = None
        self._publish(
            _ClaudeEdgeModelSelectionUncertain(
                requested_model,
                _CLAUDE_MODEL_SELECTION_UNCERTAIN_ERROR,
            )
        )

    async def _handle_provider_callback(
        self,
        tool_name: str,
        tool_input: dict[str, Any],
        context: ToolPermissionContext,
    ) -> PermissionResultAllow | PermissionResultDeny:
        if tool_name == "AskUserQuestion":
            return PermissionResultDeny(
                message="Claude structured questions are unsupported",
            )
        with self._state_lock:
            turn_ref = self._correlated_turn_ref
            if turn_ref is None:
                return PermissionResultDeny(
                    message="Claude approval has no active ORC turn",
                )
            if self._pending_approval is not None:
                return PermissionResultDeny(
                    message="another Claude approval is already pending",
                )
            self._approval_sequence += 1
            approval_ref = ApprovalRef(
                turn_ref,
                f"claude-approval-{self._approval_sequence}",
            )
        request = _normalize_claude_approval_request(
            turn_ref=turn_ref,
            approval_ref=approval_ref,
            tool_name=tool_name,
            tool_input=tool_input,
            context=context,
        )
        future: asyncio.Future[PermissionResultAllow | PermissionResultDeny] = (
            asyncio.get_running_loop().create_future()
        )
        pending = _ClaudePendingApproval(request=request, future=future)
        with self._state_lock:
            if self._correlated_turn_ref != turn_ref or self._pending_approval is not None:
                return PermissionResultDeny(
                    message="Claude approval no longer matches the active ORC turn",
                )
            self._pending_approval = pending
        self._publish(_ClaudeEdgeApprovalRequested(turn_ref, request))
        try:
            return await future
        finally:
            with self._state_lock:
                if self._pending_approval is pending:
                    self._pending_approval = None

    @staticmethod
    def _complete_approval(
        pending: _ClaudePendingApproval,
        result: PermissionResultAllow | PermissionResultDeny,
    ) -> None:
        if not pending.future.done():
            pending.future.set_result(result)

    def _cancel_pending_approval(self, turn_ref: TurnRef, *, message: str) -> None:
        with self._state_lock:
            pending = self._pending_approval
        if pending is None or pending.request.turn_ref != turn_ref:
            return
        self._complete_approval(
            pending,
            PermissionResultDeny(message=message, interrupt=True),
        )

    def _cancel_active_approval(self, message: str) -> None:
        with self._state_lock:
            pending = self._pending_approval
        if pending is not None:
            self._complete_approval(
                pending,
                PermissionResultDeny(message=message, interrupt=True),
            )

    async def _read_lifetime(
        self,
        client: ClaudeSDKClient,
        server_info: Mapping[str, Any],
    ) -> None:
        initialized = False
        async for message in client.receive_messages():
            if isinstance(message, SystemMessage) and message.subtype == "init":
                session_id = str(message.data.get("session_id") or "").strip()
                if not session_id:
                    raise _ClaudeProtocolContractError(
                        "Claude initialization omitted stable session_id"
                    )
                if session_id != self._session_id:
                    raise _ClaudeProtocolContractError(
                        "Claude initialization session did not match preassigned session"
                    )
                if initialized:
                    self._publish(_ClaudeEdgeDuplicateInitialization())
                else:
                    initialized = True
                continue
            with self._state_lock:
                turn_ref = self._correlated_turn_ref
            if turn_ref is not None and not initialized:
                raise _ClaudeProtocolContractError(
                    "Claude turn payload arrived before session initialization"
                )
            if isinstance(message, ResultMessage):
                with self._state_lock:
                    expected_session_id = self._session_id
                if message.session_id != expected_session_id:
                    raise _ClaudeProtocolContractError(
                        "Claude result session did not match active session"
                    )
                if self._is_duplicate_result(message):
                    continue
            with self._state_lock:
                turn_ref = self._correlated_turn_ref
                interrupt_requested = self._interrupt_turn_ref == turn_ref
                if isinstance(message, ResultMessage) and turn_ref is not None:
                    self._correlated_turn_ref = None
                    self._interrupt_turn_ref = None
            if isinstance(message, ResultMessage) and turn_ref is not None:
                self._cancel_pending_approval(
                    turn_ref,
                    message="Claude turn ended before approval resolution",
                )
            if turn_ref is None:
                self._publish(_ClaudeEdgeMessage(message))
            else:
                self._publish(
                    _ClaudeEdgeTurnMessage(
                        turn_ref,
                        message,
                        interrupt_requested=interrupt_requested,
                    )
                )

    def _is_duplicate_result(self, message: ResultMessage) -> bool:
        native_id = str(message.uuid or "").strip()
        if not native_id:
            return False
        rolled_over = False
        with self._state_lock:
            if native_id in self._result_uuids:
                duplicate = True
            else:
                duplicate = False
                if len(self._result_uuid_order) >= self._config.result_uuid_capacity:
                    evicted = self._result_uuid_order.popleft()
                    self._result_uuids.remove(evicted)
                    rolled_over = True
                self._result_uuid_order.append(native_id)
                self._result_uuids.add(native_id)
        if duplicate:
            self._config.logger.event(
                "claude_duplicate_result_ignored",
                provider=ProviderKind.CLAUDE.value,
            )
        elif rolled_over:
            self._config.logger.event(
                "claude_result_uuid_history_rolled_over",
                provider=ProviderKind.CLAUDE.value,
                capacity=self._config.result_uuid_capacity,
            )
        return duplicate

    def _publish_failure(self, exc: BaseException) -> None:
        error = _claude_edge_failure_error(exc)
        if isinstance(exc, _ClaudeAuthPreflightError):
            error_kind = OpenSessionErrorKind.INVALID_REQUEST
        elif isinstance(exc, ProcessError):
            error_kind = OpenSessionErrorKind.PROCESS_EXITED
        else:
            error_kind = OpenSessionErrorKind.STARTUP_FAILED
        with self._state_lock:
            turn_ref = self._correlated_turn_ref
            pending_model = self._pending_model
            self._correlated_turn_ref = None
            self._interrupt_turn_ref = None
            self._pending_model = None
            self._terminal_error = error
        if pending_model is not None:
            self._publish(
                _ClaudeEdgeModelSelectionUncertain(
                    pending_model,
                    _CLAUDE_MODEL_SELECTION_UNCERTAIN_ERROR,
                )
            )
        if turn_ref is not None:
            self._publish(_ClaudeEdgeTurnFailed(turn_ref, error))
        self._publish(_ClaudeEdgeFailure(error=error, error_kind=error_kind))

    def _publish(self, event: _ClaudeEdgeEvent) -> None:
        try:
            self._events.put_nowait(event)
        except Full:
            with self._state_lock:
                error = "Claude provider edge queue capacity exceeded"
                self._terminal_error = error
                self._overflow_error = error
            self._stop_requested.set()
            loop = self._loop
            stop_event = self._stop_event
            if loop is not None and stop_event is not None:
                try:
                    loop.call_soon_threadsafe(stop_event.set)
                except RuntimeError:
                    pass
            return
        with self._state_lock:
            self._progress = True


class ClaudeProviderHost:
    """Owns one ClaudeSDKClient/CLI process and at most one logical session."""

    provider = ProviderKind.CLAUDE
    profile = ProviderProfileId.CLAUDE_NATIVE

    def __init__(self, config: ClaudeProviderConfig) -> None:
        self._config = config
        self._edge: _ClaudeSdkEdge | None = None
        self._pending_open: _ClaudeOpenSessionOperation | None = None
        self._active_port: ClaudeAgentSessionAdapter | None = None
        self._opened_once = False
        self._stopped = False

    def open_session(self, request: OpenSessionRequest) -> OpenSessionOperation:
        if not isinstance(request, (StartSessionRequest, ResumeSessionRequest)):
            raise ValueError("Claude host requires a typed open session request")
        if self._stopped:
            return self._failed_open_operation(
                request,
                OpenSessionError(
                    OpenSessionErrorKind.CANCELLED,
                    "Claude provider host is stopped",
                ),
            )
        if self._pending_open is not None and not self._pending_open.completed:
            return self._failed_open_operation(
                request,
                OpenSessionError(OpenSessionErrorKind.BUSY, "Claude session open is already active"),
            )
        if self._opened_once:
            return self._failed_open_operation(
                request,
                OpenSessionError(
                    OpenSessionErrorKind.BUSY,
                    "dedicated Claude provider host already opened a session",
                ),
            )
        if self._edge is not None:
            return self._failed_open_operation(
                request,
                OpenSessionError(
                    OpenSessionErrorKind.BUSY,
                    "Claude provider cleanup is incomplete; retry StopHost before opening",
                ),
            )
        validation_error = self._validate_open_request(request)
        if validation_error is not None:
            return self._failed_open_operation(request, validation_error)
        resume_info: SDKSessionInfo | None = None
        if isinstance(request, ResumeSessionRequest):
            if (
                request.session_ref.provider != self.provider
                or request.session_ref.profile != self.profile
            ):
                return self._failed_open_operation(
                    request,
                    OpenSessionError(
                        OpenSessionErrorKind.INVALID_REQUEST,
                        "Claude host cannot resume a session owned by another profile",
                    ),
                )
            try:
                assert self._config.runtime_context is not None
                resume_info = self._config.runtime_context.get_session_info(
                    request.session_ref.native_id,
                    directory=request.config.cwd,
                )
            except (OSError, ValueError):
                return self._failed_open_operation(
                    request,
                    OpenSessionError(
                        OpenSessionErrorKind.STARTUP_FAILED,
                        "Claude session catalog is unavailable",
                    ),
                )
            if resume_info is None:
                return self._failed_open_operation(
                    request,
                    OpenSessionError(
                        OpenSessionErrorKind.INVALID_REQUEST,
                        "Claude session is unavailable",
                    ),
                )
        stderr = _BoundedClaudeStderr()
        resume_session_id = (
            request.session_ref.native_id
            if isinstance(request, ResumeSessionRequest)
            else None
        )
        edge = _ClaudeSdkEdge(
            self._config,
            self._options_for(
                request.config,
                session_id=resume_session_id or str(uuid4()),
                resume=resume_session_id,
                stderr=stderr,
            ),
            stderr,
        )
        operation = _ClaudeOpenSessionOperation(
            self,
            request,
            edge,
            resume_info=resume_info,
        )
        self._edge = edge
        self._pending_open = operation
        operation.dispatch()
        return operation

    def _failed_open_operation(
        self,
        request: OpenSessionRequest,
        error: OpenSessionError,
    ) -> _FailedClaudeOpenSessionOperation:
        operation = _FailedClaudeOpenSessionOperation(request, error)
        self._config.logger.event(
            "provider_session_open_failed",
            provider=self.provider.value,
            correlation_id=operation.correlation_id.value,
            error_kind=error.kind.value,
            error=error.message,
        )
        return operation

    def consume_poll_progress(self) -> bool:
        edge = self._edge
        return edge.consume_progress() if edge is not None else False

    def consume_terminal_error(self) -> str:
        edge = self._edge
        return edge.consume_terminal_error() if edge is not None else ""

    def stop_host(self, effect: StopHost) -> StopHostResult:
        if not isinstance(effect, StopHost):
            raise ValueError("Claude provider host stop requires StopHost")
        if self._stopped:
            return StopHostResult(stopped=True)
        errors: list[str] = []
        pending = self._pending_open
        if pending is not None and not pending.completed:
            pending.cancel("Claude provider host stopped during session open")
        port = self._active_port
        if port is not None:
            result = port.close_session(CloseSession())
            if result.error:
                errors.append(result.error)
        else:
            edge = self._edge
            if edge is not None:
                try:
                    edge.stop()
                except Exception as exc:
                    errors.append(_error_text(exc))
        if errors:
            return StopHostResult(stopped=False, error="; ".join(errors))
        self._pending_open = None
        self._active_port = None
        self._edge = None
        self._stopped = True
        self._config.logger.event(
            "agent_backend_stopped",
            provider=self.provider.value,
            role=self._config.role,
        )
        return StopHostResult(stopped=True)

    def _validate_open_request(self, request: OpenSessionRequest) -> OpenSessionError | None:
        backend = request.config.backend or "anthropic"
        if backend != "anthropic":
            return OpenSessionError(
                OpenSessionErrorKind.INVALID_REQUEST,
                f"Claude native supports only model backend anthropic; got {backend}",
            )
        if _has_claude_auth_override({}, self._config.env):
            return OpenSessionError(
                OpenSessionErrorKind.INVALID_REQUEST,
                "Claude local subscription mode rejects credential overrides",
            )
        return None

    def _options_for(
        self,
        session: AgentSessionConfig,
        *,
        session_id: str,
        resume: str | None,
        stderr: _BoundedClaudeStderr,
    ) -> ClaudeAgentOptions:
        return ClaudeAgentOptions(
            cwd=Path(session.cwd) if session.cwd else None,
            model=session.model or None,
            cli_path=self._config.cli_path,
            env=dict(self._config.env or {}),
            setting_sources=[],
            settings=_CLAUDE_PERMISSION_SETTINGS,
            tools={"type": "preset", "preset": "claude_code"},
            allowed_tools=[],
            disallowed_tools=["AskUserQuestion"],
            skills=[],
            mcp_servers={},
            strict_mcp_config=True,
            permission_mode="default",
            agents={},
            resume=resume,
            session_id=None if resume else session_id,
            stderr=stderr.append,
        )

    def _finish_open(
        self,
        operation: _ClaudeOpenSessionOperation,
        edge: _ClaudeSdkEdge,
        connected: _ClaudeEdgeConnected,
    ) -> Opened:
        if self._stopped:
            raise RuntimeError("Claude provider host stopped before session open completed")
        if self._pending_open is operation:
            self._pending_open = None
        session_ref = ProviderSessionRef(
            self.provider,
            connected.session_id,
            self.profile,
        )
        capabilities = claude_capability_snapshot(
            adapter_version=CLAUDE_AGENT_SDK_VERSION,
            runtime_version=connected.runtime_version,
        )
        port = ClaudeAgentSessionAdapter(
            edge=edge,
            session_ref=session_ref,
            capabilities=capabilities,
            model="",
            session_name=(operation.resume_info.summary if operation.resume_info else ""),
            loaded=operation.resume_info is not None,
            created_at=_format_claude_timestamp(
                operation.resume_info.created_at if operation.resume_info else None
            ),
            updated_at=_format_claude_timestamp(
                operation.resume_info.last_modified if operation.resume_info else None
            ),
            preview=(operation.resume_info.first_prompt or "")
            if operation.resume_info
            else "",
            cwd=Path(operation.request.config.cwd or os.getcwd()).resolve(),
            close_handler=lambda: self._close_session(port),
            logger=self._config.logger,
            turn_output_max_chars=self._config.turn_output_max_chars,
        )
        self._active_port = port
        self._opened_once = True
        effective = EffectiveRuntimeSnapshot(
            model="",
            backend="",
        )
        self._config.logger.event(
            "agent_capability_snapshot",
            provider=self.provider.value,
            **capabilities.to_log_fields(),
        )
        self._config.logger.event(
            "agent_backend_started",
            provider=self.provider.value,
            role=self._config.role,
            sdk_version=CLAUDE_AGENT_SDK_VERSION,
            runtime_version=connected.runtime_version,
        )
        self._config.logger.event(
            "provider_session_opened",
            provider=self.provider.value,
            correlation_id=operation.correlation_id.value,
            provider_session_id=session_ref.native_id,
            effective_model=effective.model,
            effective_backend=effective.backend,
        )
        return Opened(
            correlation_id=operation.correlation_id,
            session=port,
            session_ref=session_ref,
            effective=effective,
            capabilities=capabilities,
        )

    def _open_failed(
        self,
        operation: _ClaudeOpenSessionOperation,
        error: OpenSessionError,
    ) -> OpenFailed:
        if self._pending_open is operation:
            self._pending_open = None
        cleanup_succeeded = True
        try:
            operation.edge.stop()
        except Exception as cleanup_error:
            cleanup_succeeded = False
            error = OpenSessionError(
                error.kind,
                f"{error.message}; cleanup failed: {_error_text(cleanup_error)}",
            )
        if cleanup_succeeded and self._edge is operation.edge:
            self._edge = None
        self._config.logger.event(
            "provider_session_open_failed",
            provider=self.provider.value,
            correlation_id=operation.correlation_id.value,
            error_kind=error.kind.value,
            error=error.message,
        )
        return OpenFailed(operation.correlation_id, error)

    def _close_session(self, port: ClaudeAgentSessionAdapter) -> None:
        if self._active_port is not port:
            return
        edge = self._edge
        if edge is not None:
            edge.stop()
        self._active_port = None


class _ClaudeOpenSessionOperation(OpenSessionOperation):
    def __init__(
        self,
        host: ClaudeProviderHost,
        request: OpenSessionRequest,
        edge: _ClaudeSdkEdge,
        *,
        resume_info: SDKSessionInfo | None,
    ) -> None:
        super().__init__(request)
        self._host = host
        self.edge = edge
        self.resume_info = resume_info
        self._deadline = clock.monotonic() + host._config.startup_timeout_sec
        self._dispatched = False

    def dispatch(self) -> None:
        if self._dispatched:
            raise RuntimeError("Claude session open was already dispatched")
        self._dispatched = True
        self._publish_progress(phase="runtime_connecting", message="connecting Claude SDK")
        try:
            self.edge.start()
        except Exception as exc:
            self._complete(
                self._host._open_failed(
                    self,
                    OpenSessionError(OpenSessionErrorKind.STARTUP_FAILED, _error_text(exc)),
                )
            )

    def cancel(self, reason: str) -> None:
        if self.completed:
            return
        self._complete(
            self._host._open_failed(
                self,
                OpenSessionError(OpenSessionErrorKind.CANCELLED, reason),
            )
        )

    def _advance(self) -> None:
        if not self._dispatched:
            return
        if clock.monotonic() >= self._deadline:
            self._complete(
                self._host._open_failed(
                    self,
                    OpenSessionError(
                        OpenSessionErrorKind.STARTUP_TIMEOUT,
                        "Claude provider startup timed out",
                    ),
                )
            )
            return
        overflow_error = self.edge.consume_overflow_error()
        if overflow_error:
            self._complete(
                self._host._open_failed(
                    self,
                    OpenSessionError(OpenSessionErrorKind.STARTUP_FAILED, overflow_error),
                )
            )
            return
        while True:
            pending = self.edge.poll_events(limit=1)
            if not pending:
                break
            event = pending[0]
            if isinstance(event, _ClaudeEdgeConnected):
                self._complete(self._host._finish_open(self, self.edge, event))
                return
            if isinstance(event, _ClaudeEdgeFailure):
                self._complete(
                    self._host._open_failed(
                        self,
                        OpenSessionError(
                            event.error_kind,
                            event.error,
                        ),
                    )
                )
                return
        overflow_error = self.edge.consume_overflow_error()
        if overflow_error:
            self._complete(
                self._host._open_failed(
                    self,
                    OpenSessionError(OpenSessionErrorKind.STARTUP_FAILED, overflow_error),
                )
            )


class _FailedClaudeOpenSessionOperation(OpenSessionOperation):
    def __init__(self, request: OpenSessionRequest, error: OpenSessionError) -> None:
        super().__init__(request)
        self._complete(OpenFailed(self.correlation_id, error))

    def _advance(self) -> None:
        return


class ClaudeAgentSessionAdapter:
    """Provider-neutral text session port over one Claude SDK lifetime reader."""

    def __init__(
        self,
        *,
        edge: _ClaudeSdkEdge,
        session_ref: ProviderSessionRef,
        capabilities: AgentCapabilitySnapshot,
        model: str,
        session_name: str,
        loaded: bool,
        created_at: str,
        updated_at: str,
        preview: str,
        cwd: Path,
        close_handler: Callable[[], None],
        logger: LifecycleLogger,
        turn_output_max_chars: int,
    ) -> None:
        self._edge = edge
        self._session_ref = session_ref
        self._capabilities = capabilities
        self._metadata_epoch = _ClaudeMetadataEpoch(model=model)
        self._pending_metadata = _ClaudePendingMetadata()
        self._session_name = session_name
        self._loaded = loaded
        self._created_at = created_at
        self._updated_at = updated_at
        self._preview = preview
        self._cwd = cwd
        self._close_handler = close_handler
        self._logger = logger
        self._closed = False
        self._failure_emitted = False
        self._active_turn_ref: TurnRef | None = None
        self._active_approval_request: AgentApprovalRequest | None = None
        self._turn_output_max_chars = turn_output_max_chars
        self._output_text = StringIO()
        self._local_events: deque[AgentSessionEvent] = deque()

    def get_capability_snapshot(self) -> AgentCapabilitySnapshot:
        return self._capabilities

    def start_turn(self, effect: StartTurn) -> AgentEffectResult:
        if not isinstance(effect, StartTurn):
            raise ValueError("Claude turn start requires StartTurn")
        unsupported = unsupported_effect_result(effect, self._capabilities)
        if unsupported is not None:
            return unsupported
        if self._closed:
            return AgentEffectResult.rejected("Claude provider session is closed")
        if self._failure_emitted:
            return AgentEffectResult.rejected("Claude provider session is unavailable")
        if self._active_turn_ref is not None:
            return AgentEffectResult.rejected("Claude provider session is busy")
        content: tuple[dict[str, Any], ...] | None = None
        if effect.agent_input.attachments:
            try:
                content = _claude_query_content(self._cwd, effect.agent_input)
            except _ClaudeAttachmentError as exc:
                return AgentEffectResult.rejected(str(exc))
        result = self._edge.submit_turn(
            _ClaudeTurnCommand(
                effect.turn_ref,
                effect.agent_input.text,
                content=content,
            )
        )
        if result.outcome is AgentEffectOutcome.ACCEPTED:
            self._active_turn_ref = effect.turn_ref
            self._reset_output()
            self._pending_metadata = _ClaudePendingMetadata()
        return result

    def try_steer_turn(self, effect: TrySteerTurn) -> AgentEffectResult:
        return self._unsupported(effect)

    def interrupt_turn(self, effect: InterruptTurn) -> AgentEffectResult:
        if not isinstance(effect, InterruptTurn):
            raise ValueError("Claude turn interrupt requires InterruptTurn")
        unsupported = unsupported_effect_result(effect, self._capabilities)
        if unsupported is not None:
            return unsupported
        if self._closed:
            return AgentEffectResult.rejected("Claude provider session is closed")
        if self._failure_emitted:
            return AgentEffectResult.rejected("Claude provider session is unavailable")
        if self._active_turn_ref is None:
            return AgentEffectResult.rejected("Claude provider session has no active turn")
        if self._active_turn_ref != effect.turn_ref:
            return AgentEffectResult.rejected("Claude active turn no longer matches")
        return self._edge.interrupt_turn(_ClaudeInterruptCommand(effect.turn_ref))

    def resolve_approval(self, effect: ResolveApproval) -> ApprovalDecisionResult:
        request = self._active_approval_request
        if request is None:
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected("Claude approval request is no longer active")
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
                AgentEffectResult.rejected("Claude approval request no longer matches")
            )
        try:
            result = self._edge.resolve_approval(effect, expected_request=request)
        except Exception as exc:
            return ApprovalDecisionResult.provider_call_exception(exc)
        if result.outcome is AgentEffectOutcome.ACCEPTED:
            self._active_approval_request = None
        return ApprovalDecisionResult.provider_call(result)

    def set_model(self, effect: SetModel) -> AgentEffectResult:
        if not isinstance(effect, SetModel):
            raise ValueError("Claude model selection requires SetModel")
        unsupported = unsupported_effect_result(effect, self._capabilities)
        if unsupported is not None:
            return unsupported
        if self._closed:
            return AgentEffectResult.rejected("Claude provider session is closed")
        if self._failure_emitted:
            return AgentEffectResult.rejected("Claude provider session is unavailable")
        if self._active_turn_ref is not None:
            return AgentEffectResult.rejected("Claude provider session is busy")
        model = _normalized_claude_model(effect.model)
        if not model:
            return AgentEffectResult.rejected("Claude model identifier is invalid")
        return self._edge.set_model(_ClaudeSetModelCommand(model))

    def set_effort(self, effect: SetEffort) -> AgentEffectResult:
        return self._unsupported(effect)

    def close_session(self, effect: CloseSession) -> AgentEffectResult:
        if not isinstance(effect, CloseSession):
            raise ValueError("Claude session close requires CloseSession")
        if self._closed:
            return AgentEffectResult.completed()
        try:
            self._close_handler()
        except Exception as exc:
            return AgentEffectResult.rejected(_error_text(exc))
        self._closed = True
        self._active_turn_ref = None
        self._active_approval_request = None
        self._reset_output()
        return AgentEffectResult.completed()

    def refresh_inspection(self, effect: RefreshInspection) -> AgentEffectResult:
        if not isinstance(effect, RefreshInspection):
            raise ValueError("Claude inspection requires RefreshInspection")
        if self._closed:
            return AgentEffectResult.rejected("Claude provider session is closed")
        if len(self._local_events) >= _CLAUDE_EDGE_QUEUE_CAPACITY:
            return AgentEffectResult.rejected("Claude local event queue capacity exceeded")
        self._local_events.append(
            InspectionCompleted(effect.inspection_ref, self.get_local_projection())
        )
        return AgentEffectResult.accepted()

    def get_local_projection(self) -> AgentRuntimeProjection:
        metadata = self._metadata_epoch
        return AgentRuntimeProjection(
            capabilities=self._capabilities,
            session=AgentSessionSnapshot(
                SnapshotAvailability.AVAILABLE,
                session_ref=self._session_ref,
                name=self._session_name,
                status=(
                    "closed"
                    if self._closed
                    else "failed"
                    if self._failure_emitted
                    else "running"
                    if self._active_turn_ref is not None
                    else "idle"
                ),
                backend=metadata.backend,
                model=metadata.model,
                loaded=self._loaded,
                created_at=self._created_at,
                updated_at=self._updated_at,
                preview=self._preview,
            ),
            work=AgentWorkSnapshot(SnapshotAvailability.UNSUPPORTED),
            context=(
                AgentContextSnapshot(
                    SnapshotAvailability.ERROR,
                    error=metadata.context_error,
                )
                if metadata.context_error
                else AgentContextSnapshot(
                    SnapshotAvailability.AVAILABLE,
                    used_tokens=metadata.used_tokens,
                    model_context_window=metadata.context_window,
                )
                if metadata.used_tokens is not None
                and metadata.context_window is not None
                else AgentContextSnapshot(SnapshotAvailability.UNKNOWN)
            ),
            background=AgentBackgroundSnapshot(SnapshotAvailability.UNSUPPORTED),
        )

    def poll_events(self) -> list[AgentSessionEvent]:
        events: list[AgentSessionEvent] = list(self._local_events)
        self._local_events.clear()
        for event in self._edge.poll_events():
            if isinstance(event, _ClaudeEdgeTurnStarted):
                if self._is_active(event.turn_ref):
                    events.append(TurnStarted(event.turn_ref))
                else:
                    self._log_stale_turn_event(event.turn_ref, "turn_started")
                continue
            if isinstance(event, _ClaudeEdgeTurnMessage):
                if not self._is_active(event.turn_ref):
                    self._log_stale_turn_event(event.turn_ref, type(event.message).__name__)
                    continue
                events.extend(
                    self._map_turn_message(
                        event.turn_ref,
                        event.message,
                        interrupt_requested=event.interrupt_requested,
                    )
                )
                continue
            if isinstance(event, _ClaudeEdgeTurnFailed):
                if self._is_active(event.turn_ref):
                    self._clear_active_turn(event.turn_ref)
                    events.append(TurnFailed(event.turn_ref, event.error))
                else:
                    self._log_stale_turn_event(event.turn_ref, "turn_failed")
                continue
            if isinstance(event, _ClaudeEdgeApprovalRequested):
                if self._is_active(event.turn_ref):
                    if self._active_approval_request is None:
                        self._active_approval_request = event.request
                        events.append(
                            ApprovalRequested(
                                event.turn_ref,
                                event.request.approval_ref,
                                event.request,
                            )
                        )
                    else:
                        self._logger.event(
                            "claude_concurrent_approval_request_rejected",
                            provider=ProviderKind.CLAUDE.value,
                            turn_ref_sequence=event.turn_ref.sequence,
                        )
                else:
                    self._log_stale_turn_event(event.turn_ref, "approval_requested")
                continue
            if isinstance(event, _ClaudeEdgeModelSelectionAccepted):
                events.append(ModelSelectionAccepted(event.requested_model))
                continue
            if isinstance(event, _ClaudeEdgeModelSelectionFailed):
                events.append(
                    ModelSelectionFailed(event.requested_model, event.error)
                )
                continue
            if isinstance(event, _ClaudeEdgeModelSelectionUncertain):
                events.append(
                    ModelSelectionUncertain(event.requested_model, event.error)
                )
                continue
            if isinstance(event, _ClaudeEdgeFailure):
                if not self._failure_emitted and not self._closed:
                    events.append(SessionFailed(event.error))
                    self._failure_emitted = True
                continue
            if isinstance(event, _ClaudeEdgeDuplicateInitialization):
                self._logger.event(
                    "claude_duplicate_initialization_ignored",
                    provider=ProviderKind.CLAUDE.value,
                )
                continue
            if isinstance(event, _ClaudeEdgeMessage):
                self._logger.event(
                    "claude_uncorrelated_session_message_ignored",
                    provider=ProviderKind.CLAUDE.value,
                    message_type=type(event.message).__name__[:128],
                )
        overflow_error = self._edge.consume_session_overflow_error()
        if overflow_error and not self._failure_emitted:
            turn_ref = self._active_turn_ref
            if turn_ref is not None:
                self._clear_active_turn(turn_ref)
                events.append(TurnFailed(turn_ref, overflow_error))
            events.append(SessionFailed(overflow_error))
            self._failure_emitted = True
        return events

    def _map_turn_message(
        self,
        turn_ref: TurnRef,
        message: object,
        *,
        interrupt_requested: bool,
    ) -> list[AgentSessionEvent]:
        if isinstance(message, AssistantMessage):
            self._observe_assistant_metadata(message)
            events: list[AgentSessionEvent] = []
            for block in message.content:
                if isinstance(block, TextBlock):
                    if self._output_text.tell() + len(block.text) > self._turn_output_max_chars:
                        events.extend(self._output_limit_failure(turn_ref))
                        return events
                    self._output_text.write(block.text)
                    events.append(OutputDelta(turn_ref, block.text))
                elif isinstance(block, (ToolUseBlock, ToolResultBlock)):
                    tool_kind = (
                        "bash"
                        if isinstance(block, ToolUseBlock) and block.name == "Bash"
                        else "other"
                    )
                    self._logger.event(
                        "claude_autonomous_tool_event_observed",
                        provider=ProviderKind.CLAUDE.value,
                        turn_ref_sequence=turn_ref.sequence,
                        block_type=type(block).__name__,
                        tool_kind=tool_kind,
                    )
            return events
        if isinstance(message, UserMessage):
            if isinstance(message.content, list) and any(
                isinstance(block, ToolResultBlock) for block in message.content
            ):
                self._logger.event(
                    "claude_autonomous_tool_event_observed",
                    provider=ProviderKind.CLAUDE.value,
                    turn_ref_sequence=turn_ref.sequence,
                    block_type="UserToolResult",
                )
            return []
        if not isinstance(message, ResultMessage):
            self._logger.event(
                "claude_turn_message_ignored",
                provider=ProviderKind.CLAUDE.value,
                turn_ref_sequence=turn_ref.sequence,
                message_type=type(message).__name__[:128],
            )
            return []
        self._observe_result_metadata(message)
        output_text = self._output_text.getvalue()
        if message.terminal_reason in _CLAUDE_INTERRUPTED_TERMINAL_REASONS:
            self._clear_active_turn(turn_ref)
            if interrupt_requested:
                return [TurnInterrupted(turn_ref)]
            return [TurnFailed(turn_ref, "Claude turn was interrupted outside ORC control")]
        if message.is_error:
            self._clear_active_turn(turn_ref)
            return [
                TurnFailed(
                    turn_ref,
                    _claude_result_error(message),
                    RawProviderErrorPayload.from_json_object(asdict(message)),
                )
            ]
        if not output_text:
            output_text = str(message.result or "")
            if len(output_text) > self._turn_output_max_chars:
                return self._output_limit_failure(turn_ref)
        self._clear_active_turn(turn_ref)
        return [TurnCompleted(turn_ref, AgentOutput(output_text))]

    def _output_limit_failure(self, turn_ref: TurnRef) -> list[AgentSessionEvent]:
        error = (
            "Claude turn output limit exceeded "
            f"({self._turn_output_max_chars} characters)"
        )
        self._clear_active_turn(turn_ref)
        self._failure_emitted = True
        return [TurnFailed(turn_ref, error), SessionFailed(error)]

    def _is_active(self, turn_ref: TurnRef) -> bool:
        return self._active_turn_ref == turn_ref and not self._closed

    def _clear_active_turn(self, turn_ref: TurnRef) -> None:
        if self._active_turn_ref == turn_ref:
            self._active_turn_ref = None
            self._active_approval_request = None
            self._pending_metadata = _ClaudePendingMetadata()
            self._reset_output()

    def _reset_output(self) -> None:
        self._output_text = StringIO()

    def _observe_assistant_metadata(self, message: AssistantMessage) -> None:
        model = _normalized_claude_model(message.model)
        if model:
            self._pending_metadata.model = model
        try:
            used_tokens = _claude_usage_total(message.usage)
        except ValueError as exc:
            self._pending_metadata.used_tokens = None
            self._pending_metadata.context_error = _bounded_claude_diagnostic(
                exc,
                max_chars=256,
            )
            self._logger.event(
                "claude_usage_projection_rejected",
                provider=ProviderKind.CLAUDE.value,
                error=self._pending_metadata.context_error,
            )
            return
        if used_tokens is not None:
            self._pending_metadata.used_tokens = used_tokens
            self._pending_metadata.context_error = ""

    def _observe_result_metadata(self, message: ResultMessage) -> None:
        pending = self._pending_metadata
        try:
            metadata = _claude_result_usage_metadata(
                message,
                effective_model=pending.model,
            )
        except ValueError as exc:
            error = _bounded_claude_diagnostic(exc, max_chars=256)
            self._metadata_epoch = _ClaudeMetadataEpoch(
                model=pending.model,
                context_error=error,
            )
            self._logger.event(
                "claude_usage_projection_rejected",
                provider=ProviderKind.CLAUDE.value,
                error=error,
            )
            return
        if metadata is None:
            self._metadata_epoch = _ClaudeMetadataEpoch(
                model=pending.model,
                context_error=pending.context_error,
            )
            return
        model, backend, used_tokens, context_window = metadata
        correlated_used_tokens = (
            used_tokens if used_tokens is not None else pending.used_tokens
        )
        if not backend or context_window is None:
            correlated_used_tokens = None
            context_window = None
        self._metadata_epoch = _ClaudeMetadataEpoch(
            model=model or pending.model,
            backend=backend,
            used_tokens=correlated_used_tokens,
            context_window=context_window,
        )

    def _log_stale_turn_event(self, turn_ref: TurnRef, event_type: str) -> None:
        self._logger.event(
            "claude_stale_turn_event_ignored",
            provider=ProviderKind.CLAUDE.value,
            turn_ref_sequence=turn_ref.sequence,
            event_type=event_type[:128],
        )

    def _unsupported(self, effect: AgentSessionEffect) -> AgentEffectResult:
        result = unsupported_effect_result(effect, self._capabilities)
        if result is None:
            raise AssertionError(f"Claude adapter unexpectedly supports {type(effect).__name__}")
        return result


def register_claude_provider(
    registry: ProviderRegistry,
    *,
    cli_path: Path | None = None,
    env: Mapping[str, str] | None = None,
    ambient_environment: Mapping[str, str] | None = None,
) -> None:
    runtime_context = _ClaudeRuntimeContext.capture(
        ambient_environment=(
            os.environ if ambient_environment is None else ambient_environment
        ),
        overrides=env,
    )

    def list_sessions(request: ListProviderSessions):
        try:
            sessions = runtime_context.list_sessions(
                directory=request.cwd,
                limit=request.limit,
            )
        except (OSError, ValueError):
            return ProviderSessionsListFailed(
                ProviderSessionCatalogErrorKind.PROVIDER_FAILED,
                "Claude session catalog is unavailable",
            )
        return ProviderSessionsListed(
            tuple(
                ProviderSessionCatalogEntry(
                    session_ref=ProviderSessionRef(
                        ProviderKind.CLAUDE,
                        session.session_id,
                        ProviderProfileId.CLAUDE_NATIVE,
                    ),
                    name=session.summary,
                    cwd=str(session.cwd or ""),
                    adapter_name="claude-agent-sdk-python",
                    adapter_version=CLAUDE_AGENT_SDK_VERSION,
                    created_at=_format_claude_timestamp(session.created_at),
                    updated_at=_format_claude_timestamp(session.last_modified),
                    preview=str(session.first_prompt or ""),
                )
                for session in sessions
            )
        )

    def compose(
        context: ProviderHostRootConfiguration,
        _initial_session_config: AgentSessionConfig,
    ) -> ComposedProviderHost:
        config = ClaudeProviderConfig(
            logger=context.logger,
            cli_path=cli_path,
            env=runtime_context.environment,
            runtime_context=runtime_context,
            role=context.role,
            startup_timeout_sec=context.startup_timeout_sec,
            rpc_timeout_sec=context.rpc_timeout_sec,
            edge_thread_name=context.edge_thread_name.replace("jsonrpc", "claude-sdk"),
        )
        return ComposedProviderHost(
            host=ClaudeProviderHost(config),
            initial_capabilities=claude_capability_snapshot(
                adapter_version=CLAUDE_AGENT_SDK_VERSION,
            ),
        )

    registry.register(
        ProviderExecutionProfile(ProviderKind.CLAUDE, ProviderProfileId.CLAUDE_NATIVE),
        ClaudeProviderConfig,
        ClaudeProviderHost,
        composer=compose,
        session_catalog_loader=list_sessions,
    )


def register_provider(
    registry: ProviderRegistry,
    context: ProviderRegistrationContext,
) -> None:
    """Common production registration entrypoint discovered by composition."""

    register_claude_provider(
        registry,
        cli_path=context.cli_path_for(ProviderKind.CLAUDE),
        env=context.runtime_environment_for(ProviderKind.CLAUDE),
        ambient_environment={},
    )


def _error_text(exc: BaseException) -> str:
    return _bounded_claude_diagnostic(str(exc).strip() or type(exc).__name__)


def _format_claude_timestamp(value: int | None) -> str:
    if value is None or isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return ""
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def _claude_edge_failure_error(exc: BaseException) -> str:
    if isinstance(
        exc,
        (
            _ClaudeAuthPreflightError,
            _ClaudeProtocolContractError,
            _ClaudeModelSelectionUncertainError,
        ),
    ):
        return str(exc)
    if isinstance(exc, ProcessError):
        return _CLAUDE_SDK_PROCESS_FAILURE_ERROR
    return _CLAUDE_SDK_SESSION_FAILURE_ERROR


def _claude_result_error(message: ResultMessage) -> str:
    errors = tuple(
        value.strip()
        for value in (message.errors or [])
        if isinstance(value, str) and value.strip()
    )
    if errors:
        return _bounded_claude_diagnostic("; ".join(errors))
    result = str(message.result or "").strip()
    if result:
        return _bounded_claude_diagnostic(result)
    subtype = str(message.subtype or "").strip()
    return f"Claude turn failed: {subtype or 'provider error'}"
