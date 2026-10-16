"""ACP stable-v1 types and bounded protocol normalization."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import base64
import json
import math
import os
from pathlib import Path
import re
import secrets
import stat
import subprocess
import threading
from types import MappingProxyType
from typing import Mapping

from orchestrator.agent_identity import (
    ApprovalRef,
    ElicitationRef,
    TurnRef,
)
from orchestrator.agent_io import AgentAttachmentRef, AgentInput
from orchestrator.agent_provider import validate_timeout_seconds
from orchestrator.agent_session import (
    AgentApprovalKind,
    AgentApprovalRequest,
    AgentFormField,
    AgentFormRequest,
)
from orchestrator.processes import LifecycleLogger
from orchestrator.runtime_profile_catalog import AcpRuntimeProfileConfig

_CONFIG_OPTION_MAX_COUNT = 128
_CONFIG_OPTION_VALUE_MAX_COUNT = 256
_CONFIG_OPTION_VALUE_TOTAL_MAX_COUNT = 512
_CALLBACK_OPTION_MAX_COUNT = 16
_APPROVAL_DISPLAY_TITLE_MAX_CHARS = 256
_APPROVAL_REVIEW_MAX_CHARS = 2048
_APPROVAL_REVIEW_DECLINE_NOTICE = (
    "Provider permission request was declined because its effect details "
    "exceed the shared review limit."
)
_FORM_FIELD_MAX_COUNT = 16
_ATTACHMENT_MAX_COUNT = 16
_ATTACHMENT_MAX_BYTES = 8 * 1024 * 1024
_ATTACHMENT_TOTAL_MAX_BYTES = 16 * 1024 * 1024
_TEXT_RESOURCE_MAX_CHARS = 1024 * 1024
_FILESYSTEM_TEXT_MAX_BYTES = 1024 * 1024
_FILESYSTEM_LINE_MAX = 1_000_000
_TERMINAL_ARG_MAX_COUNT = 128
_TERMINAL_ENV_MAX_COUNT = 16
_TERMINAL_ENV_VALUE_MAX_CHARS = 1024
_TERMINAL_ENV_TOTAL_MAX_BYTES = 8192
_TERMINAL_TEXT_MAX_CHARS = 4096
_TERMINAL_OUTPUT_MAX_BYTES = 1024 * 1024
_IMAGE_MEDIA_TYPES = frozenset(
    {"image/gif", "image/jpeg", "image/png", "image/webp"}
)
_TEXT_RESOURCE_MEDIA_TYPES = frozenset(
    {
        "application/json",
        "application/xml",
        "application/x-yaml",
        "text/csv",
        "text/markdown",
        "text/plain",
        "text/xml",
        "text/yaml",
    }
)
_SENSITIVE_FORM_TOKENS = frozenset(
    {
        "password",
        "secret",
        "token",
        "api_key",
        "apikey",
        "private_key",
        "recovery_code",
        "credit_card",
    }
)
_ACP_ERROR_CATEGORIES = {
    -32700: "parse_error",
    -32600: "invalid_request",
    -32601: "method_not_found",
    -32602: "invalid_params",
    -32603: "internal_error",
    -32800: "request_cancelled",
    -32000: "auth_required",
    -32002: "resource_not_found",
}
_ACP_DIAGNOSTIC_SERVICES = frozenset({"session"})
_ACP_DIAGNOSTIC_ERROR_NAMES = frozenset(
    {
        "CatalogNotReady",
        "ServerUnavailable",
        "provider.error",
        "provider.invalid-output",
        "provider.invalid-request",
        "provider.no-route",
        "provider.quota",
        "provider.rate-limit",
        "provider.timeout",
        "provider.transport",
        "provider.unknown",
        "provider.unsupported-operation",
    }
)


class _AcpContractError(RuntimeError):
    pass


class _AcpConnectionError(RuntimeError):
    pass


@dataclass(frozen=True)
class _AcpInboundFrame:
    kind: str
    request_id: int | None = None
    method: str | None = None


def _classify_inbound_frame(frame: dict[str, object]) -> _AcpInboundFrame:
    if frame.get("jsonrpc") != "2.0":
        raise _AcpContractError("ACP frame has invalid JSON-RPC version")

    has_id = "id" in frame
    request_id = frame.get("id")
    has_method = "method" in frame
    method = frame.get("method")
    response_fields = int("result" in frame) + int("error" in frame)

    if response_fields:
        if (
            response_fields != 1
            or not has_id
            or type(request_id) is not int
            or has_method
            or "params" in frame
        ):
            raise _AcpContractError("ACP response shape is invalid")
        return _AcpInboundFrame("response", request_id=request_id)

    if not has_method or not isinstance(method, str) or not method:
        raise _AcpContractError("ACP frame shape is invalid")
    if "params" in frame and not isinstance(frame.get("params"), (dict, list)):
        raise _AcpContractError("ACP call params are invalid")
    if has_id:
        if type(request_id) is not int:
            raise _AcpContractError("ACP request ID is invalid")
        return _AcpInboundFrame("request", request_id=request_id, method=method)
    return _AcpInboundFrame("notification", method=method)


@dataclass(frozen=True)
class _NegotiatedPromptCapabilities:
    image: bool = False
    audio: bool = False
    embedded_context: bool = False


def _read_workspace_attachment(
    workspace: str,
    attachment: AgentAttachmentRef,
) -> tuple[Path, bytes]:
    root = Path(os.path.abspath(workspace))
    if not root.is_dir():
        raise _AcpContractError("ACP attachment workspace is invalid")
    parts = Path(attachment.relative_path).parts
    directory_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in parts[:-1]:
            next_fd = os.open(
                part,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=directory_fd,
            )
            os.close(directory_fd)
            directory_fd = next_fd
        file_fd = os.open(
            parts[-1],
            os.O_RDONLY | os.O_NOFOLLOW,
            dir_fd=directory_fd,
        )
        try:
            before = os.fstat(file_fd)
            if not stat.S_ISREG(before.st_mode):
                raise _AcpContractError("ACP attachment is not a regular file")
            if (
                before.st_size != attachment.size_bytes
                or before.st_size > _ATTACHMENT_MAX_BYTES
            ):
                raise _AcpContractError("ACP attachment size is invalid")
            data = bytearray()
            while len(data) <= _ATTACHMENT_MAX_BYTES:
                chunk = os.read(file_fd, min(64 * 1024, _ATTACHMENT_MAX_BYTES + 1 - len(data)))
                if not chunk:
                    break
                data.extend(chunk)
            after = os.fstat(file_fd)
            if (
                len(data) != before.st_size
                or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            ):
                raise _AcpContractError("ACP attachment changed during preflight")
        finally:
            os.close(file_fd)
    except (FileNotFoundError, NotADirectoryError, PermissionError, OSError) as exc:
        raise _AcpContractError("ACP attachment path is invalid") from exc
    finally:
        os.close(directory_fd)
    return root.joinpath(*parts), bytes(data)


def _prepare_prompt_blocks(
    agent_input: AgentInput,
    *,
    workspace: str,
    negotiated: _NegotiatedPromptCapabilities,
) -> tuple[dict[str, object], ...]:
    attachments = tuple(agent_input.attachments)
    if len(attachments) > _ATTACHMENT_MAX_COUNT:
        raise _AcpContractError("ACP attachment count exceeds capacity")
    blocks: list[dict[str, object]] = [
        {"type": "text", "text": agent_input.text}
    ]
    total_bytes = 0
    for attachment in attachments:
        path, data = _read_workspace_attachment(workspace, attachment)
        total_bytes += len(data)
        if total_bytes > _ATTACHMENT_TOTAL_MAX_BYTES:
            raise _AcpContractError("ACP attachment payload exceeds capacity")
        media_type = attachment.media_type.strip().lower()
        if media_type in _IMAGE_MEDIA_TYPES:
            if not negotiated.image:
                raise _AcpContractError("ACP image input is unsupported")
            blocks.append(
                {
                    "type": "image",
                    "mimeType": media_type,
                    "data": base64.b64encode(data).decode("ascii"),
                }
            )
            continue
        if media_type in _TEXT_RESOURCE_MEDIA_TYPES:
            if not negotiated.embedded_context:
                raise _AcpContractError("ACP embedded resource input is unsupported")
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise _AcpContractError("ACP text attachment is not UTF-8") from exc
            if len(text) > _TEXT_RESOURCE_MAX_CHARS:
                raise _AcpContractError("ACP text attachment exceeds capacity")
            blocks.append(
                {
                    "type": "resource",
                    "resource": {
                        "uri": path.as_uri(),
                        "mimeType": media_type,
                        "text": text,
                    },
                }
            )
            continue
        raise _AcpContractError("ACP attachment media type is unsupported")
    return tuple(blocks)


def _open_workspace_parent(
    workspace: str,
    raw_path: str,
) -> tuple[int, str]:
    root = Path(os.path.abspath(workspace))
    path = Path(raw_path)
    if not path.is_absolute() or not root.is_dir():
        raise _AcpContractError("ACP client path is invalid")
    normalized = Path(os.path.abspath(path))
    try:
        relative = normalized.relative_to(root)
    except ValueError as exc:
        raise _AcpContractError("ACP client path is outside workspace") from exc
    if not relative.parts:
        raise _AcpContractError("ACP client path must name a file")
    directory_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in relative.parts[:-1]:
            next_fd = os.open(
                part,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=directory_fd,
            )
            os.close(directory_fd)
            directory_fd = next_fd
    except (FileNotFoundError, NotADirectoryError, PermissionError, OSError) as exc:
        os.close(directory_fd)
        raise _AcpContractError("ACP client path is invalid") from exc
    return directory_fd, relative.parts[-1]


def _read_workspace_text(
    workspace: str,
    raw_path: str,
    *,
    line: int | None,
    limit: int | None,
) -> str:
    directory_fd, name = _open_workspace_parent(workspace, raw_path)
    try:
        file_fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory_fd)
        try:
            before = os.fstat(file_fd)
            if not stat.S_ISREG(before.st_mode) or before.st_size > _FILESYSTEM_TEXT_MAX_BYTES:
                raise _AcpContractError("ACP filesystem read target is invalid")
            data = bytearray()
            while len(data) <= _FILESYSTEM_TEXT_MAX_BYTES:
                chunk = os.read(
                    file_fd,
                    min(64 * 1024, _FILESYSTEM_TEXT_MAX_BYTES + 1 - len(data)),
                )
                if not chunk:
                    break
                data.extend(chunk)
            after = os.fstat(file_fd)
            if (
                len(data) != before.st_size
                or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            ):
                raise _AcpContractError("ACP filesystem read changed during access")
        finally:
            os.close(file_fd)
    except (FileNotFoundError, NotADirectoryError, PermissionError, OSError) as exc:
        raise _AcpContractError("ACP filesystem read failed") from exc
    finally:
        os.close(directory_fd)
    try:
        text = bytes(data).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _AcpContractError("ACP filesystem read is not UTF-8") from exc
    if line is None and limit is None:
        return text
    lines = text.splitlines(keepends=True)
    start = (line or 1) - 1
    return "".join(lines[start : start + limit] if limit is not None else lines[start:])


def _write_workspace_text(
    workspace: str,
    raw_path: str,
    content: str,
) -> None:
    encoded = content.encode("utf-8")
    if len(encoded) > _FILESYSTEM_TEXT_MAX_BYTES:
        raise _AcpContractError("ACP filesystem write exceeds capacity")
    directory_fd, name = _open_workspace_parent(workspace, raw_path)
    temporary_name = ""
    temporary_fd: int | None = None
    temporary_owned = False
    try:
        try:
            existing = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            existing = None
        if existing is not None and (
            stat.S_ISLNK(existing.st_mode) or not stat.S_ISREG(existing.st_mode)
        ):
            raise _AcpContractError("ACP filesystem write target is invalid")
        for _ in range(8):
            temporary_name = f".orc-acp-{secrets.token_hex(16)}.tmp"
            try:
                temporary_fd = os.open(
                    temporary_name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=directory_fd,
                )
            except FileExistsError:
                continue
            temporary_owned = True
            break
        if temporary_fd is None:
            raise _AcpContractError("ACP filesystem temporary file is unavailable")
        offset = 0
        while offset < len(encoded):
            offset += os.write(temporary_fd, encoded[offset:])
        os.fsync(temporary_fd)
        os.close(temporary_fd)
        temporary_fd = None
        os.replace(
            temporary_name,
            name,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
        )
        temporary_owned = False
    except (PermissionError, OSError) as exc:
        raise _AcpContractError("ACP filesystem write failed") from exc
    finally:
        if temporary_fd is not None:
            os.close(temporary_fd)
        if temporary_owned:
            try:
                os.unlink(temporary_name, dir_fd=directory_fd)
            except FileNotFoundError:
                pass
        os.close(directory_fd)


@dataclass(frozen=True)
class _FilesystemReadEffect:
    workspace: str
    path: str
    line: int | None
    limit: int | None


@dataclass(frozen=True)
class _FilesystemWriteEffect:
    workspace: str
    path: str
    content: str


@dataclass(frozen=True)
class _TerminalCreateEffect:
    workspace: str
    command: str
    args: tuple[str, ...]
    cwd: str
    env: tuple[tuple[str, str], ...]
    output_byte_limit: int


_ClientServiceEffect = (
    _FilesystemReadEffect | _FilesystemWriteEffect | _TerminalCreateEffect
)


@dataclass
class _OwnedTerminal:
    terminal_id: str
    native_session_id: str
    turn_ref: TurnRef
    process: subprocess.Popen[bytes]
    output_byte_limit: int
    output_chunks: deque[bytes] = field(default_factory=deque)
    output_bytes: int = 0
    truncated: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    reader: threading.Thread | None = field(default=None, repr=False)

    def append_output(self, chunk: bytes) -> None:
        with self.lock:
            self.output_chunks.append(bytes(chunk))
            self.output_bytes += len(chunk)
            overflow = self.output_bytes - self.output_byte_limit
            if overflow <= 0:
                return
            self.truncated = True
            while overflow > 0:
                first = self.output_chunks[0]
                if len(first) <= overflow:
                    self.output_chunks.popleft()
                    self.output_bytes -= len(first)
                    overflow -= len(first)
                    continue
                self.output_chunks[0] = first[overflow:]
                self.output_bytes -= overflow
                overflow = 0

    def snapshot(self) -> tuple[str, bool]:
        with self.lock:
            data = b"".join(self.output_chunks)
            truncated = self.truncated
        if truncated:
            while data and data[0] & 0xC0 == 0x80:
                data = data[1:]
        return data.decode("utf-8", errors="replace"), truncated


@dataclass(frozen=True)
class _ConfigOption:
    config_id: str
    category: str
    option_type: str
    current_value: str | bool
    values: tuple[str, ...] = ()


@dataclass(frozen=True)
class _ConfigSnapshot:
    options: tuple[_ConfigOption, ...] = ()
    known: bool = False

    def by_category(self, category: str) -> _ConfigOption | None:
        matches = tuple(option for option in self.options if option.category == category)
        return matches[0] if len(matches) == 1 else None


@dataclass(frozen=True)
class _SessionUsage:
    used_tokens: int
    context_window: int
    cost_amount: int | float | None = None
    cost_currency: str = ""


@dataclass(frozen=True)
class _EffectiveSessionConfig:
    model: str
    backend: str
    model_config_id: str = ""
    enabled_models: frozenset[str] = frozenset()
    config_snapshot: _ConfigSnapshot = _ConfigSnapshot()


@dataclass(frozen=True)
class _AcpFailureDiagnostic:
    error_code: int | None
    error_category: str
    service: str
    error_name: str


@dataclass(frozen=True)
class _NormalizedPermission:
    request: AgentApprovalRequest | None
    approval_ref: ApprovalRef
    option_ids: Mapping[str, str]
    tool_call_id: str
    review: "_PermissionReview"


@dataclass(frozen=True)
class _PermissionReview:
    reviewable: bool
    observed_size: int
    limit: int = _APPROVAL_REVIEW_MAX_CHARS


@dataclass(frozen=True)
class _NormalizedForm:
    request: AgentFormRequest


def _allowlisted_diagnostic(value: object, allowed: frozenset[str]) -> str:
    if not isinstance(value, str):
        return "missing"
    return value if value in allowed else "other"


def _acp_failure_diagnostic(frame: Mapping[str, object]) -> _AcpFailureDiagnostic:
    error = frame.get("error")
    code = error.get("code") if isinstance(error, dict) else None
    normalized_code = code if type(code) is int and abs(code) <= 1_000_000 else None
    data = error.get("data") if isinstance(error, dict) else None
    return _AcpFailureDiagnostic(
        error_code=normalized_code,
        error_category=_ACP_ERROR_CATEGORIES.get(normalized_code, "unknown"),
        service=_allowlisted_diagnostic(
            data.get("service") if isinstance(data, dict) else None,
            _ACP_DIAGNOSTIC_SERVICES,
        ),
        error_name=_allowlisted_diagnostic(
            data.get("errorName") if isinstance(data, dict) else None,
            _ACP_DIAGNOSTIC_ERROR_NAMES,
        ),
    )


def _bounded_string(value: object, *, field_name: str, limit: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise _AcpContractError(f"ACP {field_name} is invalid")
    return value.strip()


def _optional_bounded_string(value: object, *, field_name: str, limit: int) -> str:
    if value is None:
        return ""
    if not isinstance(value, str) or len(value) > limit:
        raise _AcpContractError(f"ACP {field_name} is invalid")
    return value.strip()


def _required_frame_bounded_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _AcpContractError(f"ACP {field_name} is invalid")
    return value.strip()


def _optional_frame_bounded_string(value: object, *, field_name: str) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise _AcpContractError(f"ACP {field_name} is invalid")
    return value.strip()


def _permission_display_title(title: str) -> str:
    suffix = "... [display title shortened]"
    if len(title) <= _APPROVAL_DISPLAY_TITLE_MAX_CHARS:
        return title
    prefix_length = _APPROVAL_DISPLAY_TITLE_MAX_CHARS - len(suffix)
    return f"{title[:prefix_length]}{suffix}"


def _permission_review_payload(
    *,
    title: str,
    tool_kind: str,
    raw_input: Mapping[str, object],
    locations: list[object],
    command: str,
) -> str:
    effect: dict[str, object] = {
        "kind": tool_kind or "unknown",
        "locations": locations,
        "rawInput": raw_input,
    }
    if tool_kind == "execute" and not command:
        # Execute without a structured command still relies on the complete title;
        # never substitute its shortened display form as the reviewed effect.
        effect["title"] = title
    return json.dumps(
        effect,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _canonical_sensitive_text(*values: str) -> str:
    separated = " ".join(
        re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", value) for value in values
    )
    return " ".join(re.findall(r"[a-z0-9]+", separated.casefold()))


def _normalize_permission_request(
    params: object,
    *,
    turn_ref: TurnRef,
) -> _NormalizedPermission:
    if not isinstance(params, dict):
        raise _AcpContractError("ACP permission params are invalid")
    tool_call = params.get("toolCall")
    options = params.get("options")
    if not isinstance(tool_call, dict) or not isinstance(options, list):
        raise _AcpContractError("ACP permission request is incomplete")
    if not options or len(options) > _CALLBACK_OPTION_MAX_COUNT:
        raise _AcpContractError("ACP permission options are invalid")
    option_ids: dict[str, str] = {}
    kind_to_decision = {
        "allow_once": "accept",
        "allow_always": "always_allow",
        "reject_once": "decline",
        "reject_always": "persistent_decline",
    }
    opaque_ids: set[str] = set()
    for option in options:
        if not isinstance(option, dict):
            raise _AcpContractError("ACP permission option is invalid")
        option_id = _bounded_string(
            option.get("optionId"), field_name="permission option ID", limit=256
        )
        option_kind = _bounded_string(
            option.get("kind"), field_name="permission option kind", limit=64
        )
        decision = kind_to_decision.get(option_kind)
        if decision is None or decision in option_ids or option_id in opaque_ids:
            raise _AcpContractError("ACP permission options are ambiguous")
        option_ids[decision] = option_id
        opaque_ids.add(option_id)
    approval_ref = ApprovalRef.new(turn_ref)
    tool_call_id = _bounded_string(
        tool_call.get("toolCallId"), field_name="permission tool call ID", limit=256
    )
    title = _required_frame_bounded_string(
        tool_call.get("title"),
        field_name="permission tool title",
    )
    display_title = _permission_display_title(title)
    tool_kind = _optional_frame_bounded_string(
        tool_call.get("kind"), field_name="permission tool kind"
    ).lower()
    raw_input = tool_call.get("rawInput")
    if raw_input is None:
        raw_input = {}
    if not isinstance(raw_input, dict):
        raise _AcpContractError("ACP permission raw input is invalid")
    command_value = raw_input.get("command")
    if isinstance(command_value, list):
        if (
            not command_value
            or any(not isinstance(item, str) or not item for item in command_value)
        ):
            raise _AcpContractError("ACP permission command is invalid")
        command = " ".join(command_value)
    else:
        command = _optional_frame_bounded_string(
            command_value, field_name="permission command"
        )
    cwd = _optional_frame_bounded_string(
        raw_input.get("cwd"), field_name="permission cwd"
    )
    locations = tool_call.get("locations", [])
    if not isinstance(locations, list):
        raise _AcpContractError("ACP permission locations are invalid")
    paths: list[str] = []
    for location in locations:
        if not isinstance(location, dict):
            raise _AcpContractError("ACP permission location is invalid")
        paths.append(
            _required_frame_bounded_string(
                location.get("path"), field_name="permission path"
            )
        )

    review_payload = _permission_review_payload(
        title=title,
        tool_kind=tool_kind,
        raw_input=raw_input,
        locations=locations,
        command=command,
    )
    review = _PermissionReview(
        reviewable=len(review_payload) <= _APPROVAL_REVIEW_MAX_CHARS,
        observed_size=len(review_payload),
    )
    if not review.reviewable:
        return _NormalizedPermission(
            request=None,
            approval_ref=approval_ref,
            option_ids=MappingProxyType(option_ids),
            tool_call_id=tool_call_id,
            review=review,
        )
    if "decline" not in option_ids:
        raise _AcpContractError("ACP permission request has no reject option")

    available = tuple(
        decision
        for decision in ("accept", "always_allow", "decline")
        if decision in option_ids
    )
    base_diagnostics = (
        ("provider_tool_kind", tool_kind or "unknown"),
        ("provider_tool_title", display_title),
        ("provider_effect", review_payload),
    )
    if tool_kind == "execute":
        request = AgentApprovalRequest(
            turn_ref=turn_ref,
            approval_ref=approval_ref,
            kind=AgentApprovalKind.COMMAND,
            command=command or title,
            cwd=cwd,
            reason=f"ACP requested permission for {display_title}",
            available_decisions=available,
            prompt_details=tuple(
                item
                for item in (
                    f"command: {command or title}",
                    f"cwd: {cwd}" if cwd else "",
                )
                if item
            ),
            diagnostics=base_diagnostics,
        )
    elif tool_kind in {"edit", "delete", "move"}:
        request = AgentApprovalRequest(
            turn_ref=turn_ref,
            approval_ref=approval_ref,
            kind=AgentApprovalKind.FILE_CHANGE,
            cwd=cwd,
            affected_paths=tuple(paths),
            reason=f"ACP requested permission for {display_title}",
            available_decisions=available,
            prompt_details=tuple(f"file: {path}" for path in paths)
            or (f"tool: {display_title}",),
            diagnostics=base_diagnostics,
        )
    else:
        detail_values = tuple(
            value
            for value in raw_input.values()
            if isinstance(value, str) and value and len(value) <= 2048
        )[:16]
        request = AgentApprovalRequest(
            turn_ref=turn_ref,
            approval_ref=approval_ref,
            kind=AgentApprovalKind.TOOL,
            cwd=cwd,
            affected_paths=tuple(paths),
            reason=f"ACP requested permission for {display_title}",
            available_decisions=available,
            prompt_details=(f"tool: {display_title}", *detail_values),
            diagnostics=base_diagnostics,
        )
    return _NormalizedPermission(
        request=request,
        approval_ref=approval_ref,
        option_ids=MappingProxyType(option_ids),
        tool_call_id=tool_call_id,
        review=review,
    )


def _normalize_form_request(
    params: object,
    *,
    turn_ref: TurnRef,
) -> _NormalizedForm:
    if not isinstance(params, dict) or params.get("mode", "form") != "form":
        raise _AcpContractError("ACP elicitation mode is unsupported")
    message = _bounded_string(
        params.get("message"), field_name="elicitation message", limit=2048
    )
    schema = params.get("requestedSchema")
    if not isinstance(schema, dict) or schema.get("type") != "object":
        raise _AcpContractError("ACP form schema is invalid")
    allowed_schema = {"type", "properties", "required", "additionalProperties", "title", "description"}
    additional_properties = schema.get("additionalProperties", False)
    if set(schema).difference(allowed_schema) or (
        additional_properties is not False and additional_properties is not None
    ):
        raise _AcpContractError("ACP form schema is unsupported")
    properties = schema.get("properties")
    required = schema.get("required", [])
    if (
        not isinstance(properties, dict)
        or not properties
        or len(properties) > _FORM_FIELD_MAX_COUNT
        or not isinstance(required, list)
        or len(required) > _FORM_FIELD_MAX_COUNT
        or any(not isinstance(item, str) for item in required)
        or len(required) != len(set(required))
    ):
        raise _AcpContractError("ACP form schema fields are invalid")
    required_set = set(required)
    if not required_set.issubset(properties):
        raise _AcpContractError("ACP form schema required fields are invalid")
    fields: list[AgentFormField] = []
    for raw_name, raw_field in properties.items():
        name = _bounded_string(raw_name, field_name="form field name", limit=128)
        if not isinstance(raw_field, dict):
            raise _AcpContractError("ACP form field schema is invalid")
        if set(raw_field).difference({"type", "title", "description", "enum", "items"}):
            raise _AcpContractError("ACP form field schema is unsupported")
        raw_type = raw_field.get("type")
        value_type = str(raw_type or "")
        enum: tuple[str, ...] = ()
        if raw_type == "array":
            items = raw_field.get("items")
            if not isinstance(items, dict) or items.get("type") != "string" or set(items).difference({"type", "enum"}):
                raise _AcpContractError("ACP form array schema is unsupported")
            value_type = "string_array"
            raw_enum = items.get("enum", [])
        else:
            raw_enum = raw_field.get("enum", [])
        if raw_enum:
            if (
                not isinstance(raw_enum, list)
                or len(raw_enum) > 32
                or any(not isinstance(item, str) or not item or len(item) > 512 for item in raw_enum)
                or len(raw_enum) != len(set(raw_enum))
            ):
                raise _AcpContractError("ACP form enum is invalid")
            enum = tuple(raw_enum)
        title = _optional_bounded_string(
            raw_field.get("title"), field_name="form field title", limit=256
        )
        description = _optional_bounded_string(
            raw_field.get("description"), field_name="form field description", limit=2048
        )
        sensitive_text = f" {_canonical_sensitive_text(name, title, description)} "
        if any(
            f" {_canonical_sensitive_text(token)} " in sensitive_text
            for token in _SENSITIVE_FORM_TOKENS
        ):
            raise _AcpContractError("ACP form requested sensitive input")
        try:
            fields.append(
                AgentFormField(
                    name=name,
                    value_type=value_type,
                    required=name in required_set,
                    title=title,
                    description=description,
                    enum=enum,
                )
            )
        except ValueError as exc:
            raise _AcpContractError("ACP form field schema is invalid") from exc
    elicitation_ref = ElicitationRef.new(turn_ref)
    return _NormalizedForm(
        AgentFormRequest(turn_ref, elicitation_ref, message, tuple(fields))
    )


def _open_workspace_directory(workspace: str, raw_path: str) -> tuple[int, str]:
    root = Path(os.path.abspath(workspace))
    path = Path(raw_path)
    if not path.is_absolute():
        raise _AcpContractError("ACP terminal cwd is invalid")
    normalized = Path(os.path.abspath(path))
    try:
        relative = normalized.relative_to(root)
    except ValueError as exc:
        raise _AcpContractError("ACP terminal cwd is outside workspace") from exc
    directory_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in relative.parts:
            next_fd = os.open(
                part,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=directory_fd,
            )
            os.close(directory_fd)
            directory_fd = next_fd
    except (FileNotFoundError, NotADirectoryError, PermissionError, OSError) as exc:
        os.close(directory_fd)
        raise _AcpContractError("ACP terminal cwd is invalid") from exc
    return directory_fd, str(normalized)


def _validate_workspace_directory(workspace: str, raw_path: str) -> str:
    directory_fd, normalized = _open_workspace_directory(workspace, raw_path)
    os.close(directory_fd)
    return normalized

def _parse_config_values(raw: object, *, field_name: str) -> tuple[str, ...]:
    if not isinstance(raw, list) or len(raw) > _CONFIG_OPTION_VALUE_MAX_COUNT:
        raise _AcpContractError(f"ACP {field_name} values are invalid")
    values: list[str] = []
    for item in raw:
        if not isinstance(item, dict):
            raise _AcpContractError(f"ACP {field_name} value is invalid")
        if "value" in item:
            values.append(
                _bounded_string(
                    item.get("value"),
                    field_name=f"{field_name} value",
                    limit=512,
                )
            )
            continue
        nested = item.get("options")
        if not isinstance(nested, list) or len(nested) > _CONFIG_OPTION_VALUE_MAX_COUNT:
            raise _AcpContractError(f"ACP grouped {field_name} values are invalid")
        for nested_item in nested:
            if not isinstance(nested_item, dict):
                raise _AcpContractError(f"ACP grouped {field_name} value is invalid")
            values.append(
                _bounded_string(
                    nested_item.get("value"),
                    field_name=f"{field_name} value",
                    limit=512,
                )
            )
    if (
        len(values) > _CONFIG_OPTION_VALUE_TOTAL_MAX_COUNT
        or len(values) != len(set(values))
    ):
        raise _AcpContractError(f"ACP {field_name} values are ambiguous")
    return tuple(values)


def _parse_config_snapshot(result: object) -> _ConfigSnapshot:
    if not isinstance(result, dict):
        raise _AcpContractError("ACP session result is invalid")
    raw_options = result.get("configOptions", [])
    if not isinstance(raw_options, list) or len(raw_options) > _CONFIG_OPTION_MAX_COUNT:
        raise _AcpContractError("ACP config options are invalid")
    options: list[_ConfigOption] = []
    config_ids: set[str] = set()
    semantic_categories: set[str] = set()
    value_count = 0
    for raw in raw_options:
        if not isinstance(raw, dict):
            raise _AcpContractError("ACP config option is invalid")
        config_id = _bounded_string(
            raw.get("id"), field_name="config identity", limit=256
        )
        if config_id in config_ids:
            raise _AcpContractError("ACP config option identity is duplicated")
        config_ids.add(config_id)
        category = _optional_bounded_string(
            raw.get("category"), field_name="config category", limit=128
        )
        if category in {"model", "thought_level", "model_config", "mode"}:
            if category in semantic_categories:
                raise _AcpContractError("ACP config option category is ambiguous")
            semantic_categories.add(category)
        option_type = raw.get("type")
        if option_type == "select":
            current: str | bool = _bounded_string(
                raw.get("currentValue"), field_name="config current value", limit=512
            )
            values = _parse_config_values(raw.get("options"), field_name="config")
            value_count += len(values)
            if current not in values:
                raise _AcpContractError("ACP config current value is unavailable")
        elif option_type == "boolean":
            current = raw.get("currentValue")
            if not isinstance(current, bool):
                raise _AcpContractError("ACP boolean config current value is invalid")
            if raw.get("options") not in (None, []):
                raise _AcpContractError("ACP boolean config options are invalid")
            values = ()
        else:
            raise _AcpContractError("ACP config option type is invalid")
        if value_count > _CONFIG_OPTION_VALUE_TOTAL_MAX_COUNT:
            raise _AcpContractError("ACP config value capacity exceeded")
        options.append(_ConfigOption(config_id, category, option_type, current, values))
    return _ConfigSnapshot(tuple(options), known=True)


def _effective_from_snapshot(
    snapshot: _ConfigSnapshot,
    *,
    fallback_backend: str,
) -> _EffectiveSessionConfig:
    option = snapshot.by_category("model")
    if option is None:
        return _EffectiveSessionConfig(
            "unknown", fallback_backend, config_snapshot=snapshot
        )
    if option.option_type != "select" or not isinstance(option.current_value, str):
        raise _AcpContractError("ACP model config option is invalid")
    current = option.current_value
    provider_name, separator, model_name = current.partition("/")
    backend = provider_name if separator and provider_name and model_name else fallback_backend
    return _EffectiveSessionConfig(
        model=current,
        backend=backend,
        model_config_id=option.config_id,
        enabled_models=frozenset(option.values),
        config_snapshot=snapshot,
    )


def _parse_effective_runtime(
    result: object,
    *,
    fallback_backend: str,
) -> _EffectiveSessionConfig:
    return _effective_from_snapshot(
        _parse_config_snapshot(result), fallback_backend=fallback_backend
    )


def _parse_usage_update(update: object) -> _SessionUsage:
    if not isinstance(update, dict) or update.get("sessionUpdate") != "usage_update":
        raise _AcpContractError("ACP usage update is invalid")
    used = update.get("used")
    size = update.get("size")
    if (
        type(used) is not int
        or used < 0
        or type(size) is not int
        or size <= 0
        or used > 1_000_000_000
        or size > 1_000_000_000
    ):
        raise _AcpContractError("ACP usage values are invalid")
    cost = update.get("cost")
    if cost is None:
        return _SessionUsage(used, size)
    if not isinstance(cost, dict):
        raise _AcpContractError("ACP usage cost is invalid")
    amount = cost.get("amount")
    currency = cost.get("currency")
    if (
        isinstance(amount, bool)
        or not isinstance(amount, (int, float))
        or not math.isfinite(float(amount))
        or amount < 0
        or not isinstance(currency, str)
        or re.fullmatch(r"[A-Z]{3}", currency) is None
    ):
        raise _AcpContractError("ACP usage cost is invalid")
    return _SessionUsage(used, size, amount, currency)

@dataclass(frozen=True)
class AcpProviderConfig:
    runtime_profile: AcpRuntimeProfileConfig
    logger: LifecycleLogger
    command: tuple[str, ...]
    env: Mapping[str, str] | None = field(default=None, repr=False)
    role: str = "agent"
    startup_timeout_sec: float = 120.0
    rpc_timeout_sec: float = 30.0
    edge_thread_name: str = "acp-v1-thread"

    def __post_init__(self) -> None:
        if not isinstance(self.runtime_profile, AcpRuntimeProfileConfig):
            raise ValueError("ACP provider requires AcpRuntimeProfileConfig")
        if not isinstance(self.logger, LifecycleLogger):
            raise ValueError("ACP provider logger must be LifecycleLogger")
        if not self.command or any(
            not isinstance(item, str) or not item for item in self.command
        ):
            raise ValueError("ACP command must be a non-empty string tuple")
        if not isinstance(self.role, str) or not self.role.strip():
            raise ValueError("ACP role must be a non-empty string")
        if not isinstance(self.edge_thread_name, str) or not self.edge_thread_name.strip():
            raise ValueError("ACP edge thread name must be non-empty")
        env = dict(self.env or {})
        if any(not isinstance(key, str) or not isinstance(value, str) for key, value in env.items()):
            raise ValueError("ACP env must contain string keys and values")
        object.__setattr__(self, "env", MappingProxyType(env))
        object.__setattr__(self, "role", self.role.strip())
        object.__setattr__(
            self,
            "startup_timeout_sec",
            validate_timeout_seconds(
                self.startup_timeout_sec,
                field_name="ACP startup timeout",
            ),
        )
        object.__setattr__(
            self,
            "rpc_timeout_sec",
            validate_timeout_seconds(
                self.rpc_timeout_sec,
                field_name="ACP RPC timeout",
            ),
        )
        object.__setattr__(self, "command", tuple(self.command))
        object.__setattr__(self, "edge_thread_name", self.edge_thread_name.strip())

    @property
    def edge_stop_timeout_sec(self) -> float:
        return self.runtime_profile.stop_timeout_sec

    @property
    def edge_queue_capacity(self) -> int:
        return self.runtime_profile.edge_queue_capacity

    @property
    def port_event_capacity(self) -> int:
        return self.runtime_profile.port_event_capacity

    @property
    def turn_output_max_chars(self) -> int:
        return self.runtime_profile.turn_output_max_chars
