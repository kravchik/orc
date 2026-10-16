"""Codex approval payload normalization and native decision encoding."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum

from orchestrator.agent_capabilities import AgentCapability, AgentCapabilitySnapshot
from orchestrator.agent_identity import ApprovalRef, TurnRef
from orchestrator.agent_session import AgentApprovalKind, AgentApprovalRequest
from orchestrator.file_change_approval import (
    extract_normalized_file_change_paths,
    format_file_change_prompt_lines,
)


COMMAND_APPROVAL_METHOD = "item/commandExecution/requestApproval"
FILE_CHANGE_APPROVAL_METHOD = "item/fileChange/requestApproval"
USER_INPUT_METHODS = frozenset(
    {
        "item/tool/requestUserInput",
        "tool/requestUserInput",
    }
)

_DETAIL_KEYS = (
    "command",
    "cwd",
    "reason",
    "commandActions",
    "tool",
    "path",
    "targetPath",
    "oldPath",
    "newPath",
    "grantRoot",
    "availableDecisions",
    "proposedExecpolicyAmendment",
)
_PROMPT_KEYS = (
    "prompt",
    "reason",
    "tool",
    "path",
    "targetPath",
    "oldPath",
    "newPath",
    "grantRoot",
    "availableDecisions",
)
_DETAIL_ALIASES = {"commandActions": "actions"}
_MAX_DETAIL_FIELDS = 32
_MAX_DETAIL_VALUE_CHARS = 2048
_MAX_PROMPT_LINES = 32
_NATIVE_ID_FIELDS = frozenset(
    {
        "threadId",
        "turnId",
        "itemId",
        "requestId",
        "callId",
        "toolCallId",
    }
)


@dataclass(frozen=True)
class NativeApprovalRequest:
    req_id: int
    method: str
    params: dict
    role: str

    def __post_init__(self) -> None:
        if not isinstance(self.req_id, int) or isinstance(self.req_id, bool):
            raise ValueError("Codex approval request token must be an integer")
        if not isinstance(self.method, str) or not self.method.strip():
            raise ValueError("Codex approval request method must be a non-empty string")
        if not isinstance(self.params, dict):
            raise ValueError("Codex approval request params must be a dict")


@dataclass(frozen=True)
class CodexApprovalCorrelation:
    native_request: NativeApprovalRequest
    request: AgentApprovalRequest


class CodexApprovalOccurrenceDisposition(StrEnum):
    NEW = "new"
    DUPLICATE = "duplicate"
    CAPACITY = "capacity"


@dataclass(frozen=True)
class CodexApprovalOccurrence:
    approval_ref: ApprovalRef | None
    disposition: CodexApprovalOccurrenceDisposition


class CodexApprovalCorrelations:
    """Owns the one pending Codex native-to-ORC approval correlation."""

    def __init__(self) -> None:
        self._by_ref: dict[ApprovalRef, CodexApprovalCorrelation] = {}

    @property
    def has_pending(self) -> bool:
        return bool(self._by_ref)

    def register(
        self,
        *,
        native_request: NativeApprovalRequest,
        request: AgentApprovalRequest,
    ) -> None:
        if self._by_ref:
            raise RuntimeError("a Codex approval correlation is already pending")
        self._by_ref[request.approval_ref] = CodexApprovalCorrelation(
            native_request=native_request,
            request=request,
        )

    def approval_ref_for(
        self,
        *,
        turn_ref: TurnRef,
        native_request_token: int,
    ) -> CodexApprovalOccurrence:
        if self._by_ref:
            correlation = next(iter(self._by_ref.values()))
            if (
                correlation.request.turn_ref == turn_ref
                and correlation.native_request.req_id == native_request_token
            ):
                return CodexApprovalOccurrence(
                    correlation.request.approval_ref,
                    CodexApprovalOccurrenceDisposition.DUPLICATE,
                )
            return CodexApprovalOccurrence(
                None,
                CodexApprovalOccurrenceDisposition.CAPACITY,
            )
        return CodexApprovalOccurrence(
            ApprovalRef.new(turn_ref),
            CodexApprovalOccurrenceDisposition.NEW,
        )

    def resolve(
        self,
        *,
        turn_ref: TurnRef,
        approval_ref: ApprovalRef,
    ) -> CodexApprovalCorrelation | None:
        correlation = self._by_ref.get(approval_ref)
        if correlation is None or correlation.request.turn_ref != turn_ref:
            return None
        return correlation

    def discard(self, approval_ref: ApprovalRef) -> None:
        self._by_ref.pop(approval_ref, None)

    def clear(self) -> None:
        self._by_ref.clear()


def is_codex_approval_method(method: str) -> bool:
    return method in (COMMAND_APPROVAL_METHOD, FILE_CHANGE_APPROVAL_METHOD)


def build_execpolicy_amendment_decision(params: dict) -> dict | None:
    amendment = params.get("proposedExecpolicyAmendment")
    if not isinstance(amendment, list):
        return None
    tokens: list[str] = []
    for item in amendment:
        if not isinstance(item, str):
            return None
        tokens.append(item)
    if not tokens:
        return None
    return {
        "acceptWithExecpolicyAmendment": {
            "execpolicy_amendment": tokens,
        }
    }


def build_session_approval_response(*, method: str, params: dict) -> dict | None:
    if method == FILE_CHANGE_APPROVAL_METHOD:
        return {"decision": "acceptForSession"}
    if method != COMMAND_APPROVAL_METHOD:
        return None
    decision = build_execpolicy_amendment_decision(params=params)
    if decision is None:
        return None
    return {"decision": decision}


def approval_response_decision_name(result: dict) -> str:
    decision = result.get("decision")
    if isinstance(decision, str) and decision:
        return decision
    if isinstance(decision, dict) and len(decision) == 1:
        name = next(iter(decision))
        if isinstance(name, str) and name:
            return name
    raise ValueError(f"invalid approval response decision: {decision!r}")


def supports_session_approval(*, method: str, params: dict) -> bool:
    return build_session_approval_response(method=method, params=params) is not None


def normalize_codex_approval_request(
    request: NativeApprovalRequest,
    *,
    turn_ref: TurnRef,
    approval_ref: ApprovalRef,
    capabilities: AgentCapabilitySnapshot,
) -> AgentApprovalRequest:
    params = request.params
    method = str(request.method or "").strip()
    if method == COMMAND_APPROVAL_METHOD:
        kind = AgentApprovalKind.COMMAND
    elif method == FILE_CHANGE_APPROVAL_METHOD:
        kind = AgentApprovalKind.FILE_CHANGE
    elif method in USER_INPUT_METHODS:
        kind = AgentApprovalKind.USER_INPUT
    else:
        kind = AgentApprovalKind.TOOL

    command = params.get("command")
    if not isinstance(command, str) or not command.strip():
        tool = params.get("tool")
        command = tool if isinstance(tool, str) else ""
    decisions = ["accept", "decline"]
    if (
        capabilities.supports(AgentCapability.APPROVAL_ALWAYS)
        and supports_session_approval(method=method, params=params)
    ):
        decisions.append("always_allow")
    return AgentApprovalRequest(
        turn_ref=turn_ref,
        approval_ref=approval_ref,
        kind=kind,
        role=_bounded(str(request.role or "agent"), 80) or "agent",
        command=_bounded(str(command or ""), 8192),
        cwd=_bounded(str(params.get("cwd") or ""), 4096),
        affected_paths=tuple(
            _bounded(path, 4096)
            for path in extract_normalized_file_change_paths(params)
            if path
        ),
        reason=_bounded(str(params.get("reason") or ""), 2048),
        available_decisions=tuple(decisions),
        prompt_details=_prompt_details(params),
        diagnostics=_diagnostics(params),
    )


def _prompt_details(params: dict) -> tuple[str, ...]:
    command = params.get("command")
    cwd = params.get("cwd")
    lines: list[str] = []
    if isinstance(command, str) and command.strip():
        lines.append(_bounded_prompt(f"command: {command}"))
    if isinstance(cwd, str) and cwd.strip():
        lines.append(_bounded_prompt(f"cwd: {cwd}"))
    if lines:
        return tuple(lines)

    file_lines = format_file_change_prompt_lines(params)
    if file_lines:
        reason = params.get("reason")
        if isinstance(reason, str) and reason.strip():
            file_lines = [_bounded_prompt(f"reason: {reason}"), *file_lines]
        return tuple(_bounded_prompt(line) for line in file_lines[:_MAX_PROMPT_LINES])

    emitted: set[str] = set()
    for key in _PROMPT_KEYS:
        rendered = _render_prompt_value(params.get(key))
        if rendered is None:
            continue
        lines.append(_bounded_prompt(f"{key}: {rendered}"))
        emitted.add(key)
    if not lines:
        for key in sorted(params):
            if key in emitted or key in _NATIVE_ID_FIELDS or key in {"command", "cwd"}:
                continue
            rendered = _render_prompt_value(params.get(key))
            if rendered is not None:
                lines.append(_bounded_prompt(f"{key}: {rendered}"))
            if len(lines) >= _MAX_PROMPT_LINES:
                break
    return tuple(lines[:_MAX_PROMPT_LINES])


def _diagnostics(params: dict) -> tuple[tuple[str, str], ...]:
    details: list[tuple[str, str]] = []
    emitted: set[str] = set()
    output_keys: set[str] = set()
    for key in _DETAIL_KEYS:
        if key in _NATIVE_ID_FIELDS:
            continue
        rendered = _render_detail(params.get(key))
        if rendered is None:
            continue
        output_key = _DETAIL_ALIASES.get(key, key)
        if output_key in output_keys:
            continue
        details.append((output_key, _bounded(rendered, _MAX_DETAIL_VALUE_CHARS)))
        emitted.add(key)
        output_keys.add(output_key)
    for key in sorted(params):
        if key in emitted or key in _NATIVE_ID_FIELDS:
            continue
        rendered = _render_detail(params.get(key))
        if rendered is None:
            continue
        output_key = _bounded(str(key), 80)
        if not output_key or output_key in output_keys:
            continue
        details.append((output_key, _bounded(rendered, _MAX_DETAIL_VALUE_CHARS)))
        output_keys.add(output_key)
        if len(details) >= _MAX_DETAIL_FIELDS:
            break
    return tuple(details[:_MAX_DETAIL_FIELDS])


def _render_scalar(value: object) -> str | None:
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, (int, float, bool)):
        return str(value)
    return None


def _render_prompt_value(value: object) -> str | None:
    value = _without_native_ids(value)
    scalar = _render_scalar(value)
    if scalar is not None:
        return scalar
    if isinstance(value, list) and all(isinstance(item, (str, int, float, bool)) for item in value):
        return ", ".join(str(item) for item in value)
    return None


def _render_detail(value: object) -> str | None:
    value = _without_native_ids(value)
    scalar = _render_scalar(value)
    if scalar is not None:
        return scalar
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return None


def _without_native_ids(value: object) -> object:
    if isinstance(value, dict):
        return {
            key: _without_native_ids(item)
            for key, item in value.items()
            if key not in _NATIVE_ID_FIELDS
        }
    if isinstance(value, list):
        return [_without_native_ids(item) for item in value]
    return value


def _bounded_prompt(value: str) -> str:
    return _bounded(str(value).strip(), _MAX_DETAIL_VALUE_CHARS)


def _bounded(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 3)] + "..."
