"""Typed request/response contract for approval delegation between agents."""

from __future__ import annotations

from dataclasses import dataclass
import json
import shlex

from orchestrator.agent_session import AgentApprovalKind, AgentApprovalRequest


_DELEGATION_TRANSPORT_NOISE_KEYS = frozenset(
    {
        "availableDecisions",
        "command",
        "cwd",
        "kind",
        "proposedExecpolicyAmendment",
        "reason",
        "startedAtMs",
    }
)
_COMMAND_PROVIDER_ACTIONS = frozenset({"bash", "command", "exec", "shell"})
_SHELL_EXECUTABLES = frozenset({"bash", "dash", "ksh", "sh", "zsh"})
_SHELL_COMMAND_FLAGS = frozenset({"-c", "-cl", "-lc"})


@dataclass(frozen=True)
class ApprovalDelegationDecision:
    approval_id: str
    decision: str


def build_approval_delegation_prompt(
    *,
    approval_id: str,
    source_label: str,
    target_address: str,
    request: AgentApprovalRequest,
) -> str:
    request_payload = _approval_delegation_request_projection(request)
    request_json = json.dumps(
        request_payload,
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
    )
    return (
        "ORC typed approval delegation request.\n"
        "Do not run tools. Decide only from the supplied request.\n"
        "Return exactly one JSON object and no other text:\n"
        f'{{"type":"approval_decision","approval_id":"{approval_id}",'
        '"decision":"accept|decline"}\n\n'
        f"source: {source_label}\n"
        f"target_address: {target_address}\n"
        f"kind: {request.kind.value}\n"
        f"request:\n{request_json}"
    )


def _approval_delegation_request_projection(
    request: AgentApprovalRequest,
) -> dict[str, object]:
    request_payload: dict[str, object] = {}
    if request.command:
        request_payload["command"] = request.command
    if request.cwd:
        request_payload["cwd"] = request.cwd
    if request.affected_paths:
        request_payload["affectedPaths"] = list(request.affected_paths)
    if request.reason:
        request_payload["reason"] = request.reason
    if request.saved_rule_scope:
        request_payload["savedRuleScope"] = list(request.saved_rule_scope)
    details = _delegation_details(request)
    if details:
        request_payload["details"] = details
    return request_payload


def _delegation_details(request: AgentApprovalRequest) -> dict[str, object]:
    details: dict[str, object] = {}
    for key, value in request.diagnostics:
        if key in _DELEGATION_TRANSPORT_NOISE_KEYS:
            continue
        if key == "environmentId" and value.strip().lower() == "local":
            continue
        if key == "actions":
            if _is_redundant_single_command_action(value, request.command):
                continue
            structured_actions = _structured_json(value)
            if structured_actions:
                details[key] = structured_actions
            elif structured_actions is None:
                details[key] = value
            continue
        if key == "provider_input":
            provider_input = _compact_provider_input(value, request)
            if provider_input is not None:
                details[key] = provider_input
            continue
        if key in {"provider_command", "tool"} and _commands_equivalent(
            value.strip(), request.command
        ):
            continue
        if key == "provider_cwd" and value.strip() == request.cwd:
            continue
        if key == "provider_paths" and _diagnostic_paths(value) == request.affected_paths:
            continue
        if key == "provider_resources" and _provider_resources_are_redundant(
            value, request
        ):
            continue
        if key == "provider_tool" and _provider_tool_is_redundant(value, request):
            continue
        if key == "provider_action" and _provider_action_is_redundant(value, request):
            continue
        if key == "provider_scope" and _provider_scope_is_redundant(value, request):
            continue
        if key == "provider_tool_kind" and _provider_tool_kind_is_redundant(
            value, request
        ):
            continue
        if key == "provider_tool_title" and _provider_tool_title_is_redundant(
            value, request
        ):
            continue
        details[key] = value
    return details


def _is_redundant_single_command_action(value: str, command: str) -> bool:
    if not command:
        return False
    try:
        actions = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return False
    if not isinstance(actions, list) or len(actions) != 1:
        return False
    action = actions[0]
    if not isinstance(action, dict):
        return False
    action_command = action.get("command")
    if not isinstance(action_command, str) or not _commands_equivalent(
        action_command, command
    ):
        return False
    if set(action) - {"command", "type"}:
        return False
    action_type = action.get("type", "")
    return action_type in {"", "command", "unknown"}


def _commands_equivalent(left: str, right: str) -> bool:
    left = left.strip()
    right = right.strip()
    if not left or not right:
        return False
    if left == right:
        return True
    return _shell_command_body(left) == right or _shell_command_body(right) == left


def _shell_command_body(command: str) -> str | None:
    try:
        parts = shlex.split(command)
    except ValueError:
        return None
    if len(parts) != 3:
        return None
    executable = parts[0].rsplit("/", 1)[-1]
    if executable not in _SHELL_EXECUTABLES or parts[1] not in _SHELL_COMMAND_FLAGS:
        return None
    return parts[2]


def _structured_json(value: str) -> dict[str, object] | list[object] | None:
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return None
    if isinstance(parsed, dict):
        return parsed
    if isinstance(parsed, list):
        return parsed
    return None


def _compact_provider_input(
    value: str,
    request: AgentApprovalRequest,
) -> object | None:
    parsed = _structured_json(value)
    if not isinstance(parsed, dict):
        return parsed if parsed is not None else value

    compact = dict(parsed)
    command = compact.get("command")
    if isinstance(command, str) and _commands_equivalent(command, request.command):
        compact.pop("command")
    cwd = compact.get("cwd")
    if isinstance(cwd, str) and cwd.strip() == request.cwd:
        compact.pop("cwd")
    for key in ("file_path", "path"):
        path = compact.get(key)
        if isinstance(path, str) and path.strip() in request.affected_paths:
            compact.pop(key)
    paths = compact.get("paths")
    if isinstance(paths, list) and tuple(paths) == request.affected_paths:
        compact.pop("paths")
    reason = compact.get("reason")
    if isinstance(reason, str) and reason.strip() == request.reason:
        compact.pop("reason")
    tool = compact.get("tool")
    if isinstance(tool, str) and tool.strip() == request.command:
        compact.pop("tool")
    return compact or None


def _provider_resources_are_redundant(
    value: str,
    request: AgentApprovalRequest,
) -> bool:
    resources = _diagnostic_paths(value)
    return bool(resources) and (
        resources == request.affected_paths
        or resources == request.saved_rule_scope
        or (len(resources) == 1 and resources[0] == request.command)
    )


def _provider_tool_is_redundant(value: str, request: AgentApprovalRequest) -> bool:
    tool = value.strip()
    return tool == request.command or (
        request.kind is AgentApprovalKind.COMMAND and tool.lower() == "bash"
    )


def _provider_action_is_redundant(value: str, request: AgentApprovalRequest) -> bool:
    action = value.strip()
    return action == request.command or (
        request.kind is AgentApprovalKind.COMMAND
        and action.lower() in _COMMAND_PROVIDER_ACTIONS
    )


def _provider_scope_is_redundant(value: str, request: AgentApprovalRequest) -> bool:
    scope = value.strip().lower()
    return (scope == "one_time" and not request.saved_rule_scope) or (
        scope == "explicit_saved_rule" and bool(request.saved_rule_scope)
    )


def _provider_tool_kind_is_redundant(
    value: str,
    request: AgentApprovalRequest,
) -> bool:
    return (
        request.kind is AgentApprovalKind.COMMAND
        and value.strip().lower() == "execute"
    )


def _provider_tool_title_is_redundant(
    value: str,
    request: AgentApprovalRequest,
) -> bool:
    title = value.strip()
    return title == request.command or request.reason == (
        f"ACP requested permission for {title}"
    )


def _diagnostic_paths(value: str) -> tuple[str, ...]:
    return tuple(path.strip() for path in value.splitlines() if path.strip())


def parse_approval_delegation_decision(
    text: str,
    *,
    expected_approval_id: str,
) -> ApprovalDelegationDecision:
    try:
        payload = json.loads(str(text or "").strip())
    except json.JSONDecodeError as exc:
        raise ValueError(f"approval decision must be one JSON object: {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise ValueError("approval decision must be a JSON object")
    if set(payload) != {"type", "approval_id", "decision"}:
        raise ValueError("approval decision must contain exactly type, approval_id, decision")
    if payload.get("type") != "approval_decision":
        raise ValueError("approval decision type must be approval_decision")
    approval_id = str(payload.get("approval_id") or "")
    if approval_id != expected_approval_id:
        raise ValueError(
            f"approval decision id mismatch: expected {expected_approval_id}, got {approval_id or '<empty>'}"
        )
    decision = str(payload.get("decision") or "").strip().lower()
    if decision not in {"accept", "decline"}:
        raise ValueError("approval decision must be accept or decline")
    return ApprovalDelegationDecision(approval_id=approval_id, decision=decision)


def format_approval_request_copy(
    *,
    source_label: str,
    target_address: str,
    request: AgentApprovalRequest,
) -> str:
    lines = [
        "approval request",
        f"from: {source_label}",
        f"to: {target_address}",
        f"kind: {request.kind.value}",
    ]
    projection = _approval_delegation_request_projection(request)
    if command := projection.get("command"):
        lines.append(f"command: {command}")
    if cwd := projection.get("cwd"):
        lines.append(f"cwd: {cwd}")
    for path in projection.get("affectedPaths", ()):
        lines.append(f"affected path: {path}")
    if reason := projection.get("reason"):
        lines.append(f"reason: {reason}")
    for scope in projection.get("savedRuleScope", ()):
        lines.append(f"saved rule scope: {scope}")
    details = projection.get("details")
    if isinstance(details, dict):
        for key, value in details.items():
            rendered = (
                json.dumps(value, ensure_ascii=False, sort_keys=True)
                if isinstance(value, (dict, list))
                else str(value)
            )
            lines.append(f"{key}: {rendered}")
    return "\n".join(lines)
