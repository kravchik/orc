"""Shared rendering helpers for approval details across access points."""

from __future__ import annotations

import json
from typing import Any

from orchestrator.approval import ApprovalRequest
from orchestrator.file_change_approval import format_file_change_prompt_lines


_PREFERRED_DETAIL_KEYS = (
    "command",
    "cwd",
    "reason",
    "commandActions",
    "tool",
    "path",
    "targetPath",
    "oldPath",
    "newPath",
    "itemId",
    "grantRoot",
    "availableDecisions",
    "proposedExecpolicyAmendment",
)

_KEY_ALIASES = {
    "commandActions": "actions",
}

_PREFERRED_PROMPT_KEYS = (
    "reason",
    "tool",
    "path",
    "targetPath",
    "oldPath",
    "newPath",
    "itemId",
    "grantRoot",
)


def approval_source_icon(request: ApprovalRequest) -> str:
    role = str(getattr(request, "role", "") or "").strip().lower()
    if role in {"worker", "agent", "runtime"}:
        return "🚀"
    if role == "steward":
        return "🧑‍✈️"
    if role == "lead":
        return "🧠"
    return "🚀"


def build_approval_prompt_detail_lines(
    request: ApprovalRequest,
    *,
    string_limit: int | None = None,
) -> list[str]:
    params = request.params
    command = params.get("command")
    cwd = params.get("cwd")
    lines: list[str] = []
    if isinstance(command, str) and command.strip():
        lines.append(f"command: {_render_prompt_text(command, string_limit)}")
    if isinstance(cwd, str) and cwd.strip():
        lines.append(f"cwd: {cwd}")
    if lines:
        return lines

    file_change_lines = format_file_change_prompt_lines(params)
    if file_change_lines:
        reason = params.get("reason")
        if isinstance(reason, str) and reason.strip():
            rendered = _render_prompt_text(reason.strip(), string_limit)
            return [f"reason: {rendered}"] + file_change_lines
        return file_change_lines

    emitted: set[str] = set()
    for key in _PREFERRED_PROMPT_KEYS:
        rendered = _render_prompt_scalar(params.get(key), string_limit)
        if rendered is None:
            continue
        lines.append(f"{key}: {rendered}")
        emitted.add(key)
    if lines:
        return lines

    for key in sorted(params.keys()):
        if key in emitted or key in {"threadId", "turnId", "command", "cwd"}:
            continue
        rendered = _render_prompt_scalar(params.get(key), string_limit)
        if rendered is not None:
            lines.append(f"{key}: {rendered}")
    return lines


def _render_prompt_scalar(value: object, string_limit: int | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return _render_prompt_text(stripped, string_limit) if stripped else None
    if isinstance(value, (int, float, bool)):
        return str(value)
    return None


def _render_prompt_text(text: str, string_limit: int | None) -> str:
    if string_limit is None:
        return text
    compact = " ".join(str(text).split())
    if len(compact) <= string_limit:
        return compact
    return compact[: max(0, string_limit - 3)] + "..."


def render_approval_detail_value(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, (list, dict)):
        try:
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        except Exception:
            return str(value)
    return None


def build_approval_details_lines(
    request: ApprovalRequest,
    *,
    header: str,
    include_role: bool,
) -> list[str]:
    params = request.params
    lines = [
        header,
        f"method={request.method}",
    ]
    if include_role:
        lines.append(f"role={getattr(request, 'role', '')}")
    emitted: set[str] = set()
    for key in _PREFERRED_DETAIL_KEYS:
        rendered = render_approval_detail_value(params.get(key))
        if rendered is None:
            continue
        lines.append(f"{_KEY_ALIASES.get(key, key)}={rendered}")
        emitted.add(key)
    for key in sorted(params.keys()):
        if key in emitted or key in {"threadId", "turnId"}:
            continue
        rendered = render_approval_detail_value(params.get(key))
        if rendered is None:
            continue
        lines.append(f"{key}={rendered}")
    return lines


def build_approval_request_param_lines(request: ApprovalRequest) -> list[str]:
    """Render every user-meaningful request field for an approval mirror."""

    params = request.params
    excluded = {"threadId", "turnId", "itemId"}
    lines: list[str] = []
    emitted: set[str] = set()
    for key in _PREFERRED_DETAIL_KEYS:
        if key in excluded:
            continue
        rendered = render_approval_detail_value(params.get(key))
        if rendered is None:
            continue
        lines.append(f"{_KEY_ALIASES.get(key, key)}: {rendered}")
        emitted.add(key)
    for key in sorted(params.keys()):
        if key in emitted or key in excluded:
            continue
        rendered = render_approval_detail_value(params.get(key))
        if rendered is None:
            continue
        lines.append(f"{key}: {rendered}")
    return lines
