"""Shared rendering helpers for approval details across access points."""

from __future__ import annotations

from orchestrator.agent_session import AgentApprovalRequest


def approval_source_icon(request: AgentApprovalRequest) -> str:
    role = request.role.lower()
    if role in {"worker", "agent", "runtime"}:
        return "🚀"
    if role == "steward":
        return "🧑‍✈️"
    if role == "lead":
        return "🧠"
    return "🚀"


def build_approval_prompt_detail_lines(
    request: AgentApprovalRequest,
    *,
    string_limit: int | None = None,
) -> list[str]:
    lines = list(request.prompt_details)
    lines.extend(f"persistent rule: {scope}" for scope in request.saved_rule_scope)
    return [_render_prompt_text(line, string_limit) for line in lines]


def _render_prompt_text(text: str, string_limit: int | None) -> str:
    if string_limit is None:
        return text
    compact = " ".join(str(text).split())
    if len(compact) <= string_limit:
        return compact
    return compact[: max(0, string_limit - 3)] + "..."


def build_approval_details_lines(
    request: AgentApprovalRequest,
    *,
    header: str,
    include_role: bool,
) -> list[str]:
    lines = [
        header,
        f"kind={request.kind.value}",
    ]
    if include_role:
        lines.append(f"role={request.role}")
    lines.extend(f"saved_rule_scope={scope}" for scope in request.saved_rule_scope)
    lines.extend(f"{key}={value}" for key, value in request.diagnostics)
    return lines


def build_approval_request_param_lines(request: AgentApprovalRequest) -> list[str]:
    """Render every user-meaningful request field for an approval mirror."""

    lines = [f"saved rule scope: {scope}" for scope in request.saved_rule_scope]
    lines.extend(f"{key}: {value}" for key, value in request.diagnostics)
    return lines
