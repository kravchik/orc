"""Codex-local runtime helpers for app-server approval requests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from orchestrator.approval import (
    format_regex_auto_approval_notification,
    format_regex_budget_exhausted_notification,
    format_regex_fallback_human_notification,
    format_regex_timeout_notification,
)
from orchestrator.agent_session import AgentApprovalKind
from orchestrator.codex_approval import (
    COMMAND_APPROVAL_METHOD,
    approval_response_decision_name,
    build_session_approval_response,
)
from orchestrator.processes import LifecycleLogger


@dataclass(frozen=True)
class ApprovalServerRequest:
    req_id: int
    method: str
    params: dict


@dataclass(frozen=True)
class ApprovalResponsePlan:
    req_id: int
    result: dict
    via: str
    method: str
    params: dict
    decision: str
    source: str
    command: str
    always_allow: bool


def parse_server_request(msg: dict) -> ApprovalServerRequest | None:
    if "id" not in msg or "method" not in msg or "result" in msg or "error" in msg:
        return None
    try:
        req_id = int(msg.get("id"))
    except (TypeError, ValueError):
        return None
    method = str(msg.get("method"))
    params = msg.get("params") or {}
    if not isinstance(params, dict):
        params = {}
    return ApprovalServerRequest(req_id=req_id, method=method, params=params)


def build_always_allow_cached_response_plan(
    *,
    request: ApprovalServerRequest,
    logger: LifecycleLogger,
    role: str | None,
    always_allow_commands: set[str] | None,
    log_auto_decision: bool = True,
) -> ApprovalResponsePlan | None:
    command = request.params.get("command")
    if (
        request.method != COMMAND_APPROVAL_METHOD
        or always_allow_commands is None
        or not isinstance(command, str)
        or command not in always_allow_commands
    ):
        return None
    result = build_session_approval_response(
        method=request.method,
        params=request.params,
    ) or {"decision": "accept"}
    decision = approval_response_decision_name(result)
    if log_auto_decision:
        _event(
            logger=logger,
            role=role,
            name="approval_auto_decision",
            id=request.req_id,
            method=request.method,
            decision=decision,
            source="always_allow_cache",
            command=command,
        )
    return ApprovalResponsePlan(
        req_id=request.req_id,
        result=result,
        via="always_allow_cache",
        method=request.method,
        params=request.params,
        decision=decision,
        source="always_allow_cache",
        command=command,
        always_allow=False,
    )


def log_session_approval_applied(
    *,
    logger: LifecycleLogger,
    role: str | None,
    req_id: int,
    method: str,
    result: dict,
) -> None:
    decision = result.get("decision")
    if isinstance(decision, dict):
        payload = decision.get("acceptWithExecpolicyAmendment")
        if isinstance(payload, dict):
            _event(
                logger=logger,
                role=role,
                name="approval_execpolicy_amendment_applied",
                id=req_id,
                execpolicy_amendment=payload.get("execpolicy_amendment"),
            )
            return
    if decision == "acceptForSession":
        _event(
            logger=logger,
            role=role,
            name="approval_session_accept_applied",
            id=req_id,
            method=method,
        )


def _event(
    *,
    logger: LifecycleLogger,
    role: str | None,
    name: str,
    **kwargs,
) -> None:
    payload = dict(kwargs)
    if role is not None:
        payload["role"] = role
    logger.event(name, **payload)
