"""Shared runtime helpers for handling app-server approval requests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from orchestrator.approval import (
    ApprovalDecisionProvider,
    ApprovalPolicy,
    ApprovalRequest,
    build_accept_settings,
    format_regex_auto_approval_notification,
    format_regex_fallback_human_notification,
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


def build_approval_response_plan(
    *,
    request: ApprovalServerRequest,
    logger: LifecycleLogger,
    role: str | None,
    approval_policy: ApprovalPolicy | None,
    approval_decision_provider: ApprovalDecisionProvider | None,
    approval_notify: Callable[[str], None] | None = None,
    always_allow_commands: set[str] | None = None,
    before_human_required: Callable[[], None] | None = None,
    decide_human: Callable[[ApprovalRequest], str] | None = None,
    log_approval_requested: bool = True,
    log_human_required: bool = True,
    log_human_decision: bool = True,
    log_auto_decision: bool = True,
) -> ApprovalResponsePlan:
    if not ApprovalPolicy.is_approval_method(request.method):
        return ApprovalResponsePlan(
            req_id=request.req_id,
            result={"decision": "decline"},
            via="unsupported_method",
            method=request.method,
            params=request.params,
            decision="decline",
            source="unsupported_method",
            command="",
            always_allow=False,
        )

    command_from_params = request.params.get("command")
    if (
        always_allow_commands is not None
        and isinstance(command_from_params, str)
        and command_from_params in always_allow_commands
    ):
        result: dict = {"decision": "accept"}
        accept_settings = build_accept_settings(params=request.params)
        if accept_settings is not None:
            result["acceptSettings"] = accept_settings
        return ApprovalResponsePlan(
            req_id=request.req_id,
            result=result,
            via="always_allow_cache",
            method=request.method,
            params=request.params,
            decision="accept",
            source="always_allow_cache",
            command=command_from_params,
            always_allow=False,
        )

    if log_approval_requested:
        _event(
            logger=logger,
            role=role,
            name="approval_requested",
            id=request.req_id,
            method=request.method,
            params=request.params,
        )

    decision = "human"
    decision_source = "interactive"
    command_text = ApprovalPolicy._extract_command(method=request.method, params=request.params)
    if approval_policy is not None:
        decision_trace = approval_policy.decide_with_trace(method=request.method, params=request.params)
        decision = decision_trace.decision
        decision_source = decision_trace.source
        command_text = decision_trace.command
        _log_regex_allowlist_trace(
            logger=logger,
            role=role,
            request=request,
            decision=decision,
            decision_source=decision_source,
            command=command_text,
            regex_trace=decision_trace.regex_trace,
            approval_notify=approval_notify,
        )

    always_allow = False
    human_flow = decision == "human"
    if human_flow:
        if before_human_required is not None:
            before_human_required()
        if log_human_required:
            _event(
                logger=logger,
                role=role,
                name="approval_human_required",
                id=request.req_id,
                method=request.method,
                source=decision_source,
            )
        human_request = ApprovalRequest(
            req_id=request.req_id,
            method=request.method,
            params=request.params,
            role=role or "agent",
        )
        raw_human_decision: str
        if decide_human is not None:
            raw_human_decision = decide_human(human_request)
        elif approval_decision_provider is not None:
            raw_human_decision = approval_decision_provider.decide(human_request)
        else:
            raise RuntimeError("approval requires human decision but no decision provider configured")

        normalized = ApprovalPolicy.normalize_human_decision_with_always(
            raw=raw_human_decision,
            allow_always=ApprovalPolicy.is_approval_method(request.method),
        )
        if normalized == "always_allow":
            decision = "accept"
            always_allow = True
        else:
            decision = normalized
        if log_human_decision:
            _event(
                logger=logger,
                role=role,
                name="approval_human_decision",
                id=request.req_id,
                method=request.method,
                decision=decision,
                always_allow=always_allow,
            )
    else:
        if log_auto_decision:
            _event(
                logger=logger,
                role=role,
                name="approval_auto_decision",
                id=request.req_id,
                method=request.method,
                decision=decision,
                source=decision_source,
                command=command_text,
            )

    result: dict = {"decision": decision}
    if always_allow:
        if always_allow_commands is not None and isinstance(command_from_params, str) and command_from_params:
            always_allow_commands.add(command_from_params)
        accept_settings = build_accept_settings(params=request.params)
        if accept_settings is not None:
            result["acceptSettings"] = accept_settings
            _event(
                logger=logger,
                role=role,
                name="approval_accept_settings_applied",
                id=request.req_id,
                accept_settings=accept_settings,
            )
        else:
            _event(
                logger=logger,
                role=role,
                name="approval_accept_settings_skipped",
                id=request.req_id,
                reason="missing proposedExecpolicyAmendment",
            )

    via = "interactive" if human_flow else "policy_auto"

    return ApprovalResponsePlan(
        req_id=request.req_id,
        result=result,
        via=via,
        method=request.method,
        params=request.params,
        decision=decision,
        source=decision_source,
        command=command_text,
        always_allow=always_allow,
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


def _log_regex_allowlist_trace(
    *,
    logger: LifecycleLogger,
    role: str | None,
    request: ApprovalServerRequest,
    decision: str,
    decision_source: str,
    command: str,
    regex_trace,
    approval_notify: Callable[[str], None] | None,
) -> None:
    if regex_trace is None:
        return
    if regex_trace.matched:
        _event(
            logger=logger,
            role=role,
            name="approval_regex_allowlist_match",
            id=request.req_id,
            method=request.method,
            command=command,
            file_path=regex_trace.file_path,
            matched_pattern=regex_trace.matched_pattern,
        )
        if (
            decision == "accept"
            and decision_source == "allow_regex"
            and isinstance(regex_trace.matched_pattern, str)
            and approval_notify is not None
        ):
            note = format_regex_auto_approval_notification(
                command=command,
                matched_pattern=regex_trace.matched_pattern,
                file_path=regex_trace.file_path,
            )
            approval_notify(note)
            _event(
                logger=logger,
                role=role,
                name="approval_auto_notify_sent",
                id=request.req_id,
                source=decision_source,
                command=command,
                matched_pattern=regex_trace.matched_pattern,
            )
    else:
        _event(
            logger=logger,
            role=role,
            name="approval_regex_allowlist_not_matched",
            id=request.req_id,
            method=request.method,
            command=command,
            file_path=regex_trace.file_path,
        )
        if (
            decision == "human"
            and decision_source == "regex_fallback_human"
            and approval_notify is not None
        ):
            note = format_regex_fallback_human_notification(
                command=command,
                file_path=regex_trace.file_path,
                parse_errors=regex_trace.parse_errors,
            )
            approval_notify(note)
            _event(
                logger=logger,
                role=role,
                name="approval_auto_notify_sent",
                id=request.req_id,
                source=decision_source,
                command=command,
            )
    for parse_error in regex_trace.parse_errors:
        _event(
            logger=logger,
            role=role,
            name="approval_regex_allowlist_invalid_pattern",
            id=request.req_id,
            method=request.method,
            file_path=regex_trace.file_path,
            error=parse_error,
        )
