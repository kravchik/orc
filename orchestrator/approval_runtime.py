"""Shared runtime helpers for handling app-server approval requests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from orchestrator.approval import (
    ApprovalDecisionTrace,
    ApprovalDecisionProvider,
    ApprovalPolicy,
    ApprovalRequest,
    COMMAND_APPROVAL_METHOD,
    approval_response_decision_name,
    build_session_approval_response,
    format_regex_auto_approval_notification,
    format_regex_budget_exhausted_notification,
    format_regex_fallback_human_notification,
    format_regex_timeout_notification,
    supports_session_approval,
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
    decision_trace: ApprovalDecisionTrace | None = None,
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

    cached_plan = build_always_allow_cached_response_plan(
        request=request,
        logger=logger,
        role=role,
        always_allow_commands=always_allow_commands,
        log_auto_decision=log_auto_decision,
    )
    if cached_plan is not None:
        return cached_plan

    command_from_params = request.params.get("command")

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
    if decision_trace is None and approval_policy is not None:
        decision_trace = approval_policy.decide_with_trace(
            method=request.method,
            params=request.params,
        )
    if decision_trace is not None:
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
            affected_paths=decision_trace.affected_paths,
            regex_trace=decision_trace.regex_trace,
            approval_notify=approval_notify,
        )

    always_allow = False
    session_result: dict | None = None
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
            allow_always=supports_session_approval(
                method=request.method,
                params=request.params,
            ),
        )
        if normalized == "always_allow":
            always_allow = True
            session_result = build_session_approval_response(
                method=request.method,
                params=request.params,
            )
            if session_result is None:
                raise RuntimeError("session approval is unavailable for this request")
            decision = approval_response_decision_name(session_result)
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

    result: dict = session_result or {"decision": decision}
    if always_allow:
        if (
            request.method == COMMAND_APPROVAL_METHOD
            and always_allow_commands is not None
            and isinstance(command_from_params, str)
            and command_from_params
        ):
            always_allow_commands.add(command_from_params)
        log_session_approval_applied(
            logger=logger,
            role=role,
            req_id=request.req_id,
            method=request.method,
            result=result,
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


def _log_regex_allowlist_trace(
    *,
    logger: LifecycleLogger,
    role: str | None,
    request: ApprovalServerRequest,
    decision: str,
    decision_source: str,
    command: str,
    affected_paths: tuple[str, ...],
    regex_trace,
    approval_notify: Callable[[str], None] | None,
) -> None:
    if regex_trace is None:
        return
    for timeout in regex_trace.timed_out_patterns:
        _event(
            logger=logger,
            role=role,
            name="approval_regex_allowlist_timeout",
            id=request.req_id,
            method=request.method,
            file_path=regex_trace.file_path,
            line_number=timeout.line_number,
            pattern=timeout.pattern,
            duration_sec=timeout.duration_sec,
            severity="warning",
            warning="approval regex timed out and requires correction",
            action_required="fix_regex",
            command=command,
            affected_paths=list(affected_paths),
        )
        if approval_notify is not None and regex_trace.budget_exhausted is None:
            approval_notify(
                format_regex_timeout_notification(
                    file_path=regex_trace.file_path,
                    timeout=timeout,
                    method=request.method,
                    command=command,
                    affected_paths=affected_paths,
                )
            )
            _event(
                logger=logger,
                role=role,
                name="approval_regex_timeout_warning_sent",
                id=request.req_id,
                method=request.method,
                file_path=regex_trace.file_path,
                line_number=timeout.line_number,
                duration_sec=timeout.duration_sec,
                command=command,
                affected_paths=list(affected_paths),
            )
    budget_exhausted = regex_trace.budget_exhausted
    if budget_exhausted is not None:
        _event(
            logger=logger,
            role=role,
            name="approval_regex_allowlist_budget_exhausted",
            id=request.req_id,
            method=request.method,
            file_path=regex_trace.file_path,
            budget_sec=budget_exhausted.budget_sec,
            duration_sec=budget_exhausted.duration_sec,
            severity="warning",
            warning="approval regex total budget exhausted and requires correction",
            action_required="fix_regex",
            command=command,
            affected_paths=list(affected_paths),
        )
        if approval_notify is not None:
            approval_notify(
                format_regex_budget_exhausted_notification(
                    file_path=regex_trace.file_path,
                    budget=budget_exhausted,
                    method=request.method,
                    command=command,
                    affected_paths=affected_paths,
                )
            )
            _event(
                logger=logger,
                role=role,
                name="approval_regex_budget_warning_sent",
                id=request.req_id,
                method=request.method,
                file_path=regex_trace.file_path,
                budget_sec=budget_exhausted.budget_sec,
                duration_sec=budget_exhausted.duration_sec,
                command=command,
                affected_paths=list(affected_paths),
            )
    if regex_trace.matched:
        _event(
            logger=logger,
            role=role,
            name="approval_regex_allowlist_match",
            id=request.req_id,
            method=request.method,
            command=command,
            affected_paths=list(affected_paths),
            file_path=regex_trace.file_path,
            matched_pattern=regex_trace.matched_pattern,
            matched_patterns=list(regex_trace.matched_patterns),
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
                affected_paths=affected_paths,
                matched_patterns=regex_trace.matched_patterns,
            )
            approval_notify(note)
            _event(
                logger=logger,
                role=role,
                name="approval_auto_notify_sent",
                id=request.req_id,
                source=decision_source,
                command=command,
                affected_paths=list(affected_paths),
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
            affected_paths=list(affected_paths),
            unmatched_values=list(regex_trace.unmatched_values),
            file_path=regex_trace.file_path,
        )
        if (
            decision == "human"
            and decision_source == "regex_fallback_human"
            and regex_trace.budget_exhausted is None
            and (regex_trace.parse_errors or regex_trace.match_errors)
            and approval_notify is not None
        ):
            note = format_regex_fallback_human_notification(
                command=command,
                file_path=regex_trace.file_path,
                parse_errors=regex_trace.parse_errors,
                match_errors=regex_trace.match_errors,
                affected_paths=affected_paths,
            )
            approval_notify(note)
            _event(
                logger=logger,
                role=role,
                name="approval_auto_notify_sent",
                id=request.req_id,
                source=decision_source,
                command=command,
                affected_paths=list(affected_paths),
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
    for match_error in regex_trace.match_errors:
        _event(
            logger=logger,
            role=role,
            name="approval_regex_allowlist_match_error",
            id=request.req_id,
            method=request.method,
            file_path=regex_trace.file_path,
            error=match_error,
            command=command,
            affected_paths=list(affected_paths),
        )
