"""Shared approval policy and decision provider primitives."""

from __future__ import annotations

from dataclasses import dataclass
import math
import re
import time
from pathlib import Path
from typing import Protocol

from orchestrator.bounded_regex import search_regex_with_timeout
from orchestrator.file_change_approval import extract_normalized_file_change_paths


COMMAND_APPROVAL_METHOD = "item/commandExecution/requestApproval"
FILE_CHANGE_APPROVAL_METHOD = "item/fileChange/requestApproval"


@dataclass(frozen=True)
class ApprovalRequest:
    req_id: int
    method: str
    params: dict
    role: str


class ApprovalDecisionProvider(Protocol):
    def decide(self, request: ApprovalRequest) -> str: ...


@dataclass(frozen=True)
class ApprovalRegexTimeout:
    line_number: int
    pattern: str
    duration_sec: float


@dataclass(frozen=True)
class ApprovalRegexBudgetExhausted:
    budget_sec: float
    duration_sec: float


@dataclass(frozen=True)
class ApprovalRegexAllowlistTrace:
    file_path: str
    matched: bool
    matched_pattern: str | None = None
    parse_errors: tuple[str, ...] = ()
    match_errors: tuple[str, ...] = ()
    matched_patterns: tuple[str, ...] = ()
    matched_values: tuple[str, ...] = ()
    unmatched_values: tuple[str, ...] = ()
    timed_out_patterns: tuple[ApprovalRegexTimeout, ...] = ()
    budget_exhausted: ApprovalRegexBudgetExhausted | None = None


@dataclass(frozen=True)
class ApprovalDecisionTrace:
    decision: str
    source: str
    command: str
    regex_trace: ApprovalRegexAllowlistTrace | None = None
    affected_paths: tuple[str, ...] = ()


@dataclass(frozen=True)
class ApprovalPolicy:
    allow_commands: frozenset[str]
    deny_commands: frozenset[str]
    default_decision: str = "decline"  # accept | decline | human
    allow_regex_file: str = ""
    regex_timeout_sec: float = 5.0
    regex_total_budget_sec: float = 60.0

    @classmethod
    def default(cls) -> "ApprovalPolicy":
        return cls(
            allow_commands=frozenset(),
            deny_commands=frozenset(),
            default_decision="decline",
        )

    @classmethod
    def from_csv(
        cls,
        allow_csv: str,
        deny_csv: str,
        default_decision: str = "decline",
        allow_regex_file: str = "",
        regex_timeout_sec: float = 5.0,
        regex_total_budget_sec: float = 60.0,
    ) -> "ApprovalPolicy":
        allow = {part.strip() for part in allow_csv.split(",") if part.strip()}
        deny = {part.strip() for part in deny_csv.split(",") if part.strip()}
        decision = default_decision.strip().lower()
        if decision not in ("accept", "decline", "human"):
            raise ValueError("--approval-default-decision must be accept, decline, or human")
        regex_file = allow_regex_file.strip()
        timeout_sec = float(regex_timeout_sec)
        if not math.isfinite(timeout_sec) or timeout_sec <= 0:
            raise ValueError("approval regex timeout must be positive")
        total_budget_sec = float(regex_total_budget_sec)
        if not math.isfinite(total_budget_sec) or total_budget_sec <= 0:
            raise ValueError("approval regex total budget must be positive")
        return cls(
            allow_commands=frozenset(allow),
            deny_commands=frozenset(deny),
            default_decision=decision,
            allow_regex_file=regex_file,
            regex_timeout_sec=timeout_sec,
            regex_total_budget_sec=total_budget_sec,
        )

    def decide(self, method: str, params: dict) -> str:
        return self.decide_with_trace(method=method, params=params).decision

    def decide_with_trace(self, method: str, params: dict) -> ApprovalDecisionTrace:
        command = self._extract_command(method=method, params=params)
        affected_paths = (
            extract_normalized_file_change_paths(params)
            if method == FILE_CHANGE_APPROVAL_METHOD
            else ()
        )
        if command in self.deny_commands:
            return ApprovalDecisionTrace(
                decision="decline",
                source="deny_list",
                command=command,
                affected_paths=affected_paths,
            )
        if command in self.allow_commands:
            return ApprovalDecisionTrace(
                decision="accept",
                source="allow_list",
                command=command,
                affected_paths=affected_paths,
            )
        regex_values = affected_paths if method == FILE_CHANGE_APPROVAL_METHOD else (command,)
        regex_trace = self._match_allow_regex_values(regex_values)
        if regex_trace is not None and regex_trace.matched:
            return ApprovalDecisionTrace(
                decision="accept",
                source="allow_regex",
                command=command,
                regex_trace=regex_trace,
                affected_paths=affected_paths,
            )
        if regex_trace is not None and (
            regex_trace.parse_errors
            or regex_trace.match_errors
            or regex_trace.timed_out_patterns
            or regex_trace.budget_exhausted is not None
        ):
            return ApprovalDecisionTrace(
                decision="human",
                source="regex_fallback_human",
                command=command,
                regex_trace=regex_trace,
                affected_paths=affected_paths,
            )
        if regex_trace is not None:
            return ApprovalDecisionTrace(
                decision=self.default_decision,
                source="default",
                command=command,
                regex_trace=regex_trace,
                affected_paths=affected_paths,
            )
        return ApprovalDecisionTrace(
            decision=self.default_decision,
            source="default",
            command=command,
            affected_paths=affected_paths,
        )

    @staticmethod
    def is_approval_method(method: str) -> bool:
        return method in (COMMAND_APPROVAL_METHOD, FILE_CHANGE_APPROVAL_METHOD)

    @staticmethod
    def normalize_human_decision(raw: str) -> str:
        decision = raw.strip().lower()
        if decision not in ("accept", "decline"):
            raise ValueError("approval decision must be accept or decline")
        return decision

    @staticmethod
    def normalize_human_decision_with_always(raw: str, allow_always: bool) -> str:
        decision = raw.strip().lower()
        allowed = {"accept", "decline"}
        if allow_always:
            allowed.add("always_allow")
        if decision not in allowed:
            if allow_always:
                raise ValueError("approval decision must be accept, decline, or always_allow")
            raise ValueError("approval decision must be accept or decline")
        return decision

    @staticmethod
    def _extract_command(method: str, params: dict) -> str:
        if method == COMMAND_APPROVAL_METHOD:
            cmd = params.get("command")
            return cmd if isinstance(cmd, str) else ""
        if method == FILE_CHANGE_APPROVAL_METHOD:
            tool = params.get("tool")
            return tool if isinstance(tool, str) else ""
        return ""

    def _match_allow_regex_values(
        self,
        values: tuple[str, ...],
    ) -> ApprovalRegexAllowlistTrace | None:
        regex_file = self.allow_regex_file.strip()
        if not regex_file:
            return None
        path = Path(regex_file)
        patterns: list[tuple[int, str, re.Pattern[str]]] = []
        parse_errors: list[str] = []
        try:
            content = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            parse_errors.append(f"line 0: file not found: {path}")
            return ApprovalRegexAllowlistTrace(
                file_path=str(path),
                matched=False,
                parse_errors=tuple(parse_errors),
            )
        except OSError as exc:
            parse_errors.append(f"line 0: read error: {exc}")
            return ApprovalRegexAllowlistTrace(
                file_path=str(path),
                matched=False,
                parse_errors=tuple(parse_errors),
            )
        for line_no, line in enumerate(content.splitlines(), start=1):
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            try:
                patterns.append((line_no, raw, re.compile(raw)))
            except re.error as exc:
                parse_errors.append(f"line {line_no}: invalid regex {raw!r}: {exc}")
        matched_patterns: list[str] = []
        matched_values: list[str] = []
        unmatched_values: list[str] = []
        match_errors: list[str] = []
        timed_out_by_line: dict[int, ApprovalRegexTimeout] = {}
        matching_started_at = time.monotonic()
        deadline = matching_started_at + self.regex_total_budget_sec
        budget_exhausted: ApprovalRegexBudgetExhausted | None = None
        for value_index, value in enumerate(values):
            value_pattern = None
            for line_number, source_pattern, pattern in patterns:
                remaining_sec = deadline - time.monotonic()
                if remaining_sec <= 0:
                    budget_exhausted = ApprovalRegexBudgetExhausted(
                        budget_sec=self.regex_total_budget_sec,
                        duration_sec=time.monotonic() - matching_started_at,
                    )
                    break
                result = search_regex_with_timeout(
                    pattern=pattern,
                    value=value,
                    timeout_sec=min(self.regex_timeout_sec, remaining_sec),
                )
                if result.error:
                    match_errors.append(
                        f"line {line_number}: regex match error {source_pattern!r}: {result.error}"
                    )
                    if time.monotonic() >= deadline:
                        budget_exhausted = ApprovalRegexBudgetExhausted(
                            budget_sec=self.regex_total_budget_sec,
                            duration_sec=time.monotonic() - matching_started_at,
                        )
                        break
                    continue
                if result.timed_out:
                    timed_out_by_line.setdefault(
                        line_number,
                        ApprovalRegexTimeout(
                            line_number=line_number,
                            pattern=source_pattern,
                            duration_sec=result.duration_sec,
                        ),
                    )
                if time.monotonic() >= deadline:
                    budget_exhausted = ApprovalRegexBudgetExhausted(
                        budget_sec=self.regex_total_budget_sec,
                        duration_sec=time.monotonic() - matching_started_at,
                    )
                    break
                if result.timed_out:
                    continue
                if result.matched:
                    value_pattern = source_pattern
                    break
            if budget_exhausted is not None:
                unmatched_values.extend(values[value_index:])
                break
            if value_pattern is None:
                unmatched_values.append(value)
                continue
            matched_values.append(value)
            matched_patterns.append(value_pattern)
        matched = bool(values) and not unmatched_values
        return ApprovalRegexAllowlistTrace(
            file_path=str(path),
            matched=matched,
            matched_pattern=matched_patterns[0] if matched_patterns else None,
            parse_errors=tuple(parse_errors),
            match_errors=tuple(match_errors),
            matched_patterns=tuple(dict.fromkeys(matched_patterns)),
            matched_values=tuple(matched_values),
            unmatched_values=tuple(unmatched_values),
            timed_out_patterns=tuple(timed_out_by_line.values()),
            budget_exhausted=budget_exhausted,
        )


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


def format_regex_auto_approval_notification(
    *,
    command: str,
    matched_pattern: str,
    file_path: str,
    affected_paths: tuple[str, ...] = (),
    matched_patterns: tuple[str, ...] = (),
) -> str:
    lines = ["[AUTO-APPROVAL][REGEX] accepted automatically"]
    if affected_paths:
        lines.append("paths:")
        lines.extend(f"- {path}" for path in affected_paths)
    else:
        lines.append(f"command: {command}")
    patterns = matched_patterns or (matched_pattern,)
    if len(patterns) == 1:
        lines.append(f"pattern: {patterns[0]}")
    else:
        lines.append("patterns:")
        lines.extend(f"- {pattern}" for pattern in patterns)
    lines.append(f"file: {file_path}")
    return "\n".join(lines)


def format_regex_fallback_human_notification(
    *,
    command: str,
    file_path: str,
    parse_errors: tuple[str, ...],
    match_errors: tuple[str, ...] = (),
    affected_paths: tuple[str, ...] = (),
) -> str:
    errors = parse_errors + match_errors
    details = "; ".join(errors[:3]) if errors else "unknown regex-file issue"
    lines = ["[AUTO-APPROVAL][REGEX] fallback to human"]
    if affected_paths:
        lines.append("paths:")
        lines.extend(f"- {path}" for path in affected_paths)
    else:
        lines.append(f"command: {command}")
    lines.extend((f"file: {file_path}", f"reason: {details}"))
    return "\n".join(lines)


def format_regex_timeout_notification(
    *,
    file_path: str,
    timeout: ApprovalRegexTimeout,
    method: str,
    command: str = "",
    affected_paths: tuple[str, ...] = (),
) -> str:
    lines = [
        "[AUTO-APPROVAL][REGEX] pattern timed out",
        f"file: {file_path}",
        f"line: {timeout.line_number}",
        f"duration: {timeout.duration_sec:.3f}s",
        f"request: {method}",
    ]
    if affected_paths:
        lines.append("paths:")
        lines.extend(f"- {path}" for path in affected_paths)
    elif command:
        lines.append(f"command: {command}")
    lines.append(
        "action: fix this regex; it was ignored for this approval"
    )
    return "\n".join(lines)


def format_regex_budget_exhausted_notification(
    *,
    file_path: str,
    budget: ApprovalRegexBudgetExhausted,
    method: str,
    command: str = "",
    affected_paths: tuple[str, ...] = (),
) -> str:
    lines = [
        "[AUTO-APPROVAL][REGEX] total budget exhausted",
        f"file: {file_path}",
        f"request: {method}",
        f"budget: {budget.budget_sec:.3f}s",
        f"duration: {budget.duration_sec:.3f}s",
    ]
    if affected_paths:
        lines.append("paths:")
        lines.extend(f"- {path}" for path in affected_paths)
    elif command:
        lines.append(f"command: {command}")
    lines.append(
        "action: fix slow regexes; no regex auto-approval was applied"
    )
    return "\n".join(lines)
