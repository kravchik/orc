"""Shared approval policy and decision provider primitives."""

from __future__ import annotations

from dataclasses import dataclass
import re
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class ApprovalRequest:
    req_id: int
    method: str
    params: dict
    role: str


class ApprovalDecisionProvider(Protocol):
    def decide(self, request: ApprovalRequest) -> str: ...


@dataclass(frozen=True)
class ApprovalRegexAllowlistTrace:
    file_path: str
    matched: bool
    matched_pattern: str | None = None
    parse_errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class ApprovalDecisionTrace:
    decision: str
    source: str
    command: str
    regex_trace: ApprovalRegexAllowlistTrace | None = None


@dataclass(frozen=True)
class ApprovalPolicy:
    allow_commands: frozenset[str]
    deny_commands: frozenset[str]
    default_decision: str = "decline"  # accept | decline | human
    allow_regex_file: str = ""

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
    ) -> "ApprovalPolicy":
        allow = {part.strip() for part in allow_csv.split(",") if part.strip()}
        deny = {part.strip() for part in deny_csv.split(",") if part.strip()}
        decision = default_decision.strip().lower()
        if decision not in ("accept", "decline", "human"):
            raise ValueError("--approval-default-decision must be accept, decline, or human")
        regex_file = allow_regex_file.strip()
        return cls(
            allow_commands=frozenset(allow),
            deny_commands=frozenset(deny),
            default_decision=decision,
            allow_regex_file=regex_file,
        )

    def decide(self, method: str, params: dict) -> str:
        return self.decide_with_trace(method=method, params=params).decision

    def decide_with_trace(self, method: str, params: dict) -> ApprovalDecisionTrace:
        command = self._extract_command(method=method, params=params)
        if command in self.deny_commands:
            return ApprovalDecisionTrace(decision="decline", source="deny_list", command=command)
        if command in self.allow_commands:
            return ApprovalDecisionTrace(decision="accept", source="allow_list", command=command)
        regex_trace = self._match_allow_regex(command)
        if regex_trace is not None and regex_trace.matched:
            return ApprovalDecisionTrace(
                decision="accept",
                source="allow_regex",
                command=command,
                regex_trace=regex_trace,
            )
        if regex_trace is not None and regex_trace.parse_errors:
            return ApprovalDecisionTrace(
                decision="human",
                source="regex_fallback_human",
                command=command,
                regex_trace=regex_trace,
            )
        if regex_trace is not None:
            return ApprovalDecisionTrace(
                decision=self.default_decision,
                source="default",
                command=command,
                regex_trace=regex_trace,
            )
        return ApprovalDecisionTrace(decision=self.default_decision, source="default", command=command)

    @staticmethod
    def is_approval_method(method: str) -> bool:
        return method in ("item/commandExecution/requestApproval", "item/fileChange/requestApproval")

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
        if method == "item/commandExecution/requestApproval":
            cmd = params.get("command")
            return cmd if isinstance(cmd, str) else ""
        if method == "item/fileChange/requestApproval":
            tool = params.get("tool")
            return tool if isinstance(tool, str) else ""
        return ""

    def _match_allow_regex(self, command: str) -> ApprovalRegexAllowlistTrace | None:
        regex_file = self.allow_regex_file.strip()
        if not regex_file:
            return None
        path = Path(regex_file)
        patterns: list[tuple[str, re.Pattern[str]]] = []
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
                patterns.append((raw, re.compile(raw)))
            except re.error as exc:
                parse_errors.append(f"line {line_no}: invalid regex {raw!r}: {exc}")
        for source_pattern, pattern in patterns:
            if pattern.search(command):
                return ApprovalRegexAllowlistTrace(
                    file_path=str(path),
                    matched=True,
                    matched_pattern=source_pattern,
                    parse_errors=tuple(parse_errors),
                )
        return ApprovalRegexAllowlistTrace(
            file_path=str(path),
            matched=False,
            parse_errors=tuple(parse_errors),
        )


def build_accept_settings(params: dict) -> dict | None:
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
    return {"execpolicyAmendment": tokens}


def format_regex_auto_approval_notification(
    *,
    command: str,
    matched_pattern: str,
    file_path: str,
) -> str:
    return (
        "[AUTO-APPROVAL][REGEX] accepted automatically\n"
        f"command: {command}\n"
        f"pattern: {matched_pattern}\n"
        f"file: {file_path}"
    )


def format_regex_fallback_human_notification(
    *,
    command: str,
    file_path: str,
    parse_errors: tuple[str, ...],
) -> str:
    details = "; ".join(parse_errors[:3]) if parse_errors else "unknown regex-file issue"
    return (
        "[AUTO-APPROVAL][REGEX] fallback to human\n"
        f"command: {command}\n"
        f"file: {file_path}\n"
        f"reason: {details}"
    )
