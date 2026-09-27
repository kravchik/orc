"""Shared Steward command catalog and local command parsing."""

from __future__ import annotations

from dataclasses import dataclass

from orchestrator.user_commands import UserCommandInput, parse_user_command


@dataclass(frozen=True)
class StewardCommandSpec:
    name: str
    description: str
    local_immediate: bool = False
    fallback_handler: bool = True
    usage: str = ""


@dataclass(frozen=True)
class RoutingIdentityCommand:
    operation: str
    address: str = ""
    usage_error: str = ""


@dataclass(frozen=True)
class ApproverCommand:
    operation: str
    address: str = ""
    usage_error: str = ""


@dataclass(frozen=True)
class AgentsCommand:
    scope: str = "here"
    usage_error: str = ""


@dataclass(frozen=True)
class ResumeCommand:
    scope: str = "here"
    selector: str = ""
    usage_error: str = ""


@dataclass(frozen=True)
class InspectCommand:
    scope: str = "here"
    usage_error: str = ""


@dataclass(frozen=True)
class StewardHelpContext:
    project_cwd: str = ""
    has_binding: bool = False
    auto_start: bool = False


SHAMAN_USAGE = "/shaman [show|assign <address>|rename <address>|remove]"
GRUNT_USAGE = "/grunt [show|assign <address>|rename <address>|remove]"
APPROVER_USAGE = "/approver [show|assign <address|human>]"
AGENTS_USAGE = "/agents [here|all]"
RESUME_USAGE = "/resume [here|all] <name|uuid>"
INSPECT_USAGE = "/inspect [all]"


STEWARD_COMMANDS = (
    StewardCommandSpec("bind", "start and bind a runtime agent to this access point."),
    StewardCommandSpec("help", "show this help.", local_immediate=True),
    StewardCommandSpec(
        "agents",
        "show Codex sessions and their ORC access points.",
        local_immediate=True,
        usage=AGENTS_USAGE,
    ),
    StewardCommandSpec(
        "resume",
        "continue a Codex session in this access point.",
        local_immediate=True,
        usage=RESUME_USAGE,
    ),
    StewardCommandSpec(
        "approver",
        "show or change who approves this agent.",
        local_immediate=True,
        usage=APPROVER_USAGE,
    ),
    StewardCommandSpec(
        "shaman",
        "show or change Shaman routing identity for this binding.",
        local_immediate=True,
        usage=SHAMAN_USAGE,
    ),
    StewardCommandSpec(
        "grunt",
        "show or change Grunt routing identity for this binding.",
        local_immediate=True,
        usage=GRUNT_USAGE,
    ),
    StewardCommandSpec(
        "status",
        "show current access point, runtime state, and bound agent details.",
        local_immediate=True,
    ),
    StewardCommandSpec(
        "inspect",
        "show current API-backed session summary and tracked active work.",
        local_immediate=True,
        usage=INSPECT_USAGE,
    ),
    StewardCommandSpec(
        "interrupt",
        "interrupt the current in-flight turn for this access point.",
        local_immediate=True,
    ),
    StewardCommandSpec(
        "stop",
        "stop runtime agent for this access point (keeps binding; Steward stays active).",
    ),
    StewardCommandSpec(
        "start",
        "start runtime agent for existing binding (Steward stays active).",
    ),
    StewardCommandSpec(
        "reset",
        "clear binding and reset local runtime state for this access point.",
    ),
    StewardCommandSpec(
        "steward",
        "send one message to steward (available when state is BOUND).",
        fallback_handler=False,
        usage="/steward <message>",
    ),
)

_COMMAND_BY_NAME = {spec.name: spec for spec in STEWARD_COMMANDS}


def parse_steward_user_command(raw_text: str) -> UserCommandInput:
    return parse_user_command(raw_text, known_commands=_COMMAND_BY_NAME)


def extract_fallback_command(raw_text: str) -> str | None:
    parsed = parse_steward_user_command(raw_text)
    if not parsed.is_known:
        return None
    spec = _COMMAND_BY_NAME.get(parsed.name)
    if spec is None or not spec.fallback_handler:
        return None
    return parsed.name


def is_local_immediate_command(command: str | None) -> bool:
    if command is None:
        return False
    spec = _COMMAND_BY_NAME.get(command)
    return bool(spec is not None and spec.local_immediate)


def parse_shaman_command(raw_text: str) -> RoutingIdentityCommand:
    return _parse_routing_identity_command(raw_text, command="shaman", usage=SHAMAN_USAGE)


def parse_grunt_command(raw_text: str) -> RoutingIdentityCommand:
    return _parse_routing_identity_command(raw_text, command="grunt", usage=GRUNT_USAGE)


def parse_approver_command(raw_text: str) -> ApproverCommand:
    parts = str(raw_text or "").strip().split()
    if not parts:
        return ApproverCommand(operation="", usage_error=f"usage: {APPROVER_USAGE}")
    args = parts[1:]
    if not args:
        return ApproverCommand(operation="show")
    operation = args[0].lower()
    operation_args = args[1:]
    if operation == "show":
        if operation_args:
            return ApproverCommand(operation=operation, usage_error="usage: /approver show")
        return ApproverCommand(operation=operation)
    if operation == "assign":
        if len(operation_args) != 1:
            return ApproverCommand(
                operation=operation,
                usage_error="usage: /approver assign <address|human>",
            )
        return ApproverCommand(operation=operation, address=operation_args[0])
    return ApproverCommand(operation=operation, usage_error=f"usage: {APPROVER_USAGE}")


def parse_agents_command(raw_text: str) -> AgentsCommand:
    parts = str(raw_text or "").strip().split()
    args = parts[1:]
    if not args:
        return AgentsCommand()
    if len(args) == 1 and args[0].lower() in {"here", "all"}:
        return AgentsCommand(scope=args[0].lower())
    return AgentsCommand(usage_error=f"usage: {AGENTS_USAGE}")


def parse_resume_command(raw_text: str) -> ResumeCommand:
    parts = str(raw_text or "").strip().split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return ResumeCommand(usage_error=f"usage: {RESUME_USAGE}")
    remainder = parts[1].strip()
    scope_parts = remainder.split(maxsplit=1)
    explicit_scope = scope_parts[0].lower()
    if explicit_scope not in {"here", "all"}:
        return ResumeCommand(selector=remainder)
    if len(scope_parts) < 2 or not scope_parts[1].strip():
        return ResumeCommand(scope=explicit_scope, usage_error=f"usage: {RESUME_USAGE}")
    return ResumeCommand(scope=explicit_scope, selector=scope_parts[1].strip())


def parse_inspect_command(raw_text: str) -> InspectCommand:
    parts = str(raw_text or "").strip().split()
    args = parts[1:]
    if not args:
        return InspectCommand()
    if len(args) == 1 and args[0].lower() == "all":
        return InspectCommand(scope="all")
    return InspectCommand(usage_error=f"usage: {INSPECT_USAGE}")


def _parse_routing_identity_command(
    raw_text: str,
    *,
    command: str,
    usage: str,
) -> RoutingIdentityCommand:
    parts = str(raw_text or "").strip().split()
    if not parts:
        return RoutingIdentityCommand(operation="", usage_error=f"usage: {usage}")
    args = parts[1:]
    if not args:
        return RoutingIdentityCommand(operation="show")
    operation = args[0].lower()
    operation_args = args[1:]
    if operation == "show":
        if operation_args:
            return RoutingIdentityCommand(operation=operation, usage_error=f"usage: /{command} show")
        return RoutingIdentityCommand(operation=operation)
    if operation in {"assign", "rename"}:
        if len(operation_args) != 1:
            return RoutingIdentityCommand(
                operation=operation,
                usage_error=f"usage: /{command} {operation} <address>",
            )
        return RoutingIdentityCommand(operation=operation, address=operation_args[0])
    if operation == "remove":
        if operation_args:
            return RoutingIdentityCommand(operation=operation, usage_error=f"usage: /{command} remove")
        return RoutingIdentityCommand(operation=operation)
    return RoutingIdentityCommand(operation=operation, usage_error=f"usage: {usage}")


def build_steward_help_text(
    *,
    state: str,
    startup: StewardHelpContext,
) -> str:
    lines = [f"access point state: {state}"]
    if state == "BOUND_IDLE" and startup.has_binding:
        lines.extend(["", "startup:"])
        if startup.auto_start:
            lines.extend(
                [
                    "Runtime agent is waiting for automatic startup after ORC restart.",
                    "An ordinary message, routed request, or approval will start it.",
                ]
            )
        else:
            lines.extend(
                [
                    "Runtime agent is bound but stopped.",
                    "Ordinary messages go to Keeper. Use /start to start the agent.",
                ]
            )
    elif state == "UNBOUND":
        lines.extend(["", "startup:"])
        if startup.project_cwd:
            lines.extend(
                [
                    "No runtime agent is bound.",
                    f"Folder: {startup.project_cwd}",
                    "Use /bind or /resume here <name|uuid>.",
                ]
            )
        else:
            lines.extend(
                [
                    "No runtime agent or folder is bound.",
                    "Use /bind or /resume all <name|uuid>.",
                ]
            )
    lines.extend(["", "fallback commands:"])
    lines.extend(
        f"{spec.usage or f'/{spec.name}'} - {spec.description}"
        for spec in STEWARD_COMMANDS
    )
    return "\n".join(lines)
