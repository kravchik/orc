"""Shared Steward command catalog and local command parsing."""

from __future__ import annotations

from dataclasses import dataclass


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


SHAMAN_USAGE = "/shaman [show|assign <address>|rename <address>|remove]"
GRUNT_USAGE = "/grunt [show|assign <address>|rename <address>|remove]"
APPROVER_USAGE = "/approver [show|assign <address>|change <address>|clear]"


STEWARD_COMMANDS = (
    StewardCommandSpec("bind", "start and bind a runtime agent to this access point."),
    StewardCommandSpec("help", "show this help.", local_immediate=True),
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


def extract_fallback_command(raw_text: str) -> str | None:
    stripped = str(raw_text or "").strip()
    if not stripped.startswith("/"):
        return None
    head = stripped.split(None, 1)[0].lower()
    command = head[1:].split("@", 1)[0]
    spec = _COMMAND_BY_NAME.get(command)
    if spec is None or not spec.fallback_handler:
        return None
    return command


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
    if operation in {"assign", "change"}:
        if len(operation_args) != 1:
            return ApproverCommand(
                operation=operation,
                usage_error=f"usage: /approver {operation} <address>",
            )
        return ApproverCommand(operation=operation, address=operation_args[0])
    if operation == "clear":
        if operation_args:
            return ApproverCommand(operation=operation, usage_error="usage: /approver clear")
        return ApproverCommand(operation=operation)
    return ApproverCommand(operation=operation, usage_error=f"usage: {APPROVER_USAGE}")


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


def build_steward_help_text(*, state: str) -> str:
    lines = [f"access point state: {state}", "", "fallback commands:"]
    lines.extend(
        f"{spec.usage or f'/{spec.name}'} - {spec.description}"
        for spec in STEWARD_COMMANDS
    )
    return "\n".join(lines)
