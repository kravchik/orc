"""Shared Steward command catalog and local command parsing."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Callable

from orchestrator.agent_identity import ProviderExecutionProfile
from orchestrator.provider_installation import ProviderInstallationStatus
from orchestrator.runtime_profile_catalog import RuntimeProfileCatalog
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
class ProvidersCommand:
    usage_error: str = ""


@dataclass(frozen=True)
class ProvidersListing:
    text: str
    result: dict[str, object]


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
class ModelCommand:
    operation: str
    model: str = ""
    usage_error: str = ""


@dataclass(frozen=True)
class EffortCommand:
    operation: str
    effort: str = ""
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
PROVIDERS_USAGE = "/providers"
RESUME_USAGE = "/resume [here|all] <name|provider[/profile]:session-id>"
INSPECT_USAGE = "/inspect [all]"
MODEL_USAGE = "/model [exact-model-id]"
EFFORT_USAGE = "/effort [exact-value]"
_MODEL_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$")


STEWARD_COMMANDS = (
    StewardCommandSpec("bind", "start and bind a runtime agent to this access point."),
    StewardCommandSpec("help", "show this help.", local_immediate=True),
    StewardCommandSpec(
        "agents",
        "show sessions from configured provider profiles and their ORC access points.",
        local_immediate=True,
        usage=AGENTS_USAGE,
    ),
    StewardCommandSpec(
        "providers",
        "show configured provider profiles and local installation status.",
        local_immediate=True,
        usage=PROVIDERS_USAGE,
    ),
    StewardCommandSpec(
        "resume",
        "continue the selected provider/profile session in this access point.",
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
        "model",
        "show available models or request one exact model for this session.",
        local_immediate=True,
        usage=MODEL_USAGE,
    ),
    StewardCommandSpec(
        "effort",
        "show or request one exact effort value for this session.",
        local_immediate=True,
        usage=EFFORT_USAGE,
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


def parse_model_command(raw_text: str) -> ModelCommand:
    parsed = parse_user_command(raw_text, known_commands={"model"})
    if not parsed.is_known:
        return ModelCommand(operation="", usage_error=f"usage: {MODEL_USAGE}")
    if not parsed.arguments:
        return ModelCommand(operation="list")
    if not _MODEL_ID.fullmatch(parsed.arguments):
        return ModelCommand(operation="", usage_error=f"usage: {MODEL_USAGE}")
    return ModelCommand(operation="set", model=parsed.arguments)


def parse_effort_command(raw_text: str) -> EffortCommand:
    parsed = parse_user_command(raw_text, known_commands={"effort"})
    if not parsed.is_known:
        return EffortCommand(operation="", usage_error=f"usage: {EFFORT_USAGE}")
    if not parsed.arguments:
        return EffortCommand(operation="read")
    if not _MODEL_ID.fullmatch(parsed.arguments):
        return EffortCommand(operation="", usage_error=f"usage: {EFFORT_USAGE}")
    return EffortCommand(operation="set", effort=parsed.arguments)


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


def parse_providers_command(raw_text: str) -> ProvidersCommand:
    parsed = parse_user_command(raw_text, known_commands={"providers"})
    if not parsed.is_known or parsed.arguments:
        return ProvidersCommand(usage_error=f"usage: {PROVIDERS_USAGE}")
    return ProvidersCommand()


def build_providers_listing(
    *,
    profile_catalog: RuntimeProfileCatalog,
    installation_status: Callable[
        [ProviderExecutionProfile], ProviderInstallationStatus
    ],
) -> ProvidersListing:
    """Project configured profiles without starting or querying a provider runtime."""

    if not isinstance(profile_catalog, RuntimeProfileCatalog):
        raise ValueError("providers listing requires RuntimeProfileCatalog")
    profile_rows: list[dict[str, object]] = []
    warning_rows: list[dict[str, str]] = []
    sections: list[str] = []
    for definition in profile_catalog.definitions:
        execution_profile = definition.execution_profile
        contract_projection = profile_catalog.profile_contract_projection(
            execution_profile
        )
        integration = contract_projection.integration.value
        launch_settings: list[str] = []
        if contract_projection.allows_args_suffix:
            launch_settings.append("args=append")
        if contract_projection.approval_policy_values:
            launch_settings.append(
                "approval_policy="
                + "|".join(contract_projection.approval_policy_values)
            )
        if contract_projection.sandbox_values:
            launch_settings.append(
                "sandbox=" + "|".join(contract_projection.sandbox_values)
            )

        try:
            status = installation_status(execution_profile)
            if (
                not isinstance(status, ProviderInstallationStatus)
                or status.execution_profile != execution_profile
            ):
                raise ValueError("installation status identity mismatch")
            installation = (
                "available"
                if status.available
                else f"unavailable ({status.kind.value})"
            )
            if not status.available:
                warning_rows.append(
                    {
                        "provider": execution_profile.provider.value,
                        "profile": execution_profile.profile.value,
                        "reason": status.kind.value,
                    }
                )
        except Exception:
            installation = "check_failed"
            warning_rows.append(
                {
                    "provider": execution_profile.provider.value,
                    "profile": execution_profile.profile.value,
                    "reason": "check_failed",
                }
            )

        profile_row: dict[str, object] = {
            "provider": execution_profile.provider.value,
            "profile": execution_profile.profile.value,
            "default": definition.default,
            "integration": integration,
            "launch_settings": tuple(launch_settings),
            "installation": installation,
        }
        profile_rows.append(profile_row)
        sections.append(
            "\n".join(
                (
                    "profile: "
                    f"{execution_profile.provider.value}/{execution_profile.profile.value}",
                    f"default: {'yes' if definition.default else 'no'}",
                    f"integration: {integration}",
                    "launch settings: "
                    + ("; ".join(launch_settings) if launch_settings else "none"),
                    f"installation: {installation}",
                )
            )
        )

    lines = [f"providers: {len(profile_rows)}", "", "\n\n".join(sections)]
    if warning_rows:
        lines.extend(
            [
                "",
                "warnings:",
                *(
                    "warning: "
                    f"{row['provider']}/{row['profile']} installation "
                    + (
                        "check failed"
                        if row["reason"] == "check_failed"
                        else f"unavailable ({row['reason']})"
                    )
                    for row in warning_rows
                ),
            ]
        )
    lines.extend(
        [
            "",
            "Installation status checks only the configured local command and "
            "required local dependencies.",
            "It does not confirm credentials, backend, models, or protocol readiness.",
        ]
    )
    return ProvidersListing(
        text="\n".join(lines),
        result={
            "ok": True,
            "code": "shown_with_warnings" if warning_rows else "shown",
            "profiles": profile_rows,
            "warnings": warning_rows,
        },
    )


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
                    format_folder_only_state(startup.project_cwd),
                    "Use /bind or /resume here <name|provider[/profile]:session-id>.",
                ]
            )
        else:
            lines.extend(
                [
                    "No runtime agent or folder is bound.",
                    "Use /bind or /resume all <name|provider[/profile]:session-id>.",
                ]
            )
    lines.extend(["", "fallback commands:"])
    lines.extend(
        f"{spec.usage or f'/{spec.name}'} - {spec.description}"
        for spec in STEWARD_COMMANDS
    )
    return "\n".join(lines)


def format_folder_only_state(project_cwd: str) -> str:
    cwd = str(project_cwd or "").strip()
    if not cwd:
        raise ValueError("folder-only state requires a non-empty project cwd")
    return f"Folder: {cwd}"
