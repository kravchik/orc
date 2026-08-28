"""Shared classification for user-facing slash commands."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Collection


UNKNOWN_USER_COMMAND_TEXT = "unknown command. Use /help to see available commands."
AGENT_PROXY_COMMANDS = frozenset(
    {
        "help",
        "start",
        "status",
        "inspect",
        "compact",
        "model",
        "interrupt",
        "stop",
        "quit",
    }
)
PROXY_TUI_COMMANDS = frozenset({"help", "status", "orc-status", "inspect", "interrupt"})
PROXY_TUI_TELEGRAM_COMMANDS = frozenset(
    {"help", "start", "status", "orc-status", "inspect", "interrupt", "stop", "quit"}
)


@dataclass(frozen=True)
class UserCommandInput:
    is_command: bool
    is_known: bool
    name: str = ""
    arguments: str = ""

    @property
    def is_unknown(self) -> bool:
        return self.is_command and not self.is_known


def parse_user_command(
    raw_text: str,
    *,
    known_commands: Collection[str],
) -> UserCommandInput:
    """Classify a leading slash token without interpreting command arguments."""
    stripped = str(raw_text or "").strip()
    if not stripped.startswith("/"):
        return UserCommandInput(is_command=False, is_known=False)

    parts = stripped.split(maxsplit=1)
    head = parts[0]
    command_token = head.split("@", 1)[0].lower()
    name = command_token[1:]
    known = {
        str(command).strip().lower().lstrip("/")
        for command in known_commands
    }
    return UserCommandInput(
        is_command=True,
        is_known=bool(name) and name in known,
        name=name,
        arguments=parts[1].strip() if len(parts) == 2 else "",
    )
