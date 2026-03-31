"""Shared Slack entrypoint env-file validation helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Mapping

from orchestrator.env_utils import resolve_env


def resolve_required_slack_setting(
    raw: str,
    *,
    env_key: str,
    cli_flag: str,
    file_env: Mapping[str, str],
    env_file_path: str,
) -> str:
    value = resolve_env(raw, env_key, file_env)
    if value:
        return value
    env_file = Path(env_file_path)
    if env_file.exists():
        raise ValueError(
            f"missing required Slack setting {env_key} in {env_file_path} "
            f"(or pass {cli_flag} / set {env_key})"
        )
    raise ValueError(
        f"missing required Slack setting {env_key}; env file not found: {env_file_path} "
        f"(or pass {cli_flag} / set {env_key})"
    )
