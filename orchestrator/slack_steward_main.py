"""Standalone Slack entrypoint for shared steward runtime."""

from __future__ import annotations

import argparse
import sys
import shlex
from pathlib import Path
from typing import Sequence

from orchestrator.approval import ApprovalPolicy
from orchestrator.env_utils import load_env_file
from orchestrator.processes import LifecycleLogger
from orchestrator.slack_env_config import resolve_required_slack_setting
from orchestrator.slack_interactivity import SlackSocketModeSource
from orchestrator.slack_steward import run_slack_steward


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="slack-steward")
    parser.add_argument("--env-file", default="tokens.txt", help="local KEY=VALUE file (default: tokens.txt)")
    parser.add_argument("--log-path", default="logs/slack-steward.jsonl")
    parser.add_argument("--token", default="")
    parser.add_argument("--slack-api-base-url", default="")
    parser.add_argument(
        "--channel-id",
        default="",
        help="legacy optional polling channel filter; ignored for Socket Mode multi-channel operation",
    )
    parser.add_argument(
        "--state-path",
        default="state/slack-steward-access-points.json",
        help="path to persisted access-point state registry (default: state/slack-steward-access-points.json)",
    )
    parser.add_argument(
        "--sessions-root",
        default="",
        help="override Codex sessions root (default: ~/.codex/sessions)",
    )
    parser.add_argument("--request-timeout-sec", type=float, default=0.0)
    parser.add_argument("--rpc-timeout-sec", type=float, default=5.0)
    parser.add_argument("--rpc-retries", type=int, default=3)
    parser.add_argument("--steward-command", default="codex app-server")
    parser.add_argument("--agent-command", default="codex app-server")
    parser.add_argument(
        "--steward-prompt-file",
        default="prompts/steward-control-plane.md",
        help="path to steward startup prompt file (default: prompts/steward-control-plane.md)",
    )
    parser.add_argument("--approval-allow-commands", default="")
    parser.add_argument("--approval-deny-commands", default="")
    parser.add_argument("--approval-allow-regex-file", default="config/approval_allowlist.regex.txt")
    parser.add_argument(
        "--approval-default-decision",
        choices=("accept", "decline", "human"),
        default="human",
    )
    parser.add_argument("--thread-approval-policy", default="on-request")
    parser.add_argument("--thread-sandbox", default="workspace-write")
    parser.add_argument("--slack-app-token", default="")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    file_env = load_env_file(args.env_file)
    token = resolve_required_slack_setting(
        args.token,
        env_key="SLACK_BOT_TOKEN",
        cli_flag="--token",
        file_env=file_env,
        env_file_path=args.env_file,
    )
    steward_command = shlex.split(args.steward_command)
    if not steward_command:
        raise ValueError("--steward-command must not be empty")
    agent_command = shlex.split(args.agent_command)
    if not agent_command:
        raise ValueError("--agent-command must not be empty")
    prompt_file = Path(args.steward_prompt_file).expanduser()
    if not prompt_file.exists():
        raise ValueError(f"--steward-prompt-file does not exist: {prompt_file}")
    steward_prompt = prompt_file.read_text(encoding="utf-8")
    if not steward_prompt.strip():
        raise ValueError(f"--steward-prompt-file is empty: {prompt_file}")
    app_token = resolve_required_slack_setting(
        args.slack_app_token,
        env_key="SLACK_APP_TOKEN",
        cli_flag="--slack-app-token",
        file_env=file_env,
        env_file_path=args.env_file,
    )
    interactivity_source = SlackSocketModeSource(
        app_token=app_token,
        api_base_url=(args.slack_api_base_url.strip() or None),
        logger=LifecycleLogger(Path(args.log_path)),
    )
    return run_slack_steward(
        token=token,
        channel_id=args.channel_id.strip(),
        log_path=args.log_path,
        slack_api_base_url=(args.slack_api_base_url.strip() or None),
        steward_command=steward_command,
        agent_command=agent_command,
        request_timeout_sec=float(args.request_timeout_sec),
        rpc_timeout_sec=float(args.rpc_timeout_sec),
        rpc_retries=int(args.rpc_retries),
        approval_policy=ApprovalPolicy.from_csv(
            allow_csv=args.approval_allow_commands,
            deny_csv=args.approval_deny_commands,
            default_decision=args.approval_default_decision,
            allow_regex_file=args.approval_allow_regex_file,
        ),
        thread_approval_policy=args.thread_approval_policy,
        thread_sandbox=args.thread_sandbox,
        steward_prompt=steward_prompt,
        state_path=(args.state_path.strip() or None),
        sessions_root=(args.sessions_root.strip() or None),
        interactivity_source=interactivity_source,
    )


def _run_cli(argv: Sequence[str] | None = None) -> int:
    try:
        return main(argv)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(_run_cli())
