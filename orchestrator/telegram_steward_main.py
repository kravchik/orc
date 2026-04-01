"""Standalone Telegram entrypoint for Access Point stub-Steward flow (step 18.4)."""

from __future__ import annotations

import argparse
import sys
import shlex
from pathlib import Path
from typing import Sequence

from orchestrator.approval import ApprovalPolicy
from orchestrator.env_utils import load_env_file, resolve_env
from orchestrator.telegram_steward import run_telegram_steward


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="telegram-steward")
    parser.add_argument("--env-file", default="tokens.txt", help="local KEY=VALUE file (default: tokens.txt)")
    parser.add_argument("--log-path", default="logs/telegram-steward.jsonl")
    parser.add_argument("--telegram-token", "--token", dest="telegram_token", default="")
    parser.add_argument("--telegram-api-base-url", default="")
    parser.add_argument(
        "--state-path",
        default="state/telegram-steward-access-points.json",
        help="path to persisted access-point state registry (default: state/telegram-steward-access-points.json)",
    )
    parser.add_argument(
        "--sessions-root",
        default="",
        help="override Codex sessions root (default: ~/.codex/sessions)",
    )
    parser.add_argument(
        "--steward-prompt-file",
        default="prompts/steward-control-plane.md",
        help="path to steward startup prompt file (default: prompts/steward-control-plane.md)",
    )
    parser.add_argument(
        "--telegram-allowed-chat-ids",
        "--allowed-chat-ids",
        dest="telegram_allowed_chat_ids",
        default="",
        help="required comma-separated chat ids",
    )
    parser.add_argument("--telegram-poll-timeout-sec", "--poll-timeout-sec", dest="telegram_poll_timeout_sec", type=int, default=0)
    parser.add_argument("--request-timeout-sec", type=float, default=0.0)
    parser.add_argument("--rpc-timeout-sec", type=float, default=5.0)
    parser.add_argument("--rpc-retries", type=int, default=3)
    parser.add_argument("--steward-command", default="codex app-server")
    parser.add_argument("--approval-allow-commands", default="")
    parser.add_argument("--approval-allow-regex-file", default="config/approval_allowlist.regex.txt")
    parser.add_argument("--approval-deny-commands", default="")
    parser.add_argument(
        "--approval-default-decision",
        choices=("accept", "decline", "human"),
        default="human",
    )
    parser.add_argument("--thread-approval-policy", default="on-request")
    parser.add_argument("--thread-sandbox", default="workspace-write")
    parser.add_argument(
        "--telegram-insecure-skip-verify",
        "--insecure-skip-verify",
        dest="telegram_insecure_skip_verify",
        action="store_true",
        default=False,
    )
    return parser.parse_args(argv)


def _parse_allowed_chat_ids(raw: str) -> set[int]:
    value = raw.strip()
    if not value:
        raise ValueError("--telegram-allowed-chat-ids is required and must be non-empty")
    try:
        parsed = {int(part.strip()) for part in value.split(",") if part.strip()}
    except ValueError as exc:
        raise ValueError("--telegram-allowed-chat-ids must be comma-separated integers") from exc
    if not parsed:
        raise ValueError("--telegram-allowed-chat-ids is required and must be non-empty")
    return parsed


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    file_env = load_env_file(args.env_file)
    token = resolve_env(args.telegram_token, "TELEGRAM_BOT_TOKEN", file_env)
    if not token:
        raise ValueError("--telegram-token is required (CLI, env, or --env-file)")
    allowed_raw = resolve_env(args.telegram_allowed_chat_ids, "TELEGRAM_ALLOWED_CHAT_IDS", file_env)
    allowed_chat_ids = _parse_allowed_chat_ids(allowed_raw)
    poll_timeout_str = resolve_env(
        str(args.telegram_poll_timeout_sec) if args.telegram_poll_timeout_sec > 0 else "",
        "TELEGRAM_POLL_TIMEOUT_SEC",
        file_env,
        default="30",
    )
    try:
        poll_timeout_sec = int(poll_timeout_str)
    except ValueError as exc:
        raise ValueError("--telegram-poll-timeout-sec must be integer") from exc
    insecure_from_env = resolve_env("", "TELEGRAM_INSECURE_SKIP_VERIFY", file_env, default="0")
    insecure_skip_verify = bool(args.telegram_insecure_skip_verify) or insecure_from_env == "1"
    api_base_url = resolve_env(args.telegram_api_base_url, "TELEGRAM_API_BASE_URL", file_env)
    command = shlex.split(args.steward_command)
    if not command:
        raise ValueError("--steward-command must not be empty")
    prompt_file = Path(args.steward_prompt_file).expanduser()
    if not prompt_file.exists():
        raise ValueError(f"--steward-prompt-file does not exist: {prompt_file}")
    steward_prompt = prompt_file.read_text(encoding="utf-8")
    if not steward_prompt.strip():
        raise ValueError(f"--steward-prompt-file is empty: {prompt_file}")
    return run_telegram_steward(
        token=token,
        log_path=args.log_path,
        poll_timeout_sec=poll_timeout_sec,
        allowed_chat_ids=allowed_chat_ids,
        insecure_skip_verify=insecure_skip_verify,
        telegram_api_base_url=(api_base_url.strip() or None),
        agent_command=command,
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
        sessions_root=(args.sessions_root.strip() or None),
        steward_prompt=steward_prompt,
        state_path=(args.state_path.strip() or None),
    )


def _run_cli(argv: Sequence[str] | None = None) -> int:
    try:
        return main(argv)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(_run_cli())
