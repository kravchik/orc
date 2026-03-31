# orc Telegram Setup Guide

This guide covers the practical setup for `telegram-steward`.

## Bot Setup

1. Create or manage the bot in Telegram `@BotFather`.
2. Disable bot privacy mode in `@BotFather` with `/setprivacy`.
   - If privacy mode stays enabled, the bot will mostly see only commands, mentions, and replies in groups.

## Connecting the Bot

### Bot Token

- Copy the token from `@BotFather` into `TELEGRAM_BOT_TOKEN`.

Example format:

```dotenv
TELEGRAM_BOT_TOKEN=1234567890:ABCDEabcdeABCDEabcdeABCDEabcdeABCDEabcde
```

### Allowed Chat IDs

Set `TELEGRAM_ALLOWED_CHAT_IDS` to a non-empty comma-separated list of numeric chat ids.

Example:

```dotenv
TELEGRAM_ALLOWED_CHAT_IDS=123456789,-123456789,-1001234567890
```

Notes:
- you can read the numeric id of a channel/group from the Web Telegram URL.
- private chat ids are usually positive;
- group ids are negative;
- supergroups and channels ids usually start with `-100`;
- topic ids cannot be allowlisted separately;
- allowing a supergroup allows all topics in that supergroup.

### Connecting in a Private Chat

1. Open a private chat with the bot.
2. Send `/start` once so the bot can talk to you.
3. Add that private `chat_id` to `TELEGRAM_ALLOWED_CHAT_IDS`.

### Connecting in a Group or Supergroup

1. Add the bot to the group.
2. Add the group or supergroup `chat_id` to `TELEGRAM_ALLOWED_CHAT_IDS`.
3. Admin rights are not required for normal chat usage, but the bot must still be allowed to send messages in that chat.

## Launching the Application

Show available flags:

```bash
python3 -m orchestrator.telegram_steward_main --help
```

Shortest launch when `tokens.txt` is present in the current directory:

```bash
python3 -m orchestrator.telegram_steward_main
```

Typical launch:

```bash
python3 -m orchestrator.telegram_steward_main \
  --env-file tokens.telegram.txt \
  --log-path logs/telegram-steward.jsonl \
  --state-path state/telegram-steward-access-points.json \
  --steward-command "codex app-server"
```

An example env template is included in `tokens.template.txt`.

### Main Parameters

Shared:
- `--env-file`
  - path to env-style config file
  - default: `tokens.txt`
- `--log-path`
  - path to the runtime log file
  - default: `logs/telegram-steward.jsonl`
- `--state-path`
  - path to the persistent bot state file
  - default: `state/telegram-steward-access-points.json`
- `--sessions-root`
  - override the directory where Codex sessions are stored
  - default: `~/.codex/sessions`
- `--steward-command`
  - command used to launch the steward agent
  - default: `codex app-server`
- `--steward-prompt-file`
  - startup prompt file
  - default: `prompts/steward-control-plane.md`
- `--approval-allow-regex-file`
  - path to regex allowlist file
  - default: `config/approval_allowlist.regex.txt`
- `--approval-allow-commands`
  - comma-separated auto-allow command list
  - default: empty
- `--approval-deny-commands`
  - comma-separated auto-deny command list
  - default: empty
- `--approval-default-decision`
  - one of `accept`, `decline`, `human`
  - default: `human`
- `--thread-approval-policy`
  - approval policy used for Codex worker threads
  - default: `on-request`
- `--thread-sandbox`
  - sandbox mode used for Codex worker threads
  - default: `workspace-write`

Telegram-specific:
- `--telegram-token`
  - MANDATORY
  - Telegram bot token
  - default: empty, so provide it via CLI, env, or `--env-file`
- `--telegram-allowed-chat-ids`
  - MANDATORY
  - required non-empty comma-separated Telegram chat ids
  - default: empty, so provide them via CLI, env, or `--env-file`
- `--telegram-api-base-url`
  - Telegram Bot API base URL override
  - default: use the standard Telegram Bot API
- `--telegram-poll-timeout-sec`
  - long-poll timeout
  - default: `30`
- `--telegram-insecure-skip-verify`
  - disable TLS verification for the Telegram API client
  - default: off
- `--request-timeout-sec`
  - steward request timeout
  - default: `0`
- `--rpc-timeout-sec`
  - timeout for requests to the local Codex process
  - default: `5`
- `--rpc-retries`
  - retry count for requests to the local Codex process
  - default: `3`

## Security

This bot can trigger local agent work on the machine where it runs.
For a safer setup, keep it only in chats whose members you explicitly trust.

### The steward runtime itself:
- does not open public listening ports;
- does not require inbound webhooks;
- does not depend on external services other than Telegram or Slack for transport.

### Recommended setup:
- use a private 1:1 chat whenever possible;
- if you use a group or supergroup, keep it private and tightly controlled, preferably with you - the only member;
- only allow chat ids for chats whose members you explicitly trust, preferably with you - the only member;
