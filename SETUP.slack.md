# orc Slack Setup Guide

This guide covers the practical setup for `slack-steward`.

## Bot Setup

1. Create the Slack app.
2. Enable `Socket Mode`.
3. Enable `Interactivity`.
4. Add the required bot scopes.
   - `chat:write`
   - `channels:history`
   - `groups:history`
   - `im:history`
5. Subscribe the bot to the required message events.
   - `message.channels`
   - `message.groups`
   - `message.im`
6. Install or reinstall the app to the workspace.

Notes:
- `commands` scope is not needed yet, because true Slack slash commands are not implemented in the runtime.
- `mpim:history` is not needed for the current public steward setup.
- You can either configure this manually in the Slack UI or import the provided app manifest from [slack.yaml](slack.yaml).

## Connecting the Bot

### Bot Token

- Slack App -> `OAuth & Permissions` -> `Bot User OAuth Token`
- Put it into `SLACK_BOT_TOKEN`.

Example format:

```dotenv
SLACK_BOT_TOKEN=xoxb-1234********
```

### App Token

- Slack App -> `Basic Information` -> `App-Level Tokens`
- Create an app-level token with `connections:write`
- Put the token into `SLACK_APP_TOKEN`

Example format:

```dotenv
SLACK_APP_TOKEN=xapp-1-A123********
```

### Connecting in a Channel

1. Invite the bot to the public or private channel.
2. The bot will only receive messages from channels where it is actually present.

### Connecting in a DM

1. Open a DM with the bot.
2. No extra channel invite is needed.

### Command Entry Note

- true Slack slash commands are not implemented in the runtime yet;
- send steward commands as ordinary messages;
- if Slack tries to treat `/...` as a slash command, prefix it with a leading space.

## Launching the Application

Show available flags:

```bash
python3 -m orchestrator.slack_steward_main --help
```

Shortest launch when `tokens.txt` is present in the current directory:

```bash
python3 -m orchestrator.slack_steward_main
```

Typical launch:

```bash
python3 -m orchestrator.slack_steward_main \
  --env-file tokens.slack.txt \
  --log-path logs/slack-steward.jsonl \
  --state-path state/slack-steward-access-points.json \
  --steward-command "codex app-server" \
  --agent-command "codex app-server"
```

An example env template is included in `tokens.template.txt`.

### Main Parameters

Shared:
- `--env-file`
  - path to env-style config file
  - default: `tokens.txt`
- `--log-path`
  - path to the runtime log file
  - default: `logs/slack-steward.jsonl`
- `--state-path`
  - path to the persistent bot state file
  - default: `state/slack-steward-access-points.json`
- `--sessions-root`
  - override the directory where Codex sessions are stored
  - default: `~/.codex/sessions`
- `--steward-command`
  - command used to launch the steward agent
  - default: `codex app-server`
- `--agent-command`
  - command used to launch worker agents
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

Slack-specific:
- `--token`
  - MANDATORY
  - Slack bot token
  - default: empty, so provide it via CLI, env, or `--env-file`
- `--slack-app-token`
  - MANDATORY
  - Slack app token for Socket Mode
  - default: empty, so provide it via CLI, env, or `--env-file`
- `--slack-api-base-url`
  - Slack Web API base URL override
  - default: use the standard Slack Web API
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
For a safer setup, keep it only in channels and DMs whose members you explicitly trust.

### The steward runtime itself:
- does not open public listening ports;
- does not require inbound webhooks;
- does not depend on external services other than Telegram or Slack for transport.

### Recommended setup:
- use a private DM whenever possible;
- invite the bot only into channels whose members you explicitly trust, preferably with you - the only member;
- avoid exposing the bot in broad public channels.
