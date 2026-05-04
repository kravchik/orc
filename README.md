# Orc Master

<p align="center">
  <img src="friendly-orc-warden-with-lantern.png" alt="friendly orc master" width="180">
</p>

Telegram/Slack interface for local Codex agent sessions.

I am using it instead of CLI even on desktop.

## What It Does
- Telegram/Slack interface for locally running Codex agents;
- almost the same functions as the CLI;
  - messaging to and from
  - thinking statuses
  - approval requests
  - regex-based auto-approval
- selecting directories, running/stopping agents;
- connects multiple channels and DMs, directories and sessions
- keeps channel-session mappings between restarts  

## Agents
The runtime uses two agent roles:
- master agent
  - starts and stops worker agents;
  - manages directory bindings and session context;
  - maps a specific directory to a specific channel;
- worker agent
  - has no predefined task prompt;
  - behaves like a regular Codex coding agent.

## Security And Network Model

The master runtime itself:
- does not open public listening ports;
- does not require inbound webhooks;
- does not depend on external services other than Telegram or Slack for transport.

Recommended setup:
- use private chats, private channels

## How To Use

### Start
1. Set up the bot and launch the runtime.
   - [Telegram setup](SETUP.telegram.md)
   - [Slack setup](SETUP.slack.md)
2. Send any message in the chat, channel, or DM where the bot is present.
3. In a free-form dialog, connect a directory to that chat, channel, or DM and start either a new or an existing Codex session for it.
4. Continue working with that session as usual.

## Commands

The same control-plane commands are available in Telegram and Slack:
- `/help`
  - show the built-in command help
- `/where`
  - show what this chat, channel, or DM is currently connected to
- `/status`
  - show the current bot state and current Codex session details
- `/stop`
  - stop the current Codex session while keeping the chat, channel, or DM connected
- `/start`
  - start the current Codex session again
- `/reset`
  - disconnect the current chat, channel, or DM and clear its local state

Slack note:
- Slack slash commands are not implemented in the runtime yet.
- Send the same commands as ordinary messages.
- If Slack tries to treat `/...` as a slash command, prefix it with a leading space.
- See `SETUP.slack.md`.
