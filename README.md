# Orc Master

<p align="center">
  <img src="friendly-orc-warden-with-lantern.png" alt="friendly orc master" width="180">
</p>

Telegram/Slack interface for local Codex agent sessions.

I am using it instead of CLI even on desktop.

## What It Does
- provides a Telegram/Slack interface for locally running Codex agents;
- sends messages, reasoning status updates, final replies, and approval requests;
- sends new messages directly into an active turn instead of waiting for it to finish, unless the turn is waiting for approval;
- supports interactive, regex-based, and delegated approvals;
- starts, stops, interrupts, and resumes new or existing Codex sessions;
- shows the current session, model, active turn, approval, and tracked work state;
- keeps chat-to-session mappings between restarts;
- optionally routes tasks and replies between sessions.

## Sessions And Routing

Each chat, channel, DM, or topic can be connected to one local Codex session. By default, sessions are independent and behave like regular Codex coding agents.

Routing is opt-in and enabled by assigning an address:
- Shaman mode uses explicit `FROM`/`TO` envelopes for incoming and outgoing routed messages. It is suitable for coordinators that know how the session network is organized.
- Grunt mode keeps the coding agent unaware of the routing protocol. Orc Master delivers a plain task and returns the final reply to whoever sent it.

Routed messages are mirrored in the relevant chats so a human can follow and join the conversation. Approval requests can independently stay with the human or be delegated to another addressed session.

## Security And Network Model

The Orc Master runtime itself:
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
- `/bind`
  - start and bind a new Codex session using the default launch settings
- `/help`
  - show the built-in command help
- `/status`
  - show what this chat, channel, DM, or topic is connected to, plus the current bot and Codex session state
- `/inspect`
  - show detailed session metadata, active turn, approval, and tracked work information
- `/interrupt`
  - interrupt the current in-flight turn
- `/stop`
  - stop the current Codex session while keeping the chat, channel, DM, or topic connected
- `/start`
  - start the current Codex session again
- `/reset`
  - disconnect the current chat, channel, DM, or topic and clear its local state
- `/shaman [show|assign <address>|rename <address>|remove]`
  - show or change the Shaman routing address
- `/grunt [show|assign <address>|rename <address>|remove]`
  - show or change the Grunt routing address
- `/approver [show|assign <address>|change <address>|clear]`
  - show or change which addressed session handles approvals for this agent
- `/steward <message>`
  - send one explicit message to Orc Master while a coding session is bound

Slack note:
- Slack slash commands are not implemented in the runtime yet.
- Send the same commands as ordinary messages.
- If Slack tries to treat `/...` as a slash command, prefix it with a leading space.
- See `SETUP.slack.md`.
