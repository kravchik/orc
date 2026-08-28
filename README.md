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

## Sessions And Networks

Each chat, channel, DM, or topic can be connected to one local Codex session. Sessions are independent by default and behave like regular coding agents. Assigning an address opts a session into an ORC network.

### Routing Modes

- **Shaman** is a network-aware coordinator. Every inbound message has a strict `FROM`/`TO` envelope, and every final response must address either `human` or one other session.
- **Grunt** is a network-unaware executor. It receives a plain task and returns its final response to the session or human that sent that task. A Grunt cannot choose another destination.

ORC routes final responses only. Reasoning, commentary, status updates, approvals, and tool events remain in the session where they originated. Routed messages are mirrored in the sender and recipient access points so a human can follow the exchange and intervene in either chat.

### Build A Network

A minimal coordinator/worker network looks like this:

```text
human <-> Shaman lead <-> Grunt worker
```

1. Bind one Codex session in each access point.
2. Before enabling routing, send the coordinator an adapted copy of [`prompts/shaman-network-prompt.md`](prompts/shaman-network-prompt.md). State that its address is `lead`, that `worker` is a known peer, and how work should be divided.
3. In the coordinator access point, assign the Shaman address:

   ```text
   /shaman assign lead
   ```

4. In the worker access point, assign the Grunt address:

   ```text
   /grunt assign worker
   ```

5. Send a task to the coordinator. ORC wraps the human message for `lead`; the Shaman can answer with `TO: worker`; the Grunt receives only the task body; and ORC returns its final response to `lead` with a canonical envelope.

ORC does not inject the network topology or role instructions into a Shaman. The explicit network prompt is what tells the coordinator which peers exist and how to use them. A Grunt needs no network prompt and never sees ORC routing metadata.

Addresses are global, case-normalized, and unique. They must match `[a-z][a-z0-9_-]{0,63}`; `human` is reserved. Assigning the other routing mode atomically replaces the current mode and address. Address changes are allowed only while the affected session is idle.

### Delegate Approvals

Approval routing is independent of message routing. An addressed or addressless session can delegate unresolved approvals to another addressed session:

```text
/approver assign lead
```

Local regexp auto-approval is evaluated first. If it does not decide the request, ORC sends a typed approval request to `lead`; the target receives the exact response contract automatically. A session cannot approve itself, and approval cycles are rejected. Use `/approver clear` to return approval decisions to the human.

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
