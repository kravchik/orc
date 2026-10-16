# Orc Master

<p align="center">
  <img src="friendly-orc-warden-with-lantern.png" alt="friendly orc master" width="180">
</p>

Orc Master connects local coding-agent sessions to Telegram and Slack. It is a
chat control plane for individual sessions and, when addresses are assigned, a
router for multi-agent work.

## Chat Control Plane

Use the chat for the work itself, not only for alerts and status updates. You
can talk to an agent, follow progress, review approvals, upload files, inspect
session state, and control the binding without returning to the local terminal.

Two participants can answer in a connected chat:

- the **runtime agent** reads and edits the project, runs commands, and handles
  the task;
- **Steward** manages directories, sessions, providers, bindings, and other ORC
  control-plane operations.

Ordinary messages normally go to the runtime agent. If no agent is running,
they go to Steward. Use `/steward <message>` when you explicitly want Steward
while an agent is running.

When another message arrives during an active turn, Orc Master normally steers
it into that turn as additional guidance. If the agent is waiting for approval,
resolve or interrupt the approval first.

## Agent Networks

Assign addresses to agent sessions and they can pass tasks and results directly
to one another. A lead can coordinate workers through a long plan, an agent can
consult a specialist, or one session can ask another to review a plan or diff.
Orc Master does not impose a network layout: the addresses and the agents'
prompts define how the network works.

See [Setting Up An Agent Network](#setting-up-an-agent-network) for the routing
roles and a minimal lead/worker example.

## Agent Providers

Orc Master supports Codex, Claude, and OpenCode through named runtime profiles.
The default provider is Codex. Use `/providers` to see configured profiles and
whether their local commands and dependencies are available.

Provider features are not assumed to be identical. Commands such as
`/interrupt` and `/resume` report when the selected profile does not support an
operation.

See [Agent Provider Setup](SETUP.providers.md) for installation, profile
selection, and provider-qualified session IDs.

## Quick Start

1. Install and authenticate at least one supported agent provider:
   [Agent Provider Setup](SETUP.providers.md).
2. Set up and launch Orc Master for your chat transport:
   - [Telegram Setup](SETUP.telegram.md)
   - [Slack Setup](SETUP.slack.md)
3. Open a chat, channel, DM, or Telegram topic where the bot is present.
4. Ask Steward to start an agent in a project directory, for example:

   ```text
   Start a Codex session in /Users/me/work/my-project
   ```

5. Send coding tasks as ordinary messages.

If Orc Master itself is launched from the project directory, `/bind` is a
shortcut that starts the default Codex profile there. To choose another
provider or profile for a new session, ask Steward explicitly.

## Working With Sessions

### Continue An Existing Session

Use `/agents` to list resumable sessions in the directory connected to the
current chat. Use `/agents all` to search every known directory and provider
profile.

Then continue a session by name or provider-qualified session ID:

```text
/resume Backend refactor
/resume claude:session-id
/resume opencode/opencode-acp:session-id
```

`here` is the default search scope. Use
`/resume all <name|provider[/profile]:session-id>` when the current chat has no
project directory or the session belongs to another known directory.

### Stop, Start, Or Disconnect

- `/stop` stops the bound agent runtime but keeps the chat connected to the
  session.
- `/start` starts that bound session again.
- `/reset` disconnects the chat and clears its local ORC state.
- `/interrupt` stops only the current in-flight agent or Steward turn.

After Orc Master restarts, sessions that were running are restored on demand.
The first ordinary message, routed task, or delegated approval starts the
session and waits until it is ready. A session explicitly stopped with `/stop`
stays stopped.

Orc Master stays quiet after a restart instead of posting into every known
chat. `/help` explains the current chat state. `/inspect all` explicitly sends
a state report to every chat known to the same running Telegram or Slack bot.

## Sending Files And Images

Send Telegram photos/documents or Slack files to a connected chat. Orc Master
saves them under `.orc/uploads/` in the agent's project directory and replies
with a numbered list such as:

```text
1. image.png
2. requirements.md
```

Orc Master includes the same numbered file list with your next ordinary
message to the agent, so you can say "use file 1 as the reference and update
file 2." The list is consumed once. Bot commands and approval replies do not
consume it, and pending files survive an Orc Master restart.

Files are limited to 50 MiB each, with up to 100 pending files and 100 MiB per
pending batch. Filenames are sanitized, existing files are not overwritten,
and `/reset` clears pending references without deleting files already saved to
disk.

## Approvals

By default, approval requests are shown to the human in the same chat. You can
accept, decline, inspect details, or create an "always allow" rule when that
option is available.

Approval requests can also be handled by:

- regular-expression allow rules;
- delegation to another addressed agent;
- a default policy to ask the human, accept, or decline.

For a multi-agent setup, delegate unresolved approvals with:

```text
/approver assign my_approver
```

Several agents may share one approver; requests wait and are handled one at a
time. Self-approval and approval cycles are rejected. Use
`/approver assign human` to return future approvals to the chat user.

## Setting Up An Agent Network

Independent chats require no routing setup. Add addresses only when you want
agents to pass tasks to one another.

Orc Master supports two routing roles:

- **Shaman** has an address and can choose where to send each final answer: to
  the human or another agent.
- **Grunt** has an address but can respond only to whoever sent the request.

A minimal network looks like this:

```text
human <-> Shaman lead <-> Grunt worker
```

To create it:

1. Start one agent session in each chat.
2. Give the coordinator a prompt based on
   [`prompts/shaman-network-prompt.md`](prompts/shaman-network-prompt.md). Tell
   it that its address is `lead`, that `worker` is available, and how work
   should be divided.
3. In the coordinator chat, send `/shaman assign lead`.
4. In the worker chat, send `/grunt assign worker`.
5. Send tasks to `lead` as usual.

The Shaman uses `FROM`/`TO` headers in final answers to choose a destination.
The Grunt does not need to understand those headers. Routed messages are shown
in both involved chats, so you can follow the exchange and add guidance.

Addresses are lowercase, unique within one running Telegram or Slack bot, and
must match `[a-z][a-z0-9_-]{0,63}`. The address `human` is reserved. An idle
session can switch between Shaman and Grunt; assigning one role replaces the
other.

## Commands

| Command | What it does |
| --- | --- |
| `/help` | Show available commands and explain the current chat state. |
| `/bind` | Start the default Codex profile in Orc Master's current working directory. |
| `/providers` | Show configured provider profiles and local installation status. |
| `/agents [here\|all]` | List resumable sessions. `here` is the default. |
| `/resume [here\|all] <name\|provider[/profile]:session-id>` | Continue an existing session. `here` is the default. |
| `/status` | Show the chat connection, runtime state, and bound agent details. |
| `/inspect [all]` | Show API-backed session, turn, approval, work, and context information. |
| `/interrupt` | Interrupt the current agent or Steward turn. |
| `/stop` | Stop the bound runtime while keeping the chat connected. |
| `/start` | Start the existing bound session. |
| `/reset` | Disconnect the chat and clear its local ORC state. |
| `/approver [show\|assign <address\|human>]` | Show or change who handles approvals. |
| `/shaman [show\|assign <address>\|rename <address>\|remove]` | Show or change the Shaman routing identity. |
| `/grunt [show\|assign <address>\|rename <address>\|remove]` | Show or change the Grunt routing identity. |
| `/steward <message>` | Send one explicit message to Steward. |

In Slack these are ordinary text messages, not native Slack slash commands. If
Slack tries to interpret `/...` itself, send the message with a leading space.
See [Slack Setup](SETUP.slack.md).

## Security

Orc Master gives chat participants a path to local coding agents that can read
files, edit code, and run commands on your computer. Use private chats, DMs, or
tightly controlled private channels, and allow only people you trust.

Orc Master:

- does not open a public listening port;
- uses Telegram polling or Slack Socket Mode instead of inbound webhooks;
- keeps agent runtimes and project files on the machine where Orc Master runs;
- stores incoming files only inside the connected project directory;
- does not remove the need to review provider approvals and sandbox settings.

Telegram, Slack, and the selected model provider still carry the messages and
files sent through their services.
