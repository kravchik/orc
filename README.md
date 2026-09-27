# Orc Master

<p align="center">
  <img src="friendly-orc-warden-with-lantern.png" alt="friendly orc master" width="180">
</p>

Orc Master lets you work with local Codex agents from Telegram or Slack.

You send a message in a chat. A Codex session running on your computer receives it, works in the connected project directory, and sends its status, approval requests, and final answer back to the same chat.

The basic mental model is simple:

```text
one chat, channel, DM, or topic <-> one local Codex session
```

You can use Orc Master as a remote terminal for one agent, keep several independent project chats, or optionally connect multiple agents into a small network.

## What You See In A Chat

There are two participants behind the bot:

- **your Codex agent** reads and edits the project, runs commands, and answers your task;
- **Keeper** manages the chat connection, starts or resumes Codex sessions, and handles control questions when no coding session is running.

Normally you talk directly to the Codex agent. If the agent is stopped, ordinary messages go to Keeper instead. Use `/steward <message>` when you explicitly want to ask Keeper something while an agent is running.

Orc Master forwards:

- reasoning and progress updates;
- final answers;
- command and file-change approval requests;
- session state, model, active turn, and context-window information.

If you send another message while the agent is still working, Orc Master adds it to the active turn as new guidance instead of waiting for the turn to finish. Approval prompts are the exception: resolve or interrupt the approval first.

## Quick Start

1. Set up and launch Orc Master for Telegram or Slack:
   - [Telegram setup](SETUP.telegram.md)
   - [Slack setup](SETUP.slack.md)
2. Open a chat, channel, DM, or Telegram topic where the bot is present.
3. Ask Keeper to start an agent in a project directory, for example:

   ```text
   Start a Codex session in /Users/me/work/my-project
   ```

4. Send coding tasks as ordinary messages.

If Orc Master itself is launched from the project directory, `/bind` is a shortcut that starts a new Codex session there.

## Working With Sessions

### Continue An Existing Session

Use `/agents` to list resumable Codex sessions in the directory connected to the current chat. Use `/agents all` to search every known directory.

Then continue a session by name or UUID:

```text
/resume Backend refactor
```

Use `/resume all <name|uuid>` when the current chat does not have a project directory yet.

### Stop, Start, Or Disconnect

- `/stop` stops the local Codex process but keeps the chat connected to the session.
- `/start` starts that connected session again.
- `/reset` disconnects the chat and clears its local bot state.
- `/interrupt` stops only the current in-flight turn.

After Orc Master restarts, sessions that were running are restored on demand. The first ordinary message, routed task, or delegated approval starts the session and waits until it is ready. A session explicitly stopped with `/stop` stays stopped.

Orc Master stays quiet after a restart instead of posting into every known chat. `/help` explains the current chat state. `/inspect all` explicitly sends a state report to every chat known to the same running Telegram or Slack bot.

## Sending Files And Images

Send Telegram photos/documents or Slack files to a connected chat. Orc Master saves them under `.orc/uploads/` in the agent's project directory and replies with a numbered list such as:

```text
1. image.png
2. requirements.md
```

Orc Master includes the same numbered file list with your next ordinary message to the agent, so you can say "use file 1 as the reference and update file 2." The list is consumed once. Bot commands and approval replies do not consume it, and pending files survive an Orc Master restart.

Files are limited to 50 MiB each, with up to 100 pending files and 100 MiB per pending batch. Filenames are sanitized, existing files are not overwritten, and `/reset` clears pending references without deleting files already saved to disk.

## Approvals

By default, approval requests are shown to the human in the same chat. You can accept, decline, inspect details, or create an "always allow" rule when that option is available.

Orc Master can also decide requests from local command rules:

- explicit allow/deny command lists;
- regular-expression allow rules;
- a default human, accept, or decline policy.

See the Telegram or Slack setup guide for the corresponding launch options.

For a multi-agent setup, unresolved approvals can be delegated to another addressed agent:

```text
/approver assign lead
```

Several agents may share one approver; their requests wait and are handled one at a time. Self-approval and approval cycles are rejected. Use `/approver assign human` to send future approvals back to the human.

## Agent Networks (Optional)

Independent chats require no routing setup. Add addresses only when you want agents to pass tasks to each other.

Orc Master supports two roles:

- **Shaman** is a coordinator that chooses whether to answer the human or send the result to another addressed agent.
- **Grunt** is a worker. It receives a normal task without network instructions and automatically returns its final answer to whoever sent the task.

A minimal network looks like this:

```text
human <-> Shaman lead <-> Grunt worker
```

To create it:

1. Start one Codex session in each chat.
2. Give the coordinator a prompt based on [`prompts/shaman-network-prompt.md`](prompts/shaman-network-prompt.md). Tell it that its address is `lead`, that `worker` is available, and how you want work divided.
3. In the coordinator chat, send:

   ```text
   /shaman assign lead
   ```

4. In the worker chat, send:

   ```text
   /grunt assign worker
   ```

5. Send tasks to `lead` as usual.

The Shaman uses `FROM`/`TO` headers in final answers to choose a destination. The Grunt never needs to understand those headers. Routed messages are shown in both involved chats, so you can follow the exchange and add guidance.

Addresses are lowercase, unique within one running Telegram or Slack bot, and must match `[a-z][a-z0-9_-]{0,63}`. The address `human` is reserved. An idle session can switch between Shaman and Grunt; assigning one role replaces the other.

## Commands

| Command | What it does |
| --- | --- |
| `/help` | Show available commands and explain the current chat state. |
| `/bind` | Start a new Codex session in Orc Master's current working directory. |
| `/agents [here\|all]` | List resumable Codex sessions. `here` is the default. |
| `/resume [here\|all] <name\|uuid>` | Continue an existing Codex session. `here` is the default. |
| `/status` | Show the chat connection and a compact session state. |
| `/inspect [all]` | Show detailed session, turn, approval, work, and context information. `all` reports to every chat known to this bot process. |
| `/interrupt` | Interrupt the current agent or Keeper turn. |
| `/stop` | Stop the Codex process while keeping the chat connected. |
| `/start` | Start the connected Codex session. |
| `/reset` | Disconnect the chat and clear its local bot state. |
| `/approver [show\|assign <address\|human>]` | Show or change who handles approvals for this agent. |
| `/shaman [show\|assign <address>\|rename <address>\|remove]` | Show or change the coordinator address. |
| `/grunt [show\|assign <address>\|rename <address>\|remove]` | Show or change the worker address. |
| `/steward <message>` | Send one explicit message to Keeper. |

In Slack these are ordinary text messages, not native Slack slash commands. If Slack tries to interpret `/...` itself, send the message with a leading space. See [Slack setup](SETUP.slack.md).

## Security

Orc Master gives chat participants a path to local Codex agents that can read files, edit code, and run commands on your computer. Use private chats, DMs, or tightly controlled private channels, and allow only people you trust.

Orc Master:

- does not open a public listening port;
- uses Telegram polling or Slack Socket Mode instead of inbound webhooks;
- keeps Codex and project files on the machine where Orc Master runs;
- stores incoming files only inside the connected project directory;
- does not remove the need to review agent approvals and sandbox settings.

Telegram and Slack still carry the messages and files you send through their services.
