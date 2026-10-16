# Agent Provider Setup

Orc Master runs agent sessions through a runtime-profile catalog. A provider is
an agent family such as Codex, Claude, or OpenCode. A profile selects the exact
integration used for that provider.

Provider setup is independent of the chat transport. Configure Telegram or
Slack separately after at least one provider is installed and authenticated.

## Built-In Profiles

| Provider/Profile | Default | Integration | Required local runtime |
| --- | --- | --- | --- |
| `codex/codex-native` | yes | native | `codex app-server` |
| `claude/claude-native` | yes | native | `claude-agent-sdk` and its Claude executable |
| `claude/claude-acp` | no | ACP v1 | `claude-agent-acp` |
| `opencode/opencode-native` | yes | native | `opencode serve` |
| `opencode/opencode-acp` | no | ACP v1 | `opencode acp` |

The default provider is `codex`. When a provider is specified without a
profile, Orc Master uses that provider's default profile.

## Install And Authenticate

Install each provider with its official tooling and authenticate it as the same
operating-system user that runs Orc Master.

### Codex

Install the Codex CLI, make `codex` available on `PATH`, and complete its normal
login flow. Orc Master launches `codex app-server` for `codex-native` sessions.

### Claude

For `claude-native`, install `claude-agent-sdk` into the same Python environment
that runs Orc Master. The package must expose its bundled Claude executable, or
`claude` must otherwise be available to that environment. Complete the normal
Claude Code authentication flow for the same operating-system user.

For the optional ACP profile, install `claude-agent-acp` and make that command
available on `PATH`.

### OpenCode

Install OpenCode, make `opencode` available on `PATH`, and configure its model
backend and credentials. Orc Master launches `opencode serve` for the native
profile and `opencode acp` for the optional ACP profile.

Orc Master does not own provider credentials. Existing provider configuration,
login state, model access, and backend selection remain provider-managed.

## Check Availability

After Orc Master starts, send:

```text
/providers
```

The response lists every configured `provider/profile`, its integration type,
whether it is the provider default, supported launch settings, and local
installation status.

The installation check is intentionally local and process-free. It confirms
the configured command and required local dependencies, but it does not prove
that credentials, the model backend, or the remote service are ready. The
first real session startup remains the authoritative check.

## Start A Session

If no provider is specified, Steward starts Codex. Ask Steward explicitly to
select another provider or a non-default profile:

```text
Start a Claude session in /Users/me/work/my-project
Start an OpenCode session with profile opencode-acp in /Users/me/work/my-project
```

`/bind` is intentionally a short default-provider command: it starts
`codex/codex-native` in Orc Master's current working directory. Provider and
profile selection for a new session is performed through Steward.

The Steward control-plane runtime is configured separately with
`--steward-command` and currently defaults to `codex app-server`. Selecting a
different runtime agent provider does not change the Steward runtime.

## List And Resume Sessions

Use `/agents` to query resumable sessions in the current chat directory, or
`/agents all` to query every known directory:

```text
/agents
/agents all
```

Orc Master returns provider-qualified IDs. A default profile is omitted from
the visible ID; a non-default profile is included:

```text
codex:session-id
claude:session-id
opencode/opencode-acp:session-id
```

Resume by a unique name or by the exact returned ID:

```text
/resume Backend refactor
/resume here claude:session-id
/resume all opencode/opencode-acp:session-id
```

Do not remove or rewrite the provider/profile prefix. Provider-native session
IDs can overlap, so the namespace is part of the identity.

## Feature Differences

Support is profile-specific. This applies to resume, interrupt, approvals,
attachments, and other provider operations. Orc Master reports `unsupported`
instead of silently emulating a missing provider capability.

## Additional ACP Profiles

The shipped ACP profiles are declared in
`config/agent_runtime_profiles.v2.json`. A trusted deployment can add another
strict ACP v1 profile with a separate JSON file and point Orc Master to it:

```bash
export ORC_RUNTIME_PROFILE_CONFIG=/absolute/path/to/local-runtime-profiles.v2.json
```

The local file extends the built-in catalog. Provider/profile IDs must be
unique, commands are executed locally, and malformed or conflicting
configuration fails closed during startup. Treat this file as trusted local
configuration rather than chat-controlled input.
