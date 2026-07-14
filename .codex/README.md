# Codex project permissions

This trusted project carries its Codex defaults in `config.toml` and its persistent
command policy in `rules/development.rules`.

The policy is deliberately layered:

- `workspace-write` keeps filesystem mutations confined to the checkout and temporary
  directories.
- Network remains denied inside the default sandbox, so remote mutations still need an
  approval decision.
- Tests, linters, package scripts, local servers, and repository-owned Python or shell
  scripts remain with the auto-reviewer. They execute or accept paths from the
  checked-out head, so persistently allowing them would let an untrusted public PR
  escape the sandbox or read arbitrary host files.
- GitHub PR/issue operations, direct Git invocations, common command launchers, and
  process termination use broad prompt rules. Matching the Git executable itself also
  covers repository/config overrides placed before its subcommand; matching launchers
  broadly prevents `env git` and equivalent wrappers from hiding it.
- The configured `chrome-devtools` MCP server uses a pinned package and launches an
  isolated, headless browser with host-file navigation, usage statistics, and CrUX
  lookups disabled. Page listing/selection and passive inspection are auto-approved;
  event-generating interaction, new-page and scripted navigation, uploads, screenshots,
  snapshots, traces, script exports, and other path-capable tools retain approval.

Approval settings change execution confirmation; they do not grant task authority.
`AGENTS.md`, the user's request, and the invoked skill still decide whether an action
belongs in the task.

Codex reads project configuration and rules only for trusted projects and scans them at
startup. Restart Codex or start a new task after changing these files.

Validate a representative command with:

```sh
codex execpolicy check --pretty \
  --rules .codex/rules/development.rules \
  -- gh pr view 1140 --json headRefOid
```

Rules match argument prefixes, not shell text. Avoid persisting approvals for mutable
programs under `/tmp`, checked-out scripts, or compound shell loops. Keep reusable
automation tracked, but let the auto-reviewer evaluate its execution.
