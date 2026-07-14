# Codex project permissions

This trusted project carries its Codex defaults in `config.toml` and its persistent
command policy in `rules/development.rules`.

The policy is deliberately layered:

- `workspace-write` keeps filesystem mutations confined to the checkout and temporary
  directories.
- Network remains denied inside the default sandbox, so remote mutations still need an
  approval decision. Exact rules allow routine GitHub reads, package tools, localhost
  servers, and browser tests to escape without repeated confirmation.
- The auto-reviewer handles exceptional sandbox requests that do not match a rule.
- Exact allow rules cover pinned verification commands, read-only GitHub operations,
  repository-owned coordination scripts, draft/ready PR lifecycle steps, and staging.
- Commits remain auto-reviewed because a prefix rule cannot prove that a hook-bypass
  flag was not appended later in the command.
- Destructive Git operations, force pushes, merges, worktree removal, and process
  termination retain an explicit prompt. Hook bypasses are forbidden by repository
  policy.
- The configured `chrome-devtools` MCP server uses a pinned package and launches an
  isolated, headless browser with usage statistics and CrUX lookups disabled. Its tool
  calls do not prompt individually.

Rules remove an execution confirmation; they do not grant task authority. `AGENTS.md`,
the user's request, and the invoked skill still decide whether an action belongs in the
task. In particular, a one-tick workflow does not become an ongoing watcher merely
because `cos_watch.py` is executable without a sandbox prompt.

Codex reads project configuration and rules only for trusted projects and scans them at
startup. Restart Codex or start a new task after changing these files.

Validate a representative command with:

```sh
codex execpolicy check --pretty \
  --rules .codex/rules/development.rules \
  -- gh pr view 1140 --json headRefOid
```

Rules match argument prefixes, not shell text. Avoid persisting approvals for mutable
programs under `/tmp` or compound shell loops; move reusable automation into a tracked
repository command first.
