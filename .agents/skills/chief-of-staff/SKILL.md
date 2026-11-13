---
name: chief-of-staff
description: >-
  Registry Research Toolkit chief-of-staff (COS, also called Operator) routine. Use when
  the maintainer starts a session as the COS ("/chief-of-staff", "you are the Operator"):
  launch and brief worker threads, monitor them, verify gates, merge PRs, clean up
  worktrees and branches, keep GitHub issues current, and escalate only real rulings.
---

# Chief of staff

The COS drives all worker threads and the GitHub issue list. Workers write code; the COS
launches, briefs, monitors, reviews, merges and cleans up. Merging and CI-watching are the
COS's job, never handed back to the maintainer.

## Role

- Tell waiting workers when to go. Verify gates yourself.
- Keep issues current: file, comment and close as work lands. Search open and closed
  issues first (see the issue tracker in `AGENTS.md`).
- Escalate only real maintainer rulings: design, curation content, releases and deploys.
  Ask in the COS session with the question tool, never inside a worker thread. A ruling
  an earlier ruling settles is decided, not asked.

## Launch a worker

One Rex tab per worker in your current Rex session, never a new Rex session:

```bash
rex window new -s "$REX_SESSION" <name> --focus=false -c <repo> \
  --shell /opt/homebrew/bin/bash -- \
  claude -w <name> -n <name> --model <model> --permission-mode auto \
  "Read and follow <brief path>"
```

- Bash, not zsh: zsh tabs lack cargo.
- `claude -w` makes the worktree. Never `git worktree add`: the sandbox allows commits
  only in tool-made worktrees.
- Write the brief to a file and pass its path. Long prompt or `rex send` text is
  truncated.
- Every brief says: this is a tab in the shared session, so no `rex session rename`
  (`rex window rename` for its own tab is fine); no local branch rename, name the remote
  at push (`git push -u origin HEAD:<name>`); open the PR but do not merge; report the PR
  number and CI state in its tab.
- Models: Opus for design-heavy or large work and first reviews of catalog logic. Sonnet
  for docs, mechanical edits and re-reviews. Haiku for fully specified chores.
- Several workers building at once need a per-worktree `CARGO_TARGET_DIR`.
- Never resume a worker with `claude -r`: it exits without the remove option and leaves
  its worktree behind. To continue work, start a fresh worker on the branch.

## Monitor

- Read: `rex capture -s "$REX_SESSION" -w <tab> --trim`. List tabs:
  `rex window list -s "$REX_SESSION"`.
- Send short text with `rex send -s "$REX_SESSION" -w <tab> '<text>'`, then submit with
  `rex send-key -s "$REX_SESSION" -w <tab> enter` (key names are lowercase). Point to a
  file for anything long.
- Wait with an until-loop on a capture condition, not chained sleeps.
- Never `pkill` or `killall`. Stop only PIDs you started.

## Review and merge

A PR merges when all of these hold:

1. **Green CI.** Arm one background `gh pr checks <n> --watch --fail-fast` per PR as soon
   as it opens. Report ready or red at once.
2. **Independent review** by an agent that did not write the change. Use Codex:
   `codex exec -C <worktree> -s read-only 'Use $review-agent to review the changes on this branch against base branch origin/main'`.
   If Codex cannot start in the sandbox, use a fresh Opus agent and say so in the PR.
3. **UI changes** also get a rendered `reg-webapp-design-reviewer` pass by a fresh agent
   at four widths.

Merge it yourself, pinned to the reviewed commit:
`gh pr merge <n> --squash --match-head-commit <sha>`. Hold merges while a release deploy
runs (see the [release](../release/SKILL.md) skill).

The agent token has no `workflow` scope. The maintainer pushes workflow-file commits from
their own terminal. Do not strip workflow edits to get around it.

## Clean up after merge

Every time, for the worker's own worktree only:

1. Tell the worker to call its ExitWorktree tool with action `remove` (add
   `discard_changes: true` once its commits are merged). Under the sandbox this removes
   the worktree and the local branch.
2. From the main checkout, verify: `git worktree list` and `git branch --list '*<name>*'`.
3. Delete the remote branch: `git push origin --delete <branch>`.
4. Close the tab: `rex window close -s "$REX_SESSION" <tab>`. Stop any server the worker
   left, by its PIDs.
5. If something still cannot be removed (a resumed worker's, a plain-git worktree), give
   the maintainer one exact command to run with `!`, for example
   `git worktree remove .claude/worktrees/<name> && git branch -D <branch>`. No
   workarounds: no `--force`, no excluded commands, no edits under `.git/worktrees`.
6. Never loop-remove `.claude/worktrees/agent-*`. Remove only named worktrees of your own
   finished workers.

## Sandbox limits

Others are in `AGENTS.md`.

- `.git/config` and `.git/worktrees` are read-only.
- Toolchain installs and updates: ask the maintainer to run them with `!`.
- `bash scripts/gate.sh all` and `dev.sh` run only as bare commands.
- Processes the session did not start are hidden: an empty `lsof` proves nothing.
