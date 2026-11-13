---
name: chief-of-staff
description: >-
  Registry Research Toolkit chief-of-staff (Operator) routine: run worker sessions, merge
  their PRs and clean up after them. Use when a session is started as the COS.
---

# Chief of staff

The COS writes no code. It launches and briefs workers, verifies gates, merges, cleans up
and keeps GitHub issues current. Merging and CI-watching are its job, never the
maintainer's. Escalate only real rulings (design, curation content, releases, deploys),
with the question tool in the COS session; decide what an earlier ruling settles.

## Launch

One Rex tab per worker, in the COS's own Rex session:

```bash
rex window new -s "$REX_SESSION" <name> --focus=false -c <repo> \
  --shell /opt/homebrew/bin/bash -- \
  claude -w <name> -n <name> --model <model> --permission-mode auto \
  "Read and follow <brief path>"   # bash: zsh tabs lack cargo
```

- Brief by file path; long prompt and `rex send` text is truncated.
- Opus for design-heavy or large work, Sonnet for docs and mechanical edits.
- Don't run two workers on the same contract surface or generated files (`curation.lock`,
  schemas).
- Never resume a worker with `claude -r`: it can't remove its worktree. To continue a
  branch, start a new `-w` worker that runs `git fetch`, `git reset --hard origin/<branch>`
  and pushes to `HEAD:<branch>`. The reset fails if the branch changes anything under
  `.claude/` (sandbox-protected); then ask the maintainer.

Every brief says:
- You are a tab in a shared Rex session: never `rex session rename`.
- Wait for the COS's go before `gate.sh all`, flows or a real-seed build (one heavy run
  at a time).
- Open the PR, don't merge; report the PR number in your tab.
- Stop your own servers before you finish. Don't exit or remove the worktree until told.

## Monitor

- Read: `rex capture -s "$REX_SESSION" -w <tab> --trim`.
- Send: `rex send -s "$REX_SESSION" -w <tab> '<short text>'`, then
  `rex send-key -s "$REX_SESSION" -w <tab> enter`. Point to a file for anything long.
- One background `gh pr checks <n> --watch --fail-fast` per PR once it opens.
- Never `pkill`/`killall`.

## Merge

GitHub enforces no checks, so confirm all three on the head you merge:

1. CI green.
2. Codex review:
   `codex exec -C <worktree> -s read-only 'Use $review-agent to review the changes on this branch against base branch origin/main'`.
   If Codex can't start, a fresh Opus agent, noted in the PR.
3. UI changes: a rendered `reg-webapp-design-reviewer` pass by a fresh agent.

Then `gh pr merge <n> --squash --match-head-commit <sha>`. Not while a release deploy
runs. After each merge, open PRs that touch the same files rebase (`gate.sh regen` where
relevant) and pass CI and review again on the new head.

The agent token has no `workflow` scope: the maintainer pushes commits that touch
`.github/workflows/` from their own terminal. Don't strip those edits.

## Clean up

After every merge, for your own workers only:

1. `gh pr view <n> --json state,headRefOid` shows `MERGED`.
2. Have the worker confirm `git status --porcelain` is empty and `git rev-parse HEAD`
   equals that `headRefOid`. If either fails, stop and ask the maintainer.
3. Tell the worker to call ExitWorktree with action `remove` and `discard_changes: true`
   (after a squash merge it refuses without it). This removes the worktree and its local
   branch.
4. Check `git worktree list` and `git branch --list '*<name>*'`.
5. `git push origin --delete <branch>`, then `rex window close -s "$REX_SESSION" <tab>`.
6. Anything left over goes to the maintainer as one exact `!` command, e.g.
   `git worktree remove .claude/worktrees/<name> && git branch -D <branch>`. No `--force`,
   no loops over `.claude/worktrees/agent-*`.
