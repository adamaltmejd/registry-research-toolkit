---
name: chief-of-staff
description: >-
  Registry Research Toolkit chief-of-staff (Operator) routine: run worker sessions, merge
  their PRs and clean up after them. Use when a session is started as the COS.
---

# Chief of staff

The COS writes no code. It launches and briefs workers, verifies gates, merges, cleans up
and keeps GitHub issues current. Merging, CI and all runs (gate, flows, real-seed
builds) are its job. The maintainer always approves releases and what to work on.
Curation is the COS's call: raise tricky calls and broad curation design questions,
preferably after the fact for a second look rather than by stopping. Ask with the
question tool in the COS session; decide what an earlier ruling settles.

## Launch

One worker session per task, started from the COS session, each in its own git worktree
and in auto permission mode.

- Brief with a self-contained prompt; when it is long, write it to a path the worker can
  read and brief by that path.
- Opus for design-heavy or large work, Sonnet for docs and mechanical edits.
- Don't run two workers on the same contract surface or generated files (`curation.lock`,
  schemas).
- Never reopen a finished or archived worker: a reopened worker can't remove its
  worktree. To continue a branch, start a new worktree session that runs `git fetch`,
  `git reset --hard origin/<branch>` and pushes to `HEAD:<branch>`. The reset fails if
  the branch changes anything under `.claude/` (sandbox-protected: new skill links,
  settings, hooks); then drop the branch and redo the work from main.

Every brief says:
- Before `gate.sh all`, flows or a real-seed build (one heavy run at a time), report the
  request, end your turn and wait for the COS's message. Never poll, and never run it
  without the go.
- Open the PR, don't merge; put the PR number at the top of the report you give once the
  PR is open.
- Stop your own servers before you finish. Don't exit or remove the worktree until told.

## Monitor

- Follow each worker's messages in its session.
- Steer it with a follow-up message to that session. Point to a file for anything long.
- One background `gh pr checks <n> --watch --fail-fast` per PR once it opens.
- Never `pkill`/`killall`.

## Merge

GitHub enforces no checks, so confirm all four on the head you merge:

1. CI green.
2. Codex review:
   `codex exec -C <worktree> -s read-only 'Use $review-agent to review the changes on this branch against base branch origin/main'`.
   Run it bare: no `cd`, `timeout`, environment-variable prefix, `&&` chain or output
   redirect. The sandbox exclusion `codex *` (in `.claude/settings.json`) matches
   the start of the command text, and a redirect also breaks it; inside the sandbox Codex
   fails with "Operation not permitted".
   If Codex can't start, a fresh Opus agent, noted in the PR.
3. UI changes: a rendered `reg-webapp-design-reviewer` pass by a fresh agent.
4. The worker's worktree is clean: it confirms `git status --porcelain` is empty and
   `git rev-parse HEAD` equals the PR head. Auto-archive takes the worktree on merge,
   so read anything extra first: what is worth keeping goes to a new PR or to `.local/`
   in the main checkout; the rest is discarded.

Then `gh pr merge <n> --squash --match-head-commit <sha>`. Not while a release deploy
runs. After each merge, open PRs that touch the same files rebase (`gate.sh regen` where
relevant) and pass CI and review again on the new head.

The agent token has no `workflow` scope: the maintainer pushes commits that touch
`.github/workflows/` from their own terminal. Don't strip those edits.

## Clean up

After every merge, for your own workers only:

1. `gh pr view <n> --json state,headRefOid` shows `MERGED`.
2. The desktop app archives the session on merge; if it did not, archive it. In auto
   mode the first `archive_session` call can be refused because a hook adds a `session`
   label to the input after auto mode reviewed it: resend once with exactly the input the
   refusal quotes. Archiving detaches the worktree's HEAD and keeps the worktree in the
   app's pool for a later session (the app reuses or deletes it): a detached worktree
   under `.claude/worktrees/` is not a leftover. Workers never call ExitWorktree; it only
   removes worktrees a session made itself.
3. Archiving leaves the local branch (`claude/<name>-<suffix>`): delete it with
   `git branch -D <branch>` (the `error: could not lock config file .git/config` and
   `warning: update of config-file failed` lines are expected; the delete succeeded when
   git prints `Deleted branch <branch>`).
   Delete the remote branch if it is still there (GitHub normally deletes merged head
   branches).
4. A worktree still on its branch after archiving goes to the maintainer as one exact `!`
   command, e.g. `git worktree remove <path> && git branch -D <branch>`. Worktree folders
   are pooled and reused, so the folder name need not match the session or branch: take
   `<path>` from `git worktree list` (or the worker's `pwd`), not from the branch name.
   No loops over `.claude/worktrees/agent-*`. Add `--force` only when the extras were read
   and are disposable (e.g. an ignored `target/`).
5. Catch up the main checkout (`/Users/adam/code/registry-research-toolkit`): `git fetch
   origin main`, then `git diff --name-only HEAD origin/main -- .claude`. Empty →
   `git merge --ff-only origin/main` there. Non-empty → don't run it: the sandbox denies
   writes under `.claude/` (settings, skill links, hooks), so git stops halfway with HEAD
   old and a mixed working tree. Give the maintainer one exact `!` command instead:
   `git -C /Users/adam/code/registry-research-toolkit pull --ff-only`.
