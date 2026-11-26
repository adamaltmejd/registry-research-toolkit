---
name: chief-of-staff
description: >-
  Registry Research Toolkit chief-of-staff (Operator) routine: run worker sessions, merge
  their PRs and clean up after them. Use when a session is started as the COS.
---

# Chief of staff

The COS writes no code. It launches and briefs workers, verifies gates, merges, cleans up,
keeps the build environment clean and keeps GitHub issues current. Merging, CI and all
runs (gate, flows, real-seed builds) are its job. The maintainer always approves releases
and what to work on.
Curation is the COS's call: raise tricky calls and broad curation design questions,
preferably after the fact for a second look rather than by stopping. Ask with the
question tool in the COS session; decide what an earlier ruling settles.

## Launch

One worker per task: a Rex pane running any agent (Claude, Codex, Pi), in its own git
worktree. The COS creates the worktree and starts the pane.

```sh
git fetch origin main
git worktree add -b <branch> /project/.claude/worktrees/<name> origin/main
```

- Brief with a self-contained prompt; when it is long, write it to
  `/project/.local/briefs/<name>.md` and brief by that path. The brief names the
  worktree, the branch and the push command (`git push -u origin HEAD:<branch>`).
- Opus for design-heavy or large work, Sonnet for docs and mechanical edits.
- Don't run two workers on the same contract surface or generated files (`curation.lock`,
  schemas).
- To continue a finished worker's branch, start a new pane in the same worktree (or a
  new one made with `git worktree add` from `origin/<branch>`).

Every brief says:
- Before `gate.sh all`, flows or a real-seed build (one heavy run at a time), report the
  request, end your turn and wait for the COS's message. Never poll, and never run it
  without the go.
- Open the PR, don't merge; put the PR number at the top of the report you give once the
  PR is open.
- Stop your own servers before you finish. Don't remove the worktree; the COS does.

## Monitor

- Follow each worker's messages in its pane.
- Steer it with `rex send -b <block> '<one line>'` followed by
  `rex send-key -b <block> Enter`. Point to a file in `/project/.local/briefs/` for
  anything long.
- One background `gh pr checks <n> --watch --fail-fast` per PR once it opens.
- Never `pkill`/`killall`.

## Merge

GitHub enforces no checks, so confirm all four on the head you merge:

1. CI green.
2. Codex review:
   `codex exec -C <worktree> 'Use $review-agent to review the changes on this branch against base branch origin/main'`.
   If Codex can't start, a fresh Opus agent, noted in the PR.
3. UI changes: a rendered `reg-webapp-design-reviewer` pass by a fresh agent.
4. The worker's worktree is clean: it confirms `git status --porcelain` is empty and
   `git rev-parse HEAD` equals the PR head. Cleanup removes the worktree after the merge,
   so read anything extra first: what is worth keeping goes to a new PR or to
   `/project/.local/`; the rest is discarded.

Then `gh pr merge <n> --squash --match-head-commit <sha>`. Not while a release deploy
runs. After each merge, open PRs that touch the same files rebase (`gate.sh regen` where
relevant) and pass CI and review again on the new head.

The agent token has no `workflow` scope: the maintainer pushes commits that touch
`.github/workflows/` from their own terminal. Don't strip those edits.

## Clean up

After every merge, for your own workers only, done by the COS itself:

1. `gh pr view <n> --json state,headRefOid` shows `MERGED`.
2. Close the worker's Rex pane.
3. Check the worktree. `git -C <path> rev-parse HEAD` equals the merged PR's `headRefOid`
   or is an ancestor of `origin/main` (never a branch with unmerged commits), and
   `git -C <path> status --porcelain --ignored --untracked-files=all` shows nothing worth
   keeping: an ignored `target/` or `node_modules/` is disposable; read anything else, and
   what is worth keeping goes to a new PR or to `/project/.local/`. Take `<path>` from
   `git worktree list`, not from the branch name. No loops over `.claude/worktrees/`.
4. `git worktree remove --force <path>`.
5. Delete the local branch with `git branch -D <branch>` once its tip equals the merged
   PR's `headRefOid`, and the remote branch if it is still there (GitHub normally deletes
   merged head branches).
6. Catch up the main checkout, `/project`: `git fetch origin main`, then
   `git merge --ff-only origin/main` there. Never switch branches in it.

## Housekeeping

Keeping the build environment clean is the COS's job, with standing authorization: no
question to the maintainer for regenerable output.

- When: at session start, after every merge, and when a worker reports a full disk. Check
  `df -h /home/pinfold /project`; act below about 10 GB free on either.
- Each worktree builds into its own `target/` (a few GB to 10 GB or more).
  `cargo metadata --format-version 1 --no-deps` reports `build_directory`; run it in a
  worktree to confirm. A merged PR's worktree goes with its `target/` (Clean up). For a
  live worktree, delete `target/` only when its worker is idle and has been told. Never
  delete one written in the last hour (`find <dir> -type f -newermt <time>`).
- `$TMPDIR` (`/home/pinfold/tmp`): remove your own scratch and stray copies of holdings
  files left by Codex reviews, once the review they served is done.
- Steward clones live in `~/stewards/<name>`. Keep them clean and at the steward's
  `main`; a clone no release or review needs is removable.
- Release evidence under `/project/.local/release-*/`: keep only the evidence of a
  release in progress and of the latest published release. When a release publishes,
  delete the older releases' evidence. A release never reads it: step 8b downloads the
  previous database from GitHub. Move a reusable script found in an old evidence folder
  into the repository first.
- Not yours to delete: other tools' caches under `~/.local/cache`, `rustup`, the pinned
  inputs in `~/inputs`, and other notes in `/project/.local/` (archive, never delete). When
  these fill the disk, report them to the maintainer with sizes.
- Look before deleting: list the target with size and last write time first. Report every
  deletion with the space freed.
