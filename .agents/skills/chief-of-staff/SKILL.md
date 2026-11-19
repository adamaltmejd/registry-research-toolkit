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
   refusal quotes. Archiving detaches the worktree's HEAD and leaves the worktree under
   `.claude/worktrees/`; step 4 removes it. Workers never call ExitWorktree; it only
   removes worktrees a session made itself.
3. Archiving leaves the local branch (`claude/<name>-<suffix>`): once its tip equals the
   merged PR's `headRefOid`, delete it with `git branch -D <branch>` (the `error: could
   not lock config file .git/config` and `warning: update of config-file failed` lines are
   expected; the delete succeeded when git prints `Deleted branch <branch>`).
   Delete the remote branch if it is still there (GitHub normally deletes merged head
   branches).
4. A worktree of your own archived worker, whether detached or still on its branch, is a
   leftover. The sandbox makes `.git/worktrees` read-only for you, so the maintainer
   removes it: give one exact `!` command per batch, joined with `&&`, one part per
   worktree: `[ "$(git -C <path> rev-parse HEAD)" = <sha> ] && git worktree remove
   --force <path>` (a worktree still on its branch also gets `git branch -D <branch>`).
   `<sha>` is the HEAD you checked. Re-read `git worktree list` right before handing the
   command over; the HEAD test stops it removing a path the app has reused for a new
   session in the meantime. Check each worktree first. Its session is archived (idle is
   still live) and its HEAD is the merged PR's `headRefOid` or an ancestor of
   `origin/main` (never a branch with unmerged commits); the ancestor case allows
   removing the worktree only, and `git branch -D` still needs step 3's exact match with
   `headRefOid`. `git -C <path> status --porcelain --ignored --untracked-files=all` shows
   nothing worth keeping: an ignored `target/` or `node_modules/` is disposable; read
   anything else, and what is worth keeping goes to a new PR or to `.local/` in the main
   checkout. Worktree folders are pooled and reused, so the folder name need not match
   the session or branch: take `<path>` from `git worktree list` (or the worker's `pwd`),
   not from the branch name. No loops over `.claude/worktrees/agent-*`.
5. Catch up the main checkout (`/Users/adam/code/registry-research-toolkit`): `git fetch
   origin main`, then `git diff --name-only HEAD origin/main -- .claude`. Empty →
   `git merge --ff-only origin/main` there. Non-empty → don't run it: the sandbox denies
   writes under `.claude/` (settings, skill links, hooks), so git stops halfway with HEAD
   old and a mixed working tree. Give the maintainer one exact `!` command instead:
   `git -C /Users/adam/code/registry-research-toolkit pull --ff-only`.

## Housekeeping

Keeping the build environment clean is the COS's job, with standing authorization: no
question to the maintainer for regenerable output.

- When: at session start, after every merge, and when a worker reports a full disk. Check
  `df -h ~/.local` and the `code` volume; act below about 10 GB free.
- Shared cargo build cache, `~/.local/share/cargo/build/<xx>/<hash>/`: one directory per
  workspace path, so one per worktree, 5 to 11 GB each. Find the ones in use:
  `cargo metadata --format-version 1 --no-deps` reports `build_directory` for the
  directory it runs in; run it in the main checkout and every worktree in
  `git worktree list`. The cache is shared with every cargo project on the machine: a
  directory no checkout or worktree of this repository reports, and not written for a day,
  is stale. Delete a live worktree's directory only when its session is archived, or its
  worker is idle and has been told. Never delete one written in the last hour, reported or
  not (`find <dir> -type f -newermt <time>`). The sandbox hides other sessions' processes,
  so `ps` and `lsof` prove nothing.
- A merged PR's worktree: its `target/` is disposable. Check `git worktree list` for
  detached leftovers of archived workers and hand them over as in "Clean up" step 4.
- `$TMPDIR`: remove your own scratch, steward-repository clones and stray copies of
  holdings files left by Codex reviews, once the review they served is done.
- Not yours to delete: other tools' and projects' caches under `~/.local/cache` (e.g.
  `sccache`), `rustup`, the pinned inputs under the main checkout's `.local/`, and notes
  or evidence in `.local/` (archive, never delete). When these fill the disk, report them
  to the maintainer with sizes.
- Look before deleting: list the target with size and last write time first. Report every
  deletion with the space freed.
