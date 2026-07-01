---
name: chief-of-staff
description: "Run one registry chief-of-staff tick: invoke /issue-pulse, keep the reg_webapp dev preview and default reg_meta DB install current, inspect live issue and PR claim state, automatically maintain issue metadata/priorities, squash-merge PRs with current-head pr-pipeline handoff evidence, send unblock follow-ups to stalled /pr-pipeline sessions, report merged user-facing features with preview links, run /release minor when a merge creates a required build/release boundary, and recommend the next safe /pr-pipeline lanes. Usage: /loop 30m /chief-of-staff"
---

# chief-of-staff — one coordination tick

The chief of staff is the repo's coordination agent: it keeps issue metadata and lane
priorities current, understands active `/pr-pipeline` claims, prevents conflicting work,
merges PRs with a current-head pipeline handoff, and recommends the next work to launch
in separate worktrees. It also keeps a canonical-main `reg_webapp` dev preview available
so freshly merged user-visible changes can be inspected immediately.

This skill is designed for scheduled use. For recurring use, prefer one heartbeat
attached to a single existing chief-of-staff thread. Run one tick to completion, then
stop; `/loop` or another external automation owns the cadence. Do not schedule detached
cron/workspace jobs unless the user explicitly accepts multiple independent coordinator
contexts.

## Scheduling

- Use one active heartbeat pointed at one existing chief-of-staff thread. The goal is
  one continuing coordinator context, not a set of detached jobs.
- Do not create detached cron/workspace jobs by default; they can run as independent
  chiefs of staff and duplicate merge/recommendation decisions.
- After creating or updating the automation, verify the persisted `kind`, `status`,
  cadence, and `target_thread_id` before calling it active. If it remains cron-style,
  paused, or missing the target thread, report that exact state and stop.
- Keep the scheduled prompt minimal, e.g. `Run exactly one chief-of-staff tick`, so this
  skill remains the source of truth.
- If a heartbeat fires while the prior tick in that thread is still running, skip the
  new tick and return `DONT_NOTIFY` with reason `previous tick still running`; do not
  overlap issue maintenance, merge inspection, or recommendations.

For high-frequency cadences such as every 15 minutes, prefer a deterministic preflight
outside the agent instead of waking this skill every time. The built-in heartbeat has no
shell precondition hook, so it always spends a model turn. Use:

```sh
uv run --no-project python scripts/cos_preflight.py
```

The preflight exits `0` when idle, `10` when a real tick should run, and `2` on setup or
tool errors. It records its snapshot in `.git/cos-preflight-state.json` by default and
wakes only when repo/GitHub state changes enough to justify a COS tick: lane drift,
issue-projection movement, `origin/main` movement, or relevant issue-closing PR /
merge-gate state changes. On Codex surfaces, the tested scheduler wrapper is
`scripts/cos_scheduler_tick.sh --thread <codex-chief-of-staff-thread-id>`, which resumes
that Codex thread with `codex exec`. The Codex-only foreground heartbeat is
`scripts/cos_scheduler_heartbeat.sh <codex-chief-of-staff-thread-id>`; it defaults to a
15-minute interval and runs until Ctrl-C. Do not use either Codex wrapper to resume a
Claude `/loop` thread; pair the preflight with the scheduler/resume mechanism exposed by
the Claude surface instead.

## Startup Gate

Run only from `/Users/adam/Code/registry-research-toolkit`, the canonical main checkout.
Do not run from a git worktree.

Before `/issue-pulse`, issue edits, PR inspection, merge, or lane recommendation:

1. Verify the repo top-level is exactly `/Users/adam/Code/registry-research-toolkit`
   with `git rev-parse --show-toplevel`, the current branch is `main` with
   `git branch --show-current`, and `.git` is a directory with `test -d .git`. A linked
   worktree has a `.git` file, so it must fail this gate. If any check fails, stop and
   tell the user to fix the checkout before relaunching.
2. Run `git pull --ff-only` as the first sync action.
3. Re-verify the checkout is the canonical main checkout on `main`, then run
   `git status --short`. If the repo is dirty, stop and tell the user to clean or commit
   the main checkout before relaunching.

Implementation work belongs in separate worktrees via `/pr-pipeline`. From the main
checkout, this skill may coordinate issues, inspect PRs, merge ready PRs, and recommend
new work, but it must not edit project code as part of the work itself.

## Tick

1. If this is a heartbeat invocation and the previous tick in this thread is still
   running, skip this tick with `DONT_NOTIFY`; do not overlap repo or GitHub mutations.
2. Complete the startup gate above. Stop immediately if it fails.
3. Ensure the default `reg_meta` DB install is compatible with the checked-out code,
   then ensure the canonical-main `reg_webapp` dev preview is running. Reuse a healthy
   existing preview unless the startup gate's `git pull --ff-only` moved `main` or the
   DB install was refreshed; do not start duplicate servers. Record the frontend URL. If
   preview startup still fails after the DB-refresh path below, continue the tick and
   report the preview as unavailable with the concrete reason.
4. Invoke `/issue-pulse` exactly once. Let it update only the lanes block; apply
   structural issue maintenance afterward under this skill's maintenance policy.
5. Build the operating picture:
   - run `uv run --no-project python scripts/plan_sequence.py --lane`;
   - read issue `#328` and current candidate issue bodies/comments;
   - inspect open PRs that close issues, especially drafts, ready PRs, and stacks;
   - read merge-gate handoff blocks from PR bodies with `gh pr view`, not from
     `scripts/pr_review_status.py`, which is only the Codex bot-review signal.
6. Apply clear, evidence-backed issue maintenance automatically. If it changes
   lane-affecting state such as `priority:*`, `touches`, `Relationships`, `blocked`, or
   `parked`, rerun the `/issue-pulse` lane-staleness path before recommending work; do
   not rely only on `plan_sequence.py --lane` after invalidating the ranked lanes.
7. Merge ready PRs only through the automerge gate below. After each successful merge
   and local fast-forward, restart the preview so it serves the new `main`, then capture
   the merged feature summary and inspection link.
8. For PRs that do not merge, apply the Pipeline Follow-ups policy below before final
   output. If the blocker is mechanical handoff work owned by the pipeline session, send
   a precise follow-up to that session when the thread can be identified.
9. If a merge or lane-affecting issue edit changed during the tick, rerun and follow the
   `/issue-pulse` lane-staleness path before recommending work; do not rely only on
   `plan_sequence.py --lane` after invalidating ranked lanes. Then re-run the live lane
   floor and recommend the next safe `/pr-pipeline issue ...` commands or say to wait.
   For every recommended issue, capture a one-sentence description of what it tackles
   from the issue body. If the body is too vague to support that, say so instead of
   inventing detail.

## Dev Preview

Keep one `reg_webapp` dev preview running from the canonical main checkout:

- Treat the persistent default `reg_meta` DB install as maintained state for the
  canonical preview. The default is `reg_meta.db.default_db_dir()` with no `REG_META_DB`
  override (`$XDG_DATA_HOME/reg_meta` or `~/.local/share/reg_meta`).
- Before starting or reusing the default preview on every tick, run
  `env -u REG_META_DB uv run reg-meta update --yes` from the canonical checkout. This
  updates missing or stale default DB/doc DB assets when a newer compatible release
  exists and is safe to repeat. Do not wait for app startup to reveal doc-DB problems:
  the webapp can start with docs disabled when the doc DB is missing or incompatible.
- If the update installs or replaces either DB asset, restart any existing default
  preview before reporting its URL. A healthy old process can still be serving against
  the pre-update DB handles.
- If preview startup fails because the default DB or doc DB is missing or schema-stale,
  run `env -u REG_META_DB uv run reg-meta update --force --yes`, then retry the default
  preview without `REG_META_DB`. The update command validates downloaded assets against
  the checked-out `reg_meta` code before replacing existing files, so do not hand-edit,
  delete, or rebuild the user cache directly.
- Do not work around a stale default install by downloading release DBs to `/tmp` and
  launching the ordinary preview with `REG_META_DB=/tmp/...`. Use a `/tmp` or scratch
  `REG_META_DB` only when the merged feature explicitly depends on unpublished DB
  content or the user asks to inspect a scratch build.
- Prefer `.claude/launch.json` entry `reg-webapp`, which runs
  `bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh preview` with `autoPort`, and
  preserve the returned frontend URL.
- If preview tooling is unavailable, run
  `bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh` in a managed long-running
  session and preserve the printed `frontend:` URL. Do not use `smoke` or `shot` for the
  persistent preview; those modes auto-teardown.
- Reuse a healthy existing main-checkout preview. Check the frontend URL and
  `/api/context` before starting another server.
- If the startup gate's `git pull --ff-only` moved local `main`, restart the preview
  before reusing it. A healthy URL only proves that the old process responds; the
  FastAPI process does not autoreload Python code.
- After each merge and fast-forward, restart the preview before reporting a feature
  link. A browser refresh alone can leave the FastAPI process serving pre-merge
  backend/API code. If restart fails, report the failure; do not invent a working URL.
- If the preview cannot be started or restarted, do not block a safe merge solely for
  preview availability. Report the merge, say the preview is unavailable, and give the
  concrete startup failure or missing-tool reason.
- If the merged feature depends on unpublished DB content or a scratch build-db result,
  the default preview may not show it. Say that explicitly and give the
  `REG_META_DB=<scratch-db-dir> bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh`
  form when a scratch DB is the right inspection target.

For each merged PR, summarize what was added and where to see it:

- Inspect the PR, closing issue, changed files, and merge diff; write one sentence about
  the user-visible feature.
- If visible in the SPA, link to the current preview URL plus the most specific real
  route: `/`, `/catalog`, `/catalog/<fqid>`, `/catalog/group/...`, `/search?q=...`,
  `/project`, or `/doc/<identifier>`.
- Prefer routes named by the PR's visual proof, issue, tests, or changed component. If
  no exact route is known, link to the narrowest stable entry point and say what to
  click/search.
- For internal-only, build-only, release-only, or tracker-only changes, say
  `No preview page` and name the best verification surface instead.

## Automerge

Merge only on the current head and only when every item passes:

- PR is open, non-draft, mergeable, based on `main`, and not blocked by stack order or a
  maintainer stop note.
- PR body contains `<!-- pr-pipeline-merge-gate -->` with `status: ready-to-merge` and
  `head: <sha>` matching GitHub's current `headRefOid`. This single current-head block
  is the `/pr-pipeline` handoff signal; no separate ready-to-merge comment is required.
- The handoff block has trusted provenance: the PR came from `/pr-pipeline` or a
  maintainer-run equivalent, and the block was added or refreshed by a trusted
  maintainer/agent. If the PR author could self-certify the block without that trusted
  handoff, block automerge and ask the user.
- The gate block records risk-scaled independent review, tests/checks, docs decisions,
  and required visual/build-db proof. Bot-only review is sufficient only for small,
  low-risk PRs.
- Required visual/build-db proof is durable and PR-visible; local `/tmp` paths alone do
  not pass a later tick.
- `gh pr checks <pr>` is green on the current head.
- `uv run --no-project python scripts/pr_review_status.py <pr> --once` is settled with
  `clean`, or `exhausted` with all other gates complete. `findings`, `reviewing`,
  `none`, or tool errors block automerge.
- Stale-head check passes immediately before merge, and the merge command uses
  `--match-head-commit` with that same SHA.
- For stacked PRs, branch deletion cannot close or break dependent PRs. Before merging a
  stack predecessor, inspect open successors' `baseRefName` and `headRefName`. If a
  successor is based on the predecessor branch, do not delete the predecessor branch
  during merge; immediately retarget the successor to `main` after the predecessor
  merge, then verify it remains open on the intended head. After retargeting, require
  the successor branch to be rebased or otherwise updated onto the new base, then
  regenerate checks, Codex bot review, independent-review judgment, and the merge-gate
  block before automerging it.

Merge one PR at a time:

```sh
gh pr merge <pr> --squash --match-head-commit <headRefOid>
```

After each merge, fetch `origin main`, fast-forward the local main checkout with
`git merge --ff-only origin/main`, and verify the PR's changes are actually present on
`main`, not merely that GitHub reports a merge commit. For a stack, re-check the next
PR's head, checks, bot signal, mergeability, durable proof, and gate block before
merging it.

If the merge creates a required build/release boundary, such as DB content that must be
published before dependent work can proceed, invoke `/release minor`. Let the release
skill resolve package scope and run its own gates; stop if it requests input or if the
required bump is not a minor release.

If a ready PR lacks a current-head Codex verdict, comment `@codex review` only when the
gate block shows implementation is finished, then skip the merge until a later tick
observes a settled signal.

## Pipeline Follow-ups

When a PR is close to merge-ready but blocked by mechanical pipeline handoff work, route
that work back to the owning `/pr-pipeline` session instead of only reporting the block.

Send a pipeline follow-up when the PR is open, trusted, and blocked by a narrow
pipeline-owned item such as local-only `/tmp` visual proof, missing PR-visible build-db
proof, stale/incomplete merge-gate evidence, unchanged draft/ready state after finished
work, or a current-head gate mismatch.

Use thread tools when exposed: search/list by PR number, issue number, branch, worktree,
or recent title/history; send to the best matching existing pipeline thread; do not
create a new thread; if the match is ambiguous, report that and include the exact
message text to send.

The message must name the PR, issue, current head SHA, specific blocker, exact unblock
steps, required PR-visible evidence, and
`Do not merge; chief-of-staff owns merge execution.` Do not request implementation
changes unless the evidence proves false. Avoid repeat messages for the same PR head and
blocker unless the blocker/head changes or clear evidence shows the prior request was
missed after meaningful time has passed.

## Issue Maintenance

Keep the tracker current without asking for every mechanical edit:

- required area/type/status/priority-label hygiene;
- priority labels when explicit maintainer signal or dependency graph evidence makes the
  update mechanical;
- parent/sub-issue wiring already stated by `Part of #...`;
- clear `Relationships` and `touches` fixes;
- closing issues whose closing PR is merged and verified on `main`;
- stale `blocked` / `parked` label corrections grounded in current blockers or
  maintainer comments.

Ask only when the edit would choose product direction, change issue scope, invent a
priority without evidence, unpark maintainer-deferred work without an explicit resume
signal, close disputed/partial work, create a new issue, delete substantive prose, or
resolve contradictory live signals.

## Lane Recommendation

- Do not recommend blocked, parked, held, pending-release, already claimed, or
  insufficiently described work.
- Keep active concurrency small: default to 2-3 independent lanes maximum, and only one
  high-cost or build-affecting lane at a time.
- Avoid overlapping `reg_meta_build` / build-db work while another open pipeline touches
  build, input-data, curation, or release surfaces.
- Prefer a small coherent bundle or explicit stack over a broad backlog summary.
- `Recommended next:` should list 1-3 `/pr-pipeline` launches, constrained by the free
  lane set and current active work budget. Use `none` only when no safe launch is free,
  the active-work budget is saturated, metadata is too stale to trust, or a
  release/merge gate must clear first. Do not pad to three.

## Subagents

For scheduled heartbeat ticks, default to no subagents: use direct `gh` calls and repo
scripts first so the tick finishes predictably. Spawn subagents only when a material
merge/recommendation ambiguity cannot be resolved quickly in the main context and the
tick has enough time left to close them before returning. Manual/ad-hoc runs may use
subagents more proactively for separable read-only checks, but never delegate live issue
mutation.

Return concise output:

```text
chief tick: <fresh/restamped/reranked>; <hygiene>; active <n>; free <n>
Preview: <frontend URL or unavailable: reason>
Active work: PR #<p> -> #<issue>: <status / risk>, or none
Merged: PR #<p> -> #<issue>: <merge sha>; added <one-sentence feature summary>; see
  <preview URL + route, or "No preview page: <verification surface>">, or none
Recommended next:
1. `/pr-pipeline issue <n>[,<m>]` - <lane label>; <shape>; <why / guardrail>.
   #<n>: <one sentence describing what this issue tackles>.
   #<m>: <one sentence describing what this issue tackles, if bundled>.
Issue maintenance: applied <...>; needs input <... or none>
Pipeline follow-ups: sent <PR/thread/blocker or none>; needed but not sent <... or none>
Watch: <blocked decision, pending release, stale review, or next trigger>
```

## Heartbeat Decision

- Use `DONT_NOTIFY` when there was no merge, no issue maintenance, no lane content or
  recommendation change, no new actionable blocker, and active PR statuses are
  materially unchanged.
- Use `NOTIFY` when the tick merged or released work, changed issue metadata, re-ranked
  or re-stamped lanes in a way that changes the free/active/recommended sets, found a
  gate failure on a PR that looked ready, failed to refresh the preview after a merge,
  or needs user input.
- For skipped overlapping heartbeats, use `DONT_NOTIFY` with reason
  `previous tick still running`.
