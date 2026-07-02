---
name: chief-of-staff
description: >-
  Registry Research Toolkit chief-of-staff loop for recurring issue coordination. Use
  when asked to run or schedule a staff tick, combine issue-pulse and live PR claim
  state, automatically merge ready PRs with current-head PR-pipeline handoff evidence,
  keep issue priorities or metadata current, keep the reg_webapp dev preview running,
  keep the default reg_meta DB install current for that preview, summarize merged
  user-visible features with preview links, prevent conflicting pr-pipeline work, send
  unblock follow-ups to stalled pr-pipeline sessions, or recommend the next safe issue
  bundle or PR stack to start from a separate worktree.
---

# Registry Chief Of Staff

**Only run when the user explicitly invokes this skill (or the user-configured
chief-of-staff heartbeat fires).** It merges PRs, edits issues, and runs releases —
never auto-start it because a conversation merely resembles coordination or merge work.

The chief of staff is the repo's coordination agent. It keeps the issue graph and lane
priorities current, understands active PR-pipeline claims, prevents conflicting work,
merges PRs that have a current-head pipeline handoff, and recommends the next work the
user should launch in separate worktrees. It also keeps a canonical-main `reg_webapp`
dev preview available so merged user-visible work can be inspected immediately.

The skill is designed for scheduled use. For recurring use, prefer one heartbeat
attached to a single existing chief-of-staff thread. Run one coordination tick to
completion, then stop; external automation owns the cadence. Do not schedule detached
cron/workspace jobs unless the user explicitly accepts multiple independent coordinator
contexts.

It does not implement issue code or start `pr-pipeline`. Its product is a short
operating picture, any safe merges or required release action, preview links for merged
user-facing changes, and exact recommendations for work the user should launch
separately.

Default epic is `328`.

## Scheduling

For recurring chief-of-staff use, maintain one active heartbeat pointed at one existing
chief-of-staff thread. The goal is one continuing coordinator context, not a set of
detached jobs.

- Prefer a heartbeat that resumes the chosen chief-of-staff thread rather than starting
  a fresh context each cadence.
- Do not create detached cron/workspace jobs by default; they can run as independent
  chiefs of staff and duplicate merge/recommendation decisions. Use them only after the
  user explicitly accepts that tradeoff.
- Keep the scheduled prompt minimal, e.g. `Run exactly one chief-of-staff tick`, so this
  skill remains the source of truth.
- The session surface processes turns serially, so a heartbeat that fires mid-tick lands
  as the NEXT turn once the active tick finishes; ticks never actually overlap. That
  next turn just runs the preflight probe again — an idle probe is one cheap tool call —
  and returns `DONT_NOTIFY` reason `idle` if nothing moved. Do not attempt to detect or
  skip a "still-running" prior tick; there is no such state to detect.
- After creating or updating the scheduled heartbeat, verify the persisted automation
  actually targets the existing chief-of-staff thread with the intended cadence and an
  active status. If it persisted as a detached/cron-style or paused job instead, report
  that exact state and stop rather than leaving a mis-wired schedule running.

### In-session minimal tick

There is no external wake wrapper. The surface's own scheduler resumes the one
chief-of-staff thread on a cadence of roughly 15-30 minutes, and a deterministic
preflight decides whether the model actually does any work that tick:

```sh
uv run --no-project python scripts/cos_preflight.py
```

- **The session's FIRST action each tick is the preflight probe.** It compares live
  repo/GitHub state against the last committed baseline in
  `.git/cos-preflight-state.json` and stages what it observed as a candidate next to
  that file. A steady-state probe with an existing baseline never writes the baseline
  itself; only `--commit` (or the first-run bootstrap, which writes the baseline
  directly when none exists yet) does. It fires only when state moved enough to justify
  a tick: lane drift, issue-projection movement, `origin/main` movement, or relevant
  issue-closing PR / merge-gate state changes.

- **Exit `0` (idle):** stop immediately with `DONT_NOTIFY` reason `idle`, spending
  nothing beyond that one tool call.

- **Exit `2` (tool error):** report the tool error and stop.

- **Exit `10` (wake):** run the full tick.

- **At the END of a successful active tick**, commit the staged candidate and probe once
  more:

  ```sh
  uv run --no-project python scripts/cos_preflight.py --commit
  ```

  The `--commit` run promotes the candidate the probe staged (a pure local file move —
  no gh/git calls); then run the plain probe again. If it exits `10`, handle the new
  events in the same tick. Bound this: after roughly 3 loops, finish and let the next
  heartbeat continue.

- **If `--commit` fails**, retry it once. If it still fails, report the tool error
  (`NOTIFY`) and stop, naming the consequence: the baseline was not advanced, so the
  next heartbeat re-wakes on the same events. Before re-sending any follow-up or feature
  report those events would trigger next tick, re-check it against live state so a
  duplicate isn't sent.

- **At-least-once by design:** a tick that fails before `--commit` leaves the baseline
  at the last committed candidate, so the next probe re-fires on the pending event.
  Never run `--commit` before the work is actually done.

## Startup Gate

Run only from the canonical main checkout: `/Users/adam/Code/registry-research-toolkit`.
Do not run from a git worktree.

Before invoking `issue-pulse`, editing issues, inspecting merge candidates, merging, or
recommending new work:

1. Verify the repo top-level is exactly `/Users/adam/Code/registry-research-toolkit`
   with `git rev-parse --show-toplevel`, the current branch is `main` with
   `git branch --show-current`, and `.git` is a directory with `test -d .git`. A linked
   worktree has a `.git` file, so it must fail this gate. If any check fails, stop and
   tell the user to fix the checkout before relaunching.
2. Run `git pull --ff-only` as the first sync action.
3. Re-verify the checkout is the canonical main checkout on `main`, then run
   `git status --short`. If the repo is dirty, stop and tell the user to clean or commit
   the main checkout before relaunching.

All implementation work happens in separate worktrees through `pr-pipeline`. From the
main checkout, this skill may coordinate issues, inspect PRs, merge ready PRs, and
recommend new work, but it must not edit project code as part of the work itself.

## Tick

1. Complete the startup gate above. Stop immediately if it fails.
2. Ensure the default `reg_meta` DB install is compatible with the checked-out code,
   then ensure the canonical-main `reg_webapp` dev preview is running, following the Dev
   Preview section below. Reuse a healthy existing preview; do not start duplicate
   servers unless the startup gate's `git pull --ff-only` moved `main` or the DB install
   was refreshed. Record the frontend URL for the final report.
3. Invoke and follow the `issue-pulse` skill exactly for one heartbeat tick, including
   its tick-status, basis, restamp, re-rank, and refusal safeguards. Let `issue-pulse`
   write only its generated lanes block; apply structural issue maintenance afterward
   under this skill's maintenance policy.
4. Build the current operating picture:
   - Run `uv run --no-project python scripts/plan_sequence.py --lane` to get the live
     free, held, running, blocked, parked, and pending-release floor.
   - Read issue `#328` body and comments for current editorial intent.
   - Inspect open PRs that close issues, especially draft PRs, stale ready PRs, and PRs
     touching the same surfaces as free candidates. Prefer `plan_sequence.py`'s held /
     running state for dispatch decisions. Inspect merge-gate handoff state from the PR
     body with
     `gh pr view <pr> --json body,headRefOid,author,baseRefName,headRefName,isDraft,mergeable`;
     `scripts/pr_review_status.py` does not read the PR body. For Codex bot-review
     status, use `uv run --no-project python scripts/pr_review_status.py <pr>` rather
     than inferring from `gh pr view`.
   - Read candidate issue bodies and comments before recommending them. Fetch one issue
     per command; do not pass a space-separated issue list as one `gh issue view`
     identifier.
5. Apply issue maintenance:
   - Treat `parked` as a first-class non-dispatch state.
   - Distinguish real blockers from polish: missing relationship links, stale `blocked`
     / `parked` labels, wrong area/type labels, missing `touches` blocks, and priority
     drift are different classes.
   - Apply clear, evidence-backed fixes automatically. Stop and ask only for material
     conflicts, destructive choices, or issue scope/priority judgments that are not
     grounded in current issue/epic/PR evidence.
   - If maintenance changes lane-affecting state such as `priority:*`, `touches`,
     `Relationships`, `blocked`, or `parked`, rerun and follow the `issue-pulse`
     lane-staleness path before recommending work; do not rely only on
     `plan_sequence.py --lane` after invalidating the ranked lanes.
6. Merge ready PRs, if any pass the automerge gate below. After every successful merge
   and local fast-forward, restart the canonical-main dev preview so it serves the new
   `main`, then capture the merged feature summary and inspection link.
7. For PRs that do not merge, apply the Pipeline Follow-ups policy below before final
   output. If the blocker is mechanical handoff work owned by the pipeline session, send
   a precise follow-up to that session when the thread can be identified.
8. Decide whether new pipelines should start:
   - If a merge or lane-affecting issue edit changed during the tick, rerun and follow
     the `issue-pulse` lane-staleness path before recommending work; do not rely only on
     `plan_sequence.py --lane` after invalidating ranked lanes.
   - Re-run `uv run --no-project python scripts/plan_sequence.py --lane` immediately
     before the final recommendation if any PR claim, merge, or issue edit changed
     during the tick.
   - Do not recommend blocked, parked, held, pending-release, or already-claimed work.
   - Do not recommend work whose `touches` metadata is missing or suspect until the
     metadata fix is called out.
   - Keep active concurrency small. Default to no more than 2-3 independent
     `pr-pipeline` lanes, and only 1 high-cost or build-affecting lane at a time.
   - Avoid overlapping `reg_meta_build` / build-db work while any open pipeline touches
     the same build, input-data, curation, or release surfaces.
   - Prefer a small coherent bundle or explicit sequential stack over a broad backlog
     summary.
   - For every recommended issue, capture a one-sentence description of what it tackles
     from the issue body. If the body is too vague to support that, say so instead of
     inventing detail.
9. Recommend commands only after the live floor and active PR claims agree.

## Dev Preview

Keep one `reg_webapp` dev preview running from the canonical main checkout so the user
can inspect freshly merged user-facing changes.

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
- Prefer the existing preview launch config: `.claude/launch.json` entry `reg-webapp`,
  which runs `bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh preview` with
  `autoPort`. Use the agent surface's preview tools when available, because they return
  the actual frontend URL.
- If preview tools are unavailable, start the same checkout's helper in a managed
  long-running session and preserve the printed `frontend:` URL:
  `bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh`. The plain serve mode
  auto-picks free backend/frontend ports and blocks. Do not use `smoke` or `shot` for
  the persistent preview; those modes intentionally tear down the servers.
- Reuse an existing healthy preview for `/Users/adam/Code/registry-research-toolkit`.
  Before starting a new one, check whether the recorded frontend URL still responds and
  whether `/api/context` works through it. Do not keep multiple main-checkout previews
  alive just because the old URL was forgotten.
- If the startup gate's `git pull --ff-only` moved local `main`, restart the preview
  before reusing it. A healthy URL only proves that the old process responds; the
  FastAPI process does not autoreload Python code.
- After any successful merge and fast-forward of local `main`, restart the preview
  before reporting the feature link. A browser refresh alone can leave the FastAPI
  process serving pre-merge backend/API code.
- If the merged feature depends on unpublished DB content or a scratch build-db result,
  the default preview may not show it. Say that explicitly and give the
  `REG_META_DB=<scratch-db-dir> bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh`
  form if a scratch DB is the right inspection target.
- If the preview cannot be started or refreshed, do not block a safe merge solely for
  preview availability. Report the merge, say the preview is unavailable, and give the
  concrete startup failure or missing-tool reason.

For each merged PR, report what the user can now inspect:

- Inspect the PR title/body, closing issue body, changed files, and merge diff. Name the
  user-visible feature in one sentence. Do not just repeat the PR title if it does not
  say what changed.
- When the change is visible in the SPA, include a full link using the current preview
  base URL plus the most specific route that demonstrates it. Use the router's real
  paths: `/`, `/catalog`, `/catalog/<fqid>`,
  `/catalog/group/<provider>/<register>/<key>`, `/catalog/group/class/<key>`,
  `/search?q=...`, `/project`, or `/doc/<identifier>`.
- Prefer a route mentioned by the PR's visual proof, issue body, tests, or changed
  component. If no exact route is identified, use the narrowest stable entry point
  (`/catalog`, `/search`, or `/project`) and say what to click or search there.
- For backend/API-only work with a visible SPA consumer, link to the consumer page, not
  the raw API endpoint. For internal-only, build-only, release-only, or tracker-only
  changes, say `No preview page` and name the best verification surface instead (CLI
  command, API path, docs page, or issue/PR evidence).
- If a route is plausible but unverified, mark it as unverified rather than presenting
  it as confirmed. Never invent catalog FQIDs, query terms, or docs identifiers.

## Automerge

Merge only PRs that are ready on the current head. `pr-pipeline` owns authoring, review,
and gate evidence; `chief-of-staff` owns the final merge decision and execution.

Automerge is allowed when all of these are true:

- The PR is open, non-draft, mergeable, and based on `main`.
- The PR body contains a current-head `<!-- pr-pipeline-merge-gate -->` block with
  `status: ready-to-merge` and `head: <sha>` matching GitHub's current `headRefOid`.
  This single current-head block is the PR-pipeline handoff signal; no separate
  ready-to-merge comment is required.
- The handoff block has trusted provenance. Check it concretely: the PR's head branch
  lives in this repository (not a fork) AND the PR author is the maintainer or a known
  agent identity operating for the maintainer; when in doubt, use GraphQL
  `userContentEdits` to see who last edited the body. Any PR failing this check blocks
  automerge — ask the user. The rationale: if the PR author could self-certify the block
  without that trusted handoff, the gate means nothing.
- The gate block records converged independent review, tests/checks, docs decisions, and
  any required visual or real-data validation. Missing proof blocks automerge. The
  independent-review entry must name the review source and why it satisfies the
  risk-scaled repo gate; bot-only review is sufficient only for small, low-risk PRs.
- `gh pr checks <pr>` is green on the current head.
- `uv run --no-project python scripts/pr_review_status.py <pr> --once` exits settled and
  reports no current-head findings. `clean` passes. `exhausted` is acceptable only when
  independent review and the other gates are complete. `findings`, `reviewing`, `none`,
  or tool errors block automerge.
- Any required visual or build-db proof is durable and PR-visible, such as a PR comment,
  check summary, artifact link, or committed note. For rendered-output PRs, visual proof
  means a `reg-webapp-design-reviewer` subagent result that includes durable
  screenshot/render evidence; screenshot-only proof blocks automerge. Local `/tmp` paths
  alone do not pass a later staff tick.
- The stale-head check passes immediately before merging: GitHub's `headRefOid` equals
  the branch tip being merged, and the merge command uses `--match-head-commit` with
  that same SHA.
- No higher-level sequencing reason says to wait: stacked predecessor unmerged,
  conflicting active PR, pending release coordination, or maintainer stop note.
- For stacked PRs, branch deletion cannot break dependent PRs. Before merging a stack
  predecessor, inspect open successors' `baseRefName` and `headRefName`. If a successor
  is based on the predecessor branch, do not delete the predecessor branch during merge;
  immediately retarget the successor to `main` after the predecessor merge, then verify
  it remains open on the intended head. After retargeting, require the successor branch
  to be rebased or otherwise updated onto the new base, then regenerate checks, Codex
  bot review, independent-review judgment, and the merge-gate block before automerging
  it. Never delete a branch that is the head branch of another open PR.

Use the repo's normal squash merge:

```sh
gh pr merge <pr> --squash --match-head-commit <headRefOid>
```

After each merge, fetch `origin main`, fast-forward the local main checkout with
`git merge --ff-only origin/main`, and verify the PR's changes are actually present on
`main`, not merely that GitHub reports a merge commit. For a stack, merge one PR at a
time in dependency order, then re-check the next PR's head, mergeability, checks, Codex
bot signal, and gate block before merging it. Do not batch-merge a stack from stale
evidence.

If the merge creates a required build/release boundary, such as DB content that must be
published before dependent work can proceed, the chief of staff is authorized to invoke
the release workflow as `$release minor`. Let the release skill resolve package scope
and run its own gates; stop if it requests input or if the required bump is not a minor
release.

If a PR is otherwise ready but the Codex bot has no current-head verdict, request one
with `@codex review` only if the PR-pipeline block says the implementation is finished;
then skip the merge until a later tick observes a settled signal.

## Pipeline Follow-ups

When a PR is close to merge-ready but blocked by mechanical pipeline handoff work, route
that work back to the owning `pr-pipeline` session instead of only reporting the block.

Send a pipeline follow-up when all of these are true:

- the PR is open and appears to be owned by `pr-pipeline` or a trusted equivalent;
- the current blocker is narrow, factual, and owned by the authoring pipeline, such as
  local-only `/tmp` visual proof, missing PR-visible build-db proof, a stale or
  incomplete merge-gate line, an unchanged draft/ready state after the pipeline says it
  is finished, or a current-head gate evidence mismatch;
- the requested work can be done without implementation changes unless explicitly
  stated;
- a likely owning thread can be identified from available thread tools by PR number,
  issue number, branch name, worktree path, or recent thread title/history.

Use thread tools when the agent surface exposes them:

- search/list threads by PR number, issue number, branch, and worktree path;
- send the message to the best matching existing pipeline thread;
- do not create a new thread for an existing pipeline follow-up;
- if the match is ambiguous, do not guess. Report the ambiguity and include the exact
  message text the user or next tick should send.

Make the follow-up prompt action-shaped and bounded:

```text
Chief-of-staff follow-up for PR #<pr> / issue #<issue>:

The PR is blocked only because <specific blocker>. Please fix the handoff evidence
without changing implementation code unless you discover the evidence is false.

Do this on current head <sha>:
1. <exact unblock step, e.g. post durable PR-visible visual proof with command, route,
   viewport set, inspected result, and head SHA>.
2. <update the merge-gate line/body to point to that durable proof>.
3. Re-read PR #<pr> and confirm status/head/evidence still match.

Do not merge; chief-of-staff owns merge execution.
```

Do not spam the same session. Send at most one follow-up for the same PR head and
blocker unless the blocker changes, the PR head changes, or a later tick sees clear
evidence that the prior request was missed after meaningful time has passed.

Do not use follow-ups to make product calls, alter scope, request broad refactors, or
ask a pipeline to bypass the merge gate. If the blocker is a real failed check, Codex
finding, merge conflict, or code defect, tell the pipeline to fix that specific failure;
if it is direction or priority ambiguity, ask the user instead.

## Issue Maintenance

Keep the tracker current without asking for every mechanical edit. Evidence-backed
maintenance includes:

- labels required by repo policy: exactly one area label, a type label, `blocked` /
  `parked` status agreement, and at most one `priority:*` label;
- priority labels when the maintainer signal is explicit in issue/epic/PR text or the
  current dependency graph makes the update mechanical;
- native parent/sub-issue wiring when the body already says `Part of #...`;
- `Relationships` fixes when the target issue/PR state makes the relation clear;
- `touches` additions or corrections when the issue scope names the affected paths;
- closing issues whose closing PR is merged and whose changes are verified on `main`;
- removing stale `blocked` labels when all named blockers are closed/merged, or adding
  `blocked` when an open `Blocked by` relation already exists;
- adding/removing `parked` when the issue text or maintainer comment explicitly marks
  the work deferred or resumes it.

Ask the user instead of applying when the edit would choose product direction, change an
issue's intended scope, invent a new priority without a clear maintainer signal, unpark
maintainer-deferred work without an explicit resume signal, close an issue with partial
or disputed completion, create a new issue, delete substantive issue prose, or resolve a
contradiction between labels, body text, comments, and live PR state.

## Bundle Selection

Shape recommendations as work the user can launch from a separate worktree.

- Bundle by shared file surface, semantic dependency, and review shape, not only by
  package name.
- Split into sequential PRs when one issue creates a contract or design base another
  issue should consume.
- Keep unrelated ready lanes separate even if they belong to the same epic.
- Prefer work that unblocks more issues, clears stale tracker state, or fits the current
  active-PR budget.
- If the best answer is to wait, say that directly and name the blocking PRs or stale
  metadata that must clear first.

Use command-shaped recommendations:

```text
Recommended:
1. `$pr-pipeline issue <n>[,<m>]` - <lane label>; <shape>; <why / guardrail>.
   #<n>: <one sentence describing what this issue tackles>.
   #<m>: <one sentence describing what this issue tackles, if bundled>.
```

If no new work should start:

```text
Recommendation: do not start a new pr-pipeline now.
Reason: <active claim budget / overlap / blocked metadata / pending release>
Next trigger: <PR merged, issue unparked, hygiene fix approved, or next tick>
```

## Subagents

For scheduled heartbeat ticks, default to no subagents: use direct `gh` calls and repo
scripts first so the tick finishes predictably. Spawn subagents only when a material
merge/recommendation ambiguity cannot be resolved quickly in the main context and the
tick has enough time left to close them before returning.

For manual/ad-hoc runs, use subagents for separable read-only checks when the
environment exposes them and the tick is non-trivial:

- one subagent to audit open PR claims and likely conflicts;
- one subagent to inspect candidate issue bodies/comments and stale metadata;
- optionally one subagent to sanity-check the proposed bundle for semantic conflicts.

Do not delegate live issue mutation. Reconcile subagent findings against the live
`plan_sequence.py --lane` floor before returning recommendations.

## Output

Keep output terse and operational. Be compact by grouping status into dense lines, not
by dropping decision evidence. For quiet ticks, one line is enough.

For active ticks, use this shape:

```text
chief tick: <fresh/restamped/reranked>; <hygiene state>; active <n>; free <n>
Preview: <frontend URL or unavailable: reason>

Active work:
- PR #<p> -> #<issue>: <status / risk>

Merged:
- PR #<p> -> #<issue>: <merge sha>; added <one-sentence feature summary>; see
  <preview URL + route, or "No preview page: <verification surface>">, or none

Recommended next:
1. `$pr-pipeline issue <n>[,<m>]` - <lane label>; <shape>; <why / guardrail>.
   #<n>: <one sentence describing what this issue tackles>.
   #<m>: <one sentence describing what this issue tackles, if bundled>.
2. `$pr-pipeline issue <n>` - <lane label>; <shape>; <why / guardrail>.
   #<n>: <one sentence describing what this issue tackles>.

Issue maintenance:
- applied: <specific metadata fixes or none>
- needs input: <material conflict or none>

Pipeline follow-ups:
- sent: <PR/thread/blocker or none>
- needed but not sent: <ambiguous thread or unsupported tool, plus blocker or none>

Watch:
- <blocked decision, pending release, stale review, or next trigger>
```

`Recommended next:` should list 1-3 pr-pipeline launches, constrained by the free lane
set and current active work budget. Use `none` only when no safe launch is free, the
active-work budget is saturated, metadata is too stale to trust, or a release/merge gate
must clear first. Do not pad to three: one good recommendation is better than three weak
ones.

Report checks that were not run. Do not claim a live check passed if it was skipped or
failed.

## Heartbeat Decision

When invoked by a heartbeat, wrap the final report in the platform's heartbeat decision
only when that surface expects it.

- Use `DONT_NOTIFY` when there was no merge, no issue maintenance, no lane content or
  recommendation change, no new actionable blocker, and active PR statuses are
  materially unchanged. Keep the reason to one sentence.
- Use `NOTIFY` when the tick merged or released work, changed issue metadata, re-ranked
  or re-stamped lanes in a way that changes the free/active/recommended sets, found a
  gate failure on a PR that looked ready, failed to refresh the preview after a merge,
  failed to `--commit` the preflight candidate, or needs user input.
- For a heartbeat that lands after an idle probe, use `DONT_NOTIFY` with reason `idle`.

## Guardrails

- Run one tick and stop; external automation owns the cadence. Ticks run serially, so a
  heartbeat that fires mid-tick just runs the next turn as a fresh (usually idle) probe;
  there is no overlapping-tick state to detect or skip.
- Do not start, claim, or implement `pr-pipeline` work from this skill.
- Do not fix pipeline-owned implementation or handoff evidence directly when a precise
  follow-up to the owning pipeline session can unblock it.
- Do not start duplicate `reg_webapp` main-checkout previews; reuse or restart the
  existing one.
- Do not merge without current-head PR-pipeline gate evidence plus fresh live checks.
- Do not merge without a current-head `status: ready-to-merge` merge-gate block.
- Do not merge PRs with unresolved findings, pending checks, draft status, stale heads,
  missing required visual/build-db proof, or unresolved stack predecessors.
- Do not allow branch deletion to close or break a stacked successor PR.
- Do not exceed the current active-work budget to keep agents busy.
- Do not recommend parked/deferred issues, even if epic prose still mentions them.
- Do not invent feature routes after a merge (see the Dev Preview merged-PR link rules).
- Do not treat a clean hygiene script as proof that semantic relationships are current;
  read issue text when comments imply a blocker or dependency.
- Do not hand-edit generated `plan-sequence` or `plan-lanes` markers; use the repo
  scripts through `issue-pulse`.
- Do not post status-consolidation comments on epic `#328`.
- Do not ask for routine maintenance approval. Stop only for material conflicts,
  destructive ambiguity, or direction/priority choices without clear current evidence.
