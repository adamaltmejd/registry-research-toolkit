---
name: chief-of-staff
description: "Run one registry chief-of-staff tick: invoke /issue-pulse, keep the reg_webapp dev preview and default reg_meta DB install current, inspect live issue and PR claim state, automatically maintain issue metadata/priorities, squash-merge PRs with current-head pr-pipeline handoff evidence, send unblock follow-ups to stalled /pr-pipeline sessions, report merged user-facing features with preview links, run /release minor when a merge creates a required build/release boundary, and recommend the next safe /pr-pipeline lanes. Usage: /loop 30m /chief-of-staff"
---

# chief-of-staff — one coordination tick

**Only run when the user explicitly invokes `/chief-of-staff` (or the user-configured
chief-of-staff heartbeat fires).** It merges PRs, edits issues, and runs releases —
never auto-start it because a conversation merely resembles coordination or merge work.

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

### Two-tier wake

The wake path has two tiers. Both fire the SAME tick — the preflight
probe/baseline/`--commit` contract below is identical regardless of which tier woke the
session; the tiers only change WHEN a tick fires.

- **Tier 1 — merge-gate watcher (event-driven merges).** The highest-value event — a
  `/pr-pipeline` handoff writing `status: ready-to-merge` into the local gate store — is
  a local file write, so it wakes the loop within seconds instead of waiting out the
  heartbeat. When the loop is self-paced (`/loop` dynamic mode), arm the watcher ONCE
  per session, not per tick: first call `TaskList` and skip arming if a gate-watch
  monitor is already running; otherwise arm exactly one:

  ```text
  Monitor({
    command: "uv run --no-project python scripts/cos_gate_watch.py",
    description: "merge-gate ready watch",
    persistent: true,
    timeout_ms: 60000   // schema-required; ignored when persistent
  })
  ```

  The watcher polls the gate store (`merge-gates/pr-<N>/gate.json` under
  `$XDG_STATE_HOME/registry-research-toolkit`, default `~/.local/state/...`) every
  \~20s, locally with no network calls, and emits one line —
  `ready gate: pr=<N> head=<sha12> updated=<iso>` — only when a gate BECOMES
  ready-to-merge: a new handoff, a re-verification (`updated` bump), or a new `head`.
  Steady-state ready gates, `status: blocked`, evidence-file writes, and self-serve
  `build_db` running stamps never emit, so the watcher cannot thrash the loop; those
  signals stay on the Tier-2 poll. On a watcher event, run a normal tick — starting with
  the preflight probe, exactly as below — then re-arm the ScheduleWakeup fallback.

- **Tier 2 — slow hygiene poll (fallback heartbeat).** With the gate watcher armed,
  stretch the ScheduleWakeup fallback to \~2700-3600s. This tier catches everything the
  watcher deliberately ignores: remote GitHub drift (new PRs, CI results, Codex
  verdicts, issue/label edits, `origin/main` movement), blocked gates, and in-flight
  self-serve builds. It is explicitly NOT the merge path — merges no longer wait on it.
  If arming the watcher failed, keep the heartbeat at the original 15-30 minutes; it is
  then the only wake path, including for merges.

Caveats: both tiers are session-scoped — a dead session kills the watcher and the timer
together, which is exactly why the ScheduleWakeup fallback must be re-armed on every
wake (a fixed-cadence `/loop` or scheduled heartbeat revives the loop from outside). The
watcher is a local file poll on the single-maintainer machine's gate store; it makes no
wake decisions and the preflight logic is unchanged.

### In-session minimal tick

There is no external wake wrapper. A wake — a Tier-1 gate-watch event or the Tier-2
heartbeat — resumes the one chief-of-staff session, and a deterministic preflight
decides whether the model actually does any work that tick:

```sh
uv run --no-project python scripts/cos_preflight.py
```

- **The session's FIRST action each tick is the preflight probe.** It compares live
  repo/GitHub state against the last committed baseline in
  `.git/cos-preflight-state.json` and stages what it observed as a candidate next to
  that file. Baseline-advance invariant: the probe auto-advances the baseline (writes
  the state file directly) whenever it observed NOTHING actionable and the observation
  moved — reasons empty AND (no baseline yet, or the fingerprint drifted). This is safe
  because zero reasons means there are no events to burn, and it keeps an idle drift
  from suppressing a later recurrence. An observation WITH reasons (a WAKING probe)
  never writes the state file itself; its baseline advances only via the
  fingerprint-bound `--commit`, so a crash before the end-of-tick commit re-fires. A
  probe whose fingerprint equals the baseline writes nothing. The probe's stdout JSON
  includes a `fingerprint` field — capture it; `--commit` needs it. The probe fires only
  when state moved enough to justify a tick: lane drift, issue-projection movement,
  `origin/main` movement, or relevant issue-closing PR / merge-gate state changes.

- **Exit `0` (idle):** stop immediately with `DONT_NOTIFY` reason `idle`, spending
  nothing beyond that one tool call.

- **Exit `2` (tool error):** report the tool error and stop.

- **Exit `10` (wake):** run the full tick.

- **Each round of the tick ends with its OWN commit.** The loop body is: probe → do the
  work → commit that probe's fingerprint → probe again → if it exits `10`, handle the
  new events and commit the NEW fingerprint → repeat:

  ```sh
  uv run --no-project python scripts/cos_preflight.py --commit <fingerprint-from-that-probe>
  ```

  `--commit` promotes (via an atomic rename) the candidate whose fingerprint you
  observed — no snapshot collection or network calls, though it does still verify the
  canonical checkout. Bound the loop: after roughly 3 rounds, finish and let the next
  heartbeat continue — but the LAST round's events must still be committed, or they
  re-wake next heartbeat as duplicate work.

- **If `--commit` fails**, retry it once — retry is always safe here: promotion is bound
  to the fingerprint you observed, and a `--commit` that succeeded but lost its result
  reports `already committed` (exit 0) on retry, resolving the lost-result case
  automatically. A persistent exit 2 means the baseline genuinely did not advance (a
  `fingerprint mismatch` from a stale candidate, `no staged candidate`, or a
  canonical-checkout failure): report the tool error (`NOTIFY`) and stop, naming that
  the next heartbeat re-wakes on the same events. Before re-sending any follow-up or
  feature report those events would trigger next tick, re-check it against live state so
  a duplicate isn't sent.

- **At-least-once by design:** a tick that fails before `--commit` leaves the baseline
  at the last committed candidate, so the next probe re-fires on the pending event.
  Never run `--commit` before the work is actually done.

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

1. Complete the startup gate above. Stop immediately if it fails.
2. Ensure the default `reg_meta` DB install is compatible with the checked-out code,
   then ensure the canonical-main `reg_webapp` dev preview is running. Reuse a healthy
   existing preview unless the startup gate's `git pull --ff-only` moved `main` or the
   DB install was refreshed; do not start duplicate servers. Record the frontend URL. If
   preview startup still fails after the DB-refresh path below, continue the tick and
   report the preview as unavailable with the concrete reason.
3. Invoke `/issue-pulse` exactly once. Let it update only the lanes block; apply
   structural issue maintenance afterward under this skill's maintenance policy.
4. Build the operating picture:
   - run `uv run --no-project python scripts/plan_sequence.py --lane`;
   - read issue `#328` and current candidate issue bodies/comments;
   - inspect open PRs that close issues, especially drafts, ready PRs, and stacks;
   - read merge-gate handoff entries from the local gate store
     (`~/.local/state/registry-research-toolkit/merge-gates/pr-<N>/gate.json`;
     `$XDG_STATE_HOME` root if set) — not from PR bodies, and not from
     `scripts/pr_review_status.py`, which is only the Codex bot-review signal.
5. Apply clear, evidence-backed issue maintenance automatically. If it changes
   lane-affecting state such as `priority:*`, `touches`, `Relationships`, `blocked`, or
   `parked`, rerun the `/issue-pulse` lane-staleness path before recommending work; do
   not rely only on `plan_sequence.py --lane` after invalidating the ranked lanes.
6. Merge ready PRs only through the automerge gate below. After each successful merge
   and local fast-forward, restart the preview so it serves the new `main`, then capture
   the merged feature summary and inspection link.
7. For PRs that do not merge, apply the Pipeline Follow-ups policy below before final
   output. If the blocker is mechanical handoff work owned by the pipeline session, send
   a precise follow-up to that session when the thread can be identified.
8. If a merge or lane-affecting issue edit changed during the tick, rerun and follow the
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
- For backend/API-only work with a visible SPA consumer, link to the consumer page, not
  the raw API endpoint. For internal-only, build-only, release-only, or tracker-only
  changes, say `No preview page` and name the best verification surface instead.
- If a route is plausible but unverified, mark it as unverified rather than presenting
  it as confirmed. Never invent catalog FQIDs, query terms, or docs identifiers.

## Automerge

Merge only on the current head and only when every item passes:

- PR is open, non-draft, mergeable, and based on `main`, with no higher-level sequencing
  reason to wait: stacked predecessor unmerged, conflicting active PR, pending release
  coordination, or a maintainer stop note.
- The local gate store has `pr-<N>/gate.json` with `status: ready-to-merge`, a `pr`
  field matching the directory's PR number, and `head` matching GitHub's current
  `headRefOid`. Where the `visual` / `build_db` gates apply, their per-gate head stamps
  must also equal that head — a top-level `head` refresh with a trailing per-gate SHA
  means the expensive gate was verified on an older head, and blocks automerge (the
  `build_db` case is self-served below; a trailing `visual` SHA routes a follow-up).
  This single current-head entry is the `/pr-pipeline` handoff signal; PR bodies carry
  no gate block and no ready-to-merge comment is required. Provenance is by construction
  — only agents on this machine can write the store — but never automerge a PR whose
  head branch is not in this repository (a fork PR with a gate entry is an error to
  surface, not a merge candidate).
- The gate entry records converged independent review, tests/checks, docs decisions, and
  required visual/build-db results. The independent_review line must name the review
  source and why it satisfies the risk-scaled repo gate; bot-only review is sufficient
  only for small, low-risk PRs.
- Required visual/build-db evidence files are present in the PR's gate directory. For
  rendered-output PRs, visual proof means a `/reg-webapp-design-reviewer` report with
  its screenshots in the gate directory; screenshots without the reviewer report block
  automerge. References to scratch or `/tmp` paths instead of files in the gate
  directory do not count — the artifacts must be there.
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
  regenerate checks, Codex bot review, independent-review judgment, and the gate entry
  before automerging it.

Merge one PR at a time:

```sh
gh pr merge <pr> --squash --match-head-commit <headRefOid>
```

After each merge, fetch `origin main`, fast-forward the local main checkout with
`git merge --ff-only origin/main`, and verify the PR's changes are actually present on
`main`, not merely that GitHub reports a merge commit. Then move the merged PR's gate
directory to `merge-gates/merged/pr-<N>/` — the PR deliberately carries no evidence, so
this archive is the audit trail if the merge later shows a regression. Prune (delete)
gate directories whose PR closed without merging. For a stack, re-check the next PR's
head, checks, bot signal, mergeability, evidence, and gate entry before merging it.

## Self-serve build-db verification

When a PR would merge except for its `build_db` gate — the line is missing, the evidence
files are absent, or the `build_db` head stamp trails the current `head` while every
other gate is current — run the verification yourself instead of routing a follow-up or
asking the user. It is a script run, not implementation work, so it does not violate the
no-code-edits rule. Scope guard: self-serve applies only when `build_db` is the SOLE
unmet gate. If the whole entry is stale (top-level `head` no longer matches the live
`headRefOid`, so the other gates were also verified elsewhere), that is the
"current-head gate mismatch" follow-up class — never repair it by bumping `head`
yourself, which would launder review/visual verdicts onto a head they never covered.

- Before launching, stamp the intent into `gate.json` (atomically, temp + rename): set
  the `build_db` line to `running; started <ISO-8601>; log /tmp/<slug>.log`. This is
  what makes an in-flight self-serve build survive a lost session — the byte-change is
  fingerprinted, and any later tick reading `running` with no live watcher process knows
  to harvest the log or relaunch, instead of the PR silently stalling.
- Fetch the PR head and create a throwaway worktree at that exact SHA
  (`git fetch origin <headRefOid> && git worktree add <scratch-dir> <headRefOid>`) —
  never switch branches in the main checkout.
- From that worktree, run `scripts/build_db_watch.py` (the `build-db` skill has the full
  operating guide, including the run-unattended rules) as a **backgrounded shell
  command** with an absolute `--input-dir <main-checkout>/reg_meta_build/input_data` —
  or the overlay root from the repo merge-gate rules when the PR changes tracked
  `reg_meta_build/input_data/**` — narrowing with `--providers` to the PR's affected
  providers, and `--dbdiff-against <main-baseline-db>` when the PR claims
  content-neutrality or a small inspected delta.
- Copy the watcher log and dbdiff output into the PR's gate directory, then update the
  `build_db` line (atomically) to `pass; head <sha built>; ...` naming the evidence
  files — you are a trusted local writer, but only for the gate you actually verified:
  never edit the other gates' lines or their head stamps. If the pipeline had left
  `status: blocked` solely on the missing build, you may flip it to `ready-to-merge`
  once your build passes and everything else is current-head. Remove the scratch
  worktree and DB, and merge on this or the next tick if all other automerge items still
  pass. A nonzero, unexplained dbdiff on a content-neutral claim is a finding: set
  `status: blocked` with the diff summary as `blocker` and route THAT to the pipeline.
- Guardrails: one build at a time; skip self-serve while another open pipeline is
  actively running build-affecting work (the existing lane guardrail), and let the tick
  end rather than blocking on the \~20-min build — the background task carries over.

Visual-gate gaps are NOT self-served: a missing `/reg-webapp-design-reviewer` result is
pipeline-owned work — route a follow-up.

If the merge creates a required build/release boundary, such as DB content that must be
published before dependent work can proceed, invoke `/release minor`. Let the release
skill resolve package scope and run its own gates; stop if it requests input or if the
required bump is not a minor release.

If a ready PR lacks a current-head Codex verdict, comment `@codex review` only when the
gate entry shows implementation is finished, then skip the merge until a later tick
observes a settled signal.

## Pipeline Follow-ups

When a PR is close to merge-ready but blocked by mechanical pipeline handoff work, route
that work back to the owning `/pr-pipeline` session instead of only reporting the block.

Send a pipeline follow-up when the PR is open, trusted, and blocked by a narrow
pipeline-owned item such as a missing or incomplete gate entry after finished work,
visual evidence absent from the gate directory, unchanged draft/ready state after
finished work, or a current-head gate mismatch after new pushes. Do NOT route a
follow-up for a missing/stale `build_db` gate — that is self-served (see Self-serve
build-db verification above).

Use thread tools when exposed: search/list by PR number, issue number, branch, worktree,
or recent title/history; send to the best matching existing pipeline thread; do not
create a new thread; if the match is ambiguous, report that and include the exact
message text to send.

The message must name the PR, issue, current head SHA, specific blocker, exact unblock
steps, required gate-directory evidence, and
`Do not merge; chief-of-staff owns merge execution.` Do not request implementation
changes unless the evidence proves false. Avoid repeat messages for the same PR head and
blocker unless the blocker/head changes or clear evidence shows the prior request was
missed after meaningful time has passed.

Make the follow-up prompt action-shaped and bounded:

```text
Chief-of-staff follow-up for PR #<pr> / issue #<issue>:

The PR is blocked only because <specific blocker>. Please fix the handoff evidence
without changing implementation code unless you discover the evidence is false.

Do this on current head <sha>:
1. <exact unblock step, e.g. copy the design-reviewer report + screenshots into the
   PR's merge-gate directory `merge-gates/pr-<pr>/` (`$XDG_STATE_HOME` root, default
   `~/.local/state/registry-research-toolkit/`), naming command, route, viewport set,
   inspected result, and head SHA>.
2. <update gate.json's gate line and head to match — atomically, and bump `updated`>.
3. Re-check PR #<pr>'s live head and confirm status/head/evidence still match.

Do not merge; chief-of-staff owns merge execution.
```

Do not use follow-ups to make product calls, alter scope, request broad refactors, or
ask a pipeline to bypass the merge gate. If the blocker is a real failed check, Codex
finding, merge conflict, or code defect, tell the pipeline to fix that specific failure;
if it is direction or priority ambiguity, ask the user instead.

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
- Bundle by shared file surface, semantic dependency, and review shape, not only by
  package name. Split into sequential PRs when one issue creates a contract or design
  base another issue should consume. Keep unrelated ready lanes separate even when they
  belong to the same epic.
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

Report checks that were not run. Do not claim a live check passed if it was skipped or
failed.

## Guardrails

- Do not fix pipeline-owned implementation or handoff evidence directly when a precise
  follow-up to the owning pipeline session can unblock it — except the `build_db` gate,
  which is self-served (see Self-serve build-db verification).
- Do not exceed the current active-work budget to keep agents busy.
- Do not invent feature routes after a merge (see the Dev Preview merged-PR link rules).
- Do not treat a clean hygiene script as proof that semantic relationships are current;
  read issue text when comments imply a blocker or dependency.
- Do not hand-edit generated `plan-sequence` / `plan-lanes` markers; go through
  `/issue-pulse`.
- Do not post status-consolidation comments on epic `#328`.

## Heartbeat Decision

- Use `DONT_NOTIFY` when there was no merge, no issue maintenance, no lane content or
  recommendation change, no new actionable blocker, and active PR statuses are
  materially unchanged.
- Use `NOTIFY` when the tick merged or released work, changed issue metadata, re-ranked
  or re-stamped lanes in a way that changes the free/active/recommended sets, found a
  gate failure on a PR that looked ready, failed to refresh the preview after a merge,
  failed to `--commit` the preflight candidate, or needs user input.
- For a heartbeat that lands after an idle probe, use `DONT_NOTIFY` with reason `idle`.
