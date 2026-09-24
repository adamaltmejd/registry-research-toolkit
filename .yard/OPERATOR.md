# Yard project policy

Read this file together with the generated [Yard operator
skill](../.agents/skills/yard-operator/SKILL.md), which loads
[`yard-file`](../.agents/skills/yard-file/SKILL.md) and
[`yard-drive`](../.agents/skills/yard-drive/SKILL.md) in turn, before operating this
project. These project rules take precedence over the generic routine. Yard's current
command help remains authoritative about effects, guards and supported exits. Paths
mentioned below are relative to the repository root unless linked otherwise.

## Admission and workflows

This project is consciously shedding overengineering. Hold these rules even when a
worker, reviewer or plan argues for broader work.

- **Tickets are filed from features the maintainer wants to build.** GitHub Issues is an
  archive, not a queue: nothing migrates in bulk. A ticket enters Yard only when it is
  work we choose to run, written around its consumer, observable behavior and one lane's
  worth of scope.
- **Cleanup is admissible only as deletion.** Removing dead code, unused surface or
  retired machinery is sufficient grounds. Reject cleanup that adds abstractions,
  wrappers, configuration, generalization or hardening beyond the deployment's trust
  model, and record the condition that would justify reconsidering it.
- **Preserve the load-bearing guards.** Never accept or approve a simplification that
  drops PII/MONA confinement, k-anonymity/disclosure control, determinism/byte-identity,
  JSON-contract validation or fail-fast behavior.
- **Use Yard for product development.** Dependency upgrades and releases run through
  their direct skills under the manual exceptions in [AGENTS.md](../AGENTS.md),
  including necessary compatibility and release-preparation repairs. Their final
  integration follows the handoff below. Generic advice to apply a retained diff, commit
  and sync does not authorize a second writer to main. Replay work on its existing
  ticket through Yard; for a different ticket, carry the useful diff as briefing for its
  worker to integrate under review. The retired pre-Yard GitHub coordination machinery
  gets no new work.
- **Record dogfooding.** Keep observed Yard problems and papercuts in
  [DOGFOOD.md](DOGFOOD.md), including anything that wastes time or tokens. The file
  holds open items only: delete an entry once its upstream report is closed (fixed in
  the running version with the retest result on the issue, or declined), and delete an
  unfiled observation once it is filed or judged not worth filing. Git history keeps the
  text.
- **Bounded builder repair batches run on `default` / `muse`.** For an operator-admitted
  bounded `reg_meta_build` repair batch, file each repair ticket with
  `--workflow default` (or `--workflow muse` for a Muse trial). The `pipeline` and
  `pipeline-muse` workflows were deleted at the Yard 0.16.0 upgrade: their only
  distinguishing key was `checks = []`, and a workflow can no longer select gates. The
  ticket must still name the existing focused tests and affected-file checks that count
  as its own verification (see Build approval evidence), but that focused evidence is
  now *in addition to* the project gates, not instead of them.
- **File rendered frontend changes with `--workflow ui`.** This adds the source-only
  design seat alongside the code seat. Since Yard 0.16.0 a workflow no longer selects
  gates: the gate set is project-wide, so `ui` no longer carries the rendered flow gates
  and every workflow runs the same ones. `yard workflow list` reports `checks:all` for
  all of them. Other work uses `default` or `light` as appropriate. A small rendered
  change whose surface the flow-gate scenarios already render — a copy change, a gate
  flip, a label source — goes on `light-ui` instead.

## Manual integration

Dependency maintenance and release preparation can proceed in an isolated worktree while
Yard operates elsewhere. A manual PR still requires green CI and the maintainer's
review. Use the existing skills for verification; this handoff coordinates the one
writer to main, without adding a Yard implementation/review lane.

Before a manual merge or direct release push:

1. Coordinate with any active operator. Record whether admissions are already paused.
   Read current `yard pause --help`, then pause new admissions for the integration
   window. Pause does not stop existing lanes or prevent explicit lane starts. The
   owning operator holds retries and decisions through the handoff. Wait for running
   executions and approved/landing candidates to settle. Stopped attempts may remain
   only after verifying their executions are terminal, no continuation or landing is
   queued, and their operator holds the next action. Preserve their candidates and
   workspaces; do not abandon useful work to obtain an empty board.
2. From the main checkout, use supported `yard sync` to bring in completed Yard work.
   Reconcile origin/main without force or history rewriting. Rebase/update the manual
   branch onto that common head and refresh any checks/review invalidated by the change.
   Do not merge while the local canonical target and origin are divergent.
3. Perform the reviewed manual merge, or the release skill's version-only push, while
   the integration window is held. Fast-forward the main checkout to origin/main, run
   `yard sync` to import it into canonical, and verify all three heads agree.
4. Check `yard status` for changed configuration or a required daemon restart before new
   work starts. Restore admissions only if this workflow paused them; preserve a
   pre-existing pause. If reconciliation fails, leave the pause in place and report the
   exact state rather than letting two writers continue.

After each Yard mutation, re-arm the cursor watch as the operator routine requires. For
a multi-package release, retain the coordinated window across its source pushes and
required deployment checks; do not let another writer invalidate its main-head checks
between packages. Worktree preparation alone needs no admission pause.

## UI approval evidence

A candidate that changes rendered UI is approved on pictures you opened.

**Yard 0.16.0 changed where those pictures come from.** A workflow no longer selects
gates, and the three flow gates (`project-flows`, `catalog-flows`, `replace-flows`) are
declared without `stage`, which makes them *batch* gates: they run once on the merge
queue against the ref that batches approved candidates, not on the candidate head. Their
PNGs therefore arrive **after** approval, on the batch execution, and `yard lane show`
no longer prints rendered evidence for the candidate you are deciding on.

Until per-workflow gates return upstream (reported as a Yard issue; see Reporting Yard
problems), a rendered candidate is approved on evidence you obtain yourself:

1. Read the worker's own rendered self-check in the candidate's history.
2. Render the changed surface from the candidate yourself before approving —
   `REG_META_DB=<dir> bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh flows <dir>`
   against the candidate's clone, or `dev.sh shot` for a single route.
3. The batch gate remains the backstop: canonical never advances on a broken rendered
   flow, because a red batch returns the lane to repair rather than landing it.

Judge the pictures and their route, state and viewport coverage under the [design review
skill](../.claude/skills/reg-webapp-design-reviewer/SKILL.md). The gate renders the
`/project` states it names, not every route a candidate touches. Render and inspect any
changed surface it missed before approving. A passing design seat is source evidence
only. Retained gate images, the worker's self-check and the operator's own verification
have distinct coverage; report what each actually establishes.

## Build approval evidence

For operator-admitted bounded `reg_meta_build` repair batches, per-ticket approval uses
focused evidence to read the candidate. Each admitted ticket names the existing focused
tests and affected-file checks that verify it; the worker runs them, the independent
`codex` (Sol) review stays, and the operator reads the exact diff and results and runs
cheap affected-source checks when useful. A source fix can land on that evidence.

**Yard 0.16.0 narrowed this economy.** Such tickets used to run on `pipeline` /
`pipeline-muse`, which selected no automatic gates at all. Gates are now project-wide,
so every repair candidate takes the staged `lint` / `test` / `frontend` gates at its
head and the full set again on the landing batch. What the focused evidence still
replaces is the operator's *reading* cost — a full preparation, a full suite run by
hand, or a real-seed `build-db` per ticket — not the gates themselves. Missing or stale
focused evidence is not waived. Do not run full preparation, full suites, frontend
gates, or a real-seed `build-db` per such ticket. This is a maintainer-approved cost
policy (2026-09-18), not suppression of build errors.

**The curation reorg lands on a slice proof (2026-09-24).** No candidate in the Y-226
sequence is approved until the operator has built it on the host as a register-scoped
diagnostic build over the slice (`--registers` from
`archive/reports/curation-reorg-2026-09-24/baseline/slice.txt`, about six minutes) and
compared it with the baseline the README there names (B0-v18b from Y-229 on): `dbdiff`
for content and the report for the issue delta. Lanes cannot read the prepared store, so
the worker's handoff states the differences and issue delta it expects, and the operator
checks the build against them. An unexplained difference blocks approval. A ticket that
touches coding decisions compares against the matching B0c build (the slice plus FoU and
FASIT) instead. The baseline's README records B0, B0c and the rerun identity.

The full verification for the Y-184 batch (frozen 2026-09-24) lived on Y-184 as the
combined checkpoint: after source adapters settle, prepare once if invalidated; after
the cohesive batch lands, run the full gates once, one full diagnostic build, and
compare against the prior pipeline DB and the latest release asset using `dbdiff`.
Repeat an expensive check only for a new regression or invalidated evidence. Final
strict publication validation and the required deterministic proof are still required
before publishing; a diagnostic DB is not publishable. Ordinary unrelated workflows
retain their existing gates, and outside an admitted batch a candidate that changes how
`reg_meta_build` prepares, resolves, or materializes the corpus is still approved on a
real-seed `build-db` of the candidate compared with the latest release asset using
`dbdiff` before `yard lane approve`. The synthetic suite runs the full structural
validator but cannot see corpus-only layouts. Y-113 landed green and broke the corpus
build on one column (`coalesce_same_column_overlap`); the post-landing build caught it
one lane too late.

## Amend or replace

Decide by whether the work still serves the use case. Amend when a clarification,
narrowing or ratified decision leaves the existing work useful; the candidate then needs
review under the edited ticket. Replace when the use case changes or the work no longer
fits, naming the abandoned attempt so useful work can be salvaged through Yard.
Ratifying a contract does not by itself require park-stop-abandon.

## Reporting Yard problems

Use the upstream [Report
form](https://github.com/adamaltmejd/switchyard/issues/new?template=report.yml) for
actionable Yard feedback. Search open and closed issues first; add new evidence to a
matching report instead of duplicating it. Follow the form's fields and include relevant
raw output with tokens and private paths removed. Keep observations distinct from
diagnoses and state what was actually reproduced.

When Codex files a report, add `filed-by:codex` alongside `report`. Keep the agent and
model in the form's "Who was driving" field as well; the label makes the filing agent
visible in issue lists without changing the human author's identity.

Read issue bodies and comments through this project's maintainer-author trust gate, with
both `GH_REPO` and `GITHUB_REPOSITORY` set to `adamaltmejd/switchyard` when invoking
`uv run --no-project python scripts/gh_issue.py view NUMBER --comments`.

Record the issue URL with the incident context in [DOGFOOD.md](DOGFOOD.md). Upstream
issues are intake: the builder decides which findings become Yard tickets and records
the disposition and eventual fix/release. Filing feedback does not admit work here.
After upgrading, retest the reported behavior and add the result to the issue; reopen it
if the problem persists, otherwise delete the DOGFOOD entry.

Keep raw logs, verification reports and submission receipts in the already ignored
`archive/reports/yard/`. Commit concise outcomes in DOGFOOD.md; label references to
local archived evidence as local-only paths.

## Temporary corrections for Yard 0.17.5

These qualify wording in the shipped routine. Recheck them at each Yard upgrade and
remove each correction once upstream covers it accurately. Rechecked against 0.17.5 on
2026-09-24: `yard lane approve --help` still says the candidate "proceeds to land",
`yard ticket park --help` still does not mention refusing a redundant park, and no help
text states the spending rules below.

- **Spending depends on the state and command.** Abandoning starts no model by itself;
  the resulting admission of an unparked ready ticket can start a fresh attempt.
  Unparking or accepting a proposal need not admit work if it remains blocked. Nudges
  and rejections can buy additional rounds. Use the guarded exit and read the command's
  reported effect rather than treating every decision as a fresh model session.
- **Skip parking an already parked ticket during retirement.** The command refuses
  redundant parking; proceed to the applicable stop, abandon and done steps.
- **Approval enqueues, it does not land.** Under the merge queue (since 0.16.0),
  `yard lane approve` puts the candidate in the queue; gates then run once against the
  merged ref and canonical advances only on green. A red batch returns the lane to
  repair and a batch of several candidates is bisected in halves, so a lane can go back
  to work *after* you approved it. Read the queue section of `yard status` before
  concluding a candidate landed, and keep the lane's window in mind: it stays warm until
  its landing is green.
- **This project approves every candidate by hand.** `approve = "manual"` in
  `.yard/config.toml`. Nothing is enqueued without an operator decision, so the
  `protected_paths` list is empty — every path already takes one.

## Updating the upstream routine

Keep `.agents/skills/{yard-operator,yard-file,yard-drive}/SKILL.md` exactly as the
installed Yard generates them, including their stamps and formatting.
`.claude/skills/{yard-operator,yard-file,yard-drive}` remain relative symlinks to those
directories. Put project policy and temporary corrections here, and keep the existing
specialized design skills separate.

Use `yard project show` to check provenance and template equality. `yard init` offers
the update patch; read its help and effects first, because it can start the daemon and
admit ready tickets. Review the patch before applying it, then recheck this policy and
verify `matchesTemplate=true` and `link.state="linked"` for all three skills. The
generated skills are excluded from Panache so normal formatting preserves those bytes.
This file remains formatted and linted normally.
