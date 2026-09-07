# Yard project policy

Read this file together with the generated [Yard operator
skill](../.claude/skills/yard-operator/SKILL.md) before operating this project. These
project rules take precedence over the generic routine. Yard's current command help
remains authoritative about effects, guards and supported exits. Paths mentioned below
are relative to the repository root unless linked otherwise.

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
- **Use Yard for development and integration.** Manual work is limited to the special
  cases in [AGENTS.md](../AGENTS.md). Generic advice to apply a retained diff, commit
  and sync does not authorize a second writer to main. Replay work on its existing
  ticket through Yard; for a different ticket, carry the useful diff as briefing for its
  worker to integrate under review. The retired pre-Yard GitHub coordination machinery
  gets no new work.
- **Record dogfooding.** Keep observed Yard problems and papercuts in
  [DOGFOOD.md](DOGFOOD.md), including anything that wastes time or tokens.
- **File rendered frontend changes with `--workflow ui`.** This adds the source-only
  design seat and the rendered flow gates (`project-flows`, `catalog-flows`) with
  retained screenshots alongside the code seat. No other workflow binds both. Read
  `yard workflow list` or `.yard/config.toml` for the actual checks; do not assume all
  workflows run all gates. Other work uses `default` or `light` as appropriate.

## UI approval evidence

A candidate that changes rendered UI is approved on pictures you opened. On the `ui`
workflow, `yard lane show` prints the retained PNGs of both flow gates — `project-flows`
(the /project error and retry states) and `catalog-flows` (the catalog-authored draft).
Open them and match the `candidate HEAD` in those executions' logs to the full head you
will pass to `--expect-head`.

Judge the pictures and their route, state and viewport coverage under the [design review
skill](../.claude/skills/reg-webapp-design-reviewer/SKILL.md). The gate renders the
`/project` states it names, not every route a candidate touches. Render and inspect any
changed surface it missed before approving. A passing design seat is source evidence
only. Retained gate images, the worker's self-check and the operator's own verification
have distinct coverage; report what each actually establishes.

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
if the problem persists.

Keep raw logs, verification reports and submission receipts in the already ignored
`archive/reports/yard/`. Commit concise outcomes in DOGFOOD.md; label references to
local archived evidence as local-only paths.

## Temporary corrections for Yard 0.14.1

These qualify wording in the shipped routine. Recheck them at each Yard upgrade and
remove each correction once upstream covers it accurately.

- **Spending depends on the state and command.** Abandoning starts no model by itself;
  the resulting admission of an unparked ready ticket can start a fresh attempt.
  Unparking or accepting a proposal need not admit work if it remains blocked. Nudges
  and rejections can buy additional rounds. Use the guarded exit and read the command's
  reported effect rather than treating every decision as a fresh model session.
- **Acceptance runs the proposal's recorded command.** It may create a ticket or edit
  one. `--parked` applies only to ticket creation and refuses commands creating none.
- **Skip parking an already parked ticket during retirement.** The command refuses
  redundant parking; proceed to the applicable stop, abandon and done steps.
- **Replay is not limited to infrastructure incidents.** Eligible retained work can
  replay on the same ticket when it still fits. Read `yard lane replay --help` for the
  current guards; an unchanged premise is not a separate command requirement.

## Updating the upstream routine

Keep `.claude/skills/yard-operator/SKILL.md` exactly as the installed Yard generates it,
including its stamp and formatting. `.agents/skills/yard-operator` remains a relative
symlink to that directory. Put project policy and temporary corrections here, and keep
the existing specialized design skills separate.

Use `yard project show` to check provenance and template equality. `yard init` offers
the update patch; read its help and effects first, because it can start the daemon and
admit ready tickets. Review the patch before applying it, then recheck this policy and
verify `matchesTemplate=true`. The generated skill is excluded from Panache so normal
formatting preserves those bytes. This file remains formatted and linted normally.
