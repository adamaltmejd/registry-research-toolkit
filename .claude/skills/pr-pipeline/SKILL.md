---
name: pr-pipeline
description: >-
  Drive a feature, fix, or request from intake to merge-gate handoff: invoke the
  `pr-pipeline-impl` skill for the whole implementation phase (plan → claim → implement
  → test / review / docs / visual → write each PR's `gate.json` with `codex_bot`
  deferred), then complete the `codex_bot` gate — inline on the claude surface — and
  record the current-head gate evidence for chief-of-staff automerge. The invoking
  session is the lead and owns git until handoff. Usage: /pr-pipeline <issue number(s),
  a feature/problem description, or `next` to carve a fresh lane from the sequencing
  projection>
argument-hint: "<issue number(s), a description, or `next` for a fresh lane>"
disable-model-invocation: true
---

# PR pipeline (lead — orchestrator)

**Only run when the user explicitly invokes `/pr-pipeline` (or clearly asks you to run
this pipeline).** It opens PRs and records merge-gate evidence, but it does **not**
merge — never auto-start it because a conversation merely resembles issue work.

You are the **lead** for this request:

> $ARGUMENTS

This skill is a thin orchestrator over two phases:

1. **Implementation phase** — invoke the **`pr-pipeline-impl`** skill (the `Skill`
   tool). It owns the whole build: plan the work into PRs, claim the lane (pipeline slot +
   draft PRs), implement, run the test / review / docs / visual gates, and write each
   PR's `gate.json` into the local merge-gate store with the full expected gate set
   present, the `codex_bot` line **deferred**, and `status: blocked`
   (`blocker: codex_bot`). It dispatches the one-shot role subagents and owns git during
   the build. Pass it the same `$ARGUMENTS` (issue numbers, a description, or `next`).
   It returns a handoff report: per PR, the gate.json it wrote (codex_bot deferred, or
   blocked on a different named gate), intended merge order, and any follow-ups.

2. **codex_bot completion + flip** — after `pr-pipeline-impl` returns, complete the one
   gate it deferred and flip the PR's status (see next section). Then run the Closeout.

You own ALL git throughout (the impl skill acts as the same lead session — it stages,
commits, pushes, opens PRs). Do **not** merge — leave merge execution to
`/chief-of-staff`.

## Complete the codex_bot gate (claude surface, inline)

`pr-pipeline-impl` finished with each PR's `gate.json` written, the `codex_bot` line
deferred, and `status: blocked` (`blocker: codex_bot`). On the **claude surface** you
complete that gate INLINE — a Claude agent shelling to `codex review` applies codex's
first (only) seatbelt, so nesting never occurs.

Run the local Codex review exactly per the **`CLAUDE.md` "Local Codex review"** bullet
(already in your context) — the launcher command, `--base` rule, background-launch (its
30-min ceiling outlasts the 10-min foreground Bash cap), exit-code semantics (`0` clean
· `1` findings · `2` error with `error.kind`; only `usage_limit` recordable), the
findings → fix-loop → re-run-until-`clean` protocol, and the nested-`sandbox-exec`
hazard (re-run with `dangerouslyDisableSandbox: true`; `nested_sandbox` = wrong
environment, not a PR block) are the canonical contract; do not restate them, follow
them. Route `findings` into the fix loop like `/code-review` findings (fix via a fresh
implementer, or dismiss with a reason). A fix push MOVES HEAD, staling any head-bound
gate (build_db / visual) the impl phase recorded — re-run each that applies on the new
head and refresh its `gate.json` line before flipping.

The impl-phase delta this orchestrator adds — **on `clean`, UPDATE the impl-written
`gate.json` (do NOT write a fresh one — the impl phase already wrote the full gate
set):** re-read the PR's `gate.json`, set the `codex_bot` line to the head-bound clean
form `local; codex_local_review; head <sha>; clean; see codex-review.md in this dir`
(the usage-limit form replaces `clean` with `exhausted (usage-limit)`, same head stamp +
evidence pointer — the only two legal verdict tokens), then, having confirmed the HEAD
hasn't moved since and `codex_bot` is the sole unmet gate (every other gate line met,
all head-bound gates on the current head), flip `status` to `ready-to-merge` and clear
`blocker` to `null`. Write it atomically (temp+rename) per the `CLAUDE.md` gate-store
contract, bumping `updated`. If any other gate is actually unmet, or a head-bound gate
is stale, leave `status: blocked` naming that item instead — do not flip on a clean
codex_bot alone.

Do not merge. `/chief-of-staff` performs the squash merge (per the `CLAUDE.md` merge
gate) after re-checking live head, CI, the head-bound `codex_bot` line, mergeability,
gate evidence, and stack order. If a remote branch should be deleted after merge, leave
that to the merge owner.

## Closeout

Before reporting, **re-verify the work is actually finished** — don't take the impl
phase's handoff on trust:

- **Ready for chief-of-staff** — each planned PR is open and non-draft, with a
  current-head `gate.json` in the local merge-gate store marked `status: ready-to-merge`
  (evidence files present in its directory), or `status: blocked` with `blocker` naming
  the missing item.
- **Docs current** — RE-VERIFY the impl phase's reported cross-change docs sweep (its
  handoff claims it left no authored doc stale; don't trust that on faith). Confirm no
  authored doc is still stale anywhere: the touched `<package>/DESIGN.md` (including its
  design-spec prose and any token/symbol it names), README / CLI help, docstrings,
  `CLAUDE.md`/`AGENTS.md`, `ARCHITECTURE.md`. The impl phase already ran Step D (per-PR
  drift) and its own whole-change sweep; this is the orchestrator's independent re-check
  that they actually held — especially a cross-PR rename or a new contract no single
  PR's docs-updater owned. Fix any residual drift inline — it's part of this PR, and a
  one-line doc fix in a file you already touched is never a follow-up. Record a
  follow-up ONLY when the fix needs its own scoped change (its own diff, review, or
  decision).
- **Nothing half-done** — every review finding was fixed or dismissed-with-reason, no
  role was silently skipped, no scope was quietly cut.

Then end with a **report**:

1. **What is ready** — the PR breakdown planned; per PR → ready for chief-of-staff /
   blocked, intended merge order, review rounds, external/bot comments addressed, tester
   suggestions accepted/declined, roles skipped (and why), and any fork you escalated to
   the human.
2. **Deferred / outstanding** — anything intentionally left out of scope, a finding
   dismissed as "later", a confirmed TODO/FIXME, or a doc left stale by design. Say
   "none" if there are none.
3. **Recommended new issues** — the follow-ups `pr-pipeline-impl` persisted to the
   lane's final-PR `followups.md` (chief-of-staff files them at merge via
   `/file-issue`). In an **interactive** session, list them and offer to file the ones
   the human picks immediately via `/file-issue`. Say "none" if the change is fully
   self-contained.
