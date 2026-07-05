---
name: pr-pipeline
description: >-
  Registry Research Toolkit PR development pipeline (thin orchestrator). Use when asked
  to run the PR pipeline workflow, including prompts like "$pr-pipeline issue 510":
  invoke the `pr-pipeline-impl` skill for the whole implementation phase (develop the
  lane, open draft PRs with closing keywords, run review/test/docs/visual gates, and
  write each PR's `gate.json` with `codex_bot` deferred), then — on this codex surface —
  leave `codex_bot` to the sibling lane-runner and record the handoff. Marks PRs ready
  and records current-head merge-gate evidence for chief-of-staff automerge; never
  merges.
---

# Registry PR Pipeline (orchestrator)

Only run when the user explicitly invokes this skill (or clearly asks you to run this
pipeline). It opens PRs and records merge-gate evidence, but it does not merge — never
auto-start it because a conversation merely resembles issue work.

This skill is a thin orchestrator over two phases:

1. **Implementation phase** — invoke the **`pr-pipeline-impl`** skill (by that exact
   name). It owns the whole build: plan the work into PRs, claim the lane (pipeline slot +
   draft PRs), implement directly, run the test / review / docs / visual gates, and
   write each PR's `gate.json` into the local merge-gate store with the full expected
   gate set present, the `codex_bot` line **deferred**, and `status: blocked`
   (`blocker: codex_bot`). Pass it the same target (issue numbers, a description, or
   `next`). It returns a handoff report: per PR, the gate.json it wrote (codex_bot
   deferred, or blocked on a different named gate), intended merge order, and any
   follow-ups.
2. **codex_bot ownership + closeout** — on this codex surface, `codex_bot` is already
   deferred by the impl phase and you cannot complete it (see next section). Confirm the
   handoff and run the Closeout.

You implement directly through the impl phase (same session) and own git during the
build. Do not merge, and do not mark ready-to-merge — the sibling lane-runner completes
`codex_bot` and flips status; the `chief-of-staff` skill owns merge execution.

## codex_bot is owned by the sibling lane-runner (this surface)

`pr-pipeline-impl` finished with each PR's `gate.json` written, the `codex_bot` line
deferred, and `status: blocked` (`blocker: codex_bot`). On this **codex surface** that
is the terminal state — do NOT attempt the `codex_bot` gate yourself. You are already
inside a codex seatbelt, so your own `codex review` would be a NESTED sandbox: Seatbelt
cannot nest a second profile no matter the permission (`sandbox_apply` denies EPERM even
under escalated grants; see `cos_lane_runner.py`'s docstring), so escalating your own
permissions cannot fix it.

Leave the `codex_bot` line deferred (e.g. `running; deferred-to-lane-runner`) and
`status: blocked` with `blocker: codex_bot` exactly as the impl phase wrote it.
`cos_dispatch` launches the deterministic `cos_lane_runner.py` by default for codex
lanes — a sibling process OUTSIDE your seatbelt — which runs
`scripts/codex_local_review.py` un-nested, drives the fix loop by resuming your warm
session with a findings brief, and completes the `codex_bot` line (flipping `status` to
`ready-to-merge` once codex_bot is the sole unmet gate) after you exit.
`--no-lane-runner` is the escape to the legacy self-serve path a human then runs (see
the chief-of-staff skill). The chief-of-staff then sees a single finished handoff.

Do not merge. `chief-of-staff` performs the squash merge after re-checking live head,
CI, the gate entry's head-bound `codex_bot` line, mergeability, gate evidence, and stack
order.

## Closeout

Report what the impl phase changed, PR number/status, verification commands, review
findings fixed or dismissed, docs/test decisions, and the merge-gate entry status
(`codex_bot` deferred to the lane-runner, `status: blocked` pending its completion, or
blocked on a different named gate). For multi-PR pipelines, report the intended merge
order, but leave codex_bot completion and merge execution to the lane-runner /
`chief-of-staff`.

Default to fixing doc drift inline — it's part of this PR; record a follow-up only when
the fix needs its own scoped change, never as an escape hatch for a one-liner. The
follow-ups `pr-pipeline-impl` persisted to the lane's final-PR `followups.md` are filed
by chief-of-staff at merge via the `file-issue` skill. In an **interactive** session,
additionally list them and offer to file the ones the human picks immediately via
`file-issue`. Say "none" when the change is fully self-contained.
