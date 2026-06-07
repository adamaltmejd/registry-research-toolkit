---
name: pr-pipeline
description: "Drive a feature, fix, or request from intake to merge through the agent-team pipeline: plan the work into one or more PRs, then for each run implementer → simplifier → tester → reviewer loop → external-review hold → docs-updater → merge. The invoking session is the team lead. Usage: /pr-pipeline <issue number(s), or a feature/problem description>"
argument-hint: "<issue number(s) or a feature/problem description>"
disable-model-invocation: true
---

# PR pipeline (team lead)

**Only run when the user explicitly invokes `/pr-pipeline` (or clearly asks you to
run this pipeline).** This orchestrator spawns an agent team, opens PRs, and MERGES
them — never auto-start it because a conversation merely resembles issue work.

You are the **team lead** for this request:

> $ARGUMENTS

You do NOT write code, tests, or docs yourself — you plan the work, dispatch
teammates, and you are the only one who merges. Teammates report to you via
`SendMessage`; you route work between them. The five role teammates are defined in
`.claude/agents/`: `implementer`, `simplifier`, `tester`, `reviewer`, `docs-updater`
(agent teams must be enabled — if a teammate dispatch errors, stop and tell the user
to enable `CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS`).

## Step 0 — understand the request and PLAN the work (FIRST, before any coding)

The request may be one or several GitHub issues, a freeform feature/problem
description, or a mix. Plan before building:

1. **Gather context.** Read any referenced issue(s) **including comments** (agreed
   decisions are recorded there); read the relevant code, `CLAUDE.md`, and
   `<package>/DESIGN.md` to understand intent and constraints.
2. **Decide the shape — one PR or several.** Break the request into the smallest set
   of coherent, independently reviewable/mergeable PRs. Write a one-line scope per PR.
3. **Decide the order.** Sequence the PRs by dependency (which must land before
   which); note which are independent. You execute them SERIALLY in this session —
   one fully merged before the next starts. (For parallelism the human launches a
   separate lead per independent chain; not your concern here.)
4. **Settle decisions/forks up front.** If the request or an issue has an open fork
   (naming, schema/column choice, scope judgment) not already decided on the issue,
   resolve it now with `AskUserQuestion`. Only you can ask the human; teammates can't.
5. **Confirm if non-trivial.** For a multi-PR or ambiguous request, send the human
   your plan (PR breakdown + order) for a quick confirm before building. For a clear
   single-PR request, just proceed.

Then run the per-PR pipeline below for each planned PR, in order.

## Per-PR pipeline (repeat for each planned PR, in dependency order)

### A. Implement, then open a DRAFT PR

- Create and check out a branch for this PR (`s/<slug>`); branch creation is your
  job, not the implementer's.
- Dispatch **implementer** with this PR's scope/plan and its Verify commands. For
  build-affecting work (SCB/SOS triage, slugs, DDL) that includes the real build
  `reg-meta-build build-db --input-dir reg_meta_build/input_data --providers scb,sos`
  (validates by default; the local `input_data` is read-only). It implements,
  verifies, commits, pushes, and reports the branch + summary — it does NOT open the PR.
- Once it has pushed, YOU open the PR as a **draft** (body: what the change does and
  why; name any issue it closes). Draft keeps external review bots off the raw
  implementation until it's near-final.

### B. Simplify + test-suggest, then mark ready

1. Dispatch **simplifier** → applies behaviour-preserving cleanups and pushes (or
   reports "nothing found").
2. Dispatch **tester** → returns a prioritized `must`/`nice` suggestion list. YOU
   decide which to accept; send accepted ones to **implementer** to add; it
   re-verifies and pushes.

Then mark the PR **ready for review** — now external auto-review (Codex/Copilot) and
CI-on-ready fire once, on near-final code.

### C. Review loop (iterate to convergence)

1. Dispatch **reviewer** on HEAD → findings tagged blocking / non-blocking / question,
   each with `file:line`.
2. Route blocking findings (and questions you resolve) to **implementer** → fix,
   re-verify, push.
3. Ask **reviewer** to re-review (same persistent teammate; it raises only NEW
   findings or confirms resolution).
4. Repeat 2–3 until the reviewer emits its exact stop token: **"converged — no further
   findings."**
5. Safety valve: if it won't converge after a few rounds or keeps re-raising the same
   point, STOP and surface the blocker via `AskUserQuestion`; never loop forever.

### D. Docs

Dispatch **docs-updater** on the final code → it fixes doc drift (DESIGN.md / README /
docstrings) and pushes, or reports "no doc update needed" (it re-runs the package
Verify if it touched `.py`).

### E. Merge gate (lead only) — with external auto-review hold

Merge only when ALL hold:

- the reviewer loop CONVERGED (no blocking findings),
- Verify is green (incl. the real `build-db` for build work),
- CI on the PR is green,
- **external auto-review hold:** after the PR went ready (and after your most recent
  push), wait **~10 minutes** for the external reviewers (Codex / Copilot) to post.
  Read every new review comment; route any **material** finding back through the
  implementer (then re-review, and **reset the 10-minute timer** from the new push).
  Merge only once a full ~10-minute window elapses with **no new material review
  comments**. Dismiss non-material/incorrect comments with a one-line reason — never
  merge over an unanswered material comment.

Then merge and delete the branch. Never merge on a red review, red Verify, red CI, or
an open material external comment. The implementer never merges — only you. Before
starting the next planned PR, re-sync local `main` to the just-merged base
(`git checkout main && git fetch origin main && git reset --hard origin/main`) so PR
N+1 forks off the merged code, not stale local state or the prior PR's history — then
loop back to Step 1 for the next PR.

## Conventions you enforce on dispatch

- Pre-v1: no migration/compat/dead-code retention; fail fast; deterministic with
  explicit seed/config; validate JSON contracts at boundaries; never leak row-level
  content. `uv` for Python, `bun`/`bunx` for frontend. Never bypass git hooks.

## Final report

End with: the PR breakdown you planned; per PR → merged / blocked, review rounds, any
external comments addressed, tester suggestions accepted/declined, and any fork you
escalated to the human.
