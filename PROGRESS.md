# SCB variable-slug curation — overnight progress

Tracking issue #471 (curation) + epic #209 (slug freeze) + #732 (generator rules).
Branch: `feat/scb-slug-curation` off `main`. Scratch I/O: `/tmp/scb-cur/`.

**This file is the durable checkpoint.** On restart: read it, resume from the first
unfinished phase. Every stage is idempotent (skip batches whose output already exists).

## Deliverable

1. Curated `reg_meta_build/fqid_slugs/scb.toml` `[variable.*]` slug overrides (+ 4 SOS).
2. Generator rule structure mined from the curation: implemented in `fqid_slugs.py` +
   documented in `reg_meta_build/DESIGN.md`, with tests, reproducing a slice
   mechanically.
3. Ready-to-merge draft PR (`Closes #471`, refs #732/#209). **Do not merge** — await
   sign-off.

Fallback if generator work is unstable: ship pins-only + rule structure as a documented
spec for a follow-up #732 PR. Never end with nothing mergeable.

## Phases

- [ ] **P1 — Prep**: branch + scratch dirs ✅; baseline build (`/tmp/scb-cur/db`) —
  RUNNING; precheck → `/tmp/scb-cur/precheck.json`; export per-register batches.
  Baseline build is the pre-rule dbdiff baseline — keep it.
  - batch count: *TBD*
  - worklist size (scb name-fallback): *TBD* (expected \~11,802; \~half trivial keeps)
- [ ] **P2 — Pilot**: 2 representative registers; self-evaluate vs rubric; iterate
  prompt/ validation until it passes. Verdict: *TBD*
- [ ] **P3 — Full fan-out** (Workflow, resumable): one agent/batch (sonnet) →
  `proposals/<r>.json`. Validate (code). Round 2 escalation → `round2/`. Commit
  merged proposals as a branch artifact.
  - Workflow runId(s): *TBD*
- [ ] **P4 — Reconcile + generator rules** (#732): mine (auto→final) pairs → rule
  structure; implement safe rules in `fqid_slugs.py` (+ tests + DESIGN.md); per-rule
  dbdiff guard (must move NO clean slug); reconcile → emit `scb.toml`/SOS overrides
  only where improved generator ≠ final_slug; rebuild → dbdiff + precheck confirm
  worklist shrank.
- [ ] **P5 — Gates → ready-to-merge**: ruff/ty/pytest/panache; real-seed build+dbdiff
  (attach); `/code-review` high; mark ready; Codex window; stale-head check; final
  summary. STOP.

## Key numbers

- (filled in as phases complete)

## Decisions / blockers log

- (record any blocker + the safe path taken)

## Workflow runIds

- (recorded per fan-out)
