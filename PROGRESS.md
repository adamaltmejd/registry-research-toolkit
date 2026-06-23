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

- [x] **P1 — Prep**: branch + scratch dirs ✅; baseline build at
  `/tmp/scb-cur/db/reg_meta.db` (validated OK) — this is the pre-rule dbdiff
  baseline, keep it; precheck → `/tmp/scb-cur/precheck.json`; 264 batch files in
  `/tmp/scb-cur/batches/`, 0 misses.
  - batch count: **264** (194 registers; 22 chunked >120)
  - worklist size: **11,806** (scb 11,802 + sos 4). Derivations: drift-name 5,017,
    name-fallback 5,944, name-fallback+disambiguated 675, fold+disambiguated 151,
    drift-earliest-column+disambiguated 15, v-provider-key 0. >40-char: 4,083.
  - draft PR: #746 (in-flight claim).
  - tooling (scratch): `export_batches.py`, `validate_proposals.py`, `wf_curate.js`.
- [x] **P2 — Pilot**: scb-165 (verbose financial, 109) + scb-55 (Yrkesregistret, 45).
  **PASS** after 1 iteration. Verdict: 0 grammar/reserved/period/uniqueness
  violations; conservative keeps correct (`anstand`, `kommentar-1..4` ordinals
  kept); mid register 95% of renames ≤24 (median 17); verbose financial register
  stays ≤40 (inherent — forcing ≤24 there needs cryptic contraction). Iteration:
  pilot v1 coined cryptic contractions (`yrkstall`, `syreg`, `peorgnr`) → added an
  **ABBREVIATION RULE** (reach length by dropping whole words, never opaque
  contraction) + **FAMILY CONSISTENCY** rule to the convention; v2 fixed
  yrkstall/syreg. Residual coinages/inconsistencies (`sun2000inr`, `sv` vs
  `svenska`) now caught by a **coined-token detector** added to the validator →
  routed to round 2. Round-2 trigger = hard ∪ violations ∪ coined ∪ rename>32 chars.
- \[\~\] **P3 — Full fan-out** (Workflow, resumable): one sonnet agent/batch →
  `proposals/<r>.json` (agent does its own Read/Write — workflow sandbox has no FS).
  Idempotent: agent skips if a valid output already exists, so a re-invoke resumes. Then
  validate (code) → round-2 escalation (hard ∪ violations ∪ coined ∪ >32) → `round2/`.
  - Workflow runId(s): full fan-out `wf_d5d01942-671` (264 batches; 2 pilot skip). Pilot
    runs: `wf_c2ac01f8-c81` (v1), `wf_dba9389c-0dc` (v2).
  - On restart: re-invoke `Workflow({scriptPath:"/tmp/scb-cur/wf_curate.js"})` — done
    batches skip; then `python3 /tmp/scb-cur/validate_proposals.py`.
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
