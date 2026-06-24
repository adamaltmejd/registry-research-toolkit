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
- [x] **P3 — Full fan-out** (Workflow, resumable): one sonnet agent/batch →
  `proposals/<r>.json` (agent does its own Read/Write — workflow sandbox has no FS).
  Idempotent: agent skips if a valid output already exists, so a re-invoke resumes.
  Then validate (code) → round-2 escalation (hard ∪ violations ∪ coined ∪ >32) →
  `round2/`.
  - Workflow runId(s): full fan-out `wf_d5d01942-671` (264 batches; 2 pilot skip). Pilot
    runs: `wf_c2ac01f8-c81` (v1), `wf_dba9389c-0dc` (v2).
  - On restart: re-invoke `Workflow({scriptPath:"/tmp/scb-cur/wf_curate.js"})` — done
    batches skip; then `python3 /tmp/scb-cur/validate_proposals.py`.
  - **Round-1 result**: keep 5,877 / rename 5,929 (**49.8% keep — matches \~half**), 0
    grammar/reserved/period/non-worklist collisions, 40 intra-register duplicates. 1
    batch failed mid-run (`scb-258-c3`, API error) → re-curated.
  - **Round-2**: 3,821 flagged across 107 registers → 136 chunks (`round2_input/`, run
    `wf_dfcf5c30-eb8`). Reasons: long 2,540, hard 1,018, coined 924, over40 237,
    collision 80. Validator now also flags long *keeps* (>32c) — caught two lazy
    all-keep batches (`scb-254-c4`/`scb-258-c3`). Reviewers get per-register reserved
    namespace + family slugs + flag reasons; `round2/<chunk>.json` overrides round1 per
    source_id.
  - Round-2 restart: `Workflow({scriptPath:"/tmp/scb-cur/wf_round2.js"})` (done chunks
    skip); rebuild round2_input via `python3 /tmp/scb-cur/round2_export.py` if lost.
- \[\~\] **P4 — Reconcile + generator rules** (#732):
  - **Merged final** (`final_merge.py` → `final_pairs.json`): 11,806 pairs, 5,553
    keep==auto, 6,253 changed; **0 violations** (`validate_final.py`): all ≤40 chars,
    49% ≤24, median 25, 0 collisions. 1 collision (`237.14307` vs `237.49966`) +
    manual-fixed (`-pof`, source-derived). 62 mislabeled "keep" decisions recomputed.
  - **Mining** (`mine_rules.py`): unconditional stopword strip reproduces 645 renames
    but **regresses 1,587 conservative keeps** → net worse; parenthetical strip
    regresses 307; lever-C moves non-worklist folds. Only clean systematic rule =
    measurement-unit parenthetical de-noise.
  - **Lever A IMPLEMENTED** (`_strip_unit_parentheticals` in `fqid_slugs.py` + 2 tests +
    DESIGN.md): strips pure unit parentheticals (`(areal i hektar)`→drop), keeps
    distinguishing ones. **Hard guard PASSED** (`guard_autodiff.py`, baseline vs lever-A
    `scb.auto.toml`): 20 worklist slugs moved, **0 clean slugs moved**. Reproduces \~16
    agricultural-area finals.
  - **Reconcile** (`emit_overrides.py`): PIN EVERY worklist final (sanctioned fallback —
    gen-reproduction proved unstable, see decisions log). Emitted **11,802 SCB + 4 SOS**
    `[variable.*]` overrides + 12 clean-stability pins. scb.toml 277→12,091, sos.toml
    15→18 `[variable]`. Reconcile build (`db3`) VALIDATED; `verify_full.py` fixpoint:
    **0 worklist + 0 clean discrepancies**; `.snapshot.json` refreshed (11,818 added).
- [x] **P4 — Reconcile + generator rules** — COMPLETE.
- [x] **P5 — Gates → ready-to-merge** — COMPLETE; PR #746 ready, awaiting sign-off.
  - ✅ ruff / ruff format / ty (whole repo) clean.
  - ✅ pytest 3,236 (non-integration) + integration (pre-push, Docker) green;
    test_slug_snapshot regenerated.
  - ✅ real-seed `build-db` on the **committed head** (db4) validates; db3≡db4 IDENTICAL
    (review fix build-neutral). **dbdiff** baseline→head: `variable` 6,253 renames +
    slug-derived downstream (concept_group re-key, same_as/related_to/replaced_by
    endpoint updates, variable_alias −30 benign re-parent, variable_state_lineage
    −1,232/+561 advisory = #660/#418 re-key fallout). Clean-slug guard 0; worklist 0.
  - ✅ `/code-review` high: 3 finders (generator / ref-fix / conventions). Ref-fix +
    conventions CLEAN. 1 generator finding (unit-paren strip dropping a derivable slug
    on a reserved/period remainder) FIXED + regression test (commit 4e4a0db2);
    name_freq- collapse edge → DESIGN note (dbdiff-gated); SOS comment label fixed.
  - ✅ 5 deliverable commits pushed; stale-head MATCH (4e4a0db2).
  - ⏳ Codex window polling (`pr_review_status.py 746`, bg). STOP — do not merge; await
    sign-off. A `reg_meta_build` DB release is owed (note, don't cut).

## Key numbers

- Worklist: 11,806 (scb 11,802 + sos 4) → **0** after curation (every worklist var
  pinned to its authoritative final).
- Curated overrides shipped: **11,802 SCB + 4 SOS** + 12 clean-stability pins.
- 6,253 are genuine renames (final ≠ auto); 5,553 are curator-confirmed keeps.
- Generator rule (lever A): reproduces \~16 finals; 0 clean-slug movement
  (dbdiff-verified); kept as a future-default improvement, not relied on to shrink
  overrides.
- Final slugs: 100% ≤40 chars (convention cap), 49% ≤24 (target), 0 collisions.
- reg_meta_build DB release OWED (DB slug content changes).

## Decisions / blockers log

- **Generator rules: only lever A (unit-paren de-noise) shipped;
  stopword/tail-truncation/ lever-C documented as deferred** (DESIGN.md + #732 spec).
  Reason: mining showed broad rules regress more conservative human keeps than they
  reproduce (curation is semantic); lever-C moves non-worklist fold slugs (fails the
  clean-slug guard). Safe/conservative path per the mission fallback; reconcile pins all
  finals so no outcome changes.
- Collision `237.14307`/`237.49966` (identical near-dup vars, same column) →
  disambiguated `237.14307`→`yrkeskod-fob80-nyk-arbetslosa-pof` (source-derived `pof`
  from its def).
- **Reconcile: PIN EVERY worklist final (sanctioned fallback), not the gen-shrunk
  subset.** The gen-reproduction optimization is unreliable: curated vars are excluded
  from the build's Pass-3 `pending` set, so applying overrides shifts
  `name_freq`/collision routing and un-pinned vars (incl. reproduced keeps) drift from
  their pre-override gen value (measured 419 such flips). Pinning all 11,802 worklist
  finals + 4 SOS makes the curated slug build-order-independent. lever-A stays as a
  future-default improvement (tested + documented), not relied on to shrink overrides.
  scb.toml \~12,091 `[variable]`.
- **Clean-stability pins**: overrides change the collision landscape → a few clean
  drift/fold vars ripple (drift grabs a freed name; fold loses its stem). Pin them to
  baseline (`clean_pins.json`); iterate `verify_full.py` until 0 worklist + 0 clean
  discrepancies. Broken slug-anchored refs (classification_links.toml + relations.toml,
  30) updated to new slugs via `fix_refs.py`.
- **Phase-4 resume**: `final_merge.py`→`final_pairs.json` (authoritative);
  `emit_overrides.py` (pins all worklist + clean_pins into scb/sos.toml, idempotent via
  markers); `fix_refs.py` (ref updates); rebuild slugdir3/db3; `verify_full.py`
  (fixpoint: 0 disc) + `validate_final.py` (grammar/uniqueness) + `guard_autodiff.py`
  (lever-A: 0 clean moved).

## Workflow runIds

- Curate fan-out: `wf_d5d01942-671` (264). Pilot: `wf_c2ac01f8-c81`, `wf_dba9389c-0dc`.
- Round 2: `wf_dfcf5c30-eb8` (136). Plus 2 direct re-curation agents (scb-258-c3,
  r2-253-c1).
