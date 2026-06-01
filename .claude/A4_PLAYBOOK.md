# A4 Orchestration Playbook

**Scope & lifecycle.** Working playbook for an agent orchestrating **Stage A4
(Adapter refactor + SOS)** of the Model A migration. Transient and
**self-deleting** — delete it when A4 ships (like `MIGRATION_PLAN.md`). It
explains *how* to run A4; *what* each step does lives in `MIGRATION_PLAN.md`
(the live tracker) and `REFACTOR_SPEC.md` §15. The companion `/goal` condition
carries the compressed, must-not-forget rules (re-injected every turn); this
file is the elaboration — **re-read it at the start of each step.**

## Role

You are the ORCHESTRATOR, running in the MAIN LOOP. Spawn opus subagents via
`Workflow()` for fan-out (planning, implementation, review panels). Keep in the
main loop — they cannot live inside a single `Workflow()` script — the
`/simplify` and `/review` skills, draft-PR creation, watching CI, waiting on
Codex, and merges.

## Sequencing

A4 is **strictly sequential**: A4.1 → A4.2 → A4.3 → A4.4 → A4.5, each builds on
the prior adapter/ID/SOS work. Do NOT start a step until the previous is
merged. Parallelism applies only **within** a step (e.g. A4.3's 13 SOS
workbooks, A4.4's ~800 panel-template rows), never across steps.

## Per-step pipeline

1. **Plan (Workflow, stage 1).** An opus planning agent reads the step's
   section of `MIGRATION_PLAN.md` + only the relevant `REFACTOR_SPEC.md`
   sections (§15 "Stage A4"; §16 for A4.2's property/confinement tests; the
   IR/adapter + §5.x SOS sections — don't read the whole 4,700-line spec). It
   returns a concrete file-by-file plan, flags risks, and surfaces any
   maintainer-decision forks to me.
2. **Implement (Workflow, stage 2).** An opus implementation agent implements
   per the plan. When the Workflow returns, open a **DRAFT** PR referencing the
   A4.x identifier.
3. **Simplify.** Run the `/simplify` skill on the diff (it fans out its own
   review agents). Apply the worthwhile fixes; ensure it's efficient and not
   over-engineered. Its edits weren't gate-checked — **re-run the gate after.**
4. **Gate.** Commit — pre-commit enforces the static + doc gate (ruff-check,
   ruff-format, ty, pytest, markdownlint). Then run the **real-data build
   gate** (below); pre-commit does NOT run it.
5. **Review.** Mark the PR ready. Watch CI; wait ~10 min for Codex + Copilot
   and address their inline P1/P2 (don't trigger `@codex` yourself). Also spawn
   an independent `/review` agent (or a Workflow review panel) — it RETURNS
   findings to me only and NEVER posts PR comments (posting under the
   maintainer identity trips the security classifier). Iterate; re-check the
   bots ~10 min after each fix push.
6. **Decide.** Resolve all relevant issues. Escalate genuine architecture/scope
   decisions to me instead of guessing; do NOT merge with an unresolved P1
   without my sign-off.
7. **Merge.** Squash-merge (merge-commit only to preserve distinct ride-along
   work). Flip the step's checkbox in `MIGRATION_PLAN.md` and update its status
   note as part of the PR.
8. **Follow-ups.** Triage what you found: critical → fold into the step's PR
   (or an add-on PR); useful-but-not-critical → file a "suggested task."

## The real-data build gate (what makes A4 ≠ A3)

Fixture/unit tests are NOT sufficient for A4 — it mutates the reg_meta DB
build. Pre-commit does not run this; do it manually, per step.

- **Baseline.** A pre-A4, `--validate`-clean DB (SCHEMA_VERSION 5.1.0, from the
  real SCB seed at `reg_meta_build/input_data/`) is preserved at
  `/Users/adam/.local/share/reg_meta-baseline-pre-a4/reg_meta.db`. A fresh
  build installs at the default path
  `/Users/adam/.local/share/reg_meta/reg_meta.db` (auto-discovered by the
  `reg-meta` CLI / `regmeta` skill — no env var).
- **Slug-dir footgun.** EVERY validation build MUST pass a temp `--slug-dir`,
  or `build-db` regenerates `scb.auto.toml` in the repo (UNFROZEN), dirties the
  tree, and trips the `test_slug_snapshot` pre-commit hook (don't `--no-verify`
  around it — fix it):

  ```sh
  SLUGDIR=$(mktemp -d); cp -R reg_meta_build/fqid_slugs/. "$SLUGDIR/"
  uv run reg-meta-build build-db --input-dir reg_meta_build/input_data/ --slug-dir "$SLUGDIR" --validate
  ```

- **A4.1 (SCB adapter refactor) — byte-identical output.** The acceptance test
  is identical universal-DB *content* vs the baseline, via the merged harness
  (`reg_meta_build/src/reg_meta_build/dbdiff.py`, PR #162 — do NOT rebuild it).
  Your rebuild overwrites the default path; diff it against the preserved
  baseline:

  ```sh
  uv run python -m reg_meta_build.dbdiff \
    /Users/adam/.local/share/reg_meta-baseline-pre-a4/reg_meta.db \
    /Users/adam/.local/share/reg_meta/reg_meta.db
  ```

  Must **exit 0**. It's content-level (order-independent, BLOB-safe), already
  ignores nondeterministic metadata (`import_manifest.import_date`), and on
  mismatch names the table + sample rows — so you can pinpoint which adapter
  output drifted (watch for emit-order changes that flip autoincrement IDs).
  For an in-suite check, import `reg_meta_build.dbdiff.diff_db_content`.
- **A4.2 (deterministic IDs + provenance DB).** Run the §16 namespace-invariant
  property test (10k random mints land in [2^62, 2^63); the SCB ID band
  [0, 2^32) is provably disjoint) and the provenance-DB confinement test.
  `build-db --validate` clean.
- **A4.3 / A4.4 / A4.5.** Each ends with a clean `build-db --validate`. A4.5 is
  the first combined SCB+SOS build — verify no ID collisions, no spurious
  cross-provider `same_as` edges, and no cross-provider FTS bleed (the dbdiff
  harness handles FTS virtual/shadow tables, so it's reusable for spot-checks).
- **Per-step gate order:** land → real-data validate → independent review + all
  bot reviews addressed → ~10-min window, re-check → merge.

## Conventions

- Follow the repo `CLAUDE.md` (uv not pip, bun/bunx not npm, rg/fd). Never
  bypass git hooks (`--no-verify`/`-n`); fix the cause.
- Don't begin a step on a dirty working tree — commit/stash first.
