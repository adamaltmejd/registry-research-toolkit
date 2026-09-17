---
name: build-db
description: >-
  Registry Research Toolkit build-db workflow. Use when asked to run a real
  `reg-meta-build build-db` rebuild, verify build-affecting changes with the maintainer
  seed, compare rebuilt DB content with dbdiff, profile slow phases, capture build logs,
  or inspect structured issues and post-build SQLite/invariant checks.
---

# Registry Build DB

Run from the repository root of the candidate checkout. Read `reg_meta_build/README.md`
and its `DESIGN.md` for the maintained three-stage flow: prepare machine-readable
sources, resolve checked curation, then write SQLite.

## Select inputs

Use the maintainer's exact selection JSON. It pins a prepared-source commit and manifest
digest, scope declarations and catalog metadata. Record the code revision and selection
digest alongside the build evidence. Never substitute a branch name, infer a newer input
revision, or rewrite guards to make an update pass.

If a change requires new source interpretation or curation, prepare a separate candidate
and inspect it before selection. Preparation fully validates the candidate; warm builds
reuse it without expanding cold archives or redoing preparation validation. Builds do
not mutate or commit accepted inputs. PDF interpretation and LLM work stay outside the
build. Do not overlay loose files onto a pinned candidate.

## Run a diagnostic or strict build

Choose a new scratch directory outside the accepted input and curation repositories. The
report directory must not exist. Always use an explicit scratch destination for
verification, preserving the active catalog.

```sh
run_dir="$(mktemp -d "${TMPDIR:-/tmp}/regmeta-build.XXXXXX")"
selection="/absolute/path/to/selection.json"

# Diagnostic: complete the scan and retain unresolved discrepancies (exit 10).
uv run reg-meta-build build-db --selection "$selection" \
  --report-dir "$run_dir/report" --timing \
  --diagnostic --diagnostic-db-path "$run_dir/diagnostic.db" \
  > "$run_dir/build.log" 2>&1
```

Diagnostic mode retains error severity. Invalid pins, malformed contracts and
implementation failures still abort. An exit code of 10 alone does not prove a completed
diagnostic: require `status = diagnostic_complete` in `report/summary.json`. The
database is marked nonpublishable; do not activate or release it.

Use a separate new run directory for strict verification:

```sh
run_dir="$(mktemp -d "${TMPDIR:-/tmp}/regmeta-build.XXXXXX")"
uv run reg-meta-build --db "$run_dir/catalog" build-db \
  --selection "$selection" --report-dir "$run_dir/report" --timing \
  > "$run_dir/build.log" 2>&1
```

Strict builds preserve the previous destination on failure. Publication requires exit 0,
a complete summary with `publication_ready = true`, and all structural and corpus
validation. There is no validation bypass. Reports contain `summary.json` and
`events.jsonl.gz`, with source references, applicability failures and withheld output.

Start one build and follow its existing process/session; never relaunch to check
progress. Read phase timing and the log tail. For tool polling, use bounded waits so
progress updates and user steering remain responsive. Keep failed outputs for diagnosis.

## Verify and compare

The common writer validates the database before placement. For an independent post-build
check, open the output read-only and run `PRAGMA integrity_check` and
`PRAGMA foreign_key_check`. Inspect the summary and source-linked ledger for the
behavior the change is intended to affect. A diagnostic corpus failure is evidence to
explain, not a guard to weaken.

Use dbdiff for content-neutral changes or an expected bounded delta:

```sh
uv run python -m reg_meta_build.dbdiff \
  /absolute/path/to/baseline.db /absolute/path/to/candidate.db \
  --json > "$run_dir/dbdiff.json"
```

Dbdiff exit 0 means equivalent content under its documented ignored fields; exit 1 means
differences. Inspect the report before accepting a claimed bounded change. Row counts
alone are insufficient. For a broad pipeline refactor, use its agreed comparison scope
rather than inventing an exhaustive discrepancy-by-discrepancy curation gate.

For deterministic replay, run the same pinned selection in a second fresh process and
compare database bytes and decompressed event-ledger bytes. Keep timing conditions
explicit: overlapping builds or audits are not isolated performance benchmarks.

## Report

Report the code revision, exact input selection, output/report/log paths, actual exit
status, summary status and publication readiness, relevant validation, SQLite checks,
timings and comparison results. Separate engineering verification from unresolved
curation. Preserve accepted inputs, cold archives and the active catalog. Clean only
task-owned scratch artifacts after their required evidence has been retained.
