---
name: build-db
description: >-
  Registry Research Toolkit pinned input preparation and build-db workflow. Use for new
  source candidates, batched real-corpus verification, dbdiff, profiling and build
  reports. Routine TOML curation and focused tests do not require a full rebuild.
---

# Registry Build DB

Run from the repository root of the candidate checkout. Check the relevant command's
`uv run reg-meta-build COMMAND --help` against these instructions before executing it.
The maintained flow is in `reg_meta_build/README.md` and the input storage and common
curation sections of `reg_meta_build/DESIGN.md`.

Use focused tests and typed TOML loading while editing. `check-curation` resolves
complete selected source scopes but can take minutes; it is not a seconds-long check. It
omits catalog dependencies, delivery coverage, SQLite and corpus validation, even for
selected registers. Run full verification at an agreed, coherent batch checkpoint.
Curation-only edits do not require source preparation.

## Select inputs

The same CLI option names select different artifacts at different stages:

  | Command                                                            | `--input-commit`                                           | `--input-manifest-sha256`                           |
  | ------------------------------------------------------------------ | ---------------------------------------------------------- | --------------------------------------------------- |
  | `verify-input-bundle`, `prepare-sources`, `inspect-source-records` | Clean Git commit containing the raw input bundle           | Raw bundle's `manifest.json` digest                 |
  | `check-curation`, `build-db`                                       | Clean Git commit containing the accepted prepared artifact | Prepared catalog's top-level `manifest.json` digest |

The prepared manifest's internal `input_commit` records the raw bundle commit. It is
**not** the prepared acceptance commit required by a build. `prepare-sources` also
returns raw bundle pins alongside `prepared_manifest_sha256`; do not copy its
`input_commit` into a build command. Child records/value manifest digests do not replace
the top-level prepared digest.

Use exact full commits and digests supplied by the maintainer or verified in the
selected repositories. Record the code revision and curation digest alongside them.
Never substitute a branch name, infer a newer input revision, or rewrite guards to make
an update pass.

For changed source inputs or provider-format interpretation, capture and prepare a
separate candidate. Use `prepare-input-bundle --help` for capture; it takes separate SCB
snapshot pins. Commit and verify the raw candidate before running:

```sh
uv run reg-meta-build prepare-sources --input-bundle "$raw_bundle" \
  --input-commit "$raw_bundle_commit" \
  --input-manifest-sha256 "$raw_bundle_sha256" \
  --output-dir "$new_prepared_dir"
```

The output directory must not exist and must be outside the selected source directories.
Preparation validates all selected inputs and writes atomically; it has no incremental
component-reuse API. Check disk space and allow for working files, temporary files and
local Git storage. Inspect the result, then explicitly commit its complete manifest and
`files/` inventory in a local-only Git repository and record that **prepared acceptance
commit** and top-level digest. A prepared candidate is not a publishable catalog.

Warm builds check accepted Git identity, manifest and file proofs without repeating
preparation. Builds do not mutate or commit inputs. PDF interpretation and LLM work stay
outside the build. Do not overlay loose files onto a pinned candidate.

## Run a diagnostic or strict build

Choose a new scratch directory outside the accepted input and curation repositories. The
report directory must not exist. Always use an explicit scratch destination for
verification, preserving the active catalog.

```sh
run_dir="$(mktemp -d "${TMPDIR:-/tmp}/regmeta-build.XXXXXX")"
prepared="/absolute/path/to/accepted-prepared"
prepared_commit="EXACT_PREPARED_ACCEPTANCE_SHA"
prepared_manifest_sha256="EXACT_TOP_LEVEL_PREPARED_SHA256"

# Diagnostic: complete the scan and retain unresolved discrepancies (exit 10).
uv run reg-meta-build build-db --prepared "$prepared" \
  --input-commit "$prepared_commit" --input-manifest-sha256 "$prepared_manifest_sha256" \
  --report-dir "$run_dir/report" --timing \
  --diagnostic --diagnostic-db-path "$run_dir/diagnostic.db" \
  > "$run_dir/build.log" 2>&1
```

Diagnostic mode retains error severity. Invalid pins, malformed contracts and
implementation failures still abort. An exit code of 10 alone does not prove a completed
diagnostic: require `status = diagnostic_complete` in `report/summary.json`. The
database is marked nonpublishable; do not activate or release it. A terminal
`engineering_failure` after source resolution is not a completed diagnostic. Preserve
its source ledger, repair the failure, and rerun only when the batch is ready.

Use a separate new run directory for strict verification:

```sh
run_dir="$(mktemp -d "${TMPDIR:-/tmp}/regmeta-build.XXXXXX")"
uv run reg-meta-build --db "$run_dir/catalog" build-db \
  --prepared "$prepared" \
  --input-commit "$prepared_commit" --input-manifest-sha256 "$prepared_manifest_sha256" \
  --report-dir "$run_dir/report" --timing \
  > "$run_dir/build.log" 2>&1
```

Strict builds preserve the previous destination on failure. Publication requires exit 0,
a complete summary with `publication_ready = true`, and all structural and corpus
validation. A `--registers` subset is nonpublishable, including a strict subset; it
cannot prove a complete global base for steward extension. There is no validation
bypass. Reports contain `summary.json` and `events.jsonl.gz`, with source references,
applicability failures and withheld output.

`--timing` reports phase wall time, not host CPU time or a profiler result. Capture
those separately during an agreed verification run when required; concurrent audits are
not isolated performance evidence.

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

For deterministic replay, run the same pinned inputs and curation in a second fresh
process and compare database bytes and decompressed event-ledger bytes. Keep timing
conditions explicit: overlapping builds or audits are not isolated performance
benchmarks.

## Report

Report the code revision, exact input pins and curation digest, output/report/log paths,
actual exit status, summary status and publication readiness, relevant validation,
SQLite checks, timings and comparison results. Separate engineering verification from
unresolved curation. Preserve accepted inputs, cold archives and the active catalog.
Clean only task-owned scratch artifacts after their required evidence has been retained.
