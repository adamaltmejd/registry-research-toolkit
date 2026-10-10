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

Run every real-seed `prepare-sources`, `build-db`, `extend-db` and `check-curation`
under `uv run --no-project scripts/gate.py real-seed -- CMD`. It allows one real-seed
run on the machine at a time and holds a heavy-job slot, so a concurrent run queues
instead of slowing both. A `waiting for one of 1 real-seed slots` line means another
session's run holds it: let it finish rather than working around the lock.

During the SWECOV restoration, the builder produces schema 8 databases while readers
still require schema 6. Keep restoration changes in `reg_meta_build`; reader and UI
adaptation is a later task. Verify candidate outputs with builder-owned openers or
read-only SQLite, rather than changing consumer compatibility to make a proof pass.
`precheck-slugs` matches the slug tree against a built catalog by slug path: every
register and variant row must be pinned and every pin must name a row.
`--update-snapshot` also refreshes the naming snapshot.

## Reuse a cached output first

Run real-seed preparations and builds through `scripts/real_seed_cache.py`; the sections
below show its commands. It returns a stored output when the inputs it keys are
unchanged. On a miss it runs the `reg-meta-build` command shown under the real-seed lock
and stores the output if the run completed. A main-tip baseline that another session
already built is a hit. Run `reg-meta-build` directly (under `gate.py real-seed --`)
only for what the cache does not handle: `--dump-decisions`, `check-curation` and
`extend-db`.

Each command prints one JSON object on stdout; `hit` says whether anything ran. Its
module docstring lists every field each key covers, `--key` prints a key and its fields
without running anything, and diffing two keys shows why a lookup missed.

Cached entries are shared and read-only. Use them in place as dbdiff or comparison
inputs; never activate, release or modify them. Copy a database before writing to it.
The cache lives under `$REG_REAL_SEED_CACHE`, else
`${XDG_CACHE_HOME:-~/.cache}/reg-meta-real-seed`. It keeps the two most recently used
build entries (about 1.4 GB each) and any entry used in the last 6 hours.

`build --verify` rebuilds uncached and compares database bytes and decompressed
event-ledger bytes with the stored entry. Exit 1 means the key misses an input: report
it instead of trusting that entry. It costs a full build, so run it at an agreed
checkpoint. To check a prepare entry the same way, run `prepare-sources` into a new
directory and compare its `prepared_manifest_sha256` with the stored one.

## Select inputs

The same CLI option names select different artifacts at different stages:

  | Command                                                            | `--input-commit`                                           | `--input-manifest-sha256`                           |
  | ------------------------------------------------------------------ | ---------------------------------------------------------- | --------------------------------------------------- |
  | `verify-input-bundle`, `prepare-sources`, `inspect-source-records` | Clean Git commit containing the raw input bundle           | Raw bundle's `catalog-bundle.json` digest           |
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
uv run --no-project scripts/real_seed_cache.py prepare --input-bundle "$raw_bundle" \
  --input-commit "$raw_bundle_commit" --input-manifest-sha256 "$raw_bundle_sha256" \
  --output-dir "$new_prepared_dir"
```

On a miss the cache runs `reg-meta-build prepare-sources` with these options into
`--output-dir`. On a hit `--output-dir` is unused, and the result names the stored
`prepared_path`, its `prepared_manifest_sha256` and its `prepared_commit`: the HEAD of
the tree's acceptance repository, after the builder's warm-build check passed there. A
stored tree that is not committed yet exits 3 with `awaiting_acceptance` instead of
preparing again. A tree that fails the check (a missing payload, a moved or dirty
checkout) is a miss.

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

```sh
prepared="/absolute/path/to/accepted-prepared"
prepared_commit="EXACT_PREPARED_ACCEPTANCE_SHA"
prepared_manifest_sha256="EXACT_TOP_LEVEL_PREPARED_SHA256"

# Diagnostic: complete the scan and retain unresolved discrepancies.
uv run --no-project scripts/real_seed_cache.py build --prepared "$prepared" \
  --input-commit "$prepared_commit" --input-manifest-sha256 "$prepared_manifest_sha256" \
  --diagnostic 2> "$log"
```

Every lookup, a hit included, first runs the builder's own admission checks (the
prepared pins and, for a strict full build, a clean checkout) and exits 10 with the
builder's error if they fail. A project environment that cannot import those checks
exits 4 with `probe_environment_failed`: repair the environment, not the inputs. The
result names `database` and `report`. On a miss the cache runs `build-db` in a new
directory under the cache, with `--report-dir`, `--timing` and, for a diagnostic,
`--diagnostic --diagnostic-db-path`. A run that did not complete is not stored; the
result's `run_dir` keeps its outputs for diagnosis for 6 hours. Run `build-db` directly
only for `--dump-decisions`: give it a new scratch directory outside the accepted input
and curation repositories and an explicit destination, never the active catalog.

Diagnostic mode retains error severity. Invalid pins, malformed contracts and
implementation failures still abort. An exit code of 10 alone does not prove a completed
diagnostic: require `status = diagnostic_complete` in `report/summary.json`. The
database is marked nonpublishable; do not activate or release it. A terminal
`engineering_failure` after source resolution is not a completed diagnostic. Preserve
its source ledger, repair the failure, and rerun only when the batch is ready.

Strict verification is the same command without `--diagnostic`; the cache builds it into
a new `--db` directory, so the active catalog is never touched. A strict full build
needs a clean checkout, and its key includes the checkout commit.

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

## Real-seed tier guarantees

CI checks only that the committed curation loads (`test_committed_curation_loads`).
Whether it is right is checked here, by a strict real-seed build, since the test sweeps
in #1201 and #1213 moved these out of the suite: committed register owners, roles and
naming slugs (including IoT and LISA SNI renames), and every column-owning split
resolving to a slug; the committed MFR and LOVA classification-reference lists; FDB's
two-spelling column ownership; committed SNI and other book metadata and labels; RTB
edition splits and coding windows; the overlay layout and committed curation counts;
curated `same_as`/`replaced_by` edges and their component bounds; and that the committed
`scb.toml` still names the LISA variant the SWECOV column candidates carry. A strict
build that passes but changes one of these is a content change: read the dbdiff.

## Private SWECOV extension

Extend only the new complete strict base. A private input candidate is a separate
local-only Git acceptance from the public prepared sources. Pass its exact root through
`extend-db --holdings-input`, with its full `--input-commit` and manifest digest. Select
that same candidate's `providers/`, `slugs/` and `policy/inventory.toml`; loose overlays
are refused. Regenerate and accept a fresh candidate when inventory changes.

The builder's `holdings_policy.toml` retains exact undated tables through register
warnings and identifies genuine lookups. Inventory generation takes it through
`inventory --holdings-policy`. Require complete raw-table/column accounting across dated
inventory, retained-unknown evidence and explicit lookup/exclusion dispositions.
Retained unknown tables do not become annual or year-independent holdings. Authored
source-validity bounds can also remain unknown; storage sentinels are not observation
coverage. Keep reader and UI adaptation deferred.

## Report

Report the code revision, exact input pins and curation digest, output/report/log paths,
actual exit status, summary status and publication readiness, relevant validation,
SQLite checks, timings and comparison results. Separate engineering verification from
unresolved curation. Preserve accepted inputs, cold archives and the active catalog.
Clean only task-owned scratch artifacts after their required evidence has been retained.
