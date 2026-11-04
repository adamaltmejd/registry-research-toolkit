---
name: build-db
description: >-
  Registry Research Toolkit real-seed build workflow with `reg-build`. Use for full
  real-corpus builds, curation locks, catalog and holdings diffs, verification and build
  reports. Routine TOML curation and focused tests do not require a full rebuild.
---

# Registry Build DB

Run from the repository root of the candidate checkout. `reg-build` (`crates/reg-build`)
is the only builder; its design is in `crates/DESIGN.md`, "The catalog builder". The
release skill's step 8 holds the canonical release commands; this skill is the same
toolchain for verification outside a release.

Nothing serializes real-seed runs (`build`, `lock`, `holdings`) or other heavy jobs: the
Operator schedules them with the maintainer. Start one only when the Operator has
cleared it. Build optimized (`cargo build --release --workspace --locked`); the real
build is long and the verifiers and conformance run the same binaries.

## Select inputs

Every input lives in the private repository `adamaltmejd/registry-catalog-inputs`; the
public repository holds code and authored curation (`curation/`) only.
`curation/inputs.toml` pins the inputs repository, its commit, and each input with its
reader. Run every build against a worktree of the inputs repository at exactly that
commit (`git -C <inputs-checkout> worktree add --detach <dir> <commit>`), never its
default branch, and never edit it in place. For every input with a `release` field,
download its pinned asset to `<assets>/<release>/<path>` first
(`gh release download <input-release> --repo <inputs-repo> --pattern <input-path> --dir <assets>/<input-release>`);
the reader verifies the manifest's SHA-256.

## Build

```sh
target/release/reg-build build --inputs "$inputs" --assets "$assets" \
  --curation curation --out "$run_dir/reg_meta.db" --report "$run_dir/build-report.json"
```

`--out` is a database filename. Without `--release` the build is a dev build: refused
curation statements (changed premises, empty or ambiguous selectors, conflicts) are
skipped and reported, so it completes as a diagnostic. `--release` (with
`--catalog-version`) refuses them, and also unplaced or split ledger ids and variables
without a ledger id. Read the report's warnings and confirm all expected providers and
inputs are present.

`reg-build read --inputs … --curation … --out DIR` dumps the source frames as JSONL (and
each input's warnings) without resolving: use it to inspect what a reader produced.

## Curation locks

Builds check `curation.lock` premises and never accept a changed premise automatically.
After an intended curation or input change, regenerate the locks for the affected
registers and review the lock diff:

```sh
target/release/reg-build lock [REGISTER...] --inputs "$inputs" --assets "$assets" --curation curation
```

It refuses invalid statements before writing any lock; with no registers it relocks all.

## Verify and compare

`reg-build verify` is the single structural validation authority. Against a previous
catalog it also reports id changes:

```sh
target/release/reg-build verify "$run_dir/reg_meta.db" --previous "$prev/reg_meta.db"
target/release/reg-build diff "$prev/reg_meta.db" "$run_dir/reg_meta.db" [--json]
```

`diff` exits 0 for no changes, 1 for differences to review, 2 for an error. It compares
reader-visible facts by natural identity, never raw ids. Inspect the report before
accepting a claimed bounded change: row counts alone are insufficient. A strict build
that passes but changes committed content (register owners, slugs, classification books,
edition splits) is a content change: read the diff.

For deterministic replay, build the same pinned inputs and curation twice in fresh
processes and compare database bytes. Overlapping builds are not performance benchmarks.

## SWECOV holdings

The holdings pin is the root `stewards.toml`: the steward repository, an exact commit
and the config path. Compile holdings only against a complete `--release` base, from a
clean SSH clone of the steward repository checked out at the pin, then verify the pair
and compare with a baseline holdings DB (release skill 8d, 8e and 8g):

```sh
target/release/reg-build holdings --base "$run_dir/reg_meta.db" --steward swecov \
  --steward-repo "$steward" --registry stewards.toml \
  --out "$run_dir/reg_meta_swecov.db" --report "$run_dir/holdings-report.json"
target/release/reg-build verify "$run_dir/reg_meta_swecov.db" \
  --catalog "$run_dir/reg_meta.db" --steward-repo "$steward"
target/release/reg-build diff --holdings "$old/reg_meta_swecov.db" "$run_dir/reg_meta_swecov.db"
```

Steward curation locks are recorded in the steward repository with
`reg-build lock --base <catalog> --inputs <steward-repo> --curation <steward-repo>/curation`.
The holdings report and log are private evidence: do not paste their contents anywhere
public.

## Report

Report the code revision, exact input and steward pins, output/report/log paths, actual
exit status, the report's warnings, verification and diff results, and timings. Separate
engineering verification from unresolved curation. Preserve the pinned inputs and the
active catalog. Clean only task-owned scratch artifacts after their required evidence
has been retained.
