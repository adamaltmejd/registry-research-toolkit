---
name: build-db
description: >-
  Registry Research Toolkit build-db workflow. Use when asked to run a real
  `reg-meta-build build-db` rebuild, verify build-affecting PRs with the maintainer
  seed, use or refresh the SCB value prestage cache, compare rebuilt DB content with
  dbdiff, profile slow build phases, capture build logs, inspect quiet build periods, or
  perform post-build SQLite/invariant checks.
---

# Registry Build DB

Run from the repository root of the current Registry Research Toolkit checkout.

Use the watcher script by default:

```sh
uv run --no-project python scripts/build_db_watch.py \
  --slug <short-task-name> \
  --input-dir /Users/adam/Code/registry-research-toolkit/reg_meta_build/input_data
```

The script writes a timestamped `/tmp/<slug>.log`, builds into scratch paths, copies
`reg_meta_build/fqid_slugs/` before passing `--slug-dir`, enables `--timing`, emits
sparse milestones plus quiet-period health, and runs `integrity_check`,
`foreign_key_check`, key table counts, and optional dbdiff after a successful build. It
uses the SCB value prestage cache by default when SCB is in the provider set.

## Codex Polling

Start the watcher once with `exec_command`. When it returns a running session id, poll
that same session with `write_stdin` and `yield_time_ms=300000` (5 minutes). Do not use
repeated `exec_command` probes or 30-second polling loops.

Report only phase changes, failures, quiet-period health, completion, or explicit status
requests.

## Inputs

From a worktree, use the main checkout's real seed unless the worktree has the full
untracked corpus:

```sh
--input-dir /Users/adam/Code/registry-research-toolkit/reg_meta_build/input_data
```

If the PR changes tracked `reg_meta_build/input_data/**`, build an overlay input root
that presents the main checkout's untracked seed plus the PR-head tracked inputs,
including mirrored deletions/renames, and pass that overlay as `--input-dir`.

For a full global rebuild, omit `--providers`. Use `--providers` only for deliberately
scoped investigation. Use `--no-validate` only for throwaway profiling; merge/release
evidence should use default validation.

## SCB Value Prestage Cache

The watcher defaults to:

```sh
--prestage-cache <tmp-dir>/regmeta-build-prestage/scb-value-prestage.sqlite
```

Keep this enabled for normal full rebuilds. It caches only the stable SCB Vardemangder
projection output: `value_code`, `value_set`, `value_set_member`, and CVID
value-set/version/nivå assignments. It does not cache operational definitions, slug
TOMLs, codelivery/fold/split curation, classifications, concept groups, lineage, search
indexes, validation output, or any global cross-provider derivation.

`build-db` validates the cache before using it. Missing, stale, or unusable cache files
are rebuilt from raw SCB inputs automatically. Staleness is based on `Vardemangder.csv`,
`VardemangderValidDates.csv`, a prestage format version, and the
Registerinformation-derived CVID/register-version backbone used for year projection.
Operational-definition text changes should not stale the cache; the final build still
reads `Registerinformation.csv` and recomputes op defs.

Force a rebuild with `--refresh-prestage-cache` when the PR changes SCB value-set
projection logic, prestage cache schema/versioning, or when the user explicitly asks for
cache refresh evidence. Use `--no-prestage-cache` only to measure the raw path or debug
the cache itself.

If bypassing the watcher and running `reg-meta-build build-db` directly, pass
`--scb-value-prestage-cache <path>` and optionally `--refresh-scb-value-prestage-cache`
with the same rules.

## Dbdiff Verification

Use dbdiff when the rebuild is meant to prove content identity or bounded content drift:
content-neutral code changes, refactors, performance changes, or PRs where only a known
small DB delta is intended.

```sh
uv run --no-project python scripts/build_db_watch.py \
  --slug <short-task-name> \
  --input-dir /Users/adam/Code/registry-research-toolkit/reg_meta_build/input_data \
  --dbdiff-against <baseline-reg_meta.db>
```

`--dbdiff-against` compares the built `reg_meta.db` to the baseline with
`python -m reg_meta_build.dbdiff` after validation and SQLite checks pass. Identical
content keeps exit 0. Any diff makes the watcher exit non-zero, keeps scratch outputs,
and writes the full JSON report to `/tmp/<slug>.dbdiff.json` unless `--dbdiff-json` is
set. For expected small diffs, inspect and summarize the dbdiff report; do not treat
plain row counts as sufficient evidence.

## Results

On success, report the log path, summary JSON path, scratch DB dir, prestage cache path
and whether it was applied or rebuilt, exit status, `integrity_check`,
`foreign_key_check`, important `[timing]` lines, long quiet intervals, dbdiff
status/report path if used, and any task-specific SQL probes.

On failure, keep scratch outputs and quote the first actionable failing section from the
log. Do not delete scratch paths until after checks and requested inspection complete.

Start investigation from the current build's observed slow phases. Search old commits
only if the current profile remains ambiguous.
