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
  --input-bundle /Users/adam/Code/registry-research-toolkit/.local/catalog-inputs/bundles/accepted \
  --input-commit <accepted-full-commit> \
  --input-manifest-sha256 <catalog-bundle-json-sha256>
```

The exact commit and manifest pins come from the maintainer's acceptance record; never
substitute a branch name or infer a newer commit. The input path is an ignored, separate
host-local Git repository with no remote. The script writes a timestamped
`/tmp/<slug>.log`, builds into scratch paths, lets `build-db` copy the selected bundle's
mutable slug inputs to its per-run workspace, enables `--timing`, emits sparse
milestones plus quiet-period health, and runs `integrity_check`,
`foreign_key_check`, key table counts, and optional dbdiff after a successful build. It
uses the SCB value prestage cache by default when SCB is in the provider set.

## Running unattended

The build is long; launch the watcher once and let it run to completion, then read
`/tmp/<slug>.summary.json` and the log tail — never re-launch it to check progress.

- Codex: start the watcher once with `exec_command`; when it returns a running session
  id, poll that same session with `write_stdin` and `yield_time_ms=300000` (5 minutes).
  Do not use repeated `exec_command` probes or 30-second polling loops.
- Claude Code: run the watcher as a single backgrounded shell command
  (`run_in_background`), which exits when the build finishes and yields exactly one
  completion notification; then read the summary JSON and log tail. Do not wrap the
  watcher in a subagent — a subagent that backgrounds the build returns before it
  finishes, its detached process is not tracked, and the completion result is lost.

Report only phase changes, failures, quiet-period health, completion, or explicit status
requests.

## Inputs

Routine and verification builds select the complete accepted bundle:

```sh
--input-bundle /Users/adam/Code/registry-research-toolkit/.local/catalog-inputs/bundles/accepted \
--input-commit <accepted-full-commit> \
--input-manifest-sha256 <catalog-bundle-json-sha256>
```

If a PR changes tracked provider inputs, curation, or global slug state, prepare a new
candidate from those exact bytes with `prepare-input-bundle`, inspect and explicitly
commit it in the local input repository, then use its new pins. Preparation never
overwrites or commits accepted data and never creates a remote. Do not overlay loose
files onto an accepted bundle. `--input-dir` is reserved for explicit raw source
preparation/testing, not routine selection.

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
  --input-bundle /Users/adam/Code/registry-research-toolkit/.local/catalog-inputs/bundles/accepted \
  --input-commit <accepted-full-commit> \
  --input-manifest-sha256 <catalog-bundle-json-sha256> \
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
