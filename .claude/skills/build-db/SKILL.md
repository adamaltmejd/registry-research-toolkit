---
name: build-db
description: >-
  Run and investigate Registry Research Toolkit `reg-meta-build build-db` rebuilds. Use
  when asked to run a real metadata DB rebuild, verify build-affecting PRs with the
  maintainer seed, use or refresh the SCB value prestage cache, compare rebuilt DB
  content with dbdiff, profile slow build phases, capture build logs, or perform
  post-build SQLite/invariant checks.
---

# Build DB

Run from the repository root of the current Registry Research Toolkit checkout.

Use the watcher script by default:

```sh
uv run --no-project python scripts/build_db_watch.py \
  --slug <short-task-name> \
  --input-dir /Users/adam/Code/registry-research-toolkit/reg_meta_build/input_data
```

The script:

- writes the full timestamped build log to `/tmp/<slug>.log`;
- builds into a scratch DB directory unless `--db-dir` is supplied;
- copies `reg_meta_build/fqid_slugs/` to a scratch slug dir and passes it as
  `--slug-dir`, so generated `*.auto.toml` files do not dirty the checkout;
- passes `--timing` unless `--no-timing` is supplied;
- emits sparse milestones plus quiet-period health every 300 seconds;
- runs `PRAGMA integrity_check`, `PRAGMA foreign_key_check`, and key table counts after
  a successful build;
- optionally runs dbdiff against a baseline DB after successful build checks;
- uses the SCB value prestage cache by default when SCB is in the provider set;
- writes `/tmp/<slug>.summary.json`.

## Running unattended

The build is long; launch the watcher once and let it run to completion, then read
`/tmp/<slug>.summary.json` and the log tail — never re-launch it to "check progress".

- **Claude Code:** run the watcher with `Bash` and `run_in_background: true`. The
  command exits when the build finishes, so you get exactly one completion notification;
  then read the summary JSON and the log tail. Do NOT wrap the watcher in a subagent — a
  subagent that backgrounds the build returns before it finishes, and its detached
  process is not harness-tracked, so the completion notification never fires and the
  result is lost (you end up hand-rolling a `pgrep` wait and parsing the log yourself).
- **Codex:** start the watcher once with `exec_command`; when it returns a running
  session id, poll that same session with `write_stdin` and `yield_time_ms=300000` (5
  minutes). Do not use repeated `exec_command` probes or 30-second polling loops.

Only report to the user on phase changes, failures, quiet-period health, completion, or
explicit status requests.

## Inputs

Use the main checkout's real seed from a worktree unless the worktree itself has the
untracked full input corpus:

```sh
--input-dir /Users/adam/Code/registry-research-toolkit/reg_meta_build/input_data
```

If the PR changes tracked `reg_meta_build/input_data/**`, do not validate directly
against the main checkout's input tree. Build an overlay input root that presents the
main checkout's untracked seed plus the PR-head tracked input files, including mirrored
deletions/renames, and pass that overlay as `--input-dir`.

For a full global rebuild, omit `--providers` so newly onboarded global providers are
included. Use `--providers` only for deliberately scoped investigation.

Use `--no-validate` only for throwaway profiling when validation is not part of the
question. Merge/release evidence should use default validation.

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

## Reading Results

On success, report:

- log path and summary JSON path;
- scratch DB dir;
- prestage cache path and whether it was applied or rebuilt;
- total exit status;
- `integrity_check` and `foreign_key_check`;
- important `[timing]` lines and any long quiet intervals;
- dbdiff status and JSON report path, if dbdiff was used;
- any task-specific SQL probe results.

On failure, keep scratch paths and quote the first actionable failing section from the
log. Do not delete scratch outputs until after checks and any requested inspection are
complete.

## Investigation

Start from the current build's observed slow phases. Add source instrumentation only
after the watcher identifies an opaque or long interval. Do not start by searching old
commits unless the current profile is ambiguous.
