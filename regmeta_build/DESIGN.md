# Design: regmeta_build

Design rationale and constraints. For usage, see `regmeta-build --help`.

## Scope

`regmeta_build` owns the build pipeline that produces the SQLite databases
`regmeta` queries against. Specifically:

- `regmeta.db` — main metadata DB (~520 MB uncompressed). Built from SCB
  source CSVs under `regmeta_build/input_data/`, classifications seed at
  `regmeta_build/classifications.toml`, and curated slug TOMLs under
  `regmeta_build/fqid_slugs/`. Validated by `regmeta_build/validate.py`
  before shipping.
- `regmeta_docs.db` — FTS5 search index over the curated markdown under
  `regmeta_build/docs/`.

Both DBs ship as `.zst`-compressed GitHub Release assets parallel to the
`regmeta` PyPI package (release-skill orchestrates this).

## Why split from `regmeta`

The query side (`regmeta`) needs only the sqlite3 stdlib. The build side
pulls openpyxl, owns large maintainer-edited input data, and runs on a
different cadence (most users never run a build). Separating the two:

- Keeps the `regmeta` wheel small and dep-light for end users.
- Lets the two release on independent tags (`regmeta/v*`,
  `regmeta_build/v*`).
- Mirrors the build/runtime separation needed for a future Go/Rust
  port of the query layer.

See `REFACTOR_SPEC.md` §4 *Why this split* and §15 step 2.

## Dependency direction

`regmeta_build → regmeta` only. The builder imports query helpers
(`open_db`, `default_db_dir`, `DB_FILENAME`, `SCHEMA_VERSION`,
`derive_variable_slug`, etc.) but `regmeta` never imports
`regmeta_build`. The schema contract — the set of constants and
helpers both packages agree on — lives in `regmeta`.

## What lives where

| Module                              | Package         |
| ----------------------------------- | --------------- |
| `db.py` (DDL, build_db, CSV import) | `regmeta_build` |
| `db.py` (open_db, schema constants) | `regmeta`       |
| `doc_db.py` (build_doc_db)          | `regmeta_build` |
| `doc_db.py` (open_doc_db, ensure)   | `regmeta`       |
| `cli.py` (maintain subtree)         | `regmeta_build` |
| `cli.py` (query, update, docs)      | `regmeta`       |
| `fqid_slugs.py`                     | `regmeta_build` |
| `classifications.py`                | `regmeta_build` |
| `validate.py`                       | `regmeta_build` |
| `sources/` (provider CSV parsers)   | `regmeta_build` |
| `fqid.py`, `catalog.py`, `queries.py`, `doc_queries.py`, `errors.py`, `update.py`, `download.py` | `regmeta` |

## CLI shape

Top-level commands (no `maintain` subgroup; that group is dissolved):

```text
regmeta-build build-db [--validate] [--skip-slugs] ...
regmeta-build build-docs ...
regmeta-build seed-slugs [--scb] ...
regmeta-build precheck-slugs ...
regmeta-build parse-sos ...
```

The matching `regmeta maintain *` forms are removed. `regmeta maintain
update` / `info` are promoted to top-level `regmeta update` / `regmeta
info` (query-side concerns — fetching/inspecting prebuilt DBs).
