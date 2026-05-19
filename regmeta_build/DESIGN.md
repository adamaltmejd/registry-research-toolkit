# Design: regmeta_build

Design rationale and constraints for the build pipeline. For usage, see
`regmeta-build --help`. For query-layer rationale (the data model end
users see), see [../regmeta/DESIGN.md](../regmeta/DESIGN.md).

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

## Source parsers

Provider-specific logic lives under
`regmeta_build/src/regmeta_build/sources/`:

- `sources/sos.py` — parses Socialstyrelsen register metadata Excel
  deliveries (`.xlsx` per register). Returns `SosRegister` dataclasses.
  Isolated here so the quirks of that format (sheet-name variance,
  "metadatat"-typo section headings, phantom row counts, non-standard
  kodlistor) don't leak into schema or query code.
- SCB CSV import currently lives in `db.py::build_db`. Moving it under
  `sources/scb.py` with the same intermediate-representation pattern is
  tracked as a follow-up — non-blocking for additional providers.

The build-db step consumes each provider's parser and maps into the
unified schema. Providers share `register`, `variable`, `value_code`,
etc.; provider-specific fields live on optional columns or in
enrichment tables when they have no shared analogue. Adding a new
provider means writing a parser in `sources/` and a mapping step in
`build-db`; no query-layer or CLI changes should be needed.

## CSV import and encoding

SCB exports are pipe-delimited, cp1252 encoded. Several bytes in the
exports are actually DOS cp850 remnants undefined in cp1252:

| Byte | cp850 | Mapped to |
|------|-------|-----------|
| 0x81 | ü     | ü         |
| 0x8D | ì     | ì         |
| 0x8F | Å     | Å         |
| 0x90 | É     | É         |
| 0x9D | Ø     | Ø         |

These are mapped during import. The build reads ~1M backbone rows
from `Registerinformation.csv` and ~102M value-item rows from
`Vardemangder.csv`.

## Source-register resolution

The `VariabelRegister_Källa` field is resolved using deterministic
matching only — no fuzzy logic:

1. Extract parenthesized abbreviation (e.g. "Befolkningsregistret (RTB)" → RTB)
2. Match text before ` : ` separator against register names
3. Match entire text against register names

Unresolved sources are stored as raw text in `source_label` for human
review. The resulting `source_register_id` / `source_label` pair on
`variable` is what query commands surface (see
[../regmeta/DESIGN.md](../regmeta/DESIGN.md) § "Composite registers and
source tracking").

## Vardemängder sentinel filtering

`Vardemangder.csv` ships a row for every variable, including those with no
enumerated code list. SCB encodes "no codes" by stuffing a placeholder string
into `Värdekod` so that `Värdekod == Värdemängdsversion` (and typically
`Värdemängdsnivå`). Two disjoint cases occur with this shape, classified by
two allowlists in `regmeta_build/db.py`:

`_VARDEMANGDER_SENTINELS` — placeholder strings that mean "no enumerated
code list." Not real value codes; dropped silently.

| Värdekod | Meaning |
|---|---|
| `Tal` | Numeric variable |
| `Beskrivande text` | Free-form text variable |

Importing sentinels would pollute `value_code` with
rows that are never valid lookups, and write the placeholder into
`variable_instance.vardemangds{version,niva}` where downstream consumers
would mistake it for a real classification label. The authoritative type
signal is `variable_instance.datatyp` — the placeholder adds nothing and is
sometimes misleading (e.g. cvid 207 `DatInv` is `datatyp='int'` but tagged
`Beskrivande text`).

`_VARDEMANGDER_REAL_SHAPED` — kods that *happen* to equal their version
label but are real single-code value sets. Kept silently.

| Värdekod | Label |
|---|---|
| `1` | Hade ingen anställning före YH-utbildningen |
| `2` | Övriga civilstånd |

Both classifications are required because the shape alone is ambiguous. An
unguarded skip on `kod == version` would silently drop the real codes; an
unguarded keep would let new SCB placeholders pollute the DB.

The skip rule is tight: `kod == version == niva` AND `kod` ∈ `_VARDEMANGDER_SENTINELS`. Looser variants (e.g. `kod == version` but `niva` diverging)
fall through to the drift detector below and fail the build for human
review, even when the kod is already a known sentinel string. This guards
against a future SCB change to the sentinel shape.

A cvid whose only Vardemängder rows were sentinels gets `NULL` for
`vardemangdsversion`/`vardemangdsniva` on `variable_instance`. Fully-empty
rows (kod, label, item all empty) are dropped silently.

### Drift detection

A `kod == version` row where kod is in neither allowlist is treated as drift
and fails the build with `RegmetaError(code="vardemangder_drift", exit 10)`.
The importer can't tell whether such a row is a new sentinel or a new real
single-code value set, so the build refuses to ship and prompts the
maintainer to add the kod to one of the two allowlists.

The drift trigger only requires `kod == version`, not `kod == version ==
niva`, so a placeholder where SCB drops the niva equality still surfaces.
Currently observed sentinels have all three fields equal, but no upstream
guarantee.

This makes the maintainer's release workflow self-checking: any new SCB
sentinel string causes the rebuild to fail loudly with an actionable
remediation, rather than silently shipping pollution. There is no
interactive escape hatch — drift always fails — because the only correct
response is to update the allowlists, which is a one-line code change.

## Year projection

`build-db` projects every `(cvid, code_id)` pair through validity at build
time so each cvid carries the codes that were actually valid in its
regver year. The projection rule:

- For each pair, collect validity windows of all *tracked* ItemIds (those
  with at least one row in `VardemangderValidDates.csv`).
- If no tracked windows → include the code (always-valid fallback).
- Otherwise → include iff at least one window covers the cvid year.
- Yearless cvids (regver name has no plausible 4-digit year, e.g.
  `Person-År`) include all union pairs as a fallback.
- An untracked ItemId next to a tracked one does NOT relax the constraint:
  the tracked window is authoritative.

The result is the year-projected, content-addressed `value_set` /
`value_set_member` structure query users see (documented in
[../regmeta/DESIGN.md](../regmeta/DESIGN.md) § "Value sets are
year-projected").

## Classification seed

The `classification_id` FK on `variable_instance` is populated at build
time from a maintainer-curated TOML seed at
`regmeta_build/classifications.toml`. Each entry declares a normalized
classification and lists the raw `vardemangdsversion` strings that map
to it — exact match, no fuzzy inference. Match strings are deterministic
and auditable: any maintainer can enumerate them via `SELECT DISTINCT
vardemangdsversion FROM variable_instance`.

Build-time invariants (violations fail `regmeta-build build-db` loudly,
exit 10):

- Every seed `vardemangdsversion` string must match at least one instance.
- Every classification must resolve to at least one tagged instance and
  at least one value code.
- A given `vardemangdsversion` string may belong to at most one
  classification.
- Every `supersedes` reference must resolve to a declared `short_name`.
- Every `valid_codes_file`, when present, must resolve to a CSV under the
  classifications directory with header `vardekod,vardebenamning`.

### Canonical code CSVs

A seed entry's optional `valid_codes_file` points at a CSV under
`regmeta_build/input_data/classifications/` (header
`vardekod,vardebenamning`). At build time:

- Every CSV code is ensured to exist in `value_code` (canonical-but-
  unobserved codes get a fresh row with no `value_set_member` linkage).
- Every `classification_code` row in that classification is marked
  `is_valid=1` (canonical) or `is_valid=0` (observed-only).
- `classification.valid_code_count` caches the canonical count; it is
  `NULL` for classifications without a CSV.

The CLI surface (`get classification --codes --only-valid`,
`is_valid` in JSON output) is documented in
[../regmeta/DESIGN.md](../regmeta/DESIGN.md) § "Canonical vs observed
codes". See also [CLASSIFICATIONS.md](CLASSIFICATIONS.md) for the
per-classification extraction recipes that produced the shipped CSVs.

The seed lives in the repo (alongside `DESIGN.md`) and is **not** bundled
in any wheel — same status as `regmeta_build/docs/`. End users receive
the already-populated classification tables via the prebuilt DB asset.

## Doc-DB build

`regmeta-build build-docs` is the maintainer-only command that rebuilds
the doc DB from a repo checkout of `regmeta_build/docs/` before upload.
The build:

1. Walks the curated markdown tree.
2. Parses Obsidian frontmatter (`parse_frontmatter`).
3. Cleans inline markdown noise for FTS indexing
   (`_clean_body_for_search`).
4. Writes rows into the `DOC_DDL` schema with `DOC_SCHEMA_VERSION` in
   `doc_meta`.
5. Builds the FTS5 indexes and seals the file.

The doc-DB schema constants (`DOC_*`, `open_doc_db`, `ensure_doc_db`)
stay in `regmeta` so the wheel can read the doc DB at runtime without
pulling the builder. `repo_docs_dir()` is part of `regmeta_build.doc_db`
and is only reachable from the builder package.
