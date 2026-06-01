# Design: reg_meta_build

Design rationale and constraints for the build pipeline. For usage, see
`reg-meta-build --help`. For query-layer rationale (the data model end
users see), see [../reg_meta/DESIGN.md](../reg_meta/DESIGN.md).

## Scope

`reg_meta_build` owns the build pipeline that produces the SQLite databases
`reg_meta` queries against. Specifically:

- `reg_meta.db` — main metadata DB (~520 MB uncompressed). Built from SCB
  source CSVs under `reg_meta_build/input_data/`, classifications seed at
  `reg_meta_build/classifications.toml`, and curated slug TOMLs under
  `reg_meta_build/fqid_slugs/`. Validated by `reg_meta_build/validate.py`
  before shipping.
- `reg_meta_docs.db` — FTS5 search index over the curated markdown under
  `reg_meta_build/docs/`.

Both DBs ship as `.zst`-compressed GitHub Release assets parallel to the
`reg_meta` PyPI package (release-skill orchestrates this).

## Why split from `reg_meta`

The query side (`reg_meta`) needs only the sqlite3 stdlib. The build side
pulls openpyxl, owns large maintainer-edited input data, and runs on a
different cadence (most users never run a build). Separating the two:

- Keeps the `reg_meta` wheel small and dep-light for end users.
- Lets the two release on independent tags (`reg_meta/v*`,
  `reg_meta_build/v*`).
- Mirrors the build/runtime separation needed for a future Go/Rust
  port of the query layer.

See `REFACTOR_SPEC.md` §4 *Why this split* and §15 step 2.

## Dependency direction

`reg_meta_build → reg_meta` only. The builder imports query helpers
(`open_db`, `default_db_dir`, `DB_FILENAME`, `SCHEMA_VERSION`,
`derive_variable_slug`, etc.) but `reg_meta` never imports
`reg_meta_build`. The schema contract — the set of constants and
helpers both packages agree on — lives in `reg_meta`.

## What lives where

| Module                              | Package         |
| ----------------------------------- | --------------- |
| `db.py` (DDL, build_db, CSV import) | `reg_meta_build` |
| `db.py` (open_db, schema constants) | `reg_meta`       |
| `doc_db.py` (build_doc_db)          | `reg_meta_build` |
| `doc_db.py` (open_doc_db, ensure)   | `reg_meta`       |
| `cli.py` (build / docs-build / slug commands) | `reg_meta_build` |
| `cli.py` (query, update, info, docs) | `reg_meta`       |
| `fqid_slugs.py`                     | `reg_meta_build` |
| `classifications.py`                | `reg_meta_build` |
| `validate.py`                       | `reg_meta_build` |
| `dbdiff.py` (content diff harness)  | `reg_meta_build` |
| `sources/` (provider CSV parsers)   | `reg_meta_build` |
| `fqid.py`, `catalog.py`, `queries.py`, `doc_queries.py`, `errors.py`, `update.py`, `download.py` | `reg_meta` |

## CLI shape

Top-level commands (no `maintain` subgroup; that group is dissolved):

```text
reg-meta-build build-db [--validate] [--skip-slugs] ...
reg-meta-build build-docs ...
reg-meta-build seed-slugs [--scb] ...
reg-meta-build precheck-slugs ...
reg-meta-build parse-sos ...
```

The matching `reg-meta maintain *` forms are removed. `reg-meta maintain
update` / `info` are promoted to top-level `reg-meta update` / `reg_meta
info` (query-side concerns — fetching/inspecting prebuilt DBs).

## Content diff harness (`dbdiff`)

`dbdiff.py` compares two `reg_meta.db` files by **content**, not bytes.
It is the acceptance gate for the A4.1 adapter refactor (and any future
"the rebuild should be identical" change): rebuild the DB, then diff the
new file against a preserved baseline. Raw byte comparison is useless
here — two SQLite files with identical rows differ byte-wise (page
layout, freelist, vacuum generation, FTS index segment order), so the
check has to be order-independent and storage-aware.

- **Schema compare**: same tables, and per table the same columns
  (name/type/order/NOT NULL/default/PK) and indexes (named + auto).
- **Content compare**: per user table, row count plus an
  order-independent *multiset* fingerprint — each row canonicalized to a
  type-tagged, length-prefixed byte string (NULL-aware, BLOB-as-bytes),
  hashed with BLAKE2b-128, the per-row hashes **summed** mod 2¹²⁸.
  Summation (not XOR) so duplicate/missing rows can't cancel. The pass is
  O(n) streaming / O(1) memory, so the 5.7M-row `value_set_member` table
  costs seconds, not a 330 MB load.
- **Ignore set** — deliberately minimal. Only `import_manifest.import_date`
  (wall-clock) is dropped by default; `schema_version`, `source_checksums`,
  `row_counts`, and every content/ID column are compared. `input_dir` (a
  path) is *not* ignored — it is stable for a same-machine rebuild; a
  cross-machine diff can add it explicitly.
- **FTS5**: the `*_fts` virtual tables are external-content (a projection
  of base tables, which *are* compared) and their shadow tables hold an
  insert-order-dependent serialized index. Both are schema-compared but
  content-excluded, so an emit-order change that leaves content identical
  does not false-positive.
- **Mismatch output**: names the table + count delta and dumps the first
  N differing rows per direction, found via an ATTACH-based
  multiset-difference query (`SUM(+1/-1) GROUP BY all-columns`) that
  offloads its working set to SQLite's temp store.

Standalone and read-only (`mode=ro` URIs); does not import the build
pipeline. Importable as `reg_meta_build.dbdiff.diff_db_content(...)` and
runnable as `python -m reg_meta_build.dbdiff <db_a> <db_b>` (exit 0
identical / 1 differs / 2 error). The full rationale lives in the module
docstring.

## Source parsers

Provider-specific logic lives under
`reg_meta_build/src/reg_meta_build/sources/`:

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
[../reg_meta/DESIGN.md](../reg_meta/DESIGN.md) § "Composite registers and
source tracking").

## Consumer-side lineage (`variable_state_lineage`, §5.6)

Composite registers (LISA, RAMS, …) re-deliver variables sourced from base
registers (RTB, FTB, …). `link_variable_state_lineage` materializes that
consumer→source link as **state-pair interval-overlap edges**: for each
consumer `variable_state` whose `variable.source_register_id` points at a
different register, it finds the matching source state(s) and emits one edge
per pair whose validity ranges intersect, with `(valid_from, valid_to)` set to
the intersection. A few non-obvious choices:

- **Interval-overlap, not slug equality.** v0.11 keyed lineage on slug-folded
  period and picked the source variant non-deterministically (`MIN(cvid)`).
  The interval join produces the same answer for the trivial year-equal case
  but also expresses real cross-state lineage (a consumer era sourcing from a
  pre-rename source state, then a post-rename one) at no runtime cost.
- **Source-side matching is a multi-seed `same_as` BFS.** The consumer's slug
  identifies the source variable by identity (LISA `kon` → RTB `kon`, no curated
  edge needed). `_variable_set_via_same_as` then BFS-expands from *two* seeds:
  the source-register identity node (picking up within-source renames like RTB
  `kon` ↔ `kon-v2`) and the consumer node (picking up any curated cross-register
  / cross-provider `variable_same_as` edge whose endpoints have *different*
  slugs, LISA `foo` ↔ RTB `bar`, §5.5). The common no-rename case yields just
  the identity slug, so an edge is always additive. (An earlier single-seed form
  expanded only the source node and silently missed mismatched-slug
  cross-register edges — latent while `variable_same_as` is empty, fixed in the
  A2.4 review.)
- **Variant pinning is TOML-only — no SQL table.** A `[lineage_defaults]`
  block picks one source variant per source register; a
  `[lineage."<consumer_register>.<variable_slug>"]` block overrides it per
  consumer variable. Uncurated consumers fall back to *all* source variants
  carrying a matching state plus an `ambiguous_source_variant` warning; a
  consumer with no source state at all gets `no_source_state`. A found-but-
  non-overlapping source state is neither — it is a legitimate empty result
  (zero edges, zero warnings). Warnings land in `variable_state_lineage_warning`
  and the build log for curator attention. `load_lineage_config` does shape
  validation only; existence of the named registers/variants is validated by
  the linker against the DB (fail-fast on a pin to a non-existent variant or a
  `source_register` that contradicts the variable's resolved source register).

`link_variable_state_lineage` is the sole lineage linker. (Through A2.6 it
ran **alongside** the old slug-only `link_consumer_side_bindings`, which set
`variable_instance.via_source_id` for `reg_meta`'s interim resolver; A2.7
dropped that old linker, the `via_source_id` column, and `variable_instance`
itself, leaving this the only linker.)

## Vardemängder sentinel filtering

`Vardemangder.csv` ships a row for every variable, including those with no
enumerated code list. SCB encodes "no codes" by stuffing a placeholder string
into `Värdekod` so that `Värdekod == Värdemängdsversion` (and typically
`Värdemängdsnivå`). Two disjoint cases occur with this shape, classified by
two allowlists in `reg_meta_build/db.py`:

`_VARDEMANGDER_SENTINELS` — placeholder strings that mean "no enumerated
code list." Not real value codes; dropped silently.

| Värdekod | Meaning |
|---|---|
| `Tal` | Numeric variable |
| `Beskrivande text` | Free-form text variable |

Importing sentinels would pollute `value_code` with
rows that are never valid lookups, and write the placeholder into
`variable_instance.{value_set_version_label,vardemangdsniva}` where
downstream consumers would mistake it for a real classification label.
The authoritative type signal is `variable_instance.data_type` — the
placeholder adds nothing and is sometimes misleading (e.g. cvid 207
`DatInv` is `data_type='int'` but tagged `Beskrivande text`).

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
`value_set_version_label` / `vardemangdsniva` on `variable_instance`. Fully-empty
rows (kod, label, item all empty) are dropped silently.

### Drift detection

A `kod == version` row where kod is in neither allowlist is treated as drift
and fails the build with `RegMetaError(code="vardemangder_drift", exit 10)`.
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
[../reg_meta/DESIGN.md](../reg_meta/DESIGN.md) § "Value sets are
year-projected").

## Classification seed

The `classification_id` FK is populated at build time from a
maintainer-curated TOML seed at `reg_meta_build/classifications.toml`.
Each entry declares a normalized classification and lists the raw
`value_set_version_label` strings (the SCB-published "Vardemangdsversion"
labels) that map to it — exact match, no fuzzy inference. Match strings are
deterministic and auditable: any maintainer can enumerate them via
`SELECT DISTINCT value_set_version_label FROM variable_instance`. The build
tags `variable_instance.classification_id` first; A2.7's
`_backfill_state_classifications` then projects it onto the **shipped**
`variable_state.classification_id` (per-era, attributed to the owning split
sibling) before `variable_instance` is dropped.

Build-time invariants (violations fail `reg-meta-build build-db` loudly,
exit 10):

- Every seed `value_set_version_label` string must match at least one instance.
- Every classification must resolve to at least one tagged instance and
  at least one value code.
- A given `value_set_version_label` string may belong to at most one
  classification.
- Every `supersedes` reference must resolve to a declared `short_name`.
- Every `valid_codes_file`, when present, must resolve to a CSV under the
  classifications directory with header `vardekod,vardebenamning`.

### Canonical code CSVs

A seed entry's optional `valid_codes_file` points at a CSV under
`reg_meta_build/input_data/classifications/` (header
`vardekod,vardebenamning`). At build time:

- Every CSV code is ensured to exist in `value_code` (canonical-but-
  unobserved codes get a fresh row with no `value_set_member` linkage).
- Every `classification_code` row in that classification is marked
  `is_valid=1` (canonical) or `is_valid=0` (observed-only).
- `classification.valid_code_count` caches the canonical count; it is
  `NULL` for classifications without a CSV.

The CLI surface (`get classification --codes --only-valid`,
`is_valid` in JSON output) is documented in
[../reg_meta/DESIGN.md](../reg_meta/DESIGN.md) § "Canonical vs observed
codes". See also [CLASSIFICATIONS.md](CLASSIFICATIONS.md) for the
per-classification extraction recipes that produced the shipped CSVs.

The seed lives in the repo (alongside `DESIGN.md`) and is **not** bundled
in any wheel — same status as `reg_meta_build/docs/`. End users receive
the already-populated classification tables via the prebuilt DB asset.

## Doc-DB build

`reg-meta-build build-docs` is the maintainer-only command that rebuilds
the doc DB from a repo checkout of `reg_meta_build/docs/` before upload.
The build:

1. Walks the curated markdown tree.
2. Parses Obsidian frontmatter (`parse_frontmatter`).
3. Cleans inline markdown noise for FTS indexing
   (`_clean_body_for_search`).
4. Writes rows into the `DOC_DDL` schema with `DOC_SCHEMA_VERSION` in
   `doc_meta`.
5. Builds the FTS5 indexes and seals the file.

The doc-DB schema constants (`DOC_*`, `open_doc_db`, `ensure_doc_db`)
stay in `reg_meta` so the wheel can read the doc DB at runtime without
pulling the builder. `repo_docs_dir()` is part of `reg_meta_build.doc_db`
and is only reachable from the builder package.
