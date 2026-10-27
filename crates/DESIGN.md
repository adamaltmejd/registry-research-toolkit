# Design: the Rust runtime

Design rationale and constraints for the catalog runtime: `reg-core` (grammars, folds,
interval algebra, the project contract), `reg-catalog` (the reader and its operation
set) and `reg-meta` (the `serve` and `mcp` run modes), plus the design of the builder
that replaces `reg_meta_build` ("The catalog builder"). The project contract has its own
note, [reg-core/DESIGN.md](reg-core/DESIGN.md). The artifact these crates read is built
by `reg_meta_build` ([../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md)), which
also owns the resolver, holdings compilation and the inventory TOML. Cross-package
topology is in [../ARCHITECTURE.md](../ARCHITECTURE.md).

The operation names, parameters, result shapes and routes are
`conformance/api/operations.toml`; error codes with their classes and HTTP statuses are
`conformance/api/errors.toml`. This file does not repeat them; it says why the model is
shaped the way it is.

## Why a compiled catalog and a Rust runtime

Measured on the Python reader it replaced (0.41.0 catalog, warm cache): every CLI call
paid about 250 ms of Python start-up, and search took 650–860 ms, 70–93% of it inside
SQLite, over about 1,600 `execute` calls and 139k calls into a Python fold UDF. One
listing took 72 s in a single badly planned statement. Two conclusions shape the
runtime:

- **Query cost is a query-design problem, not a language one.** A Rust reader runs the
  same SQLite engine. The fix is to move every pure function of the artifact into the
  build (`reg_meta_build/DESIGN.md`, "Base and derive"), which also keeps the reader
  small.
- **Start-up does not matter**, because the runtime runs as long-lived processes
  (`serve`, `mcp`). The case for Rust is one implementation of each shared contract
  across build, server and browser, typed contracts, and memory use.

The HTTP server is Rust rather than a Python server calling Rust through bindings. With
bindings every result type would exist twice, as a Rust struct and as a Pydantic model
for OpenAPI, which is the drift this design removes. Most of the old backend was catalog
logic (node assembly, search ranking, semantic validation) that belongs in the operation
set, where agents get it too. The deploy image is one static binary plus the baked DBs
(the 1 GB Fly VM is kept for the page cache), and local MCP needs no Python environment.

## One implementation per contract

Each contract that crosses a language or package boundary has exactly one
implementation. New code that needs one of these calls its home; it does not add a
second copy held in step by a parity test.

  | Contract                                | Single home                      | Reached from                                          |
  | --------------------------------------- | -------------------------------- | ----------------------------------------------------- |
  | Artifact schema (DDL, manifest)         | `reg_meta_build` (Python)        | The reader reads it, behind its schema gate           |
  | Resolver semantics                      | `reg_meta_build` derive (Python) | Compiled into the artifact                            |
  | FQID and period grammar                 | `reg-core`                       | Build via `reg-core-py`; SPA via `reg-core-wasm`      |
  | Interval algebra over periods           | `reg-core`                       | Build via `reg-core-py`; reader; SPA via WASM         |
  | Text folds                              | `reg-core`                       | Build via `reg-core-py`; reader; SPA via WASM         |
  | Canonical JSON + SHA-256 (build hashes) | `reg_meta_build` (Python)        | Build only                                            |
  | Project schema + structural validator   | `reg-core` (`project::check`)    | Server and MCP; SPA via WASM; JSON Schema export      |
  | Result types                            | Rust structs (serde + utoipa)    | OpenAPI; MCP tool schemas and TS types derive from it |

The SPA never parses period grammar or computes a project period's bounds: it composes
`reg-core`'s answers ([../reg_webapp/DESIGN.md](../reg_webapp/DESIGN.md)), including the
interval merge that fuses delivery windows in `catalog.ts`. Its type-to-filter fold is
`fold_search` through the same module.

The build reaches `reg-core` through the compiled `reg-core-py` extension, which
`resolve_code_sha256` (`reg_meta_build/src/reg_meta_build/resolve_code.py`) hashes. A
`reg-core` change, an interval or grammar fix included, therefore invalidates every
build resolve key: the next real-seed build resolves in full and its dbdiff shows any
artifact change.

**Text folds.** Two named folds, specified rather than inherited from a library:

- `fold_identity`: Unicode lowercase with the Final_Sigma context rule, no normalization
  (`str::to_lowercase`; Python's `str.lower()` agrees). It is the column identity rule.
- `fold_search`: full case folding, NFKD, drop characters with a nonzero canonical
  combining class, repeated until the text stops changing; then split on whitespace and
  join with single spaces. It is idempotent. Each pass maps characters independently, so
  two passes change any text at most and a third confirms; reaching the cap is a bug. A
  single pass is not idempotent: NFKD re-introduces capitals after the case fold (U+1D2C
  → `A`), and a spacing mark decomposes to a space after the strip (U+00A8).
- Helper classes follow Python, not Rust's std: whitespace is `White_Space` plus
  U+001C..U+001F; "alphanumeric" is general category L\* or N\*, not Rust's `Alphabetic`
  property (which admits marks and symbols Python's `[^\W_]` rejects).

The build fills folded columns through `reg-core-py`, with the code the reader applies
to the query string. All of `reg-core` uses one Unicode version
(`tests/unicode_version.rs`). The oracle is `conformance/cases/folds/`.

**Canonical JSON** has no floats. A float in a hashed payload is a build error.

**Messages are structured.** Errors and blocked-order findings carry a code and fields;
prose is rendered from templates. Goldens assert codes and fields, so no language's
`repr` quoting appears in a contract.

## Crates and run modes

```text
reg_meta_build (Python): base build → derive        uses reg-core-py
                 │
                 ▼
reg_meta.db (+ reg_meta_docs.db), immutable
                 │
reg-catalog: one operation set
        ┌────────┴─────────┐
reg-meta serve        reg-meta mcp
HTTP API (SPA)        MCP over stdio (local catalog)
+ /mcp (hosted)
        │
SPA (React) + reg-core-wasm
```

- `reg-core`: no IO. FQID and period grammar, folds, interval algebra, project-data
  types and the structural validator, error codes. Compiles natively, as a Python
  extension (`reg-core-py`, PyO3 via maturin) and to WASM (`reg-core-wasm`).
- `reg-catalog`: the reader. Opens and admits an artifact, owns scope as reader state,
  and defines the operation set: one function per operation, typed parameters and
  results with JSON Schemas. Search (one ranking), order materialization and project
  semantic validation live here.
- `reg-meta`: the binary, with run modes only. `serve` is the HTTP API (axum + utoipa;
  early `ETag` revalidation, body-size limit and rate limit as tower layers) plus the
  remote MCP endpoint over streamable HTTP. `mcp` is MCP over stdio against a local
  catalog, for offline use and private steward catalogs. There is no query CLI.
- `reg-core-py`: the PyO3 module the build imports. Its uv `cache-keys` cover the Rust
  sources, because uv otherwise rebuilds a path dependency only on a `pyproject.toml`
  change.
- `reg-core-wasm`: thin `wasm-bindgen` glue for the SPA. JSON text crosses the boundary
  (no `serde-wasm-bindgen`), and every export is total: it runs the matching structural
  check before a `reg-core` function that can panic. It is built with
  `wasm-bindgen-cli --target web`, the CLI pinned to its `Cargo.lock` version, and the
  `[profile.wasm]` (`opt-level = "s"`, LTO, one codegen unit); no wasm-pack, wasm-opt or
  Vite plugin. The CLI drops the name and producers sections (about 5% gzipped; a trap's
  stack trace then reads `wasm-function[N]`). Measured and declined in #1427:
  `opt-level = "z"` (2% smaller gzipped once names are dropped, a third slower on the
  fold), `panic = "abort"` (wasm32 already aborts) and `wasm-opt -Oz` (6% gzipped, too
  little for another pinned tool). The gzipped `.wasm` has a 200 KB budget
  (`WASM_GZ_BUDGET` in `scripts/gate.py`, checked by the `frontend` step); raising it is
  a maintainer decision.

Each crate's `Cargo.toml` is its dependency list. Adding a runtime crate is a design
decision, not an implementation detail. `toml` is a runtime dependency only of
`reg-readers`, which reads curation for the builder; the runtime keeps it a dev
dependency.

**Distribution.** The server image is one static binary plus the baked DBs, fetched in
the Dockerfile with `curl`, SHA-256 verification against GitHub's asset digests, and
`zstd`. Each `reg_meta/v*` release ships `reg-meta` binaries for macOS arm64 and Linux
x86_64 with SHA-256 checksums, used only for local `reg-meta mcp`; the catalog is
downloaded with `curl` and `zstd`. There is no PyPI package and no `fetch` run mode.
Windows agents use hosted MCP; a local Windows binary is added only if it builds and
runs without special effort. The `microdata-tools-se` plugin declares one hosted MCP
server whose URL is a user setting: the global catalog by default, a hosted steward
catalog (`https://data.swecov.se/mcp`) instead. A local `reg-meta mcp` is added to the
agent's MCP configuration by hand.

## The catalog builder

The builder that replaces `reg_meta_build` is being written beside it, as new crates
(`reg-frame`, `reg-readers`, `reg-resolve`, `reg-build`). [../REBUILD.md](../REBUILD.md)
tracks the milestones. This section holds the design decisions that outlive the rebuild.

**Three stages, each seeing only its inputs.**

1. **Read.** A reader turns one pinned input into a *source frame*. The frame has one
   record shape for every provider: registers, tables, stated deliveries (releases),
   documented columns (occurrences, each with a period), value sets and source-side
   links. Every fact keeps a locator back to its input. Readers are the only code that
   names a provider or a file format. Their config (header aliases, type tables, the
   delivery-name grammar) is data under `curation/readers/`.
2. **Resolve.** One register's frame plus that register's curation becomes a resolved
   register: variables, segments, coverage, findings and provenance. Resolve is a pure
   function per register, run in parallel and cached by content hash. It never sees
   another register's frame.
3. **Link and emit.** Cross-register links are checked once over all resolved registers.
   The emitter writes one SQLite file, documents included, and decides nothing.

**Change findings (F1).** Resolve reports what changed in a variable's documentation as
`notice` findings, one per (variable, sub-kind, table or none, first day of the new
form), on that day. Neighbours are compared: an `ambiguous` segment is skipped and a gap
is not a change.

- `F1:type-change`: dtype or length differs between consecutive segments of one table.
- `F1:domain-change`: the value set differs (NULL included), unless either side is
  uncoded. The message counts codes added, removed and relabelled.
- `F1:label-drift`, `F1:definition-change`, `F1:unit-change`: consecutive text runs
  differ. The definition message names the field, never the text.
- `F1:population-change`: the primary releases carrying the variable, in period order,
  state different population names. Undated releases and releases without populations
  are skipped.

V2 (value-set carry across a segment that states none) is a v4 rule not built: on the
first full build 7 coded segments of 166k sat between two neighbours with one value set,
spelling and type. A D `set` states those.

**Time.**

- Periods are half-open day intervals with a display grain: year, half, quarter, month,
  day, span or `none`.
- Readers convert inclusive source ends. An unstated end stays open.
- A timeless table has one `none`-grain period, addressed as `_default`.
- Releases are optional and never synthesized. A release has an edition (`final`,
  `preliminary` or `unstated`) and a free-text qualifier, both parsed from its name by
  the reader's grammar.
- A name the grammar cannot place is `unparsed`: a finding, with no period.
- Within one period, the final edition is preferred over the preliminary one.
  Overlapping deliveries of different periods cover their union; neither is preferred.

**Identity.**

- Each occurrence carries an opaque *source key*: the provider's variable id where one
  exists, otherwise the folded column name, scoped to the register.
- Occurrences with one key in one register are one variable. The exception is a key that
  covers two folded spellings in overlapping periods anywhere in the register. The
  comparison uses the preferred edition of each period, so a preliminary and a final
  delivery of one year never split a key. That key splits by spelling.
- Case never distinguishes columns.
- A variable has at most one spelling per table at a time.
- A shared name under different keys is a review proposal, never a merge.
- A column the source cannot place on one variable is ambiguous. Orders over that window
  are blocked; nothing is guessed.
- Variables never span registers. Cross-register relations are `continues` and
  `derived-from` links, identifier kinds, and tags.

**Three identifiers.**

- **Source key:** evidence only.
- **Public id** (`provider/register/variable`): stable through a per-register ledger
  (`ids.toml`) that maps each id to an anchor occurrence, so ids survive renames and
  splits. The first ledger is generated from today's slug pins. A release build refuses
  a variable without an id.
- **Row id:** renumbered every build and never exposed.

**Curation.**

- Six statement kinds: `bind`, `assert`, `assign`, `set`, `link` and `ack`.
- Each statement's premise is recorded in a generated `curation.lock`. When the source
  changes under a statement, the build refuses it at its file and line.
- Every published value has one provenance: source, rule or curated, with curated over
  rule over source. Losing values are kept as evidence.
- Rules never merge across keys, bind classifications or enrich; those are always
  curated.

**Value sets, populations and series.**

- Value sets are content-addressed.
- Per-code validity dates are applied at read time, keeping the codes valid in each
  occurrence's period, and are not published.
- Populations are not a segment dimension. A value set that differs by population is
  kept as population-labelled alternatives, with one review finding per key.
- There is no series entity. Each column of a wide series is a variable. A curated `set`
  groups related columns within a register for picking.
- How a series or a table sits in delivered files is the holdings layer's job. That
  layer is separate from the catalog and reads it by public id.

**Agnosticism is enforced.**

- Only `reg-readers` may name a provider or a format. A lint step in the gate scans the
  other builder crates for provider vocabulary and fails on any hit.
- A new provider is a reader or a transcription, plus curation in the existing kinds.

**Storage and determinism.**

- The published catalog is SQLite.
- Build state is Rust structures plus a content-addressed file cache.
- Intermediates are canonical zstd JSONL.
- No DuckDB, no dataframe engine, no Python bridge.
- Iteration is sorted, row ids follow natural-key order, and nothing reads the clock or
  the host.
- CI builds twice and compares bytes.
- The period grammar, interval algebra and text folds come from `reg-core`, as
  everything else does ("One implementation per contract").

**Decisions taken in milestone A.**

- **Days.** Readers, frames and resolve work in inclusive ISO days through `reg-core`'s
  interval algebra; the emitter writes half-open day numbers. Open time is
  `0001-01-01..9999-12-31`, so `open_end` is the day after `9999-12-31`.
- **Unstated bounds.** An occurrence's unstated bound is its table's stated bound, else
  the register's, else open. The T1 clamp cuts only a stated bound that reaches past
  them, so T1 records what a source claimed, never what it left silent. A table with no
  stated bound and no dated occurrence is timeless, and the register's coverage is the
  hull of its dated tables. An occurrence wholly outside its table's coverage is
  `rejected`, and a variable left with no used occurrence is withheld with its T1
  notices, since a variable needs a segment. An occurrence documented to end before it
  starts is refused, located at its source row.
- **Segments hold whole occurrences.** The variable's overlapping occurrences in one
  table form one segment; adjacent ones of one form join. A segment is ambiguous when
  its occurrences disagree on label, type or value set, or when another variable claims
  its folded spelling there. Labels compare ignoring case and spacing.
- **The implicit table** takes its register's id, so `ident` holds that id once, as the
  register's, and the namespace check covers explicit tables only.
- **The ledger** is `curation/registers/<provider>/<register>/ids.toml`: `[tables]` maps
  a table handle to its id, `[variables]` an id to anchors `<table>: <spelling>` (a bare
  spelling names the implicit table), matched by fold. An anchor may end in `@ <day>`:
  it then reaches only the variables placed at that spelling on that day. A variable no
  anchor reaches gets a proposed id from its fold, clear of the register's table ids.
  When two ids reach one variable, the first in id order wins and the other is reported.
- **Inputs.** `curation/inputs.toml` lists the pinned inputs, each with its reader and,
  for readers that need them, provider and register. A reader's config is
  `curation/readers/<reader>.toml`. An input row covers every file its reader read.
- **The transcription format** (`reg-readers/src/transcription.rs`) is how a provider
  without a machine-readable export enters: per provider, `registers.csv`, optional
  `tables.csv`, one CSV per register with one row per column, and `codes/` lists.
- **The fold** is `reg-core`'s `fold_identity` of the trimmed name; the v4 schema says
  NFC. The A inputs hold no string that NFC changes, so the question stays open for B.

**Decisions taken in milestone B.**

- **Editions.** Among a table's releases of one period and qualifier, the primary is
  final, then unstated, then preliminary. Two of the best edition are a collision (F8):
  the first by release handle stays primary, since the schema keeps one primary per
  period, and the twin is neither preferred nor preliminary, so its own columns stay
  `clear`. An occurrence of a non-primary release is `shadowed` where the primary
  documents the same key, under any spelling; otherwise it is used, and a segment made
  only of worse-edition occurrences is `preliminary`. The split test runs over used
  occurrences after shadowing, so a preliminary and a final edition of one period never
  split a key, while two worse editions spelling a key the primary lacks do. A
  non-primary release whose spellings or types for a key differ from the primary's is
  F7. An unparsed release (F9) leaves its occurrences `unplaced`.
- **Gap markers.** A row documenting a key in a release without a column attaches to the
  key's variable when the key is one variable. A key that never has a column is no
  variable: one F5 notice per key.
- **Gaps (T5)** are computed only in tables with a placed primary release. A table's
  cadence is its stated one, else the greatest common step, in months, between its
  primary releases' starts when each start is a whole number of months after the one
  before on the same day (or month end); an inferred cadence is recorded on the table
  (`irregular` for a step with no name). With a cadence, every column, release and
  marker covers its whole step from its start, so a reference-day stock table released
  yearly has no gap between deliveries and a gap is a missing step. Without one, gaps
  are the holes between stated periods. A day between the table's used columns and
  primary releases that no used column covers is a table gap: `release_without_column`
  under a primary release that delivers no column, else `no_release`. Inside a
  variable's hull in that table, an uncovered day under a marker is
  `documented_without_column` and under a primary release `release_without_column`; a
  day no release covers is stored once, as the table's gap. Reader contract: a
  variable's gaps are its own plus its table's gaps inside its span.
- **Unplaced wins.** A column-less row of an unparsed release is `unplaced`, not a gap
  marker: with no period it marks no gap.
- **SCB handles.** Source keys are `v<VarId>`, release handles `r<RegVerID>`, table
  handles the variant's name (unique per register in the export). A register's handle is
  its source coordinate (`r<RegisterId>`): ledgers, links, locators and reader config
  key on it. Its public name, the segment in every published id, is the ledger's
  top-level `public` (a slug, unique among the provider's public names and handles),
  defaulting to the handle; it is neither a D `bind` statement nor reader config (v4
  §2.3: the public id is stable through the ledger; §7 allows ledger entries; audit
  B-1). Rows that differ only by population are one occurrence with every row as a
  locator; the release lists its populations, with a reference day when one date is
  stated. A label, definition, operational definition, unit, type or length the rows
  disagree on keeps the first row's as provisional and names the field in the
  occurrence's `conflicts`, which resolve reports as F6 (source conflict), once per
  variable.
- **NFC.** CP1252 decodes to precomposed characters only, so the fold needs no
  normalization step for the SCB export.
- **SCB value sets.** A row's `CVID` names its value set in `Vardemangder.csv`. Items
  are interned by `ItemId` and text: the export reuses one `ItemId` for different codes,
  even within one `CVID` (a third of its ids carry more than one text). Rows without an
  `ItemId` are told apart by their text and are undated. `VardemangderValidDates`
  applies only the end of a code's validity (D30: at read time only; B2 ruling,
  2026-10-27). An item drops from a placed release's set only when it has dated
  intervals (all of its `ItemId`'s), none open, and the latest `ValidTo` is before the
  release's first day (ISO days, `ValidTo` inclusive). `ValidFrom` is ignored: in the B2
  sample it reads as the day the metadata was entered (a 1996 survey's codes "valid
  from" 2008), and every wrong drop was a code dated only after the release, while every
  right drop was a code whose validity ended before it. Undated items stay. The cost is
  that a code introduced after a release but listed in an older set is kept for that
  release, a superset of what the source stated. A set left empty by its dates stands
  whole and is counted in one warning. The value set is the content of the items kept,
  so CVIDs with equal content are one value set, named by every `Värdemängdsversion` its
  items carry. A code repeated across levels keeps its first item, counted in one
  warning. A CVID whose every item has version, level and code all equal to one of the
  config's `uncoded` names (`Tal`, `Beskrivande text`) marks the column as not coded
  when every row's CVID does; otherwise such a population simply has no value set. Rows
  naming different value sets give the occurrence its sets per population, and a
  population whose rows disagree gets each as an alternative; resolve reports either as
  F6. A row's "no codes" beside another row's set is not an alternative (the set
  stands), and a set named by a row without a population is left out with a warning.
- **SCB flags.** `UnikaRegisterOchVariabler` states a column's flags by register,
  variant, variable and column name (register names are one-to-one with `RegisterId`);
  they apply to every occurrence of that column, whatever its release (its version range
  is not used), and a column stated twice is flagged if either statement flags it. A
  column is sensitive when it is always or sometimes so (`KansligVariabel`,
  `KansligVariabelIbland`); a stated `0` is a statement, an unstated flag stays unknown.
  `Identifierare` makes every occurrence of a listed `VarId` an identifier, beside the
  summary's own identifiers. In any provider, a segment is sensitive or an identifier if
  any occurrence covering it says so, and an identifier segment is a row key;
  under-flagging is the dangerous direction. Occurrences that state a flag differently
  raise one F6 per key.
- **SCB `Timeseries` is read in C.** Its rows are series breaks, ends and replacements
  of registers, variants, releases and variables: links and document references, which
  no B table holds. `Entitet` fixes what `ID1` and `ID2` are (`Register` a `RegisterId`,
  `RegisterVariant` a `RegVarID`, `RegisterVersion` a `RegVerID`, `Variabel` a `VarId`;
  each row's `Namn` is `ID1`'s name). `Tidsseriebrott` and `Avslutad` are a notice on
  `ID1`'s entity, `<Handelse>: <Beskrivning>`. `Ersätter` (`ID1` takes over from `ID2`)
  and `Ersatt av` (`ID2` from `ID1`) are a `continues` link from the newer entity, and
  come in mirrored pairs the link stage folds into one relation; between releases the
  succession is a notice on the newer one, `continues <older name>`. A `VarId` stands
  for each register that documents it. The config's `[timeseries]` holds this map;
  `AktuellVariabel` (its ids name no entity of the export), `Population`, `PopContext`
  and `VS_Class` are left out and counted, and `FilID` (a document reference, on every
  row) is left to the documents. A row's `VariabelRegister_Källa` is a `derived_from`
  link to the register of that name.
- **The LISA workbook** is its own reader (`lisa`), not the Socialstyrelsen template's:
  a sheet is a table named by the sheet, a data row an occurrence keyed by its folded
  column name, a year cell one occurrence per year or range, and `Känslig1` the
  sensitivity flag ("I vissa fall" is sensitive, like SCB's `KansligVariabelIbland`).
  Through C its rows stay a register of their own, `scb/lisa-workbook` from the
  manifest's `register`: attaching its sheets to the export's LISA tables, by period
  where needed, is D's `bind`, since a workbook column keyed by name beside the export's
  `v<VarId>` would otherwise be a rival or a second variable. Section headings (group
  hints) wait for D's sets. Each `Grundregister` cell is a `derived_from` link from the
  row's variable to each register it names (split at `,`, `;` and line breaks), matched
  by name, so LISA's abbreviations (`RTB`) stay unresolved until D's `bind`. `Individ`
  repeats the year-independent columns with blank year cells; the year-less sheet's row
  stands, so `Individ` keeps its 1990– span. `Individ årsoberoende` holds three tables
  (birth country, deaths, migration) under headings, each keyed by `PersonNr`; the
  reader keeps the sheet as one table and splitting it is D's `bind`. Labels keep their
  footnote markers (`…yrkesverksamma7`) verbatim: a trailing digit cannot be told from a
  name's own (`SNI92`), and the footnotes are not read.
- **Dated ledger anchors.** SCB reuses a column name in one table for different VarIds,
  in turn (`V714_65`: one VarId 2007–2009, another 2014–2016) or at once (`Kolumn2` in
  `Gasföretag` names some 90). A spelling alone then reaches several variables, and the
  first in index order took the id: 5,299 SCB ids reached several variables. An anchor
  now carries a day where its spelling is shared, which keeps source keys out of most
  anchors and survives renumbering. The SCB ledgers (generated from today's
  `<RegisterId>.<VarId>` pins) date an anchor only where another key shares its
  spelling, on the first day its key covers and no rival does (a start of its own period
  or the day after a rival's end). Where no day separates them (spellings shared at
  once, such as the long-format `RadNr` columns), the anchor names the source key
  instead: `<table>: <spelling> # <source key>`. The key suffix is allowed only where
  the spelling is shared in that table, counting every occurrence, placed or not; a
  suffix on an unshared spelling is refused (`ledger-anchor-key-unneeded`), so ledgers
  name keys only where they must. Two ids on one anchor are refused at load
  (`ledger-anchor-shared`) rather than left to key order. Generating the SCB ledgers, a
  pin keyed by column gives way to one keyed by VarId, a pin of a whole VarId to its
  split parts, and pins of different VarIds take the key suffix; 582 pins were left out
  this way. Pins keyed by column whose spelling several keys share are left out. An
  anchor splits at its last `: `, since ten SCB table names contain one. Where a ledger
  variable id equals a table id of its register (two cases: today's catalog kept the
  namespaces apart), the table keeps it.

**Decisions taken in milestone C.**

- **Release diff.** `reg-build diff OLD NEW [--json] [-n N]` compares two v4 catalogs by
  natural key (pid, table name, meta key), never by row id, in nine sections: schema
  (`sqlite_master`), meta, row counts per table, ids (added, removed, kind changed,
  retired), provider and register names, table names and spans, per-variable segments,
  label and definition runs, value sets (with the codes added and removed where only a
  segment's value-set hash changed), and finding counts per code. Text shows at most `N`
  (default 20) per section; `--json` is complete. Exit 0 when nothing differs, 1 when
  something does (the meta keys `generation_id`, `catalog_version`, `builder_version`
  and `curation_digest` never count, since they differ every release), 2 on a usage or
  IO error or a missing or different `schema_version` major.
- **The frame reader.** `frame` reads a directory of frame JSONL files as one input, the
  neutral interchange format that conformance fixtures are written in by hand. The
  reader assigns every locator (file and line) and computes every value-set hash, so a
  line states neither, and an occurrence names its value sets. A value set's `names` are
  the source's version names (v4 D24), which the emitter publishes as `valueset_name`;
  so a fixture's names are published too and must not be read later as source data. It
  also refuses a date that is not a calendar day and a population index the occurrence's
  release does not state, each at its line, since a hand-written frame skips the checks
  a source reader's input has passed.
- **Verification.** `reg-build verify FILE [--previous FILE]` is the single artifact
  check: integrity, the schema's post-write queries, each value set's items hashing to
  its address, and with `--previous` the id rules against the previous release. `emit`
  runs it on the staged file before replacing the output, so conformance and the release
  skill call the same authority. The schema version is
  `reg_core::CATALOG_SCHEMA_VERSION`, stamped into both `meta.schema_version` and
  `PRAGMA user_version`; `verify` requires both to equal it, and a previous file of any
  other version is not comparable.
- **The link stage** (`reg-build/src/link.rs`) only matches endpoints: an endpoint names
  its register by handle or by name (`fold_identity` of the trimmed name, which exactly
  one register must match), then a table, a release or a variable by source key, where a
  split key gives every piece. A `continues` needs one entity of one kind at each end, a
  `derived_from` one object and gives a relation per subject piece, and a release end is
  never a relation (a reader states a succession between releases as a notice). A notice
  becomes one finding per subject entity, `S0:source-notice` with severity `notice`: an
  addition to v4 §4.5, which has no source-notice code. A source can state successions
  the catalog cannot hold, so after folding the stage drops them as misses rather than
  let the post-write checks refuse the build: every table `continues` of one table on
  one day (the source names no single successor), then, in locator order, each
  `continues` that closes a cycle. Every miss is one `link:` warning, counted as
  `link_unresolved` in the build report. Name matching is per provider (the index key is
  `(provider, fold(name))`), so a source text naming another provider's register never
  matches and is reported unresolved for D to `bind`: that count is not a reader defect.
  A malformed link or notice (an unknown kind, an endpoint without exactly one register
  reference) is refused at its locator. In frame JSONL a link's kind is written
  `relation`, since the record tag owns `kind`.
- **Documents: `doc.content` (plan decision C-7).** A `pdf` document stores its bytes in
  `doc.content`, which deviates from schema.md. The PDFs' only URL is the mikrometadata
  UI, not the file, so the file download needs the bytes; a `parsed` document has none.
  A CHECK ties `content` to `kind = 'pdf'`, and `verify` re-hashes it against the
  document's content hash. Measured on the six PDFs: 7,884,120 B (5,324,468 B at
  `zstd -19`). The build was 630,218,752 B (112,422,605 B at `-19`, 166,077,411 B at
  `-3`), so with the PDFs the `-19` artifact is about 117.7 MB, past schema §5.2's
  90-115 MB. The Operator ruled on 2026-10-27 to keep `doc.content` in the catalog at
  that size and informed the maintainer. If the maintainer objects, §5.2's sidecar lever
  is the fallback: PDFs outside the database.
- **Documents: metadata and subjects (plan decision C-8).** Document metadata
  (`related_documents.toml`: title, licence, fetched, URL) lives beside the PDFs in the
  inputs repository, pinned like any input. The reader states each subject in source
  coordinates (a register handle or name); the link stage resolves it against the
  published ids. An unresolved subject is a `link:` warning counted as
  `doc_subject_unresolved` in the build report, and the document stays searchable.
- **Schema addition M2.** `alias_variable` indexes `alias(variable_id)` for the
  variable-to-names direction.
- `search_item` code rows are (code, label) pairs, not (value set, item): `code_text`,
  `label_text`, no `valueset_id`. The value sets of a pair come from `vs_item_code`
  (C-10 ruling: one row per item measured 1.48 GB).
- `vs_item_code` exists only for that lookup (about 90-100 MB). Packing items into one
  BLOB per value set (schema §5.2, about −65 MB on `vs_item`) is the lever if size
  matters.
- **Edition and qualifier.** They are source fields of a project, not the period suffix
  (`2011:prelim`) of v4 §7. The field form leaves the period grammar unchanged, and its
  rule ("that edition where a release of it exists, the primary elsewhere") covers a
  mixed final and preliminary request. Links and URLs that need an edition later use the
  same field form.
- **Implicit tables are not in `ident`.** A 2-part ref resolves through `ident` to the
  register. Where a table is expected, the table is `tbl WHERE pid = ?`, and the
  implicit table has the register's pid. A 2-part table value naming a register with
  named tables is `table_is_register`, listing the tables.
- **Search keeps diacritics.** The new catalog's `search` index holds the text as
  written, under `unicode61 remove_diacritics 0` (schema §3.2, plan decision C-9), so
  `lan` and `län` differ. `reg_core::fold_search` strips combining marks and is not used
  for it; the reader folds queries with `reg_core::normalized_search_query` (case and
  whitespace only). The `fold_search` paragraph under "Search" below describes the old
  indexes.

**What the rebuild is checked against.** Not today's artifact byte for byte. It is
checked against what users see (tables, columns, variables, labels, value sets) and
against random full-register orders, with every difference classified.

## Agent and web interface

**Users are agents and the SPA.** Agents use MCP; the SPA uses HTTP. Interactive human
use is out of scope: no feature, flag or output exists for it (no text output, prompts,
progress or notebook import). Revisit when a concrete human-facing use appears.

**One operation set, two transports.** Each operation in `reg-catalog` has a name, a
typed parameter object and a typed result. The HTTP routes are generated from those
definitions, and the MCP tool schemas are built from the same utoipa OpenAPI document,
so the transports cannot drift. OpenAPI and MCP `tools/list` are the self-description;
there is no `describe` operation. A former CLI use has a transport: agents call MCP
tools, the Docker bake and the publish workflow use `curl` and a started `serve`, and an
order is `POST /api/project/order` or the `order` tool.

Rules:

- **JSON only, `{data, meta}` on every success**, except raw-bytes downloads (document
  PDFs, the order manifest file), which carry their media type and have a JSON operation
  for their metadata. `meta` holds the contract version, catalog generation and scope,
  so an agent always knows which catalog answered.
- **Deterministic responses.** `meta` carries no timing; the same request on the same
  catalog returns the same bytes, and goldens compare raw output.
- **A ref is an FQID or a bare name.** A bare name is a register, variable or
  classification name, matched case-insensitively (`fold_identity`), so a slug matches
  only where it is also the name. A unique name resolves; an ambiguous one returns the
  candidates with their FQIDs as `ambiguous_ref`, so the next call can be exact.
- **`show` plus facet operations.** `show(ref)` returns a summary for any kind; separate
  operations fetch the heavy parts (states, values, lineage, graph, coverage).
- **One time filter, `period`**, in `reg-core`'s grammar (`2019`, `2015..2019`,
  `LA2019`, `2019-03`, `2019-01-01..2019-06-30`). `diff` takes `from` and `to` in the
  same grammar.
- **Cursor paging for open-ended lists**: `limit` (default 50) and `cursor`, returning
  `{"items": [...], "next_cursor": ...}`. A list bounded by the request or the entity is
  a plain array. Cursors bind to the generation, so a stale cursor fails loudly. No
  offsets.
- **One parameter per concept and one result shape per operation**, independent of
  result count. `scope` is valid on every read.
- **Errors** are `{error, meta}` with
  `error = {code, class, message, remediation, fields}`; MCP returns the same document
  as a tool error.
- **Order manifests keep their bytes.** `order` returns `{data: manifest, meta}`; the
  download route serves the exact manifest bytes.
- **Few, coarse MCP tools**, about ten, because every tool's schema occupies agent
  context. Fine-grained HTTP routes for the SPA map onto the same operations.
- **Catalog selection is configuration**: one catalog per process, chosen at start.

**Hosted MCP.** `serve` exposes the global catalog's `/mcp` at reg-meta.vqv.uk, so
agents need no install and no 1.2 GB download. `/mcp` and every POST share one rate
limit per client address, sized for tool calls (`reg-meta/src/limit.rs`). A steward
catalog is served over hosted MCP when its artifact is already public, as SWECOV's is
(data.swecov.se); a private steward catalog stays on local `reg-meta mcp`. A published
server admits its MCP host (`--public-host`).

**Hosted MCP has no webapp handoff** (maintainer, 2026-10-20, #1709). Ordering happens
in a steward webapp; `show`'s root names none. The `order` tool stays: researchers
describe data they already hold on MONA as a `project_data.json`, and the mock-data
tools need the binding → table/column layout it computes.

## A provider-neutral substrate

The catalog is the identifier and object-model substrate every downstream artifact
references: `project_data.json`, the HTTP API and MCP tools, the order manifest. Their
contract is only as stable as the identifier scheme, so the model is built to outlast
any one provider's vocabulary.

Universal column names (`name`, `description`, `data_type`, ...) carry provider-native
string values verbatim: SCB's `registernamn` for LISA stays under `register.name`
exactly as published, because that is what a provider's intake form expects. The schema
has no provider-specific tables (no `scb_*`, no `sos_*`). Provider variation shows only
as fill rate on the universal columns. Provider-specific parsing lives in the build;
readers see one shape and need no provider conditionals. The provider is a queryable
attribute, not a code path.

An earlier scheme baked SCB's CSV vocabulary and yearly cadence into the model, which
broke when Socialstyrelsen and Försäkringskassan arrived. The current model removes
those assumptions: a two-level variable model, a three-segment binding FQID with no
variant or period slot, variable-grain edges, and build-time triage that normalizes
provider oddities into the universal shape.

## Two-level variable model

What SCB and SOS each publish as a "variable" entangles two facts, so it is split into
exactly two levels:

- **`variable`**: the addressable variable, the thing an FQID names. It holds the
  register-unique slug and the shared facts (`name`, `definition`, `description`,
  `measurement_unit`, `is_sensitive`, `is_identifier`, stable source attribution). A
  text that varies across deliveries stays NULL here and is kept at state grain.
- **`variable_state`**: the per-delivery shape. A variable has one or more states; each
  carries a variant coordinate, explicit delivery scope, data type, length, value set
  and version label. The value set anchors state identity: SCB's low-trust per-delivery
  type and length do not split a state when a value set is present.

Dated states have `period_scope = "intervals"` and two ISO bounds. A source-documented
independent table has `period_scope = "year_independent"`, NULL bounds and no
availability claim; calendar-year selection excludes it, and selecting it needs an
explicit concrete variant and the whole `_default` period. One variable and variant
cannot mix dated and year-independent states. Birth, migration or classification dates
never supply a delivery range. `pooled` marks a state spanning a pooled multi-year
edition with no annual coverage: one state over the whole range, and consumers must not
infer annual availability inside it. A state's `provenance` is NULL when its facts are
the provider's own: an ordinary provider-documented interval, including one a curation
entry only routed (to a variable, variant or edition), since routing claims no fact.
Otherwise it names who curated a fact: an `errata:<class>` or `curated:<kind>` head
followed by the entry's source locators and storage-type facts, a steward row, or
`inferred:resolution-gap`; several blocks join, sorted, with a blank line. The SPA
parses the `curated:matrix-answer` block's `Document:`, `SHA256:` and `Pages:` lines
(`reg_webapp/frontend/src/lib/curated_source.ts`), so changing that shape is a contract
change. It never carries curator evidence, rationale or a curation case pointer, which
stay in the build report (#1708). It stays at state grain so one corrected edition never
relabels a neighboring window.

A state is as long as its facts allow (#1296 2d). Resolution cuts a column wherever the
set of active source editions or code lists changes, and the builder then merges a run
of day-adjacent states on one variant, delivery column and value-set version whose facts
are all identical (value set and version label, classification links, data type and
length, texts, provenance, pooled flag; population is a variant fact) into one state
over the run. A gap or any differing fact keeps states apart, and an open-ended state
never joins a closed run (readers count it as its opening year only), so windows and
coverage are unchanged. The merged state keeps its earliest segment's `valid_from` and
so its `state_id`; the IDs of the segments it absorbed no longer resolve.

**The variant is a coordinate, not an identity level.** A variant (SCB
`registervariant`, SOS `deldatamängd`) is a delivery coordinate. "Kön in LISA" is one
variable however many variants deliver it; the same variable in variant A or B, or in
year X or Y, is a different state, not a different identity. The variable is the FQID
target, and variant and period select among its states.

Variant-less registers (Socialstyrelsen LSS, BU, SOL) get one synthesized `_default`
variant row, a real row that every state references. Because the variant is not an FQID
segment, `_default` never appears in a binding FQID.

**Keys.** The natural key is `(register_id, slug)`. It stays unique after a triage split
puts several variables under one source key, because siblings get distinct slugs. The
provider key (SCB `var_id`, the SOS merged name) is a non-unique join hint. A synthetic
`variable_id` keeps the state FK single-column and edges stable as the natural key's
provider-specific shape varies. Variable formation is the adapter's; the build design
note describes it. The normative DDL is `reg_meta_build/db.py`.

### Why two levels, not three

The decision is empirical, measured on the production SCB catalog and the 13
Socialstyrelsen workbooks at the 2026-05-22 design lock. Re-run it if the data drifts.
An earlier draft made the variant part of identity (a four-segment FQID) and recovered
`var_id` reuse with `(N choose 2)` auto-emitted `same_as` edges. The data refuted it:

  | Question                                                         | Finding                                                                | Implication                                    |
  | ---------------------------------------------------------------- | ---------------------------------------------------------------------- | ---------------------------------------------- |
  | How many SCB `(register, var)` pairs appear in several variants? | 78.9% appear in exactly one.                                           | Variant identity is degenerate for 4 in 5.     |
  | What differs across variants in the same year?                   | 4.3% of pairs diverge at all, mostly column name or grain.             | Triage resolves it; the variant never decides. |
  | Variant or period: which differentiates more?                    | 43% of multi-period `(variable × variant)` cells drift across periods. | Period is the axis, and it lives on the state. |
  | SOS: do code sets differ by deldatamängd?                        | No variable has a deldatamängd-specific code list.                     | The deldatamängd carries no identity.          |
  | SOS: do codes vary by period?                                    | 35% of code rows carry period ranges.                                  | Again, period is the axis.                     |

The 4.3% (pair grain) and 43% (cell grain) are deliberately at different grains; the
point is the contrast. Coalescing by shape shrank about 515K instance rows to about 104K
states. Free-text fields are unreliable for identity: comparing SOS `Värdemängd` prose
suggested about 50% divergence, which the structured code lists refuted. Identity
decisions anchor on structured code data, never prose.

## FQID grammar

Every entity has a fully qualified identifier: a `/`-separated string whose kind follows
from its segment count and the `class/` prefix alone. `reg-core` owns the grammar
(`grammar.rs`).

  | Segments            | Form                           | Kind                            |
  | ------------------- | ------------------------------ | ------------------------------- |
  | 1                   | `<provider>`                   | provider                        |
  | 2                   | `<provider>/<register>`        | register                        |
  | 3                   | `<provider>/<register>/<slug>` | variable binding (the variable) |
  | 2, leading `class/` | `class/<slug>`                 | classification                  |
  | 3, leading `class/` | `class/<family>/<version>`     | classification version          |

```text
scb/lisa/kon          variable binding
sos/lss/insatstyp     variable binding in a variant-less register
class/sun2020         classification, vintage in the slug
```

- **No variant slot.** A variant is browsed as a sub-resource of its register and passed
  as a coordinate, never addressed by a path, so `scb/lisa/x` is always a variable.
- **No period slot.** A definition that changes over time is several states with
  validity ranges, not several FQIDs. Time is context, not identity. The period grammar
  is `YYYY`, `YYYY-MM`, `YYYY-MM-DD`, `HTYYYY`/`VTYYYY`, `LAYYYY` (July of `YYYY`
  through June of the next year), `YYYY-Q[1-4]`, `YYYY-H[12]`, years 1900–2099, plus a
  `from..to` range. Each value has exactly one spelling.
- **A class version is lowercase `[a-z0-9][a-z0-9-]*`.** Versions are years and editions
  (`class/sun/2020`, `class/sni/2007`), so a digit may lead; the family is a slug.
- **Classification vintage is in the slug.** SUN 2020 is `class/sun2020`; ICD-10 and
  ICD-11 are distinct classifications. Each vintage is its own normative document.
- **No `@version` suffix.** Co-delivered parallel codings (SNI92 and SNI2007 in a
  transition year) are overlapping states of one variable, discriminated by
  `value_set_version_label` and by delivery column. A project picks one with
  `Binding.representation`, not an FQID pin.

A slug is lowercase ASCII kebab-case: `^[a-z](?:-?[a-z0-9])*$`. `class` is reserved
everywhere, so the classification prefix stays unambiguous. `group` is reserved as a
first segment, because group refs (`group/<provider>/<register>/<key>`) start with it.
`_default` is the variant-less coordinate and never a slug. HTTP routes put the
operation before the ref (`/api/states/{ref}`), so no slug can shadow a route.

Slugs come from the latest delivery-column alias. A rename between editions produces a
new slug for the later editions unless a curator links them; a rename is a new variable
by default. How often curators review renames is undecided.

## Edges and lineage

All relationship edges are variable grain: there is nothing below the variable to anchor
an edge on, and the edge triple `(provider, register, variable)` is the binding FQID.

- **`same_as`**: symmetric cross-register or cross-provider equivalence, curated only,
  never auto-derived. Within-register `var_id` reuse is the variable itself. The writer
  requires both endpoints to be live, so a lookup never needs to follow an edge.
- **`replaced_by`**: directional succession. Registers, variables and classification
  editions each have one. The writer requires both endpoints to be live, so a ref to an
  entity the catalog does not hold is `not_found`; the reader never redirects it to a
  successor.
- **Representation succession**: curated `representation_replaced_by` rows record a
  column rename within a variable, optionally scoped to one variant.

Same-concept grain, vintage or coding changes within one variable appear in no edge
table; they fold into one variable at build time. A concept SCB re-mints under a new
`var_id` is a different variable, so its continuation is a `replaced_by` edge even when
only the `var_id` changed. Lineage (`variable_state_lineage`) is the state-grain edge
from a composite register's column to its source register's state, with the validity
intersection; build warnings explain the edges it could not settle. Composite registers
(LISA, FRIDA, LINDA, STATIV) record a stable source on the variable and a varying one on
the state; unresolved sources remain raw text.

## One spelling per delivery column

One delivery column can be spelled several ways: the state's spelling and the alias
history's (`fastigheter` windows IDVE, IdVe and idve over one column). They fold
together under `fold_identity` (Unicode lowercase, no normalization; `reg-core`), the
same rule the build validates `variable_alias ⊇ state columns` with. So a column's
identity is its fold, and its name is one representative: the state's own spelling where
a state names the column, else the lowest by byte order. Lowest, not first seen, because
first seen would depend on a query plan. The build stores the representative as
`canonical_column`, and every reader that names a column uses it.

## Compiled states and request-time rules

The resolver lives in the build: derive compiles every representation the resolver can
emit into `expanded_state`, and its column projection into `resolver_column` (the build
design note has the rules). The reader applies only what depends on the request.

**Window fallback.** For a requested interval, a state with a `base_fallback` row emits
its `source_window` and `coded_window` rows that overlap the request, or the base when
none does. Its overlapping `curated_window` rows are always added. A `base` row is
always emitted. Without a requested period every row but `base_fallback` is emitted.
Several representations at one period are normal, not an edge case: several variants
deliver the variable, a range crosses a transition, or co-delivered value-set versions
overlap (SNI92 and SNI2007 in a transition year). Reads return a list, and ambiguity is
the caller's to narrow with a variant or representation, never an error. This is what
lets a monthly family answer `2024-03` with the March column, `2024` with all twelve,
and a gap month with the annual state.

**Warning attribution.** A warning `w` applies to an emitted representation `e` with
bounds `[lo, hi]`, after held and requested clipping, exactly when:

- `w.variable_id = e.variable_id` and `w.register_id` is the variable's register;
- `w.register_variant_id IS NULL` or equals `e.register_variant_id`;
- `w.delivery_column_name IS NULL`, or `fold_identity(w.delivery_column_name)` equals
  `fold_identity(e.canonical_column)`; a representation without a column gets only
  column-less warnings;
- `w.valid_from IS NULL` or (`hi` is not NULL and `w.valid_from <= hi`), and
  `w.valid_to IS NULL` or (`lo` is not NULL and `w.valid_to >= lo`). The comparison is
  exact, so a year-independent representation gets only unbounded warnings.

This is evaluated per request rather than compiled: a link table measured over 2.2M rows
keyed by 64-character ids, larger than every other derived table together, while one
variable's warnings read in about 1 ms. A join through the source state alone would leak
or drop warnings. Holdings scope adds nothing: clipping to held periods already implies
the held-mapping predicate.

A state narrowed to a held or requested window re-derives every window-derived field
(`period_token`, `warning_ids`) from that window. `pooled` stays the source fact.

**Co-delivered representations.** Checked per-column windows (`column_metadata` or
coding `per_column`) carry their own type, length, definitions, names and coding, and
override the base even when NULL: an absent column fact never borrows a sibling's. Each
such window has its own value-set id and classification links, so a missing override
never borrows the shared state's domain. Shared windows inherit the base. A window's
identity is `(state_id, delivery_column_name, valid_from)`, so several windows can share
one state. None of this asserts that two source editions are statistically comparable.

## Data warnings

User-facing source limitations and explicit interpretation assumptions are catalog data;
build-side bookkeeping codes (identity and lineage resolution, set-aside validity,
curator rationale) stay in the build report only. A warning keeps its severity, summary,
explanation, diagnostic SHA-256, source references, acknowledgement and supplied
delivery bounds; its id is the SHA-256 of its canonical content. Full diagnostic text
stays in the build ledger. A warning attaches to a state only when its source references
positively witness the state's coordinates and dates; unknown and year-independent
states never acquire dated warnings through an invented interval.

Registers and variables expose complete warnings. States carry `warning_ids`, so long
text is not repeated across annual states. A `data_warning` row stores each coordinate
once, as an ID column, shares summary and detail text through `data_warning_text`, and
is clustered by (register, variable); the reader rebuilds the warning from the row.
Warnings inform interpretation; they never change source values or block selection.
Builder accounting such as "temporally unassessed" range editions is not a warning.

## Read scope and artifact admission

A server or MCP process serves one artifact, chosen at start with `--db DIR` (the file
is `DIR/reg_meta.db`). Admission is exact: a supported schema version, every required
manifest key present and a `generation_id` of 64 hex digits. Anything else, a missing
key or a malformed `generation_id` included, is `schema_incompatible` (`schema_version`
is the file's value, or `null`), and the process fails before serving, with no fallback.
The file is opened read-only and `immutable=1`: published files are replaced by rename,
never changed in place. The docs database beside it is optional; only docs operations
need it.

**Three layers.** `reference` is everything in the catalog; `holdings` is what the
steward holds; a project is what one user has: a `project_data.json`, ordered from the
holdings or, with a `global_fallback` manifest, describing data the researcher already
holds (nothing orders it). The project layer is the user's file: the server stores none,
and checks one only when it is posted to validate or order (the SPA runs the structural
layer client-side first). Validation and order answer for the catalog that serves them,
so a project is checked on its own steward's server: another catalog's answer differs
without failing.

A steward artifact defaults to `holdings` scope, a catalog artifact to `reference`;
explicit `holdings` on a catalog is `scope_unavailable`. Reference means every node of
the selected generation, steward-only metadata included. Scope is reader state, valid on
every read.

  | Node kind              | Holdings predicate                                                                                                           | Reference                                 |
  | ---------------------- | ---------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------- |
  | Provider               | At least one admitted descendant register.                                                                                   | All providers.                            |
  | Register               | At least one admitted variable binding at a mapped variant.                                                                  | All registers and variants.               |
  | Variable binding/state | An explicit mapping under this register; a selected variant or representation must match it. Output narrows to held periods. | Full history, subject to request filters. |

Membership uses the authored variable and variant and the canonical representation.
Possession is physical: a table that maps a `(variable, variant)` under one
representation also serves each other spelling of it SCB did not deliver beside that
one, over its held periods, so SCB's per-year spelling never subtracts held years
(`Doda_20241231`'s `DodDatum` column holds `dodsdatum` through SCB's 1998–2023
`DodDat`). Where SCB delivered both spellings at once, where one table maps the variable
under several representations, or where the table has another column named for that
spelling (identity fold; mapped elsewhere or unmapped, that column holds it), the
spelling must match the column. `held::serves` is the one predicate every holdings read
and the order use; the builder's derived holdings rows (`browse_delivery`,
coded-variable counts) repeat it, and its one-to-one rule places every spelling it
serves, so a second location claiming such a cell is refused at build time
(`inventory_cell_conflict`). One limit stays: a table mapping `R` claims both `X` and
`Y` in a year SCB delivered `X` and `Y` side by side but not `R`, though its one column
holds at most one of them; the order then emits that column for both. A `same_as` edge
never grants possession of a different binding. Unknown-scope tables and unmapped
columns admit nothing. An interval holding contributes only the intersection of its
periods with the request and the resolved states; a year-independent holding needs
year-independent states, never invented dates. Discovery may union admitted variants for
a binding; variant-specific reads, validation and order never do. Coverage keeps
disjoint intervals and does not promise orderability.

The predicate applies before counts, grouping and pagination, so no page is backfilled.
Cursors bind to the query, its filters, the scope and the generation; a changed context
rejects continuation. An unheld direct ref is `not_found` in holdings; reference scope
can inspect the same node without making it selectable.

Exceptions: classifications, value sets, codes, documents and lineage stay reference in
either scope, but a variable-owned read still needs an admitted owner in holdings. A
held graph can show reference neighbors that are not selectable. Register warnings in
holdings show register-wide warnings and those of admitted bindings. A dated warning in
holdings reads as held (#1900): a variable's window is clipped to the held periods of
its variable at its variant and representation (`held::periods` per state of that
spelling over the state's own bounds, the same periods `states` clips to), a
register-wide one to the register's held span (the requested variant's when the request
names one; a register-wide warning has no variant of its own), and becomes the hull of
what remains, so the warning keeps one row and its id; a warning wholly outside them, or
whose clipped window misses the requested period, is dropped. The held periods are read
per state, not over the warning's window: a renamed column's warning (SCB's `DodDat`
held through a `DodDatum` mapping) would otherwise find none wherever the window also
covers years the mapped spelling was delivered, and stay unclipped. A warning without a
window, or one whose cell is held in no dated table, has nothing to clip against and
stays as stored. Validation and order take no scope; they use the artifact's kind.

## The logical order

`order` runs the order check of design v4 §9 on a 5.0.0 project and returns the logical
order of schema §2.3 as `data`, or the error `order_blocked`. `validate` runs the same
check and returns the same findings as issues. Both transports call the one
implementation in `reg-catalog`, which is what makes their results identical. The check
is written over the v4 catalog (the `r-order-check` PR), not ported from the 4.0.0
physical-order logic, which v5 deletes.

The logical order says what is ordered, never in which files. It stops at the catalog:
physical files, partitions, row filters, file names, the manifest download and the
extract format are the holdings layer's, and the physical manifest returns with it in
milestone E. Nothing in the result names a steward.

**Shape.** `shape.LogicalOrder` in `conformance/api/operations.toml` is the field list.
In short: `catalog` and `project` identify the generation and the project (with
`project_hash`); `entries` are per source, per binding in declaration order, then by
period start; `row_keys` are per source and period start; `clips` record every
availability clip, also when nothing blocks; `findings` are the non-blocking ones
(warnings and infos), as `Issue`. Periods are half-open ISO-day intervals, with `null`
for the open sentinels; a timeless table has `{start: null, end: null}`.

- An **entry** is one merged segment of one binding in one table. With a table
  succession (`continues` between tables) each day goes to the table whose `at_start`
  window holds it, so one binding can give entries in two tables. Adjacent segments
  merge when their spelling, type and value set agree (design §4.6), so a flag-only
  boundary does not split an entry. `edition` and `qualifier` appear only when the
  source names them.
- **`row_keys`** are the table's segments with role `row_key` over the period. A row key
  that no binding orders still appears there; it is not an entry. This is where a panel
  gets its keys, since the document names none.
- A blocked order is `order_blocked`, with every finding at once and nothing partial;
  the blocking ones have level `error`.

**Findings.** Errors block: `fqid_unresolved`, `table_is_register`, `retired_ref`,
`variable_not_in_table`, `outside_availability`, `ambiguous_window`,
`preliminary_without_edition`, `edition_not_found`, `period_default_on_dated_table`,
`period_on_timeless_table`, `period_not_orderable` and `project_empty` (no sources; a
whole-project finding). Warnings and infos do not: `classification_version_ambiguous`,
`edition_disagreement`, `variable_sensitive`, `variable_replaced`,
`panel_row_keys_differ`, `period_clipped`, `form_change`, `table_succession_undated`,
`identifier` and `coverage_end_unstated`. Each is an `Issue` whose `path` points at the
source, binding or panel and whose `source`, `variable`, `table` and `period` say what
it is about (`null` for a whole-project finding). The conditions are the order check's
to state in its own tests; the rules that shape them:

- A retired id reports "retired in <version>" and names no successor; no operation
  follows `ident_retired`.
- `variable_replaced` reports an incoming `continues` edge and never follows it.
- Design §9 blocks only the two window kinds (`ambiguous`, and a preliminary segment
  without its edition), so a class-version ambiguity the binding does not pin is a
  warning.
- Variables are register-scoped, so members of different registers never share a row-key
  variable. `panel_row_keys_differ` compares the row-key segments' identifier kinds
  (`segment.id_kind_id`, the full kind, pseudonymised form included) and warns only when
  every member's kind is known and they differ. Which keys join is the researcher's
  call.
- A request partly outside availability is a clip (`period_clipped`), never silent and
  never an error. Only availability empty across the whole request blocks.
- A table succession with no `at` is not followed: the check returns
  `table_succession_undated` naming the successor and clips the rest. No day is inferred
  from coverage (v4 §2.1); D's `link … at` supplies the day.

### A clear order

Catalog (illustrative): table `scb/lisa/individ` with yearly spans 2003 onward. Variable
`scb/lisa/dispink` has two segments: `DispInk` over `[2003-01-01, 2004-01-01)`, then
`DispInk04` from 2004, both `int`. Variable `scb/lisa/sun2000niva` starts in 2004.
`scb/lisa/lopnr` is the row key throughout.

Request: source `lisa`, table `scb/lisa/individ`, period `2003..2006`, binding `dispink`
and `sun2000niva`.

```json
{
  "data": {
    "catalog": {"generation_id": "9f2c…", "catalog_version": "0.65.0", "schema_version": "4"},
    "project": {"name": "income-panel", "schema_version": "5.0.0",
                "reg_meta_version": "0.65.0", "project_hash": "51ab…"},
    "entries": [
      {"source": "lisa", "binding": 0, "provider": "scb", "register": "scb/lisa",
       "table": "scb/lisa/individ", "variable": "scb/lisa/dispink", "display_name": null,
       "period": {"start": "2003-01-01", "end": "2004-01-01"},
       "spelling": "DispInk", "type": "int", "length": 8, "value_set": null,
       "classification": null},
      {"source": "lisa", "binding": 0, "provider": "scb", "register": "scb/lisa",
       "table": "scb/lisa/individ", "variable": "scb/lisa/dispink", "display_name": null,
       "period": {"start": "2004-01-01", "end": "2007-01-01"},
       "spelling": "DispInk04", "type": "int", "length": 8, "value_set": null,
       "classification": null},
      {"source": "lisa", "binding": 1, "provider": "scb", "register": "scb/lisa",
       "table": "scb/lisa/individ", "variable": "scb/lisa/sun2000niva", "display_name": null,
       "period": {"start": "2004-01-01", "end": "2007-01-01"},
       "spelling": "Sun2000niva", "type": "char", "length": 3,
       "value_set": {"hash": "c41e…", "n_items": 31}, "classification": null}
    ],
    "row_keys": [
      {"source": "lisa", "table": "scb/lisa/individ",
       "period": {"start": "2003-01-01", "end": "2007-01-01"},
       "columns": [{"variable": "scb/lisa/lopnr", "spelling": "LopNr"}]}
    ],
    "clips": [
      {"source": "lisa", "variable": "scb/lisa/sun2000niva",
       "requested": [{"start": "2003-01-01", "end": "2007-01-01"}],
       "ordered": [{"start": "2004-01-01", "end": "2007-01-01"}]}
    ],
    "findings": [
      {"level": "info", "code": "period_clipped", "path": "/sources/0/bindings/1",
       "message": "scb/lisa/sun2000niva is available from 2004-01-01; 2003 is not ordered",
       "source": "lisa", "variable": "scb/lisa/sun2000niva", "table": "scb/lisa/individ",
       "period": {"start": "2003-01-01", "end": "2004-01-01"}, "successor_fqid": null},
      {"level": "info", "code": "form_change", "path": "/sources/0/bindings/0",
       "message": "scb/lisa/dispink changes form at 2004-01-01 (DispInk -> DispInk04)",
       "source": "lisa", "variable": "scb/lisa/dispink", "table": "scb/lisa/individ",
       "period": {"start": "2003-01-01", "end": "2007-01-01"}, "successor_fqid": null}
    ]
  },
  "meta": {"contract_version": "5.0.0", "generation": "9f2c…", "catalog_version": "0.65.0"}
}
```

### A blocked order

Catalog (illustrative): in `scb/lisa/individ`, two variables claim the folded spelling
`kommun` over 2019, so both segments are `ambiguous` there. `scb/lisa/dispink` for 2020
is delivered only by a `preliminary` release.

Request: source `lisa`, period `2018..2020`, bindings `kommun` and `dispink`, no
`edition`.

```json
{
  "error": {
    "code": "order_blocked",
    "class": "order_blocked",
    "message": "the order is blocked by 2 findings",
    "remediation": "Narrow the period around the blocked windows, or name the edition; an ambiguous window needs curation.",
    "fields": {
      "findings": [
        {"level": "error", "code": "ambiguous_window", "path": "/sources/0/bindings/0",
         "message": "two variables claim the column 'kommun' in scb/lisa/individ over [2019-01-01, 2020-01-01)",
         "source": "lisa", "variable": "scb/lisa/kommun", "table": "scb/lisa/individ",
         "period": {"start": "2019-01-01", "end": "2020-01-01"}, "successor_fqid": null},
        {"level": "error", "code": "preliminary_without_edition", "path": "/sources/0/bindings/1",
         "message": "scb/lisa/dispink over [2020-01-01, 2021-01-01) has only a preliminary edition; name it with \"edition\": \"preliminary\"",
         "source": "lisa", "variable": "scb/lisa/dispink", "table": "scb/lisa/individ",
         "period": {"start": "2020-01-01", "end": "2021-01-01"}, "successor_fqid": null}
      ]
    }
  },
  "meta": {"contract_version": "5.0.0", "generation": "9f2c…", "catalog_version": "0.65.0"}
}
```

With `"edition": "preliminary"` on the source, the second finding goes and the 2020
entry carries `"edition": "preliminary"`. The first stays until curation resolves the
ambiguity; narrowing the period to 2018 alone also clears it.

## Project semantic validation

The validate operation runs the supported-version decision, then the structural
validator ([reg-core/DESIGN.md](reg-core/DESIGN.md)), and concatenates their issues with
the order check's findings ("The logical order"). A failing project is a result, not an
error. A structural failure skips the catalog layer, so a rejected document costs no
catalog read. `validate`'s `ok` is true exactly when `order` returns a logical order.

### Project document 5.0.0

`schema_version` `5.0.0` replaces 4.0.0 as a major bump: `register_variant` and
`representation` go, so a 4.0.0 document cannot be read as 5.0.0. The reader takes its
own version exactly (`unsupported_schema_version`), with no migration path. The field
tables give each change and its reason.

Top level:

  | Field            | 5.0.0     | Why                                                                                                                                                                                                       |
  | ---------------- | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `steward`        | removed   | Its readers go (the serving catalog's name, `steward_mismatch`, the holdings checks). The holdings layer (E) decides how a project names its steward or holdings. A field nothing checks would be a shim. |
  | `extract_format` | removed   | A file format is physical. It returns with the order file in E.                                                                                                                                           |
  | the rest         | unchanged | `schema_version`, `reg_meta_version`, `name`, `sources`, `panels`, `window`.                                                                                                                              |

Source:

  | Field              | 5.0.0                                               | Why                                                                                                                                                          |
  | ------------------ | --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
  | `register_variant` | replaced by `table`: a table's public id            | v4 has tables, not variants (design §2.2). A 2-part id names a register with one implicit table; for a register with named tables it is `table_is_register`. |
  | `period`           | same grammar                                        | `_default` now means "a timeless table" (design §2.1), checked against the catalog (`period_default_on_dated_table`).                                        |
  | `edition`          | new, optional: `final`, `unstated` or `preliminary` | Design §4.3 T2: a `preliminary` segment is orderable only by naming its edition.                                                                             |
  | `qualifier`        | new, optional string                                | Schema §2.3 and T3: releases with different qualifiers are distinct deliveries. Absent means every qualifier.                                                |

`edition` selects, in every window of the source's period where a release of that
edition exists, that release's occurrences. Elsewhere the primary edition serves. Naming
an edition that no release of the serving tables has over a period item is
`edition_not_found` for that item.

Binding and panel:

  | Field                                     | 5.0.0                                                    | Why                                                                                                                                                                                                                         |
  | ----------------------------------------- | -------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `value_set`                               | replaced by `classification`: `class/<family>/<version>` | A value set is content-addressed by hash in v5; the pin is a class version (schema §2.2). Until D curates classifications no version exists, so any pin is `fqid_unresolved`.                                               |
  | `representation`                          | removed                                                  | A spelling is evidence, not a selector (design §7). The order check splits by form and returns the spelling per window. Where two spellings claim a window it is `ambiguous` and blocks; curation fixes it, a pin does not. |
  | `type`, `display_name`, subtypes, formats | unchanged                                                | The researcher's declared output column. Nothing in v4 replaces it.                                                                                                                                                         |
  | panel `members`                           | source names only                                        | The member object form existed only to override keys.                                                                                                                                                                       |
  | panel `entity_key`, `time_key`            | removed                                                  | Keys come from the catalog: a table's row keys are its segments with role `row_key` (schema §0), and the time axis is the member table's `time_point` / `time_start` segment, else the member source's period.              |

A panel is a named grouping of sources with a comment. Which keys join across members is
the researcher's call; the order check warns (`panel_row_keys_differ`).

Structural rules (`reg-core`, the document alone). Removed with their fields:
`invalid_slug` and `invalid_enum_value` on `steward` and `extract_format`,
`composite_time_key_mixed_kinds`, `literal_period_invalid`,
`composite_key_inconsistent`, `time_key_member_kind_mismatch`,
`literal_time_key_duplicate`, `entity_key_unknown_column`, `time_key_unknown_column` and
the key `invalid_field_type` rules. Changed:

  | Rule                                                         | 5.0.0                                                                                                                                                           |
  | ------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | top-level keys                                               | `steward` and `extract_format` are `unexpected_field`                                                                                                           |
  | source keys                                                  | `table`; optional `edition` (`invalid_enum_value`) and `qualifier` (`invalid_field_type` if not a string); an explicit `null` of either is `invalid_field_type` |
  | `invalid_fqid` on the source                                 | `table` is a 2- or 3-part public id, provider not `class`                                                                                                       |
  | `fqid_table_mismatch` (was `fqid_register_variant_mismatch`) | the binding's `<provider>/<register>` equals the table's first two parts                                                                                        |
  | `invalid_period` for `_default`                              | gone from the structural layer; the catalog rule `period_default_on_dated_table`                                                                                |
  | binding keys                                                 | `classification`, `class/<family>/<version>` (3 segments); `representation` is `unexpected_field`                                                               |
  | panel keys                                                   | `panel_id`, `members` (strings), `comment`                                                                                                                      |

Everything else is unchanged: `invalid_root`, `missing_required_field`,
`invalid_field_type`, `invalid_window`, `duplicate_source_name`, `empty_bindings`,
`display_name_collision`, `subtype_on_wrong_type`, the period rules,
`duplicate_panel_id`, `empty_members`, `panel_member_unknown_source`,
`source_referenced_by_multiple_panels` and `unsupported_schema_version`.

Catalog rules are the order check's, listed in "The logical order": `fqid_unresolved`
(also for an unknown table or classification), `retired_ref`,
`period_default_on_dated_table`, `period_on_timeless_table`, `edition_not_found`,
`variable_not_in_table`, `table_is_register` and the warning `panel_row_keys_differ`.
`steward_mismatch`, `fqid_outside_steward_catalog`, the `representation_*` codes,
`value_set_missing` and `binding_state_drifts_within_period` go; the last becomes the
`form_change` notice.

## Search

`search` and `search_facets` read one FTS5 index, `search` (columns `name`: pid and
delivered spellings; `label`: labels; `body`: definitions, descriptions and document
text), whose rowid is `search_item.id`. The builder indexes raw text under
`unicode61 remove_diacritics 0`, which folds case only, and the reader applies no fold
of its own: `lan` does not match `län`. Pins, synonyms and type priors are curation (D).
A call returns one ranked list; the SPA makes one call per typed group it shows.

1. **Tokens.** Each maximal run of Unicode letters and digits in `q` is a quoted prefix
   term `"tok"*`, joined by spaces (AND, within one row), so FTS5 operators in `q` are
   inert and `ink` finds `inkomst`. A one-character token is a prefix term too:
   `prefix = '2 3'` only makes two- and three-character prefixes cheap. `q` over 200
   characters or holding a NUL is `invalid_parameter`; a `q` with no token returns no
   items (`{registers: []}` for facets) without a query. `ops/fts.rs::query` is the one
   implementation; `docs_search` shares it.
2. **Rank.** `bm25(search, 10.0, 5.0, 1.0)` (name, label, body), ascending. FTS5 scores
   a row by its weighted hit count against its whole token count, so a hit weighs most
   in an id or spelling and least in a definition or document text: a variable labelled
   `Vikt` ranks above a longer one whose definition says vikt.
3. **Ties.** Then `search_item.kind` in SQL text order (`classification`, `code`,
   `document`, `register`, `set`, `table`, `variable`), then `sort_key`, which is
   unique. Two rows tie on bm25 when the query hits the same weighted count in rows of
   equal token count.
4. **Filters**, ANDed with the match. `type` keeps `kind = type`; `classification` is
   empty until D. `register` resolves through `refs::resolve`; a ref of another kind is
   `invalid_ref`; it keeps `search_item.register_id = id`, which drops code rows and
   keeps a document linked to that register (its first linked subject's). `period` keeps
   `start < e AND s < end` over the row's hull, which drops code and document rows (no
   span). So `type=code` with `register` or `period` is empty.
5. **Paging.** Offset paging over that order: fetch `limit + 1` rows at the offset. The
   cursor's context is every parameter but `cursor` and `limit`, its offset the hits
   returned so far, its position the last hit's `sort_key`; `depth` is 1000. A page
   never passes the 1000th hit, and the page that reaches it has `next_cursor: null`.
6. **Hits**, each tagged `type`, from the row's entity:
   - register `{fqid, name, description, period}` (`description` `null` when none);
   - table `{fqid, name, register_name, role, period}`;
   - variable `{fqid, name, register_name, definition, spellings, period}`: `name` is
     the label, `spellings` the variable's `alias` folds, sorted;
   - code `{code, label, value_set_count}`: `label` `null` when the pair has none, the
     count is of distinct value sets holding the pair that some segment references
     (`vs_item_code`, `segment_valueset`);
   - document `{doc, title, heading, page}`: `doc` is the hash; `heading` and `page`
     come from the document's best `doc_fts` section for the same expression
     (`bm25(doc_fts)`, then `ord`), both `null` when no section matches.
7. **Facets.** One `GROUP BY register_id` over the match with `kind = 'variable'`,
   registers in pid order, each `{fqid, variable_matches}`. With `register` (resolved as
   in 4), only that register, adding `variables`: every matched variable `{fqid, name}`
   in pid order, `limit` ignored; `registers: []` when none matches. Without it, `limit`
   adds `name` and the first `limit` matched variables in pid order. With neither,
   counts only. A key a mode does not list is omitted, never `null`.

`resolve` is different: exact `fold_identity` lookup of delivered column names, no
full-text fallback and no scoring. It maps known headers; it is not discovery.

## Value sets and classifications

**Value sets are year-projected.** SCB's value-set export is the union of every code
that ever applied; its validity file is the temporal filter. Each state carries only the
codes valid in its era. Projection is year-precision on purpose: SCB metadata is annual
and sub-year bounds are administrative artifacts. The union is discarded, so there is no
"valid at" query. Value sets are content-addressed (`member_hash` over sorted
`(code, label)` pairs) and shared; a NULL `value_set_id` means the state has no codes. A
state can return a summary instead of its members: the count and, when the codes are a
dense integer run whose labels restate them, the span, so an age coding reads as `0-110`
rather than a 111-row table.

**Classifications** (SUN 2000, SSYK 2012, SNI 2007, LKF, ...) are first-class.
`classification_code` holds only published canonical codes; codes merely observed in
data are not attached to the classification. The link is on the state, not the variable:
`Utbildningsnivå` uses SUN 2000 through 2018 and SUN 2020 from 2019, and per-state links
keep the two code systems apart. A state can declare several books. Its conformance
(`conforming`, or `extended` with source extensions) is recorded per book, separating
substantive nonstandard codes from known sentinel codes. Source labels stay intact, and
local extensions never become official members. Unknown or ambiguous references stay
unresolved.

Succession is `classification_replaced_by`. A future-dated edge becomes active only when
the manifest's succession as-of year reaches it; currentness and the terminal that
classification search follows use the same policy. `supersedes` and `superseded_by` are
projections of the active edges, and a predecessor can fan out (`sun1996` → the 2000
nivå, inriktning and grupp editions). A code's `level` is the length of an all-digit
code; codes without prefix hierarchy (ICD-10, ATC) keep `level` NULL. The code tree is a
separate fact: a code may name its parent in the same book (`parent_code_id`, #1297),
only where the source delivers one, so the reader never infers a tree from levels or
prefixes. `values` with `parent` pages one level of that tree (`parent=` the roots,
`parent=<code>` its children) and gives every code a `child_count`, null in a book
without a tree, so a client fetches a subtree only when a reader opens it. A filter
(`q`) reads the book flat, because a match's ancestors are not matches.

## Concept groups and tags

**A variable concept group is one concept in one register.** It gathers everything a
researcher looking for that concept in that register should find in one place: the
variants that differ by unit, household definition, coexisting definitions or source,
and the source's own components of the concept. To the user the group is the variable.
The group page is its members' variable page: they pick columns there, and a member's
own URL opens it focused on that member. A group never spans registers: disposable
income in IoT and disposable income in LISA are two groups, and finding the concept
across registers is a tag's job. A group ref (show `group`, schema `group`, graph
`group_key`, a search group hit) names only a group whose member row for the variable is
in scope; under holdings, a variable grouped only in reference reads as ungrouped.

Machine-folded families are the narrowest case of the same rule: month-suffixed families
(`agi1lonfinkjan`…`agi1lonfinkdec`), split-sibling coding successions
(`sun2000inr`/`sun2020inr`). A group has ordered axes (stable `name`, curated `label`)
and members with per-axis facets; a variable can be several members of one group, one
per delivery column. Groups are presentation only: never an FQID kind, a binding, an
order key or a stats key. Picking on the group page adds member variables and their
delivery columns to the order; the order never names the group. Identity-level folding
by classification family was tried and dropped after 195 measured over-folds; because
grouping is presentation, a wrong group is a cosmetic bug, not identity corruption. A
variable or classification belongs to at most one variable group. Classification
umbrella groups (`group:sun`, `group/class/<key>`) are exempt from the single-register
rule: they gather classifications, which are not register-scoped.

Classification vintages are not grouped: editions are a temporal succession, not a
parallel facet. Variable definition vintages that succeed each other are likewise not
grouped as a facet (see the worked example). Curated classification umbrella groups
(`group:sun`: the three SUN 2020 dimensions plus the version-independent nivå
aggregates) have no axes; their members are distinct classifications.

### Group rubric

**When a group is warranted.** One register has two or more variables that are each an
amount (or a coding) of the same concept for some unit under some definition. A
component the source names as part of the concept counts: SCB's "Delkomponent till
disponibel inkomst" is the individual's share that sums to the family total. So does a
quantity computed from the concept: a ratio to the median, a median, a weight, a cost or
a housing-adjusted remainder. A researcher looking for the concept should find these
beside the amounts, so they join the group under the `matt` axis (below). An input to
the concept stays out: gross income (`cbrutto`, `bruttoinkomst-familj`) goes into
disposable income but is not a quantity of it, so it belongs to its own concept in the
same register. A lone variable gets no group; the builder withholds a group with fewer
than two supported members.

**Narrower groups are absorbed.** A variable belongs to at most one group, so when the
concept group is curated, any narrower group inside it (machine-folded or curated)
becomes part of it rather than sitting beside it.

**Axes come from what this register's members vary on.** Name each axis by the variation
(`enhet`, `hushallsbegrepp`, `kapitalvinst`, `kalla`, `matt`, and `definition` only
where definitions coexist in the same years). `matt` ("mått") says what kind of quantity
the member is: `belopp` for the concept itself, otherwise `andel`, `median`, `vikt` or
`justerat` (adjusted for a cost). It is a new axis because `enhet` already means the
unit of observation (individual, family, consumption unit) and `kalla` the data source;
neither covers "an amount of the concept" versus "something computed from it". A
register declares `matt` only when it holds a computed member, as with any axis.
Definition vintages that succeed one another are not an axis; they ride the period
slider and `replaced_by`. Every member supplies every axis, with `na` where the axis
does not apply. An axis that does not vary within the register is not declared, so the
same concept can carry different axes in different registers.

**Naming.** The key is the concept's slug, without unit, vintage or source qualifiers:
`disponibel-inkomst`, not `disponibel-inkomst-familj-2004`. The label is the concept's
plain Swedish name, `Disponibel inkomst`. Facet values never go into the key or the
label. The same concept takes the same key in every register, so
`group/scb/iot/disponibel-inkomst` and `group/scb/lisa/disponibel-inkomst` read as
twins.

### Tags

**Tags** are a curated thematic vocabulary across providers and registers, so a
researcher can find "a measure of income" without knowing the register. One global
vocabulary and one membership table; a membership names exactly one register or one
variable, and a variable membership can be starred with a one-line rationale. Tags are a
discovery overlay and leave identity untouched.

**Tags are search-only.** They are meant for search: finding variables by topic across
registers. Tag search is not built yet (`ops/search.rs` has no tag arm and the SPA only
displays tags); it is tracked in #1794's later steps. A variable page never lists the
other variables that share its tags. A group shows the union of its members' tags as its
own labels, and untagged siblings inherit the group's tags as neutral memberships; that
labels the group and lists no co-tagged variables.

**A tag is broad.** It names a topic a researcher types before knowing which register
holds it (`income`, `transfers`, `employment`, `education`), and it has members in
several registers. A term that would only list one register's variables, or one
concept's variants, is a group, not a tag. Comparing one concept across registers uses
the broad tag: one starred member per register, each note saying how it differs (unit,
definition vintage, population).

The vocabulary is broad: `earned-income` folded into `income`, and
`economic-assistance-amount` and `economic-assistance-months` into `transfers`, with
their starred members and notes (#1794).

### Worked example: disposable income

Each register below gets one `disponibel-inkomst` group.

- **IoT** has it: 49 members, components included, axes `enhet`, `hushallsbegrepp`,
  `kapitalvinst`, `definition` and `nordisk-lon`. This reverses #1872, which kept IoT at
  three axes with the 1991/2004 vintage off them: both definitions are delivered side by
  side in 2005-2024, so a user must choose one in the picker. Every 2004-definition
  column is co-delivered with its 1991 twin from its first year through 2024
  (`CDISP`/`CDISP04` 2005-2024), and `succession-candidates` reported each of the seven
  `replaced_by` edges between them as contradicted, so the edges were removed. The
  `definition` values read as SCB labels them: "1991 års definition", "2004 års
  definition" and "Före 1991". The pre-1991 `DIN*` columns share `fore-1991`, and some
  of them overlap in time (`DIN86`/`DIN88K` in 1990, the `DINPP` family 1981-1987).
  `hushallsbegrepp` = `hushall` is SCB's register household in both IoT and LISA, not
  HEK's kosthushåll. `nordisk-lon` holds the #1302 `_INKLGP` columns (2015-2019,
  including wage earned in another Nordic country) beside their base twins, which
  include that wage only from income year 2020.
- **LISA** has the DispInk family: individual component, family and household total, per
  consumption unit, and individualised from family, with axes `enhet`, `hushallsbegrepp`
  and `definition` (1991, 2004). `DispInkKE` and its 2004 twin `DispInkKE04` are
  co-delivered 2005-2023, so the definitions coexist. LISA's `DispInk*_INKLGP` columns
  are not yet curated as variables, so LISA has no `nordisk-lon` axis.
- **FASIT** has two machine-folded pairs, each a TRIF copy beside the FASIT-derived
  variable. The group absorbs both, adds the household, per-unit and
  alternating-residence variables, and declares a `kalla` axis (TRIF, FASIT). The weight
  `bsockvh` ("Justeringsfaktor för disponibel inkomst") is computed from the concept, so
  it joins under `matt` = `vikt`.
- **LINDA** has a machine-folded `delkomponent-disponibel-inkomst` group (12 source
  fields). The group absorbs it and adds `cdispl`, the 2004 component and the family
  total `cdisplh`.
- **HEK** has none. The group takes the individual components, the household and family
  per-unit amounts and the variants excluding capital gains. Konsumtionsutrymme
  (`cdisp3z`, `cdisp3pz`, `cdisp3kz`: disposable income minus housing cost) is computed
  from the concept and joins under `matt` = `justerat`. The inputs `cbrutto` and
  `bruttoinkomst-familj` stay out.

HEK's housing-adjusted `CDISPBOZ` and `CDISPKENZ` are still amounts of disposable income
under a stated adjustment, so they join under their own axis value, `matt` = `justerat`,
beside konsumtionsutrymme; the plain amounts carry `belopp`. Barnregistret's
`andel-av-median` is a ratio to the national median, so it joins Barnregistret's own
disposable-income group as `matt` = `andel`. It never joins another register's group:
groups are single-register. Barnregistret's catalog entries hold no disposable-income
amount beside it (`andelmediana` is its only computed sibling), so that group exists
only if it reaches two supported members; otherwise the builder withholds it and the
variable stays ungrouped. Cross-register discovery of disposable income goes through
`income`, with one starred member per register.

## Relationship graph

The graph operation returns topology plus the predicates that shape it, so the SPA
renders it as-is and never assembles graph semantics. It composes the existing reads
(succession, groups, classification chains) rather than querying again.

- **Nodes.** One per variable, carrying its state history, or one per classification
  edition. A variable node carries its group key (namespaced `provider/register/key`,
  since group keys are only register-unique), its facets in that group and the group
  label. There is no group node. An edition carries a point `version_year` (its own
  vintage, not its supersession year) and `is_current`, so time semantics live on the
  node.
- **One edge kind**, `succession`, predecessor to successor. Curated representation
  renames are succession edges carrying source and target column and optional variant.
  `same_as` is resolved away; lineage and source register are node metadata. Each edge
  has a stable id that is also its dedup key.
- **Representation runs.** Consecutive states sharing `representation_run_id` render as
  one cell. A run ends at a change of variant or period scope, or between two distinct
  states at a change of value set, version label, classifications or delivery column.
  Raw `data_type` and `data_length` never end a run: they are low-trust passthrough that
  the build's value-set-anchored fold already blanks. Windows sharing one state are one
  representation under several columns, not a boundary.
- **Empty graph means don't render.** A lone variable with no succession, no group
  siblings and one run, or a lone edition with no chain and no group, returns no nodes.
- **Group ⇄ member.** A member ref roots the graph on its group's members in scope, with
  `focus_id` on the member; a group ref roots on the members with no focus. A member
  page and its group page therefore show the same union. A classification edition adds
  the groups of every edition on its chain, so an older edition shows the family its
  successor belongs to.

## Documentation and what is not in the catalog

The catalog answers "what variables exist and what shape they have". Two siblings answer
the rest:

- **The docs database** (`reg_meta_docs.db`, its own schema version) holds register
  documentation and rehostable related documents: quality narratives, time-series
  breaks, legal text. Docs key on register and variable names, not storage ids, so the
  two databases update independently. A document's binary is served by its own download,
  never inlined in browse responses.
- **The provenance database**, maintainer-only and never shipped, holds build artifacts:
  approval dates, delivery metadata, checksums, raw provider ids.

Localization is deferred: one canonical text per field, in the provider's language.

**Sensitivity flags stay in the catalog** as `variable.is_sensitive` and
`is_identifier`. They are MONA-critical and belong to the variable, not to how a variant
delivers it. `is_identifier` is broad: it marks every identity column that is
pseudonymized at delivery (`PersonNr`, `PersonNrMor`, `PersonNrSambo`, ...), unlike the
narrower variant `panel_entity_key`, which names the subject. Consumers that need "is
this pseudonymized" key off `is_identifier`.

Population and object type are read-only metadata under a register version, not entities
and not FQID kinds.

## Versioning and determinism

- **Schema gate.** The reader requires the artifact's schema major to equal its own and
  the minor to be at least its own (`reg-catalog`'s `SCHEMA`); anything else is
  `schema_incompatible`. A major bump marks a breaking change. A minor bump marks a new
  table or column the reader starts to read, or a content change that must invalidate
  older artifacts even without a shape change, so an old artifact is refused cleanly
  instead of failing later or serving stale content. The artifact is regenerated, never
  migrated.
- **Contract version.** `CONTRACT_VERSION` versions the operation set and its JSON
  shapes, and `meta` reports it on every response.
- **Releases** are tagged per package (`reg_meta/vX.Y.Z`); each release carries the
  catalog and docs assets. `project_data.json` records the catalog release it was
  written against in that tag form.
- **Determinism.** The same request on the same artifact returns the same bytes: stable
  ordering with a unique final tie-breaker, sorted JSON keys where the contract says so,
  no timing in `meta`, cursors bound to the generation.
- **Identifiers on the wire.** Storage ids are exact integers in SQLite and opaque
  decimal strings in JSON, since they can pass 2^53. Counts, years and run ordinals stay
  numbers. The `0001-01-01` unknown-coverage sentinel reads as an absent start.
- **Security.** Metadata only, no microdata. No credentials are read or stored. The
  server makes no outbound requests.

Documentary relationships (typed source crosswalks and derivation clauses) are
`owner_bound_literal` metadata: they make no formula executable, establish no
equivalence and extend no availability.

## Glossary

The normative definitions are the DDL in `reg_meta_build/db.py`; this records the
cross-provider meanings.

  | Term                    | Meaning                                                                                                                        |
  | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
  | variable                | The addressable variable and FQID target: identity `(provider, register, slug)`, one or more states.                           |
  | variant                 | A `register_variant` row (SCB `registervariant`, SOS `deldatamängd`): a delivery coordinate, not an FQID kind.                 |
  | variable state          | Per-delivery shape: variant, validity, type, length, value set, version label. The unit of resolution at a variant and period. |
  | binding                 | A three-segment FQID naming a variable, and a project's use of it.                                                             |
  | representation          | One delivery column of a variable, named by its canonical spelling.                                                            |
  | same_as                 | Curated symmetric equivalence between variables in different registers or providers.                                           |
  | classification          | A named versioned vocabulary, `class/<slug>` with the vintage in the slug.                                                     |
  | value_set               | A content-addressed code list on a state.                                                                                      |
  | value_set_version_label | The state discriminator that lets co-delivered value-set versions overlap. `''` when there is none.                            |

Column names are universal English; values stay provider-native. SCB's Swedish columns
map as follows:

  | SCB Swedish                     | Universal English                  | Lives on                                             |
  | ------------------------------- | ---------------------------------- | ---------------------------------------------------- |
  | registernamn                    | name                               | register                                             |
  | registersyfte                   | purpose                            | register                                             |
  | registervariantnamn             | name                               | register_variant                                     |
  | registervariantbeskrivning      | description                        | register_variant                                     |
  | variabelnamn                    | name, source_name                  | variable (display name; pre-ladder name)             |
  | variabeldefinition              | definition                         | variable                                             |
  | variabelbeskrivning             | description                        | variable                                             |
  | variabeloperationell_definition | operational_definition             | variable; per-column alias windows                   |
  | variabelregister_kalla          | source_register_text, source_label | variable (interpreted text; resolved display label)  |
  | mattenhet                       | measurement_unit                   | variable (NULL when the source says "Okänd")         |
  | datatyp                         | data_type                          | variable_state                                       |
  | datalangd                       | data_length                        | variable_state (text, may carry precision and scale) |
  | vardemangdsversion              | value_set_version_label            | variable_state                                       |
  | värdekod                        | code                               | value_code                                           |
  | värdebenämning                  | label                              | value_code                                           |
  | kolumnnamn                      | delivery_column_name               | variable_alias, variable_state                       |
  | kanslig_variabel(\_ibland)      | is_sensitive                       | variable (both fold into one flag)                   |
  | identitetsvariabel              | is_identifier                      | variable                                             |
  | version_forsta / version_sista  | valid_from / valid_to              | variable_state (ISO 8601)                            |

`registerrubrik` and `registervariantrubrik` are dropped as redundant with `name`.
Register-version descriptions, population and object type ship as read-only metadata;
approval dates stay provenance-only.
