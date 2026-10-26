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
  spelling names the implicit table), matched by fold. A variable no anchor reaches gets
  a proposed id from its fold, clear of the register's table ids. When two ids reach one
  variable, the first in id order wins and the other is reported.
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
- **Gaps (T5)** are computed only in tables with a placed primary release. A day inside
  the table's coverage hull that no used column covers is a table gap:
  `release_without_column` under a primary release that delivers no column, else
  `no_release`. Inside a variable's hull in that table, an uncovered day under a marker
  is `documented_without_column`, under a primary release `release_without_column`; a
  day no release covers is the table's gap only, since a stock table released on one
  reference day a year would otherwise give every variable a gap per year.
- **Unplaced wins.** A column-less row of an unparsed release is `unplaced`, not a gap
  marker: with no period it marks no gap.
- **SCB handles.** Source keys are `v<VarId>`, release handles `r<RegVerID>`, table
  handles the variant's name (unique per register in the export). A register's handle
  comes from `[registers]` in `curation/readers/scb.toml`, else `r<RegisterId>`,
  reported. Rows that differ only by population are one occurrence with every row as a
  locator; the release lists its populations, with a reference day when one date is
  stated.
- **NFC.** CP1252 decodes to precomposed characters only, so the fold needs no
  normalization step for the SCB export.

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
is `DIR/reg_meta.db`). `--catalog NAME` names the expected catalog: `global` requires
the `catalog` kind, any other name a steward artifact whose manifest `steward` equals
it. Admission requires a compatible schema, a publishable kind (`catalog` or `steward`),
complete status, a `generation_id` and, for a steward, a slug steward id. Anything else
fails before serving, with no fallback. The file is opened read-only and `immutable=1`:
published files are replaced by rename, never changed in place. The docs database beside
it is optional; only docs operations need it.

**Three layers.** `reference` is everything in the catalog; `holdings` is what the
steward holds; a project is what one user has: a `project_data.json` against a steward
(`steward: <id>`, ordered from its holdings) or against the global catalog
(`steward: global`, a `global_fallback` manifest describing data the researcher already
holds; nothing orders it). The project layer is the user's file: the server stores none,
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

## Order manifest

The order operation is the one place a logical `project_data.json` selection meets a
steward's physical delivery topology. It returns a complete manifest or every blocking
finding, never a partial order. HTTP and MCP both call the one implementation in
`reg-catalog`, which is what makes their results identical.

A `steward` artifact always orders from its compiled holdings, even when browsing in
reference scope. A `catalog` artifact orders by global fallback: canonical resolution
alone, each slice served by its canonical column under a blank table. It is the same
pipeline with a different matching arm, so there is no second clip or coverage
implementation. `provenance.mode` (`steward_holdings` or `global_fallback`) says which.
The project's `steward` must equal the artifact's name (`global` or its steward id);
`reg-core` accepts any slug there and holds no list of stewards, so this comparison is
the only check that a steward exists. Retargeting is not a feature, and a user who means
another deployment edits the JSON and uploads it again.

Per binding, in declaration order, over its source's period
([reg-core/DESIGN.md](reg-core/DESIGN.md)):

1. **Availability clip.** A source period means "these columns, wherever each is
   available inside this window". A dated period reaches the source variant's succession
   family (#1840, [reg-core/DESIGN.md](reg-core/DESIGN.md)): the `variant_replaced_by`
   component `show` reports as `variant_family`, or the variant alone. The request is
   split at member boundaries and each day goes to one member: the member whose states
   deliver it, else the named variant, where the clip removes it, so no variant is
   invented. The build guarantees a family is a chain whose members share no delivered
   day (`validate_built_db`), so the split needs no tie-break and holdings never decide
   it. A day the delivering member holds no table for stays with it and blocks as a
   coverage gap whichever member the source names; a holding under a member that does
   not deliver the day never takes it (conformance
   `order/variant-family-held-elsewhere`). `_default` resolves the named variant
   exactly. Each binding is then resolved once over the members' shares and clipped to
   the union of its states there, so a column first delivered in 2019 under a 2018–2020
   source does not widen the order into a cross-product, and a column only one member
   delivers (LISA's 16+-only columns) gets one clip, not a block for the other member's
   years. Every clip is reported, one per binding (requested: the source period;
   ordered: the union across members), never silent and never an error, and is recorded
   before the ambiguity gate can stop. Each slice keeps its member through matching,
   coverage, emission and held keys, so a family source yields entries under each
   member's `logical.variant` and its files are each member's own tables (conformance
   `order/variant-family`, `order/variant-family-global`); the manifest shape does not
   change.
2. **Representation slicing.** The clipped request splits into slices of constant
   canonical representation. A sequential rename makes two slices; two columns valid at
   the same instant without a `representation` pin is ambiguity and blocks. Resolution
   runs once per requested segment, because the window fallback is decided per query; a
   state reaching two segments stays one state. Steps 1 and 2 are one pass shared with
   validation, so the two never disagree about availability.
3. **Matching and coverage.** A table matches a slice when one of its columns serves
   `(variant, variable, representation)` over the slice ("Read scope and artifact
   admission": a table's one representation also serves a spelling SCB did not deliver
   beside it, so a rename inside one held table is two entries of one physical column)
   and its edition overlaps the slice; only the intersection contributes. That fallback
   is decided per instant, not over the slice: it serves exactly the slice's instants
   where SCB did not deliver the mapped spelling, the inventory's `_fallbacks` rule
   (conformance `api/held-projection-served-past-co-delivery`). Coverage
   (`states::held`) still judges the fallback over each state's whole bounds, so where a
   state spans a co-delivered instant it under-reports what the order serves; it never
   over-reports. A held edition that overlaps the request but lies wholly outside the
   binding's column windows is `column_window_unavailable`. Any uncovered subperiod
   blocks the whole order (`coverage_gap`); a slice nothing serves is `mapping_missing`.
   The materializer never chooses between tables: the inventory's one-to-one invariant
   leaves at most one `(table, column)` per cell instant per partition, a spelling a
   table serves through its one representation included, so every contribution is wanted
   whole. A wide-pivot member set
   ([../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md), "Holdings resolution
   invariants") is that one location: every position member serves the table's edition
   and is emitted, while a year or month member serves only its own period, so it
   matches and covers only where that period reaches the slice. A column window
   (`holding_mapping.window_lo`/`window_hi`, #1645) likewise limits a mapping to its own
   period: the column matches, covers and is emitted only there, and a request outside
   every window of a column is a gap, never served from its table's edition. A declared
   projection's value mapping (`holding_table.projection_of`, #1634) is dropped wherever
   its parent serves it: a parent mapping serves the same canonical triple over that
   instant (its own spelling, or one its one representation serves where SCB did not
   deliver that representation anywhere in the projection mapping's period), at the same
   position. A projection's position member is served only by the parent's member of
   that position, and a plain column, a year or month member or a windowed mapping only
   by a parent mapping that is not a position member. The parent serves a cell only
   where its own served period (its edition, or the mapping's member or window) covers
   the instant the projection's mapping serves: a projection cell outside the parent's
   window stays the projection's and counts as its own cell for emission (conformance
   `order/projection-windows`). This is the inventory's rule
   ([../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md), "Projections"), which
   refuses any other overlap, so the parent alone serves each shared cell and no
   projection cell is dropped without a parent cell in its place. A key mapping (the
   bound variant's `panel_entity_key` or `panel_time_key`, a bare slug or a JSON list)
   or an identifier mapping (`is_identifier`, a linking id included) is never dropped:
   each file carries its own row keys, and a projection may sit at another grain (EKB's
   household file beside its person file, keyed by a household id the person file also
   holds), so an ordered id emitted from the parent alone would leave the projection's
   rows unjoinable on it. This filters ordered matches only; which keys a file gains
   unordered is step 5's narrower test. Tables without the declaration are never
   filtered.
4. **Emission.** Every matching table is emitted whole, every partition included. A
   projection's output file (its variant and table edition, below) is emitted when the
   projection contributes at least one cell its parent does not serve anywhere in that
   file, key or non-key, from any source or period; its shared key and identifier cells
   then come along with it. A projection file whose ordered cells its parent all serves
   is not emitted, its shared keys included, so no file holds only keys its parent's
   file also holds. Entries carry the literal physical table and column; the canonical
   representation is a join key, not an output substitute. Entries keep project order,
   then table, edition, member index and column, so members order by index (`DIA2`
   before `DIA10`), not by name.
5. **Held keys.** On a steward artifact every emitted file then carries its table's held
   row key, whether or not the order binds it: each column of the file's table that maps
   a key of the file's variant (the key test of step 3: the variant's `panel_entity_key`
   and `panel_time_key` only) and that no entry of the file already extracts, under
   whatever variable, gains an entry marked `key: true`. A linking id (a family,
   household or workplace id, an employer's org number, any other identifier) comes
   along only when the order binds it (maintainer ruling 2026-10-23: a LISA year file
   carries LopNr, not its two dozen identifiers), and a variant that declares no panel
   key carries nothing extra: the fix is to declare its key, never to fall back to every
   identifier. A file is emitted only for an ordered entry, so keys never make a file on
   their own, and only what the table holds is added: a key the table does not map under
   the variant is not invented. A key entry takes the source of the file's first entry
   and requests the union of the file's entries' `requested_period`, so the file's row
   filter is not widened; a year or month member or a windowed key column requests that
   union inside its own period and is left out where the union does not reach it, as an
   ordered member is; a year-independent file's keys request `_default`. Key entries
   follow every ordered entry, by table, variant, edition, member index and column. An
   ordered key stays an ordered entry, unmarked. An auxiliary table's carried key
   (`holding_key`: a panel key the table holds but does not serve,
   [../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md), "Carried keys") is added
   the same way; it never matches an ordered binding, so the main tables alone serve a
   key order. The global fallback has no physical table and adds none.
6. **Row filter.** A file's requested period is the union of its entries'
   `requested_period`, held keys included. When the file's table declares a period
   column (`holding_table.period_column` and `period_encoding`, from the steward spec)
   and that union leaves part of the table's held period out, every entry of the file
   carries the same `filter`: `{column, encoding, ranges}`, where `ranges` are the
   union's ascending, disjoint intervals, joined with OR. A range's `from` and `to` are
   inclusive ISO dates, not the period grammar, so the extract runner compares without
   parsing it; a bound at the table's first or last held day cuts nothing and is
   omitted, so a range may be open-ended on either side. The runner reads the column per
   `encoding` (`date`, `year`, `yyyymm`, `yyyymmdd`) and keeps a row whose year, month
   or day lies in a range (a `datetime2` value compares by its day): a bound finer than
   the encoding widens to its whole year or month, never narrowing the request. A row
   whose period value is missing or unreadable matches no range, so a filtered file
   leaves it out and an unfiltered file keeps it (settled at the v1 freeze). No filter
   when the table declares no period column (it is extracted whole), when the request
   covers every held day, for a year-independent file, or in the global fallback.

**Fail closed, in one pass.** A blocked order lists every finding across every binding
(`steward_mismatch`, `project_unnamed`, `project_empty`, `period_not_orderable`,
`variable_unresolved`, `binding_unavailable`, `representation_unknown`,
`representation_unresolved`, `representation_ambiguous`, `mapping_missing`,
`coverage_gap`, `column_window_unavailable`) as structured fields of `order_blocked`, so
a researcher fixes the whole order in one edit. An empty project is a valid draft that
cannot become a header-only manifest; so is an unnamed one (a blank or whitespace-only
`name`), which cannot fill `provenance.project_name`. Only `order` reports these two:
`validate` judges the draft, which may be both.

**The manifest is a versioned JSON contract.** It carries the extract format (the
project's `extract_format`, resolved to `dta` when the project sets none, so the runner
has no default of its own), provenance (mode, steward, artifact kind, project name,
schema version, declared reg_meta version and SHA-256 of the project's canonical JSON,
catalog schema version and `generation_id`), the entries (logical coordinate with the
canonical representation, the clipped `requested_period`, the physical output `file`
name, edition, table, column, optional partition, optional projection token and optional
`member` index, `key: true` on a held key no binding ordered, and the file's optional
row `filter`) and the clips. It is written here and read offline by the steward-side
extract system, so it is self-contained. That system is the MONA extract runner
([../reg_mona_runner/DESIGN.md](../reg_mona_runner/DESIGN.md)): a data manager runs it
inside MONA, it writes the files there, and nothing row-level leaves. Its bytes are
sorted keys, two-space indent, UTF-8 and a trailing newline, byte-identical across runs;
periods render through the shared grammar, except a row filter's ISO day bounds. No
timestamps: the only time-shaped values come from the artifact and the project.
`partition`, `projection` (the table's token), `member` (`{"position": N}`,
`{"year": YYYY}` or `{"month": "YYYY-MM"}`), `stata_name`, `key`, `filter` and a filter
range's `from` and `to` are the only optional fields; absent means none (an ordered
entry for `key`, an unfiltered file for `filter`, an open end for a bound), so an
inventory without partitions, projections, pivot members or period columns, and an order
that binds every held key, produces the bytes it did before any existed. The parent
(`projection_of`) is not in the manifest: the token is the placement.

**A `dta` order renames a column `.dta` cannot hold.** A Stata variable name is 1 to 32
characters, a letter (general category L\*) or `_` first, then letters, ASCII digits and
`_`, and not a reserved word nor `str` and a digit (Stata's rule, stricter than what
ReadStat writes). In 0.61.0, 274 SWECOV columns (68 distinct names, the
financial-statement and pregnancy-register tables) are over 32 characters and 4 more
hold a space, `-`, `,` or `(`; no two of their aliases meet in one table. On a `dta`
order of a steward artifact every entry whose column fails the rule, held keys included,
carries `physical.stata_name`: the column's first 25 characters, each that is not a
letter, ASCII digit or `_` made `_` (and `_` put first when it would start with a digit
or `str` and a digit), then `_` and the first 6 lowercase hex digits of the SHA-256 of
the column's UTF-8 name, 32 characters at most. The runner writes the column under it,
with the full name as its variable label. The alias is a function of the name alone, so
a column takes one alias in every table, order and catalog generation, and two columns
sharing their first 25 characters (up to nine in one table do) differ in the hash.
Truncation with a counter would read better but makes a column's name depend on its
table's other columns, so a new sibling could rename it under a researcher's do-file. A
hash collision within one file is not handled here: the runner refuses a file that would
hold two columns of one name. Nor is a name over 80 characters, a Stata label's limit
(the longest held name is 65): the runner refuses it. `parquet` and `csv` orders keep
every name.

**Version 3 is frozen** (2026-10-23; version 2, frozen 2026-10-23, had no `stata_name`
and refused `.dta` orders of long names; version 1, frozen 2026-10-22 with its first
reader, the extract runner, had no `extract_format`; both are retired). Every object is
closed; this is the whole field set:

- root: `version` (the integer 3), `extract_format` (`dta`, `parquet` or `csv`),
  `provenance`, `entries` (non-empty), `clips`;
- `provenance`: `mode`, `steward`, `project_name`, `project_schema_version`,
  `project_reg_meta_version`, `project_hash`, `catalog_schema_version`,
  `catalog_generation_id`, `artifact_kind`, all strings, all non-empty (`project_name`
  is never blank: an unnamed project blocks as `project_unnamed`);
- entry: `source`, `logical`, `requested_period`, `physical`, optional `key` (only
  `true`) and optional `filter`;
- `logical`: `provider`, `register`, `variant`, `variable`, `representation`;
- `physical`: `file`, `edition`, `table`, `column`, optional `partition`, `projection`,
  `member` and `stata_name` (only on a `dta` order); `member` holds exactly one of
  `position`, `year`, `month`;
- `filter`: `column`, `encoding` (`date`, `year`, `yyyymm` or `yyyymmdd`), `ranges`
  (non-empty); a range has optional `from` and `to`, inclusive `YYYY-MM-DD` days;
- clip: `source`, `variable`, `requested_period`, `ordered_period`.

Rust writes only these fields; the runner enforces the set, refusing a version other
than 3, a mode other than `steward_holdings`, another steward's order and any unknown
field. So any change to the shape, an added optional field included, is version 4: a v3
runner refuses it rather than misreading it.

The manifest stays pretty-printed. MONA's upload limit is 10 MB; a typical order is
about 0.4 MB and the widest LISA order about 7 MB. Compact JSON would roughly halve
that, but it is a second encoding beside the one the goldens pin, for headroom no real
order needs yet. The runner parses compact and pretty JSON alike (its tests run a
compact order), so a compact download needs no runner change. `simplify:` pretty output;
switch the download to compact when a real order passes 8 MB.

**Extraction keeps delivery topology.** One output file per physical table and table
edition (`physical.edition`, #1661), whatever variants bind it. `physical.table` is the
unique `physical_id`, and the partition and projection token are facts of the table, so
the edition only names the file's period. A table several variants map is one delivery,
so it is one file holding every ordered variant's columns, and its held keys are the
keys of each variant its entries bind. An entry's `requested_period` keeps the clipped
days it serves (availability, a column window, a year or month member) but never splits
a file: a year or month member, a windowed column and the ordered key of one table land
in the table's one file, whichever sources and years ordered them. A file's columns are
the distinct physical columns of its entries: two entries for one column (two sources,
or two periods) are one extracted column. Since a file holds its table's rows for every
requested period, a split by clipped period would only repeat the same rows and separate
columns from their keys; the file's row filter (step 6) narrows its rows to the union of
its entries' requested periods instead. In the global fallback the edition is the
entry's merged requested period, so nothing changes there.

The file's name, `physical.file`, is a fact of the table, never of the order (a
maintainer ruling, 2026-10-23):
`<provider>_<register>_<variant>[_<placement>][_<period>]`, plus the extract format's
extension. The variant is the table's own: the variant that maps the most of the table's
columns, ties to the first by provider, register and variant slug, so a table several
variants share keeps one name whatever the order binds; `_default` is left out. The
placement is the partition or the projection token. The period is the edition when it is
one period (a year, a month or a day); a multi-period table's file holds every period
and its name has none (`scb_lisa_individer-16plus_2020.dta`,
`scb_lisa_foretag_2020.dta`, `region-skane_primarvard.dta`). Two tables of one variant,
placement and period would share that name (106 of the 1432 SWECOV tables in 0.60.0,
such as `Partner_2005` and `RTBFamilj2005`), so a name more than one catalog table takes
gains the table's `physical_id` in slug spelling
(`scb_rtb_familjer-fran-1998_2005-12-31_partner-2005`). The order computes this from the
catalog it reads, so the manifest carries the final name and the runner never derives
one; adding a sibling table to the catalog can lengthen an existing table's name
(accepted pre-v1). The runner still refuses two files of one order whose names differ
only in case. In the global fallback there is no table: the name is the entry's own
coordinate and requested period. What goes in as two tables comes out as two files:
shard identity (reporter stream, municipality) may exist in no column, so a union would
destroy information, and a projection may have a finer grain than its parent. Each file
carries its own keys, ordered or added as held keys (step 5), so a researcher who orders
one non-key column still gets a joinable file; the user joins a projection file to its
parent's on the shared key. The extract runner writes a key entry's column like any
other and may tell it apart by `key`. A multi-period range is one file, filtered to the
requested ranges when its table declares a period column.

Design limit: sibling projections cannot share a key the parent lacks. Two projections
of one parent that both map a key the parent does not map (a child id two narrow
deliveries carry) are two claims on one cell, which the inventory refuses as
`inventory_cell_conflict`. The parent is the only table a projection's cell defers to,
so a sibling cannot serve another sibling's key. Revisit when a steward delivers such a
pair: it needs either a declared shared-key set or a projection chain.

`simplify:` a table without a declared period column is extracted whole, whatever part
of its edition the order requests. Declare one (the steward spec's `period_column`) when
a large multi-period table's whole extract is the cost.

**Deferred: companion tables.** Steward deliveries include lookup and crosswalk tables
that are not register data (VaraText beside IVP, skolkod↔skolenhetskod, the FEK
`PeOrgNr`↔`FENr` key tables, `LopNrByte`). The SWECOV generator skips them for now; read
that as awaiting this mechanism. A small closed code set becomes a catalog value set or
classification. A large lookup or any crosswalk or key table becomes a physical
companion: a table-level `companion_of = "<provider>/<register>"` in the inventory, with
no mappings, shipped with any order touching that register. Sensitivity is an access
fact: a pseudonym-bearing crosswalk must be a companion. It needs a manifest entry
without a logical coordinate and one materializer arm. Open: `LopNrByte` belongs to the
steward rather than a register, and ordering it may deserve its own access decision.

## Project semantic validation

The validate operation runs the supported-version decision, then the structural
validator ([reg-core/DESIGN.md](reg-core/DESIGN.md)), then the semantic layer here, and
concatenates their issues. A failing project is a result, not an error. A structural
failure skips the semantic layer, so a rejected document costs no catalog read. A
document whose `steward` is not the serving catalog's is one `steward_mismatch` error at
`/steward` and nothing else, as `order` refuses it: every other issue would come from
another steward's holdings.

Per source family (the source variant's succession family under a dated period, the
variant itself under `_default`; "Order manifest", step 1) and binding:

- The variant must resolve, and the binding's variable must resolve, else
  `fqid_unresolved`.
- Availability is the order's shared pass (steps 1 and 2 above); this layer only
  translates its facts. Narrower availability (a leading or trailing shortfall, an
  internal gap, a pinned representation covering only part) is no finding: the order's
  `clips` and the timeline state it (#1836). A list period (`2005..2010,2015..2020`) is
  one request with holes: a column delivered or co-existing only inside a hole is
  neither availability nor ambiguity. Empty availability across the whole request
  blocks. The pass's findings take this surface's names: `binding_unavailable` →
  `period_outside_state_validity`, `representation_unknown` →
  `binding_representation_unknown`, `representation_ambiguous` →
  `binding_value_set_version_ambiguous`, `representation_unresolved` unchanged. The pass
  runs once over the family, never per member with a merge: a member that does not
  deliver a column would otherwise block it for that member's years.
  `binding_state_drifts_within_period` (info) reports a sequential state transition
  inside the request. It counts the distinct (canonical column by `fold_identity`, value
  set) pairs among the states the request reaches: the order's partition of the request
  by column, refined by coding (#1899). So a boundary is a transition only where the
  representation or value set changes across it (LISA's `SyssStat` → `SyssStat11`),
  inside one member as well as between members; a state boundary, gap or member boundary
  one column simply runs through is not, and neither is a difference in any other fact,
  such as the data type or the `source_register_text` (LISA's `cfar-nummer`, RAMS →
  BAS), for which the order emits one entry. A value-set change on a continuing column
  still counts, though the order emits one entry across it.
- `source_period_narrower_than_available` (warning, at `/sources/<i>/period`) is the one
  period finding, the reverse of a clip. It names the days inside the study window a
  period widened into them would order, across the whole family, minus the source
  period's days, when not empty. A day counts when some binding serves it (the same pass
  asked for the whole window; on a steward artifact, only the days a holding serves) and
  no binding would block on it: available but not held, or a finding such as co-existing
  representations. So following the warning never blocks the order on availability. A
  binding whose own period misses all its data still counts, so its
  `period_outside_state_validity` comes with where the data is. Day grain, so a monthly
  or half-year period is well defined. No window, a `"_default"` source or a source with
  no resolved binding warns nothing. A list period narrower than the data warns on
  purpose, as do per-year panel members of one variant.
- `deprecated_traversal` (info) and `variable_replaced` (info, with `successor_fqid`
  when the successor is a binding) are hints; the binding stays valid.
- `variable_sensitive` (warning) marks a binding whose variable is `is_sensitive`. It is
  flagged, never blocked (#1547): the steward decides whether to share it, and every
  client of `validate` (the SPA's cart, MCP) sees the same warning.
- A binding's `value_set` must name a known classification, else `value_set_missing`.

A clean validation means the project resolves; it is never proof of physical order
readiness, because matching and coverage do not run here. This layer reads identity and
state metadata, never code membership.

**Representation, not `@version`.** One concept can have several delivery columns at the
same instant (SSYK at 3, 4 and 5 digits; age brackets). When two or more columns overlap
inside the requested instants and the binding sets no `representation`, the extract
would pull several columns: `binding_value_set_version_ambiguous`. Distinct columns in
non-overlapping windows are a rename, not ambiguity. A `representation` the catalog no
longer delivers is `binding_representation_unknown`.

A defensive backstop (two available states with different value sets at one requested
instant on one column) is unreachable from an admitted artifact, because two build
invariants close it: no two overlapping states of one column carry distinct non-null
value sets, and no code-less state overlaps a code-bearing one. The second is skipped on
steward builds, which is safe only while the steward overlay adds code-less states for
its own variables alone. If an overlay ever gains value sets or states on base
variables, revisit the skip.

**Steward membership.** On a steward artifact, a binding the steward holds no column of
is `fqid_outside_steward_catalog`, and a held concept whose resolved column is not held
is `representation_outside_steward_catalog`, naming what is held. Both are warnings, so
an uploaded project can be inspected; ordering blocks the same conditions with its own
findings. Membership uses the literal authored FQID at any family member the period
reaches (the named variant under `_default`), and a resolved column must be held under
its own member; a `same_as` edge grants nothing. Catalog artifacts emit neither.

New variables, registers and classifications onboard through slug-TOML changes to the
build; a steward publishes a new compiled generation. Data without an FQID cannot be
authored.

## Search

Four FTS5 indexes serve search: `register_fts`, `variable_fts`, `classification_fts` and
`value_code_fts` (the build design note says what they hold). Each stores `fold_search`
text under `unicode61 remove_diacritics 0`, so the tokenizer only splits and
`fold_search` (`reg-core`: full case folding, NFKD, combining marks dropped) is the one
fold, applied to indexed text by the build and to the query by the reader. It matches
what `unicode61` alone does not: `straße`/`strasse`, `ﬁlm`/`film`, `ＡＢＣ`/`abc`.
Display text always comes from the base tables. `value_code_fts` is contentless
(`content=''`): its `label` reads back NULL, so a value hit joins its rowid to
`value_code` for the code and label, and ranks with `bm25`, which needs the per-row
sizes the index keeps. The docs index uses the same fold and query builder.

A call returns one ranked list; the SPA makes one call per typed group it shows.

Each query token becomes a quoted prefix term, which neutralizes FTS5 operators and
prefix-matches (`ink` finds `inkomst`). `LIKE` patterns escape `%` and `_`. Arms over
authored names compare under `fold_identity` on both sides, because SQLite's own case
folding is ASCII-only; that folds case, not diacritics.

Every token must match (AND), within one row. The variable arm widens that one step: a
query of two or more tokens also admits a variable whose register's name or purpose
supplies some tokens and whose own text supplies the rest, so `covid provtagning` finds
SmiNet's `Provtagningsdatum` though only the register mentions covid. A register token
alone never lists a register's variables. These context rows rank after every variable
whose own text matches the whole query, which keep their order; a single-token query
admits exactly as before. Synonyms and cross-language expansion are #1490.

There are five arms: register, variable, classification, classification code and
register value. A code-shaped query (three or more characters, at least one digit) also
matches codes exactly or by prefix, and surfaces the classifications that contain a
matching code, ranked after name hits. Classifications are catalog-wide and have no
validity window, so a register or period filter turns the classification arm off.

Ranking and paging rules:

- Pins come first, then one best-bets order: an exact identity match leads, then type
  priors, then relevance. A researcher typing a whole name ("Kön") gets the variables
  carrying it first, however many registers share it. A code's identity is its code
  value, never its label: a value label that happens to name a research concept
  (`Provtagning`) is an incidental hit and ranks below the variables and registers that
  match it (#1464), while a code lookup (`C12`) still leads.
- Every arm takes one bounded candidate prefix of 1,001 rows, folded or not, so later
  pages cannot reorder earlier ones or turn a consumed leaf into a group row.
- Prefix and group-label promotion switch off when more than 50 candidates match by
  identity, so a generic prefix cannot pull hundreds of weak hits forward.
- Paging stops at depth 1,000; a forged cursor cannot ask for an unbounded prefix, and a
  researcher at the ceiling refines the query. Cursors bind to the query, filters, scope
  and generation.
- Value hits rank by bm25 down-weighted by how many variables share the label, so a
  generic enum label ranks below a discriminative one; each shown code carries a bounded
  sample of its owners and their counts.

**Folds.** Before paging, classification editions on one succession chain collapse into
one `classification_succession` row (the terminal edition, with every edition listed and
the number hit), then two or more members of one concept group collapse into one group
row. A lone hit stays a leaf; an old-vintage lone edition carries `terminal_fqid`. Group
labels also match on their own. Each folded row counts once. The succession fold runs
first so a collapsed terminal can join a curated umbrella group.

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
