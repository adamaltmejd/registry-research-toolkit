# Design: the Rust runtime

Design rationale and constraints for the catalog runtime: `reg-core` (grammars, folds,
interval algebra, the project contract), `reg-catalog` (the reader and its operation
set) and `reg-meta` (the `serve` and `mcp` run modes), plus the design of the builder,
`reg-build` ("The catalog builder"), which builds the artifact these crates read. The
project contract has its own note, [reg-core/DESIGN.md](reg-core/DESIGN.md). Holdings
are a separate layer compiled after the artifact ("Holdings"). Cross-package topology is
in [../ARCHITECTURE.md](../ARCHITECTURE.md).

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
  build ("The catalog builder"), which also keeps the reader small.
- **Start-up does not matter**, because the runtime runs as long-lived processes
  (`serve`, `mcp`). The case for Rust is one implementation of each shared contract
  across build and server, typed contracts, and memory use.

The HTTP server is Rust rather than a Python server calling Rust through bindings. With
bindings every result type would exist twice, as a Rust struct and as a Pydantic model
for OpenAPI, which is the drift this design removes. Most of the old backend was catalog
logic (node assembly, search ranking, semantic validation) that belongs in the operation
set, where agents get it too. The deploy image is one static binary plus the baked DB
(the 1 GB Fly VM is kept for the page cache), and local MCP needs no Python environment.

## One implementation per contract

Each contract that crosses a language or package boundary has one implementation, except
the isolated MONA manifest boundary validator named below. New code that needs one of
these calls its home; it does not add a second copy held in step by a parity test.

  | Contract                                | Single home                                                                                                                 | Reached from                                                                                                    |
  | --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
  | Artifact schema (DDL)                   | `reg-build/src/schema.sql` (Rust builder)                                                                                   | `reg-catalog` reads it; admission by the schema version                                                         |
  | Catalog schema version                  | `reg_core::CATALOG_SCHEMA_VERSION`                                                                                          | `reg-build` stamps it; `reg-catalog` admits on equality; the deploy guard compares it with the newest release's |
  | Artifact validation                     | `reg-build verify`                                                                                                          | The single authority; the release tier and the release skill run it                                             |
  | Resolver semantics                      | `reg-resolve` (Rust)                                                                                                        | Compiled into the artifact                                                                                      |
  | FQID and period grammar                 | `reg-core`                                                                                                                  | `reg-build`; reader                                                                                             |
  | Interval algebra over periods           | `reg-core`                                                                                                                  | `reg-build`; reader                                                                                             |
  | Text folds                              | `reg-core`                                                                                                                  | `reg-build`; reader                                                                                             |
  | Canonical JSON + SHA-256 (build hashes) | `reg-frame::canonical_line` and value-set hashing (`src/lib.rs`); `reg-build`'s `main` module (input and generation hashes) | Build only                                                                                                      |
  | Project schema + structural validator   | `reg-core` (`project::check`)                                                                                               | Server and MCP; JSON Schema export                                                                              |
  | Result types                            | Rust structs (serde + utoipa)                                                                                               | OpenAPI and MCP tool schemas derive from it                                                                     |

**Named exception: the MONA manifest boundary.** `reg-core` owns the manifest types and
exported schema; the self-contained R runner independently validates the closed manifest
and its SQL/writer safety constraints. The uploaded self-contained runner ships no Rust
library or binary. Keep this small explicit exception rather than adding an R
JSON-Schema dependency or pretending that JSON shape validation replaces SQL/type
checks. Neither R nor the webapp resolves logical orders independently. E's runner PR
changes the R validator and manifest corpus together.

The builder crates link `reg-core` directly. A `reg-core` change, an interval or grammar
fix included, is therefore a builder change: `reg-build diff` against the previous
release shows any artifact change it causes.

**Text folds.** Specified rather than inherited from a library:

- `fold_identity`: Unicode lowercase with the Final_Sigma context rule, no normalization
  (`str::to_lowercase`; Python's `str.lower()` agrees). It is the column identity rule.
- `normalized_search_query`: split on whitespace, join with single spaces, then full
  case folding (`ß` to `ss`). Evidence comparison uses it.
- Whitespace follows Python, not Rust's std: `White_Space` plus U+001C..U+001F.

`reg-build` fills folded columns with `reg-core`'s folds, the code the reader applies to
the query string. All of `reg-core` uses one Unicode version
(`tests/unicode_version.rs`). The oracle is `conformance/cases/folds/`.

**Canonical JSON** has no floats. A float in a hashed payload is a build error.

**Messages are structured.** Errors and blocked-order findings carry a code and fields;
prose is rendered from templates. Goldens assert codes and fields, so no language's
`repr` quoting appears in a contract.

## Crates and run modes

```text
reg-build: readers → frame → resolve → emit         uses reg-core
                 │
                 ▼
reg_meta.db (documents inside), immutable
                 │
reg-catalog: one operation set
        ┌────────┴─────────┐
reg-meta serve        reg-meta mcp
HTTP API             MCP over stdio (local catalog)
+ /mcp (hosted)
```

- `reg-core`: no IO. FQID and period grammar, folds, interval algebra, project-data
  types and the structural validator, error codes. Compiles natively and to WebAssembly
  (`reg-core-wasm`, for the SPA).
- `reg-catalog`: the reader. Opens and admits an artifact and defines the operation set:
  one function per operation, typed parameters and results with JSON Schemas. Search
  (one ranking), the logical order and project semantic validation live here.
- `reg-meta`: the binary, with run modes only. `serve` is the HTTP API (axum + utoipa;
  early `ETag` revalidation, body-size limit and rate limit as tower layers) plus the
  remote MCP endpoint over streamable HTTP. `mcp` is MCP over stdio against a local
  catalog, for offline use. There is no query CLI.
- `reg-export-gate`: the export gate, the disclosure check on aggregates leaving the
  data side. It reads no catalog. Its design is
  [reg-export-gate/DESIGN.md](reg-export-gate/DESIGN.md).
- `xtask`: repository tooling, not shipped. `cargo xtask gh-issue` is the
  maintainer-author trust gate for issue reads; its tests hold the repo-policy lints.
  The release skill runs `release-base-reuse` (step 8a) and `scope-check` (8h, the
  holdings asset's content gate). The scope check sits here, not in `reg-build`, so the
  gate on the public asset stays independent of the code that writes it; it reads the
  builder's DDL source but keeps its own meta-key allowlist and size bounds.

Each crate's `Cargo.toml` is its dependency list. Adding a runtime crate is a design
decision, not an implementation detail. `toml` is a runtime dependency only of
`reg-readers`, which reads curation for the builder; the runtime keeps it a dev
dependency.

**Distribution.** The server image is one static binary plus the baked `reg_meta.db`,
fetched in the Dockerfile with `curl`, SHA-256 verification against GitHub's asset
digests, and `zstd`. Each `reg_meta/v*` release ships `reg-meta` binaries for macOS
arm64 and Linux x86_64 with SHA-256 checksums, the export gate's `reg-export-gate`
binaries for the same targets (named by the gate crate's own version,
`reg-export-gate-<version>-<target>`; the Linux build links the runner's glibc, so it
needs an Ubuntu 24.04-era distribution or newer on the data VM), and two DB assets,
`reg_meta.db.zst` (documents included) and the paired `reg_meta_swecov.db.zst`, built in
the same run (the holdings pin the base's `catalog_sha256`). There is no separate docs
asset. The `reg-meta` binaries serve local HTTP or MCP; the catalog is downloaded with
`curl` and `zstd`. There is no PyPI package and no `fetch` run mode. Windows agents use
hosted MCP; a local Windows binary is added only if it builds and runs without special
effort. The `microdata-tools-se` plugin declares one hosted MCP server whose URL is a
user setting: the global catalog by default, a hosted steward catalog
(`https://data.swecov.se/mcp`) instead. A local `reg-meta mcp` is added to the agent's
MCP configuration by hand.

## The catalog builder

The builder is `reg-build`, with the crates `reg-frame`, `reg-readers` and
`reg-resolve`. This section holds its design decisions.

**Three stages, each seeing only its inputs.**

1. **Read.** A reader turns one pinned input into a *source frame*. The frame has one
   record shape for every provider: registers, tables, stated deliveries (releases),
   documented columns (occurrences, each with a period), value sets and source-side
   links. Every fact keeps a locator back to its input. Readers are the only code that
   names a provider or a file format. Their config (header aliases, type tables, the
   delivery-name grammar) is data under `curation/readers/`. Value-set normalization
   drops parents absent from the same set; classification readers report the count.
2. **Resolve.** One register's frame plus that register's curation becomes a resolved
   register: variables, segments, coverage, findings and provenance. Resolve is a pure
   function per register, run in parallel and cached by content hash. It never sees
   another register's frame.
3. **Link and emit.** Cross-register links are checked once over all resolved registers.
   The emitter writes one SQLite file, documents included, and decides nothing.

**Change findings (F1).** Resolve reports what changed in a variable's documentation as
`notice` findings, one per (variable, sub-kind, table or none, first day of the new
form), on that day, except for the grouped integer/decimal drift below. Neighbours are
compared: an `ambiguous` segment is skipped and a gap is not a change.

- `F1:type-change`: the stated type family differs between consecutive segments of one
  table. Width and length differences never create findings or reviews. One closed map
  in `reg-frame` assigns the exact frame dtype names to families; an unmapped dtype is
  refused at its source locator. Occurrences retain their normalized dtype and length.
  The neutral frame vocabulary is `int`/`integer` (integer), `decimal` (a separate
  family), `char`/`text` (text), `date`/`datetime` (temporal), `boolean`, `geometry`,
  `uuid`, `binary`, `unspecified` and `identifier`. Reader config normalizes source and
  format literals before they enter the frame. `unspecified` is recognized unknown
  evidence, not a family or a fallback for unmapped names. An unknown side never creates
  a type-change finding. Integer/decimal, text/integer, text/decimal and transitions
  between those families and temporal remain changes. Integer/decimal transitions
  produce one notice per (variable, table), dated at the first such transition; its
  message counts those transitions and names their first and last day. Other family
  changes retain one notice per transition. A curated `set type` that removes the drift
  leaves no type-change notice; switchers without strict evidence remain uncurated.
- `F1:domain-change`: the value set differs (NULL included), unless either side is
  uncoded. Skip an edition-only transition only when both sides bind versions of one
  classification family, the versions differ, each observed code set is a subset of its
  bound version's code set, and every observed code addition or removal is an edition
  addition or removal: `(old_set Δ new_set) ⊆ (V_old Δ V_new)`. These checks compare
  codes, not labels or hierarchy. An unstated observed set cannot establish the subset
  premise. Same-version transitions, local recodes, and code changes the edition does
  not explain still fire. The message counts codes added, removed and relabelled. This
  subset-and-edition rule amends D-9's former exact-set predicate.
- `F1:label-drift`, `F1:definition-change`, `F1:unit-change`: consecutive text runs
  differ. The definition message names the field, never the text.
- `F1:population-change`: the primary releases carrying the variable, in period order,
  state different population names. Undated releases and releases without populations
  are skipped.

V2 (value-set carry across a segment that states none) is a v4 rule not built: on the
first full build 7 coded segments of 166k sat between two neighbours with one value set,
spelling and type. A D `set` states those.

**Review proposals (F3, V1).** Proposals are review findings whose messages carry ranked
candidate statements as TOML in the existing curation grammar. They use the existing
findings output and never change identity, relations or classification bindings.
Accepting one requires reviewed curation and a premise lock.

- F3 compares successive, non-overlapping variables with different source keys,
  including different spellings. Label, type and value-set agreement rank candidates;
  spelling agreement is evidence, never a prerequisite. Within a register it proposes
  `set variable` when the predecessor has a ledger id; otherwise, and across registers
  of one provider, it proposes `link continues` from successor to predecessor. Existing
  continuations are excluded. Candidates meet at adjacent documentation boundaries; at
  most three are reported per successor, ranked deterministically by agreement.
  Proposals require representable whole-occurrence selectors; a cross-register
  predecessor also needs a ledger id so the link can address it.
- V1 compares unbound coded occurrences with inventory versions by value-set agreement
  (codes and labels), ranking exact matches before partial agreement. It proposes
  `set classification` for whole occurrences. Source code-system text and historic label
  rules remain evidence only; neither creates a binding. Inventory book periods do not
  imply occurrence applicability.

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

- Reader occurrences carry an opaque *source key*: the provider's variable id where one
  exists, otherwise the folded column name, scoped to the register. An asserted
  occurrence without a `key` guard currently gets a per-statement source key
  (`stmt:<scoped identity>`). Aligning that default with the folded column is deferred,
  because it changes published identities.
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
  `derived-from` links and tags.

**Three identifiers.**

- **Source key:** evidence only.
- **Public id** (`provider/register/variable`): stable through a per-register ledger
  (`ids.toml`) that maps each id to an anchor occurrence, so ids survive renames and
  splits. The first ledger is generated from today's slug pins. A release build refuses
  a variable without an id, or a ledger id reported unplaced (no placed target) or split
  (anchors reach several variables). The latter refusals name the ledger file, line and
  id; dev builds keep reporting them without refusing. Build collects publication
  refusals across registers, including refused curation and public-name collisions,
  before placing a catalog. With `--report`, it writes the normal report plus `refusals`
  (register, kind and full details), even when publication is refused. Inventory-wide
  refusals have no register; all curation refusals retain their source locations. Stderr
  summarizes counts; malformed inputs that prevent resolution still abort early.
- **Row id:** renumbered every build and never exposed.

**Curation.**

- A `link` or `ack` changes exactly its selected subjects or findings. Source links are
  not displaced to accept a conflicting curated link. With no statements, artifact bytes
  are unchanged.
- Four statement kinds: `assert`, `set`, `link` and `ack`. `bind` and `assign` are gone:
  the ledger names registers and tables; moving a row or assigning its variable is a
  field change.
- Each register has one optional `curation.toml` beside `ids.toml`, containing
  `[[statement]]` tables. Statements have no hand-written ids. Register-local locks hash
  normalized typed content, including caveat but excluding evidence, file and line.
  Evidence is commentary, not a premise: rewording it moves no lock key, no asserted
  occurrence's `stmt:` source key and no ledger anchor (maintainer, 2026-11-04).
  Publication hashes also include provider and register, preserving separate authoring
  locations for identical statements in different registers.
- The typed model and address grammar live in `reg-frame`; the TOML loader lives in
  `reg-readers`. Matching, conflicts, premise validation, application and provenance
  belong to `reg-resolve`; `reg-build` owns lock IO and located diagnostics. The emitter
  writes resolved decisions.
- Application is one ordered pass: parent table/release assertions, table coverage
  overrides, occurrence/release period overrides, table moves, remaining assertions,
  variable assignments, ordinary fields, then links, then acknowledgements. Each stage
  reads an immutable snapshot; later stages see the accepted corrections. Coverage
  precedes period checks, and corrected periods precede moves and occurrence assertions.
  A coverage or period override must be a separate statement without other fields.
  Coordinate changes carry existing provenance forward without changing its reviewed
  source values. There is no fixpoint or second application pass.
- `set table = "<slug>"` moves selected whole occurrences to an existing table before
  default resolution. It does not merge source rows or rank sources. Disagreements at
  the destination follow the existing F6 and ambiguity rules; a curator corrects the
  disagreeing field with `set`. A moved release occurrence becomes dated and detached;
  unplaced releases and population-specific value sets cannot be detached.
- A selector's period must cover every matched occurrence whole. A partial selection
  refuses with `selector period cuts occurrence …; select whole occurrences`. No
  operation splits source occurrences. Selectors read an immutable phase snapshot.
  Overlapping field changes and a table move that consumes rows another move selects are
  conflicts at both locations; moves never form a chain.
- Assertions add source-shaped occurrences, tables, releases, content-addressed value
  sets or classification versions with curated provenance. Tables and releases precede
  their consumers. Code lists are defined before occurrences consume their hashes;
  attachment to selected occurrences follows occurrence creation. A value-set assertion
  fills an occurrence whose source states no set; replacement needs `set`. Existing
  facts remain assertion refusals. Classification versions of a family cannot overlap.
- `set variable = "<ledger slug>"` assigns identity during resolution, or `"none"`
  withholds the selected occurrences. An explicit `set variable` changes the variable of
  exactly the selected occurrences; every unselected occurrence resolves as if the
  statement were absent. A default group with no selected member keeps its default id.
  Only a group with both selected and unselected members re-derives its inherited id
  from anchors reaching its unselected members, falling back to the default id if none
  reaches them; a dated anchor identifies the whole remaining group, not only its anchor
  day. This prevents the default pass's id-order winner from absorbing a curated split.
  Anchors spanning several default groups retain default identities rather than merging
  those groups. The same exact-selection rule governs every field override: change only
  selected whole occurrences, or the selected entity for fields owned by tables,
  releases or variables. Selectors and premises use the immutable phase view.
  Assignments cannot merge concurrent different folded spellings in one table;
  resolution refuses with `D-11` at lock, before a malformed segment can be published.
- `label`, `definition`, `operational_definition`, `unit`, `value_set`, `coded`,
  `spelling`, `identifier`, `sensitive`, `type`, `length` and `format` belong to
  occurrences; text runs and segments derive from those occurrences. `period` changes
  occurrence bounds or the addressed release's placement. `description` on tables and
  releases stays on those entities. `coverage` replaces a table's source bound and
  requires a table selector and one contiguous period; it lives in the table's existing
  bounds. The overridden source bound remains evidence and the review premise, so a
  source-bound change reopens review. `sets` and `tags` belong to variables; typed
  curation decisions resolve to variable memberships. Tags use the existing variable tag
  storage; register tags are derived by the reader, never curated or propagated to
  variables. `notice` resolves to a finding on its selected subject. The emitter
  serializes these resolved facts and never derives them from provenance. Occurrence
  descriptions are not a field: source wording stays in occurrence labels, definitions
  and operational definitions. A variable has one optional curated `description`, owned
  by `var:<id>` and stored in `variable.description_text`. An occurrence description
  setter is a located `description-subject` refusal; different descriptions for one
  variable are refused, not selected by period or precedence. Table and release
  descriptions remain fields of those entities. No fact gets a second semantic home. A
  value-set override chooses the occurrence's own set while retaining its source
  population alternatives as evidence. Unsupported entity/field combinations are located
  refusals. Type corrections that change the family participate in F6 and ambiguity
  resolution; length corrections do not. Corrected conflict markers clear while the
  original premises remain evidence. Format is carried and published without changing
  agreement or F1 findings.
- **One variable, one description (maintainer, 2026-11-02).** The one curated
  description states both what the variable means and how it is measured or derived.
  There is no separate curated operational definition. For example: "Usual weekly hours
  in the main job; the sum of HuVanlTim and BiVanlTim; from 2021 agreed hours are used
  where usual equals agreed hours." The provider's definition/operational definition
  split is source structure, retained as evidence and in the explicitly marked provider
  fallback when a curated description is not yet available. Spelling variants and
  wording drift are consolidated into one text. A minor change in scope or exact
  definition belongs in that description with its change and year. A material change in
  meaning requires a separate variable. Temporal succession uses `continues` with an
  evidenced `at`; different meanings in parallel tables require a candidate split,
  without a succession edge. Historical codings distinguished within one column do not
  alone imply a split. Materially different meanings cannot be separate descriptions on
  the same variable. Source-field corrections and drift findings retain their existing
  semantics; correcting an occurrence's operational definition does not create a second
  curated variable description. Identity questions require review before splitting
  variables or assigning relation dates.
- Curated values beat rules, which beat source values. Losing source values remain
  evidence in the existing `statement`/`override` tables; every curated winner carries
  statement file, line, evidence and caveat. Segment evidence names its statement.
  Overlapping curated values for the same field conflict at both locations, even when
  equal. A value equal to the source produces a redundant-set finding.
- Premises capture the overridden source field and its dependencies. Coding changes
  depend on type, existing value sets and population alternatives; period changes depend
  on release placement and parent coverage. Source changes reopen review. With no
  statements, artifact bytes remain unchanged.
- `link` applies `continues`, `derived_from`, `equivalent` and `label_of` after field
  application and identity resolution. Endpoints use the selected whole occurrences,
  ledger variables or tables, or a resolved register/variable FQID. Release endpoints
  remain refused. Optional `at` periods become merged validity intervals. Each relation
  carries its statement and caveat. An exact source-edge duplicate gains curated
  provenance without changing the graph's topology.
- A table `continues` points from successor to predecessor. The real corpus drops no
  successions, so no successor-choice or `reject` form exists. Curated links that close
  a cycle or give a table a second successor on one day refuse at their statement
  locations. The existing link stage still drops invalid source-only successions;
  curation does not choose between them or change that evidence. Order following uses
  these same rows (`follow_successor`, D-10); continuation proposals use the findings
  output described above.
- `label_of` is a relation consumed by the runtime's graph endpoint. The converter's
  legacy code/label pairs created presentation groups; D-3c does not synthesize those
  legacy groups, change identity or alter order bindings.
- Acknowledgement validation and publication share the complete finding collector,
  including F3 and V1 proposals after curated links.
- `ack` retains the finding, its severity and message, and records the statement's file,
  line, evidence and caveat through `ack_statement_id`. Its declared severity must match
  the finding. The reviewed premise snapshots the finding and its supporting source
  facts; a missing finding is a located `stale-ack` refusal. Legacy finding codes are
  not guessed to be equivalent to new findings. Resolver findings are addressable
  through their variable, occurrence or release; F5 markers retain their register-level
  published finding but carry internal key metadata for exact acknowledgement.
  Link-stage source notices are not resolver findings and cannot currently be
  acknowledged. Acknowledgements do not change structural ambiguity: orders still refuse
  ambiguous segments. F5 is currently a notice, F6 a review; neither independently
  blocks orders. Notice acknowledgements annotate; there is currently no separate
  generic review-finding order blocker to suppress. F7 edition findings remain warnings.
- `classification` selects an exact curated `class/<family>/<version>` for whole
  occurrences. The curated frame carries this optional version through resolution;
  readers leave it unset, and the source JSONL reader refuses it. Referenced inventory
  facts are locked premises; missing references are located refusals. The selected
  version follows occurrences into segments and orders. A value set links to one version
  only from curation (clean-room D29; #2082): `equal` when it is that version's content,
  or `subset` when every segment curated to a version names the same one and the set's
  codes, sentinels aside, are that version's. Several versions, a binding its codes do
  not support, or content shared by several unbound versions leave the link unset;
  containment never makes one. Variable `tags` applies as normalized membership,
  independently of identity, binding, ordering and peers. Table and occurrence `role`
  values use one validated vocabulary for each entity kind. `unit_type` populates the
  table's unit-type reference; variables do not inherit it. Table setters require a
  source-stated or asserted table, not a synthesized implicit table. Table and release
  `name` changes preserve their public ids; variable names use `label`. Variable
  `synonyms` enrich search labels without adding delivery aliases. `set sentinels` at an
  occurrence or variable address applies after value-set overrides to the published
  hashes, including population alternatives. Sentinel codes are set-wide:
  `vs_item.sentinel` changes for every reference to that shared set. Missing codes in an
  explicit setter and disagreeing local declarations for the same hash refuse at their
  authoring locations. Classification inventory `sentinel_codes` names known
  non-canonical codes in columns bound to that exact version, not codes in its canonical
  book. It marks the listed codes present in each bound occurrence's value sets,
  including population alternatives; declarations absent from all bound sets are
  counted, not refused. Classification lists sharing a bound set are additive. A listed
  code that is canonical in any version bound to that set, including the declaring
  version, refuses at the binding and inventory locations. The bits remain set-wide, and
  the hash remains based on code, label and parent, without sentinel status.
  Occurrence-scoped sentinel claims on shared sets are deferred to sentinel-aware
  value-set identity; revisit when a dropped scoped entry needs it. Register name
  corrections that differ from source remain deferred: they need a register address in
  the grammar.
- `curation.lock` is minified canonical JSON with `version = 3` and normalized statement
  hashes as keys. The top-level `objects` map stores each full object once, keyed by the
  SHA-256 of its canonical JSON. Each statement's `objects` is a sorted list of those
  keys; `subjects` is unchanged. References are resolved and digest keys checked when
  reading. Object order is not semantic; comparison uses canonical full-object order,
  retaining field-level old/new diagnostics rather than reporting only changed hashes.
  Versions 1 and 2 refuse with an instruction to run `reg-build lock`; locks are
  regenerated, never migrated. Variable-level setters are `var:` setters containing only
  `sets`, `tags`, `label`, `description`, `definition`, `operational_definition` or
  `synonyms`. Their locked `subjects` record the variable slug, distinct matched locator
  count and SHA-256 of the sorted canonical locator JSON lines. Changed membership
  refuses with the variable subject and old/new counts, even when the counts are equal.
  Occurrence field values and container facts (register, table and release objects) are
  not premises for these setters. No container fact is folded into the digest: these
  fields apply metadata to the matched variable without consuming container contents;
  selector membership is already checked by the locator digest. Non-container objects
  retain their full premises. None of these seven fields references a value set or
  classification: those dependencies arise from other setter fields, which retain full
  premises. A variable-level lock entry therefore has constant size, independent of
  occurrence count and container text length. Every other statement, including mixed
  setters with occurrence-level fields, keeps the full premise. Application and conflict
  detection always use full in-memory subjects. Full premises contain sorted `subjects`
  and `objects`: source locators, coordinates, overridden values, guards and dependent
  facts, including the destination facts used by table moves. Dependencies are evidence,
  never extra rows selected for rewriting. Missing or changed premises, empty or
  ambiguous selectors, and conflicts are located refusals. Release builds fail; dev
  builds skip refused statements and report them. A `set` equal to the source is a
  redundant-set finding.
- Source premise field projections and their canonical sort keys are shared per source
  within each immutable view. Statement, conflict and lock bookkeeping clone shared
  snapshots; acknowledgement findings copy only the snapshot they extend. Field
  applications retain before/after records only for changed facts, while metadata and
  provenance remain independent of record changes.
- Evaluation serializes shared code-list facts once per phase and indexes selectors and
  application dependencies once per register phase. Conflict candidates share source
  locators, assertion addresses or table moves; the identity and overlap rules still
  decide each candidate. Identity assignments share an immutable application snapshot
  and emit only assignments and provenance. Frame application copies local facts and
  explicitly referenced code lists, while unrelated shared facts stay in the frame.
- `reg-build lock [REGISTER...] --inputs DIR --curation DIR [--assets DIR]` evaluates
  every register's statements and records only the requested locks (all by default). It
  refuses invalid statements before writing any lock; empty selections remove stale
  locks. Builds check locks and never accept changed premises automatically.
- Provenance paths are relative to the curation root, independent of working directory.
- Rules never merge across keys, bind classifications or enrich; those are curated.

The address grammar is:

```text
<table>: <spelling> [@<period-set>] [where <field> (=|^=) '<value>' [and ...]]
var:<id>
table:<slug>
release:<table-slug>/<handle>
```

Tables and ids are catalog slugs; spellings match folded. Brace lists expand literal
alternatives (`{a,b}`) and inclusive numeric ranges (`jobbink{01..12}`), preserving
leading zeroes. Comma-separated alternatives are literal; only an entire numeric
`start..end` brace body is a range. `<none>` addresses a documented-without-column
marker. Expansion is limited to 10,000 spellings. Period sets are comma-separated unions
of `reg-core` periods, with open endpoints (`..2009`, `2019..`) parsed through that same
grammar. Guards read only source `label`, `type`, `unit`, `key` or `release`, with exact
or prefix comparison; `key` and `release` allow equality only.
`where release = '<handle>'` compares the release's source handle exactly as recorded in
the frame. Provider coordinates have slots only in `where key`, `where release`, and
direct release addresses, all opaque strings. A direct release address names the release
itself, for example to set its period or assert it. Its table is a catalog slug; the
entire remainder after the first `/` is the source handle, preserved without folding or
trimming, including `/`, `@` and `where`. It takes no additional scope or guards.
Regular expressions are refused. Edition and qualifier remain release attributes;
`:edition` and `/qualifier` address suffixes are unsupported syntax and are refused with
a message pointing at `where release` (maintainer ruling, 2026-10-27).

All four kinds deny unknown fields. `evidence` is nonblank text (including
`doc:<hash>#page=12`); only `ack` with `severity = "notice"` may omit it. Evidence
carried over in D from the retired Python builder cites its files at the git tag
`python-builder-pre-cutover`: `python-builder-pre-cutover:curation/…` is a file under
that builder's package directory, usually with a `:<line>` suffix, and
`python-builder-pre-cutover:reg_webapp/…` a repository path (a file or, for the `series`
directory, a directory), both at that tag. `caveat` is optional user-facing text.
Kind-specific content is:

- `assert`: `subject` and a `fact` tagged by `entity`: `occurrence` or `table` with
  `fields`, `release` with `name` and `period`, `value-set` with `items`, or
  `classification-version` with `family`, `version` and `items`;
- `set`: `subject` and nonempty `fields`;
- `link`: `subject`, typed `relation`, `object` (address or catalog FQID) and optional
  `at` period set;
- `ack`: `subject`, `finding`, `severity` (`notice` or `review`), and evidence when
  required.

`set.fields` is closed: `table`, `variable`, `label`, `description`, `sets`, `unit`,
`definition`, `operational_definition`, `value_set`, `coded`, `period`, `spelling`,
`identifier`, `sensitive`, `type`, `length`, `format`, `notice`, `coverage`,
`classification`, `tags`, `sentinels`, `role`, `unit_type`, `name`, `synonyms`, `axes`
and `facets`. Source-shaped assertion fields have a separate closed schema, including
table `name` and occurrence `type` and `length`. Lists of sets, tags, sentinels and
synonyms normalize as sorted sets; asserted codes sort by code, while duplicate codes
are refused. Addresses normalize folded spellings, expanded braces and sorted guards and
period tokens before hashing. Links add relations; conflicting successions refuse. There
is no successor-choice or `reject` form. Merged release changes update every claim to
the release's final placement.

The loader reads every register directory in sorted order. Unknown kinds or fields,
missing evidence, malformed addresses, misplaced coordinate fields, and duplicate
normalized statements within a register are refused at the file and statement line.
Matching and premise validation live in `reg-resolve`; lock IO and located diagnostics
live in `reg-build`. Curated values take precedence over rules and sources; two
overlapping curated overrides are refused at both locations.

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

- Only `reg-readers` may name a provider or a format. A `crates/xtask` test scans the
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
  its occurrences disagree on label, type family or value set, except a family-only
  disagreement under one source key and one folded spelling. That exception leaves dtype
  unset and emits one `F6:source-conflict` review per segment for a D `set`, including
  when case also differs. A family disagreement keeps a stated length only when all
  stating occurrences agree, and joins adjacent segments only when their lengths agree.
  Type agreement ignores unknown occurrences: only multiple known families can cause
  ambiguity or a type-only F6. Unknown evidence cannot conceal a disagreement between
  known families. Within temporal the widening order is `date` < `datetime`; within
  other families equivalent aliases use lexical order. The published length is the
  greatest stated length among known forms when any exist, else among unknown forms.
  Unknown-only components remain separate from adjacent known-family runs, like gaps; F1
  skips comparisons with an unknown side. An overlapping segment with known evidence
  never publishes `unspecified`. No length is invented. Adjacent agreeing known forms
  join with the same widest-form rule. Labels compare ignoring case and spacing; label
  and value-set disagreements still block. Another variable claiming its folded spelling
  in any overlap still makes the whole segment ambiguous, including an exempt segment.
- **The implicit table** takes its register's id, so `ident` holds that id once, as the
  register's, and the namespace check covers explicit tables only.
- **The ledger** is `curation/registers/<provider>/<register>/ids.toml`: `[tables]` maps
  a table handle to its id, `[variables]` an id to anchors `<table>: <spelling>` (a bare
  spelling names the implicit table), matched by fold. An anchor may end in `@ <day>`:
  it then reaches only the variables placed at that spelling on that day. A variable no
  anchor reaches gets a proposed id from its fold, clear of the register's table ids.
  When two ids reach one variable, the first in id order wins and the other is reported.
  Before minting, reconcile stale anchors against old pins and current placed variables.
  An existing id wins over a duplicate new mint. Pre-v1 pins with no current variable
  for their historical scope, and obsolete merged aliases, are removed without
  redirects. New variable ids use the label-slug rule: NFKD ASCII folding, lowercase
  hyphenation and pure unit parentheticals removed, without general stop-word removal.
  Prefer the latest label when its slug is unique among the register's variables, then
  the latest unique label from another canonical period. Otherwise append the anchor's
  full column spelling slug to the label slug, shortening only the label at a hyphen
  boundary to keep the id within 60 characters. Only remaining collisions take numeric
  suffixes from `-2`, in anchor order, with the same length limit and the column intact.
  With no usable canonical label, use the column alone. Reserve existing, retired and
  table ids before allocating new ones. Existing ids, including manually shortened label
  slugs, are never renamed by minting.
- **Retirements.** For ids published before v1, a rename switches the id; no retirement,
  redirect or rename warning is required. For ids published from v1,
  `curation/retired.toml` permanently records each retired public id, its kind,
  retirement version and reason (`merged`, `split` or `withdrawn`); removing a published
  id requires its entry in the same PR. The builder emits them to `ident_retired`,
  refuses live collisions with both `ident` and `var_set` (including tags), and reserves
  their names during minting and set membership admission. From v1, entries are never
  removed or redirected.
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
  defaulting to the handle; it is ledger data, not a statement or reader config (v4
  §2.3: the public id is stable through the ledger; §7 allows ledger entries; audit
  B-1). Rows that differ only by population are one occurrence with every row as a
  locator; the release lists its populations, with a reference day when one date is
  stated. Compatible known type forms widen as above; a known type replaces an unknown
  provisional type, and unknown rows do not widen known lengths. A label, definition,
  operational definition, unit or known type family the rows disagree on keeps the first
  row's as provisional and names the field in the occurrence's `conflicts`, which
  resolve reports as F6 (source conflict), once per variable.
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
- **LISA workbook attachments.** SCB `r34` carries the LISA workbook's descriptions,
  sensitivity, lineage and adopted sets as ordinary curation (D-8), converted once from
  a since-deleted `lisa` reader. VarId changes remain distinct identities, and SCB
  labels and definitions stand.
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

- **Release diff.** `reg-build diff OLD NEW [--json] [-n N]` compares two current-schema
  catalogs by natural key (pid, table name, meta key), never by row id, in nine
  sections: schema (`sqlite_master`), meta, row counts per table, ids (added, removed,
  kind changed, moved, retired), provider and register names, table names, roles,
  unit-type slugs and spans, per-variable segments, label and definition runs, value
  sets (including shared-hash sentinel changes and the codes added and removed where
  only a segment's value-set hash changed), and finding counts per code. Text shows at
  most `N` (default 20) per section; `--json` is complete. Exit 0 when nothing differs,
  1 when something does (the meta keys `generation_id`, `catalog_version`,
  `builder_version` and `curation_digest` never count, since they differ every release),
  2 on a usage or IO error or a missing or different `schema_version` major. A surviving
  variable id is listed as moved when both occurrence target sets are non-empty and no
  old/new pair shares a table slug and folded spelling with overlapping `[start, end)`
  periods. Explicit table ids are compared by table name; implicit tables share their
  register's id and are excluded. `--holdings` compares two holdings catalogs of the
  same holdings schema instead (release step 8g): census columns by (physical table,
  column), with placed, held and unplaced counts, changed placements grouped by old and
  new `table_pid` and classed newly placed, folded or renamed, variable or interval
  changed, or unmapped; reader-visible text by kind, keyed by pid and source key and
  listing keys, never bodies; and finding and holdings-notice deltas by code, compared
  by (code, subject, period).
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
  check: integrity, the schema's post-write queries (including same-register sets with
  at least two distinct members and live/retired collisions), each value set's items
  hashing to its address, and with `--previous` the id rules against the previous
  release. `emit` runs it on the staged file before replacing the output, so conformance
  and the release skill call the same authority. The schema version is
  `reg_core::CATALOG_SCHEMA_VERSION`, stamped into both `meta.schema_version` and
  `PRAGMA user_version`; `verify` requires both to equal it, and a previous file of any
  other version is not comparable. When the previous release's catalog version has an
  integer major of at least 1, removed ids from `ident` or `var_set` must appear with
  the same kind in `ident_retired`, and existing retirements must remain. A published
  variable id's targets are its column-bearing occurrences' (table slug, folded
  spelling, `[start, end)` period) triples. Targets overlap when table and spelling
  match and their periods overlap; matching names in disjoint periods do not preserve
  identity. An explicit table id targets its table name. Implicit tables share their
  register's id and are excluded from the table rule. If an id of the same kind
  survives, both target sets are non-empty, and no old/new targets overlap,
  `verify --previous` refuses it as moved under that same version rule, naming the pid
  and both target sets; retire that id and mint a new one. Otherwise, missing unretired
  ids, moved ids and removed retirements are reported on stdout, one line per id in id
  order with a total count, without refusing. The rule follows the previous release's
  version, whatever the candidate's: a `dev` or 1.0.0 candidate compared with a 0.x
  release is lenient. No flag changes this rule; other verification checks still apply.
  Overlapping targets, including growth, are allowed. `diff` lists and counts moved ids
  in its `ids` section. Registers and providers are excluded from this rule.
- **The link stage** (`reg-build/src/link.rs`) only matches endpoints: an endpoint names
  its register by handle or by name (`fold_identity` of the trimmed name, which exactly
  one register must match), then a table, a release or a variable by source key, where a
  split key gives every piece. A reviewed endpoint can instead use the existing
  `var:<local-ledger-slug>` address as its variable key to select exactly that final
  variable, without expanding a reused source key. A `continues` needs one entity of one
  kind at each end, a `derived_from` one object and gives a relation per subject piece,
  and a release end is never a relation (a reader states a succession between releases
  as a notice). A notice becomes one finding per subject entity, `S0:source-notice` with
  severity `notice`: an addition to v4 §4.5, which has no source-notice code. A source
  can state successions the catalog cannot hold, so after folding the stage drops them
  as misses rather than let the post-write checks refuse the build: every table
  `continues` of one table on one day (the source names no single successor), then, in
  locator order, each `continues` that closes a cycle. Every miss is one `link:`
  warning, counted as `link_unresolved` in the build report. Name matching is per
  provider (the index key is `(provider, fold(name))`), so a source text naming another
  provider's register never matches and is reported unresolved for D curation: that
  count is not a reader defect. A malformed link or notice (an unknown kind, an endpoint
  without exactly one register reference) is refused at its locator. In frame JSONL a
  link's kind is written `relation`, since the record tag owns `kind`.
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
  `doc_subject_unresolved` in the build report, and the document stays searchable. A
  parsed markdown text is titled by its frontmatter (often a section heading) and names
  the published document it comes from in `document_title`, the sources file's title
  (#2081); a pdf's `document_title` is its own title.
- **Schema addition M2.** `alias_variable` indexes `alias(variable_id)` for the
  variable-to-names direction.
- `search_item` code rows are (code, label) pairs, not (value set, item): `code_text`,
  `label_text`, no `valueset_id`. The value sets of a pair come from `vs_item_code`
  (C-10 ruling: one row per item measured 1.48 GB).
- `vs_item_code` exists only for that lookup (about 90-100 MB). Packing items into one
  BLOB per value set (schema §5.2, about −65 MB on `vs_item`) is the lever if size
  matters.
- **Edition and qualifier are not part of a request.** The primary segments serve every
  order (final over preliminary); the builder stores no selection segments. Naming an
  edition or a qualifier is deferred until curation, so the periods grammar and the
  source stay without them until a use is shown.
- **Implicit tables are not in `ident`.** A 2-part ref resolves through `ident` to the
  register. Where a table is expected, the table is `tbl WHERE pid = ?`, and the
  implicit table has the register's pid. A 2-part table value naming a register with
  named tables is `table_is_register`, listing the tables.
- **Search ignores diacritics.** The new catalog's `search` indexes fold diacritics
  under `unicode61 remove_diacritics 2` (schema §3.2), so `lan` finds `län` and `lon`
  finds `Lön` (non-Swedish keyboards). The maintainer ruling in #2163 reversed plan
  decision C-9, which kept them. The reader passes `q` to FTS unfolded, so the index
  tokenizer folds query and text alike; pre-folding with
  `reg_core::normalized_search_query` (full case folding, `ß` to `ss`) would miss an
  indexed `ß`.

**What the rebuild is checked against.** Not today's artifact byte for byte. It is
checked against what users see (tables, columns, variables, labels, value sets) and
against random full-register orders, with every difference classified.

## Agent and web interface

**Agents use MCP; HTTP exposes the same operations.** Interactive CLI use is out of
scope: no feature, flag or output exists for it (no text output, prompts, progress or
notebook import). Revisit when a concrete human-facing use appears.

**One operation set, two transports.** Each operation in `reg-catalog` has a name, a
typed parameter object and a typed result. The HTTP routes are generated from those
definitions, and the MCP tool schemas are built from the same utoipa OpenAPI document,
so the transports cannot drift. OpenAPI and MCP `tools/list` are the self-description;
there is no `describe` operation. A former CLI use has a transport: agents call MCP
tools, the Docker bake and the publish workflow use `curl` and a started `serve`, and an
order is `POST /api/project/order` or the `order` tool.

Rules:

- **JSON only, `{data, meta}` on every success**, except raw-bytes downloads (the
  document file), which carry their media type and have a JSON operation for their
  metadata. `meta` is `contract_version`, `generation` and `catalog_version`, so an
  agent always knows which catalog answered.
- **Deterministic responses.** `meta` carries no timing; the same request on the same
  catalog returns the same bytes, and goldens compare raw output.
- **A ref is an FQID or a bare name.** A bare name resolves through `ident.pid`, then
  `name_fold` (case-insensitive, `fold_identity`), then `ident_retired`, which answers
  `retired_ref`. A slug matches only where it is also the name. A unique name resolves;
  an ambiguous one returns the candidates with their FQIDs as `ambiguous_ref`, so the
  next call can be exact.
- **`show` plus facet operations.** `show(ref)` returns a summary for any kind; separate
  operations fetch the heavy parts: `segments`, `findings`, `values`, `graph`,
  `coverage` and `schema`/`diff`.
- **One time filter, `period`**, in `reg-core`'s grammar (`2019`, `2015..2019`,
  `LA2019`, `2019-03`, `2019-01-01..2019-06-30`). `diff` takes `from` and `to` in the
  same grammar.
- **Cursor paging for open-ended lists**: `limit` (default 50) and `cursor`, returning
  `{"items": [...], "next_cursor": ...}`. A list bounded by the request or the entity is
  a plain array. `findings` answers one group per severity and code, with its count and
  a page of its items: an entity's findings are bounded but run to thousands in a few
  codes, and a reader wants the counts first. Cursors bind to the generation, so a stale
  cursor fails loudly. No offsets.
- **One parameter per concept and one result shape per operation**, independent of
  result count.
- **Errors** are `{error, meta}` with
  `error = {code, class, message, remediation, fields}`; MCP returns the same document
  as a tool error.
- **Few, coarse MCP tools**, about ten, because every tool's schema occupies agent
  context. Fine-grained HTTP routes map onto the same operations.
- **Catalog selection is configuration**: one catalog per process, chosen at start.

**Curated by default, evidence on request (maintainer, 2026-11-02).** A variable's
`show` summary returns its name, the one optional curated `description`, register,
coverage hull (`period`), and its sets (groups) and tags when it has any. Detailed
coverage spans and gaps remain the `coverage` facet; its tables and column names are
`segments` rows. The description combines meaning, measurement and derivation; `texts`,
source spellings and a promoted top-level `definition` are not summary fields.

**One per-period list (maintainer, 2026-11-04).** `segments` is the variable's one list
of table, column name, period and provider wording, which the SPA timeline and MCP both
read; no other operation repeats it. A segment is cut where the provider's wording
changes, so each row carries one `wording`: the latest used occurrence's source
definition and operational definition in that table over that period, never a curated
correction. Equal or contained wording is shown once; otherwise the two original texts
are joined by a newline. Comparison collapses whitespace, folds case and ignores
trailing Unicode punctuation; displayed wording stays original. A silent provider has no
`wording`. A fact the catalog does not state is absent, never `null`: no type, no value
set, `coded` unknown, and `state` clear (the default) are all left out, which keeps
agent context small. Storage type stays in the catalog (orders use it) and is shown only
when stated.

**Names next to ids.** A response that refers to a register, table or variable carries
its name next to the FQID, as `show` gives it (a reference row's name over a steward
overlay's), so a reader needs no `show` per id: `segments` rows have `table_name`,
`findings` subjects `name` and `table_name`, and the `order` and `validate` rows and
findings `table_name`, `variable_name` and `successor_name` (`NamedIssue`). One batched
read per origin names a whole response (`refs::names`), never a read per row. The MONA
order file carries ids only.

`show(ref, evidence=true)` adds `evidence`: provider definition/operational-definition
texts use the same containment rule within each occurrence, then identical normalized
texts are grouped with all their periods, preserving gaps. Each text retains its source
occurrences: labels, units, periods, table, status, source keys, exact spellings and
source locators. Occurrences without either text are kept in `source_only`. Evidence
also includes delivered/curated aliases; applied curation fields with statement
file/line, evidence and caveat; and linked documents. Original provider text and
spelling values are restored from override provenance when a correction exists,
including a source that was silent. Actual description, identity and group/tag statement
locators are included; a ledger id does not acquire an invented statement. Evidence is
never loaded or returned by the default summary. `segments(evidence=true)` remains the
structural segment evidence pathway.

**Hosted MCP.** `serve` exposes the global catalog's `/mcp` at reg-meta.vqv.uk, so
agents need no install and no 1.2 GB download. `/mcp` and every POST share one rate
limit per client address, sized for tool calls (`reg-meta/src/limit.rs`). A published
server admits its MCP host (`--public-host`).

**Hosted MCP has no webapp handoff** (maintainer, 2026-10-20, #1709). The global server
carries no holdings, which a steward deployment serves (`--catalog <steward>`,
"Holdings"); `show`'s root names no webapp. The `order` tool stays: researchers describe
data they already hold on MONA as a `project_data.json`, and it returns the logical
order.

## A provider-neutral substrate

The catalog is the identifier and object-model substrate every downstream artifact
references: `project_data.json`, the HTTP API and MCP tools, the logical order. Their
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
broke when Socialstyrelsen and Försäkringskassan arrived. The v4 model removes those
assumptions: registers own tables, variables are register-scoped, a variable's history
is its segments (one form of the variable in one table over a period), and ids have no
variant or period slot. The relations between entities (`relation`) and the notices and
review items (`finding`) are rows of their own, and build-time resolution normalizes
provider oddities into the universal shape.

## FQID grammar

Every entity has a fully qualified identifier: a `/`-separated string whose kind follows
from its segment count and the leading `class/` or `set/` namespace. `reg-core` owns the
grammar (`grammar.rs`, oracle `conformance/cases/grammar/`); the catalog assigns the ids
(ledgers under `curation/`) and resolves a ref to an entity.

  | Kind                   | Public id                                                                         |
  | ---------------------- | --------------------------------------------------------------------------------- |
  | provider               | `<provider>`                                                                      |
  | register               | `<provider>/<register>`                                                           |
  | table or variable      | `<provider>/<register>/<slug>`; an implicit table uses its register's two-part id |
  | classification family  | `class/<family>`                                                                  |
  | classification version | `class/<family>/<version>`                                                        |
  | set                    | `set/<provider>/<register>/<slug>`                                                |

- `class` is reserved as a slug everywhere. `set` and `group` are reserved only as
  leading provider segments; they remain valid register and final slugs. The set
  namespace's provider obeys the same provider restrictions. The builder refuses a
  reserved provider in the manifest or source frame with located `provider-reserved`. A
  plain three-part ref never addresses a set, even when its final slug matches a set's
  slug. Set refs are presentation addresses, never project bindings, source tables or
  relation endpoints.
- A slash-separated ref is checked against this grammar before exact lookup. The leading
  `set/` namespace resolves only a set. Malformed set refs are `invalid_ref`; a
  well-formed absent set is `not_found`. Exact lookup cannot reinterpret the kind. Sets
  resolve only by their namespaced id: bare-name lookup excludes sets, so a same-slug
  set never makes an ordinary variable's bare name ambiguous. Bare `set` is an ordinary
  name and may resolve a variable; only leading `set/` selects the set namespace.
- A slug is lowercase ASCII kebab-case, `^[a-z](?:-?[a-z0-9])*$`; a class version may
  start with a digit.
- Time is context, not identity: no id has a period slot. The period grammar is `YYYY`,
  `YYYY-MM`, `YYYY-MM-DD`, `HTYYYY`/`VTYYYY`, `LAYYYY` (July of `YYYY` through June of
  the next year), `YYYY-Q[1-4]`, `YYYY-H[12]`, years 1900–2099, plus a `from..to` range.
  Each value has exactly one spelling.
- HTTP routes put the operation before the ref (`/api/segments/{ref}`), so no slug can
  shadow a route.

## Artifact admission

A server or MCP process serves one artifact, chosen at start with `--db DIR` (the file
is `DIR/reg_meta.db`, documents included). Admission is exact: `meta.schema_version`
equals `reg_core::CATALOG_SCHEMA_VERSION`, every required `meta` key is present and
`generation_id` is 64 lowercase hex digits. Anything else is `schema_incompatible`
(`schema_version` is the file's value, or `null`), and the process fails before serving,
with no fallback. The file is opened read-only and `immutable=1`: published files are
replaced by rename, never changed in place. There is no read scope and no second
database. A project is the user's file: the server stores none and checks one only when
it is posted to `validate` or `order`. Holdings are a second, optional database admitted
with `--catalog <steward>` ("Holdings").

## The logical order

`order` runs the order check of design v4 §9 on a 5.2.0 project and returns the logical
order of schema §2.3 as `data`, or the error `order_blocked`. `validate` runs the same
check and returns the same findings as issues. Both transports call the one
implementation in `reg-catalog`, which is what makes their results identical. The check
is written over the v4 catalog (the `r-order-check` PR), not ported from the 4.0.0
physical-order logic, which v5 deletes.

The logical order says what is ordered, never in which files. It stops at the catalog:
physical files, partitions, row filters, file names, the manifest download and the
extract format are the holdings layer's, and the physical manifest is built from the
holdings (`reg-catalog/src/ops/order/materialize.rs`). Nothing in the result names a
steward.

**Shape.** `shape.LogicalOrder` in `conformance/api/operations.toml` is the field list.
In short: `catalog` and `project` identify the generation and the project (with
`project_hash`); `entries` are per source, per binding in declaration order, then by
period start; `row_keys` are per source and period start; `clips` record every
availability clip, also when nothing blocks; `findings` are the non-blocking ones
(warnings and infos), as `NamedIssue`. Every table and variable id in a row has its name
beside it ("Names next to ids"). Periods are half-open ISO-day intervals, with `null`
for the open sentinels; a timeless table has `{start: null, end: null}`.

- An **entry** is one merged segment of one binding in one table. With a table
  succession (`continues` between tables) each day goes to the table whose `at_start`
  window holds it, so one binding can give entries in two tables. Adjacent segments
  merge when their spelling, type and value set agree (design §4.6), so a flag-only
  boundary does not split an entry.
- **`row_keys`** are the table's segments with role `row_key` over the period. A row key
  that no binding orders still appears there; it is not an entry. This is where a panel
  gets its keys, since the document names none. Since every order carries its row keys,
  an ambiguous row-key segment over the period blocks (`ambiguous_window`, located at
  the source) even when no binding names it.
- A blocked order is `order_blocked`, with every finding at once and nothing partial;
  the blocking ones have level `error`.

**Findings.** Errors block: `fqid_unresolved`, `table_is_register`, `retired_ref`,
`variable_not_in_table`, `outside_availability`, `ambiguous_window`,
`classification_incompatible`, `period_default_on_dated_table`,
`period_on_timeless_table`, `period_not_orderable` and `project_empty` (no sources; a
whole-project finding) and `source_outside_window` (a dated source period wholly outside
the study window). Warnings and infos do not: `classification_version_ambiguous`,
`preliminary_data` (a window only a preliminary release covers; final wins where both
exist), `edition_disagreement`, `variable_sensitive`, `variable_replaced`,
`period_clipped`, `form_change`, `table_succession_undated`, `identifier` and
`coverage_end_unstated`. Each is an `Issue` whose `path` points at the source, binding
or panel and whose `source`, `variable`, `table` and `period` say what it is about
(`null` for a whole-project finding). The conditions are the order check's to state in
its own tests; the rules that shape them:

- A retired id reports "retired in <version>" and names no successor; no operation
  follows `ident_retired`.
- `variable_replaced` reports an incoming `continues` edge. By default it is a warning
  and the edge is not followed. Opt-in refusal can raise it to an error (below).
- Design §9 blocks only the `ambiguous` window kind. A preliminary segment orders with a
  warning (selecting an edition is deferred until curation), and a class-version
  ambiguity the binding does not pin is a warning.
- `classification_version_ambiguous` warns when an unpinned binding spans multiple
  curated versions. `classification_incompatible` blocks at the binding's
  `classification` field when the pinned version has no compatible curated windows. A
  pin preserves `ambiguous_window` refusals, including ambiguous segments that state no
  version; it cannot turn them into coverage clips.
- Variables are register-scoped, so members of different registers never share a row-key
  variable. Which keys join is the researcher's call; the catalog stores no identifier
  kind, so the order check does not compare panel members' keys.
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
    "project": {"name": "income-panel", "schema_version": "5.2.0",
                "reg_meta_version": "0.65.0", "project_hash": "51ab…"},
    "entries": [
      {"source": "lisa", "binding": 0, "provider": "scb", "register": "scb/lisa",
       "table": "scb/lisa/individ", "table_name": "Individer",
       "variable": "scb/lisa/dispink", "variable_name": "Disponibel inkomst",
       "display_name": null,
       "period": {"start": "2003-01-01", "end": "2004-01-01"},
       "spelling": "DispInk", "type": "int", "length": 8, "value_set": null,
       "classification": null},
      {"source": "lisa", "binding": 0, "provider": "scb", "register": "scb/lisa",
       "table": "scb/lisa/individ", "table_name": "Individer",
       "variable": "scb/lisa/dispink", "variable_name": "Disponibel inkomst",
       "display_name": null,
       "period": {"start": "2004-01-01", "end": "2007-01-01"},
       "spelling": "DispInk04", "type": "int", "length": 8, "value_set": null,
       "classification": null},
      {"source": "lisa", "binding": 1, "provider": "scb", "register": "scb/lisa",
       "table": "scb/lisa/individ", "table_name": "Individer",
       "variable": "scb/lisa/sun2000niva", "variable_name": "Utbildningsnivå",
       "display_name": null,
       "period": {"start": "2004-01-01", "end": "2007-01-01"},
       "spelling": "Sun2000niva", "type": "char", "length": 3,
       "value_set": {"hash": "c41e…", "n_items": 31}, "classification": null}
    ],
    "row_keys": [
      {"source": "lisa", "table": "scb/lisa/individ", "table_name": "Individer",
       "period": {"start": "2003-01-01", "end": "2007-01-01"},
       "columns": [{"variable": "scb/lisa/lopnr", "variable_name": "Löpnummer",
                    "spelling": "LopNr"}]}
    ],
    "clips": [
      {"source": "lisa", "variable": "scb/lisa/sun2000niva",
       "variable_name": "Utbildningsnivå",
       "requested": [{"start": "2003-01-01", "end": "2007-01-01"}],
       "ordered": [{"start": "2004-01-01", "end": "2007-01-01"}]}
    ],
    "findings": [
      {"level": "info", "code": "period_clipped", "path": "/sources/0/bindings/1",
       "message": "scb/lisa/sun2000niva is available from 2004-01-01; 2003 is not ordered",
       "source": "lisa", "variable": "scb/lisa/sun2000niva", "table": "scb/lisa/individ",
       "period": {"start": "2003-01-01", "end": "2004-01-01"}, "successor_fqid": null,
       "variable_name": "Utbildningsnivå", "table_name": "Individer", "successor_name": null},
      {"level": "info", "code": "form_change", "path": "/sources/0/bindings/0",
       "message": "scb/lisa/dispink changes form at 2004-01-01 (DispInk -> DispInk04)",
       "source": "lisa", "variable": "scb/lisa/dispink", "table": "scb/lisa/individ",
       "period": {"start": "2003-01-01", "end": "2007-01-01"}, "successor_fqid": null,
       "variable_name": "Disponibel inkomst", "table_name": "Individer", "successor_name": null}
    ]
  },
  "meta": {"contract_version": "5.2.0", "generation": "9f2c…", "catalog_version": "0.65.0"}
}
```

### A blocked order

Catalog (illustrative): in `scb/lisa/individ`, two variables claim the folded spelling
`kommun` over 2019, so both segments are `ambiguous` there. `scb/lisa/dispink` for 2020
is delivered only by a `preliminary` release.

Request: source `lisa`, period `2018..2020`, bindings `kommun` and `dispink`.

```json
{
  "error": {
    "code": "order_blocked",
    "class": "order_blocked",
    "message": "the order is blocked by 1 finding",
    "remediation": "Narrow the period around the blocked windows; an ambiguous window needs curation.",
    "fields": {
      "findings": [
        {"level": "error", "code": "ambiguous_window", "path": "/sources/0/bindings/0",
         "message": "two variables claim the column 'kommun' in scb/lisa/individ over [2019-01-01, 2020-01-01)",
         "source": "lisa", "variable": "scb/lisa/kommun", "table": "scb/lisa/individ",
         "period": {"start": "2019-01-01", "end": "2020-01-01"}, "successor_fqid": null,
         "variable_name": "Kommun", "table_name": "Individer", "successor_name": null}
      ]
    }
  },
  "meta": {"contract_version": "5.2.0", "generation": "9f2c…", "catalog_version": "0.65.0"}
}
```

The 2020 window of `dispink` does not block: it orders, with the warning
`preliminary_data` (no final release covers it). The finding stays until curation
resolves the ambiguity; narrowing the period to 2018 alone also clears it.

## Holdings

Holdings describe possession and delivery topology. They are a separate compilation
layer after the published catalog, reading its public table and variable ids. They do
not change provider identity, segment validity or classification applicability. This
boundary and the absence of a catalog series entity are settled maintainer decisions;
see also "Value sets, populations and series" above. The decisions below include the
maintainer 2026-10-29 rulings and the Operator decision on pre-2018 current coding
derived from them. No open design questions remain.

Source references in this section use `cleanroom/design.md` (v4, 2026-10-26) and
`cleanroom/schema-v3.md`, under the maintainer-local `.local/design/first-principles/`
directory. The latter's prefatory "Maintainer rulings after the review" override its
programme-era body. The v4 design and later rulings in this file govern where they
supersede older rules: no programme, `parallel` state, separate table-key entity,
six-kind curation grammar or restored order-time edition selector is imported from that
older schema.

### Three questions, three scopes

The **reference** scope answers what the providers document. The **steward** scope
answers what this steward physically holds, including accounted but unmapped columns.
The **project** scope answers what a project actually received, described by its own
file/column inventory. Research intent in `project_data.json` and a materialized order
are not evidence that delivery occurred. These are distinct inputs, not three filters on
the logical selection (clean-room design §1 and §13 D7).

A project may order through a steward or directly from providers. `owner.kind` reserves
`steward|project` with an explicit id in the holdings schema; this is not a programme
entity. E implements steward holdings only. Project loading, `scope=project` and
project-inventory mocks are **deferred** until a real metadata-only received-file
inventory supplies an input contract. They are not needed for E's exit. A request or
materialized plan is never substituted for that inventory. Clean-room §1's project scope
remains a domain requirement, not a claim that this path ships in E.

The published catalog and a holdings database remain separate (schema-v3, prefatory
maintainer rulings; clean-room design §0 and §13 D7). The reader opens their pinned pair
read-only. Reference browsing uses the catalog alone.

**A steward deployment serves its holdings only (maintainer, 2026-11-04):** "everything
shown is held, everything held is shown." There is no scope parameter. A register,
table, variable or provider with no accepted placement is `not_found` by ref
(`Catalog::connect_for_ref`, `serves`) and is left out of every list: `show`'s children
and set members (a set's own page included), `schema` and `diff` lines, search hits and
facets (filtered by public id and the requested interval before ranking, counts and
pagination), and graph neighbours. Classifications, value sets, codes and documents are
shared and always served. Every `period` and a table's `spans` are the years held, and
`coverage` reports them per table; a table's releases are the provider's and are left
out. `segments` rows are the accepted placements: the held column name, the `file` it is
held in and the years held, with the form of the catalog segments they overlap in either
origin (preferring the same column name among those); held years no segment covers state
only the role. The reference periods, gaps and releases are not the steward's and are
not served. Cursors bind both generations. Neither an alias, a successor nor equivalence
grants possession. Base-only `order`/`validate` use the published reference catalog
(§9). In steward mode, those same functions read a composed semantic view including the
steward provider definitions and slot variables (maintainer 2026-10-29): the answer may
include steward-specific slot variables and pre-2018 delivery codings absent from the
base. That is an explicit mode divergence, not a claim of identical base-only answers.
The implementation of the logical check is shared; physical mapping then adds possession
restrictions. Its provenance names both generations and the selected semantic mode.

An open provider end stays open and keeps its `coverage_end_unstated` notice. A finite
received-file inventory bounds possession independently; it does not close the
provider's segment or infer an unobserved later delivery (clean-room §2.1 and §15, "Open
ends"). A holdings refresh can therefore change physical clips without changing the
logical order's reference availability.

For example, the checked-in SWECOV definition for `SWECOV_SOS_SV` names the provider
table `sos/par/par-sv`, a physical span 2005–2023 and the row-period column `AR`.
Reference coverage of a variable before 2005 does not make those years held. A project
selecting 2019–2020 yields the held columns and one pooled physical file, filtered on
`AR`; it does not yield two artificial annual files. Existing target variable ids are
resolved against the pinned D catalog, not copied from legacy `representation` fields.

### Inputs and ownership

**Steward repository (maintainer 2026-10-29).** Everything SWECOV-specific lives in a
new dedicated SWECOV repository, including the steward definition, scope, branding,
series/mappings, slot definitions, its own provider definitions and private inputs. The
maintainer creates the GitHub repository; **e-repo** prepares its layout and exact
commit pin before e-holdings. Its access controls must protect the private inputs;
public metadata definitions do not authorize publishing private source files.

The toolkit contains only generic compilation/serving code and a steward pin registry,
`stewards.toml`: each steward id selects `repo`, exact `commit` and relative `config`
path. The dedicated repo's `steward.toml` declares owner id, scope, presentation paths,
reader/input roles, semantic curation root and series root. Use `scope.toml`,
`branding/` (`steward.json`, logo), `series/`, `curation/registers/` (normal ledgers and
statements, including slots), `curation/readers/`, and `inputs/` (census, storage
export, provider source TOML/converted transcription CSV, inventory and legacy slug
evidence). Pin selected delivery-document evidence there too, retaining originals
privately. Reuse the catalog input-pin discipline: verify the supplied checkout against
the exact commit and declared paths/hashes; no implicit latest branch or neighbouring
inputs. The compiled holdings file carries presentation/config data needed at runtime;
serving never checks out that repo or parses its TOML. `context` serves only what the
site shows: the steward's presentation and logo, the server version and the period span.
In steward mode the span is the hull of the placements, not of the reference registers
(which cover years the steward does not hold); outside it, the hull of the registers. A
timeless period (both bounds open) is left out, so one timeless table cannot take the
study-years slider's bounds; an ongoing register still leaves the end open. Dropping
whole periods, not single open bounds, keeps the hull ordered. A hull open at both ends
(one period open at the start, another at the end) has no span either.

The 27 SWECOV-only providers are public in a legal sense but **must not enter the global
catalog**. Their normal semantic tables live in the holdings file's supplemental part,
built from this repo. This differs from reviewed SWECOV-generated knowledge about
providers already in the global catalog: that remains eligible for global curation under
the existing ruling. Delivery-only wide slots are separate steward variables, with
reviewed per-column routes; existing interval-valid D variables still win. Supplemental
search exposes a separately labelled steward result group with its own FTS ranking,
counts and cursor, alongside the reference group; independent bm25 ranks are never
merged.

A second steward such as IFAU needs its own pinned repo/config, owner id, scope,
branding, logical definitions/ledgers, mappings and metadata-only census/storage inputs,
then a holdings asset paired with the base catalog. The same compiler and
`reg-meta serve --catalog <steward>` select it without SWECOV branches. Serving that
second steward's webapp and steward-specific MCP is later, not E; E establishes the
data/config contract, not a second deployment.

`reg-build holdings --report FILE` forwards the ordinary supplemental build report,
including located release refusals; it does not admit a refused artifact. Reports and
holdings outputs cannot overwrite the reference catalog.

The compiler has explicit paths and pins; it reads no neighbouring files implicitly.

**History: the SWECOV conversion (e-holdings).** The SWECOV inputs were converted once
from the legacy steward tree and the private inputs repository's `swecov/` subtree into
the dedicated steward repository: the physical census, the storage export, 28 legacy
provider TOML files (converted to transcription CSV, read by
`reg-readers/src/transcription.rs`), the generated placement inventory, 55 legacy slug
pin files (converted to `ids.toml` ledgers) and selected delivery-document evidence.
Every legacy mapping was converted, dropped with a reason, or refused at its source.
Missing targets were retargeted to existing ids with evidence, minted as steward-only
definitions or slots, corrected through reviewed global curation, or held or unmapped
with a reason. The legacy paths survive in `steward.toml` only as conversion provenance;
the compiler never reads them. The census is matched by table and exact column; storage
facts that are missing or multiply resolved refuse for orderable holdings.

The 27 providers absent from the reference catalog have legally public definitions but
no global-catalog home (maintainer 2026-10-29): they live in the holdings file's
supplemental semantic part. The private census, storage export, inventory and original
documents are not published; release assets include only admitted metadata and safe
source locators. One generic holdings compiler reads the authored definitions and census
directly. There is no generate-then-reparse inventory stage and no steward-specific
code.

The ordinary catalog pipeline handles missing logical facts before holdings compile:
`reg-readers` → `reg-frame` → `reg-resolve` → `reg-build` emission. Existing-provider
corrections use ordinary `assert`/`set` statements and reviewed global curation;
delivery facts about existing global providers, including evidenced `*-enligt-leverans`
definitions, use that same global curation home. Facts specific to the 27 SWECOV-only
providers stay supplemental, as do the adopted steward slots. Genuinely new
steward-owned shapes get explicit normal ids in the dedicated repo and supplemental
semantic tables, including delivery-only slots (maintainer 2026-10-29). Pre-2018 current
coding uses ordinary steward coding-table definitions as decided below. No route
silently changes a variable's register; physical locations and possession remain
holdings facts; evidenced logical provider occurrences may enter global curation under
S3. There is no physical-table record in the catalog's `reg-frame::Record`: its `Table`
means a logical row shape and its `Release` a stated provider delivery.

No new crate is needed for the physical layer. Pure serialized holdings/order types live
in `reg-core`; format readers in `reg-readers`; compilation and SQLite emission in
`reg-build/src/holdings/`; queries in `reg-catalog/src/holdings.rs` and materialization
in `reg-catalog/src/ops/order/materialize.rs`. The latter consumes the existing order
check. Normal catalog curation stays in
`curation/registers/<provider>/<register>/ids.toml`, `curation.toml` and
`curation.lock`. Steward topology is data, with a closed schema, source locations and
exact pins, not another catalog statement language. Provider and format names remain
confined to readers/configuration, as for the catalog builder.

### Routing and auto-mapping

Each physical table in the steward's `inputs.holdings` topology lists its default
current `table_pid`s (`table_pids`) in its named source register; `series/*.toml` is
conversion evidence, not read by the compiler. The one-shot conversion resolved old
`register` + `variants` through D's `[tables]` ledger using source table handles, never
by copying an old slug. `bu_insats_pnrq0` already routes to `sos/bu/bu-insats-pnrq0`; an
implicit logical table uses its register pid.

A reviewed per-column `table_pid`/`variable_pid` override may route a received slot to a
different explicitly declared register. With the adopted steward slot model (maintainer
2026-10-29), SV's default remains `sos/par`, while a missing diagnosis slot may target
`swecov/par-sv-slots`; that column does not inherit the default's register restriction.
Its target register/table must exist in the holdings file's supplemental semantic part,
the variable must belong there, and evidence must explain the cross-register route. The
same mechanism covers positional `kva_typ` steward slots, without requiring a separate
long kva-typ concept. No new register is introduced by a route alone. Missing target
routes are `holdings_table_route_unresolved`, not a fallback to the whole register;
member tables may narrow the default list.

The generic compiler does the following against the exact catalog/semantic-view pin:

1. Apply table dispositions and explicit column `unmap`/`hold` decisions first. A hold
   carries a reason and follow-up issue; an unmap is permanent. Neither becomes an
   unresolved auto-map candidate. Stale decisions naming absent physical columns refuse.
2. Per routed logical table, match the census column to documented spellings using
   reader-side trimming followed by `reg-core::fold_identity` (Unicode `to_lowercase()`
   only, no trim or normalization in that function; D12). Only reader-configured
   rewrites may precede the fold: carry the old `^P1105_LopNr_` key-prefix rewrite
   explicitly into the census reader config, with its source evidence. Do not add fuzzy
   matching, label matching or provider-specific branches in resolve. An explicit
   reviewed mapping may supply a different physical/logical header association. A
   spelling must match documentation at the owed interval, not merely somewhere in
   history. Spellings are candidate evidence, never part of the compiled logical key.
3. Normalize each physical edition through the existing period grammar. A plain mapping
   owes the physical table's complete served interval set; a column window owes its
   stated subset; a positional or temporal slot owes the whole table interval (a
   year/month index is topology, checked for form only, as `CIS2006.emp04` below).
   School-year/term periods are spelled in the authored `delivery` with the period
   grammar; the legacy `edition_spelling` anchors (`spring`: year Y → LA(Y−1); `autumn`:
   Y → LAY; `vt`/`ht`: VTY/HTY) were converted into it. Point-status `2015-12-31`
   remains a one-day interval, not an invented year. Timeless can match only timeless.
   Intersect the delivered interval set with that variable's documented orderable
   clear/preliminary coverage in the routed table. Emit the nonempty intersection
   deterministically, with `period_clipped` notice carrying requested, documented and
   admitted intervals; a partial overlap is not a publication refusal. Authored column
   windows override the inferred reach and must validate as authored; they are not
   silently rewritten by this automatic clipping rule. Carried keys serve no ordered
   cell but must be evidenced row keys inside the file's reach. Current coding before
   2018 remains the open coding question, never a generic warning-shaped coverage
   extension. Preserve final preference and `preliminary_data`; ambiguous windows remain
   blocked.
4. Keep distinct rival owners ambiguous. The same `variable_pid` reached through two
   explicitly routed tables is one owner, grouped by variable identity within each table
   coordinate; it yields two valid placements, not two rival variables. Different
   variable ids claiming the same folded column in one routed table/window remain
   `ambiguous_delivered_owners`: a publish-time notice/worklist row with no orderable
   placement over the contested interval (Operator F2). The notice records its rival
   `(table_pid, variable_pid)` pairs (`holding_notice_candidate`), so the contested cell
   stays linked to its variables even where no placement of them survives. Do not select
   by longest coverage, first id or label. Uncontested intervals remain independently
   eligible. An explicit reviewed column `mapping` (`table_pid`, `variable_pid`,
   optional `delivery` window) names an evidence-backed variable and replaces automatic
   matching for that column; it cannot clear an ambiguous catalog segment.
   Partition/projection and duplicate placement checks still apply after owner grouping.
5. Emit keys `(table_pid, variable_pid, interval)` pointing to the literal physical
   column. An unmatched or empty-intersection column is retained with a
   `mapping_missing` notice and worklist row, without an orderable mapping. It does
   **not** block publication or require an invented unmap reason.
   `ambiguous_delivered_owners` is likewise a notice with no contested placement, not
   one of the refusal codes below. Explicit hold/unmap decisions retain their own
   reasons. Every census column must still be accounted in the artifact. Invalid
   authored routes/targets/windows, unaccounted census columns and illegal duplicate
   placements remain located refusals (`holdings_table_route_unresolved`,
   `stale_declared_mapping_target`, `inventory_window_invalid`,
   `inventory_cell_conflict`). At request time, a missing placement is
   `mapping_missing`, an uncovered requested remainder `coverage_gap`, and a request
   outside an authored column window `column_window_unavailable`, inside
   `order_blocked`.

The standing target is every held column orderable (maintainer, 2026-10-16), but
unmatched count is tracked coverage work, not a publication gate (Operator S1). Report
mapped/unmatched counts before and after each build, never remove unmatched columns from
the denominator.

**Pooled-rule impact.** The pinned inventory contains 2,791 range/list columns: 1,232
have all legacy target ids in D, three have some, 1,531 have none, and 25 are unmapped.
This static target census does not establish temporal coverage. No resolved D database
is available in this checkout or the authorized inputs tree, so the conservative
planning estimate for the rejected complete-coverage rule is **0–1,235 temporal-refusal
candidates among currently at-least-partly-resolved columns**. This is an upper-bound
range, not a measured point estimate; the 1,531 missing-target cases need
supplemental-definition/slot resolution before temporal assessment. Under the adopted
intersection rule, partial coverage causes zero publication refusals: it yields
clips/notices; empty intersections yield unmatched notices. Measure actual partial/empty
counts in e-holdings/e-compare against the pinned D artifact, without claiming that this
static inspection measured them.

The old inventory's historical-spelling fallbacks are replaced by variable/window
lookup: a table explicitly serving a variable can serve its other historical delivered
spellings in that table, only over owed-interval slices where that other spelling is
documented, the mapped spelling is absent, and no other physical column serves that
logical cell. This is an explicit-variable fallback, not authority to auto-match an
out-of-period historical header. Retain the exact physical column; never treat an alias
as possession of another variable or use fallback to defeat an owner/window refusal.

Storage checks run in the holdings compiler **after mapping**, separately from N1: the
`disagrees`, `narrower`, `uncurated` and `unmatched` findings of the old steward build,
against type curation and physical precision/scale/length. N1 sees catalog metadata, not
this storage export. Storage mismatch findings require review; missing or ambiguous
schema facts and unsafe output types remain publication/materialization refusals. No
disagreement auto-corrects provider types.

### Compiled facts and publication

`reg-build holdings --base <published-db> --steward <id> --steward-repo <checkout> --out <file>`
is the separate stage. The id selects the toolkit's `stewards.toml` pin; the supplied
checkout must match it, and its config supplies all input/curation paths. It verifies
the catalog, reads it without modifying or copying it, and emits only the holdings layer
(maintainer 2026-10-29). The base's entities, row ids, documents and search indexes stay
in the catalog file. Holdings inputs and stored cross-file mappings use public ids;
integer ids are local to each database and never joined across files without their
database origin. Do not import schema-v3's obsolete programme id ranges or `UNION ALL`
of unqualified row ids.
`reg-build verify <holdings-db> --catalog <catalog-db> --steward-repo <dir>` validates
the pair, including cross-file references that SQLite cannot enforce as foreign keys. It
admits the clean steward checkout against the repository, commit, config and input pins
recorded in the holdings file, then independently re-reads the census and storage
export. Every census table and exact physical column must be present, with no extra
coordinates; offered columns must match the source schema, table type, storage type,
length, precision and scale. Compiler-recorded counts are summaries, not the oracle.
Physical mappings must equal the complete admitted explicit mappings or automatic
spelling matches: interval unions grouped by table, variable, role, carried flag, caveat
and authored window. Verification derives these expectations from the admitted sources
independently of compiler candidates and placements, including shared projection keys.
Missing coverage or changed attributes refuse with located
`holdings_mapping_source_mismatch`. Refusals locate source rows/column ordinals or
artifact table/column ids without printing private physical identifiers.

The census is the authority for physical possession (maintainer, 2026-11-02), and its
replacement is an ordinary pinned input change followed by recompilation. Reviewed
routes and windows on surviving columns remain authored inputs. A vanished column named
by an authored decision is `stale_declared_mapping_target`, located to that decision and
its route locators; it is not silently retired. A new column passes through ordinary
interval-local matching and is retained with `mapping_missing` when undocumented. There
is no migration or carry-forward of old physical rows.

Steward attestation can support years absent from the reference catalog (maintainer,
2026-11-02). Provider labels, definitions and value sets for those years are not a
prerequisite. This is a review rule, applied in the steward repository when authoring
`inputs.reference_occurrences` (its evidence is that repository's
`reconcile_census_continuity.py` and `census-continuity-evidence.jsonl`); the compiler
does not evaluate it. A canonical occurrence can be added when all five conditions hold:

1. The same register and delivery-table series are established by the census naming and
   pinned series topology, including its covered-year tables.
2. The exact physical column name occurs for that variable, or its spelling is
   documented. The same exact census spelling with an unambiguous covered-year mapping
   is a spelling witness; the pinned reader's literal-prefix rules still apply.
3. The periods are contiguous or the series is continuous, with no documented break,
   recode or intervening `continues` successor. Missing provider metadata alone is not a
   documented break.
4. Storage families are compatible with covered-year occurrences. Actual covered-year
   storage facts take precedence over provider type descriptions; nearest covered facts
   are compared so an unrelated later storage change is not projected backwards.
5. No contrary fact assigns the name to another variable in that year/table or documents
   a meaning change. Accepted earlier-coding bindings remain contrary evidence to
   replacing that coding with the later canonical meaning.

A documented steward variable may retain its private definition namespace: both
placements must have located provenance to the same actual source register and census
series. That namespace alone does not establish a different physical source.

A failed condition leaves the column held. Shared-shape choices without a separating
fact remain held. Documentary new-variable definitions still use ordinary supplemental
transcription and ledgers. Compiling the declared occurrences never changes global
validity or invents historical provider types, code lists or definitions.

A census table need not have a logical route when no route is proved. Its held columns
remain held, and other columns without an accepted owner remain `mapping_missing`.
Absence of a route grants no possession; a declared route for an offered binding must
resolve. This lets a fully held table survive a census refresh without a guessed route.
An authored mapping may itself have `hold` and `follow_up`, with a reason and locator.
It produces a `mapping_held` notice with its requested window and follow-up, never a
placement or automatic fallback. This preserves a proved earlier-coding binding beside
an unproved later meaning without pretending that either widens global validity.

The compiler's closed steward config adds `inputs.holdings` (authored replacement
topology), `inputs.census_reader` and `[pins]`, mapping consumed relative paths to
SHA-256. A directory pin uses the catalog builder's sorted `path\tsha256\n` tree digest;
individual file pins are also accepted. Every supplemental manifest input is pinned too.
Config bytes must match the selected Git commit, including when index flags hide
worktree changes. Paths stay inside that checkout and directory traversal refuses
symlinks. `--registry <file>` selects an explicit alternative pin registry, including
the synthetic corpus; it does not weaken commit or input admission.

`inputs.holdings` declares physical tables and reviewed column decisions once. The old
inventory and series remain migration evidence, not additional compiler authorities.
Automatic matching still reads the raw census against the resolved semantic view; it
does not consume generated automatic mappings. The census reader's closed config names
the table header and numbered-column prefix and permits evidenced literal prefix
rewrites only. Identifiers remain exact after reader trimming; rewrites affect matching,
not emitted physical names. CSV errors identify input roles and row numbers without
echoing private source values.

Ordinary supplemental curation records its locks with
`reg-build lock --base <catalog> --inputs <steward-repo> --curation <steward-repo>/curation`.
The same premise path runs during holdings compilation. A referenced base public id
records its kind and exact catalog generation/file digest in the ordinary premise;
changing the base requires explicit lock review. `holding_relation` stores these
ordinary `derived_from` links with qualified public-id endpoints and local statement
provenance. The supplemental subject and reference object retain their own database
origins, including reverse graph queries. Only derivation crosses this boundary;
identity/succession relations are not imported as shortcuts. Standalone catalog builds
do not admit `--base` or emit a catalog with unresolved external endpoints.

Paired metadata search filters register, table and variable hits to accepted placements
and their requested intervals before counts and ranking (above). A code or document hit
is not itself a possession claim; linked values and documents remain available through
semantic metadata. Supplemental results retain their own group, count and cursor.

Steward-only logical descriptions use the ordinary readers/resolver/emitter in a
supplemental semantic part of the holdings file. No existing global register is
overwritten. Their public ids must be distinct except for the explicit same-variable
occurrence bindings below; the reader resolves an ordinary public id to one database
origin before using its local row id. Shared code lists are addressed by content hash.
The same catalog-check implementation can read this composed semantic view without
changing base entities. Ordinary SWECOV-only provider definitions and steward slots both
use this home (maintainer 2026-10-29); pre-2018 current coding also uses ordinary
supplemental coding-table definitions; there is no new addressable "form" entity and no
base-copy/append pipeline.

Steward-attested occurrences of a canonical variable use the same ordinary CSV reader
and ledger with the canonical variable/table ids. The pinned optional
`inputs.reference_occurrences` TOML declares each `(table_pid, variable_pid)` with a
locator and reviewed identity evidence from provider documents or census/series
continuity. The compiler and pair verifier require both endpoints in both origins, the
same register/provider, dated tables, nonempty source periods, and no overlap with
existing reference segments. Both endpoints must exist, but the base may omit their
membership: an evidenced, nonempty steward occurrence can supply the received table's
missing key without inventing global validity. An empty supplemental occurrence still
refuses (`holdings_reference_occurrence_empty`), even if the base lacks membership. Only
the explicit variables, tables and their parent registers may share public identities,
plus overlays: a steward register under the same provider, or a table in the same
register with the same timelessness, may reuse the reference id to add steward-minted
variables and tables to it (#2134). Readers keep the reference row and merge only the
steward's distinct contents. Variable collisions without a declaration still refuse
(`holdings_semantic_collision`). These declarations are occurrence bindings, not
aliases, derivations or copied reference rows. The verifier compares them with the
pinned input. Holdings schema 2 adds `holding_reference_occurrence`; older holdings are
recompiled.

Reference `coverage` spans retain their original meaning. In selected steward mode, the
attested periods reach readers only as held years (`coverage`) and placement rows
(`segments`); a variable's holdings include these steward-attested memberships even when
the global member list omits them. The occurrences' own text and locators are not
served: a release carries catalog items, never steward working files or census row
references (maintainer 2026-11-03).

`reg-build/src/holdings/schema.sql` owns the holdings DDL, except
`holding_reference_occurrence` (created in `reg-build/src/holdings/references.rs`) and
`holding_relation` (`reg-build/src/holdings/relations.sql`), and
`reg_core::HOLDINGS_SCHEMA_VERSION` its exact schema gate, separate from the catalog
schema. `meta` records owner kind/id, holdings generation, catalog generation and file
digest, the steward pin, the input-manifest digest and the compiler digest. Admission
checks both schemas exactly and the pairing pins; it does not accept another generation
because the public ids happen to resolve.

Holdings schema 4 adds the optional `holding_column.slot_family`; older holdings are
recompiled. Family declarations and their evidence remain steward input facts, with no
register or physical-table naming rules in the compiler.

Holdings schema 5 adds `holding_notice_candidate` (#2123) and drops background data from
the published pair (#2113): the `locator` columns of `holding_table`, `holding_column`,
`holding_mapping` and `holding_notice`,
`holding_reference_occurrence.locator`/`evidence`, and `meta.input_pins`. Both stay
steward input: a locator on every decision locates compiler and verifier refusals, and
the evidence on every reference occurrence records its review in the steward repo. The
supplemental semantic part keeps the catalog schema but emits no `occurrence_locator`
rows, since its source rows are steward working files. `meta.input_manifest_sha256` (one
digest over the sorted `path\tsha256` pins) replaces the per-file pins; `steward_repo`,
`steward_commit` and `steward_config` stay because `reg-build verify --steward-repo`
needs them to admit the pinned checkout and recompute that digest. Build provenance for
review lives in the `--report` JSON under `steward`: repo, commit, config and every
input pin. That report is private release evidence.

The following tables describe the physical layer's grains; all intervals use the
catalog's half-open day convention:

  | Table             | Columns and meaning                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
  | ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `holding_table`   | `id`, exact `physical_name`, `schema_name`, `table_type`, compiled `file_name`, `disposition` (`dated`, `timeless`, `unknown`, `lookup`, `excluded`), `delivery`, `partition`, optional `projection_of` and `projection_token`, optional `period_column_id` and `period_encoding`, pinned `key_requirements` and reason. One row per physical table. `delivery` is the canonical authored dated period-set spelling, NULL for non-dated dispositions; `_default`/`unknown` are not stored values. It is not a provider release id or final/preliminary selector. |
  | `holding_route`   | `holding_id`, public `table_pid`: the table's default logical routes.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
  | `holding_period`  | `holding_id`, `start`, `end`: disjoint normalized delivered intervals; no intervals for unknown/excluded disposition. Timeless is explicit, never inferred from missing dates.                                                                                                                                                                                                                                                                                                                                                                                   |
  | `holding_column`  | `id`, `holding_id`, exact `name`, source ordinal, storage type, length, precision, scale, optional slot family/kind/index as topology evidence, `disposition` (`candidate`, `hold`, `unmap`) with its reason and follow-up issue. Includes every census column, even when it maps to nothing.                                                                                                                                                                                                                                                                    |
  | `holding_mapping` | `id`, `column_id`, public `table_pid`, public `variable_pid`, `start`, `end`, `role`, `carried` and caveat/window. No spelling selector or concept-to-member identity shortcut. One accepted semantic placement over one served interval. Public ids are resolved and indexed at build time.                                                                                                                                                                                                                                                                     |

Required held keys are the received row's entity components and time components,
reviewed from received-row evidence. The pinned physical `[[table.key]]` declarations
are their authority, not builder panel keys or a catalog identifier role. The 2026-10-23
ruling excludes ordinary linking identifiers and descriptive fields. A donor table's
identifiers do not redefine the received row unit. Each pinned physical table declares
these requirements in `[[table.key]]`; the compiler stores that evidence as
`holding_table.key_requirements`, and standalone verification compares it to the pinned
source again. This is metadata on the physical table, not a new `holding_key` or
`tbl_key` entity. Every offered interval must have exactly one applicable declaration
and every declared physical component must have an applicable mapping on that received
table through its authored default or explicit mapping routes. Those mappings must cover
each offered span; a globally existing variable or a mapping on an unrelated route
cannot satisfy the declaration. There is no identifier-based or missing-key fallback.
Steward orders use the same declared entity/time components and their received column
names. Linking identifiers remain ordinary columns and enter the order only when
explicitly selected.

A time component can instead be supplied by a declared physical period column, or by the
table's period (`year`, `month`, `day`, or the attested delivery `period`). A constant
year/month/day must be the same across the entire physical table; a pooled multi-year
table cannot pretend to supply one year. An attested delivery `period` must be one
merged bounded span, not disjoint deliveries or an open interval. A physical period
column is offered as an implicit time supplier only where an active declaration uses
`period_key = "column"`. Physical time components remain required beside any additional
implicit period dimension. Missing physical components require an explicit, located
table-key hold and follow-up; it must be the table's sole key declaration, without a
scoped `period`. Its columns remain held, not unmatched, and never become orderable
placements. New census columns remain ordinary unmatched notices. Semantic `role` is
retained from the reference or supplemental occurrence; it does not by itself make an
identifier required. Carried copies must be declared entity/time components, including
received keys whose old donor role was not `row_key`. They grant no additional ordered
cell. Stable key ordering derives from column ordinals. Holdings listings are metadata
(§13 D9); this does not authorize publishing private source documents, secrets or
microdata.

Partition and projection are mutually exclusive. A projection parent must exist and must
not itself be a projection. Each mapped child span must have a nonempty shared declared
entity/time key: matching entity components and matching time components in the
applicable child and parent declarations. The parent must actually hold every shared
component over that span; missing claims refuse with located
`holdings_projection_key_missing`. Only shared components the parent holds are carried;
child-only key components remain ordinary child placements. The relationship claims a
shared key served by the parent; the child remains a separate extract with its own key
and file. It does not require the parent to hold every child data variable or cover its
interval. A child-only data cell is an ordinary placement ordered from the child's file.
If the same logical cell is placed in both parent and child, the duplicate is authorized
by the declared projection; undeclared duplicates still refuse. Carried-key rules remain
in force. A child's required held keys may add entity components absent from the parent
(recipient × child) and are independently key-validated. No projection is an SQL join
instruction.

This follows the maintainer's 2026-10-18 rulings "Projection, all 6 years" for
`RTB_PostNr_YYYY` of `RTBYYYY` (`series/rtb-postnr.toml`) and "HUSH projection of PERS"
(`series/swecov-sos-ekb-hush.toml`) for the EKB household extract: postal codes and
household amounts are child-only data, not claims that the parent holds them.

Index mappings by logical table, variable and start/end and columns by holding/name.
Coverage is computed from indexed facts, not stored guesses about future UI summaries.
Include the input-manifest digest and the accepted census accounting in `meta`; publish
no steward locators or per-file pins (schema 5).

`reg-build verify` is the sole publication validator. It checks exact census accounting
(one disposition per table; every physical column represented), stored disposition and
periods against the pinned authored topology before using them as bounds, referential
integrity, form/window containment, complete storage facts for orderable columns, key
roles and presence, disjointness/uniqueness of emitted placements, projection rules and
located caveats. Competing candidate owners have already become
`ambiguous_delivered_owners` notices with their contested placements omitted, so they do
not violate uniqueness; verify re-derives each blocked interval and its recorded rivals
from source, since orders refuse by them. Partitions authorize several explicitly
disjoint locations; projections, carried keys and documented position families authorize
duplicates only under their stated rules. A warning cannot license an otherwise
ambiguous mapping. Unmatched columns are notices/worklist entries, not publication
blockers; they count in coverage but do not order until a valid mapping exists.
Excluded, lookup and retained-unknown tables are accounted with reasons; a lookup used
as a logical codelist is not thereby observation data.

For example, `Swedbank_V15` is explicitly partition `v15` for 2020
(`series/swedbank-v15.toml`); its label is not inferred from its name. Conversely,
`FK_VBBARN_lev4` is projection `barn` of `FK_VB_lev4`, over 2015–2022. Its recipient ×
child × month rows carry both `P1105_LopNr_PersonNr` and `P1105_LopNr_Barn_PersonNr`;
they must never be merged into recipient rows. This is a settled maintainer ruling
recorded in `series/fk-vbbarn-lev4.toml` (2026-10-16).

Publish `reg_meta_swecov.db.zst`, the SWECOV holdings layer, beside `reg_meta.db.zst`,
the reference catalog, in the same `reg_meta/v*` release. They are a required pinned
pair, not two copies of the reference catalog. In general,
`reg-meta serve --catalog <steward> --db <dir>` opens `reg_meta.db` and
`reg_meta_<steward>.db`, checks the configured owner, and verifies the pair. For SWECOV,
`--catalog swecov` opens `reg_meta_swecov.db` and checks owner `steward:swecov`, both
schema versions and the exact catalog generation/digest named by the holdings file, then
exposes their composed view. `--catalog global` opens only `reg_meta.db`; it does not
make physical promises. Missing or mismatched files fail before serving, with no
fallback. Provider documents remain in the base catalog; approved steward-derived
documents live in the holdings document tables. Runtime TOML is not read. Each
response's generation identifies the selected pair; the manifest also records both
component generations. The default global installation still requires only one file.
Project-file loading is deferred; hosted `serve` derives its steward from startup
configuration and accepts no owner override in a request.

Generation hashes cover the exact base generation and file digest, pinned inputs,
topology/curation digests, schema and compiler semantics. Sort natural keys before row
id allocation; normalize intervals and serialize deterministically; fix SQLite and
compression settings. Never hash clocks, absolute paths or the artifact's own final
digest into itself. The external release digest authenticates the compressed bytes.
Changing a physical spelling or caveat changes generation. Two identical builds and two
identical materializations produce identical bytes; SQL extract row order is not
promised (the runner deliberately has no `ORDER BY`).

### From a logical order to physical files

`materialize_order` is `POST /api/project/materialize`, exposed on the existing coarse
`order` MCP tool. The hosted request is only `project`; the serving steward and pinned
holdings pair come from startup configuration, with no request owner override. Add
optional `extract_format = dta|parquet|csv` to project schema 5.2.0 ("Project document
5.2.0"), defaulting to `dta` in order authoring. `project.rs::SCHEMA_VERSION` admits
5.2.0 exactly; every other version is `unsupported_schema_version`. Older projects are
re-authored, not migrated or admitted by a compatibility path (AGENTS.md, "Maturity and
compatibility"; ARCHITECTURE.md, "Project schema version"). When the optional field is
absent, use the authoring default without inserting it before project hashing. The
manifest always carries the resolved format. This preserves the researcher-selected
one-format-per-project contract (`reg_mona_runner/DESIGN.md`, "Types per format") and
the hand-off in `steward.json.order_instructions`: the researcher sends only
`project_data.json`; the coordinator reviews it and generates/downloads `order.json`.
This is not reopened as a choice of operation-only format parameters.

There is no scope parameter: the configured deployment decides ("A steward deployment
serves its holdings only" above). Project loading is deferred. New
operations/shapes/refusals are registered in `conformance/api/operations.toml` and
`errors.toml`. A blank or whitespace-only project name refuses physical materialization
as `project_unnamed`, because provenance must identify the named order; it remains a
valid logical draft. Report all located findings in `order_blocked`, with no partial
manifest. Failures name physical metadata and source or binding paths, never row values.

The v3 `global_fallback` manifest is **dropped**: it described already-held data without
physical tables, filters or added held keys, and ordered nothing. Today's global logical
`order` is that use's contract ("The logical order" above). Do not emit a
runner-readable fallback or invent physical locations from catalog availability. A
caller with no holdings gets the logical result only.

Materialization takes the project and configured steward view, resolves its format, and
binds artifact identity to the result. It first calls the logical check, then intersects
each resulting table/variable/form interval with accepted holdings windows. Partial
possession is an explicit physical clip and wholly unheld selections block. A
publish-time `ambiguous_delivered_owners` notice has no accepted contested placement; a
request for a variable among its recorded rivals therefore blocks over that interval as
`mapping_missing`, not a second runtime holdings-rival resolution. Catalog ambiguity
remains blocking even if only one rival is held. Physical findings never disappear into
the logical `validate.ok` result. The separate physical operation reports its own
success/refusal.

Select all explicit partition members needed for the requested interval. Projection
duplicates use the parent's serving placement for an ordered shared key and retain the
projection's carried copy for its own file. Deduplicate repeated bindings by exact
physical column while retaining their logical provenance. The physical header wins for
that file even when the catalog displays a newer spelling (clean-room design §4.2, §13
D12); case alone never creates a second logical variable. Add only required held row
keys of each selected shape. A missing or ambiguous held key blocks the complete order;
do not substitute a vaguely similar identifier. Row-key-only requests and required
key-only files are valid. The row-key restriction is settled by the runner contract, not
a general permission to extract identifiers.

One output file corresponds to one physical table and its authored physical `delivery`.
Ordered columns come first in logical request order (wide members in numeric member
order), then held row keys in stable key order, with exact duplicates emitted once.
Several logical sources sharing a file share its row filter. Union their requested row
ranges and report that every column in that file is extracted over that union; never
silently claim per-column row restriction. This union-filter behavior is already the v3
rule (historical design, step 6), confirmed by maintainer 2026-10-29. For annual
physical SQL tables, emit one file per year without a row filter; apply union OR only
inside a physical table spanning multiple requested periods. No joins, stacks, pivots,
recodes or researcher display-name renames run in MONA.

**Positional pivots need real targets.** D's `sos/par/ids.toml` anchors the provider DIA
variable at `PAR_OV: DIA`; it does not define `DIA1`…`DIA30`. The 197 legacy
position-member uses cannot be described as existing catalog slot variables. The adopted
model for slots with no existing interval-valid D variable is steward-only slot
variables (maintainer 2026-10-29): transcribe each documented received slot, mint normal
ids, group them by curated set and record lineage to the provider concept. Existing
interval-valid D variables win over every legacy member/slot target by folded-name
resolution; only unmatched slots need new ids. In particular the five legacy year-member
uses are resolution/retargeting work, not an automatic minting queue. D32 still governs
catalog identity: no catalog series entity and no merge of co-delivered slot variables.

Under this decision, `holding_mapping` has one `variable_pid` per physical
column/window; it has no member discriminator. Optional `slot_kind/index` on
`holding_column` describes topology/order and never invents variable identity. A
documented position family may place the same existing variable in several columns of
one physical table (maintainer e-slots ruling, 2026-11-02). Each member explicitly
declares the same nonblank `slot_family`, `slot_kind = "position"`, a distinct positive
integer `slot_index`, and a locator evidencing the slot association. The exception is
local to the same physical table and logical table/variable; it grants no cross-file
duplication or temporal coverage. Index spellings such as `2` and `"02"` denote the same
position: overlapping placements of the same variable at those indexes refuse as
duplicates. The verifier compares family, kind and index with the pinned steward input
(`holdings_slot_source_mismatch`) before accepting the exception. Bare slot tags,
different families and undeclared duplicates still refuse. Existing distinct steward
slot variables keep their identities. For a worked target shape (illustrative ids
pending the normal ledger review),
`(table_pid = swecov/par-sv-slots, variable_pid = swecov/par-sv-slots/dia1, interval = [2019-01-01,2021-01-01))`
points to `SWECOV_SOS_SV.DIA1` and carries `slot_kind = position`, `slot_index = 1` on
its physical column. The provider long DIA is lineage evidence, not the target of that
placement. Without an explicit evidenced family mapping, an order of provider DIA does
**not** select `DIA1`…`DIA30`. Do not publish mappings against invented ids. A family
declaration authorizes physical placements; it does not add a logical-order expansion or
a new catalog series entity.

For example, in the adopted SV slot model, select the thirty adopted slot variables as a
set; each maps to exactly one literal `DIA<N>` column, and the person row key is added
once. CIS has two distinct `emp04` cases: `CIS2006.emp04` fold-matches D's
`scb/innovation-foretag/emp04-enligt-leverans`, asserted over 2006
(`scb/r257/curation.toml:613`). `CIS2004.emp04` does not fold-match that target at 2004:
D documents `antal-anstallda` as `EMP`. It needs the explicit reviewed mapping from
`series/cis.toml:1907–1909` to `antal-anstallda` at 2004, or stays `mapping_missing`.
Neither case mints a duplicate year slot or uses reference-year meaning to extend
documented delivery coverage. The slot model is settled; the SV ids above remain
illustrative until normal ledger review mints them.

Column windows narrow a mapping independently of slot metadata. In
`swecov-sos-barn-insats.toml`, `P1105_LopNr_PersonNr` serves the current BU shape over
2007–2021 and the older `bu-insats-pnrq0` shape over 2015–2016. `PLACDAT1` is an
ordinary 2015–2016 window onto the already-declared D table `bu-insats-pnrq0`
(`sos/bu/ids.toml`, handle `bu_insats_pnrq0`); it needs no new semantic composition
mechanism. A whole-table date cannot stand in for that window. Caveats about current
coding before 2018 stay attached to their actual window; they must not clone or widen
global segments. The source records the **Operator's warn-not-hold call** (2026-10-19,
batch-g), not a maintainer ruling. Preserve that accepted behavior. **Operator decision,
derived from maintainer 2026-10-29 Q1:** represent pre-2018 current coding with explicit
ordinary coding-table definitions in the pinned steward repo and its supplemental
semantic part, preserving provenance and the Operator 2026-10-19 warn-not-hold notice.
Never widen global validity; there is no holdings-only exception contract. This follows
Q1's ordinary semantic-record home for steward definitions. PLACDAT1 and the ordinary
older-table mapping remain ordinary column windows, outside this coding decision.

The six `RTB_PostNr_YYYY` projections are a worked key-only case: ordering the person
key is served by the main `RTBYYYY` placement; a selected projection retains its own
key, even if its selected content reduces to that key. It remains a separate file. The
projection and year-end interpretation are settled by `series/rtb-postnr.toml`
(maintainer rulings 2026-10-16 and 2026-10-18); neither requires a new policy decision.

### MONA order file

The physical manifest replaces v3 directly, with one closed versioned schema in
`reg-core`, exposed by the API operation registry and consumed by the runner. No v3
shim. File-oriented objects avoid repeating conflicting file names and filters on each
entry:

- `version`, required `extract_format` (`dta`, `parquet`, `csv`), `provenance`, `files`,
  `clips` and non-blocking `findings`;
- provenance: `mode = steward_holdings`, steward, catalog release/schema/generation,
  holdings owner/generation, base generation and project schema/hash/name;
- each file: safe `name`, exact physical `schema`, `table`, `delivery`, optional
  partition or projection, one optional `row_filter`, and ordered `columns`;
- each column: exact physical `name`, optional `stata_name`, expected storage metadata,
  logical source/binding/table/variable/period/spelling references, row-key/carried role
  and optional steward slot topology metadata (maintainer 2026-10-29). Several logical
  references may share one physical column.

For a 2019–2020 request on `SWECOV_SOS_SV`, the logical interval is
`[2019-01-01,2021-01-01)`. The file's filter names `AR`, encoding `year`, with inclusive
`ranges = [{from: 2019-01-01, to: 2020-12-31}]`. The compiler subtracts one day from a
finite half-open end; it never copies `2021-01-01` into the inclusive runner bound.
After minting the adopted steward slot definitions, select their thirty bindings plus
`P1105_LopNr_PersonNr` once. Physical-table naming yields `swecov_sos_sv.dta`
(maintainer 2026-10-29). A provider delivering one physical SQL table per year, such as
LISA per year, instead yields one file per annual table with no row filter. The union OR
filter applies only when one physical table spans several requested periods; it unites
the requested ranges for that file, with any inclusion-over-request reported. This
confirms the existing union-filter rule (maintainer 2026-10-29).

**Physical-table filenames (maintainer 2026-10-29).** This replaces the 2026-10-23 v3
rule: `<provider>_<register>_<variant>[_<placement>][_<period>]`, omitting `_default`
and adding the physical id when sibling names collide
(`git show f5993d7d8^:crates/DESIGN.md`, "The file's name"). Neither that text nor its
`file_stem` implementation includes `_default`; omission is part of the baseline.
Keeping the rule has real costs: it names the held edition rather than the filtered
request (so the period can be read incorrectly as extracted coverage), and it names a
shared physical table after one majority variant. D also removes that old variant
selector vocabulary. The adopted names follow the physical table, with periods in
manifest metadata instead; no accidental `_default` leak is asserted. Forty-four census
table names contain characters outside `[A-Za-z0-9_]`. Reuse `reg_resolve::slugify`'s
existing transliteration, run-coalescing and leading-letter rule, with `_` as separator
instead of implementing another normalizer; then apply runner length, Windows-device,
log-name and case-insensitive collision checks. The current helper hardcodes `-`, so
implementation parameterizes that same helper or uses its output with `-`→`_`, rather
than pretending it already has a separator argument. An empty or colliding result
refuses with `holdings_filename_invalid`; never disambiguate by iteration order. An
authored `file_name` override is added when the first real collision appears. Normalize
and store `holding_table.file_name` in `reg-build`, which already depends on
`reg-resolve`; readers consume it without gaining a dependency on the resolver. The old
physical-id suffix mechanism is accounted as changed, not silently forgotten.

The runner keeps its operational contract ([runner
design](../reg_mona_runner/DESIGN.md), "The run", "SQL", "Types per format", "No row
values in logs"): named `orders/<project-number>.json`, hand-run by a data manager, one
file per physical table/delivery, all identifiers/types resolved before rows are read,
parameterized filters, atomic per-order failure cleanup, success archive and
metadata-only logs. Encodings remain `year`, `yyyymm`, `yyyymmdd`, `date`; finer bounds
widen to the encoded grain and are reported. `row_filter = {column, encoding, ranges[]}`
keeps sorted disjoint ranges joined with OR; each range has optional inclusive
`from`/`to` bounds. Omit a bound when it cuts no held day. Preserve interrupted and
open-ended ranges (`row-filter-disjoint`, `row-filter-open-ended` runner cases).
NULL/unparseable period values do not pass a filter. Unfilterable pooled holdings report
that all rows are included; they do not pretend that selecting columns restricts rows.

The researcher default is `.dta`; the manifest always specifies its format and the
runner supplies no default. Preserve Stata aliases, exact large-number handling,
precision/range refusals and preflight checks. A logical inference cannot override the
actual SQL storage/writer contract. Expected schema metadata makes storage drift a
located refusal before extraction, not permission to coerce. The R script validates the
closed manifest independently at its input boundary; its validation is not another
logical order resolver.

The file carries metadata only. Agents never run on MONA. Extract files and runner logs
stay on MONA. Only aggregates passing the separate fixed disclosure policy and human
review can leave ([export-gate design](../reg_export_gate/DESIGN.md), "Trust boundaries"
and "MONA mode"). An order is neither disclosure approval nor an export receipt.

**Materialization rules (e-order, 2026-11-03).** `reg_core::manifest` holds the types,
`safe_file_name` and `stata_name`; `Issue` and the result `Period` moved to
`reg_core::issue` so the manifest owns its findings type. The cases are
`conformance/cases/materialize/`.

- `version` is the integer `4`; the runner refuses v3. Absent `extract_format` resolves
  to `dta` in the manifest only; the project and its hash are untouched. `stata_name` is
  the v3 rule byte for byte, emitted only on a `dta` order.
- Provenance: `catalog.generation` is the served pair (`meta.generation`),
  `base_generation` the reference catalog's `generation_id`, `holdings.generation` the
  holdings file's.
- Precedence: `holdings_unavailable` (no holdings served; usage, HTTP 400) before
  `project_invalid`, then `project_unnamed` (usage, 422; `order` still accepts the
  draft), then the logical `order_blocked`, then the physical one. A physical
  `order_blocked` carries the logical non-blocking findings too.
- Gaps are judged per binding over all its entries. A requested day no accepted
  placement holds is a `coverage_gap` warning plus a clip when the binding holds some
  day, and `mapping_missing` (error) when it holds none. A day a routing table delivers
  but only a column `window` leaves out is `column_window_unavailable` under the same
  partial-vs-whole rule. A day of an `ambiguous_delivered_owners` notice that names the
  variable among its rivals is `mapping_missing` even when the rest is held. A clip is
  the binding's own: its requested days are the logical clip recorded under the project
  binding's variable, so a `follow_successor` binding whose entries name only successors
  still reports the days lost between hops.
- A partition member is selected when it places a requested entry; a sibling without
  that column is no gap. A projection serves a (table, variable) only over the requested
  days its parent does not place it; a request the parent covers selects no projection
  file.
- Keys are the holding's `key_requirements` (entity, then time, in declaration order)
  over the days the file extracts (the requested days widened to the encoding's grain,
  or the whole delivery when unfiltered), scoped by a requirement's `period`; each
  source sharing the file gets key references over those days. A key column a binding
  orders stays `ordered`; else `carried` when the mapping says so, else `row_key`, with
  `binding: null` references. A source whose binding orders the key column keeps only
  that binding's reference; every other source sharing the file still gets its
  `binding: null` key references. An active requirement with `period_key = "column"`
  adds the physical period column once as a `row_key` with no references. A requirement
  on hold or uncovered is `held_key_missing`, a key two columns place over the same day
  `held_key_ambiguous`; both block the order. A key placed in one column for some years
  and another for the rest joins each over its own years. `reg-build holdings` already
  refuses an uncovered or overlapping key (`holdings_key_missing`,
  `holdings_key_ambiguous`), and a hold holds the whole table, so in an admitted pair
  both guard a foreign artifact only.
- A stored `file_name` failing `safe_file_name` or colliding with another file's when
  case is ignored is `file_name_unsafe`; a NULL `schema_name` is
  `physical_schema_missing`. Both block, located at the file's first binding.
  `reg-build holdings` already refuses such a name at publication
  (`holdings_filename_invalid`), so `file_name_unsafe` guards only a foreign artifact.
- Non-blocking infos: `rows_unfiltered` (no period column and the request is narrower
  than the delivery), `row_filter_widened` (the encoding's grain adds days around the
  source's own request that no source asked for), `row_filter_union` (a source whose
  days are narrower than its shared file's union). Each is located at the source. A
  selected mapping's `caveat` is a `mapping_caveat` warning located at the binding over
  the selected days (the Operator's 2026-10-19 warn-not-hold call); an automatically
  added key's caveat is located at its source.

### Logical types (N1) and metadata for local mocks

N1 runs after source normalization and occurrence curation, with the other review-only
proposals (`reg-build/src/proposals/types.rs`, beside V1), not in a SWECOV reader or the
runner. Keep source dtype/length evidence distinct from the inferred logical type. A
disagreement with the reader's `[types]` map emits a located `N1` review with both
types, evidence and a proposed ordinary `set type` where representable. It does not
automatically rewrite a segment, clear ambiguity or silence F1. This follows the
maintainer ruling of 2026-10-28: curate only evidenced affected occurrences; unresolved
integer/decimal switches remain visible. The input basis is settled: N1 infers from the
type string and value pattern and emits review on disagreement (clean-room design §4.5).
Value patterns mean documented code literals/pattern metadata, never MONA row samples. A
column's role is a separate attribute (§2.2); a row-key role is not a dtype
disagreement. The implementer uses a conservative closed pattern table: complete
documented code literals and normalized type strings only, no sampling threshold or
name-only inference. An all-integer code domain with no significant leading zero may
propose integer; alphanumeric or zero-padded codes preserve text, and documented
non-integral numeric literals may propose decimal. Unsupported/ambiguous domains propose
nothing. This is review output, not permission to reinterpret an amount from an
incomplete code list.

The implemented table reads a used occurrence's documented codes less its
statement-declared sentinels and emits `N1:logical-type` (review, on the occurrence)
with a `set type` proposal in two cases only: text whose codes are all integers with no
leading zero proposes `integer`, and an integer whose numeric codes include a
non-integral `.` literal proposes `decimal`. It proposes nothing for a decimal's integer
codes (the list does not prove an uncoded amount integral, and D's ruling keeps
integer/decimal switches to evidenced curation), for letters or punctuation on a numeric
column (unflagged missing markers such as `.` and `blank` are as common as real codes
there), for a comma (`1,5` may list two codes), or for a classification-bound occurrence
(its binding governs the codes). A curator adopts a proposal with the `set type` it
carries, or acknowledges the review. A read-only query of the 0.73.0 catalog estimates
about 25,900 reviews on 3,850 variables, all from the text rule (V1 publishes about
155,000). Documented date/number-format extensions require an evidenced later rule
change, not an open maintainer question for E. Curated > rule > source is fixed
(clean-room §2.4). N1 reviews propose and never apply a correction: consumers use
adopted normalized types until curation changes them (§4.5; D's TYPE FAMILIES and
NUMERIC SWITCHERS rulings). Integer and decimal remain different families; unresolved
types stay unresolved. No unreviewed inference may change mocks, output types or catalog
generation as if adopted.

For example, `curation/readers/scb.toml` maps `numeric` to `decimal`, and `char` to
`char`. An integer-looking coded column can still require text: leading-zero codes must
survive. A code list alone proves the listed codes, not that an uncoded amount never has
a decimal. N1 records that distinction; storage `numeric(p,s)` remains storage evidence,
and `s = 0` does not retroactively change provider metadata.

For a concrete pattern input, `DIA1` in the SV pivot is a diagnosis code slot. N1
compares its normalized type string with its documented code domain: an alphanumeric
pattern supports text, whereas a numeric-looking subset does not support integer
inference. `P1105_LopNr_PersonNr` and `P1105_LopNr_Barn_PersonNr` in the FK child
projection instead have separately evidenced recipient/child identifier roles. Mocks
reuse distinct synthetic key domains without recasting a numeric storage type as a role.
These are worked metadata inputs, not claims of findings from an unrun N1 pass.

**Local mock skill, later (maintainer 2026-10-29).** Mock generation is not a server
capability or part of E's build. A later skill on the researcher's machine reads
`project_data.json` and calls catalog/MCP operations for metadata. E adds no mock
endpoint, MCP mock tool, enable-mocks gate, CSV/ZIP writer dependencies or generator.
Both hosted and stdio MCP expose metadata operations only. The skill itself, writer
choices, synthetic generation rules and seed/size controls are later work.

E must expose enough metadata to support that skill. Existing operations already supply:

- `schema`: table/variable/spelling windows with adopted type, length, value-set
  reference, role and state (`reg-catalog/src/ops/schema.rs`);
- `segments`: adopted type/length/format, value-set/classification references, role,
  state and optional provenance (`ops/segments.rs`);
- `values`: paged codes, labels, sentinels and hierarchy by value-set hash or pinned
  classification (`ops/values.rs`);
- `order` on the coarse `order` MCP tool: logical entries with type/length/value-set and
  classification references, plus row keys, clips and findings (`ops/order/mod.rs`).

In reference mode `coverage` exposes documented provider spans, not possession
(`ops/coverage.rs`). In steward mode it reports the held years per table, with no
unavailable or mapping-missing intervals (#2175), and the semantic operations read the
supplemental steward part. `materialize_order` on the same `order` tool adds exact file
names, physical headers, slot topology, filters, keys and logical references. Each
column must expose its adopted logical/output type, length and value-set/classification
references, or provide exact references so `schema`/`segments`/`values` can retrieve
them. Logical row-key entries have only variable/spelling; key types come from those
semantic operations, never from the header. Unknown remains unknown; N1 proposals become
effective output types only after review/adoption.

The e-types audit (#2152) found no gap: in steward mode `segments` rows are the holdings
file's placements (column, file, years held); `materialize_order` columns carry exact
`refs` (source, binding, table, variable, period, spelling) that join the logical
order's entries, which carry the adopted type, length, value set and classification;
row-key types and identifier kinds come from `schema`/`segments`; `values` flags
sentinels; open N1 reviews are read through `findings`. The manifest keeps storage types
only, as the runner checks SQL storage; it gains no logical type.

Mocks read no MONA rows or empirical distributions. Use adopted types, lengths, value
sets and classification pins; refuse unknown/unsupported types needing a guess. Generate
conspicuously synthetic keys with stable reuse only for explicitly compatible id kinds,
never real person numbers. A panel warning is not proof that keys join. No holdings
means no physical-parity claim; the reference logical result remains usable.

For the FK child projection, a small mock produces separate parent and child files,
reuses synthetic recipient keys, and adds synthetic child keys without asserting the
real number of children or observations. For `CIS2006.emp04`, it uses the existing D
`emp04-enligt-leverans` type/window; `CIS2004.emp04` is mockable only after the reviewed
`antal-anstallda` mapping at 2004 is adopted, otherwise its `mapping_missing` blocks the
plan. No mock invents that association or a new long person-year table. Mocks preserve
file topology, columns and type boundaries; they do not claim representative
missingness, frequencies or valid causal relations. The later skill must use the extract
plan's column/name limits. These examples state the metadata contract for that later
consumer, not an E generator implementation.

### Vocabulary checklist and proof

This checklist inventories the retired Python builder's holdings and steward code, their
holdings/steward cases and the SWECOV series, and records what became of each item.

  | Today's item                                               | Disposition                   | Reason                                                                                                                                                                                |
  | ---------------------------------------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | Annual tables; pooled editions; disjoint period lists      | Kept                          | Explicit physical coverage; no runtime name inference or artificial yearly files.                                                                                                     |
  | `edition_spelling` (8 nonannual series)                    | Converted                     | LA/VT/HT anchors converted into the authored `delivery`, using the existing period grammar; no school year guessed from a filename. Not a config field: the closed config refuses it. |
  | Point-date status editions                                 | Kept                          | RTB postnr's `2015-12-31` is a one-day status, not a calendar year.                                                                                                                   |
  | Partitions                                                 | Kept                          | Declared disjoint shards such as Swedbank `v15`, not competing deliveries hidden by a label.                                                                                          |
  | Projections                                                | Kept                          | Own grain/keys/file; parent is not a projection.                                                                                                                                      |
  | Wide position/month/year members (197/36/5 uses)           | Changed                       | Existing interval-valid D variables win, including resolution of the five year members; mint only missing steward slots (maintainer 2026-10-29). Index stays topology evidence.       |
  | Column windows                                             | Kept                          | PLACDAT1 uses the existing `bu-insats-pnrq0` over 2015–2016; pre-2018 current coding uses ordinary supplemental coding tables, per the Operator decision above.                       |
  | `select_owner` (280 uses)                                  | Changed                       | Explicit reviewed column `mapping` per table/window; cannot clear catalog ambiguity. Not a config field: the closed config refuses it.                                                |
  | `hold` + `follow_up` versus `unmap`                        | Kept                          | Interim unresolved metadata with issue versus permanent non-mapping; neither is silently auto-mapped.                                                                                 |
  | `graft_onto`                                               | Changed                       | Convert provider gap-fill candidates to reviewed ordinary assertions/setters, or steward definitions; no generator silently grafts physical facts into provider history.              |
  | Historical spelling `_fallbacks`                           | Changed                       | Variable/interval lookup serves historical forms only without another physical owner; no spelling selector or alias-based possession of another variable.                             |
  | Row-period columns, `ranges[]` OR filters                  | Kept                          | Union of sorted requested ranges, optional inclusive ends and explicit encodings; confirmed by maintainer 2026-10-29; annual physical tables have no row filter.                      |
  | Column-role row keys; carried keys; key-only files         | Kept                          | Roles, no key table; only required held row keys ride along, no arbitrary identifiers.                                                                                                |
  | Timeless/year-independent; retained unknown                | Changed                       | Explicit dispositions; unknown stays accounted/unavailable. Legacy `_default` variant restrictions go.                                                                                |
  | Excluded and lookup dispositions                           | Kept                          | Exact census accounting without observation availability.                                                                                                                             |
  | Companion/crosswalk tables, including `LopNrByte`          | Kept, deferred                | Account separately from observations; no automatic extraction of pseudonym crosswalks. If the real exit order needs one, use explicit reviewed selection (maintainer 2026-10-29).     |
  | Unmapped columns and coverage denominator                  | Kept                          | Retain unmatched columns as mapping_missing notices/worklist rows; publication continues. Every-held-column orderability is a tracked target.                                         |
  | Combined tables; several logical mappings per column       | Kept                          | Each routed table/variable/window must validate; one physical extraction column may have several references.                                                                          |
  | Exact physical names; Stata aliases                        | Kept                          | Literal SQL identifiers and format-safe output; spelling is evidence.                                                                                                                 |
  | v3 logical filename template + physical-id suffix          | Changed                       | Physical-table names use shared slugify with `_`; periods stay in manifest metadata (maintainer 2026-10-29), replacing the v3 ruling.                                                 |
  | `register_variant` and canonical `representation` selector | Dropped                       | Route through D table ledgers; compile table/variable/interval keys.                                                                                                                  |
  | Mapping caveats and windows                                | Kept                          | Preserve the Operator's evidenced warn-not-hold call without extending global validity.                                                                                               |
  | Warned-gap catalog-state clones                            | Dropped                       | Ordinary supplemental coding tables preserve provenance and warn-not-hold notices; never copy widened global segments or add a holdings-only exception contract.                      |
  | Alias or succession as possession shortcut                 | Dropped                       | Documentary relations grant no possession.                                                                                                                                            |
  | Delivery-series authoring groups                           | Kept as input data            | Explicitly named tables share reviewed decisions; no catalog series entity.                                                                                                           |
  | Storage-versus-type findings                               | Kept                          | `disagrees`, `narrower`, `uncurated`, `unmatched` run after mapping; separate from N1.                                                                                                |
  | Flavor/graft/globals/errata stages                         | Changed                       | One-shot conversion to ordinary readers/ledgers/statements plus a generic holdings compiler.                                                                                          |
  | Legacy slug pins (`swecov/slugs/`)                         | Changed                       | Reconcile 55 pin files into normal ledgers; no silent fallback slug minting.                                                                                                          |
  | Steward delivery documents                                 | Changed                       | Pin selected source evidence/derived text; supplemental doc/FTS tables, original documents stay private.                                                                              |
  | Generated inventory and candidate/base/spec pins           | Changed                       | Preserve provenance/accounting; remove intermediate authority.                                                                                                                        |
  | `global_fallback` physical manifest                        | Dropped                       | The global logical order already describes the unlocated selection; it cannot run on MONA.                                                                                            |
  | `project_unnamed`                                          | Kept for physical order       | Blank/whitespace names cannot identify manifest provenance; logical drafts remain valid.                                                                                              |
  | Project-received inventories and mocks                     | Kept as deferred domain scope | `owner.kind` retained; E ships steward mode only until a real project input format exists.                                                                                            |

Companions remain the old design's explicit deferral (`f5993d7d8^`, "Deferred: companion
tables"): VaraText, school-code crosswalks, FEK keys and `LopNrByte` are not ordinary
observation variables. A small closed code set can be a value set or classification;
large lookups/crosswalks remain accounted physical companions. No `companion_of` or
automatic attach machinery is built without a concrete E order; `LopNrByte` in
particular stays deferred unless the real exit order needs explicit reviewed selection
(maintainer 2026-10-29). Its pseudonym values never enter the catalog or leave MONA.

Keep `conformance/cases/order` and its `order` runner (`crates/reg-conformance`) as the
logical contract: availability clips, row keys, ambiguity, classification and `validate`
agreement. Add a separate physical corpus driven by small readable
census/storage/curation inputs through the real holdings stage. Author expected file
plans from those inputs before implementation. Cover each behavior above at its hardest
boundary, including a refusal beside an allowed placement, a changed spelling, a column
window, a key-only projection, two disjoint partitions, pooled row filters and a storage
mismatch. Refusals assert located codes; `reg-build verify` owns structural invariants.
Adapt the existing `order` harness in E to distinguish logical and physical requests and
load the new holdings fixtures, including a mismatched catalog/holdings pair and an
actual-file spelling differing from the latest catalog spelling. Project-only cases are
deferred. This restores the holdings/steward cases removed in C; it does not introduce
another test suite or resolver oracle.

`conformance/test_release_order.py` keeps its independent published-catalog SQL census,
repeat-byte and HTTP/MCP checks. It has no steward mode. The holdings release proof is
release step 8g (`reg-build diff --holdings` against the previous paired release) plus
the integration workflow's `swecov` leg. Real-artifact runs are maintainer scheduled.

The runner and its manifests moved together (#2125). Its testthat suite reads the
materialize corpus's manifests and covers the pre-database refusals only; extraction is
verified by hand on MONA, with no SQL Server suite (#2126; [runner
design](../reg_mona_runner/DESIGN.md), "Tests"). Local fixtures prove machinery only. E
exits only after the maintainer runs one reviewed real order on MONA.

**Holdings comparison, e-compare.** The comparison checks reader-visible facts, not byte
parity ("What the rebuild is checked against" above). The orderable-column/held-period
census per physical table was compared with the released 0.64.0 SWECOV artifact (#2103),
classifying every difference as intended design/curation carryover, defect, or
unresolved input work; a failed/missing mapping is never dropped from the denominator.
From 0.72.0 on, every release compares the census with the previous paired release
(`reg-build diff --holdings`, #2143, #2172; release step 8g). The representative-order
comparison with 0.64.0 was ruled unneeded (maintainer 2026-11-04; #2070 closed).

This stage replaces the retired Python SWECOV steward build. No Rust port of its overlay
or warned-state cloning pipeline survives as a compatibility layer.

## Project semantic validation

The validate operation runs the supported-version decision, then the structural
validator ([reg-core/DESIGN.md](reg-core/DESIGN.md)), and concatenates their issues with
the order check's findings ("The logical order"). A failing project is a result, not an
error. A structural failure skips the catalog layer, so a rejected document costs no
catalog read. `validate`'s `ok` is true exactly when `order` returns a logical order. On
a steward server `validate` and `order` also run the holdings stage of
`materialize_order`, so `ok` is true, and `order` succeeds, exactly when
`materialize_order` would not block: a variable the steward does not hold is
`mapping_missing` at check time, not when the coordinator makes the order (#2194). A
steward `order` that succeeds carries the holdings stage's warnings with its own.

### Project document 5.2.0

`schema_version` `5.2.0` adds the optional top-level `extract_format`, one of `dta`,
`parquet` or `csv`: the file format of the order's extract files, one per project. It is
a string; an explicit `null` is `invalid_field_type`, any other string is
`invalid_enum_value`, both at `/extract_format`. Absent means the authoring default
`dta`. The order file resolves that default ("From a logical order to physical files");
it is never written into the project, so the project hash covers only what the
researcher wrote (an absent field hashes as `null`, like every absent optional).

Nothing else changes. The field tables and rules of 5.1.0 below hold for 5.2.0, except
that `extract_format` is a known top-level key.

### Project document 5.1.0

`schema_version` `5.1.0` adds optional boolean `follow_successor` to variable bindings
(default false). The 5.0.0 major change from 4.0.0 removed fields: `register_variant`
and `representation` were removed, so a 4.0.0 document cannot be read as 5.1.0. The
reader takes its own version exactly (`unsupported_schema_version`), with no migration
path. The field tables give each change and its reason.

Top level:

  | Field            | 5.1.0     | Why                                                                                                                                                                                                       |
  | ---------------- | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `steward`        | removed   | Its readers go (the serving catalog's name, `steward_mismatch`, the holdings checks). The holdings layer (E) decides how a project names its steward or holdings. A field nothing checks would be a shim. |
  | `extract_format` | removed   | A file format is physical. It returns in 5.2.0 as an optional field resolved into the order file.                                                                                                         |
  | the rest         | unchanged | `schema_version`, `reg_meta_version`, `name`, `sources`, `panels`, `window`.                                                                                                                              |

Source:

  | Field                  | 5.1.0                                    | Why                                                                                                                                                                                                    |
  | ---------------------- | ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
  | `register_variant`     | replaced by `table`: a table's public id | v4 has tables, not variants (design §2.2). A 2-part id names a register with one implicit table; for a register with named tables it is `table_is_register`.                                           |
  | `period`               | same grammar                             | `_default` now means "a timeless table" (design §2.1), checked against the catalog (`period_default_on_dated_table`).                                                                                  |
  | `edition`, `qualifier` | removed                                  | Selecting an edition or a qualifier is deferred until curation; the primary segments serve every order (final over preliminary). A preliminary-only window orders with the `preliminary_data` warning. |

Binding and panel:

`follow_successor` is optional and accepts only a boolean; explicit null is refused.
Absence and false leave order routing unchanged. It belongs to a binding's variable, not
a source, panel or other object (unknown keys remain `unexpected_field`). Every binding
must resolve to a variable; a non-variable reference is `fqid_unresolved`, also when the
field is present and false.

With true, follow incoming variable-to-variable `continues` rows, regardless of whether
their provenance is source or curated. Retain the original binding index, display name
and type declarations, but emit each entry with its actual variable, provider, register,
table, spelling and form. Each hop starts at the later of the predecessor's exclusive
coverage end and the relation's stated start day. Never replace an earlier window or
fill an internal gap in the predecessor. Follow transitively, including intermediate
variables outside the requested period; stop when no requested day remains or no
successor exists. A missing interval between hops is explicitly clipped. No date is
inferred for an undated relation.

A successor remains a separate register-scoped variable. Prefer the source's explicit
table succession chain when the successor occurs there, honoring its dated table
routing. Otherwise use the successor's segment tables, requiring exactly one table per
requested day. This also permits a cross-register successor without inventing a
cross-register variable or a new source. Those serving tables contribute their own row
keys. Overlapping candidate tables are `ambiguous_window`, never an arbitrary table
choice.

When the request extends beyond a predecessor's end, multiple successor rows or an
undated successor block with `variable_replaced` at error level. A cycle or a hop that
cannot advance is `ambiguous_window`. Ordinary segment checks still apply: ambiguous
successor segments block with `ambiguous_window`. Every uncovered requested successor
window blocks with `outside_availability`: withheld occurrences retain no variable
identity, so missing successor coverage cannot safely be treated as an ordinary clip.
This is deliberately conservative even for a terminal successor. A withheld identity
absent from the published catalog cannot be invented or resolved by its label. Only
published relation endpoints can be followed. Ordinary bindings and the predecessor
retain explicit clipping; preliminary data remains a warning. A refusal blocks the whole
order with the existing structured findings, never returns a partial order.

  | Field                                     | 5.1.0                                                    | Why                                                                                                                                                                                                                                                                      |
  | ----------------------------------------- | -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
  | `value_set`                               | replaced by `classification`: `class/<family>/<version>` | A value set is content-addressed by hash in v5; the pin selects an exact curated class version (schema §2.2). Missing versions are `fqid_unresolved`; unrelated versions are refused.                                                                                    |
  | `representation`                          | removed                                                  | A spelling is evidence, not a selector (design §7). The order check splits by form and returns the spelling per window. Where two spellings claim a window it is `ambiguous` and blocks; curation fixes it, a pin does not.                                              |
  | `type`, `display_name`, subtypes, formats | unchanged                                                | The researcher's declared output column. Nothing in v4 replaces it.                                                                                                                                                                                                      |
  | panel `members`                           | source names only                                        | The member object form existed only to override keys.                                                                                                                                                                                                                    |
  | panel `entity_key`, `time_key`            | removed                                                  | Steward keys come from the pinned holdings table's declared entity/time requirements, not all identifier roles. Reference-only orders use catalog `row_key` segments; their time axis is the member table's `time_point` / `time_start` segment, else the source period. |

A panel is a named grouping of sources with a comment. Which keys join across members is
the researcher's call; the order check does not compare them.

Structural rules (`reg-core`, the document alone). Removed with their fields:
`invalid_slug` and `invalid_enum_value` on `steward` and `extract_format`,
`composite_time_key_mixed_kinds`, `literal_period_invalid`,
`composite_key_inconsistent`, `time_key_member_kind_mismatch`,
`literal_time_key_duplicate`, `entity_key_unknown_column`, `time_key_unknown_column` and
the key `invalid_field_type` rules. Changed:

  | Rule                                                         | 5.1.0                                                                                             |
  | ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------- |
  | top-level keys                                               | `steward` and `extract_format` are `unexpected_field` (`extract_format` is optional from 5.2.0)   |
  | source keys                                                  | `name`, `table`, `period`, `bindings`; `edition` and `qualifier` are `unexpected_field`           |
  | `invalid_fqid` on the source                                 | `table` is a 2- or 3-part public id, provider not `class`                                         |
  | `fqid_table_mismatch` (was `fqid_register_variant_mismatch`) | the binding's `<provider>/<register>` equals the table's first two parts                          |
  | `invalid_period` for `_default`                              | gone from the structural layer; the catalog rule `period_default_on_dated_table`                  |
  | binding keys                                                 | `classification`, `class/<family>/<version>` (3 segments); `representation` is `unexpected_field` |
  | panel keys                                                   | `panel_id`, `members` (strings), `comment`                                                        |

Everything else is unchanged: `invalid_root`, `missing_required_field`,
`invalid_field_type`, `invalid_window`, `duplicate_source_name`, `empty_bindings`,
`display_name_collision`, `subtype_on_wrong_type`, the period rules,
`duplicate_panel_id`, `empty_members`, `panel_member_unknown_source`,
`source_referenced_by_multiple_panels` and `unsupported_schema_version`.

Catalog rules are the order check's, listed in "The logical order": `fqid_unresolved`
(also for an unknown table or classification), `retired_ref`,
`period_default_on_dated_table`, `period_on_timeless_table`, `variable_not_in_table`,
`table_is_register`. `steward_mismatch`, `fqid_outside_steward_catalog`, the
`representation_*` codes, `value_set_missing` and `binding_state_drifts_within_period`
go; the last becomes the `form_change` notice.

## Search

`search` and `search_facets` read the primary FTS5 index `search` (columns `name`: pid
and delivered spellings; `label`: labels; `body`: definitions, register/table
descriptions and document text), plus `search_description` for the curated variable
description. Both use `search_item.id` as rowid. The builder indexes raw text under
`unicode61 remove_diacritics 2`, which folds case and diacritics, and the reader applies
no fold of its own: `lan` finds `län` (#2163). A third index, `search_exact`, holds each
row's widest text (its description row's, else its primary row's) under
`remove_diacritics 0`, only to rank exact matches first. Pins, synonyms and type priors
are curation (D). A call returns one ranked list.

1. **Tokens.** Each maximal run of Unicode letters and digits in `q` is a quoted prefix
   term `"tok"*`, joined by spaces (AND, within one row), so FTS5 operators in `q` are
   inert and `ink` finds `inkomst`. A one-character token is a prefix term too:
   `prefix = '2 3'` only makes two- and three-character prefixes cheap. `q` over 200
   characters or holding a NUL is `invalid_parameter`; a `q` with no token returns no
   items (`{registers: []}` for facets) without a query. `ops/fts.rs::query` is the one
   implementation; `docs_search` shares it.
2. **Rank.** Primary matches first, then description matches absent from the primary
   match set. Within each, rows that also match `search_exact` (the query as typed,
   diacritics kept, case folded) come first, so `här` lists `här` before `har` and `har`
   lists `har` before `här` (#2163). Then `bm25(index, 10.0, 5.0, 1.0)` (name, label,
   body), ascending. The primary corpus and its scores are unaffected by descriptions.
   FTS5 scores a row by its weighted hit count against its whole token count, so a hit
   weighs most in an id or spelling and least in a definition or document text: a
   variable labelled `Vikt` ranks above a longer one whose definition says vikt.
3. **Ties.** Then `search_item.kind` in SQL text order (`classification`, `code`,
   `document`, `register`, `set`, `table`, `variable`), then `sort_key`, which is
   unique. Two rows tie on bm25 when the query hits the same weighted count in rows of
   equal token count.
4. **Filters**, ANDed with the match. `type` keeps `kind = type`; `classification` is
   empty until D. `register` resolves through `refs::resolve`; a ref of another kind is
   `invalid_ref`; it keeps `search_item.register_id = id`, which drops code rows and
   keeps a document linked to that register (its first linked subject's). `period` keeps
   `start < e AND s < end` over the row's hull, which drops code and document rows (no
   span). So `type=code` with `register` or `period` is empty. `tag` accepts a tag slug
   or its `tag/<slug>` public id and keeps only variables belonging to that tag; an
   unknown tag refuses with `not_found`, like an unknown register. It applies before
   paging and paired-result counts, and belongs to the cursor context like every other
   search filter.
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
7. **Facets.** One `GROUP BY register_id` over the deduplicated matches with
   `kind = 'variable'`, registers in pid order, each `{fqid, variable_matches}`. With
   `register` (resolved as in 4), only that register, adding `variables`: every matched
   variable `{fqid, name}` in pid order, `limit` ignored; `registers: []` when none
   matches. Without it, `limit` adds `name` and the first `limit` matched variables in
   pid order. With neither, counts only. A key a mode does not list is omitted, never
   `null`.

**Variable descriptions (#2039).** The one curated variable description is indexed in
`search_description`, alongside the primary name, labels, definitions and operational
definitions so AND queries can span source and curated text. A description equal to a
primary definition or operational definition is not indexed twice. The separate index
keeps the primary BM25 corpus, scores and ranks unchanged; only new matches follow,
ranked with the same weights and ties. Facets count each variable once. Search hits keep
their definition field, and `show` supplies the one description. `period` filters the
variable's coverage hull, including gaps, rather than the wording's applicability; a
dated minor change is stated in the description. The index and variable description
field are part of this PR's catalog schema 5.

`resolve` is different: exact `fold_identity` lookup of delivered column names, no
full-text fallback and no scoring. It maps known headers; it is not discovery.

## Value sets and classifications

A value set is content-addressed: its hash covers the sorted `(code, label, parent)`
items, so segments that deliver the same codes share one set, and a segment with no
value set has `value_set: null`. `values` reads a set by its hash, or lists the sets
holding a code and label. The blank code `""` is an item like any other. Classifications
(versions, codes, correspondences) use curated identities.
`curation/classifications.toml` holds family/version metadata, served names, publisher,
existence periods, sentinel codes and explicit succession edges. Its code lists
reference reader value sets; they are never copied into inventory items. Inventory
versions reuse the existing `assert classification-version` construction and emission
path. Inline `items` remain available for genuinely hand-authored assertions.

Book periods describe existence only. They are neither inferred nor truncated, and
versions of one family may overlap. Applicability comes exclusively from occurrence
`set classification` statements naming exact versions. An order classification pin
selects only compatible curated version windows, preserving coverage clipping and
refusing an unrelated version at the binding's classification field. A pin never
substitutes a version or reinterprets codes.

## Sets and tags

Variable membership is curated with `set sets` and `set tags` and emitted through the
existing membership storage. A register's `set:<slug>` subject gives an existing set its
`label`, optional `description` and ordered `axes = [{id, label}]`. These use the same
statements, resolution, locks and provenance as other curated fields. Set labels and
descriptions occupy `var_set.name_text` and `description_text`; ordered axes occupy
`var_set_axis`. A variable's `facets = {axis = {value, label}}` supplies its coordinate
in its one set, in `var_set_member_facet`; omitted labels default to the value. An
undeclared axis or a variable outside the set refuses at its statement location. These
tables are part of catalog schema 6.

Facets do not name a set, so a faceted variable must belong to exactly one set;
ambiguous membership refuses rather than choosing one alphabetically. Presentation
refusals use the ordinary curation removal and rerun path. A stale set declaration
withdraws dependent facets and their provenance: development builds continue with
located warnings, while release builds collect the refusals in their report.

`show` at `set/<provider>/<register>/<slug>` returns `kind: "set"`, its `fqid`, `name`,
optional `description`, register public id, ordered `axes`, and `members`. Each member
supplies `fqid`, `name`, `segments` (its whole per-period list: every row `segments`
serves for it, unpaged, in that order and shape, without evidence) and `facets` (`axis`,
`value`, `label`) in axis order, so the group page needs one request. The set's
`description` is its curated one; without one, a description every member states alike
is sent once as the set's (maintainer 2026-11-04). A member carries its own
`description` only where it differs from the set's. Members sort by facet value in axis
order (missing coordinates first), then by name and public id. `tags` is the union of
the members' tags in public-id order. The summary contains curated presentation data;
source evidence remains on the individual variables.

`show` at a register lists its `sets` (`fqid`, `name`, member variable `fqid`s, each in
public-id order) for the register page's grouping, and `tags`, the union of its
variables' tags in public-id order. Register tags are derived, not curated: a register
carries a tag iff one of its variables does. The 12 legacy curated register tags and
their ranks are not ported; derivation reproduces each where the register's variables
carry that tag.

The D-9 converter ported memberships; W6 restores unambiguously mapped legacy set
labels, axes and member coordinates. Delivery-column hints, multi-coordinate identity
splits, tag rank, star and note, and classification groups stay outside this
presentation model: a tag is a broad search theme with thousands of members, so a ranked
or starred representative per register would surface co-tagged variables, which the tag
ruling excludes (#1794). Tags filter search and have no `show` page.

**A set** (formerly a concept group) lies within one register and is the single-register
picking unit: one concept in one register, gathering what a researcher looking for it
there should find in one place. A set never spans registers, and finding a concept
across registers is a tag's job. Its public id addresses presentation only; it is never
a variable identity, a binding or an order key.

A set is warranted when one register has two or more variables that are each an amount
(or a coding) of the same concept for some unit under some definition. A component the
source names as part of the concept counts, and so does a quantity computed from it (a
ratio to the median, a weight, a cost-adjusted remainder). An input to the concept stays
out: gross income goes into disposable income but is not a quantity of it. A lone
variable gets no set. The build refuses fewer than two distinct final member variables
at the originating curation statement, after all memberships resolve. Repeated
occurrences of one variable never supply a second member. Artifact verification also
checks this minimum and that every member belongs to the set's register. Lock and build
refuse a variable assigned to more than one set, naming every set and originating
statement after identity resolution and before membership setters apply; tags are
exempt.

**A tag** is a curated thematic vocabulary across providers and registers, so a
researcher can find "a measure of income" without knowing the register. A tag is broad:
a topic typed before knowing which register holds it (`income`, `transfers`,
`employment`), with members in several registers. A term that would only list one
register's variables is a set. Tags are search-only: they discover and never touch
identity, and a variable page does not list the variables sharing its tags (#1794).

`curation/tag-vocabulary.toml` defines the flat vocabulary as `[[tag]]` entries with
only `id`, `label` and `definition`. Ids are unique catalog slugs; labels and
definitions are nonblank Swedish text. Unknown fields refuse. Build and lock refuse
unknown membership slugs at their curation statement locations. The vocabulary is
included in the build input hash, but never in lock premises or per-register lock
digests: editing a definition needs no relock. Tag labels and definitions populate the
existing `var_set.name_text` and `description_text`. Variable and set summaries expose
tag memberships, and `search?tag=` narrows discovery to the tag's variables.

## Relationship graph

`graph` returns the `relation` edges where the ref is the subject or the object. A
relation has a `kind`: `continues` (successor → predecessor, with its evidenced `at`),
`derived_from` (the lineage), `equivalent` and `label_of`. `kind` narrows the edges. A
lone entity with no relations returns no edges. Variable succession and table succession
use `continues`; a representation change is not a separate succession model. The public
`at` period is stored as `at_start`/`at_end` on the relation, and no day is inferred
from a coverage end.

## Documentation and what is not in the catalog

The catalog answers "what variables exist and what shape they have". Register
documentation (quality narratives, time-series breaks, legal text) is in the same
database, as catalog tables, and a document's binary is served by its own download. The
provenance database, maintainer-only and never shipped, holds build artifacts: approval
dates, delivery metadata, checksums, raw provider ids.

Localization is deferred: one canonical text per field, in the provider's language.

Population and object type are read-only metadata under a register version, not entities
and not FQID kinds.

## Versioning and determinism

- **Schema gate.** The reader requires the artifact's schema version to equal
  `reg_core::CATALOG_SCHEMA_VERSION` exactly; anything else is `schema_incompatible`.
  There is no major/minor compatibility rule. The artifact is regenerated, never
  migrated. The deploy guard is fail-closed: it compares the constant at HEAD with the
  newest release's, and a missing constant or a mismatch blocks deployment. A PR that
  bumps `CATALOG_SCHEMA_VERSION` is red at `schema-guard` until it merges and its
  release publishes. That is expected; the check is not required on main. Merge, then
  release when the maintainer runs it; a gap is allowed, with deployments blocked until
  the matching release publishes.
- **Contract version.** `CONTRACT_VERSION` versions the operation set and its JSON
  shapes, and `meta` reports it on every response.
- **Releases** are tagged per package (`reg_meta/vX.Y.Z`); each release carries the
  `reg_meta.db.zst` catalog asset (documents included), the paired SWECOV holdings asset
  `reg_meta_swecov.db.zst` and the binaries. `project_data.json` records the catalog
  release it was written against in that tag form.
- **Determinism.** The same request on the same artifact returns the same bytes: stable
  ordering with a unique final tie-breaker, sorted JSON keys where the contract says so,
  no timing in `meta`, cursors bound to the generation.
- **Security.** Metadata only, no microdata. No credentials are read or stored. The
  server makes no outbound requests.

Documentary relationships (typed source crosswalks and derivation clauses) are
`owner_bound_literal` metadata: they make no formula executable, establish no
equivalence and extend no availability.

## Glossary

The normative schema is [`reg-build/src/schema.sql`](reg-build/src/schema.sql). These
terms describe its cross-provider meanings.

  | Term           | Meaning                                                                                                |
  | -------------- | ------------------------------------------------------------------------------------------------------ |
  | variable       | The addressable variable and FQID target: identity `(provider, register, slug)`, one or more segments. |
  | set            | A register-local presentation group at `set/<provider>/<register>/<slug>`; not an order binding.       |
  | binding        | A project's use of a variable, named by its FQID.                                                      |
  | segment        | A maximal run of one form of a variable in one table.                                                  |
  | equivalent     | A curated symmetric relation between variables in different registers or providers.                    |
  | classification | A named vocabulary family, `class/<slug>`, with versions at `class/<slug>/<version>`.                  |
  | valueset       | A content-addressed code list on a segment.                                                            |

Column names are universal English; values stay provider-native. Source names and
spellings are evidence on occurrences; descriptive text varies over time in
`variable_text`. Type, length, value set, sensitivity and identifier status belong to
segments. Sensitivity follows the source per segment rather than a variable-wide flag.
Population metadata belongs to stated releases. Holdings are a separate layer
("Holdings").
