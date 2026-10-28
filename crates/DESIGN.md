# Design: the Rust runtime

Design rationale and constraints for the catalog runtime: `reg-core` (grammars, folds,
interval algebra, the project contract), `reg-catalog` (the reader and its operation
set) and `reg-meta` (the `serve` and `mcp` run modes), plus the design of the builder
that replaces `reg_meta_build` ("The catalog builder"). The project contract has its own
note, [reg-core/DESIGN.md](reg-core/DESIGN.md). The artifact these crates read is built
by `reg-build` ("The catalog builder"); `reg_meta_build`
([../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md)) stays until milestone F.
Holdings are not compiled into it: they return in milestone E. Cross-package topology is
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
  build (`reg_meta_build/DESIGN.md`, "Base and derive"), which also keeps the reader
  small.
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

Each contract that crosses a language or package boundary has exactly one
implementation. New code that needs one of these calls its home; it does not add a
second copy held in step by a parity test.

  | Contract                                | Single home                                                                                                                 | Reached from                                                                                                    |
  | --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
  | Artifact schema (DDL)                   | `reg-build/src/schema.sql` (Rust builder)                                                                                   | `reg-catalog` reads it; admission by the schema version                                                         |
  | Catalog schema version                  | `reg_core::CATALOG_SCHEMA_VERSION`                                                                                          | `reg-build` stamps it; `reg-catalog` admits on equality; the deploy guard compares it with the newest release's |
  | Artifact validation                     | `reg-build verify`                                                                                                          | The single authority; the release tier and the release skill run it                                             |
  | Resolver semantics                      | `reg-resolve` (Rust)                                                                                                        | Compiled into the artifact                                                                                      |
  | FQID and period grammar                 | `reg-core`                                                                                                                  | Build via `reg-core-py`; reader                                                                                 |
  | Interval algebra over periods           | `reg-core`                                                                                                                  | Build via `reg-core-py`; reader                                                                                 |
  | Text folds                              | `reg-core`                                                                                                                  | Build via `reg-core-py`; reader                                                                                 |
  | Canonical JSON + SHA-256 (build hashes) | `reg-frame::canonical_line` and value-set hashing (`src/lib.rs`); `reg-build`'s `main` module (input and generation hashes) | Build only                                                                                                      |
  | Project schema + structural validator   | `reg-core` (`project::check`)                                                                                               | Server and MCP; JSON Schema export                                                                              |
  | Result types                            | Rust structs (serde + utoipa)                                                                                               | OpenAPI and MCP tool schemas derive from it                                                                     |

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
  types and the structural validator, error codes. Compiles natively, as a Python
  extension (`reg-core-py`, PyO3 via maturin).
- `reg-catalog`: the reader. Opens and admits an artifact and defines the operation set:
  one function per operation, typed parameters and results with JSON Schemas. Search
  (one ranking), the logical order and project semantic validation live here.
- `reg-meta`: the binary, with run modes only. `serve` is the HTTP API (axum + utoipa;
  early `ETag` revalidation, body-size limit and rate limit as tower layers) plus the
  remote MCP endpoint over streamable HTTP. `mcp` is MCP over stdio against a local
  catalog, for offline use. There is no query CLI.
- `reg-core-py`: the PyO3 module the build imports. Its uv `cache-keys` cover the Rust
  sources, because uv otherwise rebuilds a path dependency only on a `pyproject.toml`
  change.

Each crate's `Cargo.toml` is its dependency list. Adding a runtime crate is a design
decision, not an implementation detail. `toml` is a runtime dependency only of
`reg-readers`, which reads curation for the builder; the runtime keeps it a dev
dependency.

**Distribution.** The server image is one static binary plus the baked `reg_meta.db`,
fetched in the Dockerfile with `curl`, SHA-256 verification against GitHub's asset
digests, and `zstd`. Each `reg_meta/v*` release ships `reg-meta` binaries for macOS
arm64 and Linux x86_64 with SHA-256 checksums, and one catalog asset, `reg_meta.db.zst`,
documents included. There is no separate docs asset; SWECOV's asset is not produced
until milestone E. The binaries serve local HTTP or MCP; the catalog is downloaded with
`curl` and `zstd`. There is no PyPI package and no `fetch` run mode. Windows agents use
hosted MCP; a local Windows binary is added only if it builds and runs without special
effort. The `microdata-tools-se` plugin declares one hosted MCP server whose URL is a
user setting: the global catalog by default, a hosted steward catalog
(`https://data.swecov.se/mcp`) instead. A local `reg-meta mcp` is added to the agent's
MCP configuration by hand.

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
  a variable without an id, or a ledger id reported unplaced (no placed target) or split
  (anchors reach several variables). The latter refusals name the ledger file, line and
  id; dev builds keep reporting them without refusing.
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
  normalized typed content, including evidence and caveat but excluding file and line.
  Publication hashes also include provider and register, preserving separate authoring
  locations for identical statements in different registers.
- The typed model and address grammar live in `reg-frame`; the TOML loader lives in
  `reg-readers`. Matching, conflicts, premise validation, application and provenance
  belong to `reg-resolve`; `reg-build` owns lock IO and located diagnostics. The emitter
  writes resolved decisions.
- Application is one ordered pass: parent table/release assertions, table coverage
  overrides, occurrence/release period overrides, table moves, remaining assertions,
  variable assignments, ordinary fields, then links and acknowledgements. Each stage
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
  statement were absent. The same exact-selection rule governs every field override:
  change only selected whole occurrences, or the selected entity for fields owned by
  tables, releases or variables. Selectors and premises use the immutable phase view.
- `label`, `definition`, `operational_definition`, `unit`, `value_set`, `coded`,
  `spelling`, `identifier`, `sensitive`, `type`, `length` and `format` belong to
  occurrences; text runs and segments derive from those occurrences. `period` changes
  occurrence bounds or the addressed release's placement. `description` on tables and
  releases stays on those entities. `coverage` replaces a table's source bound and
  requires a table selector and one contiguous period; it lives in the table's existing
  bounds. The overridden source bound remains evidence and the review premise, so a
  source-bound change reopens review. `sets` belongs to variables; typed curation
  decisions resolve to variable memberships. `notice` resolves to a finding on its
  selected subject. The emitter serializes these resolved facts and never derives them
  from provenance. Occurrence descriptions live in the existing `override` table and are
  returned with segment evidence; they do not replace definitions or operational
  definitions. No field gets a second home. A value-set override chooses the
  occurrence's own set while retaining its source population alternatives as evidence.
  Unsupported entity/field combinations are located refusals. Type corrections that
  change the family participate in F6 and ambiguity resolution; length corrections do
  not. Corrected conflict markers clear while the original premises remain evidence.
  Format is carried and published without changing agreement or F1 findings.
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
  these same rows (`follow_successor`, D-10); continuation proposals remain D-6.
- `label_of` is a relation consumed by the runtime's graph endpoint. The converter's
  legacy code/label pairs created presentation groups; D-3c does not synthesize those
  legacy groups, change identity or alter order bindings.
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
  version follows occurrences into segments and orders. `tags` remains refused with D-9.
  Sentinels, roles and unit types remain deferred to D's planned sentinel and
  nondefault-role/unit-type work. Entity names and synonyms remain deferred to D's
  entity-name and search curation.
- `curation.lock` is canonical JSON with `version = 1` and normalized statement hashes
  as keys. Premises contain sorted `subjects` and `objects`: source locators,
  coordinates, overridden values, guards and dependent facts, including the destination
  facts used by table moves. Dependencies are evidence, never extra rows selected for
  rewriting. Missing or changed premises, empty or ambiguous selectors, and conflicts
  are located refusals. Release builds fail; dev builds skip refused statements and
  report them. A `set` equal to the source is a redundant-set finding.
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
`doc:<hash>#page=12`); only `ack` with `severity = "notice"` may omit it. `caveat` is
optional user-facing text. Kind-specific content is:

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
`classification`, `tags`, `sentinels`, `role`, `unit_type`, `name` and `synonyms`.
Source-shaped assertion fields have a separate closed schema, including table `name` and
occurrence `type` and `length`. Lists of sets, tags, sentinels and synonyms normalize as
sorted sets; asserted codes sort by code, while duplicate codes are refused. Addresses
normalize folded spellings, expanded braces and sorted guards and period tokens before
hashing. Links add relations; conflicting successions refuse. There is no
successor-choice or `reject` form. Merged release changes update every claim to the
release's final placement.

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
- **The LISA workbook** is its own reader (`lisa`), not the Socialstyrelsen template's:
  a sheet is a table named by the sheet, a data row an occurrence keyed by its folded
  column name, a year cell one occurrence per year or range, and `Känslig1` the
  sensitivity flag ("I vissa fall" is sensitive, like SCB's `KansligVariabelIbland`).
  D-8 attaches it to SCB `r34` through ordinary curation, generated by the temporary D
  converter: folded spelling, worksheet family and whole occurrence periods select
  export rows, including D-4 assertions. VarId changes remain distinct identities.
  Workbook wording supplies occurrence descriptions only where no curated description
  exists; SCB labels and definitions stand. Evidenced sensitivity differences use
  occurrence setters, lineage uses `derived_from`, and individually reviewed quantities
  and codings of one concept use one consolidated `sets` setter per final variable.
  Headings yield reviewable candidates from the reader's section map, and adopted sets
  are ordinary curation. Distinct benefits do not form a set merely because they share a
  heading; notes, empty or broad parents and singletons supply no set. Unmatched rows
  and period mismatches remain reported gaps, never invented deliveries or spelling
  equivalences. Document subjects and lineage endpoints use the same attachment map. The
  workbook reader remains available for inspection; its standalone register is retired
  from publication. The reader also returns a source-ordered section → retained rows
  map, separate from the frame, exposed by `reg-build read` as `.sections.json`. Each
  heading, parent, note and unheaded block carries its source locator; membership lists
  physical retained row locators once, even when a year cell emits several occurrences.
  This is inspection evidence, not curation or automatic sets. Text-only cells in the
  column-name column are candidates. Source font size, emphasis and bottom borders
  distinguish headings from notes: body-sized italic text is a note unless it introduces
  undated rows; a smaller continuation closing an adjacent larger heading's border band
  is a note. Calamine exposes no font metadata, so the reader reads styles from the same
  pinned file's XML, using its relationships rather than assuming worksheet filenames.
  No heading spelling, row number or style-id table is encoded. A heading followed by
  another heading before any data is a parent; retained counts do not turn a repeated
  timeless heading into a parent. On the first dated row after undated rows, membership
  returns to an unheaded block instead of inheriting the timeless heading. Notes never
  change membership. These rules describe this source's layout; headings remain
  reviewable candidates and broad parents never become sets automatically. Each
  `Grundregister` cell is a `derived_from` link from the row's variable to each register
  it names (the reader splits at `,`, `;` and line breaks). D curation in the attach
  script splits `/` and `&` lists and strips each source's `Härledd-` prefix, then uses
  an exact curated register-alias table with evidence notes. Several sources can resolve
  to one register endpoint. Unknown, ambiguous or unpublished sources remain reported
  with reasons; a proved source in the same list still links. The attachment report
  lists endpoints and unresolved sources per original text, distinct emitted links and
  newly added links. `Individ` repeats the year-independent columns with blank year
  cells; the year-less sheet's row stands, so `Individ` keeps its 1990– span.
  `Individ årsoberoende` holds three tables (birth country, deaths, migration) under
  headings, each keyed by `PersonNr`; the reader keeps the sheet as one table and any
  later partition requires whole-occurrence `set table`. Labels keep their footnote
  markers (`…yrkesverksamma7`) verbatim: a trailing digit cannot be told from a name's
  own (`SNI92`), and the footnotes are not read.
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
  `doc_subject_unresolved` in the build report, and the document stays searchable.
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
  a plain array. Cursors bind to the generation, so a stale cursor fails loudly. No
  offsets.
- **One parameter per concept and one result shape per operation**, independent of
  result count.
- **Errors** are `{error, meta}` with
  `error = {code, class, message, remediation, fields}`; MCP returns the same document
  as a tool error.
- **Few, coarse MCP tools**, about ten, because every tool's schema occupies agent
  context. Fine-grained HTTP routes map onto the same operations.
- **Catalog selection is configuration**: one catalog per process, chosen at start.

**Hosted MCP.** `serve` exposes the global catalog's `/mcp` at reg-meta.vqv.uk, so
agents need no install and no 1.2 GB download. `/mcp` and every POST share one rate
limit per client address, sized for tool calls (`reg-meta/src/limit.rs`). A published
server admits its MCP host (`--public-host`).

**Hosted MCP has no webapp handoff** (maintainer, 2026-10-20, #1709). Holdings and
ordering return in milestone E; `show`'s root names no webapp. The `order` tool stays:
researchers describe data they already hold on MONA as a `project_data.json`, and it
returns the logical order.

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
from its segment count and the `class/` prefix alone. `reg-core` owns the grammar
(`grammar.rs`, oracle `conformance/cases/grammar/`); the catalog assigns the ids
(ledgers under `curation/`) and resolves a ref to an entity
([REBUILD.md](../REBUILD.md)).

- 1 segment is a provider, 2 a register, 3 a table or a variable (a register's implicit
  table has the register's 2-part id).
- A leading `class/` marks a classification (`class/<slug>`) or a version
  (`class/<family>/<version>`). `class` is reserved as a slug everywhere, and so is
  `group`, the first segment of the group refs the grammar still reserves.
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
it is posted to `validate` or `order`. Holdings return in milestone E.

## The logical order

`order` runs the order check of design v4 §9 on a 5.1.0 project and returns the logical
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
`panel_row_keys_differ`, `period_clipped`, `form_change`, `table_succession_undated`,
`identifier` and `coverage_end_unstated`. Each is an `Issue` whose `path` points at the
source, binding or panel and whose `source`, `variable`, `table` and `period` say what
it is about (`null` for a whole-project finding). The conditions are the order check's
to state in its own tests; the rules that shape them:

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
    "project": {"name": "income-panel", "schema_version": "5.1.0",
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
  "meta": {"contract_version": "5.1.0", "generation": "9f2c…", "catalog_version": "0.65.0"}
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
         "period": {"start": "2019-01-01", "end": "2020-01-01"}, "successor_fqid": null}
      ]
    }
  },
  "meta": {"contract_version": "5.1.0", "generation": "9f2c…", "catalog_version": "0.65.0"}
}
```

The 2020 window of `dispink` does not block: it orders, with the warning
`preliminary_data` (no final release covers it). The finding stays until curation
resolves the ambiguity; narrowing the period to 2018 alone also clears it.

## Project semantic validation

The validate operation runs the supported-version decision, then the structural
validator ([reg-core/DESIGN.md](reg-core/DESIGN.md)), and concatenates their issues with
the order check's findings ("The logical order"). A failing project is a result, not an
error. A structural failure skips the catalog layer, so a rejected document costs no
catalog read. `validate`'s `ok` is true exactly when `order` returns a logical order.

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
  | `extract_format` | removed   | A file format is physical. It returns with the order file in E.                                                                                                                                           |
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
keys and panel identifier kinds. Overlapping candidate tables are `ambiguous_window`,
never an arbitrary table choice.

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

  | Field                                     | 5.1.0                                                    | Why                                                                                                                                                                                                                         |
  | ----------------------------------------- | -------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `value_set`                               | replaced by `classification`: `class/<family>/<version>` | A value set is content-addressed by hash in v5; the pin selects an exact curated class version (schema §2.2). Missing versions are `fqid_unresolved`; unrelated versions are refused.                                       |
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

  | Rule                                                         | 5.1.0                                                                                             |
  | ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------- |
  | top-level keys                                               | `steward` and `extract_format` are `unexpected_field`                                             |
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
`table_is_register` and the warning `panel_row_keys_differ`. `steward_mismatch`,
`fqid_outside_steward_catalog`, the `representation_*` codes, `value_set_missing` and
`binding_state_drifts_within_period` go; the last becomes the `form_change` notice.

## Search

`search` and `search_facets` read one FTS5 index, `search` (columns `name`: pid and
delivered spellings; `label`: labels; `body`: definitions, descriptions and document
text), whose rowid is `search_item.id`. The builder indexes raw text under
`unicode61 remove_diacritics 0`, which folds case only, and the reader applies no fold
of its own: `lan` does not match `län`. Pins, synonyms and type priors are curation (D).
A call returns one ranked list.

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

A value set is content-addressed: its hash covers the sorted `(code, label)` items, so
segments that deliver the same codes share one set, and a segment with no value set has
`value_set: null`. `values` reads a set by its hash, or lists the sets holding a code
and label. The blank code `""` is an item like any other. Classifications (versions,
codes, correspondences) use curated identities. `curation/classifications.toml` holds
family/version metadata, served names, publisher, existence periods and explicit
succession edges. Its code lists reference reader value sets; they are never copied into
inventory items. Inventory versions reuse the existing `assert classification-version`
construction and emission path. Inline `items` remain available for genuinely
hand-authored assertions.

Book periods describe existence only. They are neither inferred nor truncated, and
versions of one family may overlap. Applicability comes exclusively from occurrence
`set classification` statements naming exact versions. An order classification pin
selects only compatible curated version windows, preserving coverage clipping and
refusing an unrelated version at the binding's classification field. A pin never
substitutes a version or reinterprets codes.

## Sets and tags

Neither is built in the v4 catalog yet (milestone D builds them); the maintainer rulings
stand.

**A set** (formerly a concept group) lies within one register and is the single-register
picking unit: one concept in one register, gathering what a researcher looking for it
there should find in one place. A set never spans registers, and finding a concept
across registers is a tag's job. It is presentation only, never an id kind, a binding or
an order key.

A set is warranted when one register has two or more variables that are each an amount
(or a coding) of the same concept for some unit under some definition. A component the
source names as part of the concept counts, and so does a quantity computed from it (a
ratio to the median, a weight, a cost-adjusted remainder). An input to the concept stays
out: gross income goes into disposable income but is not a quantity of it. A lone
variable gets no set.

**A tag** is a curated thematic vocabulary across providers and registers, so a
researcher can find "a measure of income" without knowing the register. A tag is broad:
a topic typed before knowing which register holds it (`income`, `transfers`,
`employment`), with members in several registers. A term that would only list one
register's variables is a set. Tags are search-only: they discover and never touch
identity, and a variable page does not list the variables sharing its tags (#1794).

## Relationship graph

`graph` returns the `relation` edges where the ref is the subject or the object. A
relation has a `kind`: `continues` (a successor takes over, with the day it starts),
`derived_from` (the lineage), `equivalent` and `label_of`. `kind` narrows the edges. A
lone entity with no relations returns no edges.

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
  single `reg_meta.db.zst` catalog asset (documents included) and the binaries. SWECOV's
  asset is not produced until E. `project_data.json` records the catalog release it was
  written against in that tag form.
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
  | binding        | A project's use of a variable, named by its FQID.                                                      |
  | segment        | A maximal run of one form of a variable in one table.                                                  |
  | equivalent     | A curated symmetric relation between variables in different registers or providers.                    |
  | classification | A named vocabulary family, `class/<slug>`, with versions at `class/<slug>/<version>`.                  |
  | valueset       | A content-addressed code list on a segment.                                                            |

Column names are universal English; values stay provider-native. Source names and
spellings are evidence on occurrences; descriptive text varies over time in
`variable_text`. Type, length, value set, sensitivity and identifier status belong to
segments. Sensitivity follows the source per segment rather than a variable-wide flag.
Population metadata belongs to stated releases. Holdings arrive in milestone E.
