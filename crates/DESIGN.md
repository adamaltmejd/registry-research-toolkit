# Design: the Rust runtime

Design rationale and constraints for the catalog runtime: `reg-core` (grammars, folds,
interval algebra, the project contract), `reg-catalog` (the reader and its operation
set) and `reg-meta` (the `serve` and `mcp` run modes). The project contract has its own
note, [reg-core/DESIGN.md](reg-core/DESIGN.md). The artifact these crates read is built
by `reg_meta_build` ([../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md)), which
also owns the resolver, holdings compilation and the inventory TOML. Cross-package
topology is in [../ARCHITECTURE.md](../ARCHITECTURE.md).

The operation names, parameters, result shapes and routes are
`conformance/api/operations.toml`; error codes with their classes and HTTP statuses are
`conformance/api/errors.toml`. This file does not repeat them; it says why the model is
shaped the way it is.

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
infer annual availability inside it. A state's `provenance` is NULL for an ordinary
provider-documented interval and otherwise names an errata correction (with its evidence
and exact source editions), a steward row, or `inferred:resolution-gap`. It stays at
state grain so one corrected edition never relabels a neighboring window.

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
  editions each have one. A retired register or variable ref resolves to its terminal
  successor at the manifest's policy year; a split is ambiguous rather than an arbitrary
  branch (`succession_terminal`, see the build design note).
- **Representation succession**: curated `representation_replaced_by` rows record a
  column rename within a variable, optionally scoped to one variant.

Same-concept grain, vintage or coding changes appear in no edge table; they fold into
one variable at build time. Lineage (`variable_state_lineage`) is the state-grain edge
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

A steward artifact defaults to `holdings` scope, a catalog artifact to `reference`;
explicit `holdings` on a catalog is `scope_unavailable`. Reference means every node of
the selected generation, steward-only metadata included. Scope is reader state, valid on
every read.

  | Node kind              | Holdings predicate                                                                                                           | Reference                                 |
  | ---------------------- | ---------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------- |
  | Provider               | At least one admitted descendant register.                                                                                   | All providers.                            |
  | Register               | At least one admitted variable binding at a mapped variant.                                                                  | All registers and variants.               |
  | Variable binding/state | An explicit mapping under this register; a selected variant or representation must match it. Output narrows to held periods. | Full history, subject to request filters. |

Membership uses the authored variable and variant and the canonical representation. A
`same_as` edge never grants possession of a different binding. Unknown-scope tables and
unmapped columns admit nothing. An interval holding contributes only the intersection of
its periods with the request and the resolved states; a year-independent holding needs
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
holdings show register-wide warnings and those of admitted bindings. Validation and
order take no scope; they use the artifact's kind.

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
The project's `steward` must equal the artifact's steward, or `global`; retargeting is
not a feature, and a user who means another deployment edits the JSON and uploads it
again.

Per binding, in declaration order:

1. **Availability clip.** A source period means "these columns, wherever each is
   available inside this window". Each binding is clipped to the union of its states at
   the source's variant, so a column first delivered in 2019 under a 2018–2020 source
   does not widen the order into a cross-product. Every clip is reported, never silent
   and never an error, and is recorded before the ambiguity gate can stop.
2. **Representation slicing.** The clipped request splits into slices of constant
   canonical representation. A sequential rename makes two slices; two columns valid at
   the same instant without a `representation` pin is ambiguity and blocks. Resolution
   runs once per requested segment, because the window fallback is decided per query; a
   state reaching two segments stays one state. Steps 1 and 2 are one pass shared with
   validation, so the two never disagree about availability.
3. **Matching and coverage.** A table matches a slice when one of its columns maps
   `(variant, variable, representation)` and its edition overlaps the slice; only the
   intersection contributes. A held edition that overlaps the request but lies wholly
   outside the binding's column windows is `column_window_unavailable`. Any uncovered
   subperiod blocks the whole order (`coverage_gap`); a slice nothing serves is
   `mapping_missing`. The materializer never chooses between tables: the inventory's
   one-to-one invariant leaves at most one `(table, column)` per cell instant per
   partition, so every contribution is wanted whole.
4. **Emission.** Every matching table is emitted whole, every partition included.
   Entries carry the literal physical table and column; the canonical representation is
   a join key, not an output substitute. Entries keep project order, then table, edition
   and column.

**Fail closed, in one pass.** A blocked order lists every finding across every binding
(`steward_mismatch`, `project_empty`, `period_not_orderable`, `variable_unresolved`,
`binding_unavailable`, `representation_unknown`, `representation_unresolved`,
`representation_ambiguous`, `mapping_missing`, `coverage_gap`,
`column_window_unavailable`) as structured fields of `order_blocked`, so a researcher
fixes the whole order in one edit. An empty project is a valid draft that cannot become
a header-only manifest.

**The manifest is a versioned JSON contract.** It carries provenance (mode, steward,
artifact kind, project name, schema version, declared reg_meta version and SHA-256 of
the project's canonical JSON, catalog schema version and `generation_id`), the entries
(logical coordinate with the canonical representation, the clipped `requested_period`,
and the physical edition, table, column and optional partition) and the clips. It is
written here and read offline by the steward-side extract system, so it is
self-contained. Its bytes are sorted keys, two-space indent, UTF-8 and a trailing
newline, byte-identical across runs; periods render through the shared grammar. No
timestamps: the only time-shaped values come from the artifact and the project. Version
1 stays in definition until the first external reader: shape changes stay in version 1,
and from that reader on an incompatible change bumps it. `partition` is the only
optional field; absent means no partition, so an inventory without partitions produces
the bytes it did before partitions existed.

**Extraction keeps delivery topology.** One output file per ordered variant, partition
and period unit, named in slug spelling (`lisa_individer-15plus_2019.csv`,
`agi_individuppgifter-agi_arb_2021-03.csv` for a partition). What goes in as two tables
comes out as two files: shard identity (reporter stream, municipality) may exist in no
column, so a union would destroy information. A multi-period range is one file, since v1
has no row filter.

`simplify:` v1 records no row filter and orders the whole matching table. SWECOV's one
large SQL table per SoS register is the upgrade trigger; it will need period `WHERE`
clauses.

**Deferred: per-binding period override.** If a researcher deliberately wants less than
the availability intersection, the shape is a binding-level period narrowing below the
source period. The availability clip leaves the seam. Build it when someone asks.

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
failure skips the semantic layer, so a rejected document costs no catalog read.

Per source variant and binding:

- The variant must resolve, and the binding's variable must resolve, else
  `fqid_unresolved`.
- Availability is the order's shared pass (steps 1 and 2 above); this layer only
  translates its facts. Narrower availability (a leading or trailing shortfall, an
  internal gap, a pinned representation covering only part) is one informational
  `range_period_partially_covered` per binding. A list period (`2005..2010,2015..2020`)
  is one request with holes: a column delivered or co-existing only inside a hole is
  neither availability nor ambiguity. Empty availability across the whole request
  blocks. The pass's findings take this surface's names: `binding_unavailable` →
  `period_outside_state_validity`, `representation_unknown` →
  `binding_representation_unknown`, `representation_ambiguous` →
  `binding_value_set_version_ambiguous`, `representation_unresolved` unchanged.
  `binding_state_drifts_within_period` (info) reports a sequential state transition
  inside the request.
- `deprecated_traversal` (info) and `variable_replaced` (info, with `successor_fqid`
  when the successor is a binding) are hints; the binding stays valid.
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
findings. Membership uses the literal authored FQID at the source's exact variant; a
`same_as` edge grants nothing. Catalog artifacts emit neither.

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
Display text always comes from the base tables. The docs index uses the same fold and
query builder.

Each query token becomes a quoted prefix term, which neutralizes FTS5 operators and
prefix-matches (`ink` finds `inkomst`). `LIKE` patterns escape `%` and `_`. Arms over
authored names compare under `fold_identity` on both sides, because SQLite's own case
folding is ASCII-only; that folds case, not diacritics.

There are five arms: register, variable, classification, classification code and
register value. A code-shaped query (three or more characters, at least one digit) also
matches codes exactly or by prefix, and surfaces the classifications that contain a
matching code, ranked after name hits. Classifications are catalog-wide and have no
validity window, so a register or period filter turns the classification arm off.

Ranking and paging rules:

- Pins come first, then one best-bets order: an exact identity match leads, then type
  priors, then relevance. A researcher typing a whole name ("Kön") gets the variables
  carrying it first, however many registers share it.
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
the manifest's succession as-of year reaches it; currentness and terminal redirects use
the same policy. `supersedes` and `superseded_by` are projections of the active edges,
and a predecessor can fan out (`sun1996` → the 2000 nivå, inriktning and grupp
editions). Hierarchy is a `level` integer (the length of an all-digit code), not a
parent pointer; codes without prefix hierarchy (ICD-10, ATC) keep `level` NULL.

## Concept groups and tags

**Concept groups** fold machine-stamped column families for browsing: month-suffixed
families (`agi1lonfinkjan`…`agi1lonfinkdec`), split-sibling coding successions
(`sun2000inr`/`sun2020inr`). A group has ordered axes (stable `name`, curated `label`)
and members with per-axis facets; a variable can be several members of one group, one
per delivery column. Groups are presentation only: never an FQID kind, a binding, an
order key or a stats key. Identity-level folding by classification family was tried and
dropped after 195 measured over-folds; because grouping is presentation, a wrong group
is a cosmetic bug, not identity corruption. A variable or classification belongs to at
most one group.

Classification vintages are not grouped: editions are a temporal succession, not a
parallel facet. Curated classification umbrella groups (`group:sun`: the three SUN 2020
dimensions plus the version-independent nivå aggregates) have no axes; their members are
distinct classifications.

**Tags** are a curated thematic vocabulary across providers and registers, so a
researcher can find "a measure of income" without knowing the register. One global
vocabulary and one membership table; a membership names exactly one register or one
variable, and a variable membership can be starred with a one-line rationale. Tags are a
discovery overlay and leave identity untouched. A group shows the union of its members'
tags, and untagged siblings inherit the group's tags as neutral memberships.

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
- **Group ⇄ member.** A member ref roots the graph on its group's members, with
  `focus_id` on the member; a group ref roots on the members with no focus. A member
  page and its group page therefore show the same union.

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
  | variabelnamn                    | name                               | variable                                             |
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
