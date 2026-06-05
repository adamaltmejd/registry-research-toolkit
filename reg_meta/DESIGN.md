# Design: reg_meta

Design rationale and constraints for the query layer. For usage, see
`reg-meta --help`. For the domain model, see [STRUCTURE.md](STRUCTURE.md).
For build-pipeline rationale (CSV import, sentinel filtering, year
projection, classification seeding, doc-DB build), see
[../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md). For the
cross-package topology, dependency graph, and version policy, see the
root `ARCHITECTURE.md`.

## reg_meta as the substrate

reg_meta is the identifier and object-model substrate every downstream
artifact references — the `project_data.json` schema, the webapp's
`/api/catalog/*` endpoints, the generation kit consumed by
`reg_mockdata`. The contract between them is only as stable as
reg_meta's identifier scheme, so the model is built to outlast any one
provider's vocabulary.

The design rule that drives everything below: **a provider-neutral
object model**. Universal column names (`name`, `description`,
`data_type`, ...) carry provider-native string values verbatim — the
SCB `registernamn` for LISA stays under `register.name` exactly as
published; order generation reads these strings because they are what
the provider's intake form expects. The universal schema carries **no
provider-specific tables** (no `scb_*`, no `sos_*`): provider variation
is captured purely as fill-rate on the universal columns (some
providers populate fewer fields). Provider-specific parsing lives in
`reg_meta_build`; what query commands see is the unified shape. This
keeps one mental model for consumers across every provider, and keeps
reg_meta importable from any context (Jupyter, scripts, the MONA
bundle) with no provider conditionals.

The earlier (v0.11) scheme worked but baked SCB's CSV vocabulary and
yearly publication cadence into the universal model; adding a second
provider (Socialstyrelsen) and a third (Försäkringskassan) made the
cracks visible. The current substrate is the result of removing them:
a **two-level variable model** (`variable` → `variable_state`), a
**3-segment binding FQID grammar** (`provider/register/slug`) with no
variant or period slot, **slug-anchored edge tables** at variable
grain, and a **build-time triage** pass that normalizes
provider-specific oddities into the universal shape (the triage
mechanics live in [../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md)).
Prose and narrative metadata go to the doc DB; maintainer-only build
artifacts go to a sibling provenance DB (see "What's not in the
catalog").

## Agent-first design

The primary consumers are LLM agent skills and `mock_data_wizard`.
Human terminal use is supported but secondary. This drives several choices:

- Three output formats: table (default), list, and JSON for machine consumption
- All output follows a stable envelope contract (version, timing, request echo)
- Errors are structured with codes, not just messages
- Exit codes are meaningful (see below)
- Core query functions are importable as a Python library, not just CLI

## SQLite backend

All metadata lives in a single SQLite file (~1.6 GB). Chosen because:

- Zero-dependency deployment (Python stdlib)
- Single-file distribution via GitHub Releases + zstd compression
- FTS5 built in
- Read performance is excellent for this workload

The database is read-only from the perspective of query commands.
`reg-meta-build build-db` replaces it entirely (not incremental).

## Data providers

At the query layer reg_meta is provider-agnostic: one metadata DB, one
docs DB, one CLI. Users searching or resolving variables need not know
which agency published a given register — the provider is a queryable
attribute, not a separate code path. The provider-neutral object model
that makes this possible is "reg_meta as the substrate" above; the
provider-specific parsing that feeds it lives in
[reg_meta_build](../reg_meta_build/DESIGN.md) (the IR + adapter layer).

## FTS5 configuration

Two content-synced FTS5 indexes:

- **`register_fts`** — indexes register `name`, `purpose`.
- **`variable_fts`** — indexes variable `name`, `definition`, `description`.
  Uses `unicode61` tokenizer for correct Swedish character handling.
  Delivery column names (`variable_alias.delivery_column_name`) are
  deliberately excluded — they contain technical suffixes (e.g. `_LISA`)
  that pollute search results.
  Column name matching is handled by `resolve` instead.

## Register lookup strategy

All commands accepting a register argument use a three-step resolution:

1. Exact ID match
2. Case-insensitive exact name match
3. Case-insensitive substring match

This allows `34`, `LISA`, and `utbildning` to all work.

## Resolve: exact match only

`resolve` performs exact alias lookup against `variable_alias.delivery_column_name`.
No FTS fallback, no confidence scoring. Status is `matched` or `no_match`.
This is intentional — resolve is for mapping known column headers, not
discovery.

## Composite registers and source tracking

Registers like LISA, FRIDA, LINDA, and STATIV are composites — most of
their variables originate in source registers (RTB, RAMS, etc.). The
`variable` table tracks this via `source_register_id` (FK to `register`)
and `source_label` (display abbreviation or raw text). Unresolved sources
remain as raw text for human review and surface in `get schema` (source
column) and `get lineage` (consumer/source classification). The
resolution rules used during build are documented in
[../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md) §
"Source-register resolution".

## Two-level variable model

What SCB and SOS each publish as a "variable" is split into exactly two
levels, because two distinct facts are entangled there:

- **`variable`** — the **addressable variable**, the thing an FQID
  names: the provider's "define once" identity. Holds the
  register-unique slug (the FQID leaf) and the cross-era constants
  (`name`, `definition`, `description`, `measurement_unit`,
  `is_sensitive`, `is_identifier`, source attribution).
- **`variable_state`** — the **per-delivery shape**, a child of
  `variable`. A variable has 1..N states; each carries a **variant
  coordinate** and a period range, plus the data type, length, value
  set, and version label for that delivery.

STRUCTURE.md describes the same split from the SCB-domain angle (CVID
coalescing, the input-file mapping); this section is the cross-provider
rationale. The normative DDL lives in `reg_meta_build/db.py` — not
copied here.

**Why the variant is a coordinate, not an identity level.** A *variant*
(SCB `registervariant`, SOS `deldatamängd`) is a **delivery
coordinate**. "Kön in LISA" is one variable however many variants
deliver it; the same variable delivered in variant A vs B, or year X vs
Y, is a different *state*, not a different identity. So the variable is
the FQID target, and the variant and period are coordinates that select
among its states. This is the load-bearing design decision — the
empirical basis is in the next section.

**Variable formation is adapter-defined.** What constitutes one variable
depends on the provider's source structure, but the resulting `variable`
row is uniform:

- **SCB:** variable = `(register_id, var_id)` — `var_id` is the
  define-once unit, reused verbatim across the variants that deliver it.
- **SOS:** variable = `(register, variable_name)`, formed by merging
  same-named variables across deldatamängder within a register (sound
  because the structured `Kodlista_*` sheets are register-level and
  shared across deldatamängder). Genuine name-reuse collisions split
  into distinct variables.

**Keys (DECISION POINT 1).** The natural key is `(register_id, slug)`
(register-unique, the binding FQID). It stays unique even after a triage
*split* puts several variables under one source key, because siblings
get distinct slugs. `provider_key` (SCB `str(var_id)`; SOS the merged
name) is therefore a **NON-unique join hint, not a key** — the build
join "source row → variable" refines it by the triage discriminator
when a split exists, 1:1 otherwise. A **synthetic `variable_id` PK**
backs all this so `variable_state`'s FK stays single-column and the
edge tables stay stable as the natural key's provider-specific shape
varies. (The triage fold/split mechanics live in
[../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md).)

**`classification_id` and `source_label` placement.** The per-era
classification family lives on `variable_state.classification_id` (an
era can change code system mid-life — see "Classifications"), while the
human-readable source attribution lives on `variable.source_label`
(cross-era constant). `variable_state` carries `state_id`,
`variable_id`, `register_variant_id`, `valid_from`/`valid_to`,
`data_type`, `data_length`, `delivery_column_name`, `value_set_id`,
`value_set_version_label`, and `classification_id`.

**Variant-less registers (`_default`).** Socialstyrelsen LSS, BU, SOL
ship variables without a deldatamängd sheet. Adapters synthesise a
single `_default` variant row at build time (a real row, not a
resolve-time fiction), and every state references it as its
`register_variant_id`. Because the variant is not an FQID segment,
`_default` never appears in a binding FQID — it is a browsing/state
coordinate, not a path segment.

## Why two levels, not three (the variant-identity investigation)

The decision to make the variant a coordinate rather than an identity
level is empirical, calibrated against the production SCB `reg_meta.db`
(schema v0.11.x at the 2026-05-22 design lock) and the 13
Socialstyrelsen workbooks current then. The numbers are recorded here
so a future contributor questioning the shape has the anchor — re-run
them if the data drifts.

Earlier drafts treated the variant as part of variable identity (a
4-segment FQID `provider/register/variant/variable`), recovering
`var_id` reuse across variants by auto-emitting `(N choose 2)`
`variable_same_as` edges. The investigation refuted that:

| Question | Finding | Implication |
|---|---|---|
| How many SCB `(register, var)` pairs appear in more than one variant? | **78.9%** appear in exactly one variant. | Variant identity is degenerate for 4 in 5 variables. |
| When a variable spans variants in the same year, what differs? | Rolled up to the variable grain, only **4.3%** of pairs show any same-year cross-variant divergence — and it is overwhelmingly column-name or grain. | The divergence is what triage resolves (fold or split), independent of variant. The variant is never the discriminator. |
| Variant or period — which is the stronger differentiator? | **43%** of multi-period `(variable × variant)` cells drift across periods, vs 4.3% across same-year variants. | **Period** is the real differentiation axis, and it lives in `variable_state`, not identity. |
| SOS: do code sets differ by deldatamängd? | **Zero** variables have a deldatamängd-specific code list. | The deldatamängd carries no code/identity differentiation. |
| SOS: do codes vary by period? | **35%** of code rows carry `tidsperiod` ranges. | SOS codes vary by period, not variant — again period is the axis. |

The 4.3% (pairs grain) and 43% (multi-period-cells grain) sit at
different grains **on purpose** — the point is the contrast, not a
like-for-like ratio. The full denominators (42,768 `(register, var)`
pairs; 55,309 same-year multi-variant cells; the multi-period-cell
subset) are recorded in git history if a re-run needs them.

**Conclusion.** In both providers the variant is a delivery coordinate
and period is the differentiation axis. Two levels suffice: an
addressable `variable` and per-delivery `variable_state` rows each
carrying a variant coordinate and a period range. Collapsing the
three-level draft's intermediate variant-scoped row removes the
`(N choose 2)` `variable_same_as` explosion (within-register identity
is the variable itself now) and shortens the binding FQID from 4
segments to 3.

**Two corroborating signals worth keeping.** (1) State-on-variable is
real signal, not bookkeeping: 43% of multi-version triples carry drift
across editions, and coalescing-by-shape shrank ~515K instance rows to
~104K states (≈5×). (2) **Free-text fields are unreliable for identity
decisions** — an early SOS pass comparing free-text `Värdemängd`
descriptions across deldatamängder spuriously suggested ~50% divergence;
the structured `kodlistor` refuted it. Anchor identity decisions on the
structured code data, never the prose.

## FQID grammar

Every reg_meta entity has a Fully Qualified Identifier — a stable,
`/`-separated string with strict positional grammar. The kind is
determined entirely by segment count plus the `class/` discriminator
prefix; no out-of-band lookup is needed. The parser/emitter is
`fqid.py`.

| Segments | Form | Kind |
|---|---|---|
| 1 | `<provider>` | provider |
| 2 | `<provider>/<register>` | register |
| 3 | `<provider>/<register>/<slug>` | variable binding (the variable) |
| 2, leading `class/` | `class/<slug>` | classification |

```text
scb                              provider
scb/lisa                         register
scb/lisa/kon                     variable binding (names the variable)
sos/lss/insatstyp                variable binding (variant-less register)
class/sun2020                    classification (vintage baked into the slug)
class/icd10                      classification
```

**The FQID names the variable; the binding is 3-segment.** The binding
`provider/register/slug` addresses a `variable` directly. The variant
and period are delivery coordinates that select among its states —
neither is a segment.

**No variant slot (DECISION POINT 2).** Dropping the variant makes the
binding 3-segment, which would otherwise collide with the old
3-segment *variant* address (`scb/lisa/individer-15plus`). We resolve
this by removing the variant FQID kind entirely: a variant is no longer
addressed by a slash-path. You **browse** a register's variants as a
sub-resource (the catalog UI / `/api/catalog` lists them) and
**address** a variable directly, so `scb/lisa/X` is unambiguously a
variable. The `register_variant` table still exists (panel keys,
browsing metadata) and the variant is still a coordinate on
`variable_state` and in `project_data` Sources — you just never reach a
variable *through* a variant path.

**No period slot.** The same variable can have different definitions in
different years; that drift is `variable_state` rows with explicit
validity ranges, not per-year FQIDs. Year-specific resolution is
supplied via `resolve_at(fqid, period)` or `Source.period`. Time is
data context, not identity.

**Classification vintage is in the slug.** SUN2020 is `class/sun2020`,
not `class/sun?version=2020`; ICD-10 and ICD-11 are distinct
classifications with distinct slugs. Each vintage is its own normative
document, which is how researchers think about them, and the slug alone
is the global uniqueness key (no separate version segment).

**No `@version` binding suffix.** Co-delivered parallel codings (a
classification vintage during a crosswalk era — näringsgren in both
SNI92 and SNI2007 in a transition year) are **not** addressed by an
`@<value-set-version>` FQID suffix. A binding leaf is always a bare
3-segment slug. Co-delivered codings live as overlapping
`value_set_version_label`-discriminated states of one variable, and the
caller selects one with `resolve_at(..., value_set_version=...)` — not
by an FQID pin. (`fqid.py` has no `@` handling.) The binding-side
"representation" chooser — which delivery column a co-delivery maps to
— is a `project_data`/`reg_schema` concern, not part of the reg_meta
identifier.

**Slug grammar.** Every slug matches `^[a-z](?:[a-z0-9]|-[a-z0-9])*$`
(lowercase ASCII kebab-case: starts with a letter, ends with a letter
or digit, hyphens only singly between alphanumerics; single-character
slugs match `^[a-z]$`). The regex is anchored with `\Z`, not `$`, so a
trailing newline can't sneak a slug like `kon\n` past the validator (a
footgun the webapp path-guard and build-time validation both rely on).
Period-shaped strings are rejected as slugs everywhere (legibility) —
the classification vintage-in-slug folds the only former exception
away.

**Reserved slugs.** `_default` and `class` are reserved everywhere
(`RESERVED_SLUGS` in `fqid.py`). `class` keeps the leading-`class/`
discriminator unambiguous. `_default` is the variant-less coordinate;
it is the **one literal exception** to the slug regex (it starts with
`_`), so validators short-circuit on the literal string before applying
the regex. Build rejects any other slug entry hitting these.

**Design intent — HTTP-suffix slug rejection.** The webapp's catalog
routes use suffixes (`states`, `predecessors`, `successors`, `related`,
`lineage`, `lineage_warnings`, `variants`) as path segments after a
binding; a variable slugged with one of these would be unreachable via
the canonical path. The intent is for the build to reject these in the
variable slot at curation time. **Remaining/gap:** this rejection is
**not yet implemented** (issue-tracked) — `fqid.py` reserves only
`_default` and `class`. Treat the suffix ban as design intent, not
shipped behavior.

**Open — curator review cadence on rename.** Slugs are derived from the
latest delivery-column alias. If a provider renames a column between
editions and the curator hasn't yet added a `same_as` link, the
auto-rule produces a new slug for the later editions while earlier ones
keep the old slug. This is correct in principle (rename = new variable
by default), but the operational rhythm — how often curators review
newly-shipped renames — is undecided. The slug-derivation and curation
mechanics live in
[../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md).

**FQID property tests.** The grammar invariants are property-tested:
round-trip (parse → emit → parse equals identity), segment-count
discrimination (1/2/3 + the `class/` prefix), reserved-slug rejection,
and `same_as` traversal termination (cycle detection). Reserved-slug
coverage is grammar-wide under the 3-segment grammar: `_default` and
`class` are reserved **everywhere** (there is no variant slot to exempt
`_default` in — a 4-segment FQID does not parse at all). A test
asserting `_default` is accepted in some slot, or that a 4-segment
string like `sos/lss/_default/insatstyp` parses, is testing the
obsolete variant-slot grammar and is wrong.

## Catalog API surface (§5.10)

`Catalog` (`catalog.py`) is the in-process FQID→entity API the webapp's
`/api/catalog/*` endpoints wrap. `resolve(fqid)` is polymorphic over FQID
kind; the provider / register / classification arms each return their
dedicated `Resolved*` row (variant and version are **not** FQID kinds —
variant is a register sub-resource coordinate, period a delivery axis).
The **binding** arm is longitudinal (A2.5): a binding FQID resolves to a
`ResolvedVariable` —
the addressable variable's shared metadata + its full `variable_state`
history (each state tagged with its variant coordinate) + the
variable-grain edges. Period-specific resolution lives in `resolve_at`;
cross-variable traversal in the per-edge accessors. All accessors are
list-returning; `resolve_at` returns `[]` (never raises) when no state
covers the period — only the binding FQID not resolving raises
`fqid_not_found`. The method signatures are the reference in `catalog.py`
itself; the webapp's `/api/catalog/*` shape derives directly from this
surface (see `reg_webapp/DESIGN.md` §9.5).

The exact dataclass shapes live here. They are frozen `@dataclass` (no
Pydantic — reg_meta is the no-Pydantic library surface, see root
CLAUDE.md "Stack"); collection fields are tuples for frozen-dataclass
immutability/hashability.

**Why two methods for succession.** `predecessors` / `successors` are
split (not one `replaced` returning a dict) so every edge-traversal
accessor returns `list[...]` uniformly. The longitudinal
`resolve(fqid).replaced_by` attribute carries the **outbound** edges
(successors) — "X was replaced by Y" is the natural directional read;
inbound traversal is the explicit `predecessors(fqid)` call.

**Multi-state at a period is normal, not an edge case.** `resolve_at`
returns a list because length N is genuinely common: several variants
delivered the variable at the period (omitting `variant`), a range
period crosses transitions, or — the common case for
classification-versioned variables — multiple value-set versions
co-exist in the period (a crosswalk era, SNI92 + SNI2007 in a
transition year). The list shape is the contract; no exception is
raised on ambiguity. Callers who know the variant pass `variant=…`;
callers who know the vintage pass `value_set_version=…`.

**`Period`** — `int | str | dict`, the polymorphic period `resolve_at`
accepts (mirrors `Source.period`, §6.2): a bare year (`2018`), a period
token (`"HT2020"`/`"2020-Q3"`/`"2020-08"`/`"2018-12-31"`), an explicit
range `{"from", "to"}` (endpoints are int or token), or the `"_default"`
snapshot sentinel (no period filter). Expanded to an inclusive ISO
`(lo, hi)` interval by `_period_bounds` + `fqid.period_token_to_bounds`,
intersected against the full-date `variable_state` validity ranges — so
sub-annual and range queries are precise, not year-granular.

**`ResolvedVariable`** — the longitudinal binding resolution. Fields:
`fqid` (the caller's 3-seg binding FQID `provider/register/slug`,
preserved through a `same_as` traversal), `variable_id`,
`register_id`, `provider_key`, the shared metadata (`name`,
`definition`, `description`, `measurement_unit`, `is_sensitive`,
`is_identifier`, `source_register_id`, `source_register_text`), `states`
(tuple of `VariableState`, chronological ascending), the variable-grain
edges `same_as` / `replaced_by` (OUTBOUND successors) / `related_to` /
`lineage`, and `via_same_as` (the traversal path when resolved via a
`same_as` edge, else None).

**`VariableState`** — one `variable_state` row tagged with its variant.
Fields: `state_id`, `variant` (the `register_variant.slug`),
`register_variant_id`, `valid_from` / `valid_to` (inclusive ISO dates),
`data_type`, `data_length`, `delivery_column_name` (denormalized latest
alias), `value_set_version_label` (NOT NULL, `''` = no discriminator),
`value_set_id`, and `value_set` (hydrated `(code, label)` tuple, None
when the state has no value set).

**Edge semantics (reader-facing).** All relationship edges are
**variable grain** — the variant is a delivery coordinate, not an
identity level, so there is nothing below the variable to anchor an
edge on. The edge triple `(provider, register, variable)` **is** the
binding FQID. Two edge tables partition the relationship space:

- **`same_as`** — symmetric cross-register / cross-provider
  **equivalence** (substitutable: "this variable here is that variable
  there"). **Curated only — never auto-derived.** Within-register
  `var_id` reuse is now the variable itself (one variable, many variant
  states), so the old `(N choose 2)` auto-derive from matching `var_id`
  is gone; `same_as` carries only the genuinely-curated cross-register
  set. `resolve()` traverses it transitively, recording the path in
  `via_same_as`.
- **`related_to`** — symmetric **split siblings**: distinct variables
  that triage split from one source key (`kommun-hem` ↔ `kommun-skol`,
  `land-id` ↔ `land-namn`). Related in concept but **not
  substitutable** — parallel columns a researcher orders separately.

Same-concept grain/vintage/coding appears in **neither** — it *folds*
into one variable, so there is no edge (the fold/split distinction and
auto-emit mechanics are in
[../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md)).
Succession (`replaced_by`) is directional and orthogonal; lineage
(§5.6) is the state-grain composite-source edge.

**`VariableRef`** — a variable-grain edge endpoint
(`same_as` / `predecessors` / `successors`). Fields: `fqid` (the 3-seg
binding FQID — the edge tables store exactly the `(provider, register,
variable)` triple, which **is** the binding FQID since A2.6; built via
`_ref_fqid`, None only if a slug is malformed/NULL), the load-bearing
`provider` / `register` / `variable` triple, and (#142, on
succession refs only) `reason` (the `timeseries_event.beskrivning`
transition reason) + `effective_year` (the AktuellVariabel-grain
successor edition year; None on `same_as` refs and on bare-grain
succession with no edition).

**`RelatedRef`** — a `variable_related_to` sibling (§5.7 split). Same
`fqid` (3-seg) + `provider`/`register`/`variable` triple as `VariableRef`,
plus `relation_kind` (the split reason,
e.g. `same_definition_different_column`).

**`LineageEdge`** — one `variable_state_lineage` row (§5.6
consumer-side, state grain): `consumer_state_id`, `source_state_id`,
`valid_from` / `valid_to` (the validity intersection), and `source_fqid`
(the source state's 3-seg binding FQID; None only on a malformed/NULL
slug, as with the refs' `fqid`).

**`LineageWarning`** — one `variable_state_lineage_warning` row:
`consumer_state_id`, `warning_kind` (`no_source_state` /
`ambiguous_source_variant`), `message`.

`ResolvedVariableBinding` (the interim per-edition binding row) and the
`editions()` discovery path that returned it were **removed in A2.6** along
with the v0.11 5-seg binding parse. Resolution is now `ResolvedVariable` +
`resolve_at` / `states` (§5.10): the variable's shared metadata plus its
`variable_state` rows, each tagged with its variant. The per-edition cvid is
no longer a catalog return shape, and the variant is a register sub-resource
coordinate (passed to `resolve_at`), not a slash-path FQID segment. The v0.x
per-edition `resolve()` behavior was deleted, not aliased — pre-v1 policy
(no shims).

## Value sets are year-projected

`Vardemangder.csv` is the historical union — every code that ever
applied to a variable in any register year, with no temporal qualification.
`VardemangderValidDates.csv` is the authoritative temporal filter: per
`(ItemId, valid_from, valid_to)`, with NULL bounds meaning "no boundary."
A code without a validity row is always valid throughout the variable's
lifetime (per SCB correspondence).

The DB stores year-projected value sets, not the raw union: each
`variable_state` carries the codes that were actually valid in its era
(the projection is computed per cvid at build time, then attached to the
coalesced state). Projection is intentionally year-precision, not exact-date. SCB's
metadata is annual; sub-year boundaries (e.g. `valid_from=1995-09-01`)
are administrative artifacts that year overlap absorbs losslessly. The
trade-off — losing sub-year query precision — is paid for by removing
the temporal axis from the schema entirely. There is no `get values
--valid-at` flag and no historical-union opt-in: the union is discarded
by design.

The projection rule and its build-time mechanics are documented in
[../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md) § "Year
projection".

The result is content-addressed and deduplicated: identical year-projected
sets share one `value_set` row (`member_hash` = sha256 of sorted
`(code, label)` pairs); each `variable_state` links to its set via
`variable_state.value_set_id`. NULL `value_set_id` means the state had
no codes (every union pair excluded by projection, or only sentinel rows
in the source — see [../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md)
§ "Vardemängder sentinel filtering").

## Classifications

Named code systems (SUN2000, SSYK2012, SNI2007, LKF, ...) are first-class
entities. Each `classification` row carries metadata (publisher, version,
validity range, supersedes link, canonical URL) and a cached `code_count`.
The `classification_code` junction holds the deduplicated union of value
codes that belong to the classification, with an optional `level` integer
for prefix-hierarchy filtering (length of all-digit codes; NULL for
non-numeric codes like ICD letters).

The FK lives on `variable_state` (per-era), not on `variable`. SCB's data
model already places the classification label (`value_set_version_label`)
per era, and many headline variables genuinely span multiple
classifications across their lifetime — e.g. `Utbildningsnivå` (var_id 66)
uses SUN 2000 codes through 2018 and SUN 2020 codes from 2019 onwards;
`SSYK` and `SNI` show the same generational drift. Linking at the state
level keeps each code system distinct (SUN 2000 codes never bleed into
SUN 2020), isolates split siblings (each sibling's states classify
independently — the A2.7 fix), and lets variable-level helpers aggregate
when needed.

The `classification_id` column is populated at build time from a
maintainer-curated TOML seed at `reg_meta_build/classifications.toml`
(exact match against `value_set_version_label`, no fuzzy inference). The seed
schema, build-time invariants, and validation rules live in
[../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md) §
"Classification seed".

### Canonical vs observed codes

`classification_code.is_valid` distinguishes published canonical codes
from codes that merely show up in the data. SCB's metadata exports
contain plenty of noise (`*`, `***`, `0000`, `[BLANK]`, stray prefix
levels) that has no place in an authoritative code list, but is also
useful to keep around so a researcher seeing one of those values in a
register can look it up.

Canonical codes come from per-classification CSVs ingested by the build
(see [../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md) §
"Canonical code CSVs"). A classification with a CSV gets
`classification_code.is_valid` populated as 1 (canonical) or 0
(observed-only) and `classification.valid_code_count` cached for that
canonical count. Without a CSV, every `classification_code` row carries
`is_valid=NULL` (validity unknown).

The CLI exposes this via `get classification --codes --only-valid` and
includes `is_valid` per code in JSON output (omitted when NULL).

Hierarchy is intentionally not encoded as `parent_code_id`. The `level`
column captures the most useful filter ("top-level only"); deeper
parent/child queries fall back to prefix matching on `value_code.code`. Code
sets without prefix hierarchy (ICD-10, ATC) keep `level = NULL` and use
their own conventions.

## Storage optimization

IDs stored as INTEGER (not TEXT). Tables with composite integer-only PKs
use WITHOUT ROWID. Value codes are deduplicated into `value_code` (with
`UNIQUE(code, label)`); `variable_state` → code membership is a
content-addressed `value_set` / `value_set_member` pair, where each
distinct year-projected code list is stored once and shared by every
state that observes it. SCB's validity windows are applied at build time
(see "Value sets are year-projected"), eliminating the historical-union
junction and the per-item validity tables entirely. A pre-aggregated
`code_variable_map` replaces large secondary indexes for value search
queries. The original 13 GB raw DB shrank to ~1.6 GB through
deduplication and integer keys; year-projection is expected to take it
further still.

## Documentation layer

Register documentation (parsed from SCB PDFs) is curated as
Obsidian-compatible markdown files under `reg_meta_build/docs/`,
source-of-truth for maintainers, and indexed into a separate FTS5
database (`reg_meta_docs.db`) with its own `DOC_SCHEMA_VERSION`. Docs are
keyed to register and variable names, not numeric IDs, so doc updates
and main-DB updates are independent.

End users never see the markdown files. The doc DB is distributed as a
GitHub Release asset (`reg_meta_docs.db.zst`) parallel to the main DB asset,
installed into the same cache dir (`$XDG_DATA_HOME/reg_meta/`), and fetched
by `reg-meta update` alongside the main DB. Query commands (`search`,
`get`, `resolve`, `docs/*`) refuse to run without the doc DB — on first
use the CLI offers to download both artifacts.

`reg-meta-build build-docs` is a maintainer-only command that rebuilds
the doc DB from a repo checkout of `reg_meta_build/docs/` before upload.
Runtime never reads markdown — `repo_docs_dir()` in
`reg_meta_build.doc_db` is only consulted by `build-docs` when run from a
repo checkout, and is absent in installed wheels of `reg_meta`.

See [../reg_meta_build/docs/SCHEMA.md](../reg_meta_build/docs/SCHEMA.md)
for the markdown file format.

## What's not in the catalog

The universal model is deliberately lean. The catalog answers "what
variables exist and what shape they have"; two siblings answer the
rest, and the boundary is a design decision worth stating:

- **The doc DB** answers "how to understand them" — free-form prose and
  narrative metadata at variant level: methodology (SCB
  mätinformation), quality narratives (SOS quality sheets), conceptual
  time-series breaks, long-form descriptions, legal text. When content
  drifts over a register's life, the doc uses chronological Markdown
  sections; the catalog does not try to model prose chronology.
- **The provenance DB** — a maintainer-only sibling SQLite artifact,
  not shipped to consumers — holds build artifacts: approval dates,
  workbook delivery metadata, source checksums, build manifests, and
  raw provider-side IDs not reused as universal IDs. Its build
  rationale lives in
  [../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md).

Localization is deferred (v2+): the catalog carries one canonical text
per field (the provider's native language), and the build drops SOS
DCAT-AP `*_en` variants for now.

**Structural sensitivity flags stay in the catalog** as universal
`variable` columns (`is_sensitive`, `is_identifier`) — they are
MONA-critical, apply to every variable regardless of provider, and are
inherently shared metadata (sensitivity is a property of the variable,
not of how a variant delivers it).

**`is_identifier` downstream semantics.** A variable with
`is_identifier=true` will be pseudonymized at delivery — SCB prefixes
the column header with `LopNr_` (or a project-specific prefix). The
flag is **broad**: it covers not just the subject identifier
(`PersonNr`) but every related identity column (`PersonNrMor`,
`PersonNrFar`, `PersonNrSambo`, ...). It is distinct from the narrower
"which identifier is the *subject* of this variant?", which
`variant.panel_entity_key` answers. Downstream consumers (SPA
authoring's default `display_name`, the validator's info-level
pseudonymization-prefix check, the MONA bundle's PII scanner) key off
`is_identifier`; only panel-default inheritance keys off
`panel_entity_key`.

## Versioning and compatibility

Four independent version numbers:

| Version | Location | Purpose |
|---------|----------|---------|
| Package version (`__version__`) | `__init__.py`, `pyproject.toml` | Python package / CLI release |
| Main schema version (`SCHEMA_VERSION`) | `db.py` | Main-DB schema compatibility |
| Doc schema version (`DOC_SCHEMA_VERSION`) | `doc_db.py` | Doc-DB schema compatibility |
| Contract version (`CONTRACT_VERSION`) | `cli_common.py` | CLI output envelope format |

**Schema version** uses semver. `open_db` compares the `import_manifest`'s
`schema_version` to the code's `SCHEMA_VERSION`: the major components must
match and the DB's minor must be `>=` the code's minor. A mismatch raises
`schema_incompatible` (exit 10) and directs the user to re-download the
database. Patch differences are ignored.

Bumping rules:

- **Major bump** on breaking changes (renamed/removed tables or columns,
  changed column semantics that consumers must adapt to).
- **Minor bump** in either of these cases:
  1. Code starts reading a new column/table added in the build. This
     forces old DBs (that lack it) to be rejected cleanly at `open_db`
     instead of failing later with a SQL error.
  2. Build-time content semantics change in a way that should invalidate
     prior DBs even though no schema shape changed — e.g. dropping
     polluting rows from `value_code`, populating columns with NULL where
     they used to carry placeholder strings. Old DBs would silently serve
     pre-cleanup data; the bump forces a rebuild on the next `reg_meta
     update`.

Either bump requires rebuilding and re-uploading the DB asset before the
package release goes live — see `.claude/skills/release/SKILL.md`. The
`TestSchemaCompat` tests in `reg_meta_build/tests/test_build_db.py`
verify the guard.

### Release tags and distribution

The monorepo uses **per-package release tags**: `reg_meta/v0.5.0`,
`reg_meta_build/v0.1.0`, `mock-data-wizard/v0.4.0`, etc.  Each tag
corresponds to a GitHub release scoped to that package.

| Channel | Trigger | What it distributes |
|---------|---------|---------------------|
| PyPI | `publish_reg_meta.yml` on `reg_meta/v*` release | Python package (wheel + sdist) |
| PyPI | `publish_reg_meta_build.yml` on `reg_meta_build/v*` release | Builder package (wheel + sdist) |
| GitHub Release asset | Manual upload to the `reg_meta/v*` release | Pre-built main DB (`reg_meta.db.zst`) |
| GitHub Release asset | Manual upload to the `reg_meta/v*` release | Pre-built doc DB (`reg_meta_docs.db.zst`) |

Both DB assets are **optional** per release. A package release only needs a
new main DB when `SCHEMA_VERSION` changes, and only needs a new doc DB when
`DOC_SCHEMA_VERSION` changes or `reg_meta_build/docs/` content changes.
`resolve_latest_release()` walks recent releases backwards looking for each
asset independently, so a doc-less or DB-less package release does not
orphan older assets. The publish workflow's smoke step exercises
`reg-meta update --force` before allowing PyPI publish, so a release that
breaks the walker (e.g. incompatible assets, or no resolvable asset at all)
fails CI instead of shipping.

The wheel contains Python source only. The markdown under
`reg_meta_build/docs/` is maintainer source-of-truth and is **not** bundled
— end users receive the built doc DB via `reg-meta update`.

Legacy bare `v*` tags (pre-0.6.0) are still recognized during the transition
but new releases must use the `reg_meta/v*` prefix.

**Update command**: `reg-meta update` is the single command that brings
everything current — it runs `uv tool upgrade reg-meta` for the package and
walks releases to find the latest main-DB and doc-DB assets. Already-current
assets are skipped (tracked via `.db_source` and `.docs_source` in the cache
dir). A background version checker runs once per week (cached in
`~/.local/share/reg_meta/.update_check`) and prints a hint on interactive
runs when a newer release exists.

**Auto-download on first use**: query commands (`search`, `get`, `resolve`,
`docs/*`) prompt to download whichever artifacts are missing when invoked
interactively, so users don't need to know about `reg-meta update` on first
install. Non-interactive invocations fail with structured errors
(`db_not_found`, `doc_db_not_found`) rather than silently skipping.

### Package version format

Package versions follow `X.Y.Z` with two optional pre-release suffixes:

- `X.Y.Z` — final release
- `X.Y.ZaN` — alpha (e.g. `0.5.0a1`)
- `X.Y.Z.devN` — development build (e.g. `0.5.0.dev3`)

No other suffixes (beta, rc, post, epoch) are used. The update checker
relies on this format for version comparison.

## Exit codes

| Code | Meaning |
|------|---------|
| 0    | Success |
| 2    | Usage/argument error |
| 10   | Configuration error (missing DB, bad encoding) |
| 16   | Not found |
| 17   | No match with `--require-match` |
| 25   | Network error (`reg-meta update`) |
| 30   | Unexpected internal error |

## Determinism

- Stable ordering for repeated runs against the same database
- Stable JSON key ordering
- Deterministic paging (offset, limit)

## Security

- Metadata only — no microdata
- No credentials read or stored
- No outbound network requests (except `reg-meta update` and the weekly version check)

## Glossary and Swedish↔English crosswalk

Durable reference for the universal vocabulary. The shipped-entity
glossary is in [STRUCTURE.md](STRUCTURE.md) "Working Interpretation";
this captures the cross-provider term meanings and the column-rename
pass that turned SCB's Swedish source columns into universal English.

| Term | Meaning |
|---|---|
| variable | The addressable variable — provider's "define once" identity, the FQID target. Synthetic `variable_id` PK; identity `(provider, register, slug)`. Has 1..N states across variants and time. |
| variant (coordinate) | A `register_variant` row (SCB `registervariant`, SOS `deldatamängd`): a delivery coordinate, not an identity level. Carried on `variable_state` and on `project_data` Sources. Browsed under its register; **not an FQID kind**. |
| variable state | A `variable_state` row: per-delivery shape, carrying a variant coordinate, validity range, type/length/value-set/version-label. The canonical unit of resolution at a `(variant, period)`. |
| binding | A 3-segment FQID referencing a variable. Resolves to a `ResolvedVariable` (all states) or `list[VariableState]` (with period context). |
| variable slug | `variable.slug`: the register-unique, immutable FQID leaf. Triage splits get distinct slugs; grain/vintage folds keep one slug. |
| same_as | Symmetric cross-register / cross-provider equivalence between variables. Variable grain; curated only, no auto-derive. |
| related_to | Symmetric split-sibling edge (distinct variables from one source key). Variable grain. |
| classification | A named versioned vocabulary (SUN2020, ICD10). Provider-independent; addressed via `class/<slug>` (vintage in slug). |
| value_set | A code list on a `variable_state`. Content-addressed (`member_hash`) for dedup; optional FK to `classification`. Never exposed via FQID. |
| value_set_version_label | On `variable_state`: the discriminator that lets multiple value-set versions co-exist as overlapping states (folded crosswalk vintages / LKF multi-vintage). `NOT NULL DEFAULT ''`. |

**Universal English ↔ SCB Swedish.** Column names are universal
English; column **values** stay provider-native verbatim. The validator
emits errors against strings; resolution turns strings back into
entities.

| SCB Swedish | Universal English | Lives on |
|---|---|---|
| registernamn | name | register |
| registersyfte | purpose | register |
| registervariantnamn | name | register_variant |
| registervariantbeskrivning | description | register_variant |
| variabelnamn | name | variable |
| variabeldefinition | definition | variable |
| variabelbeskrivning | description | variable |
| variabeloperationell_definition | (merged into `description` when distinct) | variable |
| variabelregister_kalla | source_label | variable |
| mattenhet | measurement_unit | variable (NULL when source was "Okänd") |
| datatyp | data_type | variable_state |
| datalangd | data_length | variable_state (TEXT — may carry precision/scale, e.g. `8,2`) |
| vardemangdsversion | value_set_version_label | variable_state |
| värdekod | code | value_code |
| värdebenämning | label | value_code |
| kolumnnamn | delivery_column_name | variable_alias / variable_state |
| kanslig_variabel(_ibland) | is_sensitive | variable (both source values fold into one flag) |
| identitetsvariabel | is_identifier | variable |
| version_forsta / version_sista | valid_from / valid_to | variable_state (mapped to ISO 8601 at ingest) |

`registerrubrik` / `registervariantrubrik` are dropped (redundant with
`name`); `variabelreferenstid`, `variabelhamtadfran`,
`variabelextern_kommentar` are dropped or moved to docs; SCB
`registerversion_*` (mätinformation, approval dates) go to docs /
provenance.

**Population and object type are build-only.** SCB's `populationnamn` /
`objekttypnamn` etc. land in scratch `population` / `object_type`
tables that the build folds into `variable_state` validity windows and
then **drops before ship** (alongside `register_version`,
`variable_instance`). They are **not catalog entities** and have no FQID
slot — do not query for them in the shipped DB.

## Explored and ruled out

- **Direct API integration** against `mikrometadata.scb.se` — no stable
  public API. Session-bound WebSocket with no documented contract.
- **Browser automation** — fragile, unrepeatable. Manual CSV export is
  more reliable.
- **Query caching / user adaptation database** — deferred. Not needed yet.
