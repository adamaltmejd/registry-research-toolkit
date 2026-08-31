# Design: reg_meta

Design rationale and constraints for the query layer. For usage, see `reg-meta --help`.
The object model lives below ("Two-level variable model"); for the per-provider source
shapes it collapses (the SCB input-file layout, the SOS workbook layout) and the rest of
the build-pipeline rationale (CSV import, sentinel filtering, year projection,
classification seeding, doc-DB build), see
[../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md). For the cross-package
topology, dependency graph, and version policy, see the root `ARCHITECTURE.md`.

## reg_meta as the substrate

reg_meta is the identifier and object-model substrate every downstream artifact
references — the `project_data.json` schema, the webapp's `/api/catalog/*` endpoints,
the generation kit consumed by `reg_mockdata`. The contract between them is only as
stable as reg_meta's identifier scheme, so the model is built to outlast any one
provider's vocabulary.

The design rule that drives everything below: **a provider-neutral object model**.
Universal column names (`name`, `description`, `data_type`, ...) carry provider-native
string values verbatim — the SCB `registernamn` for LISA stays under `register.name`
exactly as published; order generation reads these strings because they are what the
provider's intake form expects. The universal schema carries **no provider-specific
tables** (no `scb_*`, no `sos_*`): provider variation is captured purely as fill-rate on
the universal columns (some providers populate fewer fields). Provider-specific parsing
lives in `reg_meta_build`; what query commands see is the unified shape. This keeps one
mental model for consumers across every provider, and keeps reg_meta importable from any
context (Jupyter, scripts, future tooling) with no provider conditionals.

The earlier (v0.11) scheme worked but baked SCB's CSV vocabulary and yearly publication
cadence into the universal model; adding a second provider (Socialstyrelsen) and a third
(Försäkringskassan) made the cracks visible. The current substrate is the result of
removing them: a **two-level variable model** (`variable` → `variable_state`), a
**3-segment binding FQID grammar** (`provider/register/slug`) with no variant or period
slot, **slug-anchored edge tables** at variable grain, and a **build-time triage** pass
that normalizes provider-specific oddities into the universal shape (the triage
mechanics live in [../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md)). Prose and
narrative metadata go to the doc DB; maintainer-only build artifacts go to a sibling
provenance DB (see "What's not in the catalog").

## Agent-first design

The primary consumers are LLM agent skills and webapp features. Human terminal use is
supported but secondary. This drives several choices:

- Three output formats: table (default), list, and JSON for machine consumption
- All output follows a stable envelope contract (version, timing, request echo)
- Errors are structured with codes, not just messages
- Exit codes are meaningful (see below)
- Core query functions are importable as a Python library, not just CLI

## SQLite backend

All metadata lives in a single SQLite file (\~320 MB). Chosen because:

- Zero-dependency deployment (Python stdlib)
- Single-file distribution via GitHub Releases + zstd compression
- FTS5 built in
- Read performance is excellent for this workload

The database is read-only from the perspective of query commands.
`reg-meta-build build-db` replaces it entirely (not incremental).

## Data providers

At the query layer reg_meta is provider-agnostic: one metadata DB, one docs DB, one CLI.
Users searching or resolving variables need not know which agency published a given
register — the provider is a queryable attribute, not a separate code path. The
provider-neutral object model that makes this possible is "reg_meta as the substrate"
above; the provider-specific parsing that feeds it lives in
[reg_meta_build](../reg_meta_build/DESIGN.md) (the IR + adapter layer).

## FTS5 configuration

Four content-synced FTS5 indexes power search:

- **`register_fts`** — indexes register `name`, `purpose`.
- **`variable_fts`** — indexes variable `name`, `definition`, `description`,
  `operational_definition`, and a `delivery_column_names` aggregate derived from
  `variable_alias.delivery_column_name` (#735/#936). Uses `unicode61` tokenizer for
  correct Swedish character handling and for SCB column-code tokens such as
  `fedunsatreason_1` matching `fedunsatreason`. The FTS table's external content is the
  `variable_fts_content` view, not `variable` directly, so delivery-column search stays
  derived from the normalized alias table. Variable search rows surface the matched
  delivery aliases for alias hits, falling back to display aliases for non-alias hits.
  FQID slugs are **not** indexed here.
- **`classification_fts`** — indexes classification `short_name`, `name`, `name_en`,
  `description`. Searched via `search(..., type="classification")` (#350), the catalog
  discovery surface. Catalog-scoped: a `--register` scope excludes it. **Code-aware
  surfacing** (#393 item 5): a **code-shaped** query (digit + length ≥ 3, e.g. "C12",
  "F32") ALSO surfaces the classifications that CONTAIN a matching code — exact OR
  prefix on `value_code.code` joined through `classification_code` — so "find the
  classification for this code" works even with no NAME match. This arm uses the RAW
  query (not the FTS index), is catalog-scoped (excluded under `--register`, like the
  name arm), dedupes against the name-FTS hits (a both-ways match is emitted once, as
  its name hit), and is ranked AFTER all name hits (a positive `fts_rank` base vs the
  name arm's negative bm25; exact-containing classifications first within the block).
  The `classification_code` JOIN inherently excludes context-less codes, so no separate
  owner filter is needed (unlike the value arm's direct `value_code` lookup, #478).
- **`value_code_fts`** (#352) — indexes value `label` ONLY (codes are matched
  separately, see below). Searched via `search(..., field="value", type="value")`, which
  emits `type: "code"` rows. \~55% of codes are bare numbers, so labels are the primary
  search surface. A curated **stoplist** of junk labels (`NULL`, `Ja`/`Nej`,
  `Uppgift saknas`, the `Okänt*`/`Okänd*`/`Felaktig*` SCB sentinel-prefix families, …)
  is excluded at index-population time (build-side `_VALUE_CODE_STOPLIST_*`). Alongside
  the stoplist, **ownerless codes** — no owning variable (`mapping_count = 0`) AND not
  present in `classification_code` (#478) — are also excluded: they are
  year-projection-dangling orphans with no owner to annotate, and indexing them would
  surface context-less hits in unscoped value search. Classification-owned codes (no
  variable mapping but linked via `classification_code`) remain indexed, since
  classification search is name-only. All exclusions are hidden from SEARCH only — the
  leaf `value_code` / `value_set` tables keep every row. Each code hit pivots through
  `code_variable_map` → variable (and `classification_code` → classification) and is
  annotated with a bounded representative slice of its owners plus the full counts — the
  actionable target is the owning variable/classification, not the bare (code, label)
  pair. **Ranking** is bm25 relevance with a `mapping_count` (precomputed variable count
  per pair) DOWNWEIGHT, so a generic enum label shared by many variables ranks below a
  rare, discriminative one. A **code-shaped** query (digit
  + length ≥ 3, e.g. "F32", "0180") ALSO does an exact/prefix match on `value_code.code`
    (via `idx_value_code_code`), merged + deduped with the label-FTS hits and seeded
    above them (an exact code match is the strongest signal); plain-text queries do
    label FTS only. The ownerless-drop applies to BOTH paths: the label-FTS index
    (build-side filter) AND this code-shaped direct `value_code` lookup, which carries
    the same owner predicate in `_search_values_fts` (#478) — without it a code-shaped
    exact/prefix query would bypass the index and leak the context-less hit. The
    code-exact rank floor means that in the flat `type="all"` CLI path, code-exact hits
    intentionally precede other result types for a code-shaped query (the user typed a
    code); the webapp calls `search()` per type, so its typed groups are unaffected. The
    value arm applies owner scope inside SQL and returns only a bounded ranked prefix;
    owner annotation is set-based and limited to the displayed page. NB:
    `value_code_fts` is external-content, so `COUNT(*)`/`SELECT col` read the CONTENT
    table (value_code) — the honest indexed-row count is the `_docsize` shadow table.

`search` takes a RAW user query and builds the FTS5 MATCH expression internally
(`_fts_match_query`): each whitespace token becomes a quoted prefix term (`"tok"*`),
which (1) neutralizes FTS5 operators so stray syntax can't raise, and (2) prefix-matches
("ink" → "inkomst"). `unicode61` folds diacritics on BOTH the index and the query side
(å→a), so callers pass the query through unfolded. The LIKE-based fields
(datacolumn/varname/value, and concept-group label folding) bind escaped LIKE patterns
so `%` and `_` in the user query match literally rather than as wildcards. Each
register/variable/classification result row carries its navigable `fqid`.

The docs index (`doc_queries.doc_search`, a separate `reg_meta_docs.db` FTS index) uses
the same `_fts_match_query` builder, so a raw doc query is operator-safe and
prefix-matched too.

## Register lookup strategy

All commands accepting a register argument use a three-step resolution:

1. Exact ID match
2. Case-insensitive exact name match
3. Case-insensitive substring match

This allows `34`, `LISA`, and `utbildning` to all work.

## Resolve: exact match only

`resolve` performs exact alias lookup against `variable_alias.delivery_column_name`. No
FTS fallback, no confidence scoring. Status is `matched` or `no_match`. This is
intentional — resolve is for mapping known column headers, not discovery.

## Composite registers and source tracking

Registers like LISA, FRIDA, LINDA, and STATIV are composites — most of their variables
originate in source registers (RTB, RAMS, etc.). The `variable` table tracks this via
`source_register_id` (FK to `register`) and `source_label` (display abbreviation or raw
text) when the source attribution is stable at variable grain. Source codes or raw
attribution text that varies by edition stays on `variable_state.source_register_text`.
Unresolved stable sources remain as raw text for human review and surface in
`get schema` (source column) and `get lineage` (consumer/source classification). The
resolution rules used during build are documented in
[../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md) § "Source-register
resolution".

## Two-level variable model

What SCB and SOS each publish as a "variable" is split into exactly two levels, because
two distinct facts are entangled there:

- **`variable`** — the **addressable variable**, the thing an FQID names: the provider's
  "define once" identity. Holds the register-unique slug (the FQID leaf) and the
  cross-era constants (`name`, `definition`, `description`, `measurement_unit`,
  `is_sensitive`, `is_identifier`, source attribution).
- **`variable_state`** — the **per-delivery shape**, a child of `variable`. A variable
  has 1..N states; each carries a **variant coordinate** and a period range, plus the
  data type, length, value set, and version label. The **value set anchors state
  identity**: SCB's low-trust per-delivery `data_type`/`data_length` no longer split a
  state when a value set is present (a state can span several deliveries whose only
  difference was a type-string wobble; the displayed type is then the latest era's) —
  see reg_meta_build/DESIGN.md § "State-identity rule (#526)".

The SCB source delivery this collapses (the CVID grain, the input-file mapping) is
documented in [../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md) § "Source
delivery shapes"; this section is the cross-provider rationale. The normative DDL lives
in `reg_meta_build/db.py` — not copied here.

**Why the variant is a coordinate, not an identity level.** A *variant* (SCB
`registervariant`, SOS `deldatamängd`) is a **delivery coordinate**. "Kön in LISA" is
one variable however many variants deliver it; the same variable delivered in variant A
vs B, or year X vs Y, is a different *state*, not a different identity. So the variable
is the FQID target, and the variant and period are coordinates that select among its
states. This is the load-bearing design decision — the empirical basis is in the next
section.

**Variable formation is adapter-defined.** What constitutes one variable depends on the
provider's source structure, but the resulting `variable` row is uniform:

- **SCB:** variable = `(register_id, var_id)` — `var_id` is the define-once unit, reused
  verbatim across the variants that deliver it.
- **SOS:** variable = `(register, variable_name)`, formed by merging same-named
  variables across deldatamängder within a register (sound because the structured
  `Kodlista_*` sheets are register-level and shared across deldatamängder). Genuine
  name-reuse collisions split into distinct variables.

**Keys (DECISION POINT 1).** The natural key is `(register_id, slug)` (register-unique,
the binding FQID). It stays unique even after a triage *split* puts several variables
under one source key, because siblings get distinct slugs. `provider_key` (SCB
`str(var_id)`; SOS the merged name) is therefore a **NON-unique join hint, not a key** —
the build join "source row → variable" refines it by the triage discriminator when a
split exists, 1:1 otherwise. A **synthetic `variable_id` PK** backs all this so
`variable_state`'s FK stays single-column and the edge tables stay stable as the natural
key's provider-specific shape varies. (The triage fold/split mechanics live in
[../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md).)

**`classification_id` and `source_label` placement.** The per-era classification family
lives on `variable_state.classification_id` (an era can change code system mid-life —
see "Classifications"), while the human-readable source attribution lives on
`variable.source_label` (cross-era constant). `variable_state` carries `state_id`,
`variable_id`, `register_variant_id`, `valid_from`/`valid_to`, `data_type`,
`data_length`, `delivery_column_name`, `value_set_id`, `value_set_version_label`, and
`classification_id`.

**Variant-less registers (`_default`).** Socialstyrelsen LSS, BU, SOL ship variables
without a deldatamängd sheet. Adapters synthesise a single `_default` variant row at
build time (a real row, not a resolve-time fiction), and every state references it as
its `register_variant_id`. Because the variant is not an FQID segment, `_default` never
appears in a binding FQID — it is a browsing/state coordinate, not a path segment.

## Why two levels, not three (the variant-identity investigation)

The decision to make the variant a coordinate rather than an identity level is
empirical, calibrated against the production SCB `reg_meta.db` (schema v0.11.x at the
2026-05-22 design lock) and the 13 Socialstyrelsen workbooks current then. The numbers
are recorded here so a future contributor questioning the shape has the anchor — re-run
them if the data drifts.

Earlier drafts treated the variant as part of variable identity (a 4-segment FQID
`provider/register/variant/variable`), recovering `var_id` reuse across variants by
auto-emitting `(N choose 2)` `variable_same_as` edges. The investigation refuted that:

  | Question                                                              | Finding                                                                                                                                              | Implication                                                                                                             |
  | --------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
  | How many SCB `(register, var)` pairs appear in more than one variant? | **78.9%** appear in exactly one variant.                                                                                                             | Variant identity is degenerate for 4 in 5 variables.                                                                    |
  | When a variable spans variants in the same year, what differs?        | Rolled up to the variable grain, only **4.3%** of pairs show any same-year cross-variant divergence — and it is overwhelmingly column-name or grain. | The divergence is what triage resolves (fold or split), independent of variant. The variant is never the discriminator. |
  | Variant or period — which is the stronger differentiator?             | **43%** of multi-period `(variable × variant)` cells drift across periods, vs 4.3% across same-year variants.                                        | **Period** is the real differentiation axis, and it lives in `variable_state`, not identity.                            |
  | SOS: do code sets differ by deldatamängd?                             | **Zero** variables have a deldatamängd-specific code list.                                                                                           | The deldatamängd carries no code/identity differentiation.                                                              |
  | SOS: do codes vary by period?                                         | **35%** of code rows carry `tidsperiod` ranges.                                                                                                      | SOS codes vary by period, not variant — again period is the axis.                                                       |

The 4.3% (pairs grain) and 43% (multi-period-cells grain) sit at different grains **on
purpose** — the point is the contrast, not a like-for-like ratio. The full denominators
(42,768 `(register, var)` pairs; 55,309 same-year multi-variant cells; the
multi-period-cell subset) are recorded in git history if a re-run needs them.

**Conclusion.** In both providers the variant is a delivery coordinate and period is the
differentiation axis. Two levels suffice: an addressable `variable` and per-delivery
`variable_state` rows each carrying a variant coordinate and a period range. Collapsing
the three-level draft's intermediate variant-scoped row removes the `(N choose 2)`
`variable_same_as` explosion (within-register identity is the variable itself now) and
shortens the binding FQID from 4 segments to 3.

**Two corroborating signals worth keeping.** (1) State-on-variable is real signal, not
bookkeeping: 43% of multi-version triples carry drift across editions, and
coalescing-by-shape shrank \~515K instance rows to \~104K states (≈5×). (2) **Free-text
fields are unreliable for identity decisions** — an early SOS pass comparing free-text
`Värdemängd` descriptions across deldatamängder spuriously suggested \~50% divergence;
the structured `kodlistor` refuted it. Anchor identity decisions on the structured code
data, never the prose.

## FQID grammar

Every reg_meta entity has a Fully Qualified Identifier — a stable, `/`-separated string
with strict positional grammar. The kind is determined entirely by segment count plus
the `class/` discriminator prefix; no out-of-band lookup is needed. The parser/emitter
is `fqid.py`.

  | Segments            | Form                           | Kind                            |
  | ------------------- | ------------------------------ | ------------------------------- |
  | 1                   | `<provider>`                   | provider                        |
  | 2                   | `<provider>/<register>`        | register                        |
  | 3                   | `<provider>/<register>/<slug>` | variable binding (the variable) |
  | 2, leading `class/` | `class/<slug>`                 | classification                  |

```text
scb                              provider
scb/lisa                         register
scb/lisa/kon                     variable binding (names the variable)
sos/lss/insatstyp                variable binding (variant-less register)
class/sun2020                    classification (vintage baked into the slug)
class/icd10                      classification
```

**The FQID names the variable; the binding is 3-segment.** The binding
`provider/register/slug` addresses a `variable` directly. The variant and period are
delivery coordinates that select among its states — neither is a segment.

**No variant slot (DECISION POINT 2).** Dropping the variant makes the binding
3-segment, which would otherwise collide with the old 3-segment *variant* address
(`scb/lisa/individer-15plus`). We resolve this by removing the variant FQID kind
entirely: a variant is no longer addressed by a slash-path. You **browse** a register's
variants as a sub-resource (the catalog UI / `/api/catalog` lists them) and **address**
a variable directly, so `scb/lisa/X` is unambiguously a variable. The `register_variant`
table still exists (panel keys, browsing metadata) and the variant is still a coordinate
on `variable_state` and in `project_data` Sources — you just never reach a variable
*through* a variant path.

**No period slot.** The same variable can have different definitions in different years;
that drift is `variable_state` rows with explicit validity ranges, not per-year FQIDs.
Year-specific resolution is supplied via `resolve_at(fqid, period)` or `Source.period`.
Time is data context, not identity.

**Classification vintage is in the slug.** SUN2020 is `class/sun2020`, not
`class/sun?version=2020`; ICD-10 and ICD-11 are distinct classifications with distinct
slugs. Each vintage is its own normative document, which is how researchers think about
them, and the slug alone is the global uniqueness key (no separate version segment).

**No `@version` binding suffix.** Co-delivered parallel codings (a classification
vintage during a crosswalk era — näringsgren in both SNI92 and SNI2007 in a transition
year) are **not** addressed by an `@<value-set-version>` FQID suffix. A binding leaf is
always a bare 3-segment slug. Co-delivered codings live as overlapping
`value_set_version_label`-discriminated states of one variable, and the caller selects
one with `resolve_at(..., value_set_version=...)` — not by an FQID pin. (`fqid.py` has
no `@` handling.) The binding-side "representation" chooser — which delivery column a
co-delivery maps to — is a `project_data`/`reg_schema` concern, not part of the reg_meta
identifier.

**Slug grammar.** Every slug matches `^[a-z](?:[a-z0-9]|-[a-z0-9])*$` (lowercase ASCII
kebab-case: starts with a letter, ends with a letter or digit, hyphens only singly
between alphanumerics; single-character slugs match `^[a-z]$`). The regex is anchored
with `\Z`, not `$`, so a trailing newline can't sneak a slug like `kon\n` past the
validator (a footgun the webapp path-guard and build-time validation both rely on).
Period-shaped strings are rejected as slugs everywhere (legibility) — the classification
vintage-in-slug folds the only former exception away.

**Reserved slugs.** `_default` and `class` are reserved everywhere (checked inline in
`validate_slug`). `class` keeps the leading-`class/` discriminator unambiguous.
`_default` is the variant-less coordinate; it is the **one literal exception** to the
slug regex (it starts with `_`), so validators short-circuit on the literal string
before applying the regex. Build rejects any other slug entry hitting these.

**HTTP-suffix slug rejection.** The webapp's catalog router declares sub-resource routes
that use path segments after a binding; a slug equal to one of these would shadow a live
route and make the entity unreachable. Two constants in `fqid.py` encode the
reservation:

- `RESERVED_HTTP_SUFFIX_SLUGS` —
  `{states, predecessors, successors, lineage, lineage_warnings, dimensions, graph}`.
  The binding-suffix routes (`/catalog/{fqid:path}/<suffix>`) greedy-match any FQID
  path, so each collides with a 3-segment variable leaf, a 2-segment register, and a
  classification. All three slots are therefore reserved. (The former `related` suffix
  route was retired in #800 when `variable_related_to` was dropped.)
- `RESERVED_VARIANTS_SLUG` — `"variants"`. The `/catalog/{provider}/{register}/variants`
  register sub-resource shadows only a 3-segment variable leaf, so `variants` is
  reserved in the **variable slot only**. The register_variant slot (a `?variant=` query
  value, never a path segment) carries no reservation.
- `RESERVED_GROUP_SLUG` — `"group"`. The `/catalog/group/{provider}/{register}/{key}`
  concept-group **subject** route (#617) is the first route to place a literal token in
  a **non-leading** position where `{provider}` then follows, so it is reserved in the
  **provider slot only**. A provider named `group` would let a binding-suffix URL
  `/catalog/group/<register>/<variable>/states` (5 segments) be captured by that
  earlier-declared 5-segment group route → a wrong 404. This corrects the earlier
  assumption that the provider slot (always a leading segment) could never be a
  colliding URL position; it can, once a route prefixes it. A register / variable /
  classification named `group` is still fine — only the provider position lands at that
  literal.

`validate_slug` enforces both; `derive_variable_slug` delegates to it, so a column
literally named e.g. "States" or "Variants" degrades to `None` (triggering the
name/last-resort fallback) rather than minting a shadow slug. The reserved set is pinned
to the live catalog route list by a drift guard in
`reg_webapp/backend/tests/test_boot.py`.

**Open — curator review cadence on rename.** Slugs are derived from the latest
delivery-column alias. If a provider renames a column between editions and the curator
hasn't yet added a `same_as` link, the auto-rule produces a new slug for the later
editions while earlier ones keep the old slug. This is correct in principle (rename =
new variable by default), but the operational rhythm — how often curators review
newly-shipped renames — is undecided. The slug-derivation and curation mechanics live in
[../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md).

**FQID property tests.** The grammar invariants are property-tested: round-trip (parse →
emit → parse equals identity), segment-count discrimination (1/2/3 + the `class/`
prefix), reserved-slug rejection, and `same_as` traversal termination (cycle detection).
Reserved-slug coverage is grammar-wide under the 3-segment grammar: `_default` and
`class` are reserved **everywhere** (there is no variant slot to exempt `_default` in —
a 4-segment FQID does not parse at all). A test asserting `_default` is accepted in some
slot, or that a 4-segment string like `sos/lss/_default/insatstyp` parses, is testing
the obsolete variant-slot grammar and is wrong.

## Catalog API surface (§6.0)

`Catalog` (`catalog.py`) is the in-process FQID→entity API the webapp's `/api/catalog/*`
endpoints wrap. `resolve(fqid)` is polymorphic over FQID kind; the provider / register /
classification arms each return their dedicated `Resolved*` row (variant and version are
**not** FQID kinds — variant is a register sub-resource coordinate, period a delivery
axis). The **binding** arm is longitudinal: a binding FQID resolves to a
`ResolvedVariable` — the addressable variable's shared metadata + its full
`variable_state` history (each state tagged with its variant coordinate) + the
variable-grain edges. Period-specific resolution lives in `resolve_at`; cross-variable
traversal in the per-edge accessors. All accessors are list-returning; `resolve_at`
returns `[]` (never raises) when no state covers the period — only the binding FQID not
resolving raises `fqid_not_found`. The method signatures are the reference in
`catalog.py` itself; the webapp's `/api/catalog/*` shape derives directly from this
surface (see `reg_webapp/DESIGN.md`).

The catalog return shapes — and, as of #701 (2026-06-23), the search return shapes in
`search.py` — are frozen Pydantic v2 models on a shared `_CatalogModel` base
(`BaseModel` with `frozen=True, populate_by_name=True, extra="forbid"`). This mirrors
`reg_schema`'s `_Model` shape but is a **separate** base — reg_meta takes no dependency
on reg_schema. Collection fields stay `tuple[...]`; Pydantic serializes tuples to JSON
arrays. `Fqid` stays a frozen `@dataclass` but carries `__get_pydantic_core_schema__` so
`fqid` fields validate from `str` or `Fqid` and serialize to the canonical FQID string
(OpenAPI `string`). Register-bearing models use Python attr `register_name` with
`Field(alias="register")` to avoid the `BaseModel.register` shadow; wire/init name stays
`register`. The earlier no-Pydantic soft preference (import-ergonomics + aspirational
Go/Rust port) is historical — #681 (2026-06-22) resolved that the port's real cross-impl
contracts are the SQLite `SCHEMA_VERSION` + `openapi.json` (both Pydantic-independent),
so reg_meta adopted Pydantic so FastAPI can consume its catalog models directly. The
hard no-Pydantic rule applied only to `reg_monabundle`'s amalgamated bundle (now
archived); reg_meta was never subject to it. See root CLAUDE.md "Stack" and
ARCHITECTURE.md.

When a caller constructs `Catalog` with a docs-DB connection, `ResolvedRegister` and
`ResolvedVariable` also carry `related_documents`: register-version PDF metadata
(`title`, `filename`, `source_url`, `license`, `fetched`, `sha256`, `byte_size`) read
from `reg_meta_docs.db`. The list is metadata only; binary content is fetched by exact
`(register, filename)` through `doc_queries.related_document_content`.

**`search.py` — the typed search surface (#701).** `queries.search` builds its result
rows as plain dicts through the internal pipeline and converts ONCE at the end into the
`SearchResult` discriminated union (eight arms, each `type:`-literal-discriminated, each
carrying `rank: float`). The `SearchResults` envelope carries a bounded `results` tuple,
`has_more`, and an opaque `next_cursor`. Cursor context binds the normalized query,
requested scopes, steward restriction, and catalog manifest; the final order uses a
unique entity identity after query-sensitive exact/prefix relevance and FTS rank. Typed
and mixed entity searches use the same fixed bounded horizon as folding so the published
relevance order cannot change when a later page expands an early FTS prefix. Variable
delivery aliases for that bounded candidate set are batch-loaded into an internal
ranking field before slicing; display annotation still runs only for the shown page.
Steward delivery-column scope narrows that field and representation group members before
scoring, so unheld aliases cannot affect order or cursor identity. Invalid or mismatched
cursors fail at the library boundary. Cursor integrity is checked before query work and
continuation has a hard 1,000-result depth ceiling, so a forged token cannot request an
unbounded prefix; researchers reaching the ceiling must refine the broad query.
Non-foldable branches use an adaptive `limit + 1` prefix and backfill when in-scope
shaping consumes a page. Foldable variable/classification/group branches instead use one
fixed 1,001-row horizon: every cursor sees the same complete bounded fold universe, so a
later sibling cannot turn an already-consumed leaf into a group or succession row.
Type/register/year/group eligibility, classification-code exclusions, and the value
surface's published bm25-plus-mapping-count rank are applied inside SQL before each
branch's bound. This is the search-surface analog of the catalog-typing move (#681): the
webapp's per-result mapper functions and `models.py` search wrappers are deleted; the
FastAPI response models embed reg_meta's search types directly.

An optional cursor-bound `exclude_fqids` set removes register/classification identities
inside their SQL branches before the bound. Presentation layers use it when they inject
a curated identity separately: every continuation then shares one origin universe and
cannot emit the injected identity again at its natural FTS position.

`search` accepts an optional `fqids: Collection[str] | None` allow-list (#859) that
restricts the **register and variable** leaf rows (and concept-group folding) to
entities whose navigable `fqid` is in the set. A group surfaces only if ≥1 of its
members is held, and its `members` list is narrowed to held members. Classification and
value/code surfaces are catalog-global and pass through unaffected. `None` means no
restriction — the pre-#859 behavior is byte-identical. The restriction is applied before
folding and paging, so every page is in scope. reg_meta stays steward-agnostic: the
caller supplies the allow-list; the set's provenance is opaque here. The webapp's
filtered-steward `/api/search` passes `admitted_variable_fqids | held_register_fqids`
(see `reg_webapp/DESIGN.md`); the CLI never passes `fqids`.

**Why two methods for succession.** `predecessors` / `successors` are split (not one
`replaced` returning a dict) so every edge-traversal accessor returns `list[...]`
uniformly. The longitudinal `resolve(fqid).replaced_by` attribute carries the
**outbound** edges (successors) — "X was replaced by Y" is the natural directional read;
inbound traversal is the explicit `predecessors(fqid)` call. The classification-grain
duals `classification_successors(fqid)` / `classification_predecessors(fqid)` (#571)
follow the same split, but key on the **literal edition slug** (not
`_resolve_edge_triple` live-row resolution) — tolerating dead predecessor editions is
the whole point of succession, since a renamed/retired slug no longer has a
`classification` row. `ResolvedClassification.replaced_by` carries the outbound edges on
resolve (dual of `ResolvedVariable.replaced_by`).

**`resolve_terminal_successor(fqid)` — citation-stable renamed-slug redirect (#355 PART
2; register grain added in #412; classification grain added in #571).** Dispatches on
FQID kind and walks the appropriate succession table to the TERMINAL chain end (the node
with no further outbound edge), returning that terminal as an `Fqid` of the **same
kind**, or `None` when the start has no outbound edge at all (genuinely unknown). Kind
dispatch: VARIABLE_BINDING walks `variable_replaced_by` on the stored (provider,
register, variable) triple; REGISTER walks `register_replaced_by` on the stored
(provider, register) pair; CLASSIFICATION walks `classification_replaced_by` on the
stored edition slug (a 1-tuple, so an old vintage edition can redirect to the current
one); PROVIDER has no succession table and returns `None` immediately. The start FQID
**does NOT need to resolve to a live row** — that is the key distinction from
`successors`: `successors` requires the FQID to resolve (it calls `_resolve_edge_triple`
which raises `fqid_not_found` on a dead slug), whereas `resolve_terminal_successor`
walks purely on the stored string tuple, so it can follow a renamed slug whose
`variable` / `register` / `classification` row is gone. Always resolves to the ABSOLUTE
chain end (never hop-by-hop): a 301 redirect can be cached, so returning an intermediate
would leave a cached redirect pointing at a now-dead slug after a double rename (A→B
then B→C). Split pick: when a predecessor has multiple successors, takes the
lexicographically first per `ORDER BY successor_... LIMIT 1` (same rule for all grains).
Cycle guard: a `seen` set terminates a malformed loop (A→B→A) without hanging. Only
PROVIDER FQIDs return `None` immediately — that grain has no succession table.

**`dimensions(fqid)` — concept-group memberships for a binding (#489).** Returns the
register's `ConceptGroupSummary` groups (the variant facet groups — level / population /
rank / …) whose members include this binding's variable. Resolves `same_as` via
`_resolve_edge_triple` like the other edge accessors, so an alias cites its **resolved
target's** groups (not the requested register's), and raises `fqid_not_found` /
`not_a_binding_fqid` on a dead or non-binding FQID for the webapp's 4xx/301 path.

**Multi-state at a period is normal, not an edge case.** `resolve_at` returns a list
because length N is genuinely common: several variants delivered the variable at the
period (omitting `variant`), a range period crosses transitions, or — the common case
for classification-versioned variables — multiple value-set versions co-exist in the
period (a crosswalk era, SNI92 + SNI2007 in a transition year). The list shape is the
contract; no exception is raised on ambiguity. Callers who know the variant pass
`variant=…`; callers who know the vintage pass `value_set_version=…`.

**Alias windows (#319/#945).** `variable_alias_window` records validity intervals for
delivery-column aliases that must be resolver-visible representations. A curated
monthly-family merge (build-side, see reg_meta_build/DESIGN.md → Consumers: monthly
column families) folds 12 month-named delivery columns into ONE variable carrying an
ANNUAL `variable_state` per year, with each month column's sub-annual window here:
`resolve_at("2024-03")` → the `mar` column (window `2024-03-01..2024-03-31`),
`resolve_at("2024")` → all 12. Multi-alias SCB cvids (#945) use the same table for
state-window aliases such as `LoneInk_LISA2006` / `LoneInk_LISA2007`, so every concrete
delivery column can be picked/ordered rather than remaining search-only. The expansion
(`_expand_state_windows`) overrides only `delivery_column_name` + `valid_from`/
`valid_to`; `value_set`/`data_type`/`state_id`/`value_set_version_label` come from the
base claim, so windows can SHARE one `state_id` (one claim, N representations) — the
per-window identity is the compound (`state_id`, `delivery_column_name`, `valid_from`).
A variable with no window rows maps 1:1, byte-identically. The monthly merge is
explicitly retained under #518/#523; the retention rationale and the #523↔#496 two-layer
boundary are recorded in `reg_meta_build/DESIGN.md` → *Consumers: monthly column
families*.

**`Period`** — `int | str | dict`, the polymorphic period `resolve_at` accepts (mirrors
`Source.period`): a bare year (`2018`), a period token
(`"HT2020"`/`"2020-Q3"`/`"2020-08"`/`"2018-12-31"`), an explicit range `{"from", "to"}`
(endpoints are int or token), or the `"_default"` snapshot sentinel (no period filter).
Expanded to an inclusive ISO `(lo, hi)` interval by `_period_bounds` +
`fqid.period_token_to_bounds`, intersected against the full-date `variable_state`
validity ranges — so sub-annual and range queries are precise, not year-granular.

**`ResolvedVariable`** — the longitudinal binding resolution. Fields: `fqid` (the
caller's 3-seg binding FQID `provider/register/slug`, preserved through a `same_as`
traversal), `variable_id`, `register_id`, `provider_key`, the shared metadata (`name`,
`definition`, `description`, `measurement_unit`, `is_sensitive`, `is_identifier`,
`deprecated`, `source_register_id`, `source_register_text` when stable at variable
grain), `states` (tuple of `VariableState`, chronological ascending), the variable-grain
edges `same_as` / `replaced_by` (OUTBOUND successors) / `lineage`, `via_same_as` (the
traversal path when resolved via a `same_as` edge, else None), and `group` (the
binding's owning concept group as a `BindingGroupRef` `(provider, register, key)`, None
when ungrouped; #616).

**`VariableState`** — one `variable_state` row tagged with its variant. Fields:
`state_id`, `variant` (the `register_variant.slug`), `register_variant_id`, `valid_from`
/ `valid_to` (inclusive ISO dates), `data_type`, `data_length`, `delivery_column_name`
(denormalized latest alias), `source_register_text` (raw source attribution/code when it
varies by state), `value_set_version_label` (NOT NULL, `''` = no discriminator),
`value_set_id`, `value_set` (hydrated `(code, label)` tuple, None when the state has no
value set), `is_identifier` (variable-grain flag denormalized onto every state via a
JOIN — constant across all of a variable's states — so consumers holding only a
`VariableState` (e.g. the `resolve_at` / `/states` paths) can read the authoritative
identifier flag without needing the enclosing `ResolvedVariable`), and
`classification_slug` (the classification family slug (see DESIGN.md → Classifications)
for this state's value set, e.g. `lkf2007`; resolved per-state from
`variable_state.classification_id` — varies across a variable's states; None for
code-less / unclassified states). The full delivery-column history — multiple aliases
per state from cross-edition spelling drift — lives in the `variable_alias` table;
`delivery_column_name` is its denormalized latest, and `reg-meta get datacolumns`
surfaces the complete list.

**Edge semantics (reader-facing).** All relationship edges are **variable grain** — the
variant is a delivery coordinate, not an identity level, so there is nothing below the
variable to anchor an edge on. The edge triple `(provider, register, variable)` **is**
the binding FQID. Two edge tables partition the relationship space:

- **`same_as`** — symmetric cross-register / cross-provider **equivalence**
  (substitutable: "this variable here is that variable there"). **Curated only — never
  auto-derived.** Within-register `var_id` reuse is now the variable itself (one
  variable, many variant states), so the old `(N choose 2)` auto-derive from matching
  `var_id` is gone; `same_as` carries only the genuinely-curated cross-register set.
  `resolve()` traverses it transitively, recording the path in `via_same_as`.
- **`replaced_by`** — directional succession (predecessor superseded by successor). See
  `predecessors` / `successors` accessors and `ResolvedVariable.replaced_by`.

The `related_to` edge kind and `variable_related_to` table were retired in #800. The
non-foldable split siblings (`code_vs_label_pair`, `import_bug_suspect`) that formerly
rode that table are preserved only as in-build sibling pairs driving the concept-group
fold — not persisted to any researcher-facing edge table. The thematic see-also need is
deferred to the tags layer (#311). Curated `related_to` edges in `relations.toml` have
been removed; the curated pairwise surface now carries only `same_as` and `replaced_by`.

Same-concept grain/vintage/coding appears in neither edge table — it *folds* into one
variable (the fold/split distinction and auto-emit mechanics are in
[../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md)). Succession (`replaced_by`)
is directional and orthogonal; lineage (see reg_meta_build/DESIGN.md → Consumer-side
lineage (variable_state_lineage)) is the state-grain composite-source edge.

**`VariableRef`** — a variable-grain edge endpoint (`same_as` / `predecessors` /
`successors`). Fields: `fqid` (the 3-seg binding FQID — the edge tables store exactly
the `(provider, register, variable)` triple, which **is** the binding FQID; built via
`_ref_fqid`, None only if a slug is malformed/NULL), the load-bearing `provider` /
`register` / `variable` triple, and (#142, on succession refs only) `reason` (the
`timeseries_event.beskrivning` transition reason) + `effective_year` (the
AktuellVariabel-grain successor edition year; None on `same_as` refs and on bare-grain
succession with no edition). Note: `RelatedRef` (the former `variable_related_to`
see-also endpoint) was retired in #800 alongside the `related` edge kind.

**`ClassificationRef`** — a classification-grain succession edge endpoint (#571),
carried by `classification_successors` / `classification_predecessors` /
`ResolvedClassification.replaced_by`. The classification FQID is 2-segment
(`class/<slug>`), so the edge endpoint is a single slug — no provider/register triple.
Fields: `fqid` (best-effort `class/<slug>`, built via `_class_ref_fqid`; None only on a
malformed slug), the load-bearing `slug`, `effective_year` (the succession year, or
None), and `note` (build provenance — `derived:vintage_chain` for the auto edges,
`curated:slug_toml` for the curated #579 edges). There is no `reason`/`beskrivning`
column on `classification_replaced_by` (that column exists only on the variable-grain
`timeseries_event`), so `ClassificationRef` carries `note` where `VariableRef` carries
`reason`. Succession references the **exact edition slug** as identity; `fqid` is
best-effort to surface malformed slugs gracefully rather than raising.

**`LineageEdge`** — one `variable_state_lineage` row (see reg_meta_build/DESIGN.md →
Consumer-side lineage (variable_state_lineage); state grain): `consumer_state_id`,
`source_state_id`, `valid_from` / `valid_to` (the validity intersection), and
`source_fqid` (the source state's 3-seg binding FQID; None only on a malformed/NULL
slug, as with the refs' `fqid`).

**`LineageWarning`** — one `variable_state_lineage_warning` row: `consumer_state_id`,
`warning_kind` (`no_source_state` / `ambiguous_source_variant`), `message`.

`ResolvedVariableBinding` (the interim per-edition binding row) and the `editions()`
discovery path that returned it were **removed** along with the v0.11 5-seg binding
parse. Resolution is now `ResolvedVariable` + `resolve_at` / `states` (§5.10): the
variable's shared metadata plus its `variable_state` rows, each tagged with its variant.
The per-edition cvid is no longer a catalog return shape, and the variant is a register
sub-resource coordinate (passed to `resolve_at`), not a slash-path FQID segment. The
v0.x per-edition `resolve()` behavior was deleted, not aliased — pre-v1 policy (no
shims).

## Steward delivery inventory (`inventory.py`)

A `project_data.json` source is a **logical** selection — register variant, variable,
period. What a steward physically delivers is separate data, and the delivery inventory
is that contract: the public, version-controlled steward source of truth from which
edition-aware admission, coverage stats, browse unions, and order materialization are
derived. `REFACTOR_SPEC.md` §12 is the decision text; this section documents the format
and the validator. This module is the contract alone — the materializer, the coverage
gate and the order manifest live in `order.py` (next section); each steward's real
inventory content (and its generator) is separate work.

**Format: TOML** (ratified 2026-08-31), following the repo's generated-`auto.toml`-plus-
curated-overrides pattern (`reg_meta_build/fqid_slugs/`): humans curate editions, and
comments carry the curation rationale, so the format has to be comment-capable. The
compiled steward artifact's internal representation is the build's choice, not this
file's.

```toml
version = 1                       # contract version — bumped, never migrated (pre-v1)
steward = "swecov"                # the deployment this inventory belongs to

[[table]]
id = "LISA_Individ_2019.csv"      # opaque EXACT identifier: a delivery filename, or a
                                  # schema-qualified SQL table (`dbo.Patientregister`)
edition = 2019                    # ONE explicit finite period — never "_default"

[[table.column]]
name = "Kon"                      # literal, case-preserving physical column
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"
representation = "Kon"            # optional; the canonical reg_meta representation

[[table.column]]
name = "LopNr"                    # zero mappings = unresolved, but still inventoried
```

- **`id` is opaque and exact.** It is never parsed for meaning: a table whose name
  carries no period still requires an explicit curated `edition`. Filename-edition
  inference is not implemented here; if a generator adds it, §12 requires it to fail for
  review on zero or ambiguous period tokens rather than guess.
- **One explicit finite `edition` per table**, in the shared period grammar — a token
  (`2019`, `2019-03`, `2019-Q3`, `HT2019`, `2019-03-01`), a `{ from, to }` range, or a
  finite list of those for an interrupted series. A bare TOML year int canonicalizes to
  its token string. `"_default"` and any unbounded "all periods" sentinel are rejected:
  an edition is what makes coverage computable. `edition_bounds()` expands an edition
  into inclusive ISO `(lo, hi)` intervals via `fqid.period_token_to_bounds`, so an
  inventory edition and a project period expand through the same grammar.
- **Zero or more mappings per column.** A mapping names the 3-part variant coordinate,
  the 3-segment variable binding FQID, and the nullable canonical `representation` (a
  join discriminator, not an output substitute). Zero mappings keep an unresolved
  physical column in the coverage denominator without admitting or ordering it; several
  mappings let one column serve several variants (the combined Utrikeshandel table); and
  several tables may independently map to the same logical coordinate.

**Validator.** `load_inventory(path)` parses the TOML and returns the frozen Pydantic
models (`DeliveryInventory` → `InventoryTable` → `InventoryColumn` → `ColumnMapping`),
or fails fast with `RegMetaError` — `inventory_toml_unreadable` / `inventory_invalid`,
both `EXIT_CONFIG` (10), the same configuration class the curation TOMLs use. Errors
name the offending table and column by identifier rather than array index
(`table['LISA_Individ_2019.csv'].column['Kon'].mapping[0].variable`), because the author
is editing a TOML file where an index is not a locator. Structural rules beyond field
shape: unknown keys are rejected (`extra="forbid"`), a table identifier appears once (an
identifier is exact and carries exactly one edition), a physical column is declared once
per table with all of its mappings under it, and a mapping's variable must belong to its
`register_variant`'s `provider/register`. Structurally empty input is rejected too — an
inventory declares at least one table, and a table at least one column. This file is the
steward's authoritative holdings statement, so an empty one is a mis-generated or
half-authored file, never a claim to deliver nothing; accepting it would silently zero
out admission, coverage, and browse unions. Inventory ↔ reg_meta DB consistency (does
each mapping's `(register_variant, variable, representation)` resolve against the
flavored DB?) is a standing build/CI gate, deliberately NOT part of this structural pass
— the validator is pure domain code with no DB access.

The models are `reg_schema`-free: the contract needs only reg_meta's own period grammar
and FQID parser. (`order.py` takes the `reg_meta → reg_schema` dependency §12 sanctions;
the inventory contract itself does not need it.) `EditionRange` mirrors reg_schema's
`PeriodRange` wire shape (`from`/`to`, `from_` attr with a `"from"` alias), so a project
period and an inventory edition expand through one grammar without a converter.

## Order materializer and manifest (`order.py`)

`materialize_order(project, inventory, conn)` is the one place a logical
`project_data.json` selection meets a steward's physical delivery topology. It returns
either a complete `OrderManifest` or a non-empty set of `OrderFinding`s — never a
partial order. `REFACTOR_SPEC.md` §12 is the decision text. The FastAPI endpoint and the
CLI/plugin are thin adapters over this one function, which is what makes their results
byte-identical; all logic (and all fail-closing) lives here.

`inventory=None` selects §12's **global-deployment fallback**: the global deployment has
no physical delivery topology, so canonical resolution alone grounds the order. It is
the SAME function and the same pipeline — only step 3's matching arm differs — so there
is no second clip/slice/coverage implementation to drift. `OrderProvenance.mode`
(`steward_inventory` \| `global_fallback`) names which one produced a manifest, and the
provenance gate treats the global deployment like any other: `ProjectData.steward` must
equal `"global"` (`order.GLOBAL_STEWARD`).

Per `sources[*].bindings[*]`, in project declaration order:

1. **Availability clip first.** A source period means "these columns, wherever each is
   available inside this window". Each binding is clipped to its own availability — the
   union of its `variable_state` windows at the source's variant, via
   `Catalog.resolve_at` — so a column first delivered in 2019 under a 2018–2020 source
   does not widen the order into a cross-product. Every clip is reported as a
   `ClipReport` on the manifest: informational, never silent, never an error — and
   recorded BEFORE the ambiguity gate can return, so a binding that is both clipped and
   ambiguous surfaces both. This is also the seam a deferred per-binding period override
   would narrow (§12); no schema change was needed.
2. **Representation slicing.** The clipped request is partitioned into slices of
   constant canonical representation (`delivery_column_name`). A sequential rename fans
   out into two slices; two columns valid at the SAME instant with no
   `Binding.representation` pin is ambiguity and blocks. Resolution logic is not
   re-derived here — `resolve_at` is the source.
3. **Steward matching + coverage gate.** A table matches a slice only when one of its
   columns carries a mapping matching `(register_variant, variable, representation)` AND
   its edition overlaps THAT slice; the edition contributes only its overlap. A mapping
   that OMITS `representation` is the inventory's single-representation arm: it matches
   only a binding that resolves to ONE canonical representation across the request. When
   the representation changed, an unqualified mapping cannot say which slice its column
   is, so it blocks (`mapping_ambiguous`) instead of letting one physical column claim
   two canonical representations — but only from a table whose edition overlaps the
   clipped request, since a table that cannot overlap never contributes a column and so
   cannot make anything ambiguous. Any subperiod of the availability-clipped request
   left uncovered blocks the WHOLE order with the exact gap (`coverage_gap`), and a
   slice no mapping serves blocks with `mapping_missing`. Overlap alone never buys a
   partial manifest. In global-fallback mode the slice's own canonical column serves it
   under a blank table, so the slice covers itself exactly and the same gate runs
   unchanged — what canonical resolution did not deliver has already blocked upstream as
   an unresolved, unavailable or ambiguous binding.
4. **Emission.** Every matching table is emitted whole — v1 has no table chooser and no
   row filter (the §12 `simplify:` stands, with SWECOV's
   one-large-SQL-table-per-register delivery as the upgrade trigger). Entries preserve
   project source/binding order; the fan-out inside a binding sorts by table, canonical
   edition, then physical column.

**Fail-closed, one pass.** A blocking result enumerates every finding across every
binding — `steward_mismatch`, `project_empty`, `period_not_orderable`,
`variable_unresolved`, `binding_unavailable`, `representation_unknown`,
`representation_unresolved`, `representation_ambiguous`, `mapping_missing`,
`mapping_ambiguous`, `coverage_gap` — so a researcher fixes the whole order in one edit
instead of one gap per round trip. `ProjectData.steward` must equal the deployment's
steward — the inventory's, or `"global"` in fallback mode (provenance is checked before
anything resolves; retargeting is deliberately not a feature) — and an empty project
stays a valid draft that cannot produce a header-only manifest.

**The manifest is a versioned JSON contract.** Version 1 is **in definition** until the
§12 boundary ships: it has no external consumer yet, so shape changes while the
remaining §12 lanes land stay within version 1 rather than churning the number (operator
decision, Y-19/1 review). Bump discipline — an incompatible change bumps
`ORDER_MANIFEST_VERSION`, pre-v1 changed-not-migrated — binds from the first external
reader (the steward-side extract system). `OrderManifest` (version
`ORDER_MANIFEST_VERSION`) carries provenance (mode, steward, project name / schema
version / declared reg_meta version / SHA-256 of the project's canonical JSON, plus the
catalog DB's `schema_version` and `import_date`), the resolved entries — logical
coordinate (`provider,register,variant,variable` + the canonical representation), the
availability-clipped `requested_period`, and the physical coordinate
(`edition`,`table`,`column`) — and the informational clips. It is machine-written here
and machine-read offline by the steward-side extract system, so it is self-contained: no
network, no catalog lookup at extract time. Both boundaries validate against the same
frozen `extra="forbid"` models. `to_json()` is the canonical serialization (sorted keys,
stable entry order, trailing newline); repeated runs over the same inputs are
byte-identical. Periods render through the shared grammar's inverse
(`period_token_for_bounds`), so a manifest speaks the same period spelling as a project
period and an inventory edition. `extraction_filenames(entry)` pins §12's output-naming
rule — one UTF-8 CSV per variant + period unit — in the contract rather than leaving it
to the extractor.

Pure domain code: no FastAPI, no filesystem writes, no timestamps (the only time-shaped
manifest values come from the DB manifest and the project). A global-fallback entry is
the same shape with a blank `table`, the canonical column in `column`, and
`edition = requested_period`, so `extraction_filenames` gives it one file per requested
period segment without a special case.

**The adapter door.** §12's "both product surfaces emit byte-identical results" holds
only if the adapters are genuinely thin, so the two things they would otherwise each
re-type live here too, beside the materializer:

- `project_from_raw(raw)` (and `load_project(path)`, the CLI's file-reading wrapper) is
  the ONE door into `materialize_order` for an untrusted `project_data.json`. The
  `ProjectData` model enforces field TYPES only, so it runs `reg_schema`'s
  `validate_structural` first — without that gate a model-valid but structurally invalid
  spec (a malformed `register_variant`, a bad period token) would materialize a bad
  provider order. An invalid spec raises `RegMetaError` (`project_invalid` /
  `project_unreadable`, `EXIT_CONFIG`), so both adapters reject the same specs with the
  same words.
- `blocked_message(result)` renders every blocking finding, in the materializer's own
  accumulation order, each prefixed with the source/variable/period it names. The
  fail-closed path is byte-identical across adapters too, not just a produced manifest.
  It is a PRESENTATION of `OrderResult.findings`, never the record of them: the findings
  are already typed models, and an adapter whose transport can carry structure carries
  the models (the webapp's 422 body does — see `reg_webapp/DESIGN.md` → The order
  manifest), with this line as the human summary beside them.

The adapters themselves are `reg_webapp`'s `POST /api/project/order` (see
`reg_webapp/DESIGN.md` → Project-write surface) and
`reg-meta order <project.json> [--inventory <toml>]`, which writes `to_json()` verbatim
to stdout or `--output` — never through the CLI envelope or `--format`, because that
canonical serialization IS the artifact. Omitting `--inventory` is the same
`inventory=None` global fallback the webapp's global deployment passes, not a degraded
CLI mode.

## Value sets are year-projected

`Vardemangder.csv` is the historical union — every code that ever applied to a variable
in any register year, with no temporal qualification. `VardemangderValidDates.csv` is
the authoritative temporal filter: per `(ItemId, valid_from, valid_to)`, with NULL
bounds meaning "no boundary." A code without a validity row is always valid throughout
the variable's lifetime (per SCB correspondence).

The DB stores year-projected value sets, not the raw union: each `variable_state`
carries the codes that were actually valid in its era (the projection is computed per
cvid at build time, then attached to the coalesced state). Projection is intentionally
year-precision, not exact-date. SCB's metadata is annual; sub-year boundaries (e.g.
`valid_from=1995-09-01`) are administrative artifacts that year overlap absorbs
losslessly. The trade-off — losing sub-year query precision — is paid for by removing
the temporal axis from the schema entirely. There is no `get values --valid-at` flag and
no historical-union opt-in: the union is discarded by design.

The projection rule and its build-time mechanics are documented in
[../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md) § "Year projection".

The result is content-addressed and deduplicated: identical year-projected sets share
one `value_set` row (`member_hash` = sha256 of sorted `(code, label)` pairs); each
`variable_state` links to its set via `variable_state.value_set_id`. NULL `value_set_id`
means the state had no codes (every union pair excluded by projection, or only sentinel
rows in the source — see [../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md) §
"Vardemängder sentinel filtering").

## Classifications

Named code systems (SUN2000, SSYK2012, SNI2007, LKF, ...) are first-class entities. Each
`classification` row carries metadata (publisher, validity range, supersedes link,
canonical URL) and a cached `code_count`. The `classification_code` junction holds the
deduplicated union of value codes that belong to the classification, with an optional
`level` integer for prefix-hierarchy filtering (length of all-digit codes; NULL for
non-numeric codes like ICD letters).

The `supersedes link` (the `supersedes_id` FK / `supersedes` short_name surfaced by
`list/get classification`, and its reverse `superseded_by` back-pointer) is a **derived
projection** of the active subset of `classification_replaced_by`, the single canonical
succession surface (#579), not a separately-curated field. Future-dated edges remain in
the edge table and become active only when the DB manifest's classification succession
as-of year reaches their `effective_year`; read-side currentness and terminal redirects
use that same policy. Because a predecessor can fan out to several successors (the
`sun1996` → 2000 nivå/inriktning/grupp split), `superseded_by` is a `GROUP_CONCAT` over
all rows whose `supersedes_id` points back — `superseded_by(sun1996)` returns all three
2000 dimensions. See `reg_meta_build/DESIGN.md` → "Classification succession".

The FK lives on `variable_state` (per-era), not on `variable`. SCB's data model already
places the classification label (`value_set_version_label`) per era, and many headline
variables genuinely span multiple classifications across their lifetime — e.g.
`Utbildningsnivå` (var_id 66) uses SUN 2000 codes through 2018 and SUN 2020 codes from
2019 onwards; `SSYK` and `SNI` show the same generational drift. Linking at the state
level keeps each code system distinct (SUN 2000 codes never bleed into SUN 2020),
isolates split siblings (each sibling's states classify independently), and lets
variable-level helpers aggregate when needed.

The `classification_id` column is populated at build time from a maintainer-curated TOML
seed at `reg_meta_build/classifications.toml` (exact match against
`value_set_version_label`, no fuzzy inference). The seed schema, build-time invariants,
and validation rules live in [../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md)
§ "Classification seed".

### Canonical codes and state conformance

`classification_code` is the classification's definition. For CSV-backed classifications
it contains only published canonical codes; observed value-set codes that merely show up
in data are not attached to the classification page.

Canonical codes come from required per-classification CSVs ingested by the build (see
[../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md) § "Canonical code CSVs").
Fresh builds emit only canonical `classification_code.is_valid = 1` rows and cache
`classification.valid_code_count` for that canonical count.

The build records declared value-set mismatches at state grain in
`classification_conformance` / `classification_conformance_code`: kept links can warn
about a minority of nonconforming codes, and low-overlap links are severed while the
original declared classification and overlap evidence remain visible on the variable's
value-set viewer.

The CLI exposes this via `get classification --codes --only-valid` and includes
`is_valid` per code in JSON output.

Hierarchy is intentionally not encoded as `parent_code_id`. The `level` column captures
the most useful filter ("top-level only"); deeper parent/child queries fall back to
prefix matching on `value_code.code`. Code sets without prefix hierarchy (ICD-10, ATC)
keep `level = NULL` and use their own conventions.

## Concept groups (presentation layer)

The catalog renders machine-stamped SCB column *families* as flat lists of
near-identical rows (issue #303): month-suffixed variable families
(`agi1lonfinkjan`…`agi1lonfinkdec`), split-sibling coding successions
(`sun2000inr`/`sun2020inr`). The **concept-group layer** folds these for browse:
`concept_group` + `concept_group_axis` + `concept_group_variable` +
`concept_group_variable_facet` + `concept_group_classification`, derived at build time
(`reg_meta_build/concept_groups.py` documents the derivation dimensions and their
guards; see `reg_meta_build/DESIGN.md` → Concept-group derivation).

**Classification vintage editions** (`lkf1980`…`lkf2026`, `ssyk1996`→`ssyk2012`,
`sun2000-niva`→`sun2020-niva`) are **not** folded into concept groups (#571). Editions
of one classification are a temporal succession, not a parallel browse facet. They
materialize as adjacent-edition edges in `classification_replaced_by` (auto-derived from
the same year-tail detection; cross-stem restructures the year-tail can't reach are
curated, #579 — e.g. `sun1996` → the 2000 nivå/inriktning split).
`concept_group_classification` holds CURATED umbrella groups: `group:sun` (#516) groups
the three genuinely-distinct SUN 2020 dimensions (`sun2020-niva` Utbildningsnivå,
`sun2020-inriktning` Utbildningsinriktning, `sun2020-grupp` Utbildningsgrupper) PLUS the
two nivå aggregates (`niva-oldv1` / `niva-grovv1` — version-independent coarsenings of
the nivå dimension, 7-level and 5-level respectively). The aggregates carry no
succession edge (version-independent) and are terminal, so they survive the
classification-root's terminal-only filter and fold under the group. Classification
umbrellas are **axis-less** — zero `concept_group_axis` rows (#819); the members are
distinct classifications, each carrying its own curated short label, and the webapp
renders the member-noun as "members". The granularity relationship is surfaced at the
classification leaf via `Catalog.classification_dimensions`, which reads
`concept_group_classification` membership and returns the group(s) the edition belongs
to as `ConceptGroupSummary` objects — the same type returned by
`list_classification_groups()`. The value-set viewer (#609) renders this alongside
`Catalog.classification_codes` (the resolved edition's canonical `classification_code`
rows). Prior editions (`sun1996`, 2000 editions) are not members — they are temporal
predecessors of each dimension and appear in `classification_replaced_by` (the 2000→2020
steps auto-derived #571; `sun1996`'s 1→many split into the 2000 editions curated #579).

**Presentation only, identity untouched.** A group is *not* an FQID kind and never
becomes a binding/order/stats key — members keep their leaf FQIDs, and a binding's
`value_set: "class/lkf2020"` keeps referencing the exact vintage. Identity-level folding
by classification family was tried and dropped (#223 part 2, 195 measured over-folds);
because grouping is presentation, a wrong group is a cosmetic curation bug, not identity
corruption. A variable/classification belongs to **at most one** group (for
classifications: the single-column member PK; for variables: the surrogate-keyed
`concept_group_variable` no longer enforces it directly, so the build validator
re-asserts "one group per variable\_id" (#819)). When the interval-native model (#271)
merges month columns into single variables, the month groups dissolve into real
variables and the layer shrinks to edge/rank/vintage duty.

**API**: `Catalog.list_concept_groups(provider, register)` (variable groups, register
scope) and `Catalog.list_classification_groups()` (classification umbrella groups,
catalog scope) return `ConceptGroupSummary` — `key` (scope-unique derivation key, a UI
anchor), `label`, `source` (`edge`/`token`/`curated`), `axes` (the group's ordered
`GroupAxis(name, label)` objects from `concept_group_axis`, #819: match on stable
`name`, display curator-authored `label`; empty for edge/axis-less umbrella groups, one
element for single-axis groups, N for multi-axis curated families), and members ordered
by first-axis facet value then slug. Each `ConceptGroupMember` carries the leaf `Fqid`,
display name, optional `delivery_column` (None for a whole-variable member, the SCB
delivery column for a representation member), and per-axis `GroupFacet` assignments
(`month`/`rank`/`vintage`/`enhet` — sortable `value`, display `label`). The webapp's
register / classification-root responses embed these alongside the complete flat
children list, and the SPA folds (`reg_webapp/DESIGN.md`).
`list_classification_groups()` returns the curated umbrella groups: currently
`group:sun` (#516) — axis-less, so its members are distinct classifications carrying
their own curated short label, with no shared facet axis. Derived vintage editions live
in `classification_replaced_by`, not here.
`Catalog.concept_group(provider, register, key) -> ConceptGroupSummary | None` fetches a
single group by its scope-unique key (#616); returns None for an unknown key or unknown
pair (mirrors `list_concept_groups` tolerance). A group needs its own accessor because
its default selection is all members — a member FQID cannot express that. Member
bindings carry `ResolvedVariable.group` (`BindingGroupRef` `(provider, register, key)`,
None when ungrouped) so a member page can render group-aware without a second fetch.

**CLI/search surface (#322/#325)**: the same read surface backs three CLI shapes, all
result-shaping over the 5.3.0 tables (`reg_meta.queries`). `get groups REGISTER` (and
`get groups --classifications`) lists groups with members-with-facets, JSON-able like
every other command. `search` folds sibling hits: when ≥2 distinct member variables of
one group match, the leaf hits collapse into a single `type: "group"` result row (the
facet-ordered member list under `members`, and the count of folded hits as
`matched_count` on the typed model); a lone member hit stays a leaf annotated with
`concept_group`/`concept_group_label`; and group LABELS themselves match (searching a
family label finds its group row even though no single leaf row matches). `--no-fold`
flattens. `get schema` carries `concept_group`(`_label`) per column so the fold is
visible inline.

**Classification edition chains fold in search separately (#571).** Before the
concept-group fold, `_fold_classification_succession` collapses classification edition
hits that share a `classification_replaced_by` chain into one
`type: "classification_succession"` result row — the terminal (current) edition's
identity, plus the full `editions` list (terminal-first by BFS depth — date-independent,
so robust to undated `effective_year` edges; #588) and the count of folded hits as
`matched_count`. The internal dict pipeline carries the raw `matched` leaf list for fold
arithmetic and `_strip_internal_keys` drops `_classification_id` from it;
`_row_to_model` reads `matched` to compute `matched_count` and does not put `matched` on
the typed model. This fold is terminal-centric (it collapses a whole family onto its
terminal, with no queried node), so collect-all-ancestors is correct here — unlike
`Catalog.classification_chain` / `variable_chain`, which anchor on the QUERIED node's
path (also #588) so a merge sibling on a different inbound branch is excluded. A lone
edition hit (whether terminal or an old vintage) stays a leaf; an old-vintage lone hit
is annotated with `terminal_fqid` so the webapp can link "current". This fold runs
**before** the concept-group fold so collapsed terminals can then fold into a curated
umbrella group (e.g. `group:sun`, #516) cleanly — the succession row keeps the
terminal's `_classification_id` so the umbrella pass treats it as that classification.
All folds happen before pagination — a succession row and a group row each count as one
result.

## Relationship graph (#761)

The webapp's subject-page graph view (#666 epic, renderer #678) consumes a single typed
**graph object** from reg_meta — topology plus the domain predicates that shape it (is a
single-variable graph meaningful? how does a group expand? which editions dedup? where
does a representation run break?) — so the SPA renders a graph as-is and never assembles
graph *semantics*. The model + builders live in **`graph.py`** (off the \~2.6k-line
`catalog.py`); `Catalog` exposes four thin accessors that delegate there:
`graph_for_fqid(fqid)`, `graph_for_classification_fqid(fqid)` (#792, the classification
analog of `graph_for_fqid`), `graph_for_group(provider, register, key)`,
`graph_for_classification_group(key)`. `graph.py` imports from `catalog.py`;
`catalog.py` imports `graph` lazily inside those methods, so the dependency stays
one-directional. The graph models are frozen `_CatalogModel`s used **directly** as the
webapp's FastAPI response models (no wrapper, per #681); there is **no CLI surface** —
the webapp is the only consumer.

**Compose, don't re-query.** The builder orchestrates the existing accessors — each the
single source of truth for its edge type: `variable_chain` (variable succession),
`representation_successions` (curated representation-grain succession), `dimensions` /
`concept_group` (group membership), `classification_chain` +
`classification_predecessors` (classification editions), `resolve` (same_as
canonicalization). The only genuinely new logic is group expansion, edition dedup, and
the representation-run computation.

**Model.** One node per variable (`VariableGraphNode`) or per classification edition
(`ClassificationGraphNode`), discriminated by `kind`. A variable node carries its full
`variable_state` history as sub-structure (`GraphState`, ordered
`(variant, valid_from)`), plus `same_as[]` (resolved-away aliases, metadata) and a
shared `group_key` (clustering metadata — there is **no** `group:<key>` node; namespaced
`provider/register/key` so a cross-register graph never clusters two unrelated
same-keyed groups, since concept-group keys are only register-unique). A variable node
also carries its facet identity **within its canonical group** (#792, for #678's
binding-leaf header): `facets` is the variable's own member `GroupFacet`s (the
`catalog.GroupFacet` model reused directly as the wire type, per #681 — not a parallel
model), and `group_label` is the canonical group's display label. Post-#819 a variable
can be SEVERAL members of one group (one per `delivery_column`), so the variable-grain
`facets` is the deduped UNION across all of the variable's member entries
(deterministic: group-member then axis-ordinal order); the per-representation split is
the renderer's job once representations are first-class (#757). The leaf derives its
#670 header identity (group + facet label) from the graph alone without a second
`/dimensions` fetch; both `facets` and `group_label` are empty/None when the variable is
ungrouped or on group/member skew (it degrades, never crashes). The group is fetched
once per distinct group (builder-memoized, honoring "compose, don't re-query"). The
classification node does **not** carry facets yet — that increment is co-designed later
with #757. A classification node carries a **point** `version_year` (never an interval —
an edition is not "dead" after its successor; the edition's OWN vintage from the
`classification` row's `valid_from`, NOT the supersession year — so the terminal current
edition keeps its own year, not None) + `is_current`. Time semantics live on the node,
so there is no top-level `mode`: the renderer draws a time axis when interval (variable)
nodes are present and a version ordering when point-year (classification) nodes are.
**One edge kind**: `succession` (directed, predecessor→successor). The `related` edge
kind was retired in #800 — grouping is concept-groups, identity equivalence is
`same_as`, thematic see-also is deferred to tags (#311). Everything else is
metadata/affordance: `lineage` / `source_register` are #678's provenance affordance (not
edges); `same_as` is resolved away to the canonical node; ordinary value-set /
classification / column boundaries are states-within-a-node (the run ids), not edges.
Curated `representation_replaced_by` rows are the exception: they surface as
`succession` edges between the variable-grain nodes, carrying `source_column`,
`target_column`, and optional `variant` metadata so a variant-scoped rename renders only
inside that variant. Every edge carries a stable `id` that doubles as its dedup key, so
a shared succession edge surfaced from multiple members during a group union collapses.

**Representation runs (the #526 fold, query-side mirror).** Each `GraphState` carries a
`representation_run_id` (int, unique within the node): consecutive states sharing it
form **one rendered cell**. The id increments at each representation boundary **and** at
every `variant` change (a run never spans variants — this replaces an ambiguous
per-state boolean). A boundary is **exactly one of four** identity changes between
adjacent `variable_state` rows — the value-set IDENTITY (`value_set_id` **and** its
`value_set_version_label`: the #526 state-identity gkey for a valued state keys on both,
so two states sharing a `value_set_id` but differing in label are distinct materialized
states; the label is `''` for valueless states, so it never spuriously fires there),
classification (`classification_slug`), or the per-era coalesced `delivery_column_name`.
Raw `data_type` / `data_length` are **never** a boundary signal on their own: SCB's
per-delivery `Datatyp` / length is low-trust passthrough that #526 blanks, so an
`int -> bigint` or char↔varchar wobble does NOT open a run. This scopes the boundary to
"distinctions that survive in `variable_state`" — precisely what reg_meta_build's #526
value-set-anchored fold leaves in the materialized rows; reg_meta re-derives it
query-side (it must not depend on reg_meta_build — wrong dependency direction).
Per-period alias multiplexing (monthly families' 12 columns, held in
`variable_alias_window`, not in `states`) is an alias concern, **not** a coding boundary
— those expanded windows share a `state_id` and are folded back to the single claim
before runs are computed.

**Empty graph** (`nodes: []`) is the "don't render" signal (the frontend gate is
`nodes.length === 0`): a lone variable with no succession, no group siblings, and no
meaningful representation boundary (the `akters` `int -> bigint` case — `data_type` is
not a boundary signal, so it stays one run), or a lone classification edition with no
succession chain and no group context. A lone variable **with** a value-set/column
change but no succession returns one node whose states span ≥2 runs → renders (as ≥2
cells).

**Fork B (group ⇄ member).** `graph_for_fqid` roots the union on the resolved variable's
`.group` members (or itself when ungrouped) and sets `focus_id` to the resolved node;
`graph_for_classification_fqid` (#792) is its classification analog — it resolves the
edition to its canonical live slug and roots the union on the edition's curated umbrella
group(s) (`classification_dimensions`, empty for the common ungrouped case → just the
edition's own succession chain), with `focus_id` on the resolved edition. The umbrella
member-union step is shared with `graph_for_classification_group` (one
`_add_classification_group_members` helper, not re-pasted). `graph_for_group` /
`graph_for_classification_group` root on the members directly with `focus_id=None`. A
member page therefore renders the **same** group union as the group page, with the
current node highlighted client-side (highlight is the renderer's, driven by
`focus_id`); the union is entry-independent and cacheable by group key. Group keys are
`(provider, register, key)` and `class/<key>`, **not** FQIDs: register groups resolve
via `concept_group`, classification umbrellas via the new thin
`classification_group(key)` accessor (a filter over `list_classification_groups()`). The
`graph` suffix is reserved in `RESERVED_HTTP_SUFFIX_SLUGS` (it shadows a variable leaf,
a register, and a classification slot, like `states`/`lineage`).

**Leaf `/graph` route dispatches both leaf kinds.** The webapp's
`GET /catalog/{fqid}/graph` serves **both** leaf kinds, dispatched on FQID kind (#792):
a binding (3-seg) → `graph_for_fqid` (incl. the #411 301-redirect for a dead/renamed
binding); a classification edition (2-seg) → `graph_for_classification_fqid`. This is
what lets #678 render the classification leaf through the same unified graph component
(the edition chain + umbrella cross-reference both arrive as graph content), retiring
the separate lineage / dimensions panels.

## Thematic tags (discovery overlay, #311)

Orthogonal to concept groups (which fold column families *structurally* within ONE
register), the **tag layer** is a maintainer-curated *thematic* vocabulary that cuts
*across* providers/registers — so a researcher can find "a measure of income" without
already knowing the register. ONE global vocabulary (`tag`, slug globally unique) + ONE
polymorphic membership table (`tag_member`): a row carries EXACTLY ONE grain — a
`register_id` (coarse thematic browse) OR a `variable_id` (the "golden/starred"
recommendation, where `starred` flags it and `note` carries the one-line rationale
curation can give and popularity can't). Curated from `reg_meta_build/tags.toml`,
derived every build (regenerate-not-migrate); a discovery overlay that leaves identity
untouched, same family as concept groups and delivery enrichment (package-root TOMLs).
The first committed content slice is SCB-heavy and intentionally small; synthetic builds
and wheel installs can still materialize empty tag tables when the curation file is
absent. The webapp consumes memberships as catalog-node chips; tag-scoped search/facets
and tag-backed search boost remain separate consumption work.

**API**: `Catalog.list_tags()` → `TagSummary` (slug, label, description, `member_count`,
`starred_count`) is the vocabulary with counts; `tags_for_variable(fqid)` /
`tags_for_register(fqid)` → `TagMembership` (the tag's slug/label + this membership's
`rank`/`starred`/`note`), ordered by rank then slug. `Catalog.resolve()` embeds those
memberships on resolved register and variable nodes so consumers do not reimplement the
reverse lookup. `ConceptGroupSummary.tags` aggregates member variable memberships, and
`tags_for_variable()` also inherits group-level tag slugs onto untagged siblings as
neutral memberships while direct variable memberships keep their rank/star/note. Callers
that narrow a group first may supply that member set so aggregation/inheritance follows
the narrowed surface; unscoped calls keep the full-catalog behavior. Build-side
derivation + dangling-reference fail-fast live in `reg_meta_build/tags.py` (see
`reg_meta_build/DESIGN.md`).

## Storage optimization

IDs stored as INTEGER (not TEXT). Tables with composite integer-only PKs use WITHOUT
ROWID. Value codes are deduplicated into `value_code` (with `UNIQUE(code, label)`);
`variable_state` → code membership is a content-addressed `value_set` /
`value_set_member` pair, where each distinct year-projected code list is stored once and
shared by every state that observes it. SCB's validity windows are applied at build time
(see "Value sets are year-projected"), eliminating the historical-union junction and the
per-item validity tables entirely. A pre-aggregated `code_variable_map` replaces large
secondary indexes for value search queries. The original 13 GB raw DB shrank to \~320 MB
through deduplication, integer keys, and year-projection.

## Documentation layer

Register documentation (parsed from SCB PDFs) is curated as Obsidian-compatible markdown
files under `reg_meta_build/docs/`, source-of-truth for maintainers, and indexed into a
separate SQLite database (`reg_meta_docs.db`) with its own `DOC_SCHEMA_VERSION`. The doc
DB contains the FTS5 markdown index plus curated, rehostable register-version related
documents. Docs are keyed to register and variable names, not numeric IDs, so doc
updates and main-DB updates are independent. Related-document rows expose metadata via
the catalog when the caller supplies the docs connection; the PDF BLOB stays behind the
docs query accessor so catalog browse payloads never inline binary content.

End users never see the markdown files. The doc DB is distributed as a GitHub Release
asset (`reg_meta_docs.db.zst`) parallel to the main DB asset, installed into the same
cache dir (`$XDG_DATA_HOME/reg_meta/`), and fetched by `reg-meta update` alongside the
main DB. Query commands (`search`, `get`, `resolve`, `docs/*`) refuse to run without the
doc DB — on first use the CLI offers to download both artifacts.

`reg-meta-build build-docs` is a maintainer-only command that rebuilds the doc DB from a
repo checkout of `reg_meta_build/docs/` before upload. Runtime never reads markdown —
`repo_docs_dir()` in `reg_meta_build.doc_db` is only consulted by `build-docs` when run
from a repo checkout, and is absent in installed wheels of `reg_meta`.

See [../reg_meta_build/docs/SCHEMA.md](../reg_meta_build/docs/SCHEMA.md) for the
markdown file format.

## What's not in the catalog

The universal model is deliberately lean. The catalog answers "what variables exist and
what shape they have"; two siblings answer the rest, and the boundary is a design
decision worth stating:

- **The doc DB** answers "how to understand them" — free-form prose and narrative
  metadata beyond the compact register/variant/version fields shipped in the catalog:
  quality narratives (SOS quality sheets), conceptual time-series breaks, long-form
  descriptions, legal text. When content drifts over a register's life beyond the
  structured `register_version` rows, the doc uses chronological Markdown sections.
- **The provenance DB** — a maintainer-only sibling SQLite artifact, not shipped to
  consumers — holds build artifacts: approval dates, workbook delivery metadata, source
  checksums, build manifests, and raw provider-side IDs not reused as universal IDs. Its
  build rationale lives in [../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md).

Localization is deferred (v2+): the catalog carries one canonical text per field (the
provider's native language), and the build drops SOS DCAT-AP `*_en` variants for now.

**Structural sensitivity flags stay in the catalog** as universal `variable` columns
(`is_sensitive`, `is_identifier`) — they are MONA-critical, apply to every variable
regardless of provider, and are inherently shared metadata (sensitivity is a property of
the variable, not of how a variant delivers it).

**`is_identifier` downstream semantics.** A variable with `is_identifier=true` will be
pseudonymized at delivery — SCB prefixes the column header with `LopNr_` (or a
project-specific prefix). The flag is **broad**: it covers not just the subject
identifier (`PersonNr`) but every related identity column (`PersonNrMor`, `PersonNrFar`,
`PersonNrSambo`, ...). It is distinct from the narrower "which identifier is the
*subject* of this variant?", which `variant.panel_entity_key` answers. Downstream
consumers (SPA authoring's default `display_name`, the validator's info-level
pseudonymization-prefix check, the future MONA runner's PII scanner) key off
`is_identifier`; only panel-default inheritance keys off `panel_entity_key`.

## Versioning and compatibility

Four independent version numbers:

  | Version                                   | Location                        | Purpose                      |
  | ----------------------------------------- | ------------------------------- | ---------------------------- |
  | Package version (`__version__`)           | `__init__.py`, `pyproject.toml` | Python package / CLI release |
  | Main schema version (`SCHEMA_VERSION`)    | `db.py`                         | Main-DB schema compatibility |
  | Doc schema version (`DOC_SCHEMA_VERSION`) | `doc_db.py`                     | Doc-DB schema compatibility  |
  | Contract version (`CONTRACT_VERSION`)     | `cli_common.py`                 | CLI output envelope format   |

**Schema version** uses semver. `open_db` compares the `import_manifest`'s
`schema_version` to the code's `SCHEMA_VERSION`: the major components must match and the
DB's minor must be `>=` the code's minor. A mismatch raises `schema_incompatible` (exit 10)
and directs the user to re-download the database. Patch differences are ignored.

Bumping rules:

- **Major bump** on breaking changes (renamed/removed tables or columns, changed column
  semantics that consumers must adapt to).
- **Minor bump** in either of these cases:
  1. Code starts reading a new column/table added in the build. This forces old DBs
     (that lack it) to be rejected cleanly at `open_db` instead of failing later with a
     SQL error.
  2. Build-time content semantics change in a way that should invalidate prior DBs even
     though no schema shape changed — e.g. dropping polluting rows from `value_code`,
     populating columns with NULL where they used to carry placeholder strings. Old DBs
     would silently serve pre-cleanup data; the bump forces a rebuild on the next
     `reg_meta      update`.

Either bump requires rebuilding and re-uploading the DB asset before the package release
goes live — see `.claude/skills/release/SKILL.md`. The `TestSchemaCompat` tests in
`reg_meta_build/tests/test_build_db.py` verify the guard.

### Release tags and distribution

The monorepo uses **per-package release tags**: `reg_meta/v0.5.0`,
`reg_meta_build/v0.1.0`, etc. Each tag corresponds to a GitHub release scoped to that
package.

  | Channel              | Trigger                                                     | What it distributes                       |
  | -------------------- | ----------------------------------------------------------- | ----------------------------------------- |
  | PyPI                 | `publish_reg_meta.yml` on `reg_meta/v*` release             | Python package (wheel + sdist)            |
  | PyPI                 | `publish_reg_meta_build.yml` on `reg_meta_build/v*` release | Builder package (wheel + sdist)           |
  | GitHub Release asset | Manual upload to the `reg_meta/v*` release                  | Pre-built main DB (`reg_meta.db.zst`)     |
  | GitHub Release asset | Manual upload to the `reg_meta/v*` release                  | Pre-built doc DB (`reg_meta_docs.db.zst`) |

Every published release carries **both** assets (self-contained releases). A release
only needs a **freshly built** main DB when `SCHEMA_VERSION` changes, and a fresh doc DB
when `DOC_SCHEMA_VERSION`, `reg_meta_build/docs/`, or
`reg_meta_build/related_documents.toml` content changes, or when a gitignored
related-document PDF seed under `reg_meta_build/input_data/SCB/docs/` was added,
replaced, or refetched — otherwise the release flow copies the prior release's asset
forward (`.claude/skills/release/SKILL.md` step 8). The invariant exists because the
container deploy pipeline resolves the newest `reg_meta/v*` release into a concrete
`reg-meta update --tag`, which fetches both assets from that single tag — an asset-less
release blocks every image deploy (#343). `resolve_latest_release()` still walks recent
releases backwards looking for each asset independently, keeping `latest`-mode updates
robust against historical asset-less releases. The publish workflow's smoke step
exercises `reg-meta update --force` before allowing PyPI publish, so a release that
breaks the walker (e.g. incompatible assets, or no resolvable asset at all) fails CI
instead of shipping.

The wheel contains Python source only. The markdown under `reg_meta_build/docs/` is
maintainer source-of-truth and is **not** bundled — end users receive the built doc DB
via `reg-meta update`.

Legacy bare `v*` tags (pre-0.6.0) are still recognized during the transition but new
releases must use the `reg_meta/v*` prefix.

**Update command**: `reg-meta update` is the single command that brings everything
current — it walks releases to find the latest main-DB and doc-DB assets, and (when
reg-meta was installed as a uv tool) also runs `uv tool upgrade reg-meta` to upgrade the
package itself. On a venv/editable install (e.g. the Docker bake) the self-upgrade is
skipped (`result["package"] = "skipped_not_uv_tool"`) and only the DB/doc assets are
fetched; the package is managed by whatever installed the venv. Already-current assets
are skipped (tracked via `.db_source` and `.docs_source` in the cache dir). A background
version checker runs once per week (cached in `~/.local/share/reg_meta/.update_check`)
and prints a hint on interactive runs when a newer release exists.

**Auto-download on first use**: query commands (`search`, `get`, `resolve`, `docs/*`)
prompt to download whichever artifacts are missing when invoked interactively, so users
don't need to know about `reg-meta update` on first install. Non-interactive invocations
fail with structured errors (`db_not_found`, `doc_db_not_found`) rather than silently
skipping.

### Package version format

Package versions follow `X.Y.Z` with two optional pre-release suffixes:

- `X.Y.Z` — final release
- `X.Y.ZaN` — alpha (e.g. `0.5.0a1`)
- `X.Y.Z.devN` — development build (e.g. `0.5.0.dev3`)

No other suffixes (beta, rc, post, epoch) are used. The update checker relies on this
format for version comparison.

## Exit codes

  | Code | Meaning                                        |
  | ---- | ---------------------------------------------- |
  | 0    | Success                                        |
  | 2    | Usage/argument error                           |
  | 10   | Configuration error (missing DB, bad encoding) |
  | 16   | Not found                                      |
  | 17   | No match with `--require-match`; blocked order |
  | 25   | Network error (`reg-meta update`)              |
  | 30   | Unexpected internal error                      |

## Determinism

- Stable ordering for repeated runs against the same database
- Stable JSON key ordering
- Deterministic bounded paging (`limit + 1`, opaque context-bound cursor, unique final
  identity tie-breaker)

## Security

- Metadata only — no microdata
- No credentials read or stored
- No outbound network requests (except `reg-meta update` and the weekly version check)

## Glossary and Swedish↔English crosswalk

Durable reference for the universal vocabulary. The normative shipped-entity definitions
live in the `reg_meta_build/db.py` DDL; this captures the cross-provider term meanings
and the column-rename pass that turned SCB's Swedish source columns into universal
English.

  | Term                    | Meaning                                                                                                                                                                                                                                                                                                                                                              |
  | ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | variable                | The addressable variable — provider's "define once" identity, the FQID target. Synthetic `variable_id` PK; identity `(provider, register, slug)`. Has 1..N states across variants and time.                                                                                                                                                                          |
  | variant (coordinate)    | A `register_variant` row (SCB `registervariant`, SOS `deldatamängd`): a delivery coordinate, not an identity level. Carried on `variable_state` and on `project_data` Sources. Browsed under its register; **not an FQID kind**.                                                                                                                                     |
  | variable state          | A `variable_state` row: per-delivery shape, carrying a variant coordinate, validity range, type/length/value-set/version-label. The canonical unit of resolution at a `(variant, period)`.                                                                                                                                                                           |
  | binding                 | A 3-segment FQID referencing a variable. Resolves to a `ResolvedVariable` (all states) or `list[VariableState]` (with period context).                                                                                                                                                                                                                               |
  | variable slug           | `variable.slug`: the register-unique, immutable FQID leaf. Triage splits get distinct slugs; grain/vintage folds keep one slug.                                                                                                                                                                                                                                      |
  | same_as                 | Symmetric cross-register / cross-provider equivalence between variables. Variable grain; curated only, no auto-derive.                                                                                                                                                                                                                                               |
  | related_to              | Retired in #800. The `variable_related_to` table is dropped (SCHEMA_VERSION 6.0.0). The non-foldable split sibling pairs (`code_vs_label_pair`, `import_bug_suspect`) are preserved in-build as `edge_siblings` to drive concept-group folding but are no longer persisted to any researcher-facing edge table. Thematic see-also links are deferred to tags (#311). |
  | classification          | A named versioned vocabulary (SUN2020, ICD10). Provider-independent; addressed via `class/<slug>` (vintage in slug).                                                                                                                                                                                                                                                 |
  | value_set               | A code list on a `variable_state`. Content-addressed (`member_hash`) for dedup; optional FK to `classification`. Never exposed via FQID.                                                                                                                                                                                                                             |
  | value_set_version_label | On `variable_state`: the discriminator that lets multiple value-set versions co-exist as overlapping states (folded crosswalk vintages / LKF multi-vintage). `NOT NULL DEFAULT ''`.                                                                                                                                                                                  |

**Universal English ↔ SCB Swedish.** Column names are universal English; column
**values** stay provider-native verbatim. The validator emits errors against strings;
resolution turns strings back into entities.

  | SCB Swedish                     | Universal English                         | Lives on                                                      |
  | ------------------------------- | ----------------------------------------- | ------------------------------------------------------------- |
  | registernamn                    | name                                      | register                                                      |
  | registersyfte                   | purpose                                   | register                                                      |
  | registervariantnamn             | name                                      | register_variant                                              |
  | registervariantbeskrivning      | description                               | register_variant                                              |
  | variabelnamn                    | name                                      | variable                                                      |
  | variabeldefinition              | definition                                | variable                                                      |
  | variabelbeskrivning             | description                               | variable                                                      |
  | variabeloperationell_definition | (merged into `description` when distinct) | variable                                                      |
  | variabelregister_kalla          | source_label                              | variable                                                      |
  | mattenhet                       | measurement_unit                          | variable (NULL when source was "Okänd")                       |
  | datatyp                         | data_type                                 | variable_state                                                |
  | datalangd                       | data_length                               | variable_state (TEXT — may carry precision/scale, e.g. `8,2`) |
  | vardemangdsversion              | value_set_version_label                   | variable_state                                                |
  | värdekod                        | code                                      | value_code                                                    |
  | värdebenämning                  | label                                     | value_code                                                    |
  | kolumnnamn                      | delivery_column_name                      | variable_alias / variable_state                               |
  | kanslig_variabel(_ibland)       | is_sensitive                              | variable (both source values fold into one flag)              |
  | identitetsvariabel              | is_identifier                             | variable                                                      |
  | version_forsta / version_sista  | valid_from / valid_to                     | variable_state (mapped to ISO 8601 at ingest)                 |

`registerrubrik` / `registervariantrubrik` are dropped (redundant with `name`);
`variabelreferenstid`, `variabelhamtadfran`, `variabelextern_kommentar` are dropped or
moved to docs. SCB `registerversionbeskrivning`, `registerversionmatinformation`,
`population*`, and `objekttyp*` ship as read-only metadata under a register variant;
approval dates stay provenance-only.

**Population and object type are metadata, not identities.** SCB's `populationnamn` /
`objekttypnamn` etc. land in shipped `population` / `object_type` tables under
`register_version`. They are nested on `VariantSummary.versions` for display, but they
are **not catalog entities** and have no FQID slot.

## Explored and ruled out

- **Direct API integration** against `mikrometadata.scb.se` — no stable public API.
  Session-bound WebSocket with no documented contract.
- **Browser automation** — fragile, unrepeatable. Manual CSV export is more reliable.
- **Query caching / user adaptation database** — deferred. Not needed yet.
