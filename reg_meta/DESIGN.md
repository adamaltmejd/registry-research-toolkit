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
- Text output prints at most three contextual hints on stderr. A hint that says the
  rendered table omits data outranks every advisory hint: "Table view truncated" first,
  then "Long values truncated", then command hints in the order they were added. The cap
  drops advisory hints, never a truncation notice.

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
  discovery surface. Catalog-scoped: a `--register` scope excludes it, and so does a
  `--years` scope — a classification carries no validity window, so a version filter
  can't confirm one in range (the vintage lives in the slug, not a comparable column).
  Both scopes turn the arm off before its candidate query. **Code-aware surfacing**
  (#393 item 5): a **code-shaped** query (digit + length ≥ 3, e.g. "C12", "F32") ALSO
  surfaces the classifications that CONTAIN a matching code — exact OR prefix on
  `value_code.code` joined through `classification_code` — so "find the classification
  for this code" works even with no NAME match. This arm uses the RAW query (not the FTS
  index), is catalog-scoped (excluded under `--register` and `--years`, like the name
  arm), dedupes against the name-FTS hits (a both-ways match is emitted once, as its
  name hit), and is ranked AFTER all name hits (a positive `fts_rank` base vs the name
  arm's negative bm25; exact-containing classifications first within the block). The
  `classification_code` JOIN inherently excludes context-less codes, so no separate
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
so `%` and `_` in the user query match literally rather than as wildcards. The arms
matching authored text (`varname` on `variable.name`, `datacolumn` on
`variable_alias.delivery_column_name`, and the concept-group `label`) compare through
`py_lower` on BOTH sides: SQLite's own LIKE case-insensitivity is ASCII-only, so `KÖN`
would otherwise miss the `Kön` that `kön` matches. That folds CASE only, not diacritics
— unlike the FTS side, `kon` still does not match `Kön` on those arms. Arms over ASCII
identifiers (the group's `group_key`, `value_code.code`) stay on SQLite's own LIKE. Each
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

## One spelling per delivery column

One delivery column can be spelled several ways across the catalog: `variable_state`
carries the state's own spelling, `variable_alias` / `variable_alias_window` the alias
history's (`fastigheter` windows IDVE, IdVe and idve over one column). They fold
together under `py_lower` — the rule the build validates
`variable_alias ⊇ state columns` with — so they are ONE column, and every reader that
has to NAME it takes the same representative from `catalog.representative_columns`:
**the state's own spelling where a state names the column, else the lowest by byte
order**. Lowest rather than first-seen, because the reads are unordered: a first-seen
rule would answer off whatever plan SQLite picked for them.

So a column's IDENTITY is its fold and its NAME is the representative.
`Catalog.register_column_coverage` (one key per folded column, over the twins' merged
window), `Catalog.register_variable_deliveries` (one delivery row per folded column) and
`queries.resolve` (`matched_column`) had three rules between them until Y-102 — the
state's spelling, Y-93's representative, and `MIN(variable_alias.delivery_column_name)`
— so one column could come back under three names.

Holding compiler comparisons use this exact fold to resolve the authored spelling once.
The compiled mapping stores the representative spelling of that delivery column — the
one `representative_columns` names it under — and runtime holding comparisons use it
without reader-side folds. The compiled contract below adds a presence gate: the
authored spelling must fold onto a delivery column the resolver emits for that
variable/variant (states plus participating alias windows); no match fails publication.
The resolver's general representative/alias behavior remains unchanged.

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
  "define once" identity. Holds the register-unique slug (the FQID leaf) and the shared
  facts (`name`, `definition`, `description`, `measurement_unit`, `is_sensitive`,
  `is_identifier`, source attribution). Checked delivery metadata retains differing
  source text at state grain; a varying common text stays NULL.
- **`variable_state`** — the **per-delivery shape**, a child of `variable`. A variable
  has 1..N states; each carries a **variant coordinate** and explicit delivery scope,
  plus the data type, length, value set, and version label. The **value set anchors
  state identity**: SCB's low-trust per-delivery `data_type`/`data_length` no longer
  split a state when a value set is present (a state can span several deliveries whose
  only difference was a type-string wobble; the displayed type is then the latest era's)
  — see reg_meta_build/DESIGN.md § "State-identity rule (#526)".

Dated states use `period_scope = "intervals"` and two ISO bounds; pooled delivery
retains its separate flag. A source-documented independent table uses
`period_scope = "year_independent"`, two NULL bounds and no pooled flag. It has no
calendar availability claim. Calendar-year selection excludes these states. Selection
uses an explicit concrete variant and the whole `_default` period token; inventory and
ordering retain that exact nonannual coordinate. Birth, migration or classification
dates do not provide a delivery range. One owner/variant cannot mix dated and
independent states, and independent code versions remain distinct.

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

**Classification and `source_label` placement.** Per-era classification books live in
`state_classification` (an era can carry multiple books or change code systems mid-life
— see "Classifications"), while the human-readable source attribution lives on
`variable.source_label` (cross-era constant). `variable_state` carries `state_id`,
`variable_id`, `register_variant_id`, `valid_from`/`valid_to`, `data_type`,
`data_length`, `delivery_column_name`, `value_set_id`, and `value_set_version_label`.
`state_classification` links every declared book to its owning state. It also carries
`pooled` (Y-202: 1 when the state spans a pooled multi-year edition range with no
explicit annual coverage — one marked state over the whole range, never inferred annual
availability inside it; 0 otherwise). It also carries nullable `provenance`: NULL for an
ordinary provider-documented interval, `errata:<class>\n<evidence>` for an SCB
correction, and optionally `steward:<label>` for steward-only rows. When an SCB
correction overlaps a provider-documented claim,
`errata:scoped-attributions\n<JSON array>` records pair each correction class and
evidence value with its exact `source_editions`. This preserves multiple attributions
without confusing disjoint editions or relabeling the enclosing documented window.
Correction-only overlaps use the same records under `errata:overlapping-attributions`. A
source-less span retained by the existing coalescer carries `inferred:resolution-gap`,
which attributes it to neither provider nor curator. Keeping this at state grain
prevents one corrected edition from relabeling a neighboring documented window.

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
The shared period-token grammar is `YYYY`, `YYYY-MM`, `YYYY-MM-DD`, `HTYYYY`/`VTYYYY`,
`LA<YYYY>` (the school year from July `YYYY` through June `YYYY+1`), `YYYY-Q[1-4]`, or
`YYYY-H[12]`. Time is data context, not identity.

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

Checked per-column coding windows handle simultaneous columns with the same native
version label but different supplied response domains. Each expanded column returns its
own value-set ID, summary and optionally loaded codes. Native version filtering uses the
expanded label; the literal delivery column distinguishes the representations. A missing
override never borrows the shared state's domain. The initial contract requires
unclassified complete finite domains and clears classification/conformance on those
projections. Existing shared coding and lazy code loading remain unchanged.

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

## Data warnings

Source limitations and explicit interpretation assumptions are catalog data.
`DataWarning` keeps the settled severity, readable summary and explanation, exact
diagnostic SHA-256, source references, acknowledgement, and supplied delivery bounds.
Its identity is the SHA-256 of its complete canonical content. Full technical diagnostic
text stays in the completed build ledger; it is not duplicated into public warning
responses. Authored assumption explanations retain their exact concise reason and
provenance.

`Catalog.data_warnings` accepts a register or binding FQID and optional period, variant,
and literal representation filters. Binding aliases use the same canonical owner as
other catalog reads. Unassigned register warnings remain visible with a delivery filter,
with their coordinates still unassigned. A warning is attached to a state only when
source references positively witness its delivery coordinates and applicable dates.
Unknown and year-independent states do not acquire dated warnings through an invented
interval.

Registers and resolved variables expose complete warnings. States expose `warning_ids`
referencing the applicable warnings, so long diagnostic text and source references are
not copied across annual states. `Catalog.data_warnings` resolves the complete records
for any selected delivery. The web API also provides `/api/catalog/{fqid}/data_warnings`
for project selection. Warnings inform interpretation; they do not change source values
or prevent selection.

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

**Narrow reads for consumers that don't want the codes.** The binding arm is
history-hydrating by construction: `resolve` builds every state, and each state costs a
value-set code-list query. Two entry points serve a consumer that reads identity and
state *metadata* only — project validation is the one that asks for them (see "Project
semantic validation (`semantic.py`)" below); other metadata-only readers still take the
default hydration. `variable_identity(fqid)` returns a `VariableIdentity` — canonical
FQID, `deprecated`, `replaced_by`, `via_same_as` — resolving through the same `same_as`
traversal and raising the same errors as `resolve`, but stopping before the states.
`resolve_at(..., with_codes=False)` runs the ordinary period/variant resolution and
skips only the two per-state CODE-LIST queries (`value_set` members and the conformance
report's nonconforming codes), leaving those two fields None; `value_set_id` still
carries the code-set identity. `resolve_binding(fqid, ...)` IS `resolve`'s binding arm,
public so the same two hydration keywords reach the period-less whole record. Both reuse
the canonical identity and window expansion — there is no second resolver — so a narrow
read's query work scales with the identities and states asked for, not with how many
codes they share.

**Presentation summaries and the reads that replace the members** (Y-46, 2026-09-08). A
consumer that RENDERS a coding needs two facts the members carry incidentally: how many
there are, and whether they are a dense integer run (an age or code-number coding that
reads as `0-110`, not as a 111-row table). `with_code_summary=True` — a second, explicit
keyword on `resolve_at` / `resolve_binding`, never implied by `with_codes=False` — fills
`VariableState.value_set_summary` with exactly those, plus the stored conformance
verdict/declaration/counts with an EMPTY `nonconforming_codes`.
`Catalog.value_set_summary` memoizes per distinct `value_set_id` for the Catalog's
lifetime, so a history whose 290 states share one coding pays for one membership scan
rather than 290. `Catalog` likewise memoizes the holdings delivery fusion behind
coverage and deliveries for its lifetime, and the delivery-column spelling lookup pins
`idx_variable_state_variable` by name (`INDEXED BY`). Without `sqlite_stat1` the planner
picks the register-variant index; builds now end with ANALYZE, and on an analyzed
artifact the planner picks this index unaided, but the hint stays until the latency
oracle is re-run on an analyzed artifact. `dense_integer_range` is the denseness test
itself: enough members, every code a distinct canonical decimal integer inside JS's
safe-integer range, every label restating its own code, and the values covering enough
of their own span — a set whose labels carry meaning stays a table.

The members themselves are then read explicitly and narrowly:
`Catalog.value_set_codes(value_set_id)` is the full membership of ONE coding (None for
an unknown id, distinguishing it from a coding with no members), and
`Catalog.state_nonconforming_codes(state_id, value_set_id, classification_slug=...)`
reads one owned coding's extensions against one declared book. `state_nonstandard_codes`
and `state_sentinel_codes` separate substantive extras from known missing/other markers;
`state_canonical_codes` returns only delivered source pairs whose literal codes occur in
that book. Sentinel members retain their stored meaning and exact scoped certificates.
Per-column requests must also supply `delivery_column_name` and `alias_window_from`;
wrong or incomplete ownership coordinates return None. `Catalog.resolve`,
`Catalog.states`, the `/states` export and `reg-meta get values` are untouched: their
default is still the complete record, members embedded.

The catalog return shapes — and, as of #701 (2026-06-23), the search return shapes in
`search.py` — are frozen Pydantic v2 models on a shared `_CatalogModel` base
(`BaseModel` with `frozen=True, populate_by_name=True, extra="forbid"`). This mirrors
`reg_schema`'s `_Model` shape but is a **separate** catalog base. The order materializer
uses the package's `reg_meta → reg_schema` dependency; catalog return models do not
inherit the project model base. Collection fields stay `tuple[...]`; Pydantic serializes
tuples to JSON arrays. `Fqid` stays a frozen `@dataclass` but carries
`__get_pydantic_core_schema__` so `fqid` fields validate from `str` or `Fqid` and
serialize to the canonical FQID string (OpenAPI `string`). Register-bearing models use
Python attr `register_name` with `Field(alias="register")` to avoid the
`BaseModel.register` shadow; wire/init name stays `register`. The earlier no-Pydantic
soft preference (import-ergonomics + aspirational Go/Rust port) is historical — #681
(2026-06-22) resolved that the port's real cross-impl contracts are the SQLite
`SCHEMA_VERSION` + `openapi.json` (both Pydantic-independent), so reg_meta adopted
Pydantic so FastAPI can consume its catalog models directly. The hard no-Pydantic rule
applied only to `reg_monabundle`'s amalgamated bundle (now archived); reg_meta was never
subject to it. See root CLAUDE.md "Stack" and ARCHITECTURE.md.

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
unbounded prefix; researchers reaching the ceiling must refine the broad query. Every
branch whose rows can carry an identity score (registers, variable names, delivery
columns, classification names and code containment, group labels) uses one fixed
1,001-row horizon, folded or not: every cursor sees the same complete bounded fold
universe, so a later sibling cannot turn an already-consumed leaf into a group or
succession row. The identity-promotion gate counts that same universe. It switches
exact/prefix promotion off when more than 50 rows match by identity, so generic exact
matches cannot swamp the ranked order, and the page size cannot change that decision
(`--no-fold` included). The cost is the default folded search's SQL bound. Only the
value branch, whose code rows carry no identity score, keeps an adaptive `limit + 1`
prefix and backfills when in-scope shaping consumes a page. Type/register/year/group
eligibility, classification-code exclusions, and the value surface's published
bm25-plus-mapping-count rank are applied inside SQL before each branch's bound. This is
the search-surface analog of the catalog-typing move (#681): the webapp's per-result
mapper functions and `models.py` search wrappers are deleted; the FastAPI response
models embed reg_meta's search types directly.

An optional cursor-bound `exclude_fqids` set removes register/classification identities
inside their SQL branches before the bound. Presentation layers use it when they inject
a curated identity separately: every continuation then shares one origin universe and
cannot emit the injected identity again at its natural FTS position.

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
Checked `column_metadata = "per_column"` windows additionally supply their own
`data_type`, `data_length`, `operational_definition`, `source_register_text`,
`definition`, `measurement_unit`, `name` and `description`. These literal facts can
differ between representations of one variable. They override the base even when null:
an unknown or absent column fact never borrows a sibling's value. Shared-mode windows
retain inherited metadata. Checked calendar-month families expose their literal monthly
definitions here; a varying common definition remains NULL at variable grain. Checked
source names and descriptions follow the same rule; a variable with no common name
requires positive names for every delivered state. Search indexes the retained delivery
text when the common text is NULL. Coding uses its separate window mode; state identity
remains shared. This does not assert interchangeable storage or statistical
comparability between source editions. A nullable window-level `provenance` uses the
same contract as `variable_state.provenance`: source-derived windows leave it NULL,
retain the existing replacement/participation semantics, and inherit the base state's
provenance. An exact-edition curated alias carries the correction class, evidence, and
source-edition scope; the reader adds it to the source result rather than making it
participate in replacement. This preserves the original base state, including its
operational definition, without a synthetic full-state window. A variable with no window
rows maps 1:1, byte-identically. The monthly merge is explicitly retained under
#518/#523; the retention rationale and the #523↔#496 two-layer boundary are recorded in
`reg_meta_build/DESIGN.md` → *Consumers: monthly column families*.

**`Period`** — `int | str | dict`, the polymorphic period `resolve_at` accepts: a bare
year (`2018`), a period token
(`"HT2020"`/`"LA2020"`/`"2020-Q3"`/`"2020-08"`/`"2018-12-31"`), an explicit range
`{"from", "to"}` (endpoints are int or token), or the `"_default"` snapshot sentinel (no
period filter). In a project source, `_default` instead selects only year-independent
states at an explicit concrete variant; it cannot mean every calendar year or an
unspecified period. Finite periods expand to an inclusive ISO `(lo, hi)` interval by
`_period_bounds` + `fqid.period_token_to_bounds`, intersected against the full-date
`variable_state` validity ranges — so sub-annual and range queries are precise, not
year-granular.

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
varies by state), `provenance` (NULL for an ordinary provider-export interval;
correction class and evidence for an errata-backed interval, plus exact source-edition
scope as paired JSON attribution records when corrections overlap a documented claim),
`pooled` (Y-202: True when the state spans a pooled multi-year edition range with no
explicit annual coverage — consumers must not infer annual availability inside the
window; False otherwise), `value_set_version_label` (NOT NULL, `''` = no discriminator),
`value_set_id`, `value_set` (hydrated `(code, label)` tuple, None when the state has no
value set), `is_identifier` (variable-grain flag denormalized onto every state via a
JOIN — constant across all of a variable's states — so consumers holding only a
`VariableState` (e.g. the `resolve_at` / `/states` paths) can read the authoritative
identifier flag without needing the enclosing `ResolvedVariable`), and `classifications`
(a tuple of `StateClassification` links, each carrying its book slug, display names,
provenance and optional state-local conformance). Multiple books remain separate. A
physical alias with per-column coding has its own links in
`alias_window_classification`; it never inherits a sibling's coding evidence.
`coding_window_from` retains that alias's original compound-key start even when its
displayed interval is clipped by a base state. The full delivery-column history —
multiple aliases per state from cross-edition spelling drift — lives in the
`variable_alias` table; `delivery_column_name` is its denormalized latest, and
`reg-meta get datacolumns` surfaces the complete list.

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

## Compiled holdings relations and read scope

**Compiled-holdings contract (2026-10-04).** The activated SQLite artifact is the sole
runtime source of physical holdings. Accepted inventory TOML is a builder input
contract, not a second runtime catalog. Reference metadata describes meaning and
validity; holdings describe possession; the existing `Catalog` resolver determines
semantic applicability at query time. No state/window resolution is compiled.

Four relations retain these facts:

- `holding_table`: one row per exact opaque physical identifier, with explicit `scope`
  (`intervals`, `year_independent`, `unknown`), original typed `edition_json`, optional
  table-level `partition`, input evidence locator `source_ref`, and an authored
  `retain_unknown_reason` only for unknown scope. Store the edition value read from TOML
  as JSON before year-integer normalization; preserve int/token/range/list distinctions,
  list order and endpoints. Comments remain in the pinned input. Unknown may have no
  edition declaration; preserve any supplied declaration without inventing one.
- `holding_period`: inclusive ISO `(lo, hi)` physical intervals, keyed by table and
  bounds. Interval scope has one or more sorted disjoint periods from `edition_bounds`;
  year-independent and unknown have none. Preserve holes, never expand one range/list
  table into annual tables or clip physical facts to metadata validity.
- `holding_column`: artifact-local key, table FK, literal case-preserving `name` and
  nullable authored `unmapped_reason`. Zero mappings retain physical possession without
  admission. An absent reason remains absent; a reason cannot accompany mappings.
- `holding_mapping`: physical-column FK, existing `variable_id` and `variant_id` FKs,
  required `representation_literal` and `representation_canonical`. Preserve the
  authored binding identity, including a literal same_as source; do not replace its ID
  with a donor, state or alias-window ID. `_default` resolves to a real variant row,
  never a wildcard. One column may map several variants or representations.

Exact physical IDs are unique; column names are unique per table; mapping canonical
triples are unique per column. Physical names use exact comparison. SQL enforces FKs,
local CHECKs and those UNIQUEs. Shared Python validation enforces same-register
variable/ variant ownership, nonempty table/column collections, edition/scope
correspondence, reason/mapping exclusivity, interval disjointness and cross-location
logical-cell ambiguity per partition. Extract the conflict logic of
`DeliveryInventory._check_one_to_one_resolution` into one shared pure placement
validator in `reg_meta.inventory`, rather than adding an overlap algorithm. The
inventory model passes authored representation spellings; the compiler passes canonical
spellings. Preserve variable/variant and scope grouping, exact physical locations,
interval overlap, partition separation and aggregated actionable errors. The compiler
also rejects duplicate canonical triples within a column with input locators before
insertion; SQL UNIQUE remains the final guard. Distinct explicit partitions are separate
claims; equal partitions or an unlabelled whole-population claim conflict on overlapping
cells. Year-independent claims retain their separate scope check.

The compiler folds each literal with Python `str.lower()` (`py_lower`), the exact shared
fold of `representative_columns`. No NFC, `casefold()` or SQLite `lower()`.
`Catalog.delivery_columns(variable_id, variant_id)` exposes the whole-history reference
delivery universe: states plus participating alias windows under the resolver's
containment, replacement, curated-addition and coding rules. It returns a frozen set of
representative spellings, independent of browse scope; optional `period_scope` narrows
it to dated or year-independent delivery. The compiler and inventory consistency gate
share this public method. It does not promise applicability to a physical edition. The
folded literal must name exactly one column in that universe; no match fails publication
with the coordinate and locator. `representation_canonical` uses the representative
spelling: a state's spelling where a state names the column, else the lowest alias
spelling. Store both literal and canonical spellings. Reader-side holding folds go; the
resolver's own alias/state identity rule remains shared domain behavior.

`Catalog._expand_state_windows` remains the resolution authority. Holdings coverage
batches states and physical periods and shares its alias-participation projection; it
does not construct full state/code/warning models just to aggregate delivery windows.
Per-column metadata or coding windows intersect successive base states; shared windows
must be contained. Source replacement requires participating base column and request
overlap, otherwise the base remains; curated shared-coding windows add independently.
Year-independent states remain year-independent. A join on canonical
`variable_state.delivery_column_name` is sufficient only where it preserves these rules;
aliases use the existing resolver. The inventory consistency gate uses the same public
delivery universe. The builder's inventory coverage flat union answers a different
accounting question. None of the three rules is copied into DDL. Range/list physical
periods survive; the build coverage assessment retains its existing "temporally
unassessed" disposition through `data_warning`, without partial-resolution rows or a
findings relation. Unknown tables retain census columns but no logical mappings.

Candidate indexes are `holding_mapping(variable_id, variant_id)`,
`holding_mapping(column_id)`, `holding_column(table_id)` and
`holding_period(table_id, lo, hi)`. PK/UNIQUE prefixes already serve the last three; the
writer chooses nonredundant indexes and final order from query plans.

### Artifact selection

Named `--catalog NAME` selects `<data>/NAME/`; global retains the existing data root.
`--catalog` and `--db DIRECTORY` are mutually exclusive, and `--db`/`REG_META_DB` retain
directory semantics (append `reg_meta.db`). Explicit flags win; without one,
`REG_META_DB` wins over the default global directory. A named steward selection must
match manifest `steward`; the default global directory and explicit `--catalog global`
require `catalog_artifact_kind = "catalog"`. Explicit `--db` or an effective
`REG_META_DB` accepts either publishable kind, derives kind/steward and default scope
from its manifest, and reports that identity; it imposes no directory-name steward
check. Project provenance and webapp boot checks still apply to that identity. Missing,
incompatible, incomplete or nonpublishable selection fails before serving, with no
fallback.

For a named steward, `update --catalog NAME` fetches `reg_meta_<NAME>.db.zst` and the
compatible shared reference-docs asset `reg_meta_docs.db.zst` into only that named
steward directory, as `reg_meta.db` and `reg_meta_docs.db`. There is no steward-specific
docs asset name. Global keeps `reg_meta.db.zst` plus the same docs asset in the existing
data root. Docs retain their independent version/update policy; each selected directory
owns its copy. No selection configuration file. Optional docs are the selected catalog's
sibling, never another installed catalog's companion. A missing docs file does not block
metadata/holdings/order reads; docs operations report `doc_db_not_found` with an update
action for the selected directory. Absence of steward-only document content is not
filled from another installed artifact. Updates through `--db` or effective
`REG_META_DB` require an existing admitted catalog and derive the asset name from its
kind/steward; the downloaded replacement must match that identity before activation. An
empty, incompatible or nonpublishable explicit directory fails before writes, with an
action to bootstrap through a named/default installation. Never replace a path-selected
steward artifact with a global asset or infer steward identity from its directory name.

### Scope predicate and public surfaces

A publishable `steward` artifact defaults to `holdings`; a `catalog` artifact defaults
to `reference`. Explicit holdings on catalog errors. Reference means every semantic node
in the selected generation, including steward-only metadata, not a separately downloaded
or newer global DB. Unknown scope and unmapped columns do not admit logical nodes.
Membership uses authored variable/variant identity and canonical representation; a
same_as reference edge never grants possession of a different binding.

  | Node kind              | Holdings predicate                                                                                                                                             | Reference                                                   |
  | ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
  | Provider               | At least one admitted descendant register.                                                                                                                     | All providers.                                              |
  | Register               | At least one admitted variable binding at a mapped variant.                                                                                                    | All registers and variants.                                 |
  | Variable binding/state | At least one explicit mapping under this register; a selected variant/representation must match that mapping. Resolver output is narrowed to physical periods. | Full semantic history, subject to explicit request filters. |

An interval holding contributes only the intersection of its exact periods with the
request and resolved applicability; year-independent holdings require compatible
year-independent resolver output, never artificial dates. Unknown never contributes.
Without a period filter, membership uses any applicable interval or year-independent
mapping; with one, it requires an overlap in that scope. Discovery may union admitted
variants for a binding, but variant-specific leaves, validation and order never do.
Coverage retains disjoint intervals, does not overwrite catalog validity and does not
promise orderability. Missing or ambiguous semantic applicability is still a resolver/
order finding, even when physical possession exists.

The same predicate governs root/provider/register/variable browsing, resolve results,
states, coverage/deliveries, register/variable search, groups, graphs, stats and logical
CLI exports. Apply it before counts, grouping and search pagination; no backfill loop or
per-request allow-list reconstruction. Search cursors bind to query, period, variant,
read scope and `generation_id`. A changed context rejects continuation. Unheld direct
links return 404 in holdings; any existing canonical redirect targets only an admitted
node. Reference scope permits inspecting the same node without making it selectable.
Logical lookups that fall back from var_id and name to a delivery-column alias
(`get varinfo`, `get values`) apply the predicate to that arm too: an unheld variable
named by its column header is not found (exit 16), exactly as under its canonical name.

Exceptions are explicit:

- Classifications, value sets/codes, documentation and lineage/reference edges stay
  reference in either read scope. A variable-owned suffix still requires an admitted
  owner in holdings; complete code exports retain full-membership semantics for that
  admitted owner. A held graph can show labelled reference neighbors; none become
  selectable from the edge. Group membership and subject coverage use the predicate for
  variable members; classification groups stay reference.
- Register `/data_warnings` in holdings requires an admitted register and filters
  variable-owned warnings to admitted bindings (and selected variant/period where
  supplied). Register-wide warnings remain visible. Reference scope keeps all warnings.
  Holding warnings reuse `data_warning`; unknown-scope evidence comes from holding rows,
  not duplicated inventory JSON witnesses.
- Project validation and order accept no browse scope override. Steward membership
  warnings keep `fqid_outside_steward_catalog` and
  `representation_outside_steward_catalog`, derived from SQL at the source's variant.
  Global logical fallback is keyed on artifact kind, not missing inventory.
- CLI accepts `--scope` on scoped discovery and logical exports: `search`, `resolve`,
  `get register`, `get schema`, `get availability`, `get varinfo`, `get values`,
  `get datacolumns`, `get diff`, `get coded-variables`, `get groups` and the variable
  enumeration of `get classification`. Defaults still follow the artifact; inherently
  reference code/docs/classification metadata reads remain reference. API catalog/search
  routes and `/api/stats` accept explicit scope; context reports identity/default scope.

### Inventory TOML authoring contract (`inventory.py`)

**Format (ratified 2026-08-31): the inventory is authored as TOML**, following the
repo's generated-`auto.toml`-plus-curated-overrides pattern. Humans touch it (explicitly
curated editions, comments carrying curation rationale), so a comment-capable format is
required. A `project_data.json` source is a logical selection; a steward's physical
delivery topology is this separate data.

Keep `version = 1`: requiring `ColumnMapping.representation` changes validation, not
accepted bytes (both accepted and legacy inventories are fully explicit). Changed input
bytes still require fresh acceptance. Frozen models reject unknown keys, malformed
FQIDs, empty input and duplicate physical names. The input shape remains
`table → column → zero-or-more mapping`; every mapping requires `register_variant`,
`variable` and nonempty literal `representation`. Every mapping is qualified by its
explicit representation. Runtime matching uses compiled canonical spelling and authored
IDs.

A physical table has an opaque exact `id`, explicit `edition`, `period_scope` defaulting
to `intervals`, optional `partition` and all literal columns. Finite editions use the
shared year/month/day/quarter/semester/school-year token, `{from,to}` range or sorted
nonoverlapping finite list grammar. `edition_bounds` is reused for physical periods.
Year-independent input explicitly sets `period_scope = "year_independent"` and the whole
edition `"_default"`; its mappings require concrete variants. `_default` is never a
finite or unknown-scope sentinel. Retained unknown tables come from accepted
`retain_unknown` policy plus the raw census, not invented finite inventory entries.

Partitions keep `validate_slug`'s lowercase, letter-initial, single-hyphen grammar;
`_default`, `class` and period-shaped values are rejected. They describe explicit shards
of delivery, not researcher-selectable catalog populations. Unmapped physical columns
retain their authored nonblank reason where supplied; no reason is synthesized.

### Holdings resolution invariants (ratified 2026-09-01)

**One-to-one resolution.** Every admitted
`(register_variant, variable, representation, period)` cell resolves to exactly **one**
physical `(table, column)`: the extraction tool never chooses between sources. Inventory
validation, and therefore the compiled steward artifact's build, **errors** whenever two
mappings could serve the same cell: same variant + variable, same canonical
representation and overlapping editions, whether across tables or across columns of one
table. This error is also the supersession worklist; the curation rules that resolve it
(current holdings only, supersession, sub-extract exclusions) are
`reg_meta_build/DESIGN.md` → "Holdings curation rules". Zero-column cells are not
errors; they are simply not admitted. Several mappings may still let one physical
table/column serve several register variants (the combined Utrikeshandel table), and
several tables may map the same logical coordinate over **disjoint** editions (the
ordinary annual series).

**Disjoint-partition arm.** Some registers arrive as several tables partitioned by
sub-population **within one edition**: survey strata (`ITftg_Mikro`/`ITftg_Stora`),
reporter streams (`Arb_`/`Soc_AGIIndivid`), administrative splits (`NDR_adults` over/
under 70), per-municipality deliveries (SÄBO). They are deliberately unified as **one**
user-facing variant: nothing semantic differs across the shards, and no researcher
should have to know delivery trivia to get the whole register. Partitions are therefore
an inventory/order-layer concept, never a catalog concept. The one-to-one invariant
holds per `(cell × partition)`: two tables mapping the same cell over overlapping
editions conflict **unless** they carry distinct partition labels (curated, never
inferred: `reg_meta_build/DESIGN.md` → "Holdings curation rules"). The materializer
matches every partition of a cell, and **extraction preserves delivery topology: what
goes in as two tables comes out as two files**, never a union (see "Order materializer
and manifest" below). Under the invariant, "one file per (ordered variant, edition
segment, partition)" and "one file per table" coincide; a combined table backing several
ordered variants still emits per ordered variant.

### Build-time consistency gate

The compiler is the standing mapping/ownership/accounting gate before publication. It
validates every authored triple against the exact new-schema base plus steward
extension, canonicalizes spelling and preserves physical facts. Accounting over accepted
inventory, policy, overlay and raw census must prove the disjoint union dated ∪
year-independent ∪ retained-unknown ∪ excluded ∪ lookup equals the full census at table
and physical-column grain. Counts and digests go into `import_manifest`; runtime
holdings contain only the first three dispositions. No exclusions, lookups or raw census
relation.

The former runtime consistency checker and committed legacy webapp inventory are
removed. Build validation owns mapping consistency. Runtime boot checks artifact schema,
completeness, publishability and steward identity, opens SQLite read-only and never
rebuilds an inventory index. Build failures remain actionable and deterministic;
diagnostic output never activates as a catalog.

## Order materializer and manifest (`order.py`)

**Compiled-holdings contract (2026-10-04).** Ordering reads physical facts from the
selected artifact and records its generation identity.

`materialize_order(project, conn)` is the one place a logical `project_data.json`
selection meets a steward's physical delivery topology. It returns either a complete
`OrderManifest` or a non-empty set of `OrderFinding`s — never a partial order. The
compiled contract above owns the facts. The FastAPI endpoint and the CLI/plugin are thin
adapters over this one function, which is what makes their results byte-identical; all
logic (and all fail-closing) lives here. There is one common manifest and no per-steward
export template (2026-07-14, #1137). Why this domain code lives in `reg_meta` rather
than a new package is `ARCHITECTURE.md` → "Why this split".

**Artifact-driven materialization.** `import_manifest.catalog_artifact_kind = "catalog"`
selects the **global-deployment fallback**: it has no physical delivery topology, so
canonical resolution alone grounds the logical order. A `steward` artifact always uses
its compiled holdings, even when browsing reference scope. Missing holdings never
selects fallback. It is the SAME function and the same pipeline — only step 3's matching
arm differs — so there is no second clip/slice/coverage implementation to drift.
`OrderProvenance.mode` (`steward_holdings` \| `global_fallback`) names which one
produced a manifest, and the provenance gate treats the global deployment like any
other: `ProjectData.steward` must equal `"global"` (`order.GLOBAL_STEWARD`).

Per `sources[*].bindings[*]`, in project declaration order:

1. **Availability clip first (intersection semantics, ratified 2026-08-31).** A source
   period means "these columns, wherever each is available inside this window". This
   resolves the variable-by-period matrix (dogfood 2026-08-30 P0.4) without a schema
   change: one source per variant, no cross-product over-order. Each binding is clipped
   to its own availability — the union of its `variable_state` windows at the source's
   variant, via `Catalog.resolve_at` — so a column first delivered in 2019 under a
   2018–2020 source does not widen the order into a cross-product. Every clip is
   reported as a `ClipReport` on the manifest: informational, never silent, never an
   error — and recorded BEFORE the ambiguity gate can return, so a binding that is both
   clipped and ambiguous surfaces both. This is also the seam a deferred per-binding
   period override would narrow (see "Deferred (2026-08-31): per-binding period
   override" below); no schema change was needed.
2. **Representation slicing.** The clipped request is partitioned into slices of
   constant canonical representation (`delivery_column_name`). A sequential rename fans
   out into two slices; two columns valid at the SAME instant with no
   `Binding.representation` pin is ambiguity and blocks. Resolution logic is not
   re-derived here — `resolve_at` is the source, called once per REQUESTED SEGMENT. Its
   monthly-family fallback (#319 — a month with no column window keeps the annual claim)
   is decided per query, so resolving a disjoint request as one outer span would let one
   segment's window suppress another segment's fallback and drop coverage the steward
   does deliver. Segments union on the compound window identity
   `(state_id, delivery column, valid_from)`, so a state reaching two segments stays one
   state. Steps 1 and 2 together are one function, `resolve_binding`, and it is
   **shared**: project validation (`semantic.py`, below) calls it and only translates
   its facts into issues, so the two never disagree about what is available and a clip
   alone is never a validation error. A clip is not a clean bill of health, though — the
   same binding can still block on representation ambiguity, and validation consumes
   availability and slicing ONLY: steps 3 and 4 are the steward's physical topology and
   do not run there, so a clean validation is a resolvable project, never a proof of
   physical order readiness.
3. **Steward matching + coverage gate.** A table matches a slice only when one of its
   columns carries a mapping matching `(register_variant, variable, representation)` AND
   its physical edition overlaps THAT slice; only the intersection contributes. Matching
   reads compiled IDs and canonical spelling, never loose inventory TOML. All mappings
   are explicit. A finite holding edition that overlaps the original request but lies
   wholly outside the binding's applicable column windows blocks with a located
   `column_window_unavailable`, even if availability clipping removes that edition.
   Semantic resolution findings are retained alongside physical findings. Any subperiod
   of the availability-clipped request left uncovered blocks the WHOLE order with the
   exact gap (`coverage_gap`), and a slice no mapping serves blocks with
   `mapping_missing`. Overlap alone never buys a partial manifest. The materializer
   never CHOOSES between tables and needs no chooser: the one-to-one resolution
   invariant ("Holdings resolution invariants" above) means a valid inventory offers at
   most one `(table, column)` per cell instant **per partition**, so the several
   contributions one slice can collect are either disjoint pieces of it (the annual
   series) or distinct partitions of it (the sub-population split), and both are wanted
   whole. Coverage needs no partition arm of its own: contributions already union across
   matching tables, and partitions are simply more matching tables. In global-fallback
   mode the slice's own canonical column serves it under a blank table, so the slice
   covers itself exactly and the same gate runs unchanged — what canonical resolution
   did not deliver has already blocked upstream as an unresolved, unavailable or
   ambiguous binding.
4. **Emission.** Every matching table is emitted whole, **every partition included** —
   v1 has no table chooser, no population field and no row filter (see the row-filter
   `simplify:` below). A matching multi-period table is ordered whole, even when its
   matched slice covers only a subset of the table's edition. Steward entries carry the
   literal physical `table` and physical `column`: the canonical `representation` is a
   join discriminator, not an output substitute, and `display_name` is not a delivery
   coordinate. Entries preserve project source/binding order; the fan-out inside a
   binding sorts by table, canonical edition, then physical column. A partitioned
   table's entry carries its label on the physical coordinate, so extraction preserves
   delivery topology: what goes in as two tables comes out as two files.

**Fail-closed, one pass.** A blocking result enumerates every finding across every
binding — `steward_mismatch`, `project_empty`, `period_not_orderable`,
`variable_unresolved`, `binding_unavailable`, `representation_unknown`,
`representation_unresolved`, `representation_ambiguous`, `mapping_missing`,
`coverage_gap`, `column_window_unavailable` — so a researcher fixes the whole order in
one edit instead of one gap per round trip. `ProjectData.steward` must equal the
deployment's steward — the manifest's, or `"global"` in fallback mode (provenance is
checked before anything resolves; retargeting is deliberately not a feature: a user who
intends to change provenance edits the JSON and uploads it again, and an upload to a
steward deployment is always validated against that deployment's compiled holdings) —
and an empty project stays a valid draft that cannot produce a header-only manifest.
Missing, unresolved or ambiguous logical-to-physical mappings are blocking findings,
never best-effort labels.

**The manifest is a versioned JSON contract (ratified 2026-08-31, replacing the earlier
nine-column CSV decision).** A human-readable table rendering may exist as a derived
view for the executing data manager; the JSON is the contract. Version 1 is **in
definition** while it has no external consumer: shape changes stay within version 1
rather than churning the number (operator decision, Y-19/1 review). Bump discipline — an
incompatible change bumps `ORDER_MANIFEST_VERSION`, pre-v1 changed-not-migrated — binds
from the first external reader (the steward-side extract system). `OrderManifest`
(version `ORDER_MANIFEST_VERSION`) carries provenance (mode, steward, project name /
schema version / declared reg_meta version / SHA-256 of the project's canonical JSON,
plus the catalog DB's `schema_version` and `generation_id`), the resolved entries —
logical coordinate (`provider,register,variant,variable` + the canonical
representation), the availability-clipped `requested_period`, and the physical
coordinate (`edition`,`table`,`column`, plus the table's `partition` label when it has
one) — and the informational clips. `partition` is the manifest's only optional field:
an absent key IS the "no partition" spelling (`to_json` serializes with `exclude_none`),
so an inventory that uses no partitions produces exactly the bytes it did before the arm
existed, and any future optional field must accept the same reading. This shape change
stays within version 1 under the in-definition rule above. It is machine-written here
and machine-read offline by the steward-side extract system, so it is self-contained: no
network, no catalog lookup at extract time. Both boundaries validate against the same
frozen `extra="forbid"` models. `to_json()` is the canonical serialization (sorted keys,
stable entry order, trailing newline); repeated runs over the same inputs are
byte-identical. Periods render through the shared grammar's inverse
(`period_token_for_bounds`), so a manifest speaks the same period spelling as a project
period and an inventory edition. `extraction_filenames(entry)` pins the output-naming
rule — one UTF-8 CSV per variant + partition + period unit, in **slug spelling** derived
from the manifest entry (`lisa_individer-15plus_2019.csv`) — in the contract rather than
leaving it to the extractor. A multi-period range segment renders `lo..hi` and extracts
whole as one file: v1 has no row filter, so a range is never split per year. Steward
display casing is not carried in the manifest (decided 2026-08-31, A-28; re-add it only
if a steward-side consumer concretely needs display-cased filenames). A partitioned
entry inserts its label after the variant slug
(`agi_individuppgifter-agi_arb_2021-03.csv`), which is what keeps two partitions of one
(variant, edition segment) from colliding into one file. They are extracted separately
rather than unioned because shard identity (reporter stream, municipality) may not exist
as a column, so a union would destroy information; an identity-redundant split merely
costs the researcher one concatenation they can always do themselves.

Pure domain code: no FastAPI, no filesystem writes, no timestamps (the only time-shaped
manifest values come from the DB manifest and the project). A global-fallback entry is
the same shape with a blank `table`, the canonical column in `column`, and
`edition = requested_period`, so `extraction_filenames` gives it one file per requested
period segment without a special case.

**Deferred (2026-08-31): per-binding period override.** No schema change now:
intersection semantics cover every observed case. If a researcher ever deliberately
wants *less* than the availability intersection, the shape is a binding-level period
override narrowing below the variant-level source period independent of availability
(e.g. source LISA 2000–2020 but `DispInk09` only 2000–2002 even though it exists later
too). File it when someone actually asks; the availability clip leaves the seam (an
override is just a further clip).

`simplify:` v1 records no row filter and includes the whole matching table. Add
table-specific period predicates when steward delivery/extraction consumes the manifest.
SWECOV's one-large-SQL-table-per-SoS-register delivery is the known upgrade trigger; it
will need period-column `WHERE` clauses later.

**The adapter door.** The rule that both product surfaces emit byte-identical results
holds only if the adapters are genuinely thin, so the two things they would otherwise
each re-type live here too, beside the materializer:

- `parse_project(data)` is the ONE read boundary for untrusted project bytes, used by
  the CLI's `read_project(path)` and the FastAPI body reader alike. It refuses a
  malformed document with `RegMetaError` (`project_unreadable`, `EXIT_CONFIG`). The
  refusal classes are:
  - bytes that are not strict UTF-8 (RFC 8259 §8.1). A byte-order mark or UTF-16/32 is
    refused, although `json.loads(bytes)` would sniff and accept it.
  - text that is not JSON.
  - a duplicate key at any depth. Last-wins would silently validate or order the wrong
    value.
  - nesting past the recursion limit.
  - a non-object top level.

  The adapters map that one refusal onto their transports rather than serializing it
  byte-identically: the CLI writes its error envelope and exits 10, and HTTP answers 400
  with `detail` equal to the error's `message`. The conformance corpus has one case per
  refusal class, each run through `validate` and `order` on both adapters, and pins the
  two messages equal. That parity is why a parse message never names the file path. The
  CLI names the path only when the file cannot be read at all (`OSError`), a failure
  that has no HTTP counterpart.

- `project_from_raw(raw)` (and `load_project(path)`, the CLI's file-reading wrapper) is
  the ONE door into `materialize_order` for an untrusted `project_data.json`. The
  `ProjectData` model enforces field TYPES only, so it runs `reg_schema`'s
  `validate_structural` first — without that gate a model-valid but structurally invalid
  spec (a malformed `register_variant`, a bad period token) would materialize a bad
  provider order. An invalid spec raises `RegMetaError` (`project_invalid` /
  `project_unreadable`, `EXIT_CONFIG`), so both adapters reject the same specs with the
  same words.

- `schema_version_issue(raw)` is the door's FIRST question and the one piece of it
  shared beyond ordering: is this project written for the contract this build reads?
  Supported is EXACTLY `SUPPORTED_SCHEMA_VERSION` — `reg_schema.__version__` itself,
  never a second spelling. There is no acceptance window to widen it with: reg_schema
  delegates the decision to this consumer and specifies no compatible range, and
  `_check_schema_compat`'s major/minor rule governs a DIFFERENT contract (the catalog DB
  this code opens, not the document a researcher authored). Anything else — an old
  major, a newer minor, a different patch, an unparseable string — is one
  `unsupported_schema_version` error naming the claim and the contract, raised
  (`EXIT_CONFIG`) before any layer interprets the document as the current schema, so a
  foreign project is never answered with structural noise or a manifest. Nothing is
  migrated or reinterpreted; pre-v1 carries no migration path. An ABSENT or non-string
  `schema_version` is deliberately left to `validate_structural`'s precise
  `missing_required_field` / `invalid_field_type` — that is a malformed document, not a
  version claim. Project validation (`semantic.validate_project`, below) reuses this
  same function for its diagnostic (it needs the `ValidationIssue`, not the raise), so
  every SERVER-SIDE consumer of a raw project gives one answer. (The SPA's own open-time
  gate is a separate, partial pre-flight on the file a researcher picks, not this
  decision — see reg_webapp/DESIGN.md → "Project-file version gate"; the backend stays
  canonical.)

- `blocked_message(result)` renders every blocking finding, in the materializer's own
  accumulation order, each prefixed with the source/variable/period it names. The
  fail-closed path is byte-identical across adapters too, not just a produced manifest.
  It is a PRESENTATION of `OrderResult.findings`, never the record of them: the findings
  are already typed models, and an adapter whose transport can carry structure carries
  the models (the webapp's 422 body does — see `reg_webapp/DESIGN.md` → The order
  manifest), with this line as the human summary beside them.

The adapters themselves are `reg_webapp`'s `POST /api/project/order` (see
`reg_webapp/DESIGN.md` → Project-write surface) and `reg-meta order <project.json>`,
which writes `to_json()` verbatim to stdout or `--output` — never through the CLI
envelope or `--format`, because canonical serialization IS the artifact. `--inventory`
is deleted. Both adapters select physical steward order or global logical fallback from
the opened artifact's manifest. `ORDER_MANIFEST_VERSION` stays 1 under the in-definition
rule. Generation provenance intentionally changes the envelope; compare unchanged
normalized entries, clips and finding order byte-for-byte, not old whole-manifest bytes.

**Deferred (ratified 2026-09-02): companion tables.** Steward deliveries carry
reference/crosswalk tables that are useful — sometimes essential — beside certain
registers but are not register data: value/label lookups (VaraText beside IVP, the
klartext code lists), coding-scheme crosswalks (skolkod↔skolenhetskod), and
pseudonym-bearing linkage tables (the FEK `PeOrgNr`↔`FENr` key tables, the person-level
`LopNrByte` key-change table). Today the SWECOV generator lookup-skips them; read that
state as *awaiting this mechanism*, not as a decision to exclude. The settled design,
built when the first real extraction needs a companion file:

- **Two mechanisms, split by cardinality and kind.** A small closed code set is CATALOG
  work — mint it as a value set/classification (browsable, versioned, exported by the
  existing complete-code export); its MONA table copy needs no ordering surface. A large
  lookup (university course-name lists run to several 100k rows — a dataset, not a value
  set) or any crosswalk/key table is a PHYSICAL COMPANION: a table-level
  `companion_of = "<provider>/<register>"` in the delivery inventory. No mappings, no
  `(variant, variable, period)` coordinates, no researcher choice — any order touching
  that register ships its companion tables as extra output files automatically.
- **Sensitive vs public is an access fact, not a mechanism fact.** A pseudonym-bearing
  crosswalk is linkage microdata only the steward can deliver, so it must be a
  companion; a public reference table may be one purely for delivery convenience.
- **Contract implication.** A companion manifest entry has no logical coordinate, so
  this is a manifest-shape change (version 1 is still in definition — see above) plus a
  materializer arm, one lane's worth.
- **Open question, decide at build time:** `LopNrByte` companions the *steward* (every
  person-keyed order may need it), not any one register — and making it orderable at all
  may deserve a deliberate access decision rather than an automatic ride-along. The
  FEK_JE and SaBo key tables are parked behind this mechanism (deliberately NOT minted
  as flavor linkage variants).

## Project semantic validation (`semantic.py`)

The §6.8.3 reg_meta-backed validation layer is shared `reg_meta` project code beside the
order materializer: the materializer and semantic resolution are shared `reg_meta`
domain code, with thin FastAPI and CLI/plugin adapters (see "Order materializer and
manifest"). It cannot live in `reg_schema`: `reg_schema` stays reg_meta-free and cannot
resolve against a live DB, and lists these codes as defined-but-not-emitted on its own
surface. The dependency direction is `reg_meta → reg_schema`, never the reverse.
`validate_semantic(project, catalog)` emits the same frozen `reg_schema.ValidationIssue`
shape as the structural layer, takes a `Catalog`, and leaves connection ownership to its
caller.

**The adapter door.** Like `order.py`, the module owns everything the adapters would
otherwise each re-type, so they stay thin and byte-identical:

- `validate_project(raw, connect)` is the §6.8.0 composition over a raw
  `project_data.json`: the supported-version decision (`order.schema_version_issue`,
  returned ALONE — the layers under it read the document as the current contract, which
  is the claim it just rejected) → `reg_schema.validate_structural` → `ProjectData`
  model construction → `validate_semantic`, with the issues of every layer that ran
  concatenated in that order. A failing project is a result, never a raise: this is a
  diagnostic. A structural failure skips the semantic layer. A residual model-
  construction failure (a constraint structural did not replicate, effectively
  unreachable) is one defensive `invalid_field` issue, never a crash. `connect` is the
  adapter's opener for the selected artifact, called only once the DB-free layers have
  passed, so a rejected body costs no DB hit; the adapter decides HOW it opens (the
  webapp's thread-confined per-request open, the CLI's catalog selection).
- `validation_json(result)` is the canonical serialization both adapters emit VERBATIM:
  `{issues, ok}` with sorted keys, two-space indent, UTF-8 and a trailing newline (the
  `OrderManifest.to_json` conventions). An absent `successor_fqid` is an explicit
  `null`, the wire shape the SPA's codegen'd type reads.

The adapters are `reg_webapp`'s `POST /api/project/validate` (HTTP 200 for any diagnosed
project, 4xx only for a malformed request; see `reg_webapp/DESIGN.md` → Project-write
surface) and `reg-meta validate <project.json>`, which writes the same bytes to stdout
or `--output` and exits 0 with no error-level issue, 17 with one (the blocked-order
code; the findings are written either way) and 10 for an unreadable file. The
conformance corpus runs every `cases/validate` case through both and compares the bytes;
the sampled artifact agreement does the same on admitted real artifacts.

Rules, walking each source's `register_variant` + every binding:

- The `register_variant` coordinate resolves to a known variant; the binding `variable`
  (3-segment FQID) resolves to a known variable (following `same_as` links —
  `Catalog.variable_identity` does that). Unresolved → `fqid_unresolved` (error).
- **Availability is not decided here.** The source period is expanded
  (`order.requested_intervals`) and each binding resolved by the SHARED reg_meta pass
  the order materializer runs (`order.resolve_binding` — steps 1+2 in "Order
  materializer and manifest" above); this layer only translates those facts into issues,
  so the two never disagree about what is available. Intersection semantics apply: a
  binding is requested wherever it IS available inside the source window, so narrower
  availability — a leading/trailing shortfall, an **internal** gap, or a pinned
  `representation` that covers only part of the request — is an informational clip
  (`range_period_partially_covered`, naming the period actually ordered), never an error
  by itself. A clip is not a clean bill of health: the same binding can still block on
  representation/value-set ambiguity here, and on the steward's coverage gate at order
  time. A **#307 list period** (interrupted series; structurally sorted + disjoint, wire
  form comma-joined — `2005..2010,2015..2020`) is one request with holes, not a series
  of independent ones: it reports ONE clip for the whole request, and its holes are
  genuinely absent from the question — a request that skips a year is NOT equivalent to
  the range enclosing it, since a column co-existing only inside a hole is not ambiguity
  and a column delivered only inside a hole is not availability. Availability empty
  across the whole request still blocks. The pass's blocking findings map onto this
  surface's codes — `variable_unresolved` → `fqid_unresolved`, `binding_unavailable` →
  `period_outside_state_validity`, `representation_unknown` →
  `binding_representation_unknown`, `representation_ambiguous` →
  `binding_value_set_version_ambiguous`, and `representation_unresolved` under its own
  name. The kept states still carry the request instants they are available for, so the
  per-instant probes (the co-delivered-value-set backstop) keep segment precision
  without re-deriving the clip, and `binding_state_drifts_within_period` (info) reports
  a request spanning a sequential state transition. Steps 3+4 of the materializer (the
  steward's physical topology and its coverage gate) do NOT run here: a clean validation
  is a resolvable project, never a proof of physical order readiness.
- Resolved variable metadata can emit non-blocking hints. `deprecated_traversal` (info)
  fires when the binding resolves to a variable marked deprecated; the binding remains
  valid. `variable_replaced` (info) fires when a `variable_replaced_by` edge is
  effective at or before the requested period and carries `successor_fqid` when the
  successor resolves to a binding FQID.
- The binding's `value_set` (a `class/<slug>` FQID) resolves to a known classification →
  else `value_set_missing` (error).

**This layer reads identity and state metadata, never code membership.** So it takes the
narrow reg_meta reads (see "Catalog API surface" above): `Catalog.variable_identity` for
the FQID and its replacement hints, and (inside the shared pass)
`resolve_at(..., with_codes=False)` for the states. The full `resolve` would hydrate
every historical state's code list to answer a question about one period — on a
geography variable whose yearly states share one large code list that is most of the
request. Diagnostics are unchanged: aliases, expanded monthly windows, representation
identity (`state_id`, `delivery_column_name`, `valid_from`) and code-set identity
(`value_set_id`) all come from the same code path. The conformance suite pins the rule
by tracing every statement validation issues over the readable semantic fixture and
refusing any read of a code-list table (`value_set_member`, `value_code`,
`classification_code`, `classification_conformance_code`).

**Representation, not `@version`.** A FQID names one concept, but a concept may carry
several **co-existing delivery columns** at the same instant — parallel representations
(SSYK 3/4/5-digit, age brackets). When ≥2 distinct delivery columns co-exist
(overlapping windows inside the REQUESTED instants — the shared pass's test, so a
sibling that overlaps only in a hole of a list period is not co-existence) and the
binding sets no `representation`, the extract would pull more than one column →
`binding_value_set_version_ambiguous` (error); the author must pick one via
`Binding.representation` (the delivery column name; the SPA offers a chooser). This is
exactly the job the retired `@version` pin used to do, now keyed on the delivery column.
A `representation` reg_meta no longer delivers as a column →
`binding_representation_unknown` (error). Crucially, the co-existence test keys on
**overlapping** windows: distinct columns in *non*-overlapping windows are a sequential
rename (drift), NOT ambiguity, and must not demand a `representation`.

A separate defensive backstop (`binding_value_set_version_ambiguous` when two kept
states with different `value_set_id`s are available at one requested instant) is
unreachable from any admitted artifact, so it has no conformance case. Co-existing
columns block as ambiguity before it runs, so any two states it sees overlapping inside
the request share one column, and two build invariants close that:

- `overlapping_distinct_value_sets` (`_check_one_value_set_per_period`): no two
  overlapping states of one column carry distinct non-null value sets.
  `validate_built_db` runs it on every build, flavored included.
- `overlapping_codeless_codebearing_states` (`_check_no_codeless_codebearing_overlap`):
  no code-less (`NULL`) state overlaps a code-bearing one on one column. The backstop
  compares `value_set_id`s with `!=`, so this NULL-vs-non-NULL pair would also fire it.
  The check runs on global builds and is SKIPPED on flavored (`extend-db`) builds. That
  skip opens no path: the flavored base is a released global DB whose own build ran the
  check, and the steward overlay can add no such pair. It inserts only states it mints
  for its own steward-provider variables, never for a base variable, and every one is
  code-less (steward extensions cannot declare value sets). Two code-less states compare
  equal, so they never fire the backstop.

If a steward overlay ever gains value sets or states on base variables, the flavored
skip must be revisited with it.

**Onboarding.** Stewards declare a subset of what reg_meta knows; data without an FQID
can't be authored (no `{display_name + type, no FQID}` escape hatch in v1). New
variables/registers/classifications onboard via slug-TOML PRs against `reg_meta_build`;
the steward authors accepted builder inventory and publishes a new compiled generation.

**Steward catalog filtering — `fqid_outside_steward_catalog` /
`representation_outside_steward_catalog`.** When a researcher's project references a
binding outside the loaded steward catalog, the column-based admission check (#206)
emits one of two **warnings** (not errors): `fqid_outside_steward_catalog` when the
steward holds *no* column of the concept, and the distinct
`representation_outside_steward_catalog` when the steward holds the concept but not the
column the binding **resolves** to — its message enumerates what the steward *does* hold
("available there as 'Ssyk1' only" is the actionable form of "not available"). These are
warnings during editing so an uploaded project can be inspected, but ordering does not
consume them: the materializer runs its fail-closed compiled-holdings/resolution gate
and blocks these conditions with its own findings, so a steward-catalog warning never
silently becomes an order. There is no cross- steward preview, retarget, or one-click
mutation feature: the active deployment is the validation target, and the user edits and
re-uploads the JSON if they intend to change it. The warning codes remain; the
implementation reads compiled SQL mappings at the source's exact variant. It runs after
shared period/representation resolution. `Holdings.binding_ids` and `Holdings.columns`
distinguish an unheld binding or variant from a held binding with an unheld
representation. Canonical column lookup handles spelling without granting a sibling
variant or representation. If resolution is indeterminate, its existing error remains
and only the binding-level admission check runs. Catalog artifacts emit neither steward
warning. An unresolved source variant skips the probe. Admission uses the literal
authored FQID; a same-as relationship is reference evidence and grants no physical
membership. Physical period gaps remain the order materializer's responsibility.

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

The `classification_id` column is populated at build time from maintainer-curated TOML,
one file per classification under `reg_meta_build/curation/classifications/` (exact
match against `value_set_version_label`, no fuzzy inference). The file layout and loader
rules live in
[../reg_meta_build/CLASSIFICATIONS.md](../reg_meta_build/CLASSIFICATIONS.md) §
"Classification files".

### Canonical codes and state conformance

`classification_code` is the classification's definition. For CSV-backed classifications
it contains only published canonical codes; observed value-set codes that merely show up
in data are not attached to the classification page.

Canonical codes come from required per-classification CSVs ingested by the build (see
[../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md) § "Canonical code CSVs").
Fresh builds emit only canonical `classification_code.is_valid = 1` rows and cache
`classification.valid_code_count` for that canonical count.

The build records declared value-set mismatches at state grain in
`classification_conformance` / `classification_conformance_code`. A known
source-declared classification remains linked regardless of overlap. `conforming` means
all delivered codes occur in its official book; `extended` means the source also
supplies source extensions. Each book's extensions distinguish substantive nonstandard
codes from known sentinel codes. Sentinel meanings and exact scoped certificates remain
attached to the owning state or physical alias. The variable's value-set viewer keeps
matching source members, substantive extras and sentinel markers separate. Source labels
remain intact, and local extensions never become official classification members.
Unknown or ambiguous classification references remain unresolved.

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
facet-ordered member list under `members`, and the number of distinct members hit as
`matched_count` on the typed model; a member reached through both the variable and the
varname arm counts once); a lone member hit stays a leaf annotated with
`concept_group`/`concept_group_label`; and group LABELS themselves match (searching a
family label finds its group row even though no single leaf row matches). `--no-fold`
flattens. `get schema` carries `concept_group`(`_label`) per column so the fold is
visible inline.

**Classification edition chains fold in search separately (#571).** Before the
concept-group fold, `_fold_classification_succession` collapses classification edition
hits that share a `classification_replaced_by` chain into one
`type: "classification_succession"` result row — the terminal (current) edition's
identity, plus the full `editions` list (terminal-first by BFS depth — date-independent,
so robust to undated `effective_year` edges; #588) and the number of distinct editions
hit as `matched_count`. The internal dict pipeline carries the raw `matched` leaf list
for fold arithmetic and `_strip_internal_keys` drops `_classification_id` from it; each
fold computes `matched_count` from the distinct member ids when it builds the row, and
`_row_to_model` does not put `matched` on the typed model. This fold is terminal-centric
(it collapses a whole family onto its terminal, with no queried node), so
collect-all-ancestors is correct here — unlike `Catalog.classification_chain` /
`variable_chain`, which anchor on the QUERIED node's path (also #588) so a merge sibling
on a different inbound branch is excluded. A lone edition hit (whether terminal or an
old vintage) stays a leaf; an old-vintage lone hit is annotated with `terminal_fqid` so
the webapp can link "current". This fold runs **before** the concept-group fold so
collapsed terminals can then fold into a curated umbrella group (e.g. `group:sun`, #516)
cleanly — the succession row keeps the terminal's `_classification_id` so the umbrella
pass treats it as that classification. All folds happen before pagination — a succession
row and a group row each count as one result.

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
classification books (`classification_slugs`), or the per-era coalesced
`delivery_column_name`. Raw `data_type` / `data_length` are **never** a boundary signal
on their own: SCB's per-delivery `Datatyp` / length is low-trust passthrough that #526
blanks, so an `int -> bigint` or char↔varchar wobble does NOT open a run. This scopes
the boundary to "distinctions that survive in `variable_state`" — precisely what
reg_meta_build's #526 value-set-anchored fold leaves in the materialized rows; reg_meta
re-derives it query-side (it must not depend on reg_meta_build — wrong dependency
direction). Per-period alias multiplexing (monthly families' 12 columns, held in
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
curation can give and popularity can't). Curated from
`reg_meta_build/curation/tags.toml`, derived every build (regenerate-not-migrate); a
discovery overlay that leaves identity untouched, same family as concept groups and
delivery enrichment (catalog-overlay TOMLs). The first committed content slice is
SCB-heavy and intentionally small; synthetic builds and wheel installs can still
materialize empty tag tables when the curation file is absent. The webapp consumes
memberships as catalog-node chips; tag-scoped search/facets and tag-backed search boost
remain separate consumption work.

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
main DB. Docs are optional for metadata, holdings and orders; only a docs operation
requires its paired sibling and reports `doc_db_not_found` when missing. No unrelated
installed docs artifact is attached silently.

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

**Auto-download on first use**: metadata, holdings and order reads require only the
selected catalog artifact. Docs operations additionally require its paired sibling doc
DB. Interactive query commands offer missing downloads; non-interactive invocations fail
with actionable `db_not_found` / `doc_db_not_found` errors. The bootstrap catalog
download resolves the newest release carrying the selected catalog's asset; the docs
download takes the newest release carrying the shared docs asset, whatever the catalog.
Their missing-asset remediation names only end-user actions (`reg-meta update --tag`, an
issue report), never the maintainer-only `reg-meta-build` commands. Updates preserve
explicit catalog selection and do not attach an unrelated installed docs artifact.

### Package version format

Package versions follow `X.Y.Z` with two optional pre-release suffixes:

- `X.Y.Z` — final release
- `X.Y.ZaN` — alpha (e.g. `0.5.0a1`)
- `X.Y.Z.devN` — development build (e.g. `0.5.0.dev3`)

No other suffixes (beta, rc, post, epoch) are used. The update checker relies on this
format for version comparison.

## Exit codes

  | Code | Meaning                                                                                    |
  | ---- | ------------------------------------------------------------------------------------------ |
  | 0    | Success                                                                                    |
  | 2    | Usage/argument error                                                                       |
  | 10   | Configuration error (missing DB, bad encoding)                                             |
  | 16   | Not found                                                                                  |
  | 17   | No match with `--require-match`; blocked order; validate: project has an error-level issue |
  | 25   | Network error (`reg-meta update`)                                                          |
  | 30   | Unexpected internal error                                                                  |

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

### Documentary source relationships

`Catalog.documentary_relationships(fqid)` returns strict typed source crosswalks and
derivation clauses owned by the resolved variable. The original declarations, ordered
variable references and unresolved source coordinates are retained. The status is
explicitly `owner_bound_literal`: references describe documentary metadata and do not
make formulas executable, establish variable equivalence, choose a code namespace or
extend availability. Supplied periods remain literal source fields. The reader validates
persisted JSON using the same evidence models as ingestion; the schema is regenerated
directly when this contract changes.

Storage identifiers remain exact SQLite/Python integers. All JSON surfaces serialize
these identifiers as opaque decimal strings, including CLI SQL rows and API models.
Counts, years and local representation-run ordinals remain numbers. Readers require
schema 9; old catalogs must be regenerated. The `0001-01-01` unknown coverage sentinel
is presented as an absent coverage start; source declarations and warnings remain
available separately and do not establish observation availability.
