# Design: reg_schema

Design rationale and constraints for the `project_data.json` schema and
its structural validator. The code (`project_data.py` / `structural.py` /
`validation.py`) plus the generated `model_json_schema()` are the
field-level reference; this file is the WHY. Cross-cutting topology
(package tree, dependency graph, Pydantic policy) lives in the root
`ARCHITECTURE.md`; remaining/unbuilt schema work lives in
`REFACTOR_SPEC.md`.

## Scope

`reg_schema` owns:

- The `project_data.json` v2 shape (Model A): Pydantic v2 models for
  `ProjectData`, `Source`, `Binding`, `Panel`, `PanelMember`,
  `PeriodRange`, `LiteralPeriod`, `TimeRange`, and the `Period` /
  `EntityKey` / `TimeKey` / `TimePoint` type aliases (§6.1-§6.4).
  Under Model A a `Source` carries a 3-part `register_variant`
  coordinate plus a required `period`; bindings (renamed from the v0.x
  `columns`) name a 3-segment binding FQID via `variable`. Composite
  `entity_key` / `time_key` arrays are part of the schema from day one
  (the validator enforces their ordering/homogeneity rules). Remaining:
  composite-key runtime support in the extract path — see
  `REFACTOR_SPEC.md`.
- The §6.8.1 **structural validator** — rules enforceable with only
  the spec payload, no external state: required fields, type/subtype
  consistency, FQID well-formedness (3-segment binding FQID /
  2-segment `class/<slug>` value set / 3-part variant coordinate),
  `Source.period` grammar, panel composite ordering, source-collision,
  panel key-refs landing on the source's `display_name` strings, etc.
- The §6.8.0 cross-runtime contract: `ValidationIssue` /
  `ValidationResult`. Same shape consumed by `reg_schema` (Python),
  `reg_monabundle`'s amalgamated bundle-load validator (Python), and
  the SPA (TypeScript codegen'd from OpenAPI). Composition just
  concatenates `issues`.

## Not in scope (intentionally)

- **§6.8.2 namespaced-block rules.** Each namespaced block
  (`reg_monabundle`, `swecov`, …) is validated by its owning package.
  `reg_schema` treats those blocks as opaque objects — it only checks
  shape, not contents.
- **§6.8.3 semantic rules (reg_meta-backed).** FQID resolution against
  a live reg_meta DB, classification existence, steward-catalog
  membership, drift detection. Lives in `reg_webapp` (and any local
  CLI that loads reg_meta). The split is what lets `reg_schema` ship
  reg_meta-free.
- The `project_data.codes.json` sibling file (§6.9). Codes live
  alongside the spec but are written by kit-build in `reg_webapp`;
  the SPA carries pre-kit ad-hoc codes in IndexedDB. Either may grow a
  schema dataclass here later; phase 1 keeps it out.
- **Per-source SQL filtering (`where`).** There is no `where` field in
  the v1 baseline `Source`. Cohort/row filtering is a property of the
  MONA-side runner, not the order spec — it lives in `reg_monabundle`'s
  `configure()` (per `sql_table` / `file_source`). A steward that wants
  to record a filter for audit puts it in its own namespaced block
  (e.g. `swecov.filters`); see `reg_monabundle/DESIGN.md`.

## What this layer does NOT validate

`ValidationResult.__post_init__` coerces `issues` to a tuple but does
**not** verify each element is a `ValidationIssue` instance. JSON
deserialization belongs at read/write boundaries — the bundle-load
validator in `reg_monabundle` and the API ingress in `reg_webapp` —
not in the contract module itself. Python-internal callers are
type-checked; cross-runtime callers own their decode step. If
`result.ok` ever crashes with `AttributeError` on `.level`, that is a
boundary bug to fix upstream, not a defensive check to add here.

The `level` allowlist *is* enforced at construction because the cost
of a silently-weakened `ok` (returning `True` for a result that should
block) is higher than the cost of one extra check on a 3-value
frozenset.

**`schema_version` is not value-checked here.** The structural layer
only requires `schema_version` to be a present, non-null string — it
does **not** reject a v0.x (`"1.x.x"`) value. The "Model A files are
`"2.0.0"`; v0.x is hard-rejected, no migration code" policy is enforced
by the consumer that loads the file (the SPA / CLI in `reg_webapp`), not
by `validate_structural`. Reason: the version-acceptance window is a
deployment concern (which schema a given app build understands), whereas
this layer is the version-agnostic shape checker shared by every
runtime. Same split for `reg_meta_version`: required as a non-null
string here; drift against the actually-loaded reg_meta DB is a §6.8.3
semantic concern.

## Dependency direction

`reg_schema` has **one runtime dependency: Pydantic v2** — the single
deliberate exception to the workspace no-Pydantic rule (the
cross-cutting "no Pydantic on library surfaces" policy and why
`reg_schema` is exempt live in `ARCHITECTURE.md`; the
reg_schema-specific reasons follow). The §6.1-§6.4 models are the
canonical project_data shape, double as FastAPI response models in
`reg_webapp`, and feed the SPA's TypeScript types via
`model_json_schema()`; those three jobs make Pydantic's declarative
field/model validators the right tool here, and keeping `reg_schema` as
the *only* Pydantic surface kills the 1:1 wrapper-drift that a separate
validation model would create between the schema and the API.

The **structural validator** (`structural.py`) uses no Pydantic in its
rule logic — it operates on a parsed dict. (It does import the `Literal`
type aliases from `project_data.py`, which pulls Pydantic into the import
chain; the MONA bundle ships `reg_monabundle`'s own §6.8.2 validator, not
this one, so that never reaches MONA — see below.) The Pydantic models
live on the model surface (`project_data.py`).

**MONA boundary (§9.6).** Pydantic must **not** ship to MONA, and does
not: the bundle never amalgamates `project_data.py`. A
caller (the mdw CLI / `reg_webapp`) runs the Pydantic structural gate at
build time — `reg_monabundle/build/spec_loader.py`
(`validate_project_data`, plus a `LoadedSpec` round-trip that fails fast
on runtime-unsupported specs) — and the bundle then embeds the project's
**JSON**. On MONA the stdlib runtime deserializes that JSON into a
`LoadedSpec` (`reg_monabundle.runtime.spec.loadedspec_from_dict`), so it
sees only stdlib dataclasses, never Pydantic. The §9.6 CI gate
(`test_bundle_carries_no_pydantic_or_reg_schema`) line-scans + AST-checks
the emitted bundle to prove it carries no Pydantic and no `reg_schema`.

Why the dep split matters: the spec is validated in three execution
contexts with very different dependency availability — only the webapp
backend has reg_meta, only the bundle runs on MONA. Keeping the
structural layer dep-free means:

- The MONA bundle ships **no** `reg_schema` at all: the
  pure-stdlib §6.8.2 block validator in `reg_monabundle` is what runs on
  MONA, while `reg_schema`'s §6.8.1 structural validator runs only at the
  build-time gate. Confining Pydantic to `project_data.py` keeps the
  structural layer importable in dep-light contexts without dragging in
  the model surface.
- `reg_mockdata` can consume `reg_schema` reg_meta-free, without
  re-introducing reg_meta transitively.
- The TypeScript SPA mirrors a small, stable surface.

The inbound dependency graph (who imports `reg_schema`, and the
amalgamation edge into the MONA bundle) is part of the cross-cutting
package topology — see `ARCHITECTURE.md`. The constraint `reg_schema`
itself imposes is the one above: it pulls in **only** Pydantic, and only
on the model surface.

## Two layers: models vs. validator

`reg_schema` is deliberately split into a **shape** layer and a **rule**
layer, and the two are kept apart on purpose:

- **Models** (`project_data.py`, Pydantic v2): `ProjectData`, `Source`,
  `Binding`, `Panel`, `PanelMember`, `PeriodRange`, `LiteralPeriod`,
  `TimeRange`, and the `Period` / `EntityKey` / `TimeKey` / `TimePoint`
  type aliases. **Pure shape definitions** — structural rules are *not*
  re-encoded as raising field validators. Re-encoding them would replace
  the issue-accumulating contract with a fail-fast one, and the three
  runtimes that share the contract (SPA, MONA bundle, webapp) need the
  full issue list, not the first exception.
- **Structural validator** (`structural.py`, §6.8.1): the entrypoint
  `validate_structural(data: Mapping[str, object]) -> ValidationResult`
  operates on a **parsed dict, not the Pydantic models**, for two
  reasons. First, rules like "`type` ∈ enum" must fire on raw JSON values
  *before* any `Literal` cast would coerce or reject them — a wrong
  enum value has to surface as an accumulated `invalid_enum_value`
  issue, not a constructor crash. Second, staying off the model surface
  keeps the validator Pydantic-free so it ports to the MONA bundle and
  the TS SPA (§9.6).

Models are constructed at boundaries (API ingress, bundle build) only
*after* `validate_structural` has passed; a Pydantic raise at that point
signals validator/model drift, not user error.

## Shared validator corpus (`test_corpus/`)

`reg_schema/test_corpus/` is the single artifact that keeps the
§6.8.1 structural rules behaving identically in the two runtimes that
carry a copy of them — the canonical Python `validate_structural` and
the SPA's TypeScript port. (The §6.8.0 `ValidationResult` *shape* is
shared by a third runtime too, the MONA bundle's namespaced-block
validator; the structural *corpus* is not — see below.) Each case is a
directory containing an `input.json` (a
`project_data.json` payload) and an `expected_ValidationResult.json`
(the validator output the structural rules must produce). See
`test_corpus/README.md` for the directory layout, file formats, and
the rule for adding cases.

**Two** consumers run the structural corpus — the two runtimes that
own a copy of the §6.8.1 rules:

- `reg_schema/tests/test_corpus.py` runs `validate_structural(input)`
  against every case and asserts an unordered-issue equality match
  with the decoded `expected_ValidationResult.json` — the single
  Python source of truth that the SPA mirrors.
- The SPA's TypeScript test suite imports the JSON as fixtures and
  runs its TS port of the validator against them.

The MONA bundle is **not** a third consumer of this corpus: it
amalgamates the §6.8.2 namespaced-block validator
(`reg_monabundle.validate.validate_block`), not the §6.8.1 structural
layer. The bundle never re-runs structural validation on MONA — it
deserializes a `LoadedSpec` that bundle-build already gated through
`reg_schema` (`spec_loader.validate_project_data`). So the structural
corpus has only the two runtimes that carry a structural-rule copy to
keep aligned.

The corpus grows alongside the validator: at least one well-formed
empty-issues case to prove the format/harness/round-trip, plus one (or
more) negative case per structural rule. Negative cases for §6.8.2
(namespaced blocks) and §6.8.3 (reg_meta-backed semantic) layers live in
their owning packages, not here — `reg_schema` only owns the structural
layer's corpus.

## Structural rules and issue codes

Issue `code` values are stable across releases — tests pin them, the
SPA maps codes to UI affordances, new codes are additive (§6.8.0).
Current codes:

| Code | Rule (§6.8.1) |
|---|---|
| `invalid_root` | Root must be an object. |
| `missing_required_field` | A required field (top-level, source, binding, panel, member) is absent. |
| `invalid_field_type` | A field's JSON type is wrong (e.g. `steward` is not a string; `members` is not an array; `period` is null). |
| `invalid_enum_value` | `steward`, `type`, `id_subtype`, `numeric_subtype` is outside its allowed set. |
| `unexpected_field` | An unrecognized key on a CLOSED object (`Source` / `Binding` / `Panel` / member) — these are the `extra="forbid"` `_Model` subclasses (§6.2-§6.4). Top-level unknown keys are namespaced blocks, not errors (`ProjectData` is `extra="ignore"`). |
| `invalid_fqid` | FQID segment count or per-segment characters are wrong: binding `variable` is not a 3-segment `<provider>/<register>/<slug>`, `value_set` is not a 2-segment `class/<slug>`, or `register_variant` is not a 3-part `<provider>/<register>/<variant>` coordinate. The binding leaf is a bare slug — the retired `@version` pin is now a stray `@` the per-segment grammar rejects (§6.8.3 resolves the value set from `(variable, variant, period)`). |
| `fqid_register_variant_mismatch` | A binding `variable`'s first **2** segments (provider/register) don't equal the owning source's `register_variant` prefix. The variant is not repeated on the binding — it lives once on the Source (§6.2). |
| `invalid_period` | A `Source.period` is not an int year, a period-token string (`YYYY`, `YYYY-MM`, `YYYY-MM-DD`, `HTYYYY`, `VTYYYY`, `YYYY-Q[1-4]`, `YYYY-H[12]`), the snapshot sentinel `"_default"`, or a `{"from","to"}` range object with valid endpoints (§6.2). |
| `subtype_on_wrong_type` | A `*_subtype` or `*_format` field is set on a binding whose `type` doesn't own it (e.g. `id_subtype` on a categorical). |
| `empty_bindings` | A source has zero bindings. |
| `duplicate_source_name` | Two sources share a `name`. |
| `display_name_collision` | Two bindings on the same source share an explicit `display_name` (§6.3). The implicit-resolution half — one explicit + one resolving to the same reg_meta default — needs reg_meta and lives in §6.8.3. |
| `duplicate_panel_id` | Two panels share a `panel_id`. |
| `empty_members` | A panel has zero members. |
| `literal_period_invalid` | The `{"period": ...}` or `{"range": {"from","to"}}` time_key object form is malformed (missing/extra keys, or non-period endpoints). |
| `composite_time_key_mixed_kinds` | A composite `time_key` array mixes column refs and literals on a single member (§6.4). |
| `composite_key_inconsistent` | Composite `entity_key` / `time_key` tuples across members of a panel are not identically ordered. |
| `time_key_member_kind_mismatch` | A member-level composite `time_key` override has a different kind (literal vs ref) than the panel-level composite (§6.4). |
| `literal_time_key_duplicate` | Two members of one panel resolve to the same literal `time_key`. |
| `entity_key_unknown_column` | A bare-string `entity_key` ref doesn't match any `display_name` on the member's source. Skipped on sources with any unset `display_name` (the ref may resolve to a reg_meta-derived default at runtime). |
| `time_key_unknown_column` | Same rule, for `time_key` column refs. |
| `source_referenced_by_multiple_panels` | One source appears in two panels (§6.4). |
| `panel_member_unknown_source` | A panel member's `source` does not match any entry in `/sources`. |

The "ref exists on source" check is intentionally lenient: when any
binding on the source lacks an explicit `display_name`, the structural
layer skips matching that source's refs entirely. The bundle / kit /
webapp paths materialize defaults from reg_meta before they emit
artifacts (§6.3), and a pre-kit SPA-state spec shouldn't be flagged
for refs that will resolve later.

### Effective-key presence is not structural

The v0.x `missing_effective_entity_key` / `missing_effective_time_key`
codes do **not** exist in this layer. Under Model A an omitted
`entity_key` / `time_key` inherits from the member's variant's
`panel_template` (§6.4), which needs reg_meta state — so the "no
effective key" case is the semantic `panel_inheritance_unresolvable`
check (§6.8.3), raised by kit/bundle-build, not here. A member with no
panel default and no override is simply not flagged at this layer.

The composite/literal panel rules that **are** structural live in the
issue-code table above (`composite_key_inconsistent` ordering,
`composite_time_key_mixed_kinds` homogeneity, `literal_time_key_duplicate`
uniqueness, member-vs-panel `time_key_member_kind_mismatch`). The
structural layer deliberately keeps the SPA's pre-kit authoring spec
valid even while inheritance is still unresolved — it never materializes
defaults, only checks shapes.

Remaining: kit-build inheritance materialization (resolving
`panel_template` into explicit keys before emitting a kit/bundle) — see
`REFACTOR_SPEC.md` / #217.

### Semantic codes — defined, not emitted by `reg_schema`

These `code` values are part of the §6.8.0 contract but are raised by
the **reg_meta-backed §6.8.3 layer** (`reg_webapp`, kit-build,
bundle-build), never by `validate_structural`. Listed here so the
stable-code registry is complete and the SPA can map them:

| Code | Level | Rule (§6.8.3) |
|---|---|---|
| `period_outside_state_validity` | error | No `variable_state` covers the binding's `(variant, period)`. |
| `binding_state_drifts_within_period` | info | A range `period` crosses a state transition (incl. a delivery-column rename), or a chosen `representation` covers only part of the range; the resolver returns per-state subsets. |
| `range_period_partially_covered` | info | An explicit range `period` is only PARTIALLY covered by the concept's states — the union of every covering state (across all delivery columns) leaves a gap NO column delivers (e.g. SSYK first delivered 2014 under a `from:2010,to:2020` binding → 2010–2013 has no data). The covered sub-range still extracts; the gap is silently dropped, so this surfaces it. Distinct from `binding_state_drifts_within_period`: that is the CHOSEN representation under-covering vs a SIBLING column that DOES deliver the gap (whole-concept covered); this is the whole concept itself under-covering. Zero coverage is `period_outside_state_validity`, not this. Only fires for an explicit `PeriodRange` (a point/token period is a single instant; `_default` has no author-requested window). |
| `binding_value_set_version_ambiguous` | error | A binding's `(variant, period)` resolves to several **CO-EXISTING delivery columns** (distinct columns valid at the SAME instant — overlapping windows) — parallel REPRESENTATIONS of the one concept (SSYK 3/4/5-digit, age brackets) — and the binding sets no `representation`. The author must pick one (the SPA offers a chooser); this is where the retired `@version` pin's job now lives, keyed on the delivery column. Distinct columns in NON-overlapping windows (a sequential rename) are drift, not ambiguity. (Also re-used as a backstop for the rarer case of distinct value sets co-delivered on ONE column — a reg_meta build co-delivery the `validate` invariant should make unreachable.) |
| `binding_representation_unknown` | error (→ warning on the steward path) | A binding's `representation` is not a delivery column of the concept at the source's `(variant, period)`. Downgraded for steward-catalog load (reg_meta dropped/renamed the pinned column = drift), like `period_outside_state_validity`. |
| `variable_replaced` | info | The binding has a `variable_replaced_by` edge effective at or before the source's `period`; hint points at the successor. |
| `panel_inheritance_unresolvable` | error | A member has no effective `entity_key` / `time_key` and its variant has no `panel_template` to inherit from. |

## Why no FQID parser dependency

§6.8.1 phrases FQID structural checks as syntactic ("3-segment binding
FQID `<provider>/<register>/<slug>` / 2-segment `class/<slug>` / 3-part
`register_variant` coordinate"). The binding leaf is a bare slug — there
is no `@version` pin to split off (that grammar is retired; co-delivery
selection moved to the binding `representation` field, §6.8.3), so the
per-segment `_FQID_TOKEN` rejects a stray `@`. `reg_schema` implements
these checks locally rather than importing `reg_meta`'s `Fqid` parser.
Two reasons:

- Keeps the dependency direction one-way (`reg_meta` → `reg_meta_build`
  is the only cross-dep today; adding `reg_schema` → `reg_meta` would
  pull reg_meta into every consumer of reg_schema, including the MONA
  bundle).
- Structural well-formedness is a small, stable surface (segment
  count + segment slug characters). Duplicating it is cheaper than the
  coupling.

The same rationale covers the **period-token grammar** (`Source.period`
and `TimeKey` range endpoints, §6.2): `structural._PERIOD_TOKEN` is a
deliberate mirror of the canonical grammar in
`reg_meta.fqid._PERIOD_PATTERNS` (year 1900-2099, month 01-12, day 01-31,
plus `HT/VT`, quarter, half-year forms). It is kept **bound-for-bound
identical** so a spec that passes this structural gate doesn't later fail
reg_meta's period resolution — a looser copy would silently split the
period contract across the two packages. When the canonical grammar
changes, update both.

Semantic FQID resolution stays in `reg_meta` and is invoked by the
§6.8.3 layer in `reg_webapp`.
