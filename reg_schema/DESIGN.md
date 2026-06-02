# Design: reg_schema

Design rationale and constraints for the `project_data.json` schema and
its structural validator. The authoritative schema spec lives in
`REFACTOR_SPEC.md` §6 at the repo root until §15 steps 9-10 dissolve the
refactor spec into per-package DESIGN files.

## Scope

`reg_schema` owns:

- The `project_data.json` v2 shape (Model A): Pydantic v2 models for
  `ProjectData`, `Source`, `Binding`, `Panel`, `PanelMember`,
  `PeriodRange`, `LiteralPeriod`, `TimeRange`, and the `Period` /
  `EntityKey` / `TimeKey` / `TimePoint` type aliases (§6.1-§6.4).
  Under Model A a `Source` carries a 3-part `register_variant`
  coordinate plus a required `period`; bindings (renamed from the v0.x
  `columns`) name a 3-segment binding FQID via `variable`. Composite
  `entity_key` / `time_key` arrays are part of the schema from day one;
  runtime support follows in step 10b (§15 step 10b).
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

## Dependency direction

`reg_schema` has **one runtime dependency: Pydantic v2** — the
deliberate exception to the workspace no-Pydantic rule (root
`CLAUDE.md` stack §). The §6.1-§6.4 models are the canonical
project_data shape, double as FastAPI response models in `reg_webapp`,
and feed the SPA's TypeScript types via `model_json_schema()`; those
three jobs make Pydantic's declarative models the right tool here.

The **structural validator** (`structural.py`) is still pure stdlib —
it operates on a parsed dict and never imports Pydantic — so the rule
engine itself ships anywhere. The Pydantic dependency lives only on the
model surface (`project_data.py`).

**MONA boundary (§9.6).** Pydantic must **not** ship to MONA, and as of
A3.4 it does not: the bundle no longer amalgamates `project_data.py`. A
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

- The MONA bundle ships **no** `reg_schema` at all (A3.4): the
  pure-stdlib §6.8.2 block validator in `reg_monabundle` is what runs on
  MONA, while `reg_schema`'s §6.8.1 structural validator runs only at the
  build-time gate. Confining Pydantic to `project_data.py` keeps the
  structural layer importable in dep-light contexts without dragging in
  the model surface.
- `reg_mockdata` (the §15 step-9 rename of `mock_data_wizard`) can
  consume `reg_schema` after its `reg_meta` dependency is deleted,
  without re-introducing it transitively.
- The TypeScript SPA mirrors a small, stable surface.

Inbound consumers (none of these are reg_schema's concern, but they
shape the dependency direction):

- `mock_data_wizard` → `reg_schema` (step 4 adopts the new schema).
- `reg_webapp` → `reg_schema` + `reg_meta` + `reg_monabundle` (step 6).
- `reg_monabundle` → `reg_schema` (amalgamated into bundles, step 5).
- `reg_mockdata` → `reg_schema` (post-step 9, reg_meta-free).

## Phase status

- Phase 1 — scaffold + §6.8.0 `ValidationIssue` / `ValidationResult`
  contract. Shipped.
- Phase 2 — §6.1-§6.4 models. Shipped as stdlib dataclasses, then
  flipped to Model A + **Pydantic v2** in A3.1 (`reg_schema` v2.0.0):
  `ProjectData`, `Source`, `Binding` (was `Column`), `Panel`,
  `PanelMember`, `PeriodRange`, `LiteralPeriod`, `TimeRange`, and the
  `Period` / `EntityKey` / `TimeKey` / `TimePoint` type aliases. Pure
  shape definitions; structural rules are not re-encoded as raising
  field validators (the issue-accumulating contract stays in
  `validate_structural`, not a fail-fast model).
- Phase 3 — `validate_structural(data: Mapping[str, object])
  -> ValidationResult` implementing §6.8.1. Shipped. Signature
  operates on a parsed dict, not the Pydantic models, because rules
  like "type ∈ enum" must fire on raw JSON values before any `Literal`
  cast — and so the validator stays Pydantic-free for the MONA
  amalgamation (§9.6).

See `REFACTOR_SPEC.md` §15 step 3 for the load-bearing-dependency
story across phases.

## Shared validator corpus (`test_corpus/`)

`reg_schema/test_corpus/` is the single artifact that keeps the
§6.8.0 `ValidationResult` shape coherent across the three runtimes
that validate `project_data.json` (`REFACTOR_SPEC.md` §15 step 5.5).
Each case is a directory containing an `input.json` (a
`project_data.json` payload) and an `expected_ValidationResult.json`
(the validator output the structural rules must produce). See
`test_corpus/README.md` for the directory layout, file formats, and
the rule for adding cases.

Three consumers read the same JSON:

- `reg_schema/tests/test_corpus.py` runs `validate_structural(input)`
  against every case and asserts an unordered-issue equality match
  with the decoded `expected_ValidationResult.json` — the single
  Python source of truth that the other two runtimes mirror.
- `reg_monabundle`'s bundle build amalgamates the corpus into a
  self-test that runs on MONA load (§15 step 5), catching drift
  between the amalgamated validator and the reg_schema source.
- The SPA's TypeScript test suite imports the JSON as fixtures and
  runs its TS port of the validator against them (§15 step 6).

The corpus starts with well-formed inputs and empty-issues
expectations — these prove the format, harness, and round-trip work
end-to-end. Phase 3 grows the corpus alongside the validator, one
(or more) cases per rule. Negative cases for §6.8.2 (namespaced
blocks) and §6.8.3 (reg_meta-backed semantic) layers live in their
owning packages, not here — `reg_schema` only owns the structural
layer's corpus.

## Structural rules and issue codes (Phase 3)

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
| `invalid_fqid` | FQID segment count or per-segment characters are wrong: binding `variable` is not a 3-segment `<provider>/<register>/<slug>[@version]`, `value_set` is not a 2-segment `class/<slug>`, or `register_variant` is not a 3-part `<provider>/<register>/<variant>` coordinate. The binding leaf parses as `slug[@version]` (the `@` is split off before the slug grammar; §5.2). |
| `fqid_register_variant_mismatch` | A binding `variable`'s first **2** segments (provider/register) don't equal the owning source's `register_variant` prefix. The variant is not repeated on the binding — it lives once on the Source (§6.2). |
| `invalid_period` | A `Source.period` is not an int year, a period-token string (`YYYY`, `YYYY-MM`, `YYYY-MM-DD`, `HTYYYY`, `VTYYYY`, `YYYY-Q[1-4]`, `YYYY-H[12]`), the snapshot sentinel `"_default"`, or a `{"from","to"}` range object with valid endpoints (§6.2). |
| `binding_value_set_version_mismatch` | A binding pins a value-set version via the FQID's `@<version>` suffix **and** names a `value_set`, but they disagree (`…@sni2007` with `value_set = class/sni92`). A slug-string comparison — no reg_meta. The `@<version>` is the canonical pin; `value_set` may be omitted when `@<version>` is present (§5.2). |
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

### Effective-key presence is no longer structural (A3.1)

The v0.x `missing_effective_entity_key` / `missing_effective_time_key`
codes are **removed** from this layer. Under Model A an omitted
`entity_key` / `time_key` inherits from the member's variant's
`panel_template` (§6.4), which needs reg_meta state — so the "no
effective key" case is the semantic `panel_inheritance_unresolvable`
check (§6.8.3), raised by kit/bundle-build, not here. A member with no
panel default and no override is simply not flagged at this layer.

### Semantic codes — defined, not emitted by `reg_schema`

These `code` values are part of the §6.8.0 contract but are raised by
the **reg_meta-backed §6.8.3 layer** (`reg_webapp`, kit-build,
bundle-build), never by `validate_structural`. Listed here so the
stable-code registry is complete and the SPA can map them:

| Code | Level | Rule (§6.8.3) |
|---|---|---|
| `period_outside_state_validity` | error | No `variable_state` covers the binding's `(variant, period)`. |
| `binding_state_drifts_within_period` | info | A range `period` crosses a state transition; the resolver returns per-state subsets. |
| `binding_value_set_version_ambiguous` | error | A bare binding's `(variant, period)` matches several co-delivered value-set versions; the author must pin one with `@<version>`. |
| `variable_replaced` | info | The binding has a `variable_replaced_by` edge effective at or before the source's `period`; hint points at the successor. |
| `panel_inheritance_unresolvable` | error | A member has no effective `entity_key` / `time_key` and its variant has no `panel_template` to inherit from. |

## Why no FQID parser dependency

§6.8.1 phrases FQID structural checks as syntactic ("3-segment binding
FQID with optional `@version` leaf / 2-segment `class/<slug>` / 3-part
`register_variant` coordinate"). `reg_schema` implements those checks
locally rather than importing `reg_meta`'s `Fqid` parser. Two reasons:

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
