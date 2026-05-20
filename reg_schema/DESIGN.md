# Design: reg_schema

Design rationale and constraints for the `project_data.json` schema and
its structural validator. The authoritative schema spec lives in
`REFACTOR_SPEC.md` §6 at the repo root until §15 steps 9-10 dissolve the
refactor spec into per-package DESIGN files.

## Scope

`reg_schema` owns:

- The `project_data.json` v1 shape: dataclasses for `ProjectData`,
  `Source`, `Column`, `Panel`, `PanelMember`, `EntityKey`, `TimeKey`
  (§6.1-§6.4). Composite `entity_key` / `time_key` arrays are part of
  the schema from day one; runtime support follows in step 10b
  (§15 step 10b).
- The §6.8.1 **structural validator** — rules enforceable with only
  the spec payload, no external state: required fields, type/subtype
  consistency, FQID well-formedness (4/5/3-segment shapes), panel
  composite ordering, source-collision, panel key-refs landing on the
  source's `display_name` strings, etc.
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

`reg_schema` has **no runtime dependencies**. Pure stdlib.

Why: the spec is validated in three execution contexts with very
different dependency availability — only the webapp backend has
reg_meta, only the bundle runs on MONA. Keeping the structural layer
dep-free means:

- The bundle amalgamation in `reg_monabundle` can ship the same code
  on MONA, where pip-install is not an option.
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
- Phase 2 — §6.1-§6.4 dataclasses: `ProjectData`, `Source`, `Column`,
  `Panel`, `PanelMember`, `LiteralPeriod`, and the `EntityKey` /
  `TimeKey` / `TimePoint` type aliases. Shipped. Pure shape
  definitions; element types are not checked at construction.
- Phase 3 (this PR) — `validate_structural(data: Mapping[str, object])
  -> ValidationResult` implementing §6.8.1. Signature operates on a
  parsed dict, not the dataclasses, because rules like "type ∈ enum"
  must fire on raw JSON values before any `Literal` cast.

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
| `missing_required_field` | A required field (top-level, source, column, panel, member) is absent. |
| `invalid_field_type` | A field's JSON type is wrong (e.g. `steward` is not a string; `members` is not an array). |
| `invalid_enum_value` | `steward`, `type`, `id_subtype`, `numeric_subtype` is outside its allowed set. |
| `invalid_fqid` | FQID segment count or per-segment characters don't match the 4/5/3-segment shape with leading-`class/` discriminator. |
| `fqid_register_version_mismatch` | A column `name`'s first 4 segments don't equal the owning source's `register_version`. |
| `subtype_on_wrong_type` | A `*_subtype` or `*_format` field is set on a column whose `type` doesn't own it (e.g. `id_subtype` on a categorical). |
| `empty_columns` | A source has zero columns. |
| `duplicate_source_name` | Two sources share a `name`. |
| `display_name_collision` | Two columns on the same source share an explicit `display_name` (§6.3). The implicit-resolution half — one explicit + one resolving to the same reg_meta default — needs reg_meta and lives in §6.8.3. |
| `duplicate_panel_id` | Two panels share a `panel_id`. |
| `empty_members` | A panel has zero members. |
| `missing_effective_entity_key` | A panel member has no effective `entity_key` (neither panel default nor member override). |
| `missing_effective_time_key` | A panel member has no effective `time_key`. |
| `literal_period_invalid` | The `{"period": ...}` object form is malformed (missing key, extra keys, or non-int/string value). |
| `composite_time_key_mixed_kinds` | A composite `time_key` array mixes column refs and literals on a single member (§6.4). |
| `composite_key_inconsistent` | Composite `entity_key` / `time_key` tuples across members of a panel are not identically ordered. |
| `time_key_member_kind_mismatch` | A member-level composite `time_key` override has a different kind (literal vs ref) than the panel-level composite (§6.4). |
| `literal_time_key_duplicate` | Two members of one panel resolve to the same literal `time_key`. |
| `entity_key_unknown_column` | A bare-string `entity_key` ref doesn't match any `display_name` on the member's source. Skipped on sources with any unset `display_name` (the ref may resolve to a reg_meta-derived default at runtime). |
| `time_key_unknown_column` | Same rule, for `time_key` column refs. |
| `source_referenced_by_multiple_panels` | One source appears in two panels (§6.4). |
| `panel_member_unknown_source` | A panel member's `source` does not match any entry in `/sources`. |

The "ref exists on source" check is intentionally lenient: when any
column on the source lacks an explicit `display_name`, the structural
layer skips matching that source's refs entirely. The bundle / kit /
webapp paths materialize defaults from reg_meta before they emit
artifacts (§6.3), and a pre-kit SPA-state spec shouldn't be flagged
for refs that will resolve later.

## Why no FQID parser dependency

§6.8.1 phrases FQID structural checks as syntactic ("4 segments / 5
segments / leading `class/`"). `reg_schema` will implement those
checks locally rather than importing `reg_meta`'s `Fqid` parser. Two
reasons:

- Keeps the dependency direction one-way (`reg_meta` → `reg_meta_build`
  is the only cross-dep today; adding `reg_schema` → `reg_meta` would
  pull reg_meta into every consumer of reg_schema, including the MONA
  bundle).
- Structural well-formedness is a small, stable surface (segment
  count + segment slug characters). Duplicating it is cheaper than the
  coupling.

Semantic FQID resolution stays in `reg_meta` and is invoked by the
§6.8.3 layer in `reg_webapp`.
