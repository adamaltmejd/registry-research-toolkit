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

Phase 1 (this PR): scaffold + §6.8.0 dataclasses only. Subsequent
phases land top-level `ProjectData`, `Source`/`Column`, `Panel`, then
the unified `validate_structural()` entrypoint. See
`REFACTOR_SPEC.md` §15 step 3 for the load-bearing-dependency story.

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
