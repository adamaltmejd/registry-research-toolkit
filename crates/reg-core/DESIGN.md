# Design: the project contract (`reg-core`)

Design rationale and constraints for `project_data.json`: its types, its structural
validator and the validation result every layer shares. The code (`project.rs`,
`structural.rs`, `validation.rs`) is the field-level reference; this file is the why.
The runtime around it, including the semantic layer and the order manifest, is
[../DESIGN.md](../DESIGN.md). Composite-key runtime support and the MONA side are
remaining work in `REFACTOR_SPEC.md`.

`reg-core` also holds the FQID and period grammars, the text folds and the interval
algebra. It does no IO, so the server, the MCP tools and the build (through
`reg-core-py`) share one implementation of each.

## Scope

- **The `project_data.json` shape.** `ProjectData`, `Source`, `Binding`, `Panel`,
  `PanelMember`, the study `window` and the period and key types. A `Source` carries a
  three-part `register_variant` coordinate (`provider/register/variant`) and a required
  `period`; each binding names a three-segment binding FQID in `variable`. The optional
  top-level `window: {"from": <year>, "to": <year>}` is the study period the SPA uses as
  an authoring default; absent means the full history.
- **`Source.period`** is an int year, a period token, a `{"from", "to"}` range, or a
  finite list of those segments. A list is an interrupted series
  (`[{"from": 2005, "to": 2010}, {"from": 2015, "to": 2020}]`, wire form
  `2005..2010,2015..2020`): one source stays one register extraction, so panel keys and
  bindings are not duplicated across pseudo-sources. A list is non-empty, its members
  are segments (not nested lists), each member is non-inverted, and the members are
  sorted and non-overlapping. Adjacency is allowed: rejecting it would need calendar
  adjacency math for no safety gain. Sorted and disjoint keeps the wire form canonical
  and per-segment resolution deterministic. Bare `"_default"` is a separate
  year-independent selection at a concrete variant, never a list or range member.
- **Composite keys.** `entity_key` and `time_key` arrays are in the schema from the
  start; the validator enforces their ordering and homogeneity.
- **The structural validator**: every rule checkable from the document alone.
- **The validation result** (`{ok, issues}`), shared by the structural layer, the
  semantic layer and the SPA. Composition concatenates `issues`.

## Logical selection, not physical delivery

`project_data.json` records research intent, not steward storage. A `Source` groups
bindings by logical variant and requested period; `Source.name` is an internal handle
for panels, never a filename or table. One logical source can resolve to many
edition-specific physical tables, and one multi-period table can serve several requested
periods, so a `table` or physical `edition` field on `Source` would conflate two grains.
The order manifest joins the two worlds ([../DESIGN.md](../DESIGN.md), "Order
manifest"); this contract stays independent of holdings.

Every source carries an explicit period. Dated selections use finite periods. Bare
`"_default"` selects year-independent data and needs a concrete `register_variant` whose
last segment is not `_default`; it is not a whole-history request. The SPA may offer the
study window as a default, but each source persists its own concrete period: adding a
source defaults to the full available intersection (disjoint segments included), and an
empty intersection blocks the add rather than inventing a period. Editing the window
later never rewrites existing sources; a source left disjoint keeps its period and the
project becomes blocking. Divergence stays visible rather than hidden as inheritance. An
empty project is a valid draft that cannot be ordered.

## Closed project root

`ProjectData` is a closed object (decided 2026-07-14). An unknown top-level key is
`unexpected_field`, as on `Source`, `Binding`, `Panel` and panel members. There is no
steward-namespaced block and no placeholder `extensions` field. If a real consumer needs
extension data, add one explicit `extensions` container with a defined owner and
validation boundary then.

Out of scope on purpose:

- **Semantic rules.** FQID resolution against a catalog, classification existence,
  steward membership and drift need an artifact and live in `reg-catalog`'s validate
  operation. The dependency runs one way: this crate knows no catalog.
- **`project_data.codes.json`.** Deferred to the MONA rebuild.
- **Per-source row filtering (`where`).** Cohort filtering belongs to the MONA-side
  runner, not the order spec. An audit-filter use case needs its own contract; v1
  reserves no escape hatch.

## The supported version

The structural layer requires `schema_version` and `reg_meta_version` to be present
strings and does not compare them. The version decision is separate and runs first:
`version_issue` accepts exactly `SCHEMA_VERSION` and reports anything else, an old
major, a newer minor or a different patch, as one `unsupported_schema_version` issue
(path `/schema_version`). It is reported instead of the layers below it, because they
would read the document as the current contract, the claim just rejected. Nothing is
migrated or reinterpreted. An absent or non-string `schema_version` is a malformed
document, not a version claim, and gets the structural layer's `missing_required_field`
or `invalid_field_type`. Whether `reg_meta_version` matches the loaded catalog is a
semantic concern.

## Two layers: types and validator

The crate splits a shape layer from a rule layer:

- **Types** (`project.rs`) are pure shape. They keep each value's raw spelling (an int
  year stays an int, `"2018"` stays a string) and serialize every optional field, absent
  as `null`; that encoding is what the project hash covers. Structural rules are not
  re-encoded as deserialization failures: that would turn the issue-accumulating
  contract into a fail-fast one, and every consumer needs the full list.
- **The structural validator** (`structural.rs`) reads the raw JSON tree, not the types.
  A wrong enum value must surface as an accumulated `invalid_enum_value`, not a
  deserialization error, and the validator must report on documents the types cannot
  hold.

`ProjectData::from_value` is the only way to build a project from JSON: it runs the
validator first and deserializes only an accepted document, so a deserialization error
after acceptance is validator/type drift, not user error. Enum values come from the same
list that defines their serde encoding, so the two cannot disagree.

`ok` is true when no issue is an error. Warnings and infos do not block, so `ok` is not
a clean bill of health.

## Shared validator corpus

The structural rules are pinned as data: each case is a directory with an `input.json`
and its `expected_ValidationResult.json`, readable without code. The corpus is
`crates/reg-core/tests/project/corpus/`, which also pins the Rust validator's message
quoting. The SPA's tests import the same expected files, so a drift in the issue shape
fails both runtimes. There are one or more cases per rule, positive and negative, each a
whole payload with its complete expected issue set; the directory name states the
behavior. Semantic cases live with the semantic layer.

## Structural rules and issue codes

Issue codes are stable: tests pin them, the SPA maps them to UI affordances, and new
codes are additive.

  | Code                                   | Rule                                                                                                                                                                                                                                                                                                                                             |
  | -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
  | `invalid_root`                         | The root is not an object.                                                                                                                                                                                                                                                                                                                       |
  | `missing_required_field`               | A required field (top level, source, binding, panel, member) is absent.                                                                                                                                                                                                                                                                          |
  | `invalid_field_type`                   | A field has the wrong JSON type (`members` not an array, `period` null, a non-string `representation`). An int is a JSON integer that fits `i64`.                                                                                                                                                                                                |
  | `invalid_enum_value`                   | `steward`, `type`, `id_subtype` or `numeric_subtype` is outside its allowed set.                                                                                                                                                                                                                                                                 |
  | `unexpected_field`                     | An unknown key on a closed object, whatever its value.                                                                                                                                                                                                                                                                                           |
  | `invalid_fqid`                         | A binding `variable` is not `<provider>/<register>/<slug>`, a `value_set` is not `class/<slug>`, or a `register_variant` is not `<provider>/<register>/<variant>`. Segments are exactly `[A-Za-z0-9_-]+`, so the retired `@version` pin is a stray `@`.                                                                                          |
  | `fqid_register_variant_mismatch`       | A binding's provider and register differ from its source's `register_variant`. The variant lives once, on the source.                                                                                                                                                                                                                            |
  | `invalid_period`                       | A `Source.period` is not an int year in 1900–2099, a period token, bare `"_default"` at a usable concrete variant, a range with valid finite endpoints, or a valid list (empty, a non-segment member, an inverted member, unsorted or overlapping members; member paths `/period/<i>`). A calendar-impossible day (`2019-02-29`) is invalid too. |
  | `invalid_window`                       | The study window's `to` is before its `from`.                                                                                                                                                                                                                                                                                                    |
  | `subtype_on_wrong_type`                | A subtype or format field is set on a binding whose `type` does not own it.                                                                                                                                                                                                                                                                      |
  | `empty_bindings`                       | A source has no bindings.                                                                                                                                                                                                                                                                                                                        |
  | `duplicate_source_name`                | Two sources share a `name`.                                                                                                                                                                                                                                                                                                                      |
  | `display_name_collision`               | Two bindings of one source share an explicit `display_name`.                                                                                                                                                                                                                                                                                     |
  | `duplicate_panel_id`                   | Two panels share a `panel_id`.                                                                                                                                                                                                                                                                                                                   |
  | `empty_members`                        | A panel has no members.                                                                                                                                                                                                                                                                                                                          |
  | `literal_period_invalid`               | A literal `time_key` (`{"period": ...}` or `{"range": {"from", "to"}}`) is malformed.                                                                                                                                                                                                                                                            |
  | `composite_time_key_mixed_kinds`       | A composite `time_key` mixes column refs and literals on one member.                                                                                                                                                                                                                                                                             |
  | `composite_key_inconsistent`           | Composite `entity_key` or `time_key` tuples are not identically ordered across a panel's members.                                                                                                                                                                                                                                                |
  | `time_key_member_kind_mismatch`        | A member's composite `time_key` override has a different kind (literal or ref) than the panel's.                                                                                                                                                                                                                                                 |
  | `literal_time_key_duplicate`           | Two members of one panel have the same literal `time_key`.                                                                                                                                                                                                                                                                                       |
  | `entity_key_unknown_column`            | An `entity_key` ref matches no `display_name` on the member's source.                                                                                                                                                                                                                                                                            |
  | `time_key_unknown_column`              | The same, for `time_key` refs.                                                                                                                                                                                                                                                                                                                   |
  | `source_referenced_by_multiple_panels` | One source appears in two panels.                                                                                                                                                                                                                                                                                                                |
  | `panel_member_unknown_source`          | A panel member's `source` names no source.                                                                                                                                                                                                                                                                                                       |

The key-ref checks are lenient on purpose: when any binding of a source lacks an
explicit `display_name`, that source's refs are not matched, because a default name from
the catalog may satisfy them later and a draft must stay valid while authoring.

**Effective-key presence is not structural.** An omitted `entity_key` or `time_key`
inherits from the variant's panel template, which needs the catalog, so "no effective
key" can only be checked once inheritance is materialized. That check is deferred to the
MONA rebuild. The structural layer never materializes defaults; it only checks shapes.

**Codes other layers emit.** The semantic layer adds `fqid_unresolved`,
`value_set_missing`, `period_outside_state_validity`, `range_period_partially_covered`,
`binding_state_drifts_within_period`, `binding_value_set_version_ambiguous`,
`binding_representation_unknown`, `deprecated_traversal`, `variable_replaced`,
`fqid_outside_steward_catalog` and `representation_outside_steward_catalog`, plus two
order codes under their own names, `representation_unresolved` and
`period_not_orderable` ([../DESIGN.md](../DESIGN.md), "Project semantic validation").
The version decision adds `unsupported_schema_version`. All belong to the same stable
registry.

## One grammar

The structural layer parses periods with the period grammar in this crate, the same code
the reader resolves them with, and compares list members by the bounds that grammar
gives (`HT2018` lies inside `2018`). The Python era kept a bound-for-bound copy in
`reg_schema` and a parity test to hold the two together; with one implementation a
document that passes the structural gate cannot later fail period resolution over a
grammar disagreement. FQIDs get only a shape check here (segment count and characters);
whether a slug is well formed and names something is resolution's job. The SPA keeps
hand-written mirrors (`period.ts`, `validation.ts`) until it uses this crate through
WASM.
