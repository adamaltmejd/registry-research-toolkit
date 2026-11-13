# Design: the project contract (`reg-core`)

Design rationale and constraints for `project_data.json`: its types, its structural
validator and the validation result every layer shares. The code (`project.rs`,
`structural.rs`, `validation.rs`) is the field-level reference; this file is the why.
The runtime around it, including the semantic layer and the order manifest, is
[../DESIGN.md](../DESIGN.md).

`reg-core` also holds the FQID and period grammars, the text folds and the interval
algebra. It does no IO, so the server, the MCP tools, the builder (`reg-build`) and the
SPA (through `reg-core-wasm`) share one implementation of each. The FQID grammar
includes `set/<provider>/<register>/<slug>` as a distinct presentation kind, disjoint
from three-part table and variable refs. A set ref is valid for catalog discovery and
`show`; it is not a valid project binding or source table. `class/` and `set/` lead with
literal kind namespaces; malformed forms cannot resolve as another kind.

## Scope

- **The `project_data.json` shape.** `ProjectData`, `Source`, `Binding`, `Panel`, the
  study `window` and the period types. A `Source` carries a `table` (a table's public
  id, `provider/register` or `provider/register/table`) and a required `period`; each
  binding names a three-segment binding FQID in `variable`. The optional top-level
  `window: {"from": <year>, "to": <year>}` is the study period the semantic layer
  measures a source period against; absent means the full history.
- **`extract_format`** is an optional top-level string (contract 5.2.0 on): `dta`,
  `parquet` or `csv`. Absent means the order authoring default (`dta`), which the order
  file resolves; the default is never written into the project. The structural layer
  refuses a non-string, including explicit `null` (`invalid_field_type`), and a string
  outside the three (`invalid_enum_value`).
- **`linkage`** is an optional top-level closed object (contract 6.1.0 on),
  `{"database": <string>}`: the steward database the order draws from. The structural
  layer checks the shape only; whether the steward needs one and whether the id is one
  of its databases are holdings findings (`database_required`, `database_unknown`,
  `not_in_database`; [../DESIGN.md](../DESIGN.md), "Linkage spaces").
- **`Binding.follow_successor`** is an optional boolean in contract 5.1.0. Absent or
  false orders only the named variable. True permits the catalog order layer to extend
  along `continues` successors after that variable ends; the precise chain and refusal
  rules live in [../DESIGN.md](../DESIGN.md). The structural layer refuses non-boolean
  values, including explicit `null`; catalog resolution ensures the binding names a
  variable. Other object types remain closed and refuse this field.
- **`Binding.type`** is an optional override of the catalog type for the whole binding
  (contract 6.0.0, maintainer G10–G12 in #2071), in the catalog's published type names:
  `integer`, `decimal`, `text`, `date`, `datetime`, `boolean`, `geometry`, `uuid`,
  `binary`, `identifier`. Absent means the catalog type. A binding is not split by year:
  years whose delivery differs go in their own source by period, or the catalog is
  fixed. The structural layer refuses a non-string or explicit `null`
  (`invalid_field_type`) and any other name (`invalid_enum_value`), so a 5.x name
  (`categorical`, `numeric`, `id`, `opaque`) is refused. The 5.x `id_subtype`,
  `numeric_subtype`, `date_format` and `datetime_format` are gone (`unexpected_field`):
  an extract's storage comes from the steward's SQL types, never from the project. The
  override informs the order check only (`binding_type_differs`,
  [../DESIGN.md](../DESIGN.md), "The logical order"); it changes no entry and no order
  file.
- **Manual items** (contract 6.2.0 on) are data the catalog does not document, entered
  by hand. A binding has `variable` (catalog) or `manual` (a closed
  `{column, type, codes?}`), and a source has `table` (catalog) or `manual` (a closed
  `{provider, register}` of free-text names, not ids). `Source` and `Binding` are
  untagged enums of closed structs, so each kind is a type and the schemas carry a
  `oneOf`. Rules, each preventing a document the order or the mock would misread:
  - the kind is decided by key presence, `null` included (`source_kind`,
    `binding_kind`): a `null` read as absent would pass the validator and then match
    neither variant, which panics `from_value` and aborts the WASM module;
  - a manual source holds manual bindings only (`binding_kind` on a `variable`): it has
    no table to resolve against;
  - a catalog source keeps at least one catalog binding (`catalog_source_unbound`):
    without one its period goes unchecked, it gets no file on a steward and its manual
    columns have no mock file to join;
  - `follow_successor`, `classification` and `type` on a manual binding are
    `unexpected_field`: each names a catalog fact. The manual column's type lives in
    `manual.type`, because the binding `type` means "override the catalog";
  - `codes` on an `identifier` column is `identifier_codes`: the project is posted to
    the hosted server and copied into the order file, and codes listed there would be
    real ids;
  - a repeated code (`duplicate_code`) and two manual columns of one name in a source,
    case ignored as in a delivery (`duplicate_manual_column`), would give the mock two
    definitions of one thing.
  Blank names, blank codes and empty or malformed `codes` are `invalid_field_type`.
  Whether a manual column's name meets a catalog column in the same file, or whether
  the catalog does document it, needs the catalog and is not checked here (reg-mock refuses the first).
  A manual binding keeps its position, so binding indices still point at the same
  bindings.
- **A source has no `edition` or `qualifier`.** Selecting an edition or a qualifier is
  deferred until curation; the primary segments serve every order (final over
  preliminary), and the closed source object refuses a document that carries either.
- **A panel is a named grouping of sources** with a comment: its members are source
  names. The keys a panel needs come from the catalog (a table's row-key segments), not
  from the document.
- **`Source.period`** is an int year, a period token, a `{"from", "to"}` range, or a
  finite list of those segments. A list is an interrupted series
  (`[{"from": 2005, "to": 2010}, {"from": 2015, "to": 2020}]`): one source stays one
  register extraction, so panel keys and bindings are not duplicated across
  pseudo-sources. A list is non-empty, its members are segments (not nested lists), each
  member is non-inverted, and the members are sorted and non-overlapping. Adjacency is
  allowed: rejecting it would need calendar adjacency math for no safety gain. Sorted
  and disjoint keeps per-segment resolution deterministic. Bare `"_default"` is a
  separate year-independent selection (a timeless table's period), never a list or range
  member.
- **One period per source.** Every column of a source is ordered for the source period,
  and the order clips each column to the years it is delivered inside it. There is no
  per-column period (maintainer, 2026-10-23, #1838; it reverses #1462's
  `Binding.period`): a column first delivered in 2019 under a 2014–2020 source is
  ordered for 2019–2020 by that clip, and a researcher who wants fewer years narrows the
  source. Add a per-column period back only if researchers ask for one.
- **The structural validator**: every rule checkable from the document alone.
- **The validation result** (`{ok, issues}`), shared by the structural layer, the
  semantic layer. Composition concatenates `issues`.

## Logical selection, not physical delivery

`project_data.json` records research intent, not storage. A `Source` groups bindings by
the catalog's table and a requested period; `Source.name` is an internal handle for
panels, never a filename in the order file (a manual source's mock files take its
name, since it has no other id; [../reg-mock/DESIGN.md](../reg-mock/DESIGN.md)). The catalog's tables are logical (the same table over many
years and deliveries), so a `table` names the grain the researcher thinks in, and the
order check joins it to what is delivered ([../DESIGN.md](../DESIGN.md), "The logical
order"); this contract stays independent of holdings, and the document has no steward.

Every source carries an explicit period. Dated selections use finite periods. Bare
`"_default"` selects a timeless table's data; whether the table is timeless is the
catalog rule `period_default_on_dated_table`, not a structural one; it is not a
whole-history request. Each source persists its own concrete period. Editing the study
window does not change that period; a source disjoint from the window blocks the order.
An empty project is a valid draft that cannot be ordered.

## Closed project root

`ProjectData` is a closed object (decided 2026-07-14). An unknown top-level key is
`unexpected_field`, as on `Source`, `Binding` and `Panel`. There is no
steward-namespaced block and no placeholder `extensions` field. If a real consumer needs
extension data, add one explicit `extensions` container with a defined owner and
validation boundary then.

Out of scope on purpose:

- **Semantic rules.** FQID resolution against a catalog, classification existence,
  steward membership and drift need an artifact and live in `reg-catalog`'s validate
  operation. The dependency runs one way: this crate knows no catalog.
- **`project_data.codes.json`.** Deferred to the MONA rebuild. It is about recoding
  catalog columns; a manual column's `manual.codes` states the codes it holds, the same
  kind of fact as its type, and is distinct from it.
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
or `invalid_field_type`. `reg_meta_version` is a record, not a check: nothing compares
it with the loaded catalog, and the order manifest echoes it.

`project::check` runs the two in that order and is the single entry point: the server's
validate and order operations call it.

## Two layers: types and validator

The crate splits a shape layer from a rule layer:

- **Types** (`project.rs`) are pure shape. They keep each value's raw spelling (an int
  year stays an int, `"2018"` stays a string) and serialize every optional field, absent
  as `null`; that encoding is what the project hash covers. `follow_successor` retains
  absence separately from explicit false, although both disable following. Structural
  rules are not re-encoded as deserialization failures: that would turn the
  issue-accumulating contract into a fail-fast one, and every consumer needs the full
  list.
- **The structural validator** (`structural.rs`) reads the raw JSON tree, not the types.
  A wrong enum value must surface as an accumulated `invalid_enum_value`, not a
  deserialization error, and the validator must report on documents the types cannot
  hold.

`ProjectData::from_value` is the only way to build a project from JSON: it runs the
validator first and deserializes only an accepted document, so a deserialization error
after acceptance is validator/type drift, not user error. Enum values come from the same
list that defines their serde encoding, so the two cannot disagree.

The optional `openapi` feature derives the types' input-shape schemas (`ProjectData` and
`Project*`) and `IssueLevel`. `reg-catalog` enables it and publishes them in
`openapi.json`, with its own `Validation` and `Issue` for the result; the plugin's
project JSON Schema is generated from that contract. The enum schemas come from the same
wire list. A doc comment on one of these types is its schema description.

`ok` is true when no issue is an error. Warnings and infos do not block, so `ok` is not
a clean bill of health.

## Shared validator corpus

The structural rules are pinned as data: each case is a directory with an `input.json`
and its `expected_ValidationResult.json`, readable without code. The corpus is
`crates/reg-core/tests/project/corpus/`, which also pins the Rust validator's message
quoting. There are one or more cases per rule, positive and negative, each a whole
payload with its complete expected issue set; the directory name states the behavior.
Semantic cases live with the semantic layer.

## Structural rules and issue codes

Issue codes are stable: tests pin them and new codes are additive.

  | Code                                   | Rule                                                                                                                                                                                                                                                                                                                |
  | -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `invalid_root`                         | The root is not an object.                                                                                                                                                                                                                                                                                          |
  | `missing_required_field`               | A required field (top level, source, binding, panel, member) is absent.                                                                                                                                                                                                                                             |
  | `invalid_field_type`                   | A field has the wrong JSON type (`members` not an array, `period` null). An int is a JSON integer that fits `i64`.                                                                                                                                                                                                  |
  | `invalid_enum_value`                   | A binding `type` or `extract_format` is outside its allowed set.                                                                                                                                                                                                                                                    |
  | `unexpected_field`                     | An unknown key on a closed object, whatever its value (`steward`, `register_variant`, `value_set`, `representation`, the 5.x subtype and format fields and a panel's keys are unknown keys in 6.0.0).                                                                                                               |
  | `invalid_fqid`                         | A binding `variable` is not `<provider>/<register>/<slug>`, a `classification` is not `class/<family>/<version>`, or a `table` is not `<provider>/<register>[/<table>]`. Segments are exactly `[A-Za-z0-9_-]+`, so the retired `@version` pin is a stray `@`.                                                       |
  | `fqid_table_mismatch`                  | A binding's provider and register differ from its source's `table`. The table lives once, on the source.                                                                                                                                                                                                            |
  | `invalid_period`                       | A `Source.period` is not an int year in 1900–2099, a period token, bare `"_default"`, a range with valid finite endpoints, or a valid list (empty, a non-segment member, an inverted member, unsorted or overlapping members; member paths `/period/<i>`). A calendar-impossible day (`2019-02-29`) is invalid too. |
  | `invalid_window`                       | The study window's `to` is before its `from`.                                                                                                                                                                                                                                                                       |
  | `empty_bindings`                       | A source has no bindings.                                                                                                                                                                                                                                                                                           |
  | `duplicate_source_name`                | Two sources share a `name`.                                                                                                                                                                                                                                                                                         |
  | `display_name_collision`               | Two bindings of one source share an explicit `display_name`.                                                                                                                                                                                                                                                        |
  | `duplicate_panel_id`                   | Two panels share a `panel_id`.                                                                                                                                                                                                                                                                                      |
  | `empty_members`                        | A panel has no members.                                                                                                                                                                                                                                                                                             |
  | `source_referenced_by_multiple_panels` | One source appears in two panels.                                                                                                                                                                                                                                                                                   |
  | `panel_member_unknown_source`          | A panel member's `source` names no source.                                                                                                                                                                                                                                                                          |

**Codes other layers emit.** The order check adds the catalog rules of project document
6.0.0 ([../DESIGN.md](../DESIGN.md), "The logical order"); the version decision adds
`unsupported_schema_version`. All belong to the same stable registry.

## One grammar

The structural layer parses periods with the period grammar in this crate, the same code
the reader resolves them with, and compares list members by the bounds that grammar
gives (`HT2018` lies inside `2018`). The Python era kept a bound-for-bound copy in
`reg_schema` and a parity test to hold the two together; with one implementation a
document that passes the structural gate cannot later fail period resolution over a
grammar disagreement. FQIDs get only a shape check here (segment count and characters);
whether a slug is well formed and names something is resolution's job.
