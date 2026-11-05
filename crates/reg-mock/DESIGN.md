# Design: `reg-mock`, local mock data

Mock data lets a researcher write and run analysis code before the real extract exists,
on their own machine, against files that look like the delivery. Issue #707; the plan
and its maintainer rulings (2026-11-04, 2026-11-05) are summarized here. The skill that
drives it lives in `plugins/microdata-tools-se`.

## Boundaries

- **Metadata only.** Mocks read no data rows: catalog metadata, the project and the
  researcher's reviewed rules (crates/DESIGN.md, "Local mock skill, later"). Keys are
  synthetic, never real person numbers.
- **Not a server capability.** The hosted and stdio MCP expose metadata only. Mock
  generation runs in `reg-meta mock` on the researcher's machine, offline.
- **No network in the binary.** The skill saves the catalog's HTTP answers verbatim with
  `curl`, as the runner workflow saves `materialize` (reg_mona_runner/README.md).
  `compile` reads those files. Output then depends only on files in the project.

## Two contracts, one crate

- `mock_rules.json` (`rules.rs`): what the catalog cannot say, written by the agent and
  reviewed by the researcher. Per project source: the unit (person or firm), the
  population (age bounds, whether departed units stay), rows per unit and year with a
  stated reason, and column rules (a unit attribute such as birth year or sex, its code
  map, a missing rate, the delivery's own codes). Also file renames and tables the
  catalog does not know, in the spec's file shape.
- `mock_spec.json` (`spec.rs`): everything `generate` reads, compiled. Committed in the
  project so a reviewer sees exactly what is generated.

Both are closed and versioned (`deny_unknown_fields`). They live here, not in
`reg-core`, because this crate is their only reader and writer, and `reg-core` is also
the SPA's WebAssembly build, which has no use for them. The saved API answers are
another contract's documents (conformance/api/operations.toml): `input.rs` reads only
the fields it uses and ignores the rest.

## Compile

`reg-meta mock compile --project --rules --answers DIR --out`. `DIR` holds exactly one
order answer, `order.json` (`POST /api/project/order`, any catalog) or `manifest.json`
(`POST /api/project/materialize`, a steward's), plus every page under `segments/`
(`GET /api/segments/{table}?variable=…`, and `GET /api/segments/{variable}` for a row
key from another register) and `values/` (`GET /api/values?value_set=…`, pages named to
sort in page order). Answers from different catalog generations are refused.

**Layout.** With a manifest, files and columns are the runner's: the order file's names,
`stata_name` aliases and labels, and the row filter column as the year
(`Attribute::Year` with its encoding). A column held in fewer years than its file
carries its own years and is missing in the others. Without one, most researchers' case
(they get data straight from the provider, with no delivery names yet), files follow a
standard style: the table's public id with `_` for `/` and `-`, one file per year,
`scb_lsum_individer_sociala_agi_ku_2019` (maintainer, 2026-11-05). Columns keep the
catalog's names. Names never mark the data as mock, so a pipeline switches between real
and mock by its input directory alone. Renames in the rules adopt the delivery's names
once known.

**Types** (#2071, G8/G10, `resolve_type`): the binding's `type` in `project_data.json`
wins; else the held storage's type (`reg_core::manifest::StorageFamily`, split into
date/datetime and boolean); else the catalog's type. With an override, the column drops
its storage: it is written in the override's type. An unknown type, and geometry, uuid
or binary (maintainer, 2026-11-05), is refused, never guessed. The identifier flag comes
from the segment, not from `type = identifier`.

**Rules.** A file delivering several sources needs their unit, population and rows to
agree. A column rule that names no column is refused, since a misspelt name would
otherwise do nothing. Every refusal is returned at once, located, with a stable code.
