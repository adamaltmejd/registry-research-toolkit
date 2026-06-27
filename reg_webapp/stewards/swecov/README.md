# SWECOV steward catalog

`steward.toml` + `steward.project_data.json` scope a SWECOV deployment to the columns
SWECOV physically holds. Both files are **generated** — do not hand-edit. See
`reg_webapp/DESIGN.md` → *Steward layering* for how the webapp consumes them and
`reg_meta_build/DESIGN.md` → *Steward-flavored DB — extend-db* for the build side.

## What this is

A `project_data.json` (many `sources`, no `panels`) whose bindings admit SWECOV's
holdings. Admission is **column-based** (#206): each binding pins the resolved
`delivery_column_name` of a held column, at `period: "_default"` (the whole-history,
no-period-filter sentinel — a steward catalog is a statement of *holdings*, not a time
window). The catalog is validated against a **flavored** reg_meta DB (the released
global DB + SWECOV's flavor providers, built by `reg-meta-build extend-db`).

## How it is generated

The generator is the `steward` subcommand of the untracked, maintainer-local
`reg_meta_build/input_data/swecov/build_catalog.py` (it lives next to its confidential
SWECOV inputs; only this output is committed — same pattern as the `flavor` subcommand,
#421). To regenerate after a reg_meta release or a flavor change:

```sh
# 1. build the flavored DB (released global + SWECOV flavor providers)
reg-meta-build --db "$db_dir" extend-db \
    --base-db ~/.local/share/reg_meta/reg_meta.db \
    --inventory reg_meta_build/input_data/swecov/derived/flavor_inventory.json \
    --slug-dir reg_meta_build/fqid_slugs/swecov
# 2. emit this catalog against it (also writes derived/steward_coverage.json).
#    --reg-meta-version is REQUIRED: it stamps the catalog's reg_meta_version
#    field, and must name the release `--base-db` was built from. (The generator
#    rejects an omitted flag rather than defaulting to a fixed tag, which would
#    silently downgrade the stamp on a later release — caught in the 0.25.0
#    release.) Add `--out <checkout>/reg_webapp/stewards/swecov` when running from
#    a git worktree: the default writes to the generator's own repo root.
python3 reg_meta_build/input_data/swecov/build_catalog.py \
    --db "$db_dir/reg_meta.db" steward \
    --reg-meta-version reg_meta/vX.Y.Z
```

Output is deterministic (sources sorted by coordinate, bindings by variable+column).

## Coverage

Coverage is bounded by what reg_meta currently mints, and **rises automatically** as the
residue below lands upstream — just regenerate against a fresh flavored DB. The current
breakdown lives in the untracked `derived/steward_coverage.json`; as of reg_meta 0.25.0
the catalog admits **67.0%** of physical columns. The residue is, by disposition:

- **survey-wave items (FOU/CIS/IT, \~2,000)** — documented in SWECOV's delivery lists
  but absent from reg_meta machine metadata; a follow-up **global graft** effort, not
  steward flavor.
- **global-provider alias gaps** — holdings routed to a global provider/register
  (FOHM/FK #422, the Umeå/Läkemedelsverket/Pliktverket/Riksarkivet agencies #443, AGI
  employer-header and utrikeshandel-tjänster #444) where some delivery column names
  don't yet match reg_meta's — a global alias/onboarding follow-up, *not* flavor-routed
  (scope follows what a fact is *about*, #365). The catalog picks them up as those
  aliases land.
- **excluded** — pure lookup / key-crosswalk tables with no catalogable variables: a
  documented non-gap, kept out of the coverage denominator.
- **pruned** — a handful of reg_meta columns with co-delivered value sets (SOS-PAR
  `HDIA`/`ATCO`, AKU `Omb10b`): un-authorable by anyone (the value-set-version pin is
  retired), pending reg_meta co-delivery curation.

Near-duplicate physical columns (`AVERAGE_SPENDING`/`AVERAGE_SPENDINGS`,
`Covid-19 antikroppar`/`Covid_19_antikroppar`) are kept **1:1, never merged** — each is
a literal delivery column, and admission is column-based, so collapsing one would make
the other un-orderable.
