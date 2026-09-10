# SWECOV steward inventory

`steward.toml` (identity) + `inventory.toml` (the delivery inventory) scope a SWECOV
deployment to the columns SWECOV physically holds. `inventory.toml` is **generated** —
do not hand-edit; policy lives in the committed `inventory_overlay.toml` next to it. See
`reg_webapp/DESIGN.md` → *Steward layering* for how the webapp consumes it and
`reg_meta_build/DESIGN.md` → *Steward-flavored DB — extend-db* for the build side.

SWECOV is kept in this repo as the proving steward while the steward workflow is tested
pre-v1. Before release, move the SWECOV steward to its own steward repo/system and use
that extracted shape as the copyable pattern for additional stewards.

## What this is

The steward's **single source of truth** (REFACTOR_SPEC.md §12): every delivered table
with its one explicit finite `edition`, every literal physical column, and each column's
mappings to `(register_variant, variable, representation)`. Admission is
**column-based** (#206) and **edition-aware**: a coordinate is admitted iff some mapping
states it, over the union of its tables' edition bounds. An unmapped column stays in the
coverage denominator without becoming admitted or orderable. Every mapping pins an
explicit `representation` — the resolved `delivery_column_name` — so the deployment
derives its whole admission set with no DB access at boot. The inventory is generated
against a **flavored** reg_meta DB (the released global DB + SWECOV's flavor providers,
built by `reg-meta-build extend-db`).

## How it is generated

The generator is the `inventory` subcommand of the tracked, maintainer-run
`reg_meta_build/input_data/swecov/build_catalog.py` (it lives next to its confidential
SWECOV inputs, which stay untracked along with its `derived/` outputs; only this output
is committed — same pattern as the `flavor` subcommand, #421). To regenerate after a
reg_meta release or a flavor change:

```sh
# 1. build the flavored DB (released global + SWECOV flavor providers)
reg-meta-build --db "$db_dir" extend-db \
    --base-db ~/.local/share/reg_meta/reg_meta.db \
    --inventory reg_meta_build/input_data/swecov/derived/flavor_inventory.json \
    --slug-dir reg_meta_build/fqid_slugs/swecov
# 2. emit this inventory against it (also writes derived/steward_coverage.json).
#    Add `--out <checkout>/reg_webapp/stewards/swecov` when running from a git
#    worktree: the default writes to the generator's own repo root.
python3 reg_meta_build/input_data/swecov/build_catalog.py \
    --db "$db_dir/reg_meta.db" inventory
```

Output is deterministic (tables sorted by id, columns and mappings by coordinate).

## Coverage

Coverage is bounded by what reg_meta currently mints, and **rises automatically** as the
residue below lands upstream — just regenerate against a fresh flavored DB. The current
breakdown lives in the untracked `derived/steward_coverage.json`; as of reg_meta 0.34.0
the inventory admits **6,674 / 7,017 = 95.1%** of physical columns. The unresolved
residue is 343 columns. By disposition:

- **other (233 columns)** — provider/register-specific residue that still needs a global
  onboarding, alias, or modeling decision before it can resolve.

- **global-provider alias gaps (89 columns)** — holdings routed to a global
  provider/register (FOHM/FK #422, the Umeå/Läkemedelsverket/Pliktverket/Riksarkivet
  agencies #443, AGI employer-header and utrikeshandel-tjänster #444) where some
  delivery column names don't yet match reg_meta's — a global alias/onboarding
  follow-up, *not* flavor-routed (scope follows what a fact is *about*, #365). The
  inventory picks them up as those aliases land.

- **survey-wave global-graft follow-ups (21 columns)** — documented in SWECOV's delivery
  lists but absent from reg_meta machine metadata; handled as **global grafts**, not
  steward flavor. The steward inventory picks them up after a fresh reg_meta
  build/release and inventory regeneration.

Separately, the generator records 13 excluded pure lookup / key-crosswalk columns with
no catalogable variables (documented non-gaps outside the coverage denominator), and 0
pruned co-delivered value-set columns.

Near-duplicate physical columns (`AVERAGE_SPENDING`/`AVERAGE_SPENDINGS`,
`Covid-19 antikroppar`/`Covid_19_antikroppar`) must never be collapsed away: each
literal delivery column remains orderable. The `flavor` pass groups spellings that
differ only in punctuation, case or diacritics — `Covid-19 antikroppar` /
`Covid_19_antikroppar` — into one steward variable whose single co-delivery state keeps
every literal spelling as its own orderable representation in the flavored DB. Pairs
that differ in letters (`AVERAGE_SPENDING`/`AVERAGE_SPENDINGS`) fold to different forms,
so they stay separate generated variables until the maintainer groups them.
