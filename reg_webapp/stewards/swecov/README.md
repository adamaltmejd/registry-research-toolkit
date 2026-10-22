# SWECOV steward configuration

`steward.json` supplies identity, branding, the landing page's description
(`description`) and the order hand-off text (`order_instructions`) for the SWECOV
deployment (JSON, so no runtime parses TOML). The webapp serves a compiled steward
artifact whose manifest names `swecov`; boot checks that identity against `--catalog`
(`reg-meta serve`). It does not load an inventory or reconstruct holdings at runtime.
See `reg_webapp/DESIGN.md` → *Compiled artifact identity and read scope* and
`reg_meta_build/DESIGN.md` for the compiler contract.

The steward spec (`scope.toml` and `series/*.toml`, schema in
`reg_meta_build/src/reg_meta_build/steward_spec.py`) is the only steward-authored input:
the generator decides the inventory by it, and `extend-db` reads it from the builder
revision for holdings accounting, refusing a candidate whose `steward_spec_sha256` pin
names another spec. Generated inventory, physical census, storage export, intermediate
reports, and confidential source inputs stay outside Git. The compiler validates their
mappings and coverage before publishing one immutable DB. Only the compiled artifact
supplies browse membership, semantic admission, physical coverage, and order
materialization to the deployed webapp.

The inventory command checks storage types against the inventory it writes: each curated
type is compared with the MONA storage-schema export (`--storage-csv`, required) under
the physical columns the inventory maps to that type's variable. The global catalog
build reads no steward input, so that check is where SWECOV storage meets curated global
types: it writes `derived/storage_type_findings.tsv` under `--out` and warns in the run
summary when storage disagrees with a curated type, a steward-held column lacks one, or
a curated entry matches no storage column. Curation decides the fix.

SWECOV remains the proving steward while the workflow is tested pre-v1. Extracting its
branding and delivery pipeline into a steward-owned system is separate from this runtime
cut; use that extracted system as the pattern for later steward deployments.

## Builder ownership

The maintainer-run `build_catalog.py` in this directory generates delivery inputs. It is
tracked; its inputs are not. It cannot run in CI, so regeneration is maintainer-only.
Every input is an explicit flag, so the tool reads nothing from beside itself:

- `--csv` (required) — the holdings census (`SWECOV_variables_full_<date>.csv`), one row
  per physical table (Category/Detail/Table + delivery columns). Ground truth of what
  SWECOV physically holds.
- `--spec` — the steward spec, this directory by default. The spec directory's name is
  the steward id.
- `enrich --docs-root` — the SWECOV delivery-docs root (in the private inputs repo,
  `swecov-delivery-docs/`), described below.
- `grafts --lisa-docs-dir` — the markdown register docs root whose children are register
  dirs (`lisa/*.md`; in the private inputs repo, `catalog/docs/markdown/`).
- `--out` (required on `enrich`, `globals`, `grafts`, `flavor`, `inventory`, `errata`) —
  one scratch directory shared by the stages. Each stage writes under it (`derived/`,
  `providers/`, `inventory.toml`) and reads the earlier stages' files from it.

The one write into this repo is `flavor`'s steward slug pins under
`reg_meta_build/fqid_slugs/<steward>/`.

The inventory command builds its own mapping DB from the pinned global (`--base-db`) and
the candidate's providers (`--providers`), writes the inventory to `--out` and prints
the candidate's `steward_spec_sha256` and `global_db_sha256` pins. Run from the repo
root:

```sh
uv run python reg_webapp/stewards/swecov/build_catalog.py \
    --csv <census CSV> inventory \
    --base-db <global reg_meta.db> --providers <candidate>/providers \
    --storage-csv <storage-schema export> --out <scratch dir>
```

Generated inventory is accepted with provider overlays, the raw census and the
storage-schema export in a clean private input candidate; it is not committed or shipped
as a runtime file. `extend-db` consumes the accepted candidate and exact commit/manifest
pins to publish one compiled steward DB, refusing a candidate whose pins name another
spec or global. Never regenerate a runtime file under this directory. Follow the
documented builder workflow for strict publication.

### Delivery documents

The delivery-docs root holds curated copies of SWECOV-internal metadata documents,
copied 2026-06-12 from the maintainer's SWECOV document store (directory structure
preserved). They are internal order/delivery documents: commit *derived* metadata, never
these originals. They live in the private inputs repo, not here.

- `SCB/` — master P1105 delivery variable lists (2022, 2023-24, 2025 v5): per-register
  sheets with `Variabelnamn | Beskrivning | År | Källa | Vy`. `Källa` separates
  register-sourced from SWECOV-constructed columns; `Vy` joins to the census's table
  names. Plus Bakgrundsfiler variable lists (BAS, AMU, Geografi, Fordon, LISA) for
  upcoming or expanded registers.
- `General/` — prose overview of SWECOV data (steward description material) and lists of
  views being archived.
- `Socialstyrelsen/` — per-delivery variable lists (Variabelnamn, Klartext, Variabeltyp,
  Variabellängd per dataset) for lev 2020/2022/2024 and the lev3 order.
- `Data management/` — SWECOV-side view-creation SQL (table → register mapping
  evidence).
- `FK/`, `Fohm/`, `Skatteverket/`, `Kvalitetsregister/`, `Graviditetsregistret/`,
  `Primärvård/`, `Mönstringsdata/`, `Läkemedelsverket/`, `Nycklar/`, `Telia/` —
  per-agency variable lists with descriptions for the non-SCB/SOS residue categories.

Deliberately excluded: contracts, offers, applications, decisions,
personuppgiftsbiträdesavtal, and correspondence — no catalog metadata, highest
confidentiality.

Unmapped physical columns remain in the coverage denominator without becoming admitted
or orderable. Mapped columns keep their literal physical spellings; canonical logical
representations do not collapse distinct physical columns. A mapping admits only its
source variant and canonical representation. Alias and succession metadata grant no
physical possession.

Coverage worklists are intermediate builder reports. Regenerate them against current
inputs rather than recording a versioned coverage snapshot here. Compiler validation is
the publication gate; the webapp has no drift banner or runtime inventory release test.
