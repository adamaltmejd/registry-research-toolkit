# SWECOV steward inputs (untracked, except this file and the generator)

Curated copies of SWECOV-internal metadata documents, copied 2026-06-12 from the
maintainer's local SWECOV document store (directory structure preserved). Source
of truth stays there; this copy exists so the catalog build pipeline depends on
nothing outside the repo tree.

**Confidentiality**: these are internal order/delivery documents. They live in
the untracked `input_data/` seed area on purpose — commit *derived* metadata
(TOML/JSON extracts), never these originals. Two exceptions are force-tracked
(`git add -f`, decision 2026-09-02): this README and `build_catalog.py` — code
and documentation, not data.

Contents:

- `SWECOV_variables_2025-12-11.csv` — the holdings inventory (one row per
  physical table; Category/Detail/Table + delivery columns). Ground truth of
  what SWECOV physically holds.
- `build_catalog.py` — the deterministic generator (normalize + ground + emit).
  Lives here next to its inputs, and since 2026-09-02 it is TRACKED (`git add
  -f`; reversing the 2026-06-12 untracked decision): it is the sole generator
  of the committed `reg_webapp/stewards/swecov/inventory.toml` and defines the
  `inventory_overlay.toml` semantics, so losing it would mean reverse-engineering
  the overlay contract. It still cannot run in CI — its CSV/workbook inputs stay
  confidential and untracked — so regeneration remains maintainer-only:
  `uv run python build_catalog.py --csv <holdings CSV> --db <flavored db>
  inventory` from this directory.
- `SCB/` — master P1105 delivery variable lists (2022, 2023-24, 2025 v5):
  per-register sheets with `Variabelnamn | Beskrivning | År | Källa | Vy`.
  `Källa` separates register-sourced from SWECOV-constructed columns; `Vy`
  joins to the inventory's table names. Plus Bakgrundsfiler variable lists
  (BAS, AMU, Geografi, Fordon, LISA) for upcoming/expanded registers.
- `General/` — prose overview of SWECOV data (steward description material)
  + lists of views being archived.
- `Socialstyrelsen/` — per-delivery variable lists (Variabelnamn, Klartext,
  Variabeltyp, Variabellängd per dataset) for lev 2020/2022/2024 + lev3 order.
- `Data management/` — SWECOV-side view-creation SQL (table → register
  mapping evidence).
- `FK/`, `Fohm/`, `Skatteverket/`, `Kvalitetsregister/`, `Graviditetsregistret/`,
  `Primärvård/`, `Mönstringsdata/`, `Läkemedelsverket/`, `Nycklar/`, `Telia/` —
  per-agency variable lists with descriptions for the non-SCB/SOS residue
  categories.

Deliberately excluded: contracts, offers, applications, decisions,
personuppgiftsbiträdesavtal, and correspondence — no catalog metadata, highest
confidentiality.
