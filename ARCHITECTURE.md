# Architecture

Cross-cutting design for the Registry Research Toolkit — the topology,
dependency graph, and repo-wide invariants that no single package
owns. Package-local rationale lives in each `<package>/DESIGN.md`;
remaining (not-yet-built) work lives in [`REFACTOR_SPEC.md`](REFACTOR_SPEC.md).

This document is the durable home for what used to be §1–§4, §9.3,
§11–§13, and the §16 overview of the now-dissolved Model A refactor
spec. The Model A migration (the two-level catalog, the FQID grammar,
the IR/adapter build, `reg_schema` v2, the webapp + SPA) shipped; its
design rationale now lives in the package DESIGN.md files this document
points to.

## Domain

Swedish register research uses administrative microdata produced by
Statistics Sweden (SCB) and other agencies (Socialstyrelsen,
Försäkringskassan, …). Each agency publishes **registers** — population-scale
administrative datasets named LISA (labour market), RTB (population),
PAR (patient registry), FRIDA (firms), etc. A register holds
**variables** (columns), each with a stable identifier, definition, data
type, and — for categorical variables — an enumerated **value set**
(`Kön ∈ {1=Man, 2=Kvinna}`). Value sets are versioned (SUN2000 vs
SUN2020) and registers themselves drift edition to edition.

A project requests a **variable list** (register × variable × period)
plus a population definition, and — after ethical approval and SCB
processing — receives data inside MONA. Person identifiers are
pseudonymized as project-specific `LopNr` running numbers, shared
across registers so records link.

### The MONA constraint

Data delivery is not a download. SCB hosts the data inside **MONA**
(Microdata Online Access), a remote-desktop environment. Three
consequences shape the whole toolkit:

- **PII may not leave MONA.** Only aggregate, disclosure-controlled
  outputs are exported. This is the contractual basis for data access,
  not a nice-to-have.
- **No internet on MONA.** Code that runs there is self-contained:
  dependencies are pre-installed (WinPython, incl. duckdb/pyodbc/numpy)
  or amalgamated into a single uploaded `.py` (the *bundle*).
- **LLM agents are not allowed inside MONA.** This is the operational
  reason the mock-data path exists: researchers develop analysis code
  with coding agents *outside* MONA against realistic mock data that
  matches the shape of the real data inside.

### Data stewards

The central multi-tenancy axis. Some research organizations re-license
registry data from their own warehouses instead of each project going
through SCB directly:

- **global** — the full multi-agency catalog `reg_meta` indexes; orders
  go to the relevant agency.
- **ifau** — the subset in IFAU's warehouse.
- **swecov** — the subset in the SWECOV research program.

One codebase, three steward-scoped views: same UX, different catalog and
order export. See [`reg_webapp/DESIGN.md`](reg_webapp/DESIGN.md) for the
runtime steward dispatch.

## What the toolkit is

A web application (FastAPI + Svelte SPA), deployed in three
steward-scoped flavours off one image, that lets researchers browse a
catalog, author a per-project variable list, export it as a data order,
and later drive mock-data generation outside MONA from the same authored
file. Backed by Python packages usable standalone via CLI.

The unifying artifact is **`project_data.json`** — written by the
webapp, consumed by everything downstream (the MONA bundle, the mock
generator, future exporters). Its schema and structural validator are
`reg_schema` ([`reg_schema/DESIGN.md`](reg_schema/DESIGN.md)).

Pipeline coverage: explore metadata → author a variable list → order
data → mock-data bootstrap before delivery. Population definition (a
predicate over base registers, executed only inside MONA) is
deliberately out of scope.

## Package layout

Monorepo, five Python packages + one webapp, all sharing the `reg_*`
prefix. CLI binaries match package names (`reg-meta`, `reg-meta-build`,
`reg-mockdata`); no short aliases for v1.

```text
registry-research-toolkit/
  reg_meta/         # catalog query lib + CLI (binary: reg-meta)
  reg_meta_build/   # catalog DB builder (binary: reg-meta-build)
  reg_schema/       # project_data.json schema + structural validator
  reg_monabundle/   # MONA bundle build + bundle runtime + PII scanner
  reg_mockdata/     # local mock CSV generation + compare  [PLANNED — see REFACTOR_SPEC.md]
  reg_webapp/
    backend/        # FastAPI; depends on reg_meta + reg_schema + reg_monabundle
    frontend/       # Svelte 5 + Vite (bun)
    stewards/
      global/       # steward.toml only (full universe)
      ifau/         # steward.toml + steward.project_data.json   [PLANNED]
      swecov/       # steward.toml + steward.project_data.json   [PLANNED]
```

> Current vs target: `reg_mockdata` does **not** exist yet — its code
> still lives in `mock_data_wizard/` (reg_meta-coupled, not yet
> renamed/split). Only the `global/` steward dir is populated. These are
> the headline remaining items in [`REFACTOR_SPEC.md`](REFACTOR_SPEC.md).

Dependency graph (acyclic):

```text
reg_meta_build → reg_meta
reg_webapp     → reg_meta, reg_schema, reg_monabundle
reg_monabundle → reg_schema
reg_mockdata   → reg_schema
reg_schema     → (none)
```

Each Python package releases to PyPI on its own tag (`reg_meta/v*`,
`reg_meta_build/v*`, …); the webapp ships as a container image on
`reg_webapp/v*`.

### Why this split

- **`reg_meta` vs `reg_meta_build`** — different deps (query needs only
  stdlib `sqlite3`; build needs CSV/Excel parsers), cadence, and
  operators. The built SQLite DBs (`reg_meta.db` plus the smaller
  `reg_meta_docs.db`) are too large to ship inside the wheel and are
  distributed as `.zst`-compressed **GitHub release artifacts** on
  `reg_meta/v*` tags; `reg-meta update` fetches the matching version
  into `$XDG_DATA_HOME/reg_meta/`. Mirrors the
  build/runtime separation a future Go/Rust port of the query layer
  would need.
- **`reg_schema` standalone** — the `project_data.json` schema has many
  consumers (webapp authors it, `reg_monabundle` validates it on MONA,
  `reg_mockdata` reads it, future exporters read it). Tiny, focused, no
  `reg_meta` dep: the schema uses string IDs and leaves resolution to
  the consumer.
- **`reg_monabundle` vs `reg_mockdata`** — the MONA bundle (built by the
  webapp, runs on MONA) and the local mock generator (runs on a
  researcher's laptop) are different tools with different deps and
  audiences. `reg_monabundle` is two halves: a *lightweight* surface
  (`build`, `scan`, `validate`) imported by the webapp, and a *runtime*
  surface (`runtime.*`) amalgamated into the bundle. The runtime's heavy
  deps (duckdb/pyodbc) are lazily imported on use and will be declared
  behind a `runtime` extras group (currently empty, reserved); MONA's
  WinPython preinstalls them, and the webapp container installs
  `reg_monabundle` without extras so it never pulls them.
- **`reg_meta` is absent from MONA-side code** — types come from the
  spec (authored against `reg_meta` in the webapp), so neither the
  bundle nor `reg_mockdata` needs `reg_meta` in-process.

## Repo-wide invariants

These are hygiene that keeps options open, enforced in CI where noted.
Package-local mechanisms are documented in the owning DESIGN.md and only
summarized here.

- **No Pydantic on library surfaces.** `reg_meta`, `reg_meta_build`,
  `reg_monabundle`, `reg_mockdata` model with `@dataclass` so they
  import from any context (Jupyter, scripts, the MONA bundle).
  `reg_schema` is the deliberate exception (it is the canonical
  validator and the webapp's response-model source); the build-side IR
  in `reg_meta_build` is Pydantic but build-time-only and never reaches
  the bundle. See `reg_schema/DESIGN.md` and `reg_meta_build/DESIGN.md`.
- **Build / runtime cleanly separated.** `reg_meta` (query) is small and
  pure; `reg_meta_build` is operator-side. A future port replaces query
  only; build stays Python.
- **Stateless server.** No process-local caches that change behavior
  across requests; the catalog DB is opened read-only.
- **OpenAPI is the canonical contract.** `reg_webapp/backend/openapi.json`
  is committed and regenerated by `reg_webapp/backend/scripts/gen_openapi.py`. CI guards
  drift with a pytest **snapshot test**
  (`reg_webapp/backend/tests/test_openapi_snapshot.py`)
  that asserts the committed file equals a fresh render, and the
  frontend CI job runs `bun run gen:types` + `git diff --exit-code` so
  the codegen'd TS types stay in sync. (There is no `make` target —
  drift is a failing test, not a Makefile step.) A future Go/Rust port
  of the query API reproduces the same spec; clients are unaffected.
- **Bundle-size budget — 1 MB.** The MONA bundle's emitted `.py` is
  capped at 1 MB (uploaded through MONA's GUI per round-trip);
  `reg_monabundle/tests/test_bundle_size_budget.py` byte-counts it in
  CI. Current ~104 KB.
- **Performance budget (v1 targets, not yet enforced).** Starting points:
  `/api/catalog/*` p95 ≤ 200 ms (cache miss); `/api/project/validate`,
  `/api/project/order` p95 ≤ 1 s; `/api/bundle`, `/api/kit` p95 ≤ 5 s.
  The 200-column load-test fixture is committed
  (`reg_schema/test_corpus/load_test_200col/`) and reused by the
  bundle-size test, but the load-test harness and CI perf gate are
  remaining work (see REFACTOR_SPEC.md).
- **Cross-package version compatibility.** `reg_webapp` **floor-pins**
  its runtime deps (`reg-meta>=…`, `reg-schema>=…`, `reg-monabundle>=…`),
  not exact pins: the packages resolve via `[tool.uv.sources]` in the
  workspace, and exact pins would force monorepo-wide lockstep without
  enabling out-of-workspace builds. `reg_meta_build` releases
  independently (it produces the DB asset `reg_meta` fetches).
  `reg_mockdata` (once it exists) floor-pins `reg_schema` so kits
  authored against a newer webapp still generate on a slightly-older
  local install. Schema breakage is signalled by `project_data.json`'s
  `schema_version` (major 2 = Model A); per the compatibility policy
  below, v1 ships no migration shims.

## API style

The webapp API is **REST**, not GraphQL or tRPC: edge-cacheability is
the primary cost lever (`/api/catalog/*` reads are `Cache-Control`d and
ETagged so Cloudflare absorbs repeat traffic), and a stable resource
grammar is the thing a future port must reproduce. See
[`reg_webapp/DESIGN.md`](reg_webapp/DESIGN.md).

## Testing strategy

Eight load-bearing test categories span the packages; the consolidated
view (full detail in each owning DESIGN.md):

1. **Shared validator corpus** — `reg_schema/test_corpus/` golden
   `(input.json, expected_ValidationResult.json)` pairs, run by **two**
   consumers: `reg_schema`'s Python tests and the SPA's TS tests. (The
   MONA bundle amalgamates the §6.8.2 namespaced-block validator, not
   the structural corpus.) — shipped.
2. **FQID property tests** — round-trip, segment-count discrimination,
   reserved-slug rejection, slug-immutability snapshot. — shipped.
3. **Bundle determinism** — same spec → byte-identical `.py` (ast.unparse
   amalgamation), asserted by
   `reg_monabundle/tests/test_bundle_determinism.py`. — shipped.
4. **Kit reproducibility** — same spec + codes + stats → identical kit
   zip. **Remaining** (blocked on `/api/kit`).
5. **Steward catalog filtering** — `fqid_outside_steward_catalog`
   semantics. Machinery shipped (the boot-time drop); wiring into
   `/validate` is an open gap (issue-tracked).
6. **MONA-shape integration** — a `mcr.microsoft.com/mssql/server` Docker
   container running the bundle's extract SQL end-to-end via `pyodbc`
   (`reg_monabundle/tests/test_integration_mssql.py`), gated behind
   `@pytest.mark.integration` and skipping cleanly without Docker/pyodbc.
   — shipped. (The marker is also used by `reg_meta`'s install test.)
7. **Per-deploy smoke tests** — golden `/api/context` + shallow
   `/api/catalog` walk on container start. **Remaining** (no deployment
   yet).
8. **Server-side input-validation gates** — period canonicalization and
   FQID route-segment validation (422-before-SQL), provider-ID namespace
   property, provenance-DB confinement. — shipped (see
   `reg_webapp/DESIGN.md` and `reg_meta_build/DESIGN.md`). Plus the
   grow-only PII-scanner regression corpus (`reg_monabundle`).

## Maturity and compatibility policy

Pre-v1, small group of testers, no external users. Breaking changes are
clean breaks; testers re-author affected projects. The toolkit does
**not** ship migration scripts, deprecation wrappers, or
backwards-compatibility shims — `project_data.json` carries a
`schema_version` so a future self *can* migrate, but `reg_mockdata`
refuses an incompatible kit with a clear error rather than shimming it.
This policy is the canonical statement in [`AGENTS.md`](AGENTS.md)
("Maturity and compatibility"); it is revisited only when the toolkit
graduates to a wider user base.

## Where the dissolved spec went

The Model A refactor spec dissolved into the docs below. Per-PR landing
history lives in git (the `MIGRATION_PLAN.md` tracker was retired once
A5 shipped).

| Old spec section | New home |
|---|---|
| §1 background, §2/§3 product, §4 layout/deps, §9.3 REST, §11 changes, §12 invariants, §13 policy, §16 overview | this file |
| §5 object model, FQID grammar, edge semantics, library API, glossary | `reg_meta/DESIGN.md` |
| §4.4 IR/adapter, §5.3 slug curation, §5.4 immutability, §5.6 lineage, §5.7 triage, ID minting | `reg_meta_build/DESIGN.md` |
| §5.9, §6 `project_data.json` schema + structural/return-shape rules | `reg_schema/DESIGN.md` |
| §6.8.3 semantic rules, §9 webapp | `reg_webapp/DESIGN.md` |
| §7 (shipped bundle bits), §10-bundle, §16 PII/determinism | `reg_monabundle/DESIGN.md` |
| §6.6 codes, §7 realign/merged-mode, §8 stats+kit, §9 deployment/kit/stewards, §10 mockdata, §14 open decisions, §15 steps 6.5–12, remaining §16 | `REFACTOR_SPEC.md` (remaining work) |
