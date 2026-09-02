# Architecture

Cross-cutting design for the Registry Research Toolkit — the topology, dependency graph,
and repo-wide invariants that no single package owns. Package-local rationale lives in
each `<package>/DESIGN.md`; remaining (not-yet-built) work lives in
[`REFACTOR_SPEC.md`](REFACTOR_SPEC.md).

This document is the durable home for what used to be §1–§4, §9.3, §11–§13, and the §16
overview of the now-dissolved Model A refactor spec. The Model A migration (the
two-level catalog, the FQID grammar, the IR/adapter build, `reg_schema` v2, the webapp +
SPA) shipped; its design rationale now lives in the package DESIGN.md files this
document points to.

## Domain

Swedish register research uses administrative microdata produced by Statistics Sweden
(SCB) and other agencies (Socialstyrelsen, Försäkringskassan, …). Each agency publishes
**registers** — population-scale administrative datasets named LISA (labour market), RTB
(population), PAR (patient registry), FRIDA (firms), etc. A register holds **variables**
(columns), each with a stable identifier, definition, data type, and — for categorical
variables — an enumerated **value set** (`Kön ∈ {1=Man, 2=Kvinna}`). Value sets are
versioned (SUN2000 vs SUN2020) and registers themselves drift edition to edition.

A project requests a **variable list** (register × variable × period) plus a population
definition, and — after ethical approval and SCB processing — receives data inside MONA.
Person identifiers are pseudonymized as project-specific `LopNr` running numbers, shared
across registers so records link.

### The MONA constraint

Data delivery is not a download. SCB hosts the data inside **MONA** (Microdata Online
Access), a remote-desktop environment. Three consequences shape the whole toolkit:

- **PII may not leave MONA.** Only aggregate, disclosure-controlled outputs are
  exported. This is the contractual basis for data access, not a nice-to-have.
- **No internet on MONA.** Code that runs there is self-contained: dependencies are
  pre-installed (WinPython, incl. duckdb/pyodbc/numpy) or amalgamated into a single
  uploaded `.py` (the *bundle*).
- **LLM agents are not allowed inside MONA.** This is the operational reason the
  mock-data path exists: researchers develop analysis code with coding agents *outside*
  MONA against realistic mock data that matches the shape of the real data inside.

### Data stewards

The central multi-tenancy axis. Some research organizations re-license registry data
from their own warehouses instead of each project going through SCB directly:

- **global** — the full multi-agency catalog `reg_meta` indexes; orders go to the
  relevant agency.
- **ifau** — the subset in IFAU's warehouse.
- **swecov** — the subset in the SWECOV research program.

One codebase, three steward-scoped views: same UX, different catalog and order export.
See [`reg_webapp/DESIGN.md`](reg_webapp/DESIGN.md) for the runtime steward dispatch;
[`reg_meta_build/DESIGN.md`](reg_meta_build/DESIGN.md) § "Steward-flavored DB —
extend-db (#365 PR2)" for the build-side `extend-db` machinery (ships steward-only
registers/variables on top of a released global DB).

## What the toolkit is

A web application (FastAPI + Svelte SPA), designed for three steward-scoped flavours off
one image, that lets researchers browse a catalog, author a per-project variable list,
and export it as a data order. The human SPA and agent/CLI are equal v1 product
surfaces. The current CLI covers catalog metadata, not project validate/order yet; §12's
shared materializer closes that parity gap rather than duplicating the workflow. The SPA
will call it through FastAPI; the agent/CLI v1 path will load the versioned catalog DB
and public delivery inventory locally and emit byte-identical results without depending
on a deployed API.

The unifying research-intent artifact is **`project_data.json`** — written by the
webapp, consumed by future exporters and the planned MONA runner rebuild. Its schema and
structural validator are `reg_schema` ([`reg_schema/DESIGN.md`](reg_schema/DESIGN.md)).
It deliberately does not encode physical filenames or SQL tables. The v1 steward
boundary will add a separate public delivery inventory
(`table + edition → literal columns → zero-or-more logical mappings`); joining a
project, reg_meta resolution, and that optional inventory will produce one normalized
order manifest for both web and CLI consumers. See `REFACTOR_SPEC.md` §12.

Current shipped coverage is explore metadata → author a variable list → provisional
seven-column binding CSV. The normalized physical delivery manifest remains v1 work; the
former mock-data bootstrap is archived pending the from-scratch MONA rebuild. Population
definition (a predicate over base registers, executed only inside MONA) is deliberately
out of scope.

## Package layout

Monorepo, four Python packages + one webapp, all sharing the `reg_*` prefix. CLI
binaries match package names (`reg-meta`, `reg-meta-build`); no short aliases for v1.

```text
registry-research-toolkit/
  reg_meta/         # catalog query lib + CLI (binary: reg-meta)
  reg_meta_build/   # catalog DB builder (binary: reg-meta-build)
  reg_schema/       # project_data.json schema + structural validator
  reg_webapp/
    backend/        # FastAPI; depends on reg_meta + reg_schema
    frontend/       # Svelte 5 + Vite (bun)
    stewards/
      global/       # steward.toml only (full universe)
      ifau/         # steward.toml + steward.project_data.json   [PLANNED]
      swecov/       # steward.toml + current steward.project_data.json
```

> The `reg_monabundle` and `mock_data_wizard` packages have been archived to
> `archive/mona-subsystem` (tag `mona-subsystem-pre-rebuild`), pending a from-scratch
> MONA rebuild. The `global/` and `swecov/` steward dirs are populated. See
> [`REFACTOR_SPEC.md`](REFACTOR_SPEC.md).

Dependency graph (acyclic):

```text
reg_meta_build → reg_meta
reg_meta       → reg_schema
reg_webapp     → reg_meta, reg_schema
reg_schema     → (none)
```

Each Python package releases to PyPI on its own tag (`reg_meta/v*`, `reg_meta_build/v*`,
…); the webapp ships as a container image on `reg_webapp/v*`.

### Why this split

- **`reg_meta` vs `reg_meta_build`** — different deps (query needs only stdlib
  `sqlite3`; build needs CSV/Excel parsers), cadence, and operators. The built SQLite
  DBs (`reg_meta.db` plus the smaller `reg_meta_docs.db`) are too large to ship inside
  the wheel and are distributed as `.zst`-compressed **GitHub release artifacts** on
  `reg_meta/v*` tags; `reg-meta update` fetches the matching version into
  `$XDG_DATA_HOME/reg_meta/`. Mirrors the build/runtime separation a future Go/Rust port
  of the query layer would need.
- **`reg_schema` standalone** — the `project_data.json` schema has many consumers
  (webapp authors it, future exporters and the planned MONA runner rebuild read it).
  Tiny, focused, no `reg_meta` dep: the schema uses string IDs and leaves resolution to
  the consumer.

## Repo-wide invariants

These are hygiene that keeps options open, enforced in CI where noted. Package-local
mechanisms are documented in the owning DESIGN.md and only summarized here.

- **No Pydantic rule (historical; now retired).** `reg_meta` adopted Pydantic for its
  catalog return surface in #681 (2026-06-22); the earlier no-Pydantic preference was a
  soft import-ergonomics call, not a MONA constraint (`reg_meta` is absent from
  MONA-side code). `reg_schema` is Pydantic (canonical validator + webapp response-model
  source); the build-side IR in `reg_meta_build` is Pydantic but build-time-only. The
  hard MONA air-gap rule (no Pydantic + stdlib-only module-level imports) applied to
  `reg_monabundle`'s amalgamated bundle — that package is now archived; see
  `REFACTOR_SPEC.md`. Decided 2026-06-22 (#680 re-attribution, #681 adoption). See
  `reg_schema/DESIGN.md` and `reg_meta_build/DESIGN.md`.
- **Build / runtime cleanly separated.** `reg_meta` (query) is small and pure;
  `reg_meta_build` is operator-side. A future port replaces query only; build stays
  Python.
- **Stateless server.** No process-local caches that change behavior across requests;
  the catalog DB is opened read-only.
- **OpenAPI is the canonical contract.** `reg_webapp/backend/openapi.json` is committed
  and regenerated by `reg_webapp/backend/scripts/gen_openapi.py`. CI guards drift with a
  pytest **snapshot test** (`reg_webapp/backend/tests/test_openapi_snapshot.py`) that
  asserts the committed file equals a fresh render, and the frontend CI job runs
  `bun run gen:types` + `git diff --exit-code` so the codegen'd TS types stay in sync.
  (There is no `make` target — drift is a failing test, not a Makefile step.) A future
  Go/Rust port of the query API reproduces the same spec; clients are unaffected.
- **Performance budget (v1 targets, not yet CI-enforced).** Starting points:
  `/api/catalog/*` p95 ≤ 200 ms (cache miss); `/api/project/validate` and
  `/api/project/order` p95 ≤ 1 s; representative broad all-scope `/api/search`
  cache-miss p95 ≤ 500 ms and browser-cold search LCP < 2.5 s. Search correctness and
  latency are origin properties: edge hits do not substitute for cold-origin evidence.
  Classification/value-set initial responses must be bounded by bucket/page limits
  rather than total code cardinality, and cold/repeat rendered routes target CLS < 0.1.
  The 200-column load-test fixture is committed
  (`reg_schema/test_corpus/load_test_200col/`), but the load-test harness and CI perf
  gate are remaining work (see `REFACTOR_SPEC.md`).
- **Cross-package version compatibility.** `reg_webapp` **floor-pins** its runtime deps
  (`reg-meta>=…`, `reg-schema>=…`), not exact pins: the packages resolve via
  `[tool.uv.sources]` in the workspace, and exact pins would force monorepo-wide
  lockstep without enabling out-of-workspace builds. `reg_meta_build` releases
  independently (it produces the DB asset `reg_meta` fetches). Schema breakage is
  signalled by `project_data.json`'s `schema_version` (major 2 = Model A); per the
  compatibility policy below, v1 ships no migration shims.

## API style

The webapp API is **REST**, not GraphQL or tRPC: edge-cacheability is the primary cost
lever (`/api/catalog/*` reads are `Cache-Control`d and ETagged so Cloudflare absorbs
repeat traffic), and a stable resource grammar is the thing a future port must
reproduce. See [`reg_webapp/DESIGN.md`](reg_webapp/DESIGN.md).

## Testing strategy

Six load-bearing test categories span the packages; the consolidated view (full detail
in each owning DESIGN.md):

1. **Shared validator corpus** — `reg_schema/test_corpus/` golden
   `(input.json, expected_ValidationResult.json)` pairs, run by **two** consumers:
   `reg_schema`'s Python tests and the SPA's TS tests. — shipped.
2. **FQID property tests** — round-trip, segment-count discrimination, reserved-slug
   rejection, slug-immutability snapshot. — shipped.
3. **Kit reproducibility** — same spec + codes + stats → identical kit zip.
   **Remaining** (see REFACTOR_SPEC.md).
4. **Steward catalog filtering** — `fqid_outside_steward_catalog` /
   `representation_outside_steward_catalog` semantics. Shipped: boot-time catalog drop +
   wiring into `/validate` (issue #227); column-based admission (issue #206); browse +
   search scoping (issue #859) — catalog root/provider/register/binding narrowed to held
   holdings, search register/variable surfaces scoped via `fqids` allow-list.
5. **Per-deploy smoke tests** — golden `/api/context` + shallow `/api/catalog` walk on
   container start. **Remaining** (no deployment yet).
6. **Server-side input-validation gates** — period canonicalization and FQID
   route-segment validation (422-before-SQL), provider-ID namespace property,
   provenance-DB confinement. — shipped (see `reg_webapp/DESIGN.md` and
   `reg_meta_build/DESIGN.md`).

## Maturity and compatibility policy

Pre-v1, small group of testers, no external users. Breaking changes are clean breaks;
testers re-author affected projects. The toolkit does **not** ship migration scripts,
deprecation wrappers, or backwards-compatibility shims — `project_data.json` carries a
`schema_version` so a future self *can* migrate, but `reg_mockdata` refuses an
incompatible kit with a clear error rather than shimming it. This policy is the
canonical statement in [`AGENTS.md`](AGENTS.md) ("Maturity and compatibility"); it is
revisited only when the toolkit graduates to a wider user base.

## Where the dissolved spec went

The Model A refactor spec dissolved into the docs below. Per-PR landing history lives in
git (the `MIGRATION_PLAN.md` tracker was retired once A5 shipped).

  | Old spec section                                                                                                    | New home                            |
  | ------------------------------------------------------------------------------------------------------------------- | ----------------------------------- |
  | §1 background, §2/§3 product, §4 layout/deps, §9.3 REST, §11 changes, §12 invariants, §13 policy, §16 overview      | this file                           |
  | §5 object model, FQID grammar, edge semantics, library API, glossary                                                | `reg_meta/DESIGN.md`                |
  | §4.4 IR/adapter, §5.3 slug curation, §5.4 immutability, §5.6 lineage, §5.7 triage, ID minting                       | `reg_meta_build/DESIGN.md`          |
  | §5.9, §6 `project_data.json` schema + structural/return-shape rules                                                 | `reg_schema/DESIGN.md`              |
  | §6.8.3 semantic rules, §9 webapp                                                                                    | `reg_webapp/DESIGN.md`              |
  | §7 (bundle), §10-bundle, §16 PII/determinism (archived)                                                             | `archive/mona-subsystem`            |
  | §6.6 codes, §8 stats+kit, §9 deployment/stewards, §10 mockdata, §14 open decisions, §15 steps 6.5–12, remaining §16 | `REFACTOR_SPEC.md` (remaining work) |
