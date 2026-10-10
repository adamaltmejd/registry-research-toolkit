# Architecture

Cross-cutting design for the Registry Research Toolkit — the topology, dependency graph,
and repo-wide invariants that no single package owns. Package-local rationale lives in
each `<package>/DESIGN.md`; remaining (not-yet-built) work lives in
[`REFACTOR_SPEC.md`](REFACTOR_SPEC.md) and, for the Rust runtime and compiled-catalog
refactor, [`RUST_RUNTIME_SPEC.md`](RUST_RUNTIME_SPEC.md).

This document is the durable home for what used to be §1–§4, §9.3, §11–§13, and the §16
overview of the now-dissolved Model A refactor spec. The Model A migration (the
two-level catalog, the FQID grammar, the IR/adapter build, the `project_data.json` v2
schema, the webapp + SPA) shipped; its design rationale now lives in the package
DESIGN.md files this document points to.

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

- **global** — the full multi-agency catalog; orders go to the relevant agency.
- **ifau** — the subset in IFAU's warehouse.
- **swecov** — the subset in the SWECOV research program.

One codebase, steward-scoped views: same UX, different holdings and order target. The
compiled-holdings contract below gives each steward one self-identifying SQLite artifact
containing full reference metadata plus its physical holdings. TOML inventory and policy
are accepted builder inputs; readers do not load them. SWECOV proves the first cut; IFAU
delivery authoring remains outside it. See
[`reg_meta_build/DESIGN.md`](reg_meta_build/DESIGN.md) § "Steward extension" and
[`reg_webapp/DESIGN.md`](reg_webapp/DESIGN.md) for the build and runtime boundaries.

## What the toolkit is

A web application (Rust server + Svelte SPA), designed for three steward-scoped flavours
off one image, that lets researchers browse a catalog, author a per-project variable
list, and export it as a data order. The HTTP API (which the SPA uses) and the MCP tools
are equal surfaces over the same `reg-catalog` operations. The SPA validates the draft
automatically on every edit.

The unifying research-intent artifact is **`project_data.json`** — written by the
webapp, consumed by the shared materializer above and the planned MONA runner rebuild.
Its schema and structural validator are `reg-core`'s
([`crates/reg-core/DESIGN.md`](crates/reg-core/DESIGN.md)). It deliberately does not
encode physical filenames or SQL tables. Compiled physical holdings
(`table + edition → literal columns → zero-or-more explicit logical mappings`) join a
project and query-time catalog resolution to produce one normalized, versioned JSON
order manifest for web and MCP, with no per-steward export template. The materializer
reads the selected compiled artifact for `global`/`swecov`; inventory remains a builder
input.

Current shipped coverage is browse/search the catalog → choose variables and periods →
automatic validation → the versioned JSON order manifest. The former mock-data bootstrap
is archived pending the from-scratch MONA rebuild. Population definition (a predicate
over base registers, executed only inside MONA) is deliberately out of scope.

## Package layout

Monorepo: one Python build package, the Rust runtime crates and one webapp. CLI binaries
are `reg-meta` (the runtime) and `reg-meta-build` (the builder); no short aliases for
v1.

```text
registry-research-toolkit/
  reg_meta_build/   # catalog DB builder, Python (binary: reg-meta-build)
  crates/
    reg-core/       # project_data.json contract + structural validator, grammars
    reg-catalog/    # catalog reader and its operations
    reg-meta/       # binary: `serve` (HTTP API + /mcp) and `mcp` (stdio)
    reg-core-py/    # PyO3 bindings of reg-core for the builder
  conformance/      # cross-adapter contract corpus, run against the Rust server
  plugins/          # the microdata-tools-se agent plugin (hosted MCP)
  reg_webapp/
    frontend/       # Svelte 5 + Vite (bun)
    edge/           # edge worker serving the SPA
    stewards/
      global/       # steward.json only (full universe)
      swecov/       # steward.json (compiled holdings artifact)
```

> The `reg_monabundle` and `mock_data_wizard` packages have been archived to
> `archive/mona-subsystem` (tag `mona-subsystem-pre-rebuild`), pending a from-scratch
> MONA rebuild. The `global/` and `swecov/` steward dirs are populated. See
> [`REFACTOR_SPEC.md`](REFACTOR_SPEC.md).

Dependency graph (acyclic):

```text
reg-meta       → reg-catalog → reg-core
reg-core-py    → reg-core
reg_meta_build → reg-core-py
reg_webapp     → reg-meta (HTTP, through the codegen'd OpenAPI types)
```

Nothing is published to PyPI. The runtime releases on `reg_meta/v*` tags: the `reg-meta`
crate version, the DB assets and the `reg-meta` binaries (`RUST_RUNTIME_SPEC.md` package
4.12). `reg_meta_build` is tagged (`reg_meta_build/v*`) but not published: it depends on
the workspace-only `reg-core-py` and runs from a maintainer checkout. The webapp ships
as a container image built from the newest `reg_meta/v*` release's assets.

### Why this split

- **Runtime vs builder** — the Rust runtime reads; the Python builder writes. They have
  different deps, cadence and operators, and share only the artifact contract and
  `reg-core` (through `reg-core-py`). The built SQLite DBs (`reg_meta.db` plus the
  smaller `reg_meta_docs.db`) are distributed as `.zst`-compressed **GitHub release
  artifacts** on `reg_meta/v*` tags, fetched with `curl`, SHA-256 verification and
  `zstd`. See [`crates/DESIGN.md`](crates/DESIGN.md) for the runtime and
  [`crates/reg-core/DESIGN.md`](crates/reg-core/DESIGN.md) for the project contract.

### Accepted source revisions and catalog generations

`reg_meta_build` follows a four-step contract: provider adapters condense raw deliveries
into one common compact form; provider-generic rules process it on every build; tracked
curation decides what the rules leave unresolved; and the build writes the SQLite
catalog, erroring on anything unresolved until curation resolves or acknowledges it.
Machine-readable inputs are selected by the exact commit and manifest of a host-local
input repository; curation is tracked TOML in this repository. Builds reuse validated
preparation; they never call LLMs or extract PDFs. The catalog embeds its
prepared-source pins, and every build records a digest of the curation it used. See
`reg_meta_build/DESIGN.md` for the contracts, the update workflow and what is still
transitional.

The runtime and `reg_webapp` consume only the activated SQLite generation; they never
read the input repository or recompute authority. Diagnostic builds write a separate
nonpublishable database with unresolved discrepancies and withheld output. They cannot
replace the active catalog. When correction detail is exposed, it remains collapsed into
the existing catalog metadata rather than creating a second public evidence API.

The accepted input branch and active catalog are related but distinct local states. A
candidate is reconciled, built, validated, and compared before its input commit is
accepted. Catalog publication then atomically activates the artifact and its embedded
pins. A failed build leaves both states unchanged; a publication failure after input
acceptance may leave the input branch ahead while the active catalog retains its prior
embedded pins. Runtime readers must never combine those two generations implicitly.

The document index and steward inventories retain separate authority. Indexing or
preserving a handbook/workbook does not make it a catalog source, and a steward's
holdings establish possession only for that steward. The LISA supplemental reader enters
through `reg_meta_build`'s machine-readable source-record boundary; PDF extraction stays
upstream. None of these inputs cross the MONA boundary, and no source reconciliation
changes the rule that PII stays in MONA and only aggregate, disclosure-controlled
results leave it.

### Real-seed output cache

Real-seed `prepare-sources` and `build-db` runs take one to two hours each, so
`scripts/real_seed_cache.py` reuses their outputs by input key (#1329). It is tooling
over the builder's existing reports and checks, not builder code. Each key covers only
what its step reads, so a resolution change reuses the preparation; the module docstring
lists the fields. The invariant is that a key leaving out a real input would return
stale output silently, so every key errs generous, a hit re-checks what it returns, only
completed runs are stored, and `build --verify` reruns uncached and compares bytes. The
digest, staging and eviction helpers it shares with the G1 derive cache and the
synthetic fixture cache live in `scripts/keyed_cache.py`. Phase caching inside the
builder waits for the builder work in #1296.

### Catalog artifact identity and read scope

**Compiled-holdings contract (2026-10-04).** A publishable `reg_meta.db` identifies
itself through `import_manifest.catalog_artifact_kind`: `catalog` for a global reference
artifact, `steward` for reference metadata plus that steward's compiled holdings. Both
use the same schema. A steward installation needs no separate global DB to inspect
unheld metadata. The optional sibling `reg_meta_docs.db` remains separately paired; full
document content is outside the single-file guarantee. Diagnostic/incomplete artifacts
remain nonpublishable and fail runtime admission.

Identity stays in `import_manifest`, including `generation_id`; a steward also records
`steward`, `base_db_sha256`, `base_generation_id`, `holdings_input_commit`,
`holdings_manifest_sha256` and `holdings_policy_sha256`. Generation identity hashes
canonical semantic inputs and the schema version, not output bytes, timestamps or host
paths. Builder revision, accepted public pins and curation digest participate; steward
identity additionally includes its base generation, accepted holdings pins, policy and
accounting digest. The exact base file digest is byte-level provenance only and does not
enter generation identity. Publish the final file SHA-256 separately. The builder design
specifies the digest encoding. Search cursors and `OrderProvenance` use `generation_id`
instead of `import_date`; HTTP read identities include generation and scope. Import time
may remain display data.

Read scope defaults to `holdings` on a steward artifact and `reference` on a catalog
artifact; explicit `holdings` on a catalog artifact errors. The holdings predicate
applies to provider, register and variable binding/state nodes, using the same compiled
mapping membership before counts, groups or pagination. Classifications, value sets,
documents and lineage stay reference metadata. Query-time resolution intersects physical
periods with applicable states; no resolved segments are compiled. Unknown-scope
holdings remain physical evidence, cannot admit logical nodes and cannot be ordered. The
public-surface rules and exceptions live in `crates/DESIGN.md` → "Read scope and
artifact admission".

The reader and webapp open the selected artifact read-only and validate its identity.
`REG_WEBAPP_STEWARD` must equal its manifest steward; unset/global selects the `catalog`
kind, not an implicit fallback for a named steward. Orders use
`materialize_order(project, conn)` and the artifact's kind, independent of browse scope.
Accepted inventory/policy/raw-census accounting is a build gate with counts and digests
in the manifest; exclusions and lookups are not runtime holdings. Failures leave prior
artifacts and accepted inputs unchanged.

## Repo-wide invariants

These are hygiene that keeps options open, enforced in CI where noted. Package-local
mechanisms are documented in the owning DESIGN.md and only summarized here.

- **Build / runtime cleanly separated.** The Rust runtime (`crates/`) only reads the
  artifact; `reg_meta_build` is operator-side, stays Python and is the only writer.
- **Stateless server.** No process-local caches that change behavior across requests;
  the catalog DB is opened read-only.
- **OpenAPI is the canonical contract.** `crates/reg-meta/openapi.json` is committed and
  regenerated by `scripts/gate.py regen`. CI guards drift with a **snapshot test**
  (`cargo test -p reg-meta --test openapi_snapshot`) that asserts the committed file
  equals the server's rendered document, and the frontend CI job runs
  `bun run gen:types` + `git diff --exit-code` so the codegen'd TS types stay in sync.
  (There is no `make` target — drift is a failing test, not a Makefile step.)
- **Performance budget (v1 targets, not yet CI-enforced).** Starting points: the catalog
  reads the Rust server answers (`/api/catalog/*` and the facets
  `/api/{states,warnings,values,graph,lineage}/*`) p95 ≤ 200 ms (cache miss);
  `/api/project/validate` and `/api/project/order` p95 ≤ 1 s; representative broad
  all-scope `/api/search` cache-miss p95 ≤ 500 ms and browser-cold search LCP < 2.5 s.
  Search correctness and latency are origin properties: edge hits do not substitute for
  cold-origin evidence. Classification/value-set initial responses must be bounded by
  bucket/page limits rather than total code cardinality, and cold/repeat rendered routes
  target CLS < 0.1. The 200-column load-test fixture is committed
  (`crates/reg-core/tests/project/corpus/load_test_200col/`), but the load-test harness
  and CI perf gate are remaining work (see `REFACTOR_SPEC.md`).
- **Project schema version.** Schema breakage is signalled by `project_data.json`'s
  `schema_version` (major 2+ = Model A — 3 since the whole-history `Source.period`
  sentinel was removed; bare `_default` now selects year-independent data only at a
  concrete variant). Per the compatibility policy below, v1 ships no migration shims.

## API style

The webapp API is **REST**, not GraphQL or tRPC: edge-cacheability is the primary cost
lever (the Rust server's catalog reads, `/api/catalog/*` and its facets, carry
`Cache-Control` and ETags so Cloudflare absorbs repeat traffic), and a stable resource
grammar is the thing a future port must reproduce. See
[`reg_webapp/DESIGN.md`](reg_webapp/DESIGN.md).

## Testing strategy

The toolkit is a deterministic compiler feeding an immutable artifact to stateless
readers, so its behavior is observable at a small number of boundaries that outlive the
code behind them. Tests assert at those boundaries and nowhere else; the binding rules
for agents are in `AGENTS.md` → "Testing policy". This section names the boundaries, the
oracle each one uses, and the three cost tiers tests run in.

### Boundaries and oracles

  | Boundary                                 | Oracle (data, not code)                                                                                 | Status  |
  | ---------------------------------------- | ------------------------------------------------------------------------------------------------------- | ------- |
  | Prepared inputs + curation → artifact    | synthetic source fixtures → `validate_built_db` + content snapshot                                      | shipped |
  | Artifact → HTTP and MCP                  | `openapi.json` snapshot + request → response goldens run against the Rust server over the fixture DB    | shipped |
  | `project_data.json` → validation result  | `crates/reg-core/tests/project/corpus/` run by the Rust and TS consumers                                | shipped |
  | Project + artifact → order manifest      | byte-identical `order.json` goldens, cross-adapter identity                                             | shipped |
  | Curation TOML → load or located failure  | committed TOML must load; malformed cases name the locator                                              | shipped |
  | FQID / period grammars, interval algebra | Hypothesis properties + round-trip snapshots                                                            | shipped |
  | Real artifact ↔ accepted inputs          | conformance artifact checks and explicit accepted-input table/cell census, policy and authored mappings | shipped |

A function that is not one of these is reached through one that is. Structural artifact
invariants have one authority, `validate_built_db`; tests run it, they do not re-derive
its checks. Expected outputs are files reviewed in the diff; a golden that changes is a
content decision, not a test fix.

Compiled holdings admission, scoped browse/search and selection guards are pinned
through source-built request/expected corpora in `conformance/cases/`. Public library
return-model cases retain contracts that have no equivalent CLI or HTTP projection.

Conformance cases exercise only public contracts: CLI JSON, HTTP responses, order
manifests, or documented public library return-model contracts. A public function name
alone does not establish a contract: cases assert observable domain results or located
errors, never object internals, call graphs or query implementation, and no product
adapter exists solely to expose a test seam. Requests and expected results stay readable
as data; Python-specific operation names can be revised in a separate portability pass
after a byte-identical relocation.

### Tiers

1. **Package suite (budgeted, every change).** Contract tests over synthetic fixtures
   built from readable source, property tests, golden corpora: the default `pytest`,
   `cargo test --workspace` and `bun run test`. Run narrowed by package while iterating.
   Each package stays inside its budget below.
2. **Push / CI.** `ci.yml` runs the Python suites as a `test` matrix, one leg per root
   `testpaths` entry, each with `timeout-minutes` at its CI budget below. The `rust`
   job's 3-minute timeout is the `crates/` budget, and it also covers the gate's `rust`
   and `release` steps (`scripts/gate.py`: the workspace build, the whole conformance
   suite against the Rust server, release admission on the synthetic steward artifact).
   The `hook-tests` job runs the Claude Code hook tests (`.claude/hooks/tests`, plain
   bash) with a 2-minute timeout. The `reg-webapp-frontend` job has a 6-minute timeout
   and includes the codegen drift check; the OpenAPI snapshot is a `crates/reg-meta`
   test. A job that exceeds its budget fails. The Playwright drivers (`dev.sh smoke`,
   the gate's `flows` step) are local checks, not CI jobs.
3. **Artifact (maintainer or release gate).** Run
   `pytest conformance --run-release --artifact-dir=/path/to/catalog --server-cmd=...`
   after `cargo build --workspace` (the search traversal runs against the Rust server;
   see `conformance/README.md` for the template). The reader admits the selected
   artifact before execution; incompatible or non-publishable artifacts fail, never
   silently skip. The artifact checks compare manifest accounting, sampled
   browse/search/validate agreement, repeated search/order bytes, CLI/HTTP order
   identity and located refusal. CI runs them against both published global catalog and
   SWECOV steward assets in independent `integration.yml` jobs. Published schema drift
   is a release-drift failure. Accepted-private-input census additionally requires
   explicit `--holdings-input=/path/to/accepted-candidate`; mismatched commit/manifest,
   dirty input, missing members and wrong paths fail admission. Full HTTP variable-node
   admission comparison and deterministic stratified CLI/HTTP binding agreement derive
   requests from the artifact. CLI delivery schemas expose a different membership grain.
   Pinned historical baseline acceptance, exhaustive representative resolution,
   performance and rendered checks remain separate maintainer work.

Budgets are what a lean suite for the package needs, derived from its boundaries, not
from the suite's current size. Local is wall time with `pytest <tree> -n auto -q` (or
`bun run test`) on a 10-core developer Mac; CI is the job's `timeout-minutes` in
`ci.yml` on `ubuntu-latest`, including setup. A suite over budget is a finding for the
`test-audit` skill, not a reason to raise the number.

  | Suite                                         | Local | CI job | Rationale                                                                                                                            |
  | --------------------------------------------- | ----- | ------ | ------------------------------------------------------------------------------------------------------------------------------------ |
  | `reg_meta_build/tests`                        | 45 s  | 6 min  | The compiler: a few hundred source-fixture → artifact cases at about 1 s CPU each, plus one session load of the committed curation.  |
  | `conformance`                                 | 20 s  | 4 min  | Session-built catalog and steward artifacts once, then data-driven CLI, HTTP, order and validate cases.                              |
  | `scripts/tests`                               | 20 s  | 2 min  | Repository tooling contracts (skill discovery, lints, the opt-in marker gate); a few nested pytest runs dominate.                    |
  | `.claude/hooks/tests`                         | 5 s   | 2 min  | Exit-code and message contracts of the Claude Code hooks (deny payloads, bootstrap idempotence); plain bash with stubbed `uv`/`bun`. |
  | `crates/` (`cargo test --workspace`)          | 10 s  | 3 min  | The Rust runtime's unit and property tests; G0 always runs them.                                                                     |
  | Rust conformance run (`scripts/gate.py rust`) | 30 s  | 3 min  | The whole conformance suite against the Rust server; Shares the `rust` job with `crates/`.                                           |
  | frontend (`bun run test`)                     | 15 s  | 6 min  | Rendered DOM and accessibility tree for the user flows, jsdom for grammars. The CI job also installs, type-checks, lints and builds. |

G0 of `RUST_RUNTIME_SPEC.md` §4 runs conformance, the touched packages and
`cargo test --workspace`. The runtime-side rows, conformance and `crates/`, sum to 30 s,
so any change touching the runtime fits G0's 60 s. From slice 3a G0 also runs the Rust
HTTP run (§10: `scripts/gate.py rust`, the whole suite against the Rust server, out of
process) within its own 30 s row above. G1 (under 5 min) and G2 run on real artifacts in
tier 3 and are not package budgets. `reg_meta_build` stays Python and is outside G0.

### Conformance suite

Every tier runs `conformance/` at the repository root. Its readable case data moved
byte-identically from the reader and backend suites; thin loaders execute CLI JSON,
documented public library return models, order manifests and HTTP/boot/validation
boundaries. Public naming alone is insufficient: cases assert domain outputs or located
errors, not object internals or query implementation. Product adapters are not added
solely for testing. Fixture-bound cases always use their named source-built synthetic
artifacts, including in a tier 3 run. Artifact-parametrized checks default to
session-built catalog/steward artifacts and use exactly `--artifact-dir` under tier 3.
They derive expectations from artifact content or cross-adapter agreement, never
real-identifier goldens.

Order comparisons across adapters and repeated runs use raw bytes, including provenance;
no volatile fields are removed, and key/list order remains observable. Existing scope
accounting covers tables and columns; no manifest count contract yet exists for
mappings, periods or unmapped reasons. Catalog artifacts permit global fallback orders;
steward reference visibility does not grant orderability. The existing
`crates/reg-core/tests/project/corpus/` serves its Rust and TS consumers.

The corpus depends on public contracts rather than private Python internals. Private
imports/internal patches and oversized test modules are gated by repo lints with no
exemptions or file allowlists; the per-package test sweep (plans 06a–06c, #1169) emptied
the last ones. A future language-independent reader port reuses these oracles.

### Discipline against ballooning

Pre-v1 agentic development produced a suite that churns faster than the code it covers
and reaches into private names in over a hundred places. The policy stops new growth of
that kind; the existing tests are swept one package at a time after the
compiled-holdings cut, deleting any test that pins an internal whose behavior a boundary
case already covers and rewriting the rest against the artifact. Coverage is measured as
boundaries with a corpus, not as line percentage; a boundary without a corpus is the
gap.

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

  | Old spec section                                                                                                        | New home                            |
  | ----------------------------------------------------------------------------------------------------------------------- | ----------------------------------- |
  | §1 background, §2/§3 product, §4 layout/deps, §9.3 REST, §11 changes, §12 invariants, §13 policy, §16 overview          | this file                           |
  | §5 object model, FQID grammar, edge semantics, library API, glossary; §6.8.3 semantic rules; §15 step 12 order manifest | `crates/DESIGN.md`                  |
  | §4.4 IR/adapter, §5.3 slug curation, §5.4 immutability, §5.6 lineage, §5.7 triage, ID minting                           | `reg_meta_build/DESIGN.md`          |
  | §5.9, §6 `project_data.json` schema + structural/return-shape rules                                                     | `crates/reg-core/DESIGN.md`         |
  | §9 webapp                                                                                                               | `reg_webapp/DESIGN.md`              |
  | §7 (bundle), §10-bundle, §16 PII/determinism (archived)                                                                 | `archive/mona-subsystem`            |
  | §6.6 codes, §8 stats+kit, §9 deployment/stewards, §10 mockdata, §14 open decisions, §15 steps 6.5–11, remaining §16     | `REFACTOR_SPEC.md` (remaining work) |

Shared literal source evidence types live in `reg_meta_build` (`source_evidence.py`);
the Rust reader reads the documentary relationships from the artifact. Documentary
owner/operand links are metadata, not state availability, equivalence, lineage or
executable transformation edges.
