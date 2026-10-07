# Rust runtime and a compiled catalog

**Status: accepted 2026-10-07; root-level refactor tracker.** The maintainer took the
decisions in section 13 on 2026-10-07. This file is a scoped, self-deleting tracker
under the governance exception in `CLAUDE.md`/`AGENTS.md`, alongside `REFACTOR_SPEC.md`.
As each stage ships, its design rationale moves into `ARCHITECTURE.md` and the package
`DESIGN.md` files and its section here shrinks. **Completion gate: deleted when stage 6
(SPA on WASM) ships.**

The question asked: should `reg_meta` be ported to Rust, both for speed and to rebuild
the CLI cleanly? The answer is yes, but not as a port of `reg_meta` alone. This is a
redesign of everything that runs after the artifact is built, with the build itself
staying in Python.

## 1. What was measured

All timings: v0.41.0 release catalog (schema 9.0.0, 1.23 GB `reg_meta.db`), warm page
cache, Apple Silicon, median of three runs of the installed `reg-meta` CLI.

  | Command                                         | Wall time  | Where the time goes                                                                   |
  | ----------------------------------------------- | ---------- | ------------------------------------------------------------------------------------- |
  | `--version`, `get register`, `get varinfo`, ... | 220–300 ms | Python start-up. `import reg_meta.cli` alone is ~190 ms, mostly Pydantic model build. |
  | `search --query kön`                            | ~650 ms    | 70% inside SQLite. 1,646 `execute` calls. 139k calls into the Python `py_lower` UDF.  |
  | `search --query 0115`                           | ~860 ms    | 93% inside SQLite. 1,221 `execute` calls.                                             |
  | `get schema --register LISA --summary`          | ~420 ms    | Mostly Python: slug validation, storage-ID JSON rewriting.                            |
  | `get coded-variables`                           | **~72 s**  | One SQL statement (COUNT DISTINCT over 5.9M value-set members). Bad plan.             |

What this says:

- **Start-up is the only cost that Rust removes by itself.** A native binary starts in a
  few milliseconds. Agents call the CLI in loops, so 250 ms per call adds up.
- **Search and the slow commands are query-design problems.** A Rust reader runs the
  same SQLite engine. Ported line by line, it would still issue 1,600 queries per search
  and still scan 140k rows through a folding function.
- **The fix for query cost is to move work into the build** (section 3). That fix is
  also what makes a Rust reader small enough to be worth writing.

## 2. What `reg_meta` actually is today

`reg_meta` is three things sharing one package:

1. **Contracts the build depends on.** `reg_meta_build` subclasses `source_evidence` and
   `documentary` Pydantic models, calls `canonical_sha256` ~60 times (it feeds
   generation identity), mints slugs with `fqid`, uses the inventory interval helpers
   (including private `_merge`, `_render`, `_intersect`), and builds its own CLI on
   `reg_meta.cli_common`. `holdings_compile` imports the reader's `Catalog` to
   canonicalize representations, so the compiler depends on the reader.
2. **A read library.** The webapp builds a `Catalog` per request and calls ~35 of its
   methods. About 30 `reg_meta` Pydantic models are FastAPI response models, so they
   shape `openapi.json` and the frontend's generated types. The webapp also runs raw SQL
   against the catalog schema.
3. **The CLI.** Its main users are agents, through the `register-metadata-search` skill.
   The Docker build and the publish workflow also call it.

Duplication found along the way:

- **Two query layers.** `queries.py` serves the CLI and `Catalog` serves the webapp.
  Terminal succession, classification editions, classification codes, lineage and
  register resolution each exist twice.
- **Two search rankings.** The webapp calls `search()` five times per request, then
  applies its own best-bets ranking and golden pins (`routes/search.py`, `golden.py`).
  CLI users and agents never see that ranking.
- **Four period grammars.** `reg_meta/fqid.py`, `reg_schema/structural.py`, and the
  frontend's hand-written `period.ts` (1,115 lines) and `validation.ts`. A parity test
  keeps two of them aligned.
- **Four text folds.** Python `str.lower` (`py_lower`), `str.casefold`,
  NFKD-strip-casefold, and SQLite's ASCII-only `NOCASE`/`LOWER`, mixed without a stated
  rule.

Smaller defects (not blocking, listed so they are not ported):

- Row-order-dependent results: the `same_as` BFS and the classification editions year
  map read rows without `ORDER BY`.
- CLI inconsistencies: register is positional in some commands and `--register` in
  others (defined 11 times); years are `--years`, `--year` or `--from/--to`; pagination
  is `--cursor` or `--offset`; `data` changes shape with result cardinality;
  `--summary`/`--flat` are silently ignored in JSON; `order` bypasses the envelope and
  the dispatch table; exit 17 means both "no match" and "order blocked"; exit 20 is
  defined and unused.
- `update` crashes as `internal_error` on non-tty stdin without `--yes`; malformed
  `resolve` stdin exits 30 instead of 2.
- Downloads are not checksum-verified. Legacy bare `v*` release tags are still accepted.
- Import cycles (`catalog` ↔ `graph`, `catalog` → `doc_queries` → `queries` → `catalog`)
  hidden by lazy imports.

## 3. Principle 1: compile, don't resolve

`reg_meta/DESIGN.md` currently says: "No state/window resolution is compiled." Decision
1 reverses that rule; the text changes when the derive slice for states ships.

The artifact is immutable and rebuilt from scratch on every release. Anything that is a
pure function of the artifact can be computed once instead of on every query. An audit
of the reader's hotspots classified them:

- **A — pure function of the artifact.** Becomes a derived table.
- **B — query-dependent, but served by a precomputed column or index.**
- **C — genuinely request-dependent.** Stays in the reader.

About 70% of the hotspots are A or B. The main ones:

  | Read-time work today                                                                                                     | Class | Compiled replacement                                                    |
  | ------------------------------------------------------------------------------------------------------------------------ | ----- | ----------------------------------------------------------------------- |
  | `py_lower(col) LIKE py_lower(?)` scans (alias 82k rows, variable 54k)                                                    | B     | `*_folded` columns + index; trigram FTS5 for substring match            |
  | `py_catalog_column` UDF, `representative_columns`, spelling caches                                                       | A     | `canonical_column` stored on states, aliases, windows, warnings         |
  | `_expand_state_windows`, alias-window participation rules                                                                | A     | `variable_state` stores the projected states (delta ~2.3k rows)         |
  | `register_variable_deliveries`, `_fuse_windows`, coverage                                                                | A     | `delivery` and `delivery_window` tables                                 |
  | `_fuse_provider_held_deliveries`, `scope_predicate` string splicing                                                      | A     | Per-scope rows (`scope = 'holdings'`) in steward artifacts              |
  | `_state_warning_ids` (one `data_warnings` call per state)                                                                | A     | `state_warning` link table                                              |
  | Classification/variable chains, terminal successors, editions, families                                                  | A     | `succession_terminal`, `classification_chain`, `classification_family`  |
  | `same_as` BFS                                                                                                            | A     | `same_as_resolution` table                                              |
  | Concept-group tag N+1, group member assembly                                                                             | A     | `concept_group_tag`, pre-ordered member rows                            |
  | `get coded-variables` (72 s)                                                                                             | A     | `coded_variable_stats` per scope; reader applies only filters and limit |
  | `variable_fts_content` view (correlated `group_concat` for 43k variables)                                                | A     | Materialized FTS content table                                          |
  | Search arm merge, scoring, cursor, fold decisions; period intersection with a request; `get diff`; order materialization | C     | Stays in the reader                                                     |

Consequences:

- **The resolver gets one home, in the build** (the derive step, section 4). The
  alias-window, replacement and coding rules are implemented once, in Python, where
  validation can check them. The reader never re-implements them. This is the largest
  reduction in port risk: the hardest semantics never need a second implementation.
- **The compiler stops importing the reader.** `holdings_compile` canonicalizes against
  the derived `delivery` table.
- **Scope becomes data.** Only two scopes exist (`reference`, `holdings`), and holdings
  exists only in steward artifacts. Precomputing per-scope rows replaces the
  string-spliced predicates in ~25 queries.
- **Reader size.** Of ~10.9k lines in `catalog.py` + `queries.py`, an estimated 35–40%
  of lines and 10–15% of the algorithmic logic remain. The remainder is mostly SQL plus
  sort/merge.
- **Artifact size.** Roughly +70–100 MB (6–8%). Do not add a second copy of the
  projected states (+200 MB) or precomputed code owners (up to 3.9M rows).
- **New invariants the derive validator must own:** delivery windows disjoint and equal
  to the fuse of projected states; one canonical spelling per (variable, variant, fold);
  chain/terminal tables acyclic and consistent with `*_replaced_by` at the manifest
  year; `state_warning` equal to the attribution predicate; held-\* tables equal to
  holdings facts; aggregate tables equal to a recomputation; fixed insertion order for
  byte-identical output.
- **Lost test seam.** The `classification_as_of_year` override goes away. Tests build
  artifacts with a different manifest year instead.

## 4. Working structure: decouple the refactor from the slow build

A full base build takes about an hour (section 11). If every step of this refactor
needed one, the refactor would crawl. It does not need one: almost all of its work sits
downstream of curation.

**Split the build at a stable boundary.**

- **Base** — today's curation-driven build (`build-db`, `extend-db`). It produces the
  core tables. Only curation and source changes touch it. It stays slow until the build
  track (section 11) speeds it up.
- **Derive** — a new step, `reg-meta-build derive`, that reads a built database and
  writes the derived tables of section 3. It is a pure function of the base artifact,
  deterministic, and has its own validator. Steward artifacts run it after `extend-db`;
  `holdings_compile` uses the reference-scope derived tables from the global base.

**Bootstrap derive by moving, not rewriting.** The first implementation of each derived
table calls today's reader functions (`Catalog.states`, the delivery fusing, the chain
walks), moved into `reg_meta_build`. Its output then equals what the current reader
returns, by construction, and the risky resolver logic moves without semantic change.
Measured cost: `Catalog.states` over a 300-variable sample takes 6 ms per variable, so
all 54k variables take ~5 min on one core, about 1 min in parallel. Replace a moved
function with set-based SQL only when the tier-1 budget below requires it.

**Ship derived tables additively.** The schema gate accepts a database whose minor
version is at least the code's. Derived tables land as schema 9.x minor bumps, so the
current Python reader and webapp keep working on every intermediate release. There is no
big-bang artifact cutover; the major bump comes only when the Python reader is deleted.

**Develop against pinned artifacts.** Refactor work uses a pinned release base (0.41.0
today, moved forward only at agreed checkpoints) plus the synthetic conformance
fixtures. Curation work keeps changing the base; the refactor changes derive and the
reader. Neither waits for the other.

**Three verification tiers, with budgets.**

  | Tier | What runs                                                                                                                                                                                                  | Budget      | When                                         |
  | ---- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- | -------------------------------------------- |
  | 0    | Unit tests and conformance on synthetic artifacts, Python and Rust. Today 215 conformance cases take 12 s, synthetic builds included.                                                                      | under 30 s  | every change                                 |
  | 1    | Derive on the pinned real base, then a differential run: current Python reader vs derived tables vs Rust reader, over generated queries (every register, sampled variables, the search-eval corpus terms). | under 5 min | every PR touching derive or the reader       |
  | 2    | Full base build plus derive and tier 1 on the result.                                                                                                                                                      | ~1 h today  | batch checkpoints and releases, never per PR |

A slow tier is a defect to fix, not a reason to skip the tier. If derive exceeds its
budget, make the slow table set-based before adding more tables.

**Work in vertical slices.** Each slice adds derived tables, the Rust reader functions
that read them, and their conformance and differential cases, and is verified at tiers 0
and 1 only. Slices share the pinned base, so independent slices can be built in parallel
by separate agents. Slice order is set by risk: admission + search first, order last.

## 5. Principle 2: one implementation per contract

Each contract that crosses a language or package boundary has exactly one
implementation:

  | Contract                              | Single home                           | Reached from                                         |
  | ------------------------------------- | ------------------------------------- | ---------------------------------------------------- |
  | Artifact schema (DDL, manifest)       | `reg_meta_build` (Python)             | Rust reader reads it; `SCHEMA_VERSION` gate          |
  | Resolver semantics                    | `reg_meta_build` derive (Python)      | Compiled into the artifact                           |
  | FQID and period grammar               | Rust core crate                       | Build via Python bindings; SPA via WASM              |
  | Text folds                            | Rust core crate                       | Build via Python bindings (fills `*_folded` columns) |
  | Canonical JSON + SHA-256              | Rust core crate                       | Build via Python bindings                            |
  | Project schema + structural validator | Rust core crate (replaces reg_schema) | Server, CLI; SPA via WASM; JSON Schema export        |
  | Result types                          | Rust structs (serde + schemars)       | CLI JSON, HTTP OpenAPI, frontend TS types            |

Notes:

- **Text folds get a written spec.** Two named folds:
  - `fold_identity` — per-character Unicode lowercase, no context rules (no final
    sigma), no normalization. This is today's `py_lower` and column identity rule. In
    Rust this is `char::to_lowercase` applied per character, not `str::to_lowercase`,
    which applies final sigma.
  - `fold_search` — NFKD, drop combining marks, full case folding, collapse whitespace.
    This is today's search-text fold and is aligned with FTS5 `unicode61`.

  The build computes folded columns with the same code the reader uses for the query
  string, through the bindings. Pin the Unicode version. Keep a property-test corpus of
  Swedish and SCB strings.

- **Canonical JSON** has no floats today. Keep it that way. A float in a hashed payload
  is a build error.

- **Messages are structured.** Error and blocked-order findings carry a code and fields.
  Prose is rendered from templates. Goldens assert codes and fields, so no Python `repr`
  quoting (`'other'`, `None`) appears in any contract.

## 6. Target architecture

```
                 reg_meta_build (Python, maintainer-only)
                   base: adapters → IR → curation → core tables      (slow)
                   derive: core tables → derived tables              (fast)
                   uses reg_core (Python bindings) for FQID, folds, canonical hash
                                   │
                                   ▼
                    reg_meta.db (+ docs DB), immutable
                                   │
            ┌──────────────────────┼───────────────────────┐
            ▼                      ▼                       ▼
     reg-meta CLI           reg-meta serve            reg-meta mcp
     (agents, humans)       (HTTP API for the SPA)    (agent tools over stdio)
            └──────────── one Rust binary, one query library ───┘
                                   │
                         SPA (Svelte) + reg_core WASM
                         for period grammar and project validation
```

Rust workspace (`crates/`):

- `reg-core` — no IO. FQID and period grammar, folds, interval algebra, canonical
  JSON/hash, project-data types and structural validator, error codes. Compiles to
  native, Python extension (PyO3, via maturin) and WASM.
- `reg-catalog` — the reader. Opens and admits an artifact (read-only, `immutable=1`,
  schema gate, identity checks), owns scope as reader state, and exposes one function
  per query returning typed results. Includes search (one ranking, including the
  webapp's best-bets and golden pins, which become curated build input), order
  materialization and project semantic validation.
- `reg-meta` — the binary. Subcommands for the CLI, `serve` (axum + utoipa; ETag,
  body-size limit and rate limit as tower layers) and `mcp`. `update` downloads,
  verifies the SHA-256 and atomically activates an artifact.
- `reg-core-py` — the PyO3 module the build imports. Optionally later, a read API for
  notebooks.

What is deleted: the Python `reg_meta` package, `reg_schema`, the FastAPI backend, and
the frontend's hand-written grammar and validation mirrors. Modules that only the build
uses (`source_evidence`, `documentary`, `cli_common`, inventory TOML loading) move into
`reg_meta_build` first.

Why the HTTP server moves to Rust rather than FastAPI calling Rust through bindings:

- With bindings, every result type exists twice: as a Rust struct and as a Pydantic
  model for OpenAPI. That is the drift this design removes.
- The backend is ~6k lines. About 3k of them are catalog logic (node assembly, search
  ranking, semantic validation) that belongs in the reader anyway, so the CLI gets it
  too. The rest is HTTP glue that axum and tower handle directly.
- The frontend depends on FastAPI only lightly: it reads `detail` as a string and
  `detail[0].msg`. The server can emit that shape.
- The deploy image becomes one static binary plus the baked DBs. Keep the 1 GB Fly VM
  for the page cache.

## 7. CLI v4

**Draft, under iteration (decision 4).** Nothing below is settled until the maintainer
signs off on the surface.

Design rules:

- **Address by FQID first.** Any entity is a ref: `provider`, `provider/register`,
  `provider/register/slug`, `class/slug`, group keys, or a bare name. An ambiguous bare
  name returns the candidates and exit 17.
- **One flag per concept, the same everywhere.** `--register`, `--period` (the FQID
  period grammar, replacing `--years`/`--year`/`--from`/`--to`), `--limit` and
  `--cursor` on every list. `--scope` is valid on every read command because scope is
  reader state.
- **Output follows the destination.** JSON when stdout is not a terminal, text when it
  is. `--format json|text|ndjson` overrides.
- **One output shape per command.** The shape does not depend on result count. Every
  list is `{"items": [...], "next_cursor": ...}`. Every JSON document carries a small
  `meta` object (contract version, artifact identity, scope, timing).
- **Errors** are always JSON on stderr: `{code, class, message, remediation, fields}`.
- **Exit codes:** keep 0/2/10/16/25/30, split 17 into 17 (no match / ambiguous) and 18
  (order blocked), and drop 20.
- **Self-description.** `reg-meta describe` prints the command tree and the JSON Schema
  of every output. Help text is generated from the same definitions. This replaces ~580
  lines of hand-written help and examples.

Command sketch:

```
reg-meta search <query> [--type T]... [--register R] [--period P]
reg-meta show <ref>                 # register, variable, classification, group
reg-meta states <ref> [--period P]
reg-meta values <ref> [--period P]
reg-meta lineage <ref>
reg-meta graph <ref>
reg-meta coverage <ref>             # was: get availability
reg-meta schema <register> [--variant V] [--period P]
reg-meta diff <register> --from P --to P [--variant V]
reg-meta resolve <column>... | -
reg-meta coded [--min-codes N] [--min-registers N]
reg-meta order <project.json> [-o FILE]
reg-meta docs search <query> | docs show <id> | docs list
reg-meta catalog info | catalog update [--catalog NAME] [--tag T] [--yes]
reg-meta serve [--port N]
reg-meta mcp
reg-meta describe
```

`get datacolumns`, `get varinfo` and `get lineage` collapse into `show`, `states` and
`lineage`. `get groups --classifications` collapses into `show class/...`.

`mcp` exposes the same query functions as MCP tools with the same JSON Schemas. Agents
then call typed tools instead of parsing shell output. The skill keeps the CLI as a
fallback.

## 8. Distribution

- **Binaries** for macOS, Linux and Windows on each `reg_meta/v*` release, built with
  cargo-dist or a plain matrix workflow, with SHA-256 checksums.
- **PyPI** keeps `uv tool install reg-meta` working: maturin with `bindings = "bin"`
  publishes the binary as a wheel. The skill's install instructions stay the same.
- **Self-upgrade** defers to the installer (`uv tool upgrade`, or the release binary).
  `catalog update` manages artifacts only.
- **Release DB assets** gain a checksum file. `update` verifies before activation.
- **reg_meta_build** depends on `reg-core-py` as a workspace member built by maturin.
  `uv sync` builds it, which needs a Rust toolchain on the maintainer machine and in CI.

## 9. Verification

The conformance corpus is the acceptance gate. Today every runner calls Python
in-process, so it must become implementation-neutral first:

- Add a runner seam: one `run_cli(argv)` helper that runs `$REG_META_BIN` as a
  subprocess (~30 lines plus ~10 call sites). HTTP cases take a base URL instead of a
  `TestClient`.
- Rewrite the 42 `logical` cases and the 5 `coverage` cases, which name Python
  functions, as CLI v4 argv cases. Write them against v4 directly, not against today's
  surface.
- Order cases compare against committed `order.json` bytes, not against Python's own
  output.
- `update` and selection cases need a URL override (`REG_META_RELEASES_URL`) and a local
  HTTP fixture server instead of patching `urlopen`.
- Fixtures: a script builds each synthetic (fixture, kind) artifact once through the
  real Python pipeline, cached by a hash of the fixture sources and build code. Rust
  tests and conformance read the same built files. Determinism makes caching safe.
- The tier-1 differential harness (section 4) maps v4 output back to the current
  reader's results where the shapes differ; the mapping is part of the harness, not the
  product.
- Expected outputs change where v4 changes the surface. Those diffs are reviewed as
  content decisions, per the testing policy.

## 10. Staging

There are no users, so each surface switches over in one step, with no compatibility
layers. The derived tables ship additively (section 4), so the deployed webapp keeps
running on every intermediate release until stage 5 replaces it.

0. **Decide and prove (days).** Iterate the CLI v4 surface with the maintainer (decision
   4). In parallel, a throwaway Rust spike: open with schema gate plus `search` against
   the pinned base. Measure start-up and search latency, check fold parity with a
   property corpus, and confirm maturin + PyO3 + uv workspace ergonomics. Adjust the
   plan if anything is worse than expected.
1. **Groundwork.** Write the CLI v4 spec, the fold spec and the error code catalog. Move
   build-only modules into `reg_meta_build`. Add the conformance runner seam, the cached
   fixture builder and the tier-1 differential harness.
2. **Derive step.** `reg-meta-build derive` with its validator, bootstrapped from moved
   reader functions, shipped as schema 9.x. `holdings_compile` stops importing
   `Catalog`.
3. **Rust reader slices.** `reg-core`, `reg-catalog` and the `reg-meta` binary, one
   vertical slice at a time, each paired with its derived tables: (a) admission +
   search, (b) show / states / values, (c) schema / diff / coverage / coded, (d) chains
   and graph, (e) order and project validation. The build switches to `reg-core-py` for
   FQID, folds and hashing. Gated by tiers 0 and 1.
4. **Cutover.** When all slices pass, the Rust binary becomes `reg-meta`; delete the
   Python `reg_meta` CLI and reader; bump the schema major. Then add `reg-meta mcp` over
   the same query functions.
5. **Rust server.** `reg-meta serve` replaces FastAPI. Regenerate `openapi.json` and
   frontend types. Move `reg_schema` into `reg-core`. Delete the Python backend and
   `reg_schema`.
6. **SPA on WASM.** Replace `period.ts`, `validation.ts` and the hand-written
   `project_data.ts` with `reg-core` compiled to WASM plus generated types.

Stage 2 can be done without Rust and pays for itself. If the spike fails, stop after
stage 2 and keep the Python reader, now much thinner.

The build track (section 11) runs alongside, independent of these stages.

## 11. The build: faster, incremental, still Python

The build stays Python. That was checked against measurements, not assumed.

**Where the build's time goes.** The 0.41.0 full strict build took 66 min on one core
(the host has 10 cores and 24 GB):

  | Phase                                  | Time   | Note                                             |
  | -------------------------------------- | ------ | ------------------------------------------------ |
  | Per-register resolve, 290 registers    | 41 min | Six SCB registers take 23 min; the largest 320 s |
  | Compile selected curation              | 19 min |                                                  |
  | Database materialization, dependencies | 3 min  |                                                  |
  | Load inputs, reference metadata        | 1 min  |                                                  |

A cProfile of a one-register subset build (`--registers 102`; 23 min wall, 8.6 GB peak
RSS) showed:

- **Algorithmic waste dominates.** `_partition_originals` asks for one native family,
  but `iter_native_families` scans and decodes the whole register and discards the other
  families in Python. 1,912 calls cost 423 s. Elsewhere: 8.4M SQLite `execute` calls
  (159 s), `record_ref` called 52M times for 7,943 records, and `coding_source_sha256`
  114 s over 6,092 calls.
- **Python object overhead is the second layer.** 3.2 billion function calls in total.
  Pydantic `validate_python` runs 68M times (186 s cumulative), `model_construct` 8.4M
  times, JSON encoding 72 s, and small key helpers 50–160M times each.
- **Out-of-slice work.** A one-register build spent 15.5 min on deferred naming for
  registers outside the slice.

**Why not Polars.** None of the hot spots is a column-shaped computation. They are
per-record rule logic with source locators, located errors, preserved order and
multiplicity, and byte-identical output. Polars would add a third data model next to the
Pydantic IR and SQLite, convert at every boundary, and needs `maintain_order` discipline
everywhere to stay deterministic. The parts that are column-shaped (value binding,
coverage, the derived tables) are better written as SQL in the SQLite stores the build
already uses.

**Why not Rust yet.** Rust would remove the second layer, likely by a large factor, and
cut memory. It would also make the stack one language and remove the bindings layer. But
the build is 71k lines and the most frequently changed code in the repo: rule and
curation tickets land weekly and the prepared schema has passed version 15. A rewrite
must reproduce byte-identical artifacts and the same diagnostic baseline while both are
moving. The first layer is cheap to fix in Python.

**Order of build work:**

1. Fix the algorithmic waste: the family scan, N+1 queries, repeated `record_ref` and
   hashing. Byte-identical, verified with dbdiff.
2. Run per-register resolve in parallel processes (decision 7). `resolve_source_scope`
   is close to a pure map over one register's records plus read-only shared context. Its
   results are merged in a fixed order, so output stays deterministic. The shared
   accumulators (`matched_labels`, `duplicate_overrides`, the coding callback) become
   per-scope outputs merged afterwards. The floor is the largest register (~5 min).
3. Cache per-register resolve results by content: the register's prepared-record digest,
   the curation entries that reach it, and a hash of the build code. A curation-only fix
   then re-resolves only the registers it affects. Determinism makes the cache safe; a
   periodic uncached build, byte-compared with the cached one, catches a key that misses
   an input.
4. Keep Pydantic validation at trust boundaries only (prepared-input reads, curation
   TOML). Internal hot-path objects already validated upstream do not revalidate.
5. Re-profile. If Python object overhead still dominates, move specific kernels into
   `reg-core` through the same bindings the build already uses for grammars.
6. Reconsider a Rust build only after the runtime port, once the curation rules
   stabilize. Then do it stage by stage behind the same artifact-equality gate, never as
   one rewrite.

## 12. Risks

- **Maintenance surface.** Two languages instead of one (Python for the build, Rust for
  the runtime). Rust build times and toolchain pinning in CI.
- **Fold parity.** The main source of silent drift. Mitigated by computing folded
  columns through the same code via bindings, and a property corpus.
- **Native extension in the build.** The build needs a compiled `reg-core-py`. That is
  only for the maintainer and CI, but it is a new prerequisite.
- **Derive speed.** Derive is fast only while it stays set-based or parallel. The tier-1
  budget is the early warning.
- **Cache keys in the incremental build.** A key that misses an input yields a stale,
  wrong artifact. Mitigated by the periodic uncached byte-compare.
- **Frontend churn in stages 5–6.** Type regeneration will move many generated names.
- **Lost notebook import.** `import reg_meta` goes away unless the optional read
  bindings are built. Nothing in the repo depends on it today.

## 13. Decisions (2026-10-07)

  | #   | Question                               | Decision                                                                                                                       |
  | --- | -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
  | 1   | Where are catalog facts resolved? (§3) | **Compiled in the build**, in the derive step (§4). Reverses "No state/window resolution is compiled" in `reg_meta/DESIGN.md`. |
  | 2   | What serves the webapp API? (§6)       | **Rust server** (`reg-meta serve`). The FastAPI backend is deleted in stage 5.                                                 |
  | 3   | `reg_schema`? (§5)                     | **Merged into `reg-core`.** The Python package is deleted in stage 5.                                                          |
  | 4   | CLI v4 surface (§7)                    | **Iterate first.** §7 is a draft; the surface is settled with the maintainer in stage 0.                                       |
  | 5   | MCP server mode                        | **Yes, after CLI parity** (stage 4).                                                                                           |
  | 6   | WASM in the SPA                        | **Yes, as the last stage** (stage 6).                                                                                          |
  | 7   | Parallel per-register resolve (§11)    | **Yes**, after the family-scan fix lands.                                                                                      |
  | 8   | Where this plan lives                  | **Its own root tracker**, with the governance rule amended to allow one tracker per concurrent refactor.                       |
  | 9   | How to avoid full rebuilds per step    | **Base/derive split, pinned artifacts, three tiers with budgets, incremental base build** (§4, §11).                           |
