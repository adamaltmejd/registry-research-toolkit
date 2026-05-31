# Design: reg_monabundle

Design rationale and constraints for the MONA bundle builder, bundle
runtime, PII scanner, and type compatibility map. The authoritative
spec lives in `REFACTOR_SPEC.md` §4 ("Target package layout"), §7
("MONA workflow"), and §15 step 5 ("Carve `reg_monabundle` out of
`mock_data_wizard`") at the repo root until §15 steps 9-10 dissolve
the refactor spec into per-package DESIGN files.

## Scope

`reg_monabundle` owns the MONA-bundle half of the toolkit:

- **Bundle builder** (`reg_monabundle.build`, phase 2). Amalgamates
  the runtime modules + the lightweight `reg_monabundle` slices
  (`constants`, `scan`) into a single `.py` for upload to MONA.
  Pure-stdlib, importable by `reg_webapp` at container build time.
  **Carries no `reg_schema` and no Pydantic** (§9.6) — structural
  validation is the build-time gate, not an on-MONA step (see below).
- **Build-time spec gate** (`reg_monabundle.build.spec_loader`,
  A3.4). The Pydantic side of the §9.6 boundary, run at bundle-build
  only (never amalgamated): `validate_project_data` runs the full
  `reg_schema` structural validator + the `reg_monabundle` block
  validator + the cross-block referential checks, then
  `project_data_to_loadedspec` converts the validated Pydantic
  `ProjectData` into the stdlib `LoadedSpec` the runtime consumes.
- **Bundle runtime** (`reg_monabundle.runtime.*`, phase 2). The
  modules amalgamated into the bundle and executed on MONA:
  `classify`, `sql_emit`, `sources`, `summarize`, `spec`, `extract`.
  Pulls duckdb/pyodbc, which WinPython preinstalls on MONA.
- **PII scanner** (`reg_monabundle.scan`, phase 2). Pre-flight check
  on the spec to catch PII patterns before any data is touched.
- **Type compatibility map** (`reg_monabundle.types.is_compatible`,
  phase 2 / step 10a). Pure function — used by merged-mode realign
  to detect type drift between author-time decisions and
  bundle-execution-time data.
- **§6.8.2 namespaced-block validator** (`reg_monabundle.validate_block`,
  phase 1 — shipped). Validates the `reg_monabundle` block of
  `project_data.json`: allowed keys, binding-FQID well-formedness on
  `column_options`, the `suppress_k` floor against `SUPPRESS_K`.

## Not in scope (intentionally)

- **§6.8.1 structural rules.** Owned by `reg_schema`. Run once at
  **bundle-build time** as the validation gate
  (`reg_monabundle.build.spec_loader.validate_project_data`), never on
  MONA: the bundle is `reg_schema`/Pydantic-free (§9.6) and its
  runtime trusts the already-validated embedded/sidecar JSON,
  deserializing it into a stdlib `LoadedSpec` via
  `runtime.spec.loadedspec_from_dict`. A hand-edit on MONA that breaks
  deserialization errors with a stdlib exception, by design — the
  bundle is a build artifact, not an authoring surface.
- **§6.8.3 semantic rules (reg_meta-backed).** Owned by `reg_webapp`
  (FQID resolution, classification existence, steward-catalog
  membership). `reg_monabundle` is `reg_meta`-free by design — the
  bundle runs on MONA where reg_meta cannot reach.
- **`reg-mockdata generate` / `reg-mockdata compare`.** Local
  researcher-side tooling, owned by `reg_mockdata` (post-rename of
  `mock_data_wizard`). The MONA bundle and the local mock generator
  are different workflows with different audiences.

## Dependency direction

```text
reg_monabundle → reg_schema
```

No `reg_meta` dep — the bundle has no reg_meta on MONA. No
`mock_data_wizard` dep — the dep direction is the other way around
(mdw's CLI invokes `reg_monabundle.build`). `reg_schema` (Pydantic)
is a build-time-only dep of `reg_monabundle.build.spec_loader`: it
runs the validation gate but is **never amalgamated** into the bundle
(§9.6). The amalgamated slices are stdlib-only.

## The two halves

The lightweight surface (`reg_monabundle.build`, `.scan`, `.types`,
`.validate_block`) is pure-stdlib and importable from any context
including `reg_webapp` at container build time. The runtime surface
(`reg_monabundle.runtime.*`) is what gets amalgamated into the bundle
and executes on MONA; it pulls duckdb / pyodbc on use (lazy imports
inside function bodies — module-level imports stay stdlib so the
amalgamation step doesn't require duckdb to slice).

The amalgamator (`build/__init__.py`) reads runtime modules off disk
via `ast.parse` rather than through Python's import machinery: an
amalgamation-only environment never needs duckdb installed. The CI
gate in `tests/test_lightweight_surface.py` enforces this — importing
`reg_monabundle` or any of its public submodules from a fresh
subprocess must leave `sys.modules` runtime-free. `build.spec_loader`
is deliberately **not** imported by `build/__init__.py` (its lazy
`runtime.spec` import would otherwise pull the runtime tier); callers
import it directly. The no-Pydantic/no-`reg_schema` invariant on the
emitted bundle is gated separately by
`tests/test_build_mona_bundle.py::test_bundle_carries_no_pydantic_or_reg_schema`
(source line-scan + AST import check).

```toml
[project]
dependencies = ["reg-schema"]

[project.optional-dependencies]
# Reserved — populated when callers running the runtime locally need
# the heavy deps installed. The MONA WinPython env already has them.
runtime = []
```

`reg_webapp` pins `reg-monabundle` with no extras (the container never
installs duckdb / pyodbc). It uses `reg_schema.ProjectData` directly
for read-only views and only invokes `build_bundle` for bundle
emission, never importing the runtime tier.

## Phase plan (§15 step 5)

- **Phase 1.** ✅ Package created, `validate_block` relocated from
  `mock_data_wizard.spec`, `SUPPRESS_K` relocated from
  `mock_data_wizard.summarize` so both consumers (mdw + validator)
  share one constant. mdw depends on reg_monabundle.
- **Phase 2a.** ✅ `mock_data_wizard/_bundle.py` →
  `reg_monabundle/build.py`. `build_bundle` is now generic over the
  runtime layout: callers pass `runtime_pkg_dir` and
  `runtime_module_order`. mdw exports `BUNDLE_PKG_DIR` +
  `BUNDLE_MODULE_ORDER` and its CLI's `build-bundle` rebases through
  `reg_monabundle.build_bundle`. Bundle output is unchanged — mdw's
  runtime modules still live in mdw (phase 2c moves them).
- **Phase 2b.** ✅ `mock_data_wizard/scan.py` →
  `reg_monabundle/scan.py`. Pure-stdlib lift — scan never imported
  another mdw module, so the move is a straight relocation with the
  three callsites (`mdw.extract`, `mdw.cli._cmd_scan`, the test
  module) rebased onto `reg_monabundle.scan`. `scan` is appended to
  `REG_MONABUNDLE_MODULE_ORDER` and dropped from
  `mock_data_wizard.BUNDLE_MODULE_ORDER`, keeping the amalgamation
  order intact (the runtime `extract` slice consumes
  `write_export` from the amalgamated `scan` slice that precedes it).
  `reg_monabundle.types.is_compatible` does not land here — it
  arrives when §15 step 10a needs it.
- **Phase 2c.** ✅ Runtime modules (classify, sql_emit, sources,
  summarize, spec, extract) → `reg_monabundle/runtime/`. The MONA
  project-prefix helpers (`strip_project_prefix`,
  `lookup_with_prefix_fallback`) moved to
  `reg_monabundle.runtime._util` as part of the lift — mdw's CLI and
  enrich reach in via the public path. mdw deleted
  `BUNDLE_PKG_DIR`/`BUNDLE_MODULE_ORDER`; `build_bundle` defaults
  `runtime_pkg_dir` / `runtime_module_order` to the in-package
  runtime. `LoadedSpec` stays in `reg_monabundle.runtime.spec`:
  webapp consumers use `reg_schema.ProjectData` directly (no runtime
  adapter needed for read-only views), so the no-runtime-deps loader
  is deferred until webapp needs it. The lightweight/runtime split
  is enforced by `reg_monabundle/tests/test_lightweight_surface.py`,
  which spawns a fresh subprocess and asserts `import reg_monabundle`
  / `.build` / `.scan` / `.validate` never load a runtime submodule.
- **Phase 3.** ✅ 1 MB bundle-size budget gate
  (`REFACTOR_SPEC.md` §12). The 200-column load-test fixture lives
  under `reg_schema/test_corpus/load_test_200col/` (regenerable via
  `build.py`; the structural corpus harness picks it up as a regular
  validation case). `reg_monabundle/tests/test_bundle_size_budget.py`
  embeds it into `build_bundle`, byte-counts the emitted `.py`, and
  asserts `≤ 1_048_576`. Current shape lands at ~176 KB (~17% of
  cap) — the v1 ceiling is forward-looking, not a tight bound. The
  passing test prints actual size + headroom on every run so
  creeping growth is visible before it trips the gate.

## SUPPRESS_K

Lives in `reg_monabundle.constants` because it is the bundle's
privacy floor — the validator's "overrides may only raise the
threshold" rule is part of the namespaced-block contract, and the
constant is also amalgamated into the bundle alongside the runtime
modules that suppress cells against it.
`reg_monabundle.runtime.summarize` imports it directly; mdw consumes
it from the top-level surface (`from reg_monabundle import
SUPPRESS_K`).
