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
  the runtime modules + `reg_schema` into a single `.py` for upload
  to MONA. Pure-stdlib, importable by `reg_webapp` at container build
  time.
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

- **§6.8.1 structural rules.** Owned by `reg_schema`. The bundle
  amalgamates `reg_schema.validate_structural` and calls it at load
  time — but the rules themselves live in `reg_schema`.
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
(mdw's CLI invokes `reg_monabundle.build`). `reg_schema` is pure
stdlib so amalgamation is safe.

## The two halves

The lightweight surface (`reg_monabundle.build`, `.scan`, `.types`,
`.validate_block`) is pure-stdlib and importable from any context
including `reg_webapp` at container build time. The runtime surface
(`reg_monabundle.runtime.*`) is what gets amalgamated into the bundle
and executes on MONA; it pulls duckdb / pyodbc.

```toml
# Phase 2 target shape — placeholder today.
[project]
dependencies = ["reg-schema"]

[project.optional-dependencies]
runtime = ["duckdb", "pyodbc"]
```

`reg_webapp` will pin `reg-monabundle` with no extras (the container
never installs duckdb/pyodbc). Import paths used by the webapp must be
lazy enough that `import reg_monabundle.build` does not transitively
pull `reg_monabundle.runtime.*`.

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
- **Phase 2b.** `scan.py` → `reg_monabundle/scan.py`;
  `reg_monabundle.types.is_compatible` lands if §15 step 10a needs
  it sooner.
- **Phase 2c.** Runtime modules (classify, sql_emit, sources,
  summarize, spec, extract) → `runtime/`. The bundle's import
  discipline (lightweight vs runtime) is enforced by a CI test that
  imports the lightweight surface in a duckdb-less env. **Open
  design question:** where does `LoadedSpec` live once `spec.py`
  moves under `runtime/`? mdw's CLI needs it; `reg_webapp` will want
  a no-runtime-deps version.
- **Phase 3.** 1 MB bundle-size budget gate (§12). Real-MONA-shape
  fixture, byte-counted in CI; fails on budget overrun.

## SUPPRESS_K

Lives in `reg_monabundle.constants` because it is the bundle's
privacy floor — the validator's "overrides may only raise the
threshold" rule is part of the namespaced-block contract, and the
constant is also amalgamated into the bundle alongside the runtime
modules that suppress cells against it. `mock_data_wizard.summarize`
re-exports it for callers that still import the legacy path; phase 2
will retire that re-export.
