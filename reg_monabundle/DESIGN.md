# Design: reg_monabundle

Design rationale and constraints for the MONA bundle builder, bundle
runtime, PII scanner, and namespaced-block validator. Cross-cutting
topology (package tree, dependency graph, perf/bundle budgets) lives in
the root `ARCHITECTURE.md`; remaining/unbuilt work (the
realign-then-extract redesign, the `reg_monabundle.types` compatibility
map, merged-mode, composite keys, build-time `display_name`
pre-resolution) lives in `REFACTOR_SPEC.md`.

## Scope

`reg_monabundle` owns the MONA-bundle half of the toolkit:

- **Bundle builder** (`reg_monabundle.build`). Amalgamates
  the runtime modules + the lightweight `reg_monabundle` slices
  (`constants`, `validate`, `scan`) into a single `.py` for upload to
  MONA. Pure-stdlib, importable by `reg_webapp` at container build time.
  **Carries no `reg_schema` and no Pydantic** (§9.6) — structural
  validation is the build-time gate, not an on-MONA step (see below).
- **Build-time spec gate** (`reg_monabundle.build.spec_loader`).
  The Pydantic side of the §9.6 boundary, run at bundle-build
  only (never amalgamated): `validate_project_data` runs the full
  `reg_schema` structural validator + the `reg_monabundle` block
  validator + the cross-block referential checks, then
  `project_data_to_loadedspec` converts the validated Pydantic
  `ProjectData` into the stdlib `LoadedSpec` the runtime consumes.
- **Bundle runtime** (`reg_monabundle.runtime.*`). The
  modules amalgamated into the bundle and executed on MONA:
  `classify`, `sql_emit`, `sources`, `summarize`, `spec`, `extract`.
  Pulls duckdb/pyodbc, which WinPython preinstalls on MONA.
- **PII scanner** (`reg_monabundle.scan`). Final
  pre-export gate over every JSON payload the bundle writes (see
  "PII scanner" below).
- **§6.8.2 namespaced-block validator** (`reg_monabundle.validate_block`).
  Validates the `reg_monabundle` block of
  `project_data.json`: allowed keys, binding-FQID well-formedness on
  `binding_options`, the `suppress_k` floor against `SUPPRESS_K`. Pure-stdlib,
  so unlike the §6.8.1 structural gate it **is amalgamated** and runs at
  bundle **load on MONA** too (`loadedspec_from_dict` calls it), not only at
  build — per §6.8.2 ("same code, runs at build and on MONA"). The
  cross-block referential checks that need reg_schema-typed bindings
  (`_validate_binding_options_against_columns`) are the build-only exception,
  living in `build.spec_loader`.
  - **Build-side ISSUE forms** (`build.spec_loader`, never amalgamated, may
    import reg_schema): `block_issue` (translates `validate_block`'s raise to
    one `ValidationIssue`, code `invalid_block`) and `binding_options_issues`
    (codes `binding_options_orphan_fqid`, `suppress_k_on_non_categorical`) let
    `reg_webapp`'s `/api/project/validate` concatenate these as `§6.8.0`
    issues instead of catching the raise. The raising path
    (`validate_project_data`) is preserved verbatim (shared message helpers),
    so the bundle build + MONA-load `validate_block` are unchanged. `path` is
    an RFC 6901 pointer (the FQID map key is escaped, `/`→`~1`).

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

The lightweight surface (`reg_monabundle.build`, `.scan`,
`.validate`) is pure-stdlib and importable from any context
including `reg_webapp` at container build time. The runtime surface
(`reg_monabundle.runtime.*`) is what gets amalgamated into the bundle
and executes on MONA; it pulls duckdb / pyodbc on use (lazy imports
inside function bodies — module-level imports stay stdlib so the
amalgamation step doesn't require duckdb to slice).

Remaining: a `reg_monabundle.types` SQL→spec-type compatibility map
(`is_compatible` / `suggest_spec_type`) for the realign-then-extract
redesign — see REFACTOR_SPEC.md. It is not shipped; the runtime is
still the legacy discover/extract two-MODE model.

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

## `build_bundle` runtime layout

`build_bundle` is generic over the runtime layout: it defaults
`runtime_pkg_dir` / `runtime_module_order` to the in-package
`reg_monabundle.runtime`, but a caller can pass **both** to wire in a
steward-private extract pipeline. Overriding only one is refused —
applying the default module list to a different directory would crash
later with a missing-file error. The module order is dep-locked
(`_util` leads because `classify` imports its prefix helpers; `extract`
is last because it depends on everything). `LoadedSpec` stays in
`runtime.spec` rather than the lightweight surface: webapp consumers use
`reg_schema.ProjectData` directly for read-only views, so the
no-runtime-deps loader only needs to exist on the MONA-execution path.

## Bundle-size budget

The amalgamated bundle has a 1 MB v1 ceiling, gated by
`tests/test_bundle_size_budget.py`: it embeds a 200-column load-test
fixture (`reg_schema/test_corpus/load_test_200col/`, regenerable, picked
up by the structural corpus harness as a regular validation case) into
`build_bundle`, byte-counts the emitted `.py`, and asserts
`≤ 1_048_576`. Current shape lands at ~104 KB (~10% of cap) — the
ceiling is forward-looking, not a tight bound. The passing test prints
actual size + headroom on every run so creeping growth is visible before
it trips the gate. This budget is the reason `runtime.spec` carries a
deliberately leaner dataclass tree than `reg_schema` (no
register_variant / period / value_set), and the reason the bundle ships
no Pydantic — pydantic-core alone is a multi-MB binary.

## Disclosure control

The bundle's privacy guarantee is layered. **By construction**, the
per-type branches in `runtime.summarize` only ever emit aggregates —
no individual-level row crosses the JSON boundary. On top of that,
four library-default constants govern the disclosure-control
post-processing applied to those aggregates:

- `SUPPRESS_K = 10` (`reg_monabundle.constants`) — k-anonymity floor.
  Cell counts below it are suppressed (categorical frequency cutoff,
  per-period `n_entity_ids` drop, null-count censoring when
  `0 < null_count < k`).
- `SMALL_POP_MULT = 20` (`runtime.summarize`) — warn when
  `n_rows < SMALL_POP_MULT * SUPPRESS_K`.
- `NOISE_PCT = 0.005` (`runtime.summarize`) — ±0.5% uniform relative
  noise injected on numeric aggregates.
- `DATE_JITTER_DAYS = 7` (`runtime.summarize`) — ±7-day uniform jitter
  on date min/max/quantiles.

These are **fixed library defaults, not steward-configurable knobs**.
The spec plus `reg_monabundle`'s release version fully determine bundle
behavior — no out-of-band steward configuration influences a run, which
keeps the spec freestanding and reproducible. The only per-spec lever is
`reg_monabundle.column_options[<fqid>].suppress_k`, and it is
**raise-only**: the effective value is `max(SUPPRESS_K, override)`, so a
typo'd low value floors to the library default rather than weakening
disclosure control. The §6.8.2 validator rejects an override below the
floor outright (see `validate.py`).

`SUPPRESS_K` lives in `reg_monabundle.constants` rather than inside
`runtime.summarize` for two reasons: the §6.8.2 namespaced-block
validator must enforce the raise-only floor without importing the
runtime tier, and the constant must amalgamate into the bundle
alongside the runtime modules that suppress cells against it.
`runtime.summarize` imports it directly (and re-exports it so the
runtime keeps a stable import surface); mdw consumes it from the
top-level surface (`from reg_monabundle import SUPPRESS_K`).

## Single-file bundle, embedded config

`build_bundle` amalgamates the lightweight `reg_monabundle` slices
(`constants`, `validate`, `scan`) plus the `runtime.*` modules into one
self-contained `.py` for upload to MONA, and embeds the
`project_data.json` spec as a JSON string literal near the top
(`_PROJECT_DATA_JSON`). The runner deserializes that literal on load
and hands the resulting `LoadedSpec` to `extract.main()`; when the
literal is empty it falls back to a sidecar `project_data.json` in the
bundle directory (embedded wins when both are present). `reg_webapp`
builds one `.py` per upload via `POST /api/bundle`, running the same
validate-then-amalgamate sequence as the mdw CLI's `build-bundle`.

The amalgamator reads runtime modules off disk via `ast.parse` rather
than Python's import machinery (so an amalgamation-only env never needs
duckdb), then `ast.unparse`s the sliced body. Slicing drops the
`__future__` import, every intra-package import of an amalgamated prefix
(inlined directly into the artifact), `if TYPE_CHECKING:` blocks, and
**all** module/class/function docstrings — `ast.unparse` preserves
string-literal statements and `#` comments are already lost, so
stripping docstrings keeps the artifact text-clean (several mentioned
`reg_schema`/Pydantic) and slightly smaller. The repo source stays the
documentation; the bundle is the artifact.

The shipped runtime is the legacy **discover/extract two-MODE model**:
`MODE = "discover"` emits a metadata-only `mock_data_discovery.json`,
`MODE = "extract"` emits the typed-aggregate `mock_data_stats.json`.
Every binding must carry a hand-written `display_name` — the runtime
runs without reg_meta and rejects a binding that lacks one
(`runtime.spec._build_column`). Remaining: build-time `display_name`
pre-resolution from reg_meta, the realign-then-extract redesign, and
merged-mode — see REFACTOR_SPEC.md.

## Permissive embedded JSON, threat model

The embedded JSON literal is editable inline on MONA (e.g. a tweak under
time pressure) — **no checksum lock, no integrity hash**. The threat
model is: there is no adversary. The user is the sole editor; corrupting
the spec yields a clear stdlib parse/deserialize error naming the
offending path (see `runtime.spec._require`), not a silent
miscomputation. The webapp is the *recommended* authoring surface, not
the only one.

What runs **at bundle load on MONA** is only the pure-stdlib §6.8.2
namespaced-block validator (`validate_block`, via
`loadedspec_from_dict`) plus the step-4 runtime capability gates. The
§6.8.1 **structural** validation does NOT run on MONA — the bundle ships
no Pydantic and no reg_schema (§9.6), so structural validation is a
**build-time-only gate** (`build.spec_loader.validate_project_data`).
Drift introduced by an inline MONA edit is caught at the next webapp
round-trip, not on MONA.

## PII scanner

`scan.write_export` is the **final** step before any file leaves the
bundle's `output_dir`. Defense-in-depth: even though `summarize` only
emits aggregates, a misclassified column (e.g. a personnummer column
that flickered into the categorical bucket) could leak row-level data as
frequency-table keys. The scanner does an in-memory scan → temp-file →
atomic-rename, so the PII payload never touches disk on a dirty scan and
a half-written file can never become canonical. It matches three shapes
(personnummer, email, Swedish mobile), each behind a cheap pre-filter
guard and — for personnummer — a date-validity + Luhn gate to drop
arbitrary digit runs. Conservatively scoped to strings only: numeric
scalars aren't scanned because a plain large integer (a row count that
looks like a date) would false-positive. Also runnable ad-hoc as
`python -m reg_monabundle.scan <path>`.

Its regression corpus is **grow-only**: a PII shape the scanner misses
in production becomes a new fixture row the scanner must thereafter
flag. The corpus never shrinks, so a fix can't silently regress.

## Provenance-DB confinement

The reg_meta provenance DB is a maintainer-only build artifact that must
never leave the build host. The bundle is enforced free of any path to
it by an **output-artifact gate** on the emitted `.py`
(`tests/test_build_mona_bundle.py::test_bundle_carries_no_reg_meta_or_provenance`):
an AST import walk rejects any `reg_meta` / `reg_meta_build` /
`provenance` import, and a literal scan rejects the provenance-DB
filename and its constant name as text. (The gate is on the artifact,
not an allow-list inside the amalgamator — `reg_meta` simply isn't an
amalgamated prefix, so any reach would surface as a live import the gate
catches.) It does not raw-text-scan for `reg_meta` because the bundle
legitimately carries `reg_meta`-prefixed runtime helpers that consult an
embedded signal table via a caller-supplied connection.

## Bundle determinism

`build_bundle` is a pure function of its input: building the same spec
twice yields a byte-identical `.py` (no embedded timestamps, stable
module ordering). Remaining: a CI test pinning the byte-identical
invariant on a fixed-content fixture is not yet written (issue-tracked)
— see REFACTOR_SPEC.md.
