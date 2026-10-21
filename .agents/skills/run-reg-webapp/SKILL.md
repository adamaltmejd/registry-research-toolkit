---
name: run-reg-webapp
description: Run, screenshot, and drive the reg_webapp dev setup (Rust server + Svelte
  SPA). Use when asked to run/start the webapp, verify a webapp
  change in the running app, screenshot the SPA, or smoke-test catalog browsing locally.
---

# Run reg_webapp locally

The tooling lives in `reg_webapp/dev/`: `dev.sh` starts the Rust server
(`reg-meta serve`) and Vite (whose `/api` proxy, in
`reg_webapp/frontend/vite.config.ts`, sends every route to the Rust server) on FREE
ports, and `driver.mjs` is the Playwright driver. Every command starts at the **repo
root**; `dev.sh` serves the checkout it lives in, whatever your cwd.

## Prerequisites

`dev.sh` installs the checkout's own `.venv` (`uv sync --frozen`) and
`reg_webapp/frontend/node_modules` (`bun install --frozen-lockfile`) on every run — a
no-op when in sync, so a fresh worktree needs no setup. It then runs
`cargo build --workspace` and `bun run gen:wasm`. You need:

- `uv`, `bun` and `cargo`; for `gen:wasm`, `rustup target add wasm32-unknown-unknown`
  and `cargo install wasm-bindgen-cli --version <v> --locked`, with `<v>` the `=` pin in
  `crates/reg-core-wasm/Cargo.toml`.
- Playwright's Chromium (the frontend's vitest-browser setup installs it; if missing:
  `(cd reg_webapp/frontend && bunx playwright install chromium)`).
- A catalog: `--fixture-db` (below), or `REG_META_DB` set to a directory holding a
  release asset (`reg_meta.db` + `reg_meta_docs.db`; root `README.md`). The server
  refuses a catalog whose schema it does not admit (`SCHEMA` in
  `crates/reg-catalog/src/lib.rs`) with `schema_incompatible`.

## Modes

```sh
bash reg_webapp/dev/dev.sh --fixture-db smoke
bash reg_webapp/dev/dev.sh --fixture-db shot [--mobile|--tablet|--desktop|--wide|--all|--viewport WxH]... <route>...
bash reg_webapp/dev/dev.sh --fixture-db flows <out-dir> [scenario...]
bash reg_webapp/dev/dev.sh --fixture-db          # interactive, Ctrl-C stops
```

`smoke`, `shot` and `flows` are one-shot: free ports, then **every server is torn down
on exit**, and the exit status is the driver's.

- **`smoke`** drills `/catalog` → provider → register → variable, narrows the leaf's
  **Period** slider from the keyboard, applies it (asserting `?period=` and the
  "narrowed to …" text), then cold-reloads the deep link. Steps land as `01-root` …
  `05-deep-link-reload`. Desktop only — not a substitute for the four widths.
- **`shot`** writes `_<route>.png` per route × viewport; non-desktop shots get a
  `-<label>` suffix. `--all` is the four widths (375 / 768 / 1280 / 1920) the
  design-reviewer skill judges.
- **`flows`** runs eight project scenarios (blocked order, order/validation retries,
  catalog-authored draft, period-required pick, deliberate replace, per-source period
  edit, keyboard period focus) at four viewports, each in a fresh browser context
  against the real servers, and writes the PNGs into `<out-dir>`. Name scenarios to run
  a subset
  (`blocked-order order-retry validation-retry catalog-draft catalog-period-required project-source-period catalog-period-focus replace-confirm`).
  It asserts on the `--fixture-db` content (`scb/lisa/kon` at `individer-15plus`,
  `scb/rams/syss`, `scb/lisa/forsamling`), so run it with `--fixture-db`. The gate's
  `flows` step (`uv run --no-project scripts/gate.py flows`) runs all eight that way.
- **Interactive** prints the URLs; the Rust server's OpenAPI document is at
  `<rust>/openapi.json`. Pin ports with `RUST_PORT=… FRONTEND_PORT=…` if needed.

**Look at the pictures.** The first line `dev.sh` prints names the checkout, the full
HEAD and the per-invocation shots directory under `/tmp`, kept after teardown:

```
dev: repo /path/to/checkout HEAD 0123456789abcdef… shots /tmp/reg-webapp-shots.C8VNZN
```

`REG_WEBAPP_SHOTS=<dir>` collects a run elsewhere (relative to the repo root).

**`--fixture-db`** serves a deterministic synthetic catalog + docs DB pair as
`REG_META_DB`: the real builder's output for `conformance/cases/fixtures/webapp-dev`,
from the shared fixture cache (`conformance/fixture_cache.py`, built on a miss), copied
into a temp dir that is deleted on exit. It is byte-identical run to run, so a
screenshot diff means a code change. It covers every route the design reviewer walks
(`/`, `/catalog`, `fk/midas`, `scb/lisa`, `scb/rams`, `/catalog/scb/lisa/kon`,
`/catalog/group/scb/rams/ink`, `/catalog/group/class/sun`, `/search?q=kon`, `/project`,
`/doc/Kon.md`) but has a handful of rows — use a real DB for density questions. With
`REG_WEBAPP_STEWARD` set, the catalog is the conformance suite's synthetic steward
artifact (`conformance/fixture_cache.py`) instead.

**A scratch DB** verifies content not yet released (a `build-db` / curation change):

```sh
REG_META_DB="$db_dir" bash reg_webapp/dev/dev.sh shot <route>
```

## Parallel instances (concurrent worktrees)

- **`preview_start`**: `.claude/launch.json`'s `reg-webapp` config (`autoPort: true`)
  runs `dev.sh preview`, which binds the frontend to the MCP-assigned `$PORT` and the
  Rust server to a private free port wired through `REG_META_SERVER_URL`. Sessions never
  collide.
- **`dev.sh smoke` / `shot`**: free ports, per-run shots directory, guaranteed teardown.
  The path to reach for.

Server-only changes rarely need the SPA: the conformance `api` corpus runs the Rust
server on synthetic fixtures (`uv run --no-project scripts/gate.py rust`). Frontend
component tests: `(cd reg_webapp/frontend && bun run test)`.

## Gotchas

- **`networkidle` is not "rendered".** Svelte swaps in data after the network settles.
  The driver's `settled()` waits for every `[aria-busy="true"]` to clear — use it after
  every navigation/click. Each loading placeholder carries `aria-busy="true"`; new ones
  must too (don't key on copy like "Loading…").
- **`waitForFunction` takes `(fn, arg, options)`.** Options as the second argument
  become the page-function argument and the timeout never applies; pass `null` for
  `arg`.
- **The first `a[href^="/catalog"]` is the header nav link.** To drill, click the first
  link strictly deeper than the current path (`a[href^="<current>/"]`).
- **Both year sliders label their thumbs "From year" / "To year".** Scope by group
  (`Period window (years)` vs `Project window (years)`).
- **The driver must run from `reg_webapp/frontend/`** (it `createRequire`s playwright
  from the cwd): `cd reg_webapp/frontend && bun ../dev/driver.mjs …`. Elsewhere:
  `Cannot find package 'playwright'`. `dev.sh` does this for you.
- The Vite proxy defaults to `http://127.0.0.1:8001` but honors `REG_META_SERVER_URL`;
  `dev.sh` sets it.
- Every route the SPA calls is the Rust server's; bodies are `{data, meta}` (a refusal
  `{error, meta}`).

## Troubleshooting

- Screenshot shows breadcrumbs + `Loading…` → the fetch hadn't landed; re-run, or raise
  `settled()`'s 10 s timeout.
- The Rust server exits at boot about the DB/schema → no `REG_META_DB`, or an
  inadmissible schema. Use `--fixture-db` or a current release asset.
- Chromium fails to launch → the driver tries a ladder (sandboxed, no-sandbox,
  single-process; `LAUNCH_LADDER` in `driver.mjs`) and reports each rung's error; read
  the FIRST. In a sandboxed shell prefer `dev.sh smoke` / `shot` over `preview_start`,
  which has no ladder.
