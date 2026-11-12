---
name: run-reg-webapp
description: Run, screenshot, and drive the reg_webapp dev setup (Rust server + React
  SPA). Use when asked to run/start the webapp, verify a webapp
  change in the running app, screenshot the SPA, or smoke-test catalog browsing locally.
---

# Run reg_webapp locally

The tooling lives in `reg_webapp/dev/`: `dev.sh` starts the Rust server
(`reg-meta serve`) and Vite (whose `/api` proxy, in
`reg_webapp/frontend/vite.config.ts`, sends every route to the Rust server) on FREE
ports. `smoke`, `shot` and `flows` drive the page with
[agent-browser](https://github.com/vercel-labs/agent-browser) (a frontend devDep, pinned
exactly in `reg_webapp/frontend/package.json`); `flows` runs the user journeys in
`journeys.sh`. `dev.sh` serves the checkout it lives in.

**Run it exactly as `bash reg_webapp/dev/dev.sh <flags> [mode]` from the repo or
worktree root: no env-var prefix, no `cd X &&`.** The Claude Code sandbox exclusion
(`.claude/settings.json`) matches the command text, and a command that does not match
stays sandboxed, where Chrome dies. Pass only the flags below: ports, the cargo target
and the shots directory are automatic and per-run, so concurrent runs never collide.
`bash reg_webapp/dev/dev.sh --help` prints the usage.

## Prerequisites

`dev.sh` installs the checkout's own `reg_webapp/frontend/node_modules`
(`bun install --frozen-lockfile`) on every run — a no-op when in sync, so a fresh
worktree needs no setup. It then runs `cargo build --workspace` and `bun run gen:wasm`.
You need:

- `bun` and `cargo`; for `gen:wasm`, `rustup target add wasm32-unknown-unknown` and
  `cargo install wasm-bindgen-cli --version <v> --locked`, with `<v>` the `=` pin in
  `crates/reg-core-wasm/Cargo.toml`.
- For `smoke`, `shot` and `flows`: a Chrome for agent-browser. It finds an installed
  Google Chrome; with none, run
  `bun reg_webapp/frontend/node_modules/agent-browser/bin/agent-browser.js install` once
  (Chrome for Testing into `~/.agent-browser`); the same command with `doctor` says
  which browser it resolves.
- A catalog: `--fixture-db` or `--paired-fixture` (below), or `--db DIR`, a directory
  holding a release asset (`reg_meta.db`; root `README.md`). The server refuses a catalog whose
  schema it does not admit (`CATALOG_SCHEMA_VERSION` in `crates/reg-core/src/lib.rs`)
  with `schema_incompatible`.

## Modes

```sh
bash reg_webapp/dev/dev.sh --fixture-db smoke
bash reg_webapp/dev/dev.sh --fixture-db shot [--mobile|--tablet|--desktop|--wide|--all|--viewport WxH]... <route>...
bash reg_webapp/dev/dev.sh --fixture-db flows <out-dir> [journey...]
bash reg_webapp/dev/dev.sh --fixture-db          # interactive, Ctrl-C stops
bash reg_webapp/dev/dev.sh --paired-fixture ...  # the same modes on the synthetic pair
bash reg_webapp/dev/dev.sh --db DIR [--catalog NAME] ...  # a real or scratch catalog
```

`--catalog NAME` also serves the holdings partner `reg_meta_<NAME>.db` in `DIR` (a
steward site, e.g. `swecov`).

`--paired-fixture` (instead of `--fixture-db`) builds the synthetic steward holdings
pair into a temp directory (`target/debug/reg-fixture --paired`) and exports
`REG_WEBAPP_FIXTURE=paired`. The SPA is steward-only, so the pair is the state to render
for steward pages (e.g. `/catalog/demo/held`).

`smoke`, `shot` and `flows` are one-shot: free ports, then **every server (and the
agent-browser session) is torn down on exit**, and the exit status is the run's.

Every `smoke` / `shot` capture is three files under one name: `<name>.png`,
`<name>.snapshot.txt` (the accessibility tree, `agent-browser snapshot -c`) and
`<name>.a11y.txt` (an axe-core audit, `agent-browser a11y`). Each capture prints its axe
summary line, then the page's console errors (`[error] …`) and uncaught exceptions
(`[pageerror] …`). Axe violations do not fail the run; judging them is the reviewer's
job.

- **`smoke`** captures `/` as `01-home`, desktop only; not a substitute for the
  four widths.
- **`shot`** captures `_<route>` per route × viewport; non-desktop captures get a
  `-<label>` suffix. `--all` is the four widths (375 / 768 / 1280 / 1920) the
  design-reviewer skill judges.
- **`flows`** runs the user journeys (the `JOURNEYS` table in `journeys.sh`: `browse`,
  `search`, `variable`, `classification`, `order` and `narrow` on `--fixture-db`,
  `steward` on `--paired-fixture`; name some to run only those), each in a fresh
  agent-browser session against the real servers, at desktop width except `narrow` (375
  px). It writes one PNG per journey into `<out-dir>` and prints each journey's time.
  Journeys click with the pointer and assert only the rendered page; a click a journey
  cannot make is a suspected UI defect. `bash scripts/gate.sh flows` also runs the
  production-build smoke (`reg_webapp/frontend/scripts/smoke_build.ts`).
- **Interactive** prints the URLs; the Rust server's OpenAPI document is at
  `<rust>/openapi.json`.

**Look at the pictures.** The first line `dev.sh` prints names the checkout, the full
HEAD and the per-invocation shots directory under `$TMPDIR`, kept after teardown:

```
dev: repo /path/to/checkout HEAD 0123456789abcdef… shots $TMPDIR/reg-webapp-shots.C8VNZN
```

**`--fixture-db`** serves a deterministic synthetic catalog as `REG_META_DB`: current
`reg-build` output for `conformance/cases/fixtures/webapp-dev`, from the shared fixture
cache (`target/debug/reg-fixture`, built on a miss), copied into a temp dir that is
deleted on exit. It contains `demo/bas`, `demo/hush` and `demo2/reg`. Use a real DB for
density questions.

**A scratch DB** verifies content not yet released (a `build-db` / curation change):

```sh
bash reg_webapp/dev/dev.sh --db "$db_dir" shot <route>
```

## Parallel instances (concurrent worktrees)

- **`preview_start`**: `.claude/launch.json`'s `reg-webapp` config (`autoPort: true`)
  runs `dev.sh --fixture-db preview`; its `reg-webapp-paired` config runs
  `dev.sh --paired-fixture preview`. Both bind the frontend to the MCP-assigned `$PORT`
  and the Rust server to a private free port wired through `REG_META_SERVER_URL`.
  Sessions never collide.

- **`dev.sh smoke` / `shot`**: free ports, per-run shots directory, guaranteed teardown.
  The path to reach for.

- **Driving a state `shot` cannot reach** (a dialog, a filled form): start `dev.sh`
  interactively in the background and drive the printed frontend URL with the pinned
  agent-browser in a **named session** — the default session is one browser shared by
  every agent on the machine:

  ```sh
  ab="bun reg_webapp/frontend/node_modules/agent-browser/bin/agent-browser.js --session $(basename "$PWD")-ui"
  $ab open http://localhost:<port>/catalog/demo/bas
  $ab snapshot -i         # interactive elements with @refs to click/fill
  $ab set viewport 375 812 && $ab screenshot "$TMPDIR/bas-mobile.png"
  $ab a11y                # axe audit of the current page
  $ab close
  ```

Server-only changes rarely need the SPA: the conformance `api` corpus runs the Rust
server on synthetic fixtures (`bash scripts/gate.sh rust`). Frontend unit tests (jsdom,
vitest): `(cd reg_webapp/frontend && bun run test)`.

## Gotchas

- **`networkidle` is not "rendered".** React swaps in data after the network settles.
  `settled()` (in `dev.sh`, used by `journeys.sh`) waits for every `[aria-busy="true"]`
  to clear — by hand, `$ab wait --fn '!document.querySelector("[aria-busy=\"true\"]")'`
  after every navigation/click. Each loading placeholder carries `aria-busy="true"`; new
  ones must too (don't key on copy like "Loading…").
- **agent-browser 0.41.2's text `errors` prints a bare `✗`** whatever it holds; read
  `errors --json` (`dev.sh` does).
- **`find role … --name` does not wait**: it fails at once when the element is not there
  yet, and agent-browser has no role selector for `wait`. `journeys.sh`'s `see` retries
  it for 10 s; `press` and `fill` go through `see`. `--exact` matches the whole name.
- **File paths go absolute** (`upload`, `download`, `screenshot`): the daemon resolves a
  relative path against its own cwd.
- **Never drive agent-browser with a sync spawn from the process serving the page**
  (`smoke_build.ts` runs Vite preview in process): the blocked event loop serves nothing
  and the command fails with `os error 35`.
- **The first `a[href^="/catalog"]` is the header nav link.** To drill, click the first
  link strictly deeper than the current path (`a[href^="<current>/"]`).
- **Every year slider labels its thumbs "From year" / "To year".** Scope by group:
  `Years to add` (variable coverage), `Years for <table>` (source card) and
  `Study years` (project banner).
- The Vite proxy defaults to `http://127.0.0.1:8001` but honors `REG_META_SERVER_URL`;
  `dev.sh` sets it.
- Every route the SPA calls is the Rust server's; bodies are `{data, meta}` (a refusal
  `{error, meta}`).

## Troubleshooting

- Screenshot shows breadcrumbs + `Loading…` → the fetch hadn't landed; re-run. A
  placeholder that never clears and lacks `aria-busy="true"` is the defect.
- The Rust server exits at boot about the DB/schema → an inadmissible schema. Use
  `--fixture-db` or a current release asset.
- Chrome aborts at launch (`smoke` / `shot` / `flows`) → the command ran sandboxed:
  re-run it in the exact form above. Do not work around it with Chrome flags. Exit 3
  is the Codex sandbox, where rendered checks cannot run. `$ab doctor` names the
  browser agent-browser resolved.
- `agent-browser install` writes `~/.agent-browser`; run it outside the sandbox, or use
  an installed Google Chrome.
