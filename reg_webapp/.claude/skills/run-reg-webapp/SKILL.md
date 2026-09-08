---
name: run-reg-webapp
description: Run, screenshot, and drive the reg_webapp dev setup (FastAPI backend +
  Svelte SPA). Use when asked to run/start the webapp, verify a webapp change in the
  running app, screenshot the SPA, or smoke-test catalog browsing locally.
---

# Run reg_webapp locally

Two dev servers — a FastAPI backend, and Vite serving the SPA with an `/api` proxy
pointed at that backend — plus a Playwright driver that loads the SPA, drills through
the catalog, exercises the period slider, and screenshots each step. `dev.sh` picks a
FREE port for each server on every run, so nothing here is pinned to a port.

This skill is a helper you invoke by its explicit repo path, not a skill the root loader
discovers: it lives under `reg_webapp/.claude/skills/`, which is nested and therefore
not walked. Every command below starts at the **repo root**.

## Prerequisites

- `uv` and `bun` (repo-standard toolchain — see root CLAUDE.md).
- Playwright's Chromium. The frontend's vitest-browser setup already installs it; if
  missing: `(cd reg_webapp/frontend && bunx playwright install chromium)`.
- A catalog to serve. `--fixture-db` builds a synthetic one and needs nothing installed
  — that is the default path below. Serving a real catalog instead needs a DB where
  `reg_meta.db.db_path_from_args(None)` resolves (`REG_META_DB` > XDG, e.g.
  `~/.local/share/reg_meta/reg_meta.db`).

## Setup

```sh
uv sync --frozen
(cd reg_webapp/frontend && bun install --frozen-lockfile)
```

No SPA build needed for dev — Vite serves source. Regenerate API types only after a
contract change (`(cd reg_webapp/frontend && bun run gen:types)`; CI pins drift).

## Run

**Visual verification (agents) — one-shot driver modes.** `dev.sh smoke` / `dev.sh shot`
pick free ports, run the Playwright driver against them, and **tear both servers down on
exit** — no port collisions, no leaked dev servers. Exit status is the driver's:

```sh
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh --fixture-db smoke
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh --fixture-db shot /catalog/scb/lisa
```

`smoke` loads `/catalog`, clicks provider → register → variable, narrows the leaf's
**Period** slider by one year from the keyboard and presses **Apply period** — asserting
both the `?period=` the app writes and the "narrowed to …" the page then shows — and
finally cold-reloads the deep link. Its steps land as `01-root` … `05-deep-link-reload`;
`shot` writes `_<route>.png`. **Look at them.**

**Where the pictures go.** One UNIQUE directory per invocation, under `/tmp`: every
route × viewport of that run shares it, two concurrent runs cannot overwrite each other,
and it outlives the servers so the images stay inspectable. `dev.sh` prints it —
together with the full HEAD, and the checkout it resolved from its OWN path rather than
your cwd — as its first line:

```
dev: repo /path/to/checkout HEAD 0123456789abcdef… shots /tmp/reg-webapp-shots.C8VNZN
```

Set `REG_WEBAPP_SHOTS=<dir>` to collect a run into a directory you own instead. A
relative `<dir>` is taken relative to the **repo root** (the checkout `dev.sh` resolved,
not your cwd); the reported line always shows the absolute path, and that is the path
the PNGs are written to.

**Responsive screenshots (`shot` viewports).** `shot` defaults to a 1280×900 desktop
viewport; viewport flags before the routes capture other breakpoints:

```sh
# one route, four breakpoints (375 / 768 / 1280 / 1920)
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh --fixture-db shot --all /catalog/scb/lisa
# just mobile + tablet, or an exact size
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh --fixture-db shot --mobile --tablet /catalog
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh --fixture-db shot --viewport 414x896 /catalog
```

Presets: `--mobile` (375×812), `--tablet` (768×1024), `--desktop` (1280×900), `--wide`
(1920×1080), `--all` (the four — the widths the design-reviewer skill judges, so `--all`
alone satisfies it), `--viewport WxH` (repeatable). Each viewport × route is shot;
non-desktop shots get a `-<label>` suffix (e.g. `_catalog_scb_lisa-mobile.png`,
`…-wide.png`, `…-414x896.png`) so they don't clobber the desktop shot.

**Inside a Yard lane.** The same commands work unchanged in the `.yard/Dockerfile` image
under the gates' `env -i` environment (no system `python3`, `HOME` wherever the runner
puts it): free ports come from this checkout's `.venv/bin/python`, and the driver honors
`PLAYWRIGHT_BROWSERS_PATH`, defaulting to the image's baked `/opt/pw-browsers` when it
is unset. Chromium is launched up a ladder — `sandboxed` (Chromium's own sandbox on),
then `no-sandbox` multi-process (a container running as a uid with no user-namespace
grant for the sandbox helper), then `single-process` (a sandboxed agent shell, issue
#1049) — and stderr names the rung that rendered, e.g.
`driver: chromium launched (no-sandbox)`, which is the one a lane container gets. Quote
that line as evidence. A lane must provision first the way the gates do (offline
`uv sync`, `cp -a /opt/frontend/node_modules reg_webapp/frontend/`).

**Project flows (`flows`) — what the `project-flows` and `catalog-flows` yard gates
run.** One command drives the whole project evidence set: five scenarios — an empty
project the backend blocks, an order request that fails in transport and is retried, a
validation request that fails and is retried, a draft authored from a catalog leaf
(picked, reloaded, recovered, then extended by a further pick on a cold catalog entry),
and a pick made on that leaf with no period chosen (refused, then recovered by choosing 2018)
— at 375×812, 768×1024, 1280×900 and 1920×1080 — 20 cases, each in a fresh browser
context against the real backend, with one failing request injected per error scenario
and none into the two catalog ones. It asserts the behavior (real 422 + `project_empty`,
which retry the banner offers, a real `order.json` download whose manifest entry matches
the synthetic catalog, the request counts behind a recovery, and what the browser's own
IndexedDB holds across reloads — and does not hold after a refusal) and writes 32 PNGs
into the directory you name:

```sh
db="$(mktemp -d)"
uv run python reg_webapp/.claude/skills/run-reg-webapp/catalog_fixture_db.py "$db"
REG_META_DB="$db" bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh flows /tmp/project-flows
```

The flows assert against the synthetic catalog those two lines set up (`scb/lisa/kon` at
variant `individer-15plus` → column `Kon`, and `scb/rams/syss` → column `Syss` for the
catalog-draft case's second pick), which `catalog_fixture_db.py` builds with the shared
fixture builder (`reg_webapp/backend/scripts/fixture_db.py`, below) — not a released DB.
A nonzero exit is a failed assertion, an unexpected JS page error, horizontal overflow
at some viewport, or a server that never started; the servers are torn down either way.

Naming scenarios after the output directory runs just those —
`dev.sh flows <dir> blocked-order order-retry validation-retry`, or
`dev.sh flows <dir> catalog-draft catalog-period-required`. The bare form above runs all
five and is the local verification invocation; the names exist for the gates.

In yard this is **two** gates (`.yard/config.toml`, both selected by the `ui` workflow),
each handed `$YARD_ARTIFACT_DIR` as its output directory: `project-flows` runs the three
`/project` scenarios and declares their 16 filenames, `catalog-flows` runs the two
catalog ones and declares their 16. The split is an artifact-list limit, not a
distinction of concern — a gate declares at most 16 filenames and the five scenarios
write 32 — and each gate names its own scenarios so neither runs the other's cases.
Unlike the ephemeral `/tmp` captures above, these are **retained**:
`yard lane show <lane>` prints the artifact paths for the execution — they outlive the
container and view cleanup, so open the PNGs there and judge them against
`reg_webapp/frontend/DESIGN.md`. The gate log carries the rest (route, scenario,
viewport, request counts, candidate HEAD).

**Deterministic UI verification (`--fixture-db`) — the default.** Pass `--fixture-db`
before the mode and `dev.sh` serves a *synthetic* catalog: it runs
`reg_webapp/backend/scripts/fixture_db.py` (the same builder the backend tests'
`catalog_db` / `docs_db` fixtures use) into a temp directory, exports it as
`REG_META_DB` for both servers, and deletes it on exit. Content is fixed — no seed, no
clock — so the DB pair is byte-identical run to run and a screenshot diff means a code
change, not catalog drift. It is small but populated enough that every route the
design-reviewer skill walks renders rows: `/`, `/catalog`, providers `fk` (register
`midas`) and `scb` (`lisa` / `rams`), bindings like `/catalog/scb/lisa/kon` (value set,
succession, lineage), the groups `/catalog/group/scb/rams/ink` and
`/catalog/group/class/sun`, `/search?q=kon`, `/project`, and `/doc/Kon.md`. `smoke`
drills it end to end.

```sh
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh --fixture-db          # interactive
```

Use it to verify *layout and interaction*, not catalog realism: the fixture has a
handful of rows, so density/overflow questions want a real or scratch DB instead.

**Alternative: a released or custom `REG_META_DB`.** Without `--fixture-db`, `dev.sh`
renders against whatever DB `reg_meta` resolves and inherits the caller's `REG_META_DB`
(a *directory*), which wins over the installed default (see Prerequisites). So a change
whose rendering depends on DB content not yet in the installed/released DB — a
`build-db` / curation change — is verified by building a scratch DB and pointing the dev
server at it; **no release required**:

```sh
db_dir="$(mktemp -d "${TMPDIR:-/tmp}/regmeta-verify.XXXXXX")"
reg-meta-build --db "$db_dir" build-db --input-dir <seed>
REG_META_DB="$db_dir" bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh shot <route>
```

(Verified: a non-default `REG_META_DB` directory renders correctly through `dev.sh`.)
Don't assume the installed/last-released DB is the only one the dev server can serve —
it's the default, not a constraint.

**Interactive (humans).** `dev.sh` with no mode starts the same auto-free-port servers
and stays up until Ctrl-C (which tears both down). It prints the URLs — open the
frontend in a browser, backend API docs at `<backend>/docs`. Ports are automatic, so
parallel worktrees / lanes never collide; pin with `BACKEND_PORT=… FRONTEND_PORT=…` if
you need to know them up front.

```sh
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh
```

## Parallel instances (concurrent worktrees / lanes)

Both runners are collision-free across parallel sessions — pick by need:

- **`preview_start` (interactive poking).** `.claude/launch.json` has a single
  `reg-webapp` config with `autoPort: true` whose entry point is `dev.sh preview`. The
  preview MCP picks a free frontend port (exported as `$PORT`; it does this even when it
  keeps the configured 5173) and `dev.sh preview` binds exactly that, then starts the
  backend on its own private free port and points the Vite `/api` proxy at it via
  `REG_WEBAPP_BACKEND_URL`. So two sessions each get a distinct frontend **and** backend
  port and a correctly-wired proxy — no collision, no cross-talk. (This replaced the old
  two-config `autoPort: false` setup, which collided because a static launch config
  can't inject the backend's chosen port into the frontend.) `preview_start` starts both
  servers under one `serverId`; the browser attaches to the frontend, and the backend is
  reached through the `/api` proxy.
- **`dev.sh smoke` / `dev.sh shot` (visual verification / screenshots).** Free ports, a
  per-invocation screenshot directory, guaranteed teardown, `shot --all` for the four
  responsive breakpoints. This is the path to reach for.

In a worktree both run from the checkout's own `.venv` (the `preview` entry routes
through `dev.sh`, which resolves the repo root from its own path and launches from
`.venv/bin/uvicorn`), so a worktree serves ITS code, not main's — the historical
"`preview_start` serves main" footgun is gone now that the entry point is `dev.sh`.

## Direct invocation (backend-only changes)

Most backend changes don't need the SPA at all: the pytest suite runs against a fixture
DB (no real reg_meta DB required) — `uv run python -m pytest reg_webapp/` from the repo
root. Frontend unit/component tests: `(cd reg_webapp/frontend && bun run test)` (vitest,
includes the Playwright browser project).

## Gotchas

- **`networkidle` is not "rendered".** Svelte swaps in fetched data after the network
  settles; a screenshot taken straight after navigation captures the loading
  placeholder. The driver's `settled()` waits for every `[aria-busy="true"]` element to
  clear — use it after every navigation/click, including before the first capture. The
  attribute is the contract: each loading placeholder in
  `reg_webapp/frontend/src/lib/*.svelte` carries `aria-busy="true"`, so new loading
  states must too (don't make the driver key on UI copy like "Loading…").
- **`waitForFunction` takes `(fn, arg, options)`.** Options passed as the second
  argument are silently the page-function ARGUMENT, so the timeout never applies. Pass
  `null` for the argument slot.
- **The first `a[href^="/catalog"]` is the header nav link** (it goes to `/catalog`, not
  deeper). To drill the tree, click the first link strictly deeper than the current path
  (`a[href^="<current>/"]`) — that's what `smoke` does.
- **Both year sliders label their thumbs "From year" / "To year"** — the header's
  project window and the leaf's period control share one primitive. Scope to the one you
  mean by its group (`Period window (years)` vs `Project window (years)`), the way
  `smoke` does, or the locator is ambiguous.
- **The driver must run from `reg_webapp/frontend/`** — bun resolves imports relative to
  the importing file, so the driver `createRequire`s playwright from the CWD. From
  anywhere else: `Cannot find package 'playwright'`. (`dev.sh` does this for you.)
- **HEAD requests 405** by design (routes register GET only; see DESIGN.md → ETag).
  Probe with `curl` GETs, not `-I`.
- The Vite proxy defaults to `http://localhost:8000` but honors `REG_WEBAPP_BACKEND_URL`
  (`reg_webapp/frontend/vite.config.ts`) — `dev.sh` sets it automatically; it only
  matters if you start Vite by hand against a non-default backend port.
- **Git worktrees are auto-provisioned.** A `SessionStart` hook
  (`.claude/hooks/worktree_bootstrap.sh`) gives the checkout its OWN `.venv` (editable
  installs resolve to the worktree, not main) and `node_modules` — it runs `uv sync` +
  `bun install` in the **background** (SessionStart gates the session, so it never
  blocks) when the env is missing or its dependency fingerprint is stale (lockfile
  changed). `dev.sh` also self-provisions synchronously and launches from the checkout's
  own `.venv`, so a worktree serves ITS code. (Deliberately NOT a `WorktreeCreate` hook:
  that event *replaces* git's worktree creation — a provisioner there would abort it.)
  The historical footgun — a `uv run` / `preview_start` started with the **main**
  checkout as cwd served main's source (bit an agent 2026-06-11) — is closed by the
  `dev.sh preview` entry point (see Parallel instances above).

## Troubleshooting

- `Cannot find package 'playwright'` → you ran the driver outside
  `reg_webapp/frontend/`. `cd` there first, or use `dev.sh`.
- Screenshot shows breadcrumbs + `Loading…` only → data fetch hadn't landed; re-run (the
  driver waits via `settled()`), or raise its 10s timeout.
- Backend exits at boot complaining about the DB/schema → no resolvable reg_meta DB, or
  one with a stale `SCHEMA_VERSION`. Use `--fixture-db`, or install/refresh a DB per
  Prerequisites.
- Chromium fails to launch → if every rung of the launch ladder (above) failed, the
  driver reports each rung's own error, unabridged — read the FIRST one, it is the
  failure of the sandboxed launch. In a sandboxed shell prefer `dev.sh smoke` / `shot`
  over `preview_start`, which has no ladder.
