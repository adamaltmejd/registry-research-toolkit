---
name: run-reg-webapp
description: Run, screenshot, and drive the reg_webapp dev setup (FastAPI backend +
  Svelte SPA). Use when asked to run/start the webapp, verify a webapp change in the
  running app, screenshot the SPA, or smoke-test catalog browsing locally.
---

# Run reg_webapp locally

Two dev servers (FastAPI on :8000, Vite on :5173 with an `/api` proxy) plus a Playwright
driver that loads the SPA, drills through the catalog, exercises the period-resolve
form, and screenshots each step. All paths below are relative to **`reg_webapp/`**;
commands were verified on macOS against a real reg_meta DB.

## Prerequisites

- `uv` and `bun` (repo-standard toolchain — see root CLAUDE.md).
- A reg_meta DB where `reg_meta.db.db_path_from_args(None)` resolves (`REG_META_DB` >
  XDG, e.g. `~/.local/share/reg_meta/reg_meta.db`). A maintainer's `build-db` output
  works; without one, `uv run reg-meta update` fetches the latest release DB pair (not
  exercised here — a local DB existed).
- Playwright's Chromium. The frontend's vitest-browser setup already installs it
  (`~/Library/Caches/ms-playwright/chromium-*`); if missing:
  `bunx playwright install chromium` from `frontend/`.

## Setup

From the **repo root** (uv workspace) and the frontend:

```sh
uv sync --frozen
cd reg_webapp/frontend && bun install --frozen-lockfile
```

No SPA build needed for dev — Vite serves source. Regenerate API types only after a
contract change (`bun run gen:types`; CI pins drift).

## Run

**Quickest — `dev.sh` (works in any checkout, including worktrees):**

```sh
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh
```

Picks free backend + frontend ports, points the Vite `/api` proxy at the backend
(`REG_WEBAPP_BACKEND_URL`), starts both from THIS checkout's `.venv`, and prints the
URLs; Ctrl-C stops both. Ports are automatic, so two checkouts (parallel worktrees / PR
lanes) never collide — no manual port juggling. Pin them with
`BACKEND_PORT=… FRONTEND_PORT=…` when you need to know them up front (e.g. to script the
driver against a fixed `REG_WEBAPP_DEV_URL`).

**Manual (full control / background driving), from the repo root:**

```sh
uv run uvicorn reg_webapp.app:create_app --factory --port 8000 &
(cd reg_webapp/frontend && bun run dev) &
```

Wait a few seconds, then confirm all three hops before driving anything:

```sh
curl -s http://localhost:8000/api/context | head -c 120   # backend boots + DB found
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:5173/            # vite
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:5173/api/context # proxy
```

Then drive the SPA with the Playwright driver — run it **from `reg_webapp/frontend/`**
(it resolves `playwright` from the CWD's `node_modules`):

```sh
cd reg_webapp/frontend
bun ../.claude/skills/run-reg-webapp/driver.mjs smoke
```

`smoke` loads `/catalog`, clicks provider → register → variable, fills the period input
with `2022` and clicks **Apply** (expects "narrowed to 2022"), then cold-reloads the
deep link. Screenshots land in `/tmp/reg-webapp-shots/` (`01-root` …
`05-deep-link-reload`) — **look at them**. Other commands:

```sh
bun ../.claude/skills/run-reg-webapp/driver.mjs shot /catalog/scb/lisa
bun ../.claude/skills/run-reg-webapp/driver.mjs eval / "document.title"
```

Stop the servers when done:

```sh
lsof -ti :8000 -ti :5173 | xargs kill
```

## Parallel instances (concurrent worktrees / PR lanes)

`dev.sh` already picks free ports, so just run it in each checkout — two instances never
collide, no manual port juggling. (`preview_start` / `.claude/launch.json` are
fixed-port single-host: the static config can't inject the backend's chosen port into
the frontend config, so use `dev.sh` for concurrency.)

## Direct invocation (backend-only PRs)

Most backend changes don't need the SPA at all: the pytest suite runs against a fixture
DB (no real reg_meta DB required) — `uv run python -m pytest reg_webapp/` from the repo
root. Frontend unit/component tests: `bun run test` from `frontend/` (vitest, includes
the Playwright browser project).

## Run (human path)

Same two servers, then open <http://localhost:5173/> in a browser. Backend API docs at
<http://localhost:8000/docs>.

## Gotchas

- **`networkidle` is not "rendered".** Svelte swaps in fetched data after the network
  settles; a screenshot taken straight after navigation captures the loading
  placeholder. The driver's `settled()` waits for every `[aria-busy="true"]` element to
  clear — use it after every navigation/click. The attribute is the contract: each
  loading placeholder in `frontend/src/lib/*.svelte` carries `aria-busy="true"`, so new
  loading states must too (don't make the driver key on UI copy like "Loading…").
- **The first `a[href^="/catalog"]` is the header nav link** (it goes to `/catalog`, not
  deeper). To drill the tree, click the first link strictly deeper than the current path
  (`a[href^="<current>/"]`) — that's what `smoke` does.
- **The driver must run from `frontend/`** — bun resolves imports relative to the
  importing file, so the driver `createRequire`s playwright from the CWD. From anywhere
  else: `Cannot find package 'playwright'`.
- **HEAD requests 405** by design (routes register GET only; see DESIGN.md → ETag).
  Probe with `curl` GETs, not `-I`.
- The Vite proxy defaults to `http://localhost:8000` but honors `REG_WEBAPP_BACKEND_URL`
  (`frontend/vite.config.ts`) — `dev.sh` sets it automatically; it only matters if you
  start Vite by hand against a non-default backend port.
- **Git worktrees are auto-provisioned.** `.claude/hooks/worktree_bootstrap.sh`
  (`WorktreeCreate` + an idempotent `SessionStart` guard) runs `uv sync --frozen` +
  `bun install` so each worktree gets its OWN `.venv` (editable installs resolve to the
  worktree, not main) and `node_modules`. `dev.sh` also self-provisions and launches
  from the checkout's own `.venv`, so a worktree serves ITS code. The historical footgun
  — a `uv run` / `preview_start` started with the **main** checkout as cwd served main's
  source (bit an agent 2026-06-11) — is why `dev.sh` is preferred in a worktree; raw
  `preview_start` there still serves main.

## Troubleshooting

- `Cannot find package 'playwright'` → you ran the driver outside
  `reg_webapp/frontend/`. `cd` there first.
- Screenshot shows breadcrumbs + `Loading…` only → data fetch hadn't landed; re-run (the
  driver now waits via `settled()`), or raise its 10s timeout.
- Backend exits at boot complaining about the DB/schema → no resolvable reg_meta DB, or
  one with a stale `SCHEMA_VERSION`; install/refresh per Prerequisites.
