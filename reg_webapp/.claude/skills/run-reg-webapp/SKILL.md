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

## Run (agent path)

Start both servers in the background, **from the repo root**:

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
- The Vite proxy targets `http://localhost:8000` (hardcoded in
  `frontend/vite.config.ts`) — the backend must be on :8000, not a random port.
- **Verifying from a git worktree (`.claude/worktrees/*`): anything launched with the
  main checkout as cwd serves main's code.** "Repo root" above means the checkout under
  test — `uv run uvicorn …` at the main checkout's root, and the `preview_start`
  launch.json configs (which always run there), bind the main checkout's venv and
  silently exercise stale main-branch behavior (bit an agent 2026-06-11: a fix "didn't
  work" because the servers ran main's code). Verified fix: `uv sync --frozen` inside
  the worktree (uv stops at the worktree's own `pyproject.toml`, creating a
  worktree-local `.venv`), then start the backend via that venv's binary
  (`.venv/bin/uvicorn reg_webapp.app:create_app --factory --port 8000`) and the frontend
  via `bun run dev` from the worktree's `reg_webapp/frontend/`.

## Troubleshooting

- `Cannot find package 'playwright'` → you ran the driver outside
  `reg_webapp/frontend/`. `cd` there first.
- Screenshot shows breadcrumbs + `Loading…` only → data fetch hadn't landed; re-run (the
  driver now waits via `settled()`), or raise its 10s timeout.
- Backend exits at boot complaining about the DB/schema → no resolvable reg_meta DB, or
  one with a stale `SCHEMA_VERSION`; install/refresh per Prerequisites.
