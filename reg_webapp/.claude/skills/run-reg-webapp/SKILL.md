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

**Visual verification (agents) — one-shot driver modes.** `dev.sh smoke` / `dev.sh shot`
pick free ports, run the Playwright driver against them, and **tear both servers down on
exit** — no port collisions, no leaked dev servers. This is the path the PR pipeline's
visual-verification step uses; exit status is the driver's:

```sh
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh smoke                   # full smoke flow
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh shot /catalog/scb/lisa  # specific route(s)
```

`smoke` loads `/catalog`, clicks provider → register → variable, fills the period input
with `2022` and clicks **Apply** (expects "narrowed to 2022"), then cold-reloads the
deep link. Screenshots land in `/tmp/reg-webapp-shots/` (`01-root` …
`05-deep-link-reload`; `shot` writes `_<route>.png`) — **look at them**.

**Responsive screenshots (`shot` viewports).** `shot` defaults to a 1280×900 desktop
viewport, but viewport flags before the routes capture other breakpoints — this is how
the free-port path does responsive/mobile visual checks (no fixed-port preview server
needed):

```sh
# one route, three breakpoints (375 / 768 / 1280)
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh shot --all /catalog/scb/lisa
# just mobile + tablet, or an exact size
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh shot --mobile --tablet /catalog
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh shot --viewport 414x896 /catalog
```

Presets: `--mobile` (375×812), `--tablet` (768×1024), `--desktop` (1280×900), `--all`
(the three), `--viewport WxH` (repeatable). Each viewport × route is shot; non-desktop
shots get a `-<label>` suffix (e.g. `_catalog_scb_lisa-mobile.png`, `…-414x896.png`) so
they don't clobber the desktop shot.

**Verifying against unreleased DB content (custom DB).** `dev.sh` renders against
whatever DB `reg_meta` resolves, and `$REG_META_DB` (a *directory*) wins over the
installed default (see Prerequisites) — `dev.sh` never sets it, so it inherits the
caller's env. So to verify a change whose rendering depends on DB content not yet in the
installed/released DB — a `build-db` / curation change, e.g. an earlier PR in the same
lane — build a scratch DB and point the dev server at it; **no release required**:

```sh
db_dir="$(mktemp -d "${TMPDIR:-/tmp}/regmeta-verify.XXXXXX")"
reg-meta-build --db "$db_dir" build-db --input-dir <seed>   # the merge gate builds this anyway
REG_META_DB="$db_dir" bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh shot <route>
```

(Verified: a non-default `REG_META_DB` directory renders correctly through `dev.sh`.)
Don't assume the installed/last-released DB is the only one the dev server can serve —
it's the default, not a constraint.

**Interactive (humans).** `dev.sh` with no args starts the same auto-free-port servers
and stays up until Ctrl-C (which tears both down). It prints the URLs — open the
frontend in a browser, backend API docs at `<backend>/docs`. Ports are automatic, so
parallel worktrees / lanes never collide; pin with `BACKEND_PORT=… FRONTEND_PORT=…` if
needed.

```sh
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh
```

**Manual (escape hatch).** Only when you need long-lived background servers for custom
driving — fixed ports, and **you** must tear down (prefer the modes above, which do it
for you):

```sh
uv run uvicorn reg_webapp.app:create_app --factory --port 8000 &
(cd reg_webapp/frontend && bun run dev) &
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:5173/api/context  # 200 ⇒ proxy ok
(cd reg_webapp/frontend && bun ../.claude/skills/run-reg-webapp/driver.mjs eval / "document.title")
lsof -ti :8000 -ti :5173 | xargs kill   # don't forget this
```

## Parallel instances (concurrent worktrees / PR lanes)

Both runners are now collision-free across parallel sessions — pick by need:

- **`preview_start` (interactive poking).** `.claude/launch.json` has a single
  `reg-webapp` config with `autoPort: true` whose entry point is `dev.sh preview`. The
  preview MCP picks a free frontend port (exported as `$PORT`; it does this even when it
  keeps the configured 5173) and `dev.sh preview` binds exactly that, then starts the
  backend on its own private free port and points the Vite `/api` proxy at it via
  `REG_WEBAPP_BACKEND_URL`. So two sessions each get a distinct frontend **and** backend
  port and a correctly-wired proxy — no `:8000`/`:5173` collision, no cross-talk. (This
  replaced the old two-config `autoPort: false` setup, which collided because a static
  launch config can't inject the backend's chosen port into the frontend.)
  `preview_start` starts both servers under one `serverId`; the browser attaches to the
  frontend, and the backend is reached through the `/api` proxy (it has no standalone
  preview entry — for raw backend/`/docs` poking use the manual escape hatch above).
- **`dev.sh smoke` / `dev.sh shot` (visual verification / screenshots).** Free ports,
  guaranteed teardown, `shot --all` for responsive breakpoints — the PR-pipeline path.

In a worktree both run from the checkout's own `.venv` (the `preview` entry routes
through `dev.sh`, which `cd`s to `git rev-parse --show-toplevel` and launches from
`.venv/bin/uvicorn`), so a worktree serves ITS code, not main's — the historical
"`preview_start` serves main" footgun is gone now that the entry point is `dev.sh`.

## Direct invocation (backend-only PRs)

Most backend changes don't need the SPA at all: the pytest suite runs against a fixture
DB (no real reg_meta DB required) — `uv run python -m pytest reg_webapp/` from the repo
root. Frontend unit/component tests: `bun run test` from `frontend/` (vitest, includes
the Playwright browser project).

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
- **Git worktrees are auto-provisioned.** A `SessionStart` hook
  (`.claude/hooks/worktree_bootstrap.sh`) gives the checkout its OWN `.venv` (editable
  installs resolve to the worktree, not main) and `node_modules` — it runs `uv sync` +
  `bun install` in the **background** (SessionStart gates the session, so it never
  blocks) when the env is missing or its dependency fingerprint is stale (lockfile
  changed). `dev.sh` also self-provisions synchronously and launches from the checkout's
  own `.venv`, so a worktree serves ITS code. (Deliberately NOT a `WorktreeCreate` hook:
  that event *replaces* git's worktree creation — a provisioner there would abort it.)
  The historical footgun — a `uv run` / `preview_start` started with the **main**
  checkout as cwd served main's source (bit an agent 2026-06-11) — is closed now that
  `preview_start` routes through `dev.sh preview`, which `cd`s to its own toplevel and
  launches from that checkout's `.venv` (verified: the preview ran with cwd = the
  worktree and served the worktree's `.venv`). `dev.sh` is still the right tool for any
  one-shot/screenshot work.

## Troubleshooting

- `Cannot find package 'playwright'` → you ran the driver outside
  `reg_webapp/frontend/`. `cd` there first.
- Screenshot shows breadcrumbs + `Loading…` only → data fetch hadn't landed; re-run (the
  driver now waits via `settled()`), or raise its 10s timeout.
- Backend exits at boot complaining about the DB/schema → no resolvable reg_meta DB, or
  one with a stale `SCHEMA_VERSION`; install/refresh per Prerequisites.
