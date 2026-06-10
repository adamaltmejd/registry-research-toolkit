# reg_webapp deployment image (REFACTOR_SPEC.md §6.5 — Deployment).
#
# Build context is the REPO ROOT (not reg_webapp/), because reg-webapp is a uv
# workspace member whose deps (reg-meta / reg-schema / reg-monabundle) resolve
# via [tool.uv.sources] against the root uv.lock — the lockfile and the sibling
# member source trees must all be in context. Build from the repo root with:
#
#   docker build -f reg_webapp/Dockerfile -t reg-webapp .
#
# Three baked-in stages: (1) bun builds the SPA, (2) uv installs the Python
# backend + workspace deps and runs `reg-meta update` to bake the release DB
# pair into a layer, (3) a lean runtime that serves the JSON API with uvicorn
# behind an entrypoint smoke gate.
#
# Base images are pinned by tag (not :latest) for deterministic rebuilds. Bump
# the tags deliberately; floating tags would make image content non-reproducible.

# ---------------------------------------------------------------------------
# Stage 1 — frontend: build the Svelte SPA with bun.
# ---------------------------------------------------------------------------
# The SPA codegens src/lib/api-types.ts from the committed backend/openapi.json,
# which is already in the tree (CI gates it against drift), so no backend is
# needed at SPA-build time. Output is the static dist/ bundle (Vite default
# outDir), carried forward to the runtime stage. The backend stays a pure JSON
# API (see reg_webapp/DESIGN.md → "SPA routing + production fallback"): it mounts
# no StaticFiles, so the SPA is shipped as a static artifact for the edge /
# reverse proxy to serve. This image bakes dist/ at /opt/reg_webapp/spa so a
# single-container deploy can point any static server at it; the production edge
# (Cloudflare) does the deep-link → index.html rewrite.
FROM oven/bun:1.3.0-alpine AS frontend

WORKDIR /build/frontend

# Install deps against the committed lockfile first (cache layer keyed on the
# manifests only — source edits don't bust the dep install).
COPY reg_webapp/frontend/package.json reg_webapp/frontend/bun.lock ./
RUN bun install --frozen-lockfile

# The committed API contract the SPA types are generated from.
COPY reg_webapp/backend/openapi.json /build/backend/openapi.json
COPY reg_webapp/frontend/ ./

# Regenerate types from the committed contract, then production-build. gen:types
# is a no-op against a clean tree (CI pins it), but running it keeps the image
# build self-consistent if a stale base-layer cache carried older types.
RUN bun run gen:types && bun run build

# ---------------------------------------------------------------------------
# Stage 2 — backend build: install the Python backend + bake the release DB.
# ---------------------------------------------------------------------------
# Pin BOTH the interpreter and uv deterministically: base on python:3.13.7-slim
# (same image the runtime stage uses, so the baked venv's interpreter path
# matches) and copy a version-pinned uv binary off its official image. uv's own
# combined `uv:<ver>-python<x>-bookworm-slim` tags don't pin the uv version and
# python jointly the way we need, so we compose the pin ourselves.
FROM python:3.13.7-slim-bookworm AS backend
COPY --from=ghcr.io/astral-sh/uv:0.11.19 /uv /usr/local/bin/uv

# Deterministic install: bytecode-compile ahead of time, copy (not symlink) into
# .venv so the venv is self-contained when copied to the runtime stage.
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_INSTALLER_METADATA=1

WORKDIR /app

# Copy the workspace skeleton needed for `uv sync --frozen` to resolve. The root
# pyproject + uv.lock define the workspace; every member listed in
# [tool.uv.workspace].members must have its source tree present for the locked
# resolution to validate, even though we only install the reg-webapp package.
COPY pyproject.toml uv.lock ./
COPY reg_meta/ ./reg_meta/
COPY reg_meta_build/ ./reg_meta_build/
COPY reg_monabundle/ ./reg_monabundle/
COPY reg_schema/ ./reg_schema/
COPY mock_data_wizard/ ./mock_data_wizard/
COPY reg_webapp/backend/ ./reg_webapp/backend/

# Install ONLY the reg-webapp package + its runtime deps (reg-meta / reg-schema /
# reg-monabundle), no dev deps. --frozen asserts the lockfile is up to date (CI
# parity). This pulls the `reg-meta` console script into /app/.venv/bin.
#
# uv installs the workspace members as EDITABLE (.pth links into /app/<member>/src)
# regardless of --no-editable for a lockfile-driven workspace sync, so the runtime
# stage copies those member src trees alongside the venv (below) — the venv is not
# self-contained on its own.
RUN uv sync --frozen --no-dev --package reg-webapp

# Bake the matching reg_meta release DB pair (reg_meta.db + reg_meta_docs.db)
# into an image layer. reg-meta update resolves the newest `reg_meta/v*` release
# carrying reg_meta.db.zst / reg_meta_docs.db.zst and installs into --db <dir>.
# The DB schema matches the installed reg_meta code (reg_meta/v0.9.0 → SCHEMA
# 5.2.0); rebuilding the image after a reg_meta bump re-bakes the matching pair.
ENV REG_META_DB=/opt/reg_meta
RUN uv run reg-meta update --db "$REG_META_DB" --yes \
    && test -f "$REG_META_DB/reg_meta.db" \
    && test -f "$REG_META_DB/reg_meta_docs.db"

# The published DBs ship in WAL mode. reg_meta's `open_db` opens `?mode=ro`
# (no `immutable=1`), and a read-only open of a WAL-mode DB still needs to create
# `-shm`/`-wal` sidecars — which fails as a non-root user against a root-owned,
# read-only catalog dir ("attempt to write a readonly database"). Checkpoint the
# WAL into the main DB and switch journal mode to DELETE so the read-only open is
# fully self-contained (no sidecar writes). Idempotent + deterministic; touches
# only the baked image copy, never reg_meta's shipped artifact. Also drop any
# leftover decompression `.tmp-*` sidecars.
RUN for db in "$REG_META_DB/reg_meta.db" "$REG_META_DB/reg_meta_docs.db"; do \
        python -c "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); \
c.execute('PRAGMA wal_checkpoint(TRUNCATE)'); c.execute('PRAGMA journal_mode=DELETE'); \
c.commit(); c.close()" "$db"; \
    done \
    && rm -f "$REG_META_DB"/*.tmp-shm "$REG_META_DB"/*.tmp-wal \
    && find "$REG_META_DB" -type f -name '*-wal' -delete \
    && find "$REG_META_DB" -type f -name '*-shm' -delete

# #224 provenance-confinement assertion #3 (deployment-side). Assertions #1
# (bundle amalgamates no provenance-importing module) and #2 (no FastAPI route
# references the provenance path — backend/tests/test_write_boundary.py) already
# ship; this is the third. `reg-meta update` only fetches reg_meta.db.zst /
# reg_meta_docs.db.zst (verified in reg_meta/download.py — no provenance asset),
# so the maintainer-only reg_meta.provenance.db* debug sibling should never enter
# the image. This RUN is defense in depth: it FAILS THE BUILD if any
# provenance.db* file is present anywhere in the image filesystem (.dockerignore
# also excludes local copies from the build context). Stable non-zero exit on hit.
RUN if find / -name 'reg_meta.provenance.db*' -print -quit 2>/dev/null | grep -q .; then \
        echo 'FATAL: reg_meta.provenance.db* present in image (provenance confinement #3 breached)' >&2; \
        exit 1; \
    fi

# ---------------------------------------------------------------------------
# Stage 3 — runtime: lean image serving the API with uvicorn.
# ---------------------------------------------------------------------------
# python:slim (no uv) — runtime needs only the interpreter + the baked .venv.
# Pinned to the same 3.13 minor the backend stage built against so the venv's
# absolute interpreter path resolves.
FROM python:3.13.7-slim-bookworm AS runtime

# Non-root runtime user (the API needs no write access to anything it ships).
RUN groupadd --system app && useradd --system --gid app --home /app app

WORKDIR /app

# The venv + the editable member src trees its .pth files point at (uv installs
# workspace members editable; the .pth entries reference /app/<member>/src). Only
# the members reg-webapp actually imports are copied — reg_meta, reg_schema,
# reg_monabundle, and reg_webapp itself; reg_meta_build / mock_data_wizard .pth
# entries dangle harmlessly (their dirs are never on an import path). Plus the
# baked DB pair, the SPA bundle, and the steward configs. UV_LINK_MODE=copy made
# the venv relocatable; both stages are python3.13 slim-bookworm so the venv's
# interpreter path matches.
COPY --from=backend /app/.venv /app/.venv
COPY --from=backend /app/reg_meta/src /app/reg_meta/src
COPY --from=backend /app/reg_schema/src /app/reg_schema/src
COPY --from=backend /app/reg_monabundle/src /app/reg_monabundle/src
COPY --from=backend /app/reg_webapp/backend/src /app/reg_webapp/backend/src
COPY --from=backend /opt/reg_meta /opt/reg_meta
COPY --from=frontend /build/frontend/dist /opt/reg_webapp/spa
COPY reg_webapp/stewards/ /opt/reg_webapp/stewards/
# chmod explicitly: don't rely on the source file's executable bit surviving the
# CI checkout / build-context transfer.
COPY reg_webapp/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENV PATH="/app/.venv/bin:$PATH" \
    # reg_meta read-only DB path (lifespan resolves REG_META_DB first).
    REG_META_DB=/opt/reg_meta \
    # The packaged image has no stewards/ sibling next to the backend module, so
    # point the loader at the copied configs (DESIGN.md → Steward layering).
    REG_WEBAPP_STEWARDS_DIR=/opt/reg_webapp/stewards \
    # Which steward this process serves (default `global` = full universe).
    REG_WEBAPP_STEWARD=global \
    REG_WEBAPP_HOST=0.0.0.0 \
    REG_WEBAPP_PORT=8000

USER app
EXPOSE 8000

# The entrypoint runs the per-deploy smoke gate (golden /api/context + shallow
# /api/catalog walk) IN-PROCESS before exec'ing uvicorn — a smoke failure exits
# non-zero and the container never serves traffic (REFACTOR_SPEC.md → Remaining
# test coverage → Per-deploy smoke tests).
ENTRYPOINT ["docker-entrypoint.sh"]
