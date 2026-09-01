# syntax=docker/dockerfile:1.26@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32

# reg_webapp deployment image (REFACTOR_SPEC.md §6.5 — Deployment).
#
# Build context is the REPO ROOT (not reg_webapp/), because reg-webapp is a uv
# workspace member whose deps (reg-meta / reg-schema) resolve via
# [tool.uv.sources] against the root uv.lock — the lockfile and the sibling
# member source trees must all be in context. Build from the repo root with:
#
#   docker build -f reg_webapp/Dockerfile -t reg-webapp .
#
# Four baked-in stages: (1) bun builds the SPA, (2) a DB-bake stage fetches the
# reg_meta release DB pair and, for steward deployments, swaps in the private
# flavored main DB — isolated so app-code edits don't re-download it (#290),
# (3) uv installs the Python backend + workspace deps, (4) a lean runtime that
# serves the JSON API with uvicorn behind an entrypoint smoke gate.
#
# Every external image — the syntax frontend, each FROM base, and the uv tool
# image — is pinned as a readable exact tag PLUS its immutable multi-arch
# manifest digest, so a rebuild resolves byte-identical content even after the
# tag is republished. Bump both halves together, and resolve the digest from
# the exact tag you name; a tag alone is mutable and would make image content
# non-reproducible.

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
FROM oven/bun:1.4.0-alpine@sha256:07235578f79ef8c6f97d94aee7938e76f5cdba5f21ae5dbfdd3d3d38058437eb AS frontend

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
# Stage 2 — regmeta-db: fetch + bake the reg_meta DB pair.
# ---------------------------------------------------------------------------
# Isolated from the app-code stages (#290): its cache key covers only the
# workspace skeleton (root pyproject + lockfile + member pyprojects/READMEs),
# the reg_meta source tree, REG_META_TAG, and REG_META_FLAVOR — so
# backend/frontend edits reuse the cached DB layer (~70 MB download, several
# hundred MB unpacked) instead of re-fetching it on every build. `reg-meta
# update` (not raw curl) fetches the public release pair: it owns release
# resolution, asset naming, and zstd unpack, and tag slashes (`reg_meta/vX.Y.Z`)
# make hand-built download URLs fragile. A steward flavor then replaces only
# `reg_meta.db` from a private zstd URL; `reg_meta_docs.db` stays the public
# release asset for the same tag.
FROM python:3.14.7-slim-bookworm@sha256:9ab8d9c8514b44f90cf0029dd42fdd7e9e211e639c8b995304cc04568dee900f AS regmeta-db
COPY --from=ghcr.io/astral-sh/uv:0.12.6@sha256:88bc6eb1ccd4b82efd0e1b530caffabddf50dc2bf612e66c14ea25b8ee8a4d3d /uv /usr/local/bin/uv

ENV UV_LINK_MODE=copy \
    UV_NO_INSTALLER_METADATA=1

WORKDIR /app

# Workspace skeleton only: a frozen sync validates every [tool.uv.workspace]
# member's pyproject (hatchling also reads declared READMEs at editable build),
# but only reg_meta needs its real source here. Copying the other members' full
# trees would couple this stage's cache to app-code churn — the exact coupling
# this stage exists to break.
COPY pyproject.toml uv.lock ./
COPY reg_meta/ ./reg_meta/
COPY reg_meta_build/pyproject.toml reg_meta_build/README.md ./reg_meta_build/
COPY reg_schema/pyproject.toml reg_schema/README.md ./reg_schema/
COPY reg_webapp/backend/pyproject.toml reg_webapp/backend/README.md ./reg_webapp/backend/

# Install ONLY reg-meta + its runtime dep closure — just enough to run the
# `reg-meta update` fetch below.
RUN uv sync --frozen --no-dev --package reg-meta

# Bake the matching reg_meta release DB pair (reg_meta.db + reg_meta_docs.db)
# into an image layer. `reg-meta update` resolves a `reg_meta/v*` release
# carrying reg_meta.db.zst / reg_meta_docs.db.zst and installs into --db <dir>.
# The DB schema matches the installed reg_meta code (reg_meta/v0.9.0 → SCHEMA
# 5.2.0); rebuilding after a reg_meta bump re-bakes the matching pair.
#
# Invoke the venv's reg-meta binary by ABSOLUTE PATH (NOT `uv run`, which would
# re-sync: re-resolving the lockfile drops the global exclude-newer 7-day
# release-age guard and reinstalls the dev group + all workspace members).
#
# REG_META_TAG is a cache-bust knob: a DATA-only reg_meta release reuses the same
# code/lockfile, so the bake layer's cache key would otherwise never change and a
# cached rebuild (GHA cache-from / local cache) would silently keep a stale DB.
# Default `latest` resolves whatever release is newest at build time; CI always
# passes the resolved concrete tag (`--build-arg REG_META_TAG=reg_meta/vX.Y.Z`).
# A concrete tag fetches BOTH assets from that single release — valid because
# every published reg_meta release is self-contained (release skill step 8
# copies assets forward on code-only releases; an asset-less release broke
# every main-push build in #343 and fails this RUN with exit 10).
# REG_META_FLAVOR selects the main DB artifact:
# - global: keep the public release DB fetched above;
# - swecov: replace `reg_meta.db` with the private SWECOV flavored DB described
#   by the BuildKit secret `swecov_reg_meta_db_zst_manifest`. The secret value is
#   a JSON object with `tag`, `url`, and `sha256` for the zstd-compressed
#   reg_meta.db generated from the same release tag. The ignored SWECOV inventory
#   never enters the Docker context or image. Because BuildKit secrets are not
#   cache-key material, SWECOV_REG_META_DB_REV must change when that manifest
#   changes.
#
# ARGs declared after the sync layer so a tag/flavor change re-runs only the
# bake.
ARG REG_META_TAG=latest
ARG REG_META_FLAVOR=global
ARG SWECOV_REG_META_DB_REV=unset
RUN /app/.venv/bin/reg-meta update --db /opt/reg_meta --tag "$REG_META_TAG" --yes \
    && test -f /opt/reg_meta/reg_meta.db \
    && test -f /opt/reg_meta/reg_meta_docs.db
RUN --mount=type=secret,id=swecov_reg_meta_db_zst_manifest,required=false <<'EOF'
set -eu
case "$REG_META_FLAVOR" in
    global)
        ;;
    swecov)
        if [ -z "$SWECOV_REG_META_DB_REV" ] || [ "$SWECOV_REG_META_DB_REV" = "unset" ]; then
            echo "FATAL: REG_META_FLAVOR=swecov requires non-default SWECOV_REG_META_DB_REV to bust Docker's secret-insensitive cache" >&2
            exit 1
        fi
        secret=/run/secrets/swecov_reg_meta_db_zst_manifest
        if [ ! -s "$secret" ]; then
            echo "FATAL: REG_META_FLAVOR=swecov requires BuildKit secret swecov_reg_meta_db_zst_manifest" >&2
            exit 1
        fi
        /app/.venv/bin/python - "$secret" "$REG_META_TAG" <<'PY'
from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import urllib.request
from pathlib import Path

import zstandard

from reg_meta.db import open_db

secret_path = Path(sys.argv[1])
tag = sys.argv[2]
try:
    manifest = json.loads(secret_path.read_text(encoding="utf-8"))
except json.JSONDecodeError as exc:
    raise SystemExit(
        "FATAL: swecov_reg_meta_db_zst_manifest must be JSON with "
        "`tag`, `url`, and `sha256`"
    ) from exc
if not isinstance(manifest, dict):
    raise SystemExit("FATAL: swecov_reg_meta_db_zst_manifest must be a JSON object")
url = manifest.get("url")
expected_tag = manifest.get("tag")
expected_sha256 = manifest.get("sha256")
if not all(isinstance(v, str) and v for v in (url, expected_tag, expected_sha256)):
    raise SystemExit(
        "FATAL: swecov_reg_meta_db_zst_manifest requires non-empty string "
        "`tag`, `url`, and `sha256`"
    )
if expected_tag != tag:
    raise SystemExit(
        "FATAL: SWECOV flavored DB manifest tag "
        f"{expected_tag!r} does not match REG_META_TAG {tag!r}"
    )

db_dir = Path("/opt/reg_meta")
dest = db_dir / "reg_meta.db"
request = urllib.request.Request(url, headers={"User-Agent": "reg-webapp-build"})

digest = hashlib.sha256()
with urllib.request.urlopen(request, timeout=30) as response:
    fd, compressed_name = tempfile.mkstemp(
        prefix="swecov-reg_meta.", suffix=".db.zst", dir="/tmp"
    )
    with os.fdopen(fd, "wb") as compressed:
        while chunk := response.read(1024 * 1024):
            digest.update(chunk)
            compressed.write(chunk)
actual_sha256 = digest.hexdigest()
if actual_sha256.lower() != expected_sha256.lower():
    raise SystemExit(
        "FATAL: SWECOV flavored DB sha256 mismatch: "
        f"expected {expected_sha256.lower()}, got {actual_sha256}"
    )

compressed_path = Path(compressed_name)
tmp_db = dest.with_suffix(".db.swecov.tmp")
try:
    with compressed_path.open("rb") as src, tmp_db.open("wb") as out:
        zstandard.ZstdDecompressor().copy_stream(src, out)
    conn = open_db(tmp_db)
    try:
        expected_overlay_providers = {
            "arbetsformedlingen",
            "region-stockholm",
            "swecov",
            "swedbank",
        }
        placeholders = ",".join("?" for _ in expected_overlay_providers)
        present = {
            row[0]
            for row in conn.execute(
                f"SELECT slug FROM provider WHERE slug IN ({placeholders})",
                sorted(expected_overlay_providers),
            )
        }
    finally:
        conn.close()
    if missing := sorted(expected_overlay_providers - present):
        raise SystemExit(
            "FATAL: SWECOV flavored DB is missing expected overlay provider(s): "
            + ", ".join(missing)
        )
    tmp_db.replace(dest)
    (db_dir / ".db_source").write_text(
        json.dumps({"tag": tag, "flavor": "swecov"}) + "\n",
        encoding="utf-8",
    )
finally:
    compressed_path.unlink(missing_ok=True)
    tmp_db.unlink(missing_ok=True)
PY
        ;;
    *)
        echo "FATAL: unknown REG_META_FLAVOR '$REG_META_FLAVOR' (expected global or swecov)" >&2
        exit 1
        ;;
esac
EOF

# Read-only confinement of the baked WAL-mode DBs relies on `open_db`'s
# `immutable=1` (#283) — no WAL checkpoint / sidecar cleanup needed here.

# ---------------------------------------------------------------------------
# Stage 3 — backend build: install the Python backend venv.
# ---------------------------------------------------------------------------
# Pin BOTH the interpreter and uv deterministically: base on python:3.14-slim
# (same image the runtime stage uses, so the baked venv's interpreter path
# matches) and copy a version-pinned uv binary off its official image. uv's own
# combined `uv:<ver>-python<x>-bookworm-slim` tags don't pin the uv version and
# python jointly the way we need, so we compose the pin ourselves.
FROM python:3.14.7-slim-bookworm@sha256:9ab8d9c8514b44f90cf0029dd42fdd7e9e211e639c8b995304cc04568dee900f AS backend
COPY --from=ghcr.io/astral-sh/uv:0.12.6@sha256:88bc6eb1ccd4b82efd0e1b530caffabddf50dc2bf612e66c14ea25b8ee8a4d3d /uv /usr/local/bin/uv

# Deterministic install: bytecode-compile ahead of time, copy (not symlink) into
# .venv so the venv is self-contained when copied to the runtime stage.
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_INSTALLER_METADATA=1

WORKDIR /app

# Copy the workspace skeleton needed for `uv sync --frozen` to resolve. The root
# pyproject + uv.lock define the workspace; every member listed in
# [tool.uv.workspace].members must have its pyproject present for the locked
# resolution to validate. Full source trees only for the members this stage
# actually installs (reg-webapp's dep closure); reg_meta_build stays
# pyproject-only so its churn doesn't bust the sync layer (#290).
COPY pyproject.toml uv.lock ./
COPY reg_meta/ ./reg_meta/
COPY reg_meta_build/pyproject.toml reg_meta_build/README.md ./reg_meta_build/
COPY reg_schema/ ./reg_schema/
COPY reg_webapp/backend/ ./reg_webapp/backend/

# Install ONLY the reg-webapp package + its runtime dep closure (reg-meta /
# reg-schema), no dev deps. --frozen asserts the lockfile is up to date (CI
# parity). --package scopes the sync so reg-meta-build (another workspace member,
# not a reg-webapp dep) is NOT installed. This pulls the `reg-meta` console script
# into /app/.venv/bin.
#
# uv installs the workspace members as EDITABLE (.pth links into /app/<member>/src),
# so the runtime stage copies those member src trees alongside the venv (below) —
# the venv is not self-contained on its own.
RUN uv sync --frozen --no-dev --package reg-webapp

# ---------------------------------------------------------------------------
# Stage 4 — runtime: lean image serving the API with uvicorn.
# ---------------------------------------------------------------------------
# python:slim (no uv) — runtime needs only the interpreter + the baked .venv.
# Pinned to the same 3.14 minor the backend stage built against so the venv's
# absolute interpreter path resolves.
FROM python:3.14.7-slim-bookworm@sha256:9ab8d9c8514b44f90cf0029dd42fdd7e9e211e639c8b995304cc04568dee900f AS runtime

# Non-root runtime user (the API needs no write access to anything it ships).
RUN groupadd --system app && useradd --system --gid app --home /app app

WORKDIR /app

ARG REG_WEBAPP_STEWARD=global

# The venv + the editable member src trees its .pth files point at (uv installs
# workspace members editable; the .pth entries reference /app/<member>/src). The
# frozen --package sync installed exactly reg-webapp's dep closure — reg_meta,
# reg_schema, and reg_webapp itself — so all three src trees are copied and no
# .pth entry dangles. Plus the baked DB pair, the SPA bundle, and the steward
# configs. UV_LINK_MODE=copy made the venv relocatable; both stages are
# python3.14 slim-bookworm so the venv's interpreter path matches.
COPY --from=backend /app/.venv /app/.venv
COPY --from=backend /app/reg_meta/src /app/reg_meta/src
COPY --from=backend /app/reg_schema/src /app/reg_schema/src
COPY --from=backend /app/reg_webapp/backend/src /app/reg_webapp/backend/src
COPY --from=regmeta-db /opt/reg_meta /opt/reg_meta
COPY --from=frontend /build/frontend/dist /opt/reg_webapp/spa
COPY reg_webapp/stewards/ /opt/reg_webapp/stewards/
# chmod explicitly: don't rely on the source file's executable bit surviving the
# CI checkout / build-context transfer.
COPY reg_webapp/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# #224 provenance-confinement assertion #3 (deployment-side). Assertion #2 (no
# FastAPI route references the provenance path — backend/tests/test_write_boundary.py)
# already ships; this is the third. (Assertion #1 lived in the now-archived MONA
# bundle build.) `reg-meta update` only fetches reg_meta.db.zst /
# reg_meta_docs.db.zst (verified in reg_meta/download.py — no provenance asset),
# so the maintainer-only reg_meta.provenance.db* debug sibling should never enter
# the image. This RUN is defense in depth: it FAILS THE BUILD if any
# provenance.db* file is present anywhere in the image filesystem (.dockerignore
# also excludes local copies from the build context). Stable non-zero exit on
# hit. Runs in THIS stage (not the bake stage) so it scans the assembled
# shipping filesystem — everything the stage COPYs in — not just the fetch
# environment.
RUN if find / -name 'reg_meta.provenance.db*' -print -quit 2>/dev/null | grep -q .; then \
        echo 'FATAL: reg_meta.provenance.db* present in image (provenance confinement #3 breached)' >&2; \
        exit 1; \
    fi

ENV PATH="/app/.venv/bin:$PATH" \
    # reg_meta read-only DB path (lifespan resolves REG_META_DB first).
    REG_META_DB=/opt/reg_meta \
    # The packaged image has no stewards/ sibling next to the backend module, so
    # point the loader at the copied configs (DESIGN.md → Steward layering).
    REG_WEBAPP_STEWARDS_DIR=/opt/reg_webapp/stewards \
    # Which steward this process serves (default `global` = full universe).
    REG_WEBAPP_STEWARD=$REG_WEBAPP_STEWARD \
    REG_WEBAPP_HOST=0.0.0.0 \
    REG_WEBAPP_PORT=8000

USER app
EXPOSE 8000

# The entrypoint runs the per-deploy smoke gate (golden /api/context + shallow
# /api/catalog walk) IN-PROCESS before exec'ing uvicorn — a smoke failure exits
# non-zero and the container never serves traffic (REFACTOR_SPEC.md → Remaining
# test coverage → Per-deploy smoke tests).
ENTRYPOINT ["docker-entrypoint.sh"]
