# syntax=docker/dockerfile:1.27@sha256:bde3983e9c939224420ddaf6b784cc30e09b035a4dea01f581230c50809f372e

# reg_webapp deployment image: the Rust server (`reg-meta serve`) alone, with the
# baked reg_meta DB pair and the steward branding (RUST_RUNTIME_SPEC.md section 8,
# package 3a.9). It serves the API and `/mcp` only; the edge worker serves the SPA
# (reg_webapp/edge/, built by container-build.yml's edge jobs).
#
# Build context is the REPO ROOT (not reg_webapp/): the bake stage needs the uv
# workspace skeleton and the server stage the Cargo workspace. Build with:
#
#   docker build -f reg_webapp/Dockerfile -t reg-webapp .
#
# Three stages: (1) a DB-bake stage fetches the reg_meta release DB pair with the
# Python `reg-meta update` — isolated so server edits don't re-download it (#290),
# (2) a pinned Rust stage builds `reg-meta`, (3) a lean runtime that runs
# `reg-meta serve` behind an entrypoint smoke gate.
#
# Every external image — the syntax frontend, each FROM base, and the uv tool
# image — is pinned as a readable exact tag PLUS its immutable multi-arch
# manifest digest, so a rebuild resolves byte-identical content even after the
# tag is republished. Bump both halves together, and resolve the digest from
# the exact tag you name; a tag alone is mutable and would make image content
# non-reproducible.

# ---------------------------------------------------------------------------
# Stage 1 — regmeta-db: fetch + bake the reg_meta DB pair.
# ---------------------------------------------------------------------------
# Isolated from the server stage (#290): its cache key covers only the
# workspace skeleton (root pyproject + lockfile + member pyprojects/READMEs),
# the reg_meta source tree, REG_META_TAG, and REG_META_FLAVOR — so
# server edits reuse the cached DB layer (~70 MB download, several
# hundred MB unpacked) instead of re-fetching it on every build. `reg-meta
# update` (not raw curl) fetches the public release pair: it owns release
# resolution, asset naming, and zstd unpack, and tag slashes (`reg_meta/vX.Y.Z`)
# make hand-built download URLs fragile. A steward flavor then replaces only
# `reg_meta.db` from a private zstd URL; `reg_meta_docs.db` stays the public
# release asset for the same tag. This stage is the image's only Python; the
# fetch stays here until stage 4 (RUST_RUNTIME_SPEC.md section 10).
FROM python:3.14.7-slim-bookworm@sha256:9ab8d9c8514b44f90cf0029dd42fdd7e9e211e639c8b995304cc04568dee900f AS regmeta-db
COPY --from=ghcr.io/astral-sh/uv:0.12.11@sha256:79c6f4776b851471cc73b7d21d0cc834bb94383c292e83640d27eff512864df7 /uv /usr/local/bin/uv

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
COPY crates/reg-core-py/pyproject.toml ./crates/reg-core-py/
COPY reg_schema/pyproject.toml reg_schema/README.md ./reg_schema/
COPY reg_webapp/backend/pyproject.toml reg_webapp/backend/README.md ./reg_webapp/backend/

# Install ONLY reg-meta + its runtime dep closure — just enough to run the
# `reg-meta update` fetch below.
RUN uv sync --frozen --no-dev --package reg-meta

# Bake the matching reg_meta release artifact pair (reg_meta.db + reg_meta_docs.db)
# into an image layer. `reg-meta update --catalog NAME` bootstraps a named
# installation under $XDG_DATA_HOME/reg_meta: `global` fetches reg_meta.db.zst into
# the data root, a steward name fetches reg_meta_<NAME>.db.zst into its own
# subdirectory, and both take the shared reg_meta_docs.db.zst beside it. The
# selected directory is then moved to /opt/reg_meta, which the runtime stage
# serves with `reg-meta serve --db`. An explicit `--db` update is NOT usable here: it
# requires an existing admitted catalog before any writes (reg_meta/DESIGN.md →
# Artifact selection), and this layer starts empty.
#
# Invoke the venv's reg-meta binary by ABSOLUTE PATH (NOT `uv run`, which would
# re-sync: re-resolving the lockfile reinstalls the dev group and all workspace
# members into this runtime image).
#
# REG_META_TAG is a cache-bust knob: a DATA-only reg_meta release reuses the same
# code/lockfile, so the bake layer's cache key would otherwise never change and a
# cached rebuild (GHA cache-from / local cache) would silently keep a stale DB.
# The tag is the ONLY cache key for the bake: re-uploading an asset under an
# existing tag needs a `no-cache` build (or a fresh tag) to be picked up.
# Default `latest` resolves whatever release is newest at build time; CI always
# passes the resolved concrete tag (`--build-arg REG_META_TAG=reg_meta/vX.Y.Z`).
# A concrete tag fetches BOTH assets from that single release — valid because
# every published reg_meta release is self-contained (release skill step 8
# copies assets forward on code-only releases; an asset-less release broke
# every main-push build in #343 and fails this RUN with exit 10 — including a
# steward build whose release lacks reg_meta_<NAME>.db.zst, #1091).
# REG_META_FLAVOR is the catalog name: `global` (public catalog) or a steward
# slug such as `swecov`. The runtime serves it as `--catalog`, so artifact
# admission at boot rejects a baked DB of another catalog.
#
# ARGs declared after the sync layer so a tag/flavor change re-runs only the
# bake.
ARG REG_META_TAG=latest
ARG REG_META_FLAVOR=global
RUN XDG_DATA_HOME=/opt/reg_meta_data /app/.venv/bin/reg-meta update \
        --catalog "$REG_META_FLAVOR" --tag "$REG_META_TAG" --yes \
    && if [ "$REG_META_FLAVOR" = global ]; then src=/opt/reg_meta_data/reg_meta; \
       else src="/opt/reg_meta_data/reg_meta/$REG_META_FLAVOR"; fi \
    && mv "$src" /opt/reg_meta \
    && test -f /opt/reg_meta/reg_meta.db \
    && test -f /opt/reg_meta/reg_meta_docs.db

# Read-only confinement of the baked WAL-mode DBs relies on `open_db`'s
# `immutable=1` (#283) — no WAL checkpoint / sidecar cleanup needed here.

# ---------------------------------------------------------------------------
# Stage 2 — server: build the `reg-meta` binary.
# ---------------------------------------------------------------------------
# Same Debian release as the runtime base, so the binary's glibc matches. The
# workspace manifest names every crate under crates/ as a member, so all of them
# are copied; only `reg-meta` and its dependencies are built (`reg-core-py`'s PyO3
# extension is not). `--locked` asserts Cargo.lock is current (CI parity).
FROM rust:1.99.0-slim-trixie@sha256:24e632c09342c20abf8312cf4f61430a911c01ed3a5e4c02b87292b1c39c5273 AS server
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
RUN cargo build --release --locked -p reg-meta

# ---------------------------------------------------------------------------
# Stage 3 — runtime: `reg-meta serve` behind the entrypoint smoke gate.
# ---------------------------------------------------------------------------
# Debian slim plus curl, which the entrypoint's smoke gate probes with.
FROM debian:trixie-20261005-slim@sha256:a29215f6a35e51e22adffa17f89e9d2ef06214e64a2bad10d765c46aea49f11f AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root runtime user (the server needs no write access to anything it ships).
RUN groupadd --system app && useradd --system --gid app --home /app app

COPY --from=server /build/target/release/reg-meta /usr/local/bin/reg-meta
COPY --from=regmeta-db /opt/reg_meta /opt/reg_meta
COPY reg_webapp/stewards/ /opt/reg_webapp/stewards/
# chmod explicitly: don't rely on the source file's executable bit surviving the
# CI checkout / build-context transfer.
COPY reg_webapp/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# #224 provenance-confinement assertion (deployment-side). The earlier route-source
# check was vacuous and is retired, and assertion #1 lived in the now-archived MONA
# bundle build, so this is the one that remains. `reg-meta update` only fetches reg_meta.db.zst /
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

# The catalog the image serves: the baked flavor (`global` or a steward slug),
# admitted as `--catalog` and branded from /opt/reg_webapp/stewards/<catalog>/.
ARG REG_META_FLAVOR=global
ENV REG_META_CATALOG=$REG_META_FLAVOR

USER app
EXPOSE 8000

# The entrypoint probes `context`, `search` and `/mcp` before handing the
# foreground to the server; a failed probe exits non-zero and the container never
# serves traffic (reg_webapp/DESIGN.md → Deployment).
ENTRYPOINT ["docker-entrypoint.sh"]
