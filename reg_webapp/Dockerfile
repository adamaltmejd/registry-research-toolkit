# syntax=docker/dockerfile:1.28.0@sha256:bb22d9815c728170f72750f4e5b0d672e06176142e1d602c7e66c050100b7e5b

# reg_webapp deployment image: the Rust server (`reg-meta serve`) alone, with the
# baked reg_meta DB (crates/DESIGN.md, "Distribution"). It serves the API and `/mcp`
# only; the global edge worker routes hosted MCP (reg_webapp/edge/).
#
# Build context is the REPO ROOT (not reg_webapp/): the server stage needs the
# Cargo workspace. The DB bake needs the release tag and the SHA-256 digest GitHub
# records for the asset (container-build.yml resolves them). Build with:
#
#   docker build -f reg_webapp/Dockerfile -t reg-webapp \
#     --build-arg REG_META_TAG=reg_meta/vX.Y.Z --build-arg REG_META_DB_SHA256=<hex> .
#
# (`gh release view reg_meta/vX.Y.Z --json assets` lists the asset's digest.)
#
# Three stages: (1) a DB-bake stage fetches the reg_meta release DB with curl,
# verifies it and unpacks it with zstd — isolated so server edits don't re-download it
# (#290), (2) a pinned Rust stage builds `reg-meta`, (3) a lean runtime that runs
# `reg-meta serve` behind an entrypoint smoke gate.
#
# Every external image — the syntax frontend, each FROM base, and the uv tool
# image — is pinned as a readable exact tag PLUS its immutable multi-arch
# manifest digest, so a rebuild resolves byte-identical content even after the
# tag is republished. Bump both halves together, and resolve the digest from
# the exact tag you name; a tag alone is mutable and would make image content
# non-reproducible.

# ---------------------------------------------------------------------------
# Stage 1 — regmeta-db: fetch + bake the reg_meta DB.
# ---------------------------------------------------------------------------
# Isolated from the server stage (#290): it copies nothing from the build context,
# so its cache key is the base image plus the two ARGs below, and server edits
# reuse the cached DB layer instead of re-fetching it. The digest is part of
# that key, so an asset re-uploaded under an existing tag rebakes on its own.
#
# The stage downloads `reg_meta.db.zst` from the one release REG_META_TAG names,
# checks it against the SHA-256 digest GitHub records for it, and unpacks it into
# /opt/reg_meta, which the runtime serves with `reg-meta serve --db`. A missing
# asset is a broken release: curl fails the build (#343). Schema admission is the
# server's, at boot (the entrypoint smoke gate); container-build.yml's schema guard
# keeps a mismatched-schema asset from being built at all.
#
# No ARG has a usable default: nothing in this stage resolves `latest`, and an
# unverified download never enters the image. The digest is bare lowercase hex
# (GitHub's `sha256:` prefix stripped). The tag's `/` is URL-encoded in the URL.
FROM debian:trixie-20261020-slim@sha256:62bd423aeee912639ca8ee681c2b762cb3e25137e0a92b20be455898a9f034ad AS regmeta-db

# ca-certificates is only a Recommends of curl, so it is named for the HTTPS fetch.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl zstd \
    && rm -rf /var/lib/apt/lists/*

ARG REG_META_TAG
ARG REG_META_DB_SHA256
WORKDIR /opt/reg_meta
RUN : "${REG_META_TAG:?build arg REG_META_TAG (reg_meta/vX.Y.Z) is required}" \
    && : "${REG_META_DB_SHA256:?build arg REG_META_DB_SHA256 is required (the release has no such catalog asset?)}" \
    && base="https://github.com/adamaltmejd/registry-research-toolkit/releases/download/$(printf '%s' "$REG_META_TAG" | sed 's|/|%2F|g')" \
    && curl -fsSL --retry 3 -o db.zst "$base/reg_meta.db.zst" \
    && printf '%s  db.zst\n' "$REG_META_DB_SHA256" | sha256sum --strict -c - \
    && zstd -q -d --rm db.zst -o reg_meta.db

# Read-only confinement of the baked WAL-mode DB relies on the server opening it
# `immutable=1` (#283) — no WAL checkpoint / sidecar cleanup needed here.

# ---------------------------------------------------------------------------
# Stage 2 — server: build the `reg-meta` binary.
# ---------------------------------------------------------------------------
# Same Debian release as the runtime base, so the binary's glibc matches. The
# workspace manifest names every crate under crates/ as a member, so all of them
# are copied; only `reg-meta` and its dependencies are built (`reg-core-py`'s PyO3
# extension is not). `--locked` asserts Cargo.lock is current (CI parity).
FROM rust:1.99.1-slim-trixie@sha256:b8e062b1c3a80183df83d9ba187cadd2b5ab84bc78d453dbfeb2fccd5f982247 AS server
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
RUN cargo build --release --locked -p reg-meta

# ---------------------------------------------------------------------------
# Stage 3 — runtime: `reg-meta serve` behind the entrypoint smoke gate.
# ---------------------------------------------------------------------------
# Debian slim plus curl, which the entrypoint's smoke gate probes with.
FROM debian:trixie-20261020-slim@sha256:62bd423aeee912639ca8ee681c2b762cb3e25137e0a92b20be455898a9f034ad AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root runtime user (the server needs no write access to anything it ships).
RUN groupadd --system app && useradd --system --gid app --home /app app

COPY --from=server /build/target/release/reg-meta /usr/local/bin/reg-meta
COPY --from=regmeta-db /opt/reg_meta /opt/reg_meta
# chmod explicitly: don't rely on the source file's executable bit surviving the
# CI checkout / build-context transfer.
COPY reg_webapp/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# The cache validators' content digest (crates/reg-meta/src/validator.rs): the served
# binary and database, hashed here because hashing the DB at boot on Fly's throttled
# rootfs (~8 MiB/s cold) would outlast the entrypoint's readiness deadline. The
# entrypoint passes it to the server as REG_META_CONTENT_DIGEST.
RUN mkdir -p /opt/reg_webapp \
    && sha256sum /usr/local/bin/reg-meta /opt/reg_meta/reg_meta.db \
    | sha256sum | cut -d' ' -f1 > /opt/reg_webapp/content.sha256

# #224 provenance-confinement assertion (deployment-side). The earlier route-source
# check was vacuous and is retired, and assertion #1 lived in the now-archived MONA
# bundle build, so this is the one that remains. The bake stage fetches only
# reg_meta.db.zst (no provenance asset), so the maintainer-only reg_meta.provenance.db*
# debug sibling should never enter the image. This RUN is defense in depth: it FAILS THE BUILD if any
# provenance.db* file is present anywhere in the image filesystem (.dockerignore
# also excludes local copies from the build context). Stable non-zero exit on
# hit. Runs in THIS stage (not the bake stage) so it scans the assembled
# shipping filesystem — everything the stage COPYs in — not just the fetch
# environment.
RUN if find / -name 'reg_meta.provenance.db*' -print -quit 2>/dev/null | grep -q .; then \
        echo 'FATAL: reg_meta.provenance.db* present in image (provenance confinement #3 breached)' >&2; \
        exit 1; \
    fi

USER app
EXPOSE 8000

# The entrypoint probes `context`, `search`, docs search and `/mcp` before handing the
# foreground to the server; a failed probe exits non-zero and the container never
# serves traffic (reg_webapp/DESIGN.md → Deployment).
ENTRYPOINT ["docker-entrypoint.sh"]
