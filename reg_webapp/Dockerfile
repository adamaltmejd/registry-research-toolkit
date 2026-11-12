# syntax=docker/dockerfile:1.29.0@sha256:572cebbc2b15e6f84c077742952674889cb63cd5e9b80abdffcbe587b6f19183

# reg_webapp deployment image for the global catalog: the Rust server (`reg-meta serve`)
# alone, with the baked catalog (crates/DESIGN.md, "Distribution"). It serves the API
# and `/mcp`. A steward's image is steward.Dockerfile.
#
# Build context is the REPO ROOT (not reg_webapp/): the server stage needs the
# Cargo workspace. The DB bake needs the release tag, the SHA-256 digest GitHub
# records for the asset (container-build.yml resolves both) and a GitHub token as the
# `gh_token` build secret. Build with:
#
#   GH_TOKEN=$(gh auth token) docker build -f reg_webapp/Dockerfile -t reg-webapp \
#     --secret id=gh_token,env=GH_TOKEN \
#     --build-arg REG_META_TAG=reg_meta/vX.Y.Z --build-arg REG_META_DB_SHA256=<hex> .
#
# (`gh release view reg_meta/vX.Y.Z --json assets` lists each asset's digest.)
#
# Three stages: (1) a DB-bake stage fetches the reg_meta release DB with curl,
# verifies it and unpacks it with zstd — isolated so server edits don't re-download it
# (#290), (2) a pinned Rust stage builds `reg-meta`, (3) a lean runtime that runs
# `reg-meta serve` behind an entrypoint smoke gate.
#
# Every external image — the syntax frontend and each FROM base — is pinned as
# a readable exact tag PLUS its immutable multi-arch
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
# The stage downloads `reg_meta.db.zst` from the release REG_META_TAG names, verifies
# it against GitHub's SHA-256 digest and unpacks it into /opt/reg_meta, which the
# runtime serves with `reg-meta serve --db`. A missing asset is a broken release: curl
# fails the build (#343). Schema admission is the server's, at boot (the entrypoint
# smoke gate); container-build.yml's schema guard keeps a mismatched schema from being
# built at all.
#
# The tag and digest have no defaults: nothing resolves `latest`, and an unverified
# download never enters the image. The digest is bare lowercase hex
# (GitHub's `sha256:` prefix stripped). The tag's `/` is URL-encoded in the URL.
#
# The fetch goes through the REST API with the `gh_token` secret, not the anonymous
# releases/download URL: anonymous downloads 504ed for 30 min in GitHub's 2026-11-09
# incident and blocked a deploy. The secret is required, so a build without it fails
# rather than falling back to anonymous. A secret mount is in no layer and not in the
# cache key. curl drops the Authorization header on the API's redirect to the object
# store, which is what that host expects (no --location-trusted).
FROM debian:trixie-20261020-slim@sha256:62bd423aeee912639ca8ee681c2b762cb3e25137e0a92b20be455898a9f034ad AS regmeta-db

# ca-certificates is only a Recommends of curl, so it is named for the HTTPS fetch.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl jq zstd \
    && rm -rf /var/lib/apt/lists/*

ARG REG_META_TAG
ARG REG_META_DB_SHA256
WORKDIR /opt/reg_meta
RUN --mount=type=secret,id=gh_token,required=true,env=GH_TOKEN \
    : "${REG_META_TAG:?build arg REG_META_TAG (reg_meta/vX.Y.Z) is required}" \
    && : "${REG_META_DB_SHA256:?build arg REG_META_DB_SHA256 is required (the release has no such catalog asset?)}" \
    && api="https://api.github.com/repos/adamaltmejd/registry-research-toolkit/releases/tags/$(printf '%s' "$REG_META_TAG" | sed 's|/|%2F|g')" \
    && auth="Authorization: Bearer $GH_TOKEN" \
    && asset=$(curl -fsSL --retry 3 -H "$auth" "$api" \
        | jq -er '[.assets[] | select(.name == "reg_meta.db.zst") | .url] | if length == 1 then .[0] else error("no single reg_meta.db.zst") end') \
    && curl -fsSL --retry 3 -H "$auth" -H "Accept: application/octet-stream" -o db.zst "$asset" \
    && printf '%s  db.zst\n' "$REG_META_DB_SHA256" | sha256sum --strict -c - \
    && zstd -q -d --rm db.zst -o reg_meta.db \
    && mkdir -p /opt/reg_webapp \
    && sha256sum reg_meta.db | cut -d' ' -f1 > /opt/reg_webapp/catalog.sha256

# The last step hashes the unpacked catalog, which came from the verified asset. The
# entrypoint, shared with steward.Dockerfile, reads it and passes it to the server as
# REG_META_CATALOG_SHA256, which only a steward's pair admission uses. It lives outside
# /opt/reg_meta, the served --db directory.

# Read-only confinement of the baked WAL-mode DB relies on the server opening it
# `immutable=1` (#283) — no WAL checkpoint / sidecar cleanup needed here.

# ---------------------------------------------------------------------------
# Stage 2 — server: build the `reg-meta` binary.
# ---------------------------------------------------------------------------
# Same Debian release as the runtime base, so the binary's glibc matches. The
# workspace manifest names every crate under crates/ as a member, so all of them
# are copied; only `reg-meta` and its dependencies are built. `--locked` asserts
# Cargo.lock is current (CI parity).
FROM rust:1.99.1-slim-trixie@sha256:f6c4a35d21dbb3c89d7a8d056c193e94fc09037f2b58dd858b09c39a47dd02d7 AS server
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
COPY --from=regmeta-db /opt/reg_webapp/catalog.sha256 /opt/reg_webapp/catalog.sha256
# chmod explicitly: don't rely on the source file's executable bit surviving the
# CI checkout / build-context transfer.
COPY reg_webapp/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# The cache validators' content digest (crates/reg-meta/src/validator.rs): the served
# binary and databases, hashed here because hashing the DBs at boot on Fly's throttled
# rootfs (~8 MiB/s cold) would outlast the entrypoint's readiness deadline. The
# entrypoint passes it to the server as REG_META_CONTENT_DIGEST.
RUN mkdir -p /opt/reg_webapp \
    && { sha256sum /usr/local/bin/reg-meta; \
        printf '%s\n' /opt/reg_meta/*.db | LC_ALL=C sort | xargs sha256sum; } \
    | sha256sum | cut -d' ' -f1 > /opt/reg_webapp/content.sha256

# The entrypoint's catalog selection: the global catalog, no holdings partner.
ENV REG_META_CATALOG=global

USER app
EXPOSE 8000

# The entrypoint probes `context`, `search`, docs search and `/mcp` before handing the
# foreground to the server; a failed probe exits non-zero and the container never
# serves traffic (reg_webapp/DESIGN.md → Deployment).
ENTRYPOINT ["docker-entrypoint.sh"]
