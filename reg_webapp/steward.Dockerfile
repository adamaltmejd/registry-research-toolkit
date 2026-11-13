# syntax=docker/dockerfile:1.29.0@sha256:572cebbc2b15e6f84c077742952674889cb63cd5e9b80abdffcbe587b6f19183

# Steward deployment image: the released `reg-meta` binary, the release's base catalog
# and the steward's holdings, behind the same entrypoint smoke gate as the toolkit's
# own image (reg_webapp/Dockerfile, whose runtime stage this mirrors). Built by
# `steward-deploy.yml`, which downloads and verifies every input; the image compiles
# nothing and fetches nothing, so it needs no token (SWECOV move T5, R7).
#
# Build context: a directory holding exactly
#
#   reg-meta                  the reg_meta release's reg-meta-x86_64-unknown-linux-gnu
#   docker-entrypoint.sh      reg_webapp/docker-entrypoint.sh at the same tag
#   db/reg_meta.db            the release's base catalog, unpacked
#   db/reg_meta_<id>.db       the steward's holdings, unpacked
#
#   docker build -f reg_webapp/steward.Dockerfile --build-arg REG_META_CATALOG=<id> <context>
#
# glibc premise: the binary is built on the `ubuntu-latest` runner (glibc 2.39 for
# Ubuntu 24.04) and runs on Debian trixie (2.41). A runner image newer than this base's
# glibc breaks the binary at boot, which the entrypoint gate refuses before traffic.
#
# The base image is pinned as an exact tag plus its manifest digest, as in
# reg_webapp/Dockerfile; bump both files together.
FROM debian:trixie-20261104-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2

# curl for the entrypoint's smoke gate.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system app && useradd --system --gid app --home /app app

COPY --chmod=755 reg-meta /usr/local/bin/reg-meta
COPY --chmod=755 docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
COPY db/ /opt/reg_meta/

# The pair admission digest of the base catalog (reg_webapp/Dockerfile, stage 1) and the
# cache validators' content digest (its runtime stage), with the same recipes: hashing
# at boot on Fly's throttled rootfs would outlast the entrypoint's readiness deadline.
RUN mkdir -p /opt/reg_webapp \
    && sha256sum /opt/reg_meta/reg_meta.db | cut -d' ' -f1 > /opt/reg_webapp/catalog.sha256 \
    && { sha256sum /usr/local/bin/reg-meta; \
        printf '%s\n' /opt/reg_meta/*.db | LC_ALL=C sort | xargs sha256sum; } \
    | sha256sum | cut -d' ' -f1 > /opt/reg_webapp/content.sha256

ARG REG_META_CATALOG
RUN : "${REG_META_CATALOG:?build arg REG_META_CATALOG (the steward id) is required}" \
    && test -f "/opt/reg_meta/reg_meta_${REG_META_CATALOG}.db"
ENV REG_META_CATALOG=$REG_META_CATALOG

USER app
EXPOSE 8000
ENTRYPOINT ["docker-entrypoint.sh"]
