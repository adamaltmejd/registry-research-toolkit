# The container image Yard builds lane workers and gates from
# ([isolation].dockerfile in .yard/config.toml). Lanes read this file from the
# candidate's history, not from your working tree — commit changes to it.
#
# Contract: keep /bin/sh, /usr/bin/env, sleep, git, and /yard/state/tmp.
#
# Gates run network-disabled under `env -i` (no image ENV reaches them), so
# everything a gate needs is pre-warmed here at image build time (which does
# have network) into fixed /opt paths the gate commands in .yard/config.toml
# reference explicitly:
#   /opt/uv/cache   — uv wheel + uvx tool-env cache (deps, ty, panache,
#                     hatchling for editable workspace builds)
#   /opt/uv/python  — uv-managed CPython matching .python-version
#   /opt/frontend/node_modules — bun install of the frontend lockfile
# The warm layers are keyed on the manifest/lock files alone, so ordinary
# source changes reuse them; a lockfile change rebuilds only its own layer.
FROM debian:stable-slim@sha256:04634311a8d5fc442b6eb06d792293c4f3e2268652ca7634e00ce8ef5cc0a28a

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates git \
       ripgrep jq curl file diffutils patch tar xz-utils zstd unzip coreutils \
    && install -d -m 1777 /yard/state/tmp \
    && rm -rf /var/lib/apt/lists/*

# node: the frontend tools' launchers (biome, svelte-check, vitest, vite,
# openapi-typescript) are `#!/usr/bin/env node` scripts — bun alone does not
# satisfy them (CI is green only because GitHub runners ship system node), and
# Debian's node 20 trips an undici incompatibility in the vitest jsdom teardown.
# Pinned digest = node 24.20.0.
COPY --from=node:24-bookworm-slim@sha256:ba849c60be29959425b8734d57b8b4b7d56f98edd9504c9af091d5281095a71e /usr/local/bin/node /usr/local/bin/node

# Pinned toolchain binaries. Move these pins deliberately, with the repo's own
# uv/bun versions (uv: brew-installed on the host; bun: reg_webapp toolchain).
COPY --from=ghcr.io/astral-sh/uv:0.12.6 /uv /uvx /usr/local/bin/
COPY --from=oven/bun:1.4.0 /usr/local/bin/bun /usr/local/bin/bun
RUN ln -s /usr/local/bin/bun /usr/local/bin/bunx

# --- Python warm layer: keyed on workspace manifests + lockfile only. ---
# --no-install-workspace: members' own builds are done by each gate's offline
# two-step `uv sync` in the candidate tree (hatchling is a locked dev dep, so
# the first sync puts it in the venv and --no-build-isolation builds the
# members with it — uv's offline resolver refuses build-system.requires
# lookups, so isolated builds cannot work offline); here we only need every
# locked wheel in the cache and the managed interpreter installed. Gates run as the host uid, so the caches must be
# world-writable (uv takes lock files in its cache dir) — chmod in the same
# layer to avoid duplicating it.
COPY .python-version pyproject.toml uv.lock /opt/warm/
COPY reg_meta/pyproject.toml /opt/warm/reg_meta/
COPY reg_meta_build/pyproject.toml /opt/warm/reg_meta_build/
COPY reg_schema/pyproject.toml /opt/warm/reg_schema/
COPY reg_webapp/backend/pyproject.toml /opt/warm/reg_webapp/backend/
RUN cd /opt/warm \
    && export UV_CACHE_DIR=/opt/uv/cache UV_PYTHON_INSTALL_DIR=/opt/uv/python \
    && uv python install \
    && uv sync --frozen --no-install-workspace \
    && rm -rf /opt/warm/.venv \
    # Tool envs for the pinned checkers (keep versions == the CI/CLAUDE.md pins):
    && uvx --from ty==0.0.72 ty --version \
    && uvx --from panache-cli==3.4.0 panache --version \
    && chmod -R a+rwX /opt/uv

# --- Frontend warm layer: keyed on the frontend manifest + lockfile. ---
# Gates copy /opt/frontend/node_modules into the candidate's frontend dir
# rather than running a network install.
COPY reg_webapp/frontend/package.json reg_webapp/frontend/bun.lock /opt/frontend/
RUN cd /opt/frontend \
    && bun install --frozen-lockfile \
    && chmod -R a+rwX /opt/frontend

# Playwright Chromium for the frontend `browser` vitest project, at the exact
# playwright version the lockfile pins (resolved from the baked node_modules).
# --with-deps apt-installs Chromium's shared libraries here, with build-time
# network; the gate then runs it offline via PLAYWRIGHT_BROWSERS_PATH.
RUN cd /opt/frontend \
    && PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers bunx playwright install --with-deps chromium \
    && chmod -R a+rwX /opt/pw-browsers

WORKDIR /workspace
