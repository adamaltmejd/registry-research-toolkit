# Lane image. Boxes set their own HOME and PATH, so tools are linked onto the
# default PATH and uv reads its settings from /etc/uv/uv.toml, not ENV.
# Package downloads go through [isolation].egress (PyPI, crates.io) at run
# time; uv, the interpreter .python-version names and the Rust toolchain are
# baked in. Rust and a C linker are needed because `uv sync` builds the
# editable crates/reg-core-py member with maturin.
FROM ghcr.io/astral-sh/uv:0.13.5 AS uv

# The toolchain reg_webapp/Dockerfile pins; copied out of rustup because boxes
# set their own HOME and PATH.
FROM rust:1.99.1-slim-trixie@sha256:1a939748a7cbab172de91a014e3804a97af1394a8c844faa67fce9401c4e40fb AS rust
RUN cp -a "$(rustc --print sysroot)" /toolchain

FROM pinfold/profile-default:latest
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*
COPY --from=rust /toolchain /opt/rust
RUN ln -s /opt/rust/bin/* /usr/local/bin/ && cargo --version
COPY --from=uv /uv /uvx /usr/local/bin/
# uv takes its interpreter install dir only from the environment, so the
# interpreter is linked onto PATH and uv finds it there with downloads off.
RUN export UV_PYTHON_INSTALL_DIR=/opt/uv/python \
    && uv python install 3.15 \
    && ln -s "$(uv python find --managed-python 3.15)" /usr/local/bin/python3.15 \
    && chmod -R a+rX /opt/uv \
    && mkdir -p /etc/uv \
    && printf 'python-downloads = "never"\n' > /etc/uv/uv.toml \
    && env -i PATH=/usr/local/bin:/usr/bin:/bin HOME=/tmp uv python find 3.15

WORKDIR /workspace
