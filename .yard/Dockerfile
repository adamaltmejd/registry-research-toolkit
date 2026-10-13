# Lane image. Boxes set their own HOME and PATH, so tools are linked onto the
# default PATH and uv reads its settings from /etc/uv/uv.toml, not ENV.
# Package downloads go through [isolation].egress (PyPI) at run time; only uv
# and the interpreter .python-version names are baked in.
FROM ghcr.io/astral-sh/uv:0.13.0 AS uv

FROM pinfold/profile-default:latest
COPY --from=uv /uv /uvx /usr/local/bin/
# uv takes its interpreter install dir only from the environment, so the
# interpreter is linked onto PATH and uv finds it there with downloads off.
RUN export UV_PYTHON_INSTALL_DIR=/opt/uv/python \
    && uv python install 3.14 \
    && ln -s "$(uv python find --managed-python 3.14)" /usr/local/bin/python3.14 \
    && chmod -R a+rX /opt/uv \
    && mkdir -p /etc/uv \
    && printf 'python-downloads = "never"\n' > /etc/uv/uv.toml \
    && env -i PATH=/usr/local/bin:/usr/bin:/bin HOME=/tmp uv python find 3.14

WORKDIR /workspace
