"""reg_webapp: the workspace member beside the Svelte SPA.

The Rust server (``reg-meta serve``) serves every route. This member holds dev
tooling only (``scripts/``: the synthetic fixture DB and the search eval) and the
period-grammar parity test; see ``../../README.md``. It stays a workspace member
because the Dockerfile's ``regmeta-db`` stage copies its pyproject until stage 4.
"""

__version__ = "0.1.0"
