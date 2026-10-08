"""reg_webapp: FastAPI backend serving the reg_meta catalog to the SPA.

See ``../DESIGN.md`` for the boot seam, steward layering, and the
Pydantic boundary. The Rust server (``reg-meta serve``) answers ``/api/context``.
"""

__version__ = "0.1.0"
