"""FastAPI routers for reg_webapp.

``catalog`` (A5.1b-ii), ``search``, ``docs`` and ``project``. The catalog router owns the
``{fqid:path}`` catch-all and its per-segment slug-grammar guard (see
DESIGN.md → FQID path guard (catalog_fqid.py)); A5.2's
suffixed routes (``/states`` etc.) declare ABOVE the catch-all in
``catalog.py``.
"""
