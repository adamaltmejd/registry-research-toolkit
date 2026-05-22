"""Bundle-runtime modules amalgamated into the MONA single-file bundle.

This subpackage is the "heavy" half of ``reg_monabundle`` — the modules
that drive on-MONA extraction (file/SQL source iteration, type
classification, summary aggregation, JSON-stable output) and that
``reg_monabundle.build.build_bundle`` amalgamates into ``mdw_runner.py``.

The lightweight top-level surface (``build_bundle``, ``scan_file``,
``validate_block``, ``SUPPRESS_K``) must remain importable without
pulling any module from this package — the local CLI / webapp / bundle
builder must not transitively load ``duckdb`` / ``pyodbc``. See
``reg_monabundle/DESIGN.md`` for the lightweight-vs-runtime split.
"""
