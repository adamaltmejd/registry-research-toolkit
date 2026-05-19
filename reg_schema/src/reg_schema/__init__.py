"""reg_schema: project_data.json schema + structural validator.

See ``DESIGN.md`` for scope and dependency direction. Per
``REFACTOR_SPEC.md`` §6 the package owns the ``project_data.json`` v1
shape; this scaffold lands the cross-runtime ``ValidationIssue`` /
``ValidationResult`` contract first so later layers (structural rules,
top-level / Source / Column / Panel dataclasses) and downstream
consumers (mdw, reg_webapp, the amalgamated MONA bundle, the SPA's
TypeScript codegen) can pin against a stable shape.
"""

from .validation import IssueLevel, ValidationIssue, ValidationResult

__all__ = ["IssueLevel", "ValidationIssue", "ValidationResult"]
