"""reg_schema: project_data.json schema + structural validator.

See ``DESIGN.md`` for scope and dependency direction; ``REFACTOR_SPEC.md``
§6 is the authoritative schema spec.
"""

from .validation import IssueLevel, ValidationIssue, ValidationResult

__all__ = ["IssueLevel", "ValidationIssue", "ValidationResult"]
