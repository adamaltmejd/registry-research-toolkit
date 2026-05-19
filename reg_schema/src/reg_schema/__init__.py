"""reg_schema: project_data.json schema + structural validator.

See ``DESIGN.md`` for scope and dependency direction; ``REFACTOR_SPEC.md``
§6 is the authoritative schema spec.
"""

from .project_data import (
    Column,
    ColumnType,
    EntityKey,
    IdSubtype,
    LiteralPeriod,
    NumericSubtype,
    Panel,
    PanelMember,
    ProjectData,
    Source,
    Steward,
    TimeKey,
    TimePoint,
)
from .validation import IssueLevel, ValidationIssue, ValidationResult

__all__ = [
    "Column",
    "ColumnType",
    "EntityKey",
    "IdSubtype",
    "IssueLevel",
    "LiteralPeriod",
    "NumericSubtype",
    "Panel",
    "PanelMember",
    "ProjectData",
    "Source",
    "Steward",
    "TimeKey",
    "TimePoint",
    "ValidationIssue",
    "ValidationResult",
]
