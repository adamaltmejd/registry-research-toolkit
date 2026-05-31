"""reg_schema: project_data.json schema + structural validator.

See ``DESIGN.md`` for scope and dependency direction; ``REFACTOR_SPEC.md``
§6 is the authoritative schema spec.
"""

from .project_data import (
    Binding,
    ColumnType,
    EntityKey,
    IdSubtype,
    LiteralPeriod,
    NumericSubtype,
    Panel,
    PanelMember,
    Period,
    PeriodRange,
    ProjectData,
    Source,
    Steward,
    TimeKey,
    TimePoint,
    TimeRange,
)
from .structural import validate_structural
from .validation import IssueLevel, ValidationIssue, ValidationResult

__all__ = [
    "Binding",
    "ColumnType",
    "EntityKey",
    "IdSubtype",
    "IssueLevel",
    "LiteralPeriod",
    "NumericSubtype",
    "Panel",
    "PanelMember",
    "Period",
    "PeriodRange",
    "ProjectData",
    "Source",
    "Steward",
    "TimeKey",
    "TimePoint",
    "TimeRange",
    "ValidationIssue",
    "ValidationResult",
    "validate_structural",
]
