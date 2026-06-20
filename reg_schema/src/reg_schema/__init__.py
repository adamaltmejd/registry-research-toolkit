"""reg_schema: project_data.json schema + structural validator.

See ``DESIGN.md`` for scope and dependency direction; the models in
``project_data.py`` are the authoritative schema (see DESIGN.md → Two
layers: models vs. validator).
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
    StudyWindow,
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
    "StudyWindow",
    "TimeKey",
    "TimePoint",
    "TimeRange",
    "ValidationIssue",
    "ValidationResult",
    "validate_structural",
]
