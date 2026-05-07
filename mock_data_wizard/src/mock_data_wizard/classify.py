"""Column-name patterns and date-format helpers shared across the package.

Pure functions, no IO. The data-driven ``classify_column`` path was
removed when extract switched to a config-driven workflow (every column
must carry a ``mdw_step2_config.json`` override). What remains is the
name-pattern surface used by ``configure.py`` to author that config,
plus the date-format helpers consumed by ``summarize.py`` when a date
override has no inline ``date_format`` hint.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Sequence

# Canonical inferred-type enum. One source of truth -- imported by
# config (validation), sql_emit (dispatch), and stats (consumer-side).
COLUMN_TYPES: tuple[str, ...] = (
    "id",
    "categorical",
    "numeric",
    "high_cardinality",
    "date",
)


# -- Name-based patterns ---------------------------------------------------
# First match wins. Patterns are regexes matched case-insensitively.


@dataclass(frozen=True)
class IdPattern:
    pattern: str
    exclude: str | None = None


ID_PATTERNS: tuple[IdPattern, ...] = (
    # Unanchored on purpose so "LopNr", "LopNr_PersNr", and "AarLopNr"
    # all match. "LopNrByte" (RTB pid-change flag) is a near-miss that
    # carries pid lineage in its name but isn't itself an identifier;
    # exclude it explicitly.
    IdPattern("lopnr", exclude=r"lop_?nr_?byte$"),
    # Anchored at segment start so "FelPersonNr" (a non-id flag column —
    # see scan.py) does NOT match while "PersonNr", "PersNr", and
    # "LopNr_PersNr" all do.
    IdPattern(r"(^|_)pers(on)?nr"),
)


# Register-scoped exact-name categoricals. Names regmeta is known to be
# missing under specific registers but where SCB convention pins the
# semantics unambiguously. Exact name match only (case-insensitive) and
# only when the configured register matches — outside that context the
# names are ambiguous enough that we'd rather the user see
# "high_cardinality" in the inspector and override manually than silently
# mistype.
RTB_NAMED_CATEGORICAL: frozenset[str] = frozenset(
    {
        # Record-quality flags shipped on most RTB extracts (binary 0/1)
        "ateranv",  # återanvändning flag
        "felpersonnr",  # incorrect-pid flag
        "lopnrbyte",  # pid-change flag
        # Birth-year and birth-year-month: low-cardinality grouping
        # variables in any register that ships them, but the name
        # convention is RTB-specific. Treated as categorical because
        # mdw's date pipeline currently assumes day-precision (see
        # github issue on year/year-month support).
        "fodelsear",
        "fodelsearman",
    }
)


def is_rtb_named_categorical(col_name: str, register: str | None) -> bool:
    """Whether ``col_name`` is in the RTB-scoped exact-name categorical set.

    Both inputs are matched case-insensitively. The register check is
    intentionally a substring match on ``"RTB"`` so it catches both the
    short alias (``"RTB"``) and the full Swedish register name
    (``"Registret över totalbefolkningen (RTB)"``).
    """
    if not register or "rtb" not in register.lower():
        return False
    return col_name.lower() in RTB_NAMED_CATEGORICAL


# -- Date detection --------------------------------------------------------

DATE_FORMATS: tuple[str, ...] = (
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%Y%m%d",
)
DATE_CLASSIFY_THRESHOLD = 0.8  # ratio of sample that must parse to be a date


# -- Helpers ---------------------------------------------------------------


def is_known_id(col_name: str) -> bool:
    """Whether the column name matches a hardcoded ID pattern."""
    name = col_name.lower()
    for p in ID_PATTERNS:
        if re.search(p.pattern, name) and not (
            p.exclude and re.search(p.exclude, name)
        ):
            return True
    return False


def _parses_as_date(s: str, fmt: str) -> bool:
    try:
        datetime.strptime(s, fmt)
        return True
    except (ValueError, TypeError):
        return False


def detect_date_format(values: Sequence[str]) -> str | None:
    """Return the first DATE_FORMATS entry that parses a high-enough fraction
    of ``values``, or None if no format does. Caller supplies non-null
    string values only.
    """
    if not values:
        return None
    sample = values[:200]
    threshold = len(sample) * DATE_CLASSIFY_THRESHOLD
    for fmt in DATE_FORMATS:
        ok = sum(1 for v in sample if _parses_as_date(v, fmt))
        if ok > threshold:
            return fmt
    return None


def _python_kind(values: Sequence[object]) -> str:
    """Coarse type label derived from the non-null sample.

    Returns one of: 'date', 'bool', 'numeric_int', 'numeric_float', 'string',
    'empty'. SQL drivers can return numbers as ``Decimal`` -- treat those as
    numeric; the int-vs-float split is decided by whether all values are
    integral.
    """
    non_null = [v for v in values if v is not None]
    if not non_null:
        return "empty"
    if all(isinstance(v, (date, datetime)) for v in non_null):
        return "date"
    # bool is a subclass of int in Python; check it first
    if all(isinstance(v, bool) for v in non_null):
        return "bool"
    # SQL drivers can return numerics as int, float, or decimal.Decimal.
    if all(
        isinstance(v, (int, float)) or hasattr(v, "to_eng_string") for v in non_null
    ) and not any(isinstance(v, str) for v in non_null):
        # All integral?
        try:
            if all(float(v) == int(float(v)) for v in non_null):
                return "numeric_int"
        except (ValueError, TypeError):
            pass
        return "numeric_float"
    if all(isinstance(v, str) for v in non_null):
        return "string"
    # Mixed: treat as string (the sample might have been coerced)
    return "string"
