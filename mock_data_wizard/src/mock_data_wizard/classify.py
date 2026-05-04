"""Column-name patterns and date-format helpers shared across the package.

Pure functions, no IO. The data-driven ``classify_column`` path was
removed when extract switched to a config-driven workflow (every column
must carry a ``mdw_config.json`` override). What remains is the
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


@dataclass(frozen=True)
class CategoricalPattern:
    pattern: str
    max_distinct: int  # advisory cap, used by future regmeta-aware paths
    exclude: str | None = None


ID_PATTERNS: tuple[IdPattern, ...] = (
    IdPattern("lopnr"),  # MONA record-linkage key
)

CATEGORICAL_PATTERNS: tuple[CategoricalPattern, ...] = (
    CategoricalPattern("kommun", max_distinct=500, exclude="kommunikation"),  # ~290
    CategoricalPattern("ssyk", max_distinct=1000),  # SSYK ~400 at 4-digit
    CategoricalPattern("sun2000", max_distinct=1000),  # SUN2000 ~600
    CategoricalPattern("sun2020", max_distinct=1000),  # SUN2020 ~600
    CategoricalPattern(r"sni(\d|_|$)", max_distinct=1500),  # SNI ~800
    CategoricalPattern("(fodelse|fodelses?)land", max_distinct=300),  # ~230
    CategoricalPattern("medb(orgarskap)?", max_distinct=300),  # citizenship ~230
    CategoricalPattern(r"(^|_)kon$", max_distinct=3),  # sex
    CategoricalPattern("civil", max_distinct=10),  # CivilStand
    CategoricalPattern(r"(^|_)lan$", max_distinct=30),  # län (region) — 21
    CategoricalPattern(r"land(skap)?$", max_distinct=300),  # FodelseLand
    CategoricalPattern(r"_kod$", max_distinct=10000),  # generic "...kod" code columns
)


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


def known_categorical_cap(col_name: str) -> int | None:
    """Max n_distinct cap for a name-based categorical match, or None."""
    name = col_name.lower()
    for p in CATEGORICAL_PATTERNS:
        if re.search(p.pattern, name) and not (
            p.exclude and re.search(p.exclude, name)
        ):
            return p.max_distinct
    return None


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
