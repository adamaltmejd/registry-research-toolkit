"""Column classifier -- decides one of {id, categorical, numeric,
high_cardinality, date} for a column based on its name, true n_distinct,
true n_rows, and a sample of values.

Pure functions, no IO. Mirrors the R-side ``classify_column`` from the
legacy ``script_gen.py`` template, but takes the true ``n_distinct``
(from a SQL pre-classify pass) as an explicit input rather than
recomputing from a fully-materialised column. This is what makes the
DuckDB-on-MONA path work: the column never lives in memory, only the
sample does.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Sequence

# -- Name-based patterns ---------------------------------------------------
# First match wins. Patterns are regexes matched case-insensitively.


@dataclass(frozen=True)
class IdPattern:
    pattern: str
    exclude: str | None = None


@dataclass(frozen=True)
class CategoricalPattern:
    pattern: str
    max_distinct: int  # if true n_distinct exceeds this, ignore the match
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
)


# -- Data-driven thresholds ------------------------------------------------

FREQ_CAP = 50  # absolute cap on n_distinct for "categorical"
FREQ_RATIO = 0.01  # relative cap (fraction of n_rows)
NUMERIC_ID_RATIO = 0.95  # numeric ID if nd > ratio * n_rows AND ...
NUMERIC_ID_MIN = 100  # ... nd > this minimum
STRING_ID_RATIO = 0.5  # string ID if nd > ratio * n_rows AND ...
STRING_ID_MIN = 100  # ... nd > this minimum

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


def _is_yyyymmdd_int_date(values: Sequence[int]) -> bool:
    """YYYYMMDD-shaped integer column heuristic, matching the R behaviour."""
    if not values:
        return False
    sample = values[:200]
    if not all(18000101 <= v <= 22001231 for v in sample):
        return False
    parsed = sum(1 for v in sample if _parses_as_date(str(int(v)), "%Y%m%d"))
    return parsed > len(sample) * DATE_CLASSIFY_THRESHOLD


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


# -- Public API ------------------------------------------------------------


def classify_column(
    col_name: str,
    n_rows: int,
    n_distinct: int,
    sample: Sequence[object],
) -> str:
    """Classify a column.

    Args:
        col_name:    The column's name (used for pattern matching).
        n_rows:      Exact total row count for the source.
        n_distinct:  Exact COUNT(DISTINCT col) over the full source.
        sample:      Non-null sample values pulled from the source. Used
                     for type detection (numeric vs string vs date) and
                     date-format detection. Can be empty.

    Returns:
        One of "id", "categorical", "numeric", "high_cardinality", "date".
    """
    if is_known_id(col_name):
        return "id"

    cap = known_categorical_cap(col_name)
    if cap is not None and n_distinct <= cap:
        return "categorical"

    threshold = max(2, min(FREQ_CAP, int(n_rows * FREQ_RATIO)))
    kind = _python_kind(sample)

    if kind == "bool":
        return "categorical"

    if kind in ("numeric_int", "numeric_float"):
        if n_distinct > n_rows * NUMERIC_ID_RATIO and n_distinct > NUMERIC_ID_MIN:
            return "id"
        if n_distinct <= threshold:
            return "categorical"
        if kind == "numeric_int":
            int_vals = [int(v) for v in sample if v is not None]
            if _is_yyyymmdd_int_date(int_vals):
                return "date"
        return "numeric"

    if kind == "date":
        return "date"

    # string (or empty)
    str_vals = [str(v) for v in sample if v is not None]
    if not str_vals:
        # No sample data to inspect (column may be all-null in the sample
        # window even though n_distinct from the SQL pre-classify pass
        # tells us about the full column). Use cardinality alone.
        if n_distinct <= threshold:
            return "categorical"
        return "high_cardinality"

    if detect_date_format(str_vals) is not None:
        return "date"

    if n_distinct > n_rows * STRING_ID_RATIO and n_distinct > STRING_ID_MIN:
        return "id"
    if n_distinct <= threshold:
        return "categorical"
    return "high_cardinality"
