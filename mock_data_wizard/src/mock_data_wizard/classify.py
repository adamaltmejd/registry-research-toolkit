"""Column classification primitives shared across the package.

Pure functions (and one regmeta DB query). The data-driven
``classify_column`` path was removed when extract switched to a
config-driven workflow. What remains:

* Name-pattern surface (``is_known_id``, ``is_rtb_named_categorical``)
  used by the editor to author the per-column type config.
* Date-format helpers consumed by ``summarize.py`` when a date override
  has no inline ``date_format`` hint.
* The 5-type classifier (``_classify``) that combines name patterns,
  regmeta evidence, and SQL declared types into one of
  ``COLUMN_TYPES``.
* Regmeta evidence: ``RegmetaSignal`` dataclass, ``_regmeta_lookup``
  (joins ``variable_alias`` → ``variable_instance``), and
  ``regmeta_implied_type`` (mirror of the regmeta branch for conflict
  warnings).
* Discover-payload validator (``_validate_discover_payload``) so the
  editor can fail-fast on malformed or wrong-file inputs.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Sequence

from ._util import lookup_with_prefix_fallback, strip_project_prefix

# Canonical inferred-type enum. One source of truth -- imported by
# config (validation), sql_emit (dispatch), and stats (consumer-side).
COLUMN_TYPES: tuple[str, ...] = (
    "id",
    "categorical",
    "numeric",
    "opaque",
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
# "opaque" in the inspector and override manually than silently
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


# -- SQL / regmeta storage-type tokens -------------------------------------
# Match the leading bare keyword: "BIGINT", "INTEGER", "DECIMAL(18,2)",
# "TIMESTAMP WITH TIME ZONE" all reduce to their first token. Covers
# DuckDB's CSV-inferred types and T-SQL declared types both.

_NUMERIC_SQL = frozenset(
    {
        "tinyint",
        "smallint",
        "int",
        "integer",
        "bigint",
        "hugeint",
        "decimal",
        "numeric",
        "real",
        "float",
        "double",
        "money",
        "smallmoney",
    }
)
_DATE_SQL = frozenset(
    {
        "date",
        "datetime",
        "datetime2",
        "smalldatetime",
        "timestamp",
        "datetimeoffset",
    }
)

# Regmeta `variable_instance.datatyp` tokens, normalised to lowercase.
# Storage type only — used to pick numeric vs. date when nothing else
# has classified the column. The categorical signal comes from
# `value_set_id` / `classification_id`, not from a "char/varchar"
# datatyp (a char column with no code list is usually free text).
_REGMETA_NUMERIC = frozenset(
    {
        "tinyint",
        "smallint",
        "int",
        "integer",
        "bigint",
        "decimal",
        "numeric",
        "real",
        "float",
        "double",
        "money",
        "smallmoney",
    }
)
_REGMETA_DATE = frozenset(
    {"date", "datetime", "datetime2", "smalldatetime", "timestamp"}
)


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


# -- Regmeta evidence ------------------------------------------------------


@dataclass(frozen=True)
class RegmetaSignal:
    """Per-column evidence pulled from regmeta for one register."""

    # "numeric" / "date" / None. Text storage (char/varchar) maps to
    # None — it isn't a categorical signal on its own.
    datatyp_kind: str | None
    # short_name of the classification attached to this variable
    # (SUN2020-GRUPP, SSYK2012, ...). None when no shared classification.
    # When n_classifications > 1 this is the most-common winner.
    classification_short_name: str | None
    # True when any cvid for this column under this register has a
    # non-null value_set_id — i.e. SCB enumerated the codes (covers
    # register-local code lists like ALKod / Kon).
    has_value_codes: bool = False
    # Count of distinct ``vi.value_set_id`` across cvids for this column
    # under this register. ``> 1`` means the code set differs across
    # years — the popup union may include codes that aren't valid every
    # year. NULL value_set_ids are excluded.
    n_value_sets: int = 0
    # Count of distinct ``classification.short_name`` across cvids.
    # ``> 1`` means the column maps to different classifications across
    # years (e.g. Kommun → LKF2012 vs other LKF versions) — the inline
    # badge says "varies" instead of showing one winner.
    n_classifications: int = 0


def _sql_type_kind(sql_type: str | None) -> str | None:
    """Map a sql_type string to ``"numeric"``, ``"date"``, or ``None``."""
    if not sql_type:
        return None
    stripped = sql_type.strip()
    if not stripped:
        return None
    # "DECIMAL(18,2)" → "DECIMAL"; "TIMESTAMP WITH TIME ZONE" → "TIMESTAMP".
    head = stripped.split("(", 1)[0].split()[0].lower()
    if head in _NUMERIC_SQL:
        return "numeric"
    if head in _DATE_SQL:
        return "date"
    return None


def _regmeta_datatyp_kind(datatyp: str | None) -> str | None:
    """Map a regmeta ``variable_instance.datatyp`` to a kind bucket.

    Returns ``"numeric"`` / ``"date"`` / ``None``. Text storage tokens
    (char/varchar/...) intentionally return ``None``: text is not a
    semantic categorical signal on its own — see ``RegmetaSignal``.
    """
    if not datatyp:
        return None
    head = datatyp.strip().split("(", 1)[0].split()[0].lower()
    if head in _REGMETA_NUMERIC:
        return "numeric"
    if head in _REGMETA_DATE:
        return "date"
    return None


def regmeta_implied_type(signal: RegmetaSignal | None) -> str | None:
    """The semantic type regmeta implies for a column, if any.

    Mirrors the regmeta branch of ``_classify`` so the editor can detect
    when a manual override conflicts with what regmeta says. Returns
    ``None`` when regmeta has no opinion (storage type alone is not
    enough — see ``RegmetaSignal``).
    """
    if signal is None:
        return None
    if signal.has_value_codes or signal.classification_short_name:
        return "categorical"
    if signal.datatyp_kind in {"numeric", "date"}:
        return signal.datatyp_kind
    return None


def _classify(
    col_name: str,
    sql_type: str | None,
    signal: RegmetaSignal | None,
    register: str | None = None,
) -> str:
    """Return one of the five mock_data_wizard column types.

    ``signal`` is the regmeta evidence for this column under the chosen
    register. ``None`` means regmeta wasn't consulted (no register set)
    or the column doesn't appear in regmeta — in which case the name
    pattern + sql_type fallback drives the type. ``register`` is the
    user-supplied register string (``"RTB"``, ``"LISA"``, …); only
    consulted by register-scoped overrides like the RTB flag set.
    """
    if is_known_id(col_name):
        return "id"
    implied = regmeta_implied_type(signal)
    if implied is not None:
        return implied
    if is_rtb_named_categorical(col_name, register):
        return "categorical"
    kind = _sql_type_kind(sql_type)
    if kind:
        return kind
    return "opaque"


def _regmeta_lookup(
    conn: Any, col_names: set[str], register_ids: list[int]
) -> dict[str, RegmetaSignal]:
    """Look up regmeta evidence for ``col_names`` under ``register_ids``.

    Returns a dict keyed by lowercased column name; callers must
    lowercase their lookup keys. Mirrors the ``variable_alias`` join
    used by ``enrich._resolve_columns`` so configure agrees with
    enrichment on which name maps to which variable_instance.

    Aggregation across cvids: when the same alias points at multiple
    ``variable_instance`` rows (one per year/variant), the first
    non-null ``datatyp`` wins (SCB rarely changes storage type across
    versions), the most-common ``classification.short_name`` wins, and
    ``has_value_codes`` is True if *any* cvid has a non-null
    ``value_set_id``. Columns absent from regmeta are absent from the
    result.

    ``conn`` is owned by the caller (kept open across
    ``resolve_register_ids`` and this lookup so we don't reopen the DB).
    """
    if not col_names or not register_ids:
        return {}
    # Strip project prefixes so `P1105_LopNr` resolves to `LopNr` —
    # the same trick enrich uses. Both raw and stripped names go in.
    lookup: set[str] = set()
    for c in col_names:
        lookup.add(c)
        stripped = strip_project_prefix(c)
        if stripped != c:
            lookup.add(stripped)
    col_list = sorted(lookup)
    col_placeholders = ",".join("?" for _ in col_list)
    reg_placeholders = ",".join("?" for _ in register_ids)
    sql = (
        "SELECT LOWER(va.kolumnnamn) AS lower_name, "
        "       vi.datatyp AS datatyp, "
        "       c.short_name AS short_name, "
        "       vi.value_set_id AS value_set_id "
        "FROM variable_alias va "
        "JOIN variable_instance vi ON va.cvid = vi.cvid "
        "LEFT JOIN classification c ON vi.classification_id = c.id "
        f"WHERE LOWER(va.kolumnnamn) IN ({col_placeholders}) "
        f"  AND vi.register_id IN ({reg_placeholders})"
    )
    params = [c.lower() for c in col_list] + list(register_ids)
    rows = conn.execute(sql, params).fetchall()

    datatyp_kinds: dict[str, str] = {}
    short_name_counts: dict[str, Counter] = {}
    value_set_ids: dict[str, set[int]] = {}
    seen: set[str] = set()
    for r in rows:
        name = r["lower_name"]
        seen.add(name)
        if name not in datatyp_kinds:
            kind = _regmeta_datatyp_kind(r["datatyp"])
            if kind is not None:
                datatyp_kinds[name] = kind
        sn = r["short_name"]
        if sn:
            short_name_counts.setdefault(name, Counter())[sn] += 1
        vsid = r["value_set_id"]
        if vsid is not None:
            value_set_ids.setdefault(name, set()).add(int(vsid))
    out: dict[str, RegmetaSignal] = {}
    for name in seen:
        sn_counter = short_name_counts.get(name)
        sn = sn_counter.most_common(1)[0][0] if sn_counter else None
        vsids = value_set_ids.get(name, set())
        out[name] = RegmetaSignal(
            datatyp_kind=datatyp_kinds.get(name),
            classification_short_name=sn,
            has_value_codes=bool(vsids),
            n_value_sets=len(vsids),
            n_classifications=len(sn_counter) if sn_counter else 0,
        )
    return out


def lookup_signal(
    signals: dict[str, RegmetaSignal], col_name: str
) -> RegmetaSignal | None:
    """Find the regmeta signal for ``col_name`` (case-insensitive,
    project-prefix-tolerant). Mirrors ``_util.lookup_with_prefix_fallback``."""
    return lookup_with_prefix_fallback(signals, col_name)


# -- Discover payload validator -------------------------------------------


def _validate_discover_payload(payload: Any, source_label: str) -> None:
    """Type-check a mock_data_discovery.json payload.

    Raises ``ValueError`` with a CLI-friendly message when the user
    points the editor at the wrong file (e.g. ``mock_data_stats.json``
    -- which has the same top-level shape but lacks a discover contract
    version) or a partial / malformed discover file.
    """
    if not isinstance(payload, dict):
        raise ValueError(f"{source_label}: top-level value must be an object")
    cv = payload.get("contract_version")
    if not (isinstance(cv, str) and cv.startswith("discover-")):
        raise ValueError(
            f"{source_label}: expected a mock_data_discovery.json (contract_version "
            f"like 'discover-1.0.0'), got contract_version={cv!r}. "
            f"Did you point the editor at mock_data_stats.json by mistake?"
        )
    sources = payload.get("sources")
    if not isinstance(sources, list):
        raise ValueError(f"{source_label}: 'sources' must be a list")
    seen_names: dict[str, int] = {}
    for i, src in enumerate(sources):
        if not isinstance(src, dict):
            raise ValueError(f"{source_label}: sources[{i}] must be an object")
        if "source_name" not in src:
            raise ValueError(f"{source_label}: sources[{i}] missing 'source_name'")
        name = src["source_name"]
        if name in seen_names:
            raise ValueError(
                f"{source_label}: duplicate source_name {name!r} at sources["
                f"{seen_names[name]}] and sources[{i}]. The editor keys column "
                f"overrides by source_name; collisions would silently drop one "
                f"source's column map."
            )
        seen_names[name] = i
        if "columns" not in src:
            raise ValueError(
                f"{source_label}: sources[{i}] ({name!r}) missing 'columns'. "
                f"A truncated mock_data_discovery.json would silently produce "
                f"an incomplete mock_data_config.json."
            )
        cols = src["columns"]
        if not isinstance(cols, list):
            raise ValueError(f"{source_label}: sources[{i}].columns must be a list")
        for j, col in enumerate(cols):
            if not isinstance(col, dict) or "name" not in col:
                raise ValueError(
                    f"{source_label}: sources[{i}].columns[{j}] must be an "
                    f"object with a 'name' key"
                )
