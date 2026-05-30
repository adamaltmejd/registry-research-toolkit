"""Column classification primitives shared across the package.

Pure functions (and one reg_meta DB query). The data-driven
``classify_column`` path was removed when extract switched to a
config-driven workflow. What remains:

* Name-pattern surface (``is_known_id``, ``is_rtb_named_categorical``)
  used by the editor to author the per-column type config.
* Date-format helpers consumed by ``summarize.py`` when a date override
  has no inline ``date_format`` hint.
* The 5-type classifier (``_classify``) that combines name patterns,
  reg_meta evidence, and SQL declared types into one of
  ``COLUMN_TYPES``.
* RegMeta evidence: ``RegMetaSignal`` dataclass, ``_reg_meta_lookup``
  (A2.7: joins ``variable_alias`` → ``variable`` → ``variable_state``), and
  ``reg_meta_implied_type`` (mirror of the reg_meta branch for conflict
  warnings).
* Discover-payload validator (``_validate_discover_payload``) so the
  editor can fail-fast on malformed or wrong-file inputs.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, cast

from ._util import lookup_with_prefix_fallback, strip_project_prefix

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

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


# Register-scoped exact-name categoricals. Names reg_meta is known to be
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


# -- SQL / reg_meta storage-type tokens -------------------------------------
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

# RegMeta `variable_instance.data_type` tokens, normalised to lowercase.
# Storage type only — used to pick numeric vs. date when nothing else
# has classified the column. The categorical signal comes from
# `value_set_id` / `classification_id`, not from a "char/varchar"
# data_type (a char column with no code list is usually free text).
_REG_META_NUMERIC = frozenset(
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
_REG_META_DATE = frozenset(
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
            # The `all(isinstance(...) or hasattr(...))` predicate above is
            # too rich for ty to narrow `v` past `object`; cast at the use
            # site rather than restructuring the (perf-sensitive) predicate.
            if all(float(v) == int(float(v)) for v in non_null):  # ty: ignore[invalid-argument-type]
                return "numeric_int"
        except (ValueError, TypeError):
            pass
        return "numeric_float"
    if all(isinstance(v, str) for v in non_null):
        return "string"
    # Mixed: treat as string (the sample might have been coerced)
    return "string"


# -- RegMeta evidence ------------------------------------------------------


@dataclass(frozen=True)
class RegMetaSignal:
    """Per-column evidence pulled from reg_meta for one register."""

    # "numeric" / "date" / None. Text storage (char/varchar) maps to
    # None — it isn't a categorical signal on its own.
    data_type_kind: str | None
    # short_name of the classification attached to this variable
    # (SUN2020-GRUPP, SSYK2012, ...). None when no shared classification.
    # When n_classifications > 1 this is the most-common winner.
    classification_short_name: str | None
    # True when any cvid for this column under this register has a
    # non-null value_set_id — i.e. SCB enumerated the codes (covers
    # register-local code lists like ALKod / Kon).
    has_value_codes: bool = False
    # ``> 1`` means the code set / classification differs across years
    # (drives the variance-tier popup picker).
    n_value_sets: int = 0
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


def _reg_meta_data_type_kind(data_type: str | None) -> str | None:
    """Map a reg_meta ``variable_instance.data_type`` to a kind bucket.

    Returns ``"numeric"`` / ``"date"`` / ``None``. Text storage tokens
    (char/varchar/...) intentionally return ``None``: text is not a
    semantic categorical signal on its own — see ``RegMetaSignal``.
    """
    if not data_type:
        return None
    head = data_type.strip().split("(", 1)[0].split()[0].lower()
    if head in _REG_META_NUMERIC:
        return "numeric"
    if head in _REG_META_DATE:
        return "date"
    return None


def reg_meta_implied_type(signal: RegMetaSignal | None) -> str | None:
    """The semantic type reg_meta implies for a column, if any.

    Mirrors the reg_meta branch of ``_classify`` so the editor can detect
    when a manual override conflicts with what reg_meta says. Returns
    ``None`` when reg_meta has no opinion (storage type alone is not
    enough — see ``RegMetaSignal``).
    """
    if signal is None:
        return None
    if signal.has_value_codes or signal.classification_short_name:
        return "categorical"
    if signal.data_type_kind in {"numeric", "date"}:
        return signal.data_type_kind
    return None


def _classify(
    col_name: str,
    sql_type: str | None,
    signal: RegMetaSignal | None,
    register: str | None = None,
) -> str:
    """Return one of the five mock_data_wizard column types.

    ``signal`` is the reg_meta evidence for this column under the chosen
    register. ``None`` means reg_meta wasn't consulted (no register set)
    or the column doesn't appear in reg_meta — in which case the name
    pattern + sql_type fallback drives the type. ``register`` is the
    user-supplied register string (``"RTB"``, ``"LISA"``, …); only
    consulted by register-scoped overrides like the RTB flag set.
    """
    if is_known_id(col_name):
        return "id"
    implied = reg_meta_implied_type(signal)
    if implied is not None:
        return implied
    if is_rtb_named_categorical(col_name, register):
        return "categorical"
    kind = _sql_type_kind(sql_type)
    if kind:
        return kind
    return "opaque"


def _reg_meta_lookup(
    conn: Any,
    col_names: set[str],
    register_ids: list[int],
    *,
    relevant_years: set[int] | None = None,
) -> dict[str, RegMetaSignal]:
    """Look up reg_meta evidence for ``col_names`` under ``register_ids``.

    Returns a dict keyed by lowercased column name; callers must
    lowercase their lookup keys. Mirrors the ``variable_alias`` join
    used by ``enrich._bulk_resolve`` so configure agrees with
    enrichment on which name maps to which variable.

    Aggregation across states (A2.7: was per-cvid ``variable_instance``):
    when the same alias's variable has multiple ``variable_state`` eras
    (one per year/variant/coding), the first non-null ``data_type`` wins
    (SCB rarely changes storage type across versions), the most-common
    ``classification.short_name`` wins, and ``has_value_codes`` is True
    if *any* era has a non-null ``value_set_id``. Columns absent from
    reg_meta are absent from the result.

    ``relevant_years`` scopes the variance counts (``n_value_sets``,
    ``n_classifications``) to eras whose ``valid_from`` year is in the
    set — keeps the "varies · N" badge consistent with the year-filtered
    popup. Yearless eras (the 0001 fallback sentinel) contribute to the
    counts since we can't disprove their relevance.
    ``data_type_kind`` / ``classification_short_name`` / ``has_value_codes``
    stay unfiltered: they answer "does reg_meta know this column" which
    isn't a per-year question.

    ``conn`` is owned by the caller (kept open across
    ``resolve_register_ids`` and this lookup so we don't reopen the DB).
    """
    if not col_names or not register_ids:
        return {}
    lookup: set[str] = set()
    for c in col_names:
        lookup.add(c)
        stripped = strip_project_prefix(c)
        if stripped != c:
            lookup.add(stripped)
    col_list = sorted(lookup)
    col_placeholders = ",".join("?" for _ in col_list)
    reg_placeholders = ",".join("?" for _ in register_ids)
    # A2.7: the shipped reg_meta DB no longer carries `variable_instance` /
    # `register_version` — read the per-era unit `variable_state` instead.
    # `variable_alias` is variable_id-keyed; the state's `valid_from` year is the
    # year signal (was register_version.registerversionnamn). `var_id` is the
    # variable's `provider_key`. A column is matched via `variable_alias`, then
    # the variable's states supply data_type / classification / value_set / year.
    #
    # The `variable_state` join is scoped by `register_variant_id` too, NOT
    # `variable_id` alone: a column delivered under K register_variants would
    # otherwise pull in all K variants' states, multiplying every classification's
    # vote in `short_name_counts_all` by K and possibly flipping the
    # `most_common(1)` badge (PR #149). Scoping to the alias's own variant keeps
    # one vote per (column, variant, era) — the per-cvid-variant scoping the
    # cvid-keyed `variable_alias` had before A2.7.
    sql = (
        "SELECT LOWER(va.delivery_column_name) AS lower_name, "
        "       vs.data_type AS data_type, "
        "       c.short_name AS short_name, "
        "       vs.value_set_id AS value_set_id, "
        "       vs.valid_from AS valid_from "
        "FROM variable_alias va "
        "JOIN variable v ON va.variable_id = v.variable_id "
        "JOIN variable_state vs ON vs.variable_id = v.variable_id "
        "    AND vs.register_variant_id = va.register_variant_id "
        "LEFT JOIN classification c ON vs.classification_id = c.id "
        f"WHERE LOWER(va.delivery_column_name) IN ({col_placeholders}) "
        f"  AND v.register_id IN ({reg_placeholders})"
    )
    params = [c.lower() for c in col_list] + list(register_ids)
    rows = conn.execute(sql, params).fetchall()

    data_type_kinds: dict[str, str] = {}
    short_name_counts_all: dict[str, Counter] = {}
    short_name_counts_scoped: dict[str, set[str]] = {}
    value_set_ids_all: dict[str, set[int]] = {}
    value_set_ids_scoped: dict[str, set[int]] = {}
    seen: set[str] = set()
    for r in rows:
        name = r["lower_name"]
        seen.add(name)
        if name not in data_type_kinds:
            kind = _reg_meta_data_type_kind(r["data_type"])
            if kind is not None:
                data_type_kinds[name] = kind
        sn = r["short_name"]
        vsid = r["value_set_id"]
        if sn:
            short_name_counts_all.setdefault(name, Counter())[sn] += 1
        if vsid is not None:
            value_set_ids_all.setdefault(name, set()).add(int(vsid))
        if relevant_years is None:
            in_scope = True
        else:
            # A2.7: the era's year is the state's `valid_from` (was the
            # register_version name). The `0001` yearless-fallback sentinel reads
            # as "no parseable year" → can't disprove relevance, so kept in scope.
            vf_year = int(r["valid_from"][:4])
            year = vf_year if vf_year > 1 else None
            in_scope = year is None or year in relevant_years
        if in_scope:
            if sn:
                short_name_counts_scoped.setdefault(name, set()).add(sn)
            if vsid is not None:
                value_set_ids_scoped.setdefault(name, set()).add(int(vsid))
    out: dict[str, RegMetaSignal] = {}
    for name in seen:
        sn_counter = short_name_counts_all.get(name)
        sn = sn_counter.most_common(1)[0][0] if sn_counter else None
        out[name] = RegMetaSignal(
            data_type_kind=data_type_kinds.get(name),
            classification_short_name=sn,
            has_value_codes=bool(value_set_ids_all.get(name)),
            n_value_sets=len(value_set_ids_scoped.get(name, set())),
            n_classifications=len(short_name_counts_scoped.get(name, set())),
        )
    return out


def lookup_signal(
    signals: dict[str, RegMetaSignal], col_name: str
) -> RegMetaSignal | None:
    """Find the reg_meta signal for ``col_name`` (case-insensitive,
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
        # Post-isinstance ty narrows JSON-loaded dicts to dict[Unknown, Unknown];
        # cast so the rest of the validator can use string-key access.
        src_obj = cast("Mapping[str, Any]", src)
        if "source_name" not in src_obj:
            raise ValueError(f"{source_label}: sources[{i}] missing 'source_name'")
        name = src_obj["source_name"]
        if not isinstance(name, str):
            raise ValueError(
                f"{source_label}: sources[{i}].source_name must be a string"
            )
        if name in seen_names:
            raise ValueError(
                f"{source_label}: duplicate source_name {name!r} at sources["
                f"{seen_names[name]}] and sources[{i}]. The editor keys column "
                f"overrides by source_name; collisions would silently drop one "
                f"source's column map."
            )
        seen_names[name] = i
        if "columns" not in src_obj:
            raise ValueError(
                f"{source_label}: sources[{i}] ({name!r}) missing 'columns'. "
                f"A truncated mock_data_discovery.json would silently produce "
                f"an incomplete project_data.json."
            )
        cols = src_obj["columns"]
        if not isinstance(cols, list):
            raise ValueError(f"{source_label}: sources[{i}].columns must be a list")
        for j, col in enumerate(cols):
            if not isinstance(col, dict) or "name" not in col:
                raise ValueError(
                    f"{source_label}: sources[{i}].columns[{j}] must be an "
                    f"object with a 'name' key"
                )
