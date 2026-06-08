"""Per-column summarizer.

Runs the typed SQL queries from ``sql_emit.queries_for_column`` against a
DB-API 2.0 connection (DuckDB or pyodbc-wrapped MS SQL), applies
disclosure-control post-processing (k-anonymity on frequencies, uniform
relative noise on numeric aggregates, jitter on dates), and returns the
per-column dict that lands in ``mock_data_stats.json``.

PII safety: suppression and noise are applied to **aggregates**, not
rows. No individual-level data passes through Python.
"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any

# Import the floor directly from ``constants`` (rather than through the
# top-level ``reg_monabundle`` re-export) so a runtime submodule never
# pulls the lightweight surface (``build``, ``scan``, ``validate``)
# back through ``reg_monabundle/__init__.py``. The slicer drops both
# forms; the difference matters only when the runtime is loaded as a
# regular Python package locally.
from reg_monabundle.constants import SUPPRESS_K

from .classify import DATE_FORMATS, _python_kind, detect_date_format
from .sql_emit import queries_for_column

if TYPE_CHECKING:
    from collections.abc import Sequence

    from .spec import ColumnTypeOverride

# Disclosure-control thresholds. SUPPRESS_K lives in reg_monabundle because
# it's the bundle's privacy floor (also enforced by the namespaced-block
# validator); re-exported here so the runtime modules (summarize,
# extract) keep their stable import surface.
__all__ = ["SUPPRESS_K"]

NOISE_PCT = 0.005  # +/-0.5% relative noise on numeric aggregates
SMALL_POP_MULT = 20  # warn when n_rows < SMALL_POP_MULT * SUPPRESS_K
DATE_JITTER_DAYS = 7  # +/- N days uniform jitter on date min/max/quantiles

OTHER_LABEL = "_other"
DATE_QUANTILES = (0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99)


def small_pop_threshold() -> int:
    return SMALL_POP_MULT * SUPPRESS_K


def _perturb(val: Any, rng: random.Random, *, is_int: bool = False) -> Any:
    if val is None:
        return None
    fv = float(val)
    out = fv + fv * rng.uniform(-NOISE_PCT, NOISE_PCT)
    return int(round(out)) if is_int else round(out, 6)


def _row_to_dict(description, row) -> dict[str, Any]:
    # Lowercase keys: DuckDB lowercases aliases by default, MS SQL preserves
    # case. Normalising here lets summarize logic key on lowercase names
    # regardless of dialect.
    return {d[0].lower(): row[i] for i, d in enumerate(description)}


def _fetch_one(conn, sql: str) -> dict[str, Any]:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        row = cur.fetchone()
        if row is None:
            return {}
        return _row_to_dict(cur.description, row)
    finally:
        cur.close()


def _fetch_all(conn, sql: str) -> list[dict[str, Any]]:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        desc = cur.description
        return [_row_to_dict(desc, r) for r in cur.fetchall()]
    finally:
        cur.close()


def _to_date(v: Any, override_format: str | None = None) -> date | None:
    """Coerce a SQL-returned value to a ``date``. Returns ``None`` when no
    parser succeeds. ``override_format`` is tried first so a config-pinned
    format (e.g. ``%Y.%m.%d``) wins over the built-in DATE_FORMATS list."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()
    if not s:
        return None
    formats = (override_format, *DATE_FORMATS) if override_format else DATE_FORMATS
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).date()
        except (ValueError, TypeError):
            continue
    # ISO-ish fallback: trim a trailing time component before retrying.
    head = s.split(" ", 1)[0]
    try:
        return datetime.strptime(head, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _detect_id_subtype(sample: Sequence[Any]) -> str:
    """Pick ``"integer"`` vs ``"string"`` for an unpinned id column.

    The MSSQL path returns native ints/floats for numeric columns, so
    ``_python_kind`` is enough there. The DuckDB file-source path now
    reads CSVs with ``all_varchar=true`` (issue #40), so a numeric LOPNR
    arrives as a string — we additionally treat a sample of all-digit
    strings (leading zeros allowed, no sign, no decimal, no exponent)
    as integer. SCB pids and study-IDs are positive digit strings; this
    excludes ``"+5"``, ``"-1"``, ``"1.0"``, ``"1e3"`` which ``int(s)``
    would have accepted but which signal "not really an id" in this
    domain. Mock output for int ids drops leading zeros, mirroring the
    pre-all_varchar behaviour where the read inferred BIGINT.
    """
    kind = _python_kind(sample)
    if kind in ("numeric_int", "numeric_float"):
        return "integer"
    if kind != "string":
        return "string"
    non_null = [v for v in sample if v is not None]
    if not non_null:
        return "string"
    for v in non_null:
        # .strip() tolerates surrounding whitespace as an SCB-export
        # artifact (some exporters pad numeric fields); the integer
        # mock generator will emit unpadded digits regardless.
        s = str(v).strip()
        if not s.isdigit():
            return "string"
    return "integer"


def _suppress_below_k(
    rows: Sequence[dict[str, Any]], suppress_k: int = SUPPRESS_K
) -> dict[str, int]:
    """Apply k-anonymity to a frequency-table query result.

    Drops the NULL group (null_count is tracked separately), folds counts
    below ``suppress_k`` into ``_other``, and returns a dict in original
    (descending-count) order. ``_other`` is dropped entirely when its
    count is below ``suppress_k``: emitting it as ``None`` would still
    leak the existence of a handful of outliers, so the bucket simply
    disappears in that case (consumers default to weight 0).
    """
    out: dict[str, int] = {}
    other = 0
    for r in rows:
        val = r.get("val")
        n = int(r.get("n", 0))
        if val is None:
            continue
        if n < suppress_k:
            other += n
        else:
            out[str(val)] = n
    if other >= suppress_k:
        out[OTHER_LABEL] = other
    return out


def _jitter_date(d: date, rng: random.Random) -> date:
    """Apply uniform +/- DATE_JITTER_DAYS jitter to a date."""
    return d + timedelta(days=rng.randint(-DATE_JITTER_DAYS, DATE_JITTER_DAYS))


def _date_quantiles_from_sample(
    sample: Sequence[Any], date_format: str, rng: random.Random
) -> dict[str, str]:
    """Compute jittered date quantiles in Python.

    Doing this server-side would require a per-dialect, per-storage-format
    DATEDIFF dance; the sample is already on the wire so quantile
    estimation here is cheap and correct enough for mock-data fidelity.

    Independent jitter can violate monotonicity (p25 > p50 etc.) on
    narrow samples. We sort the jittered values and re-assign them to
    the quantile labels so consumers see a non-decreasing sequence.
    """
    parsed: list[date] = []
    for v in sample:
        if v is None:
            continue
        try:
            parsed.append(datetime.strptime(str(v), date_format).date())
        except (ValueError, TypeError):
            continue
    if not parsed:
        return {}
    parsed.sort()
    n = len(parsed)
    jittered = sorted(
        _jitter_date(parsed[min(n - 1, int(q * n))], rng) for q in DATE_QUANTILES
    )
    return {
        f"p{int(round(q * 100)):02d}": d.isoformat()
        for q, d in zip(DATE_QUANTILES, jittered)
    }


def summarize_column(
    conn,
    table: str,
    col_name: str,
    col_type: str,
    n_rows: int,
    n_distinct: int,
    null_count: int,
    sample: Sequence[Any],
    dialect: str,
    rng: random.Random | None = None,
    *,
    override: ColumnTypeOverride | None = None,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compute the per-column ``mock_data_stats.json`` fragment for one column.

    Args:
        conn: DB-API 2.0 connection.
        table: Already-quoted/qualified table reference for the dialect.
        col_name: Unquoted column name (``sql_emit`` handles quoting).
        col_type: One of ``classify.COLUMN_TYPES``.
        n_rows: Total rows in the source.
        n_distinct, null_count: Pre-computed by the caller (during the
            same scan that produced the sample) so we don't pay for them
            twice.
        sample: Sample of values used for subtype / date-format detection.
            May be empty when an inline override hint is supplied.
        dialect: ``duckdb`` or ``mssql``.
        rng: Optional seeded RNG for deterministic noise (tests).
        override: When supplied, marks ``source_of_type="override"`` and
            carries any inline subtype/format hints that let the caller
            skip the per-column sample query.
        options: Per-column option overrides loaded from the
            ``reg_monabundle.binding_options`` block of
            ``project_data.json``. Reserved for downstream consumers
            (e.g. ``suppress_k`` in disclosure-control hardening).
    """
    rng = rng or random.Random()
    options = options or {}
    suppress_k = int(options.get("suppress_k", SUPPRESS_K))
    id_subtype = override.id_subtype if override else None
    numeric_subtype = override.numeric_subtype if override else None
    date_format = override.date_format if override else None

    queries = queries_for_column(table, col_name, col_type, dialect)

    base: dict[str, Any] = {
        "column_name": col_name,
        "inferred_type": col_type,
        "source_of_type": "override" if override else "auto",
        "nullable": null_count > 0,
        "n_distinct": int(n_distinct),
    }
    # Censor null_count when 0 < null_count < suppress_k: an exact small
    # count exposes a handful of outliers. nullable: True still tells
    # downstream code that nulls exist (without saying how many).
    if null_count == 0 or null_count >= suppress_k:
        base["null_count"] = int(null_count)
        base["null_rate"] = round(null_count / max(n_rows, 1), 6)

    stats: dict[str, Any] = {}

    if col_type == "numeric":
        agg = _fetch_one(conn, queries["aggs"])
        if numeric_subtype is not None:
            stats["numeric_subtype"] = numeric_subtype
            is_int = numeric_subtype == "integer"
        else:
            kind = _python_kind(sample)
            is_int = kind == "numeric_int"
            stats["numeric_subtype"] = "integer" if is_int else "double"
        # Independent +/- relative noise on min/max can swap them on
        # narrow ranges (and on min == max); sort the perturbed pair so
        # the bound invariant holds.
        mn = _perturb(agg.get("min_v"), rng, is_int=is_int)
        mx = _perturb(agg.get("max_v"), rng, is_int=is_int)
        if mn is not None and mx is not None:
            mn, mx = sorted((mn, mx))
        stats["min"] = mn
        stats["max"] = mx
        stats["mean"] = _perturb(agg.get("mean_v"), rng)
        stats["sd"] = _perturb(agg.get("sd_v"), rng)
        q = _fetch_one(conn, queries["quantiles"])
        # Same independent-noise problem on adjacent quantiles -- sort
        # the perturbed values and re-zip with the labels so consumers
        # see a non-decreasing sequence.
        q_labels = ("p01", "p05", "p25", "p50", "p75", "p95", "p99")
        q_vals = [_perturb(q.get(label), rng, is_int=is_int) for label in q_labels]
        if all(v is not None for v in q_vals):
            q_vals = sorted(q_vals)
        stats["quantiles"] = dict(zip(q_labels, q_vals))

    elif col_type == "categorical":
        rows = _fetch_all(conn, queries["freqs"])
        stats["frequencies"] = _suppress_below_k(rows, suppress_k)
        stats["suppressed_below_k"] = suppress_k

    elif col_type == "opaque":
        agg = _fetch_one(conn, queries["aggs"])
        if agg.get("min_length") is not None:
            stats["min_length"] = int(agg["min_length"])
            stats["max_length"] = int(agg["max_length"])
            stats["mean_length"] = round(float(agg["mean_length"]), 1)

    elif col_type == "date":
        agg = _fetch_one(conn, queries["aggs"])
        min_d = _to_date(agg.get("min_v"), date_format)
        max_d = _to_date(agg.get("max_v"), date_format)
        if min_d is not None and max_d is not None:
            # Independent jitter can produce min > max on narrow ranges;
            # sort the jittered pair so the bound invariant holds.
            jittered = sorted((_jitter_date(min_d, rng), _jitter_date(max_d, rng)))
            stats["min"] = jittered[0].isoformat()
            stats["max"] = jittered[1].isoformat()
        elif min_d is not None:
            stats["min"] = _jitter_date(min_d, rng).isoformat()
        elif max_d is not None:
            stats["max"] = _jitter_date(max_d, rng).isoformat()
        # Detect format if not pinned via override.
        fmt = date_format
        if fmt is None:
            str_sample = [str(v) for v in sample if v is not None]
            if str_sample:
                fmt = detect_date_format(str_sample)
        if fmt is not None:
            stats["date_format"] = fmt
            # Sample-based quantiles in Python (jittered). When the
            # sample is empty (inline-override path) we just skip them.
            qs = _date_quantiles_from_sample(sample, fmt, rng)
            if qs:
                stats["quantiles"] = qs

    elif col_type == "id":
        if id_subtype is not None:
            stats["id_subtype"] = id_subtype
        else:
            stats["id_subtype"] = _detect_id_subtype(sample)

    else:
        raise ValueError(f"unknown col_type: {col_type!r}")

    base["stats"] = stats
    return base
