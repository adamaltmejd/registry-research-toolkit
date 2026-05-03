"""Per-column summarizer.

Runs the typed SQL queries from ``sql_emit.queries_for_column`` against a
DB-API 2.0 connection (DuckDB or pyodbc-wrapped MS SQL), applies
disclosure-control post-processing (k-anonymity on frequencies, uniform
relative noise on numeric aggregates, jitter on dates), and returns the
per-column dict that lands in ``stats.json``.

PII safety: suppression and noise are applied to **aggregates**, not
rows. No individual-level data passes through Python.
"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta
from typing import Any, Sequence

from .classify import DATE_FORMATS, _python_kind, detect_date_format
from .sql_emit import queries_for_column

# Disclosure-control thresholds.
SUPPRESS_K = 10  # categorical counts below this fold into _other
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


def _to_iso(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    s = str(v).strip()
    if not s:
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except (ValueError, TypeError):
            continue
    # Already ISO-ish? Trim time component if present.
    return s.split(" ", 1)[0] if "-" in s[:10] else s


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


def _jitter_iso_date(iso: str, rng: random.Random) -> str:
    """Apply uniform +/- DATE_JITTER_DAYS jitter to an ISO date."""
    d = datetime.strptime(iso, "%Y-%m-%d").date()
    d2 = d + timedelta(days=rng.randint(-DATE_JITTER_DAYS, DATE_JITTER_DAYS))
    return d2.isoformat()


def _date_quantiles_from_sample(
    sample: Sequence[Any], date_format: str, rng: random.Random
) -> dict[str, str]:
    """Compute jittered date quantiles in Python.

    Doing this server-side would require a per-dialect, per-storage-format
    DATEDIFF dance; the sample is already on the wire so quantile
    estimation here is cheap and correct enough for mock-data fidelity.
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
    out: dict[str, str] = {}
    for q in DATE_QUANTILES:
        idx = min(n - 1, int(q * n))
        d = parsed[idx]
        d2 = d + timedelta(days=rng.randint(-DATE_JITTER_DAYS, DATE_JITTER_DAYS))
        out[f"p{int(round(q * 100)):02d}"] = d2.isoformat()
    return out


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
    source_of_type: str = "auto",
    id_subtype: str | None = None,
    numeric_subtype: str | None = None,
    date_format: str | None = None,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compute the per-column ``stats.json`` fragment for one column.

    Args:
        conn: DB-API 2.0 connection.
        table: Already-quoted/qualified table reference for the dialect.
        col_name: Unquoted column name (``sql_emit`` handles quoting).
        col_type: One of ``classify.classify_column``'s outputs.
        n_rows: Total rows in the source.
        n_distinct, null_count: Pre-computed by the caller (during the
            same scan that produced the sample) so we don't pay for them
            twice.
        sample: Sample of values used for subtype / date-format detection.
            May be empty when an inline override hint is supplied.
        dialect: ``duckdb`` or ``mssql``.
        rng: Optional seeded RNG for deterministic noise (tests).
        source_of_type: ``"auto"`` for classifier-inferred, ``"override"``
            when a ``mdw_config.json`` entry forced ``col_type``.
        id_subtype, numeric_subtype, date_format: When supplied, used
            verbatim instead of being inferred from ``sample``. This is
            the path that lets a fully-typed config skip the per-column
            sample query.
        options: Per-column option overrides loaded from
            ``mdw_config.json``. Reserved for downstream consumers
            (e.g. ``suppress_k`` in disclosure-control hardening).
    """
    rng = rng or random.Random()
    options = options or {}
    suppress_k = int(options.get("suppress_k", SUPPRESS_K))

    queries = queries_for_column(table, col_name, col_type, dialect)

    base: dict[str, Any] = {
        "column_name": col_name,
        "inferred_type": col_type,
        "source_of_type": source_of_type,
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
        stats["min"] = _perturb(agg.get("min_v"), rng, is_int=is_int)
        stats["max"] = _perturb(agg.get("max_v"), rng, is_int=is_int)
        stats["mean"] = _perturb(agg.get("mean_v"), rng)
        stats["sd"] = _perturb(agg.get("sd_v"), rng)
        q = _fetch_one(conn, queries["quantiles"])
        stats["quantiles"] = {
            label: _perturb(q.get(label), rng, is_int=is_int)
            for label in ("p01", "p05", "p25", "p50", "p75", "p95", "p99")
        }

    elif col_type == "categorical":
        rows = _fetch_all(conn, queries["freqs"])
        stats["frequencies"] = _suppress_below_k(rows, suppress_k)
        stats["suppressed_below_k"] = suppress_k

    elif col_type == "high_cardinality":
        agg = _fetch_one(conn, queries["aggs"])
        if agg.get("min_length") is not None:
            stats["min_length"] = int(agg["min_length"])
            stats["max_length"] = int(agg["max_length"])
            stats["mean_length"] = round(float(agg["mean_length"]), 1)

    elif col_type == "date":
        agg = _fetch_one(conn, queries["aggs"])
        min_iso = _to_iso(agg.get("min_v"))
        max_iso = _to_iso(agg.get("max_v"))
        if min_iso is not None:
            stats["min"] = _jitter_iso_date(min_iso, rng)
        if max_iso is not None:
            stats["max"] = _jitter_iso_date(max_iso, rng)
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
            kind = _python_kind(sample)
            stats["id_subtype"] = (
                "integer" if kind in ("numeric_int", "numeric_float") else "string"
            )

    else:
        raise ValueError(f"unknown col_type: {col_type!r}")

    base["stats"] = stats
    return base
