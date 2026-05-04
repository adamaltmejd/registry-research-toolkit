"""Local ``configure`` step: discover.json -> mdw_config.json.

Reads ``discover.json`` produced by the bundle in discover mode and
writes a ``mdw_config.json`` next to it. Per-column type assignment
priority (first match wins):

1. **Known id name.** ``is_known_id(name)`` — ``lopnr`` / ``persnr``.
   sql_type can't tell a BIGINT identifier apart from a BIGINT measure;
   the name has to.
2. **Regmeta classification.** When a ``--register`` is supplied and
   the column joins to a ``variable_instance`` row with
   ``classification_id IS NOT NULL``, the variable is a code-list
   variable by SCB's own definition: ``categorical``.
3. **Known categorical name.** ``known_categorical_cap(name) is not
   None`` — ``Kon`` / ``Sun2000Inr`` / ``Kommun`` / ... . SCB doesn't
   always wire up a ``classification_id`` for these in regmeta, so
   the name pattern stays as a backstop.
4. **sql_type.** BIGINT / INTEGER / DOUBLE / DECIMAL / ... → ``numeric``.
   DATE / TIMESTAMP / ... → ``date``. For SQL sources, the database's
   declared type drives the answer; for CSVs read by DuckDB, sql_type
   is DuckDB's own inference (which already does int-vs-double on the
   data) — no separate value-peeking pass at discover time.
5. **Fallthrough.** Anything we don't recognise (VARCHAR/TEXT/...) →
   ``high_cardinality``. Misclassified, you fix it in mdw_config.json.

Year-source carry-through: any ``year`` from ``discover.json``'s
``source_detail`` lands in a top-level ``sources`` block so the extract
step can pass it back through to ``stats.json``.
"""

from __future__ import annotations

import json
import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from ._util import strip_project_prefix
from .classify import is_known_id, known_categorical_cap
from .config import SCHEMA_VERSION as CONFIG_SCHEMA_VERSION

log = logging.getLogger("mdw.configure")

CONFIG_FILENAME = "mdw_config.json"


# SQL type tokens, normalised to lowercase. Match the leading bare
# keyword: "BIGINT", "INTEGER", "DECIMAL(18,2)", "TIMESTAMP WITH TIME
# ZONE" all reduce to their first token. Covers DuckDB's CSV-inferred
# types and T-SQL declared types both.
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


def _sql_type_kind(sql_type: str | None) -> str | None:
    """Map a sql_type string to ``"numeric"``, ``"date"``, or ``None``."""
    if not sql_type:
        return None
    head = sql_type.strip().split("(", 1)[0].split()[0].lower()
    if head in _NUMERIC_SQL:
        return "numeric"
    if head in _DATE_SQL:
        return "date"
    return None


def _classify(
    col_name: str,
    sql_type: str | None,
    has_classification: bool,
) -> str:
    """Return one of the five mock_data_wizard column types.

    ``has_classification`` is the regmeta signal: ``True`` when the
    column maps to at least one ``variable_instance`` row with a
    non-null ``classification_id`` for the user-supplied register.
    """
    if is_known_id(col_name):
        return "id"
    if has_classification:
        return "categorical"
    if known_categorical_cap(col_name) is not None:
        return "categorical"
    kind = _sql_type_kind(sql_type)
    if kind:
        return kind
    return "high_cardinality"


def _validate_discover_payload(payload: Any, source_label: str) -> None:
    """Type-check a discover.json payload.

    Raises ``ValueError`` with a CLI-friendly message when the user
    points configure at the wrong file (e.g. ``stats.json`` -- which
    has the same top-level shape but lacks a discover contract version)
    or a partial / malformed discover file.
    """
    if not isinstance(payload, dict):
        raise ValueError(f"{source_label}: top-level value must be an object")
    cv = payload.get("contract_version")
    if not (isinstance(cv, str) and cv.startswith("discover-")):
        raise ValueError(
            f"{source_label}: expected a discover.json (contract_version "
            f"like 'discover-1.0.0'), got contract_version={cv!r}. "
            f"Did you point configure at stats.json by mistake?"
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
                f"{seen_names[name]}] and sources[{i}]. The configurer keys "
                f"column overrides by source_name; collisions would silently "
                f"drop one source's column map."
            )
        seen_names[name] = i
        if "columns" not in src:
            raise ValueError(
                f"{source_label}: sources[{i}] ({name!r}) missing 'columns'. "
                f"A truncated discover.json would silently produce an "
                f"incomplete mdw_config.json."
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


def _classification_lookup(
    col_names: set[str], register_ids: list[int], db_path: Path
) -> set[str]:
    """Return the lowercased subset of ``col_names`` that have a
    non-null ``classification_id`` in ``variable_instance`` for any of
    the supplied registers.

    Mirrors the ``variable_alias`` join used by ``enrich._resolve_columns``
    so configure agrees with enrichment on which name maps to which
    variable_instance. Returns lowercased names — callers must lowercase
    their lookup keys.
    """
    if not col_names or not register_ids:
        return set()
    from regmeta import open_db

    conn = open_db(db_path)
    try:
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
            "SELECT DISTINCT LOWER(va.kolumnnamn) AS lower_name "
            "FROM variable_alias va "
            "JOIN variable_instance vi ON va.cvid = vi.cvid "
            f"WHERE LOWER(va.kolumnnamn) IN ({col_placeholders}) "
            f"  AND vi.register_id IN ({reg_placeholders}) "
            "  AND vi.classification_id IS NOT NULL"
        )
        params = [c.lower() for c in col_list] + list(register_ids)
        rows = conn.execute(sql, params).fetchall()
        return {r["lower_name"] for r in rows}
    finally:
        conn.close()


def build_config(
    discover: dict[str, Any],
    *,
    register: str | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    """Author a mdw_config.json payload from a discover.json payload.

    When ``register`` is supplied, opens the regmeta DB at ``db_path``
    (or the default location), resolves the register string to one or
    more register ids, and uses ``classification_id IS NOT NULL`` from
    ``variable_instance`` as the categorical signal.
    """
    classified_lower: set[str] = set()
    if register is not None:
        from regmeta import open_db, resolve_register_ids
        from regmeta.db import db_path_from_args

        resolved_db = Path(db_path) if db_path else db_path_from_args(None)
        conn = open_db(resolved_db)
        try:
            register_ids = resolve_register_ids(conn, register)
        finally:
            conn.close()
        if not register_ids:
            raise ValueError(
                f"register {register!r} not found in regmeta. "
                f"Either fix the spelling or omit --register."
            )
        all_names: set[str] = set()
        for src in discover.get("sources", []):
            for col in src.get("columns", []):
                all_names.add(col["name"])
        classified_lower = _classification_lookup(all_names, register_ids, resolved_db)

    column_types: dict[str, dict[str, dict[str, str]]] = {}
    sources: dict[str, dict[str, Any]] = {}
    for src in discover.get("sources", []):
        source_name = src["source_name"]
        cols_out: dict[str, dict[str, str]] = {}
        for col in src.get("columns", []):
            col_name = col["name"]
            sql_type = col.get("sql_type")
            stripped = strip_project_prefix(col_name).lower()
            has_classification = (
                col_name.lower() in classified_lower or stripped in classified_lower
            )
            cols_out[col_name] = {
                "type": _classify(col_name, sql_type, has_classification)
            }
        if cols_out:
            column_types[source_name] = cols_out
        year = src.get("source_detail", {}).get("year")
        if year is not None:
            sources[source_name] = {"year": int(year)}
    payload: dict[str, Any] = {
        "contract_version": CONFIG_SCHEMA_VERSION,
        "column_types": column_types,
    }
    if sources:
        payload["sources"] = sources
    return payload


def _summary_counts(payload: dict[str, Any]) -> Counter[str]:
    """Count assigned types across all sources (one combined tally)."""
    c: Counter[str] = Counter()
    for cols in payload.get("column_types", {}).values():
        for entry in cols.values():
            c[entry["type"]] += 1
    return c


def write_config(path: Path, payload: dict[str, Any]) -> None:
    """Pretty-write ``mdw_config.json``. UTF-8, keys preserved as-is."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def configure_from_discover(
    discover_path: Path,
    output_path: Path | None = None,
    *,
    overwrite: bool = False,
    register: str | None = None,
    db_path: Path | None = None,
) -> Path:
    """Top-level entry point: read ``discover_path``, write mdw_config.json.

    Returns the path of the written file. Raises on:
    - missing discover.json
    - existing mdw_config.json without ``overwrite=True``
    - empty discover (zero sources -- nothing to configure)
    - register name supplied but unresolvable in regmeta.
    """
    discover_path = Path(discover_path)
    if not discover_path.exists():
        raise FileNotFoundError(f"discover.json not found: {discover_path}")

    target = (
        Path(output_path)
        if output_path is not None
        else discover_path.with_name(CONFIG_FILENAME)
    )
    if target.exists() and not overwrite:
        raise FileExistsError(
            f"{target} already exists; pass --overwrite to replace it."
        )

    payload = json.loads(discover_path.read_text(encoding="utf-8"))
    _validate_discover_payload(payload, str(discover_path))
    if not payload["sources"]:
        raise ValueError(f"{discover_path} has no sources -- nothing to configure.")

    config = build_config(payload, register=register, db_path=db_path)
    write_config(target, config)

    counts = _summary_counts(config)
    summary = ", ".join(f"{t}={n}" for t, n in sorted(counts.items()))
    n_sources = len(config["column_types"])
    n_cols = sum(counts.values())
    print(
        f"Wrote {target} ({n_sources} source(s), {n_cols} column(s)): {summary}",
        file=sys.stderr,
    )
    return target
