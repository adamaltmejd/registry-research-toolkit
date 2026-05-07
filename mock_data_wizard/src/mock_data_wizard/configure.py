"""Local ``configure`` step: mdw_step1_discovery.json -> mdw_step2_config.json.

Reads ``mdw_step1_discovery.json`` produced by the bundle in discover mode and
writes a ``mdw_step2_config.json`` next to it. Per-column type assignment
priority (first match wins):

1. **Known id name.** ``is_known_id(name)`` — ``lopnr`` / ``persnr``.
   sql_type can't tell a BIGINT identifier apart from a BIGINT measure;
   the name has to.
2. **Regmeta evidence.** When a ``--register`` is supplied, we join
   ``variable_instance`` + ``classification`` and trust regmeta's
   *semantic* signals over the CSV-derived ``sql_type``:
   - non-null ``variable_instance.value_set_id`` → ``categorical``
     (SCB enumerated the codes — e.g. ``Kon`` {1=Man, 2=Kvinna},
     ``ALKod``, ``SyssStat``, ``FamStF`` — even when ``datatyp`` is
     ``tinyint``/``int`` and the CSV reads BIGINT)
   - any non-null ``classification_id`` → ``categorical``
   - ``datatyp`` ∈ {int, decimal, float, ...} → ``numeric``
   - ``datatyp`` ∈ {date, datetime, ...} → ``date``
   ``datatyp`` is *storage* type, not semantic type — a bare ``char``
   with no value codes / classification is not enough to call a column
   categorical (it's often free text), so we don't.
3. **Register-scoped exact-name backstop.** ``is_rtb_named_categorical``
   covers a small allowlist of SCB names that regmeta is known to be
   missing (``AterAnv`` / ``FelPersonNr`` / ``LopNrByte`` /
   ``FodelseAr`` / ``FodelseArMan``). Exact-name match only; no fuzzy
   patterns. Anything else regmeta doesn't know about falls through.
4. **sql_type.** BIGINT / INTEGER / DOUBLE / DECIMAL / ... → ``numeric``.
   DATE / TIMESTAMP / ... → ``date``. For SQL sources, the database's
   declared type drives the answer; for CSVs read by DuckDB, sql_type
   is DuckDB's own inference (which already does int-vs-double on the
   data) — no separate value-peeking pass at discover time.
5. **Fallthrough.** Anything we don't recognise (VARCHAR/TEXT/...) →
   ``high_cardinality``. The inspector surfaces these as a manual-review
   prompt — no name-pattern guessing, since the false-positive risk on
   common Swedish stems (``land``, ``utbildning``, ``civil``, ...)
   outweighs the convenience.

Year-source carry-through: any ``year`` from ``mdw_step1_discovery.json``'s
``source_detail`` lands in a top-level ``sources`` block so the extract
step can pass it back through to ``mdw_step3_stats.json``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from ._util import lookup_with_prefix_fallback, strip_project_prefix
from .classify import is_known_id, is_rtb_named_categorical
from .config import SCHEMA_VERSION as CONFIG_SCHEMA_VERSION

Confidence = Literal["high", "partial", "none"]

log = logging.getLogger("mdw.configure")

CONFIG_FILENAME = "mdw_step2_config.json"

# Match rate at or above which a register guess is labelled "high" rather
# than "partial" — i.e. a clear majority of non-id columns resolve inside
# the winning register.
_CONFIDENCE_HIGH = 0.75


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

# Regmeta `variable_instance.datatyp` tokens, normalised to lowercase.
# Storage type only — we use it to pick numeric vs. date when nothing
# else has classified the column. The categorical signal comes from
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


@dataclass(frozen=True)
class RegmetaSignal:
    """Per-column evidence pulled from regmeta for one register."""

    # "numeric" / "date" / None. Text storage (char/varchar) maps to
    # None — it isn't a categorical signal on its own.
    datatyp_kind: str | None
    # short_name of the classification attached to this variable
    # (SUN2020-GRUPP, SSYK2012, ...). None when no shared classification.
    classification_short_name: str | None
    # True when any cvid for this column under this register has a
    # non-null value_set_id — i.e. SCB enumerated the codes (covers
    # register-local code lists like ALKod / Kon).
    has_value_codes: bool = False


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

    Mirrors the regmeta branch of ``_classify`` so the inspector can
    detect when a manual override conflicts with what regmeta says.
    Returns ``None`` when regmeta has no opinion (storage type alone is
    not enough — see ``RegmetaSignal``).
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
    return "high_cardinality"


def _validate_discover_payload(payload: Any, source_label: str) -> None:
    """Type-check a mdw_step1_discovery.json payload.

    Raises ``ValueError`` with a CLI-friendly message when the user
    points configure at the wrong file (e.g. ``mdw_step3_stats.json`` -- which
    has the same top-level shape but lacks a discover contract version)
    or a partial / malformed discover file.
    """
    if not isinstance(payload, dict):
        raise ValueError(f"{source_label}: top-level value must be an object")
    cv = payload.get("contract_version")
    if not (isinstance(cv, str) and cv.startswith("discover-")):
        raise ValueError(
            f"{source_label}: expected a mdw_step1_discovery.json (contract_version "
            f"like 'discover-1.0.0'), got contract_version={cv!r}. "
            f"Did you point configure at mdw_step3_stats.json by mistake?"
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
                f"A truncated mdw_step1_discovery.json would silently produce an "
                f"incomplete mdw_step2_config.json."
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
        "       vi.value_set_id IS NOT NULL AS has_value_codes "
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
    has_codes: set[str] = set()
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
        if r["has_value_codes"]:
            has_codes.add(name)
    out: dict[str, RegmetaSignal] = {}
    for name in seen:
        sn_counter = short_name_counts.get(name)
        sn = sn_counter.most_common(1)[0][0] if sn_counter else None
        out[name] = RegmetaSignal(
            datatyp_kind=datatyp_kinds.get(name),
            classification_short_name=sn,
            has_value_codes=name in has_codes,
        )
    return out


def build_config(
    discover: dict[str, Any],
    *,
    register: str | None = None,
    register_per_source: dict[str, str | None] | None = None,
    precomputed_signals: dict[str, dict[str, RegmetaSignal]] | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    """Author a mdw_step2_config.json payload from a mdw_step1_discovery.json payload.

    Per-source register precedence: ``register_per_source[name]`` (when
    the key is present, even if the value is ``None``) overrides the
    global ``register`` argument. Sources whose effective register is
    ``None`` skip the regmeta classification path entirely; sources
    sharing a register are batched into one DB query.

    ``precomputed_signals`` is keyed by register string and lets the
    caller pass in regmeta evidence already fetched (e.g. by
    ``guess_register_per_family``) so we don't requery the same rows.
    Registers not in the dict still trigger a DB lookup.
    """
    sources_in = discover.get("sources", [])
    overrides = register_per_source or {}
    cached = dict(precomputed_signals) if precomputed_signals else {}
    effective: dict[str, str | None] = {}
    for src in sources_in:
        name = src["source_name"]
        effective[name] = overrides[name] if name in overrides else register

    # Group sources by their effective register (may be None) so a single
    # `_regmeta_lookup` call covers every source under that register.
    groups: dict[str, list[str]] = {}
    for name, reg in effective.items():
        if reg is None:
            continue
        # An empty string usually means an unset env var in scripts like
        # `--register "$REGISTER"`. Refuse before opening the DB: the
        # `LIKE '%' || ? || '%'` fallback in resolve_register_ids would
        # otherwise match every register and over-type the whole config
        # as `categorical`.
        if not reg.strip():
            raise ValueError(
                "register must be a non-empty register name or id "
                "(got empty string). Pass None to skip regmeta lookup."
            )
        groups.setdefault(reg, []).append(name)

    signals_per_source: dict[str, dict[str, RegmetaSignal]] = {}
    needs_db = [reg_str for reg_str in groups if reg_str not in cached]
    if needs_db:
        from regmeta import open_db, resolve_register_ids
        from regmeta.db import db_path_from_args

        # Match `compare`'s `--db` semantics: argument is a directory;
        # `db_path_from_args` appends `regmeta.db`.
        resolved_db = db_path_from_args(str(db_path) if db_path else None)
        conn = open_db(resolved_db)
        try:
            sources_by_name = {s["source_name"]: s for s in sources_in}
            for reg_str in needs_db:
                register_ids = resolve_register_ids(conn, reg_str)
                if not register_ids:
                    raise ValueError(
                        f"register {reg_str!r} not found in regmeta. "
                        f"Either fix the spelling or skip regmeta for the "
                        f"affected sources."
                    )
                col_names: set[str] = set()
                for sn in groups[reg_str]:
                    for col in sources_by_name[sn].get("columns", []):
                        col_names.add(col["name"])
                cached[reg_str] = _regmeta_lookup(conn, col_names, register_ids)
        finally:
            conn.close()
    for reg_str, source_names in groups.items():
        for sn in source_names:
            signals_per_source[sn] = cached[reg_str]

    column_types: dict[str, dict[str, dict[str, str]]] = {}
    sources_out: dict[str, dict[str, Any]] = {}
    for src in sources_in:
        source_name = src["source_name"]
        signals = signals_per_source.get(source_name, {})
        src_register = effective.get(source_name)
        cols_out: dict[str, dict[str, str]] = {}
        for col in src.get("columns", []):
            col_name = col["name"]
            sql_type = col.get("sql_type")
            signal = lookup_with_prefix_fallback(signals, col_name)
            cols_out[col_name] = {
                "type": _classify(col_name, sql_type, signal, src_register)
            }
        if cols_out:
            column_types[source_name] = cols_out
        year = src.get("source_detail", {}).get("year")
        if year is not None:
            sources_out[source_name] = {"year": int(year)}
    payload: dict[str, Any] = {
        "contract_version": CONFIG_SCHEMA_VERSION,
        "column_types": column_types,
    }
    if sources_out:
        payload["sources"] = sources_out
    return payload


# -- Schema family grouping + per-family register guessing -----------------
# Used by the interactive flow to bucket annual snapshots of the same
# register into one review unit, then auto-pick the best-matching register
# per family via `enrich._vote_register`.


@dataclass
class FamilyGuess:
    """Auto-detected register and column metadata for a schema family.

    A *schema family* is a set of sources that share identical
    ``(column_name, sql_type)`` tuples — typically annual snapshots of
    the same register (`slutbetyg_Ak9_2018.csv`, `..._2019.csv`, …).
    Members are grouped so the user reviews 14 schema families instead
    of 150 files.
    """

    family_id: str
    sources: list[str]
    columns: list[tuple[str, str | None]]
    register_id: int | None
    register_name: str | None
    confidence: Confidence
    match_count: int
    nonid_count: int
    regmeta_signals: dict[str, RegmetaSignal] = field(default_factory=dict)


def _schema_family_id(columns: list[dict]) -> str:
    """Stable short id from ordered ``(name, sql_type)`` tuples."""
    payload = repr(tuple((c["name"], c.get("sql_type")) for c in columns))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]


def group_schema_families(discover: dict[str, Any]) -> dict[str, list[dict]]:
    """Group sources by identical column schema. Preserves discovery order."""
    families: dict[str, list[dict]] = {}
    for src in discover.get("sources", []):
        fid = _schema_family_id(src.get("columns", []))
        families.setdefault(fid, []).append(src)
    return families


def _confidence_label(
    register_id: int | None, match_count: int, nonid_count: int
) -> Confidence:
    if register_id is None:
        return "none"
    if nonid_count == 0:
        return "partial"
    rate = match_count / nonid_count
    if rate >= _CONFIDENCE_HIGH:
        return "high"
    return "partial"


def guess_register_per_family(
    families: dict[str, list[dict]],
    *,
    db_path: Path | None = None,
) -> dict[str, FamilyGuess]:
    """For each family, vote on the best-matching register via regmeta.

    Reuses ``enrich._vote_register`` (margin-guarded weighted vote) and
    ``enrich._source_name_register_fallback`` (RTB / Flergen filename
    rules) so the wizard agrees with the enrich pipeline on which
    register a file belongs to.

    If the regmeta DB isn't reachable, every guess is returned with
    ``register_id=None`` / ``confidence="none"`` so callers can fall
    back to name-pattern classification without erroring out.
    """
    from .enrich import (
        _bulk_resolve_all_registers,
        _vote_register,
    )

    # Empty-family bookkeeping used in both the no-DB and with-DB paths.
    def _empty(fid: str, sources: list[dict]) -> FamilyGuess:
        first = sources[0]
        cols = first.get("columns", [])
        nonid = [c["name"] for c in cols if not is_known_id(c["name"])]
        return FamilyGuess(
            family_id=fid,
            sources=[s["source_name"] for s in sources],
            columns=[(c["name"], c.get("sql_type")) for c in cols],
            register_id=None,
            register_name=None,
            confidence="none",
            match_count=0,
            nonid_count=len(nonid),
        )

    # Open regmeta lazily; missing DB / import error → graceful fallback.
    # Narrow the catch to genuinely-absent-DB conditions so test assertions
    # (or programmer errors stubbing regmeta) still surface instead of
    # being silently swallowed.
    import sqlite3

    from regmeta.errors import RegmetaError

    conn = None
    try:
        from regmeta import open_db
        from regmeta.db import db_path_from_args

        resolved_db = db_path_from_args(str(db_path) if db_path else None)
        conn = open_db(resolved_db)
    except (
        ImportError,
        FileNotFoundError,
        OSError,
        sqlite3.OperationalError,
        RegmetaError,
    ) as exc:
        log.debug("regmeta unavailable for family guess: %s", exc)
        conn = None

    if conn is None:
        return {fid: _empty(fid, sources) for fid, sources in families.items()}

    out: dict[str, FamilyGuess] = {}
    try:
        all_names: set[str] = set()
        for sources in families.values():
            for col in sources[0].get("columns", []):
                if not is_known_id(col["name"]):
                    all_names.add(col["name"])
        col_to_registers = (
            _bulk_resolve_all_registers(conn, all_names) if all_names else {}
        )

        for fid, sources in families.items():
            first = sources[0]
            cols = first.get("columns", [])
            nonid = [c["name"] for c in cols if not is_known_id(c["name"])]
            vote = _vote_register(nonid, col_to_registers, first["source_name"])
            match_count = 0
            if vote.register_id is not None:
                for raw in nonid:
                    stripped = strip_project_prefix(raw).lower()
                    if vote.register_id in col_to_registers.get(stripped, []):
                        match_count += 1
            out[fid] = FamilyGuess(
                family_id=fid,
                sources=[s["source_name"] for s in sources],
                columns=[(c["name"], c.get("sql_type")) for c in cols],
                register_id=vote.register_id,
                register_name=None,
                confidence=_confidence_label(vote.register_id, match_count, len(nonid)),
                match_count=match_count,
                nonid_count=len(nonid),
            )

        # One batched name lookup for all winning registers.
        reg_ids = sorted({g.register_id for g in out.values() if g.register_id})
        if reg_ids:
            placeholders = ",".join("?" for _ in reg_ids)
            sql = (
                "SELECT register_id, registernamn FROM register "
                f"WHERE register_id IN ({placeholders})"
            )
            rows = conn.execute(sql, list(reg_ids)).fetchall()
            names = {r["register_id"]: r["registernamn"] for r in rows}
            for g in out.values():
                if g.register_id is not None:
                    g.register_name = names.get(g.register_id)

        # Fetch regmeta evidence per family so the inspector and
        # build_config can both reuse it without a second DB pass.
        # Batched per register: every family under the same register
        # shares one lookup keyed by the columns of all those families.
        by_register: dict[int, list[str]] = {}
        for fid, g in out.items():
            if g.register_id is not None:
                by_register.setdefault(g.register_id, []).append(fid)
        for reg_id, fids in by_register.items():
            col_names: set[str] = set()
            for fid in fids:
                for name, _sql in out[fid].columns:
                    col_names.add(name)
            signals = _regmeta_lookup(conn, col_names, [reg_id])
            for fid in fids:
                fam_signals: dict[str, RegmetaSignal] = {}
                for n, _sql in out[fid].columns:
                    sig = lookup_with_prefix_fallback(signals, n)
                    if sig is not None:
                        fam_signals[n] = sig
                out[fid].regmeta_signals = fam_signals
    finally:
        conn.close()

    return out


def resolve_register_to_id_and_name(
    register: str, *, db_path: Path | None = None
) -> tuple[int, str] | None:
    """Resolve a register name or id; return ``(register_id, registernamn)``.

    Returns ``None`` when the register can't be resolved. Used by the
    interactive flow to validate a user-typed override before applying
    it to the family.
    """
    import sqlite3

    from regmeta import open_db, resolve_register_ids
    from regmeta.db import db_path_from_args
    from regmeta.errors import RegmetaError

    try:
        resolved_db = db_path_from_args(str(db_path) if db_path else None)
        conn = open_db(resolved_db)
    except (FileNotFoundError, OSError, sqlite3.OperationalError, RegmetaError):
        return None
    try:
        ids = resolve_register_ids(conn, register)
        if not ids:
            return None
        row = conn.execute(
            "SELECT registernamn FROM register WHERE register_id = ?",
            (ids[0],),
        ).fetchone()
        return (ids[0], row["registernamn"] if row else register)
    finally:
        conn.close()


def _summary_counts(payload: dict[str, Any]) -> Counter[str]:
    """Count assigned types across all sources (one combined tally)."""
    c: Counter[str] = Counter()
    for cols in payload.get("column_types", {}).values():
        for entry in cols.values():
            c[entry["type"]] += 1
    return c


def write_config(path: Path, payload: dict[str, Any]) -> None:
    """Pretty-write ``mdw_step2_config.json``. UTF-8, keys preserved as-is."""
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
    """Top-level entry point: read ``discover_path``, write mdw_step2_config.json.

    Returns the path of the written file. Raises on:
    - missing mdw_step1_discovery.json
    - existing mdw_step2_config.json without ``overwrite=True``
    - empty discover (zero sources -- nothing to configure)
    - register name supplied but unresolvable in regmeta.
    """
    discover_path = Path(discover_path)
    if not discover_path.exists():
        raise FileNotFoundError(f"mdw_step1_discovery.json not found: {discover_path}")

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


def run_configure_from_discover(
    discover_path: Path,
    *,
    output_path: Path | None = None,
    overwrite: bool = False,
    register: str | None = None,
    db_path: Path | None = None,
    regmeta_skip_hint: str = "omit --register to skip regmeta lookup entirely",
) -> int:
    """CLI/interactive shim around ``configure_from_discover``.

    Catches the documented exception types, prints user-friendly errors
    to stderr, and returns a CLI exit code (0 success, 1 failure).
    """
    from regmeta.errors import RegmetaError

    try:
        configure_from_discover(
            discover_path,
            output_path=output_path,
            overwrite=overwrite,
            register=register,
            db_path=db_path,
        )
    except (FileNotFoundError, FileExistsError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except RegmetaError as exc:
        print(f"Error: regmeta lookup failed: {exc.message}", file=sys.stderr)
        if exc.remediation:
            print(f"  {exc.remediation}", file=sys.stderr)
        print(f"  ({regmeta_skip_hint})", file=sys.stderr)
        return 1
    return 0
