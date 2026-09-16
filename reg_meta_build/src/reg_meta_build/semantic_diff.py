"""Read-only catalog comparison across storage-ID changes.

Temporary, disk-backed projections replace IDs by declared natural identities;
the existing dbdiff engine compares their typed row multisets and samples. All
non-ID content, including provenance and build metadata, remains compared.
Valid intervals compare as coverage with multiplicity, so splitting one unchanged
interval is neutral without hiding gaps or overlapping duplicates. Invalid or
reversed bounds remain opaque, exact rows: baseline defects must be comparable.

State-linked conformance and lineage compare over their effective periods, using
the complete endpoint facts instead of row IDs. Historical events retain their
native ID1/ID2/FilID tokens; only their assigned event row ID is omitted. Unknown
surfaces and populated legacy staging tables fail explicitly. Search indexes are
derived and receive the same schema-only treatment as dbdiff.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

from reg_meta_build.dbdiff import (
    DiffReport,
    _compare_schema,
    _content_tables,
    _read_schema,
    _row_hash,
    diff_db_content,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence


class UnsupportedSemanticSurface(ValueError):
    """Comparison cannot account safely for a source schema or populated surface."""


@dataclass(frozen=True)
class SemanticDiffReport:
    schema: DiffReport
    content: DiffReport

    @property
    def identical(self) -> bool:
        return not self.schema.schema_differs and self.content.identical


_EMPTY_ONLY = frozenset(
    {
        "classification_candidate",
        "variable_instance",
        "variable_alias_build",
        "unika_summary",
    }
)
_PLAIN_TABLES = frozenset(
    {
        "population",
        "object_type",
        "variable_state",
        "variable_alias",
        "variable_alias_window",
        "classification_code",
        "value_set_member",
        "identifier_semantics",  # var_id is a native source key, not a storage ID.
        "code_variable_map",
        "variable_same_as",
        "classification_same_as",
        "classification_derived_from",
        "concept_group_axis",
        "concept_group_variable_facet",
        "concept_group_classification",
        "tag_member",
        "register_replaced_by",
        "variant_replaced_by",
        "variable_replaced_by",
        "representation_replaced_by",
        "classification_replaced_by",
        "source_column_type",
        "source_join_key",
        "import_manifest",
        "classification_conformance",
        "classification_conformance_code",
        "variable_state_lineage",
        "variable_state_lineage_warning",
        "timeseries_event",
    }
)


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _reference(table: str, expression: str) -> str:
    return (
        f"(SELECT identity FROM {_quoted('_id_' + table)} "
        f"WHERE original_id = {expression})"
    )


# In dependency order. Each expression consumes the source row alias ``s``.
# JSON tuples disambiguate nullable representation/edition/group coordinates;
# public variable/register/classification identities use their normal FQIDs.
_IDENTITIES: dict[str, tuple[str, str]] = {
    "provider": ("provider_id", "s.slug"),
    "register": (
        "register_id",
        _reference("provider", "s.provider_id") + " || '/' || s.slug",
    ),
    "register_variant": (
        "register_variant_id",
        "json_array('variant', "
        + _reference("register", "s.register_id")
        + ", s.slug)",
    ),
    "register_version": (
        "regver_id",
        "json_array('edition', "
        + _reference("register_variant", "s.register_variant_id")
        + ", s.registerversionnamn)",
    ),
    "variable": (
        "variable_id",
        _reference("register", "s.register_id") + " || '/' || s.slug",
    ),
    "classification": ("id", "'class/' || s.slug"),
    "value_code": ("code_id", "json_array(s.code, s.label)"),
    "value_set": ("value_set_id", "hex(s.member_hash)"),
    "concept_group": (
        "group_id",
        "json_array(s.kind, "
        + _reference("register", "s.register_id")
        + ", s.group_key)",
    ),
    "concept_group_variable": (
        "member_id",
        "json_array("
        + _reference("concept_group", "s.group_id")
        + ", "
        + _reference("variable", "s.variable_id")
        + ", s.delivery_column_name)",
    ),
    "tag": ("tag_id", "s.slug"),
}

_INHERITED_SCOPE = {
    "classification_conformance": "state_id",
    "classification_conformance_code": "state_id",
    "variable_state_lineage_warning": "consumer_state_id",
}
_INTERVAL_TABLES = {
    "variable_state",
    "variable_alias_window",
    "variable_state_lineage",
    *_INHERITED_SCOPE,
}


def _foreign_columns(
    conn: sqlite3.Connection, table: str
) -> dict[str, tuple[str, str]]:
    return {
        row["from"]: (row["table"], row["to"])
        for row in conn.execute(f"PRAGMA foreign_key_list({_quoted(table)})")
    }


def _column_expression(
    table: str, column: str, foreign: dict[str, tuple[str, str]]
) -> str:
    expression = f"s.{_quoted(column)}"
    if table in _IDENTITIES and column == _IDENTITIES[table][0]:
        return _reference(table, expression)
    if column in foreign:
        parent, parent_column = foreign[column]
        if parent in _IDENTITIES and parent_column == _IDENTITIES[parent][0]:
            return _reference(parent, expression)
        if (parent, parent_column) in {
            ("variable_state", "state_id"),
            ("classification_conformance", "state_id"),
        }:
            return _reference("variable_state", expression)
        if table not in _EMPTY_ONLY:
            raise UnsupportedSemanticSurface(
                f"unsupported reference {table}.{column} to {parent}.{parent_column}"
            )
    return expression


def _state_identities(
    conn: sqlite3.Connection, original: sqlite3.Connection, columns: Sequence[str]
) -> None:
    """Intern state facts excluding period; retain exact scope for attached facts.

    The shared typed dbdiff row hash keeps repeated 888k+ conformance-code rows
    compact. Every original non-ID state field is also compared directly in the
    variable_state surface, so this reference does not replace content comparison.
    """
    foreign = _foreign_columns(original, "variable_state")
    facts = [
        _column_expression("variable_state", column, foreign)
        for column in columns
        if column not in {"state_id", "valid_from", "valid_to"}
    ]
    variable = _column_expression("variable_state", "variable_id", foreign)
    variant = _column_expression("variable_state", "register_variant_id", foreign)
    identity = (
        f"json_array({variable}, {variant}, s.delivery_column_name, "
        "row_fingerprint(" + ", ".join(facts) + "))"
    )
    conn.execute(
        "CREATE TEMP TABLE _id_variable_state (original_id INTEGER PRIMARY KEY, "
        "identity TEXT NOT NULL, valid_from TEXT NOT NULL, valid_to TEXT NOT NULL)"
    )
    conn.execute(
        f"INSERT INTO _id_variable_state SELECT state_id, {identity}, valid_from, "
        "valid_to FROM input.variable_state s"
    )


def _connect_readonly(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise FileNotFoundError(f"catalog not found: {path}")
    conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _date_ordinal(value: str) -> int | None:
    try:
        parsed = date.fromisoformat(value)
    except TypeError, ValueError:
        return None
    return parsed.toordinal() if parsed.isoformat() == value else None


_VALID_INTERVAL = (
    "iso_ordinal(valid_from) IS NOT NULL AND iso_ordinal(valid_to) IS NOT NULL "
    "AND iso_ordinal(valid_from) <= iso_ordinal(valid_to)"
)


def _intervals(
    conn: sqlite3.Connection, payload_columns: Sequence[str]
) -> Iterator[tuple[object, ...]]:
    """Stream endpoint events; count coverage instead of unioning away duplicates."""
    names = ", ".join(_quoted(column) for column in payload_columns)
    typed_names = ", ".join(
        f"typeof({_quoted(column)}), {_quoted(column)}" for column in payload_columns
    )
    for row in conn.execute(
        f"SELECT {names}, valid_from, valid_to FROM _interval_rows "
        f"WHERE NOT ({_VALID_INTERVAL})"
    ):
        yield (*row, 1, "opaque")
    query = (
        f"WITH events AS (SELECT {names}, iso_ordinal(valid_from) AS boundary, "
        f"1 AS delta FROM _interval_rows WHERE {_VALID_INTERVAL} UNION ALL "
        f"SELECT {names}, iso_ordinal(valid_to) + 1, -1 FROM _interval_rows "
        f"WHERE {_VALID_INTERVAL}) "
        f"SELECT {names}, boundary, SUM(delta) FROM events "
        f"GROUP BY {typed_names}, boundary ORDER BY {typed_names}, boundary"
    )
    prior_key: tuple[tuple[type, object], ...] | None = None
    start = 0
    count = 0
    for row in conn.execute(query):
        payload = tuple(row[:-2])
        boundary, delta = row[-2:]
        key = tuple((type(value), value) for value in payload)
        if key != prior_key:
            if count:
                raise UnsupportedSemanticSurface("unbalanced state interval coverage")
            prior_key, start = key, boundary
        if delta == 0:
            continue
        if count:
            yield (
                *payload,
                date.fromordinal(start).isoformat(),
                date.fromordinal(boundary - 1).isoformat(),
                count,
                "coverage",
            )
        count += delta
        if count < 0:
            raise UnsupportedSemanticSurface("state ends before its start")
        start = boundary
    if count:
        raise UnsupportedSemanticSurface("unbalanced state interval coverage")


def _project(source: Path, output: Path) -> None:
    with closing(_connect_readonly(source)) as original:
        schema = _read_schema(original)
        tables = set(_content_tables(schema))
        unsupported = tables - _PLAIN_TABLES - _EMPTY_ONLY - _IDENTITIES.keys()
        if unsupported:
            raise UnsupportedSemanticSurface(
                f"{source}: unsupported catalog tables: {', '.join(sorted(unsupported))}"
            )
        for table in sorted(tables & _EMPTY_ONLY):
            if original.execute(f"SELECT 1 FROM {_quoted(table)} LIMIT 1").fetchone():
                raise UnsupportedSemanticSurface(
                    f"{source}: populated {table} needs an explicit semantic projection"
                )
        broken_fk = original.execute("PRAGMA foreign_key_check").fetchone()
        if broken_fk is not None:
            raise UnsupportedSemanticSurface(
                f"{source}: broken foreign-key reference: {tuple(broken_fk)!r}"
            )

        with closing(sqlite3.connect(output, uri=True)) as conn:
            conn.execute("PRAGMA temp_store=FILE")
            conn.execute("PRAGMA cache_size=-8192")
            conn.execute("PRAGMA journal_mode=OFF")
            conn.execute(
                "ATTACH DATABASE ? AS input", (source.resolve().as_uri() + "?mode=ro",)
            )
            conn.create_function("iso_ordinal", 1, _date_ordinal, deterministic=True)
            conn.create_function(
                "row_fingerprint",
                -1,
                lambda *values: f"{_row_hash(values):032x}",
                deterministic=True,
            )
            for table, (primary, expression) in _IDENTITIES.items():
                if table not in tables:
                    continue
                if (
                    "slug" in {column[0] for column in schema.tables[table]}
                    and conn.execute(
                        f"SELECT 1 FROM input.{_quoted(table)} "
                        "WHERE slug IS NULL OR slug = '' LIMIT 1"
                    ).fetchone()
                ):
                    raise UnsupportedSemanticSurface(
                        f"{source}: {table} has no stable nonempty slug"
                    )
                mapping = _quoted("_id_" + table)
                conn.execute(
                    f"CREATE TEMP TABLE {mapping} (original_id INTEGER PRIMARY KEY, "
                    "identity TEXT NOT NULL UNIQUE)"
                )
                try:
                    conn.execute(
                        f"INSERT INTO {mapping} SELECT s.{_quoted(primary)}, "
                        f"{expression} FROM input.{_quoted(table)} s"
                    )
                except sqlite3.Error as exc:
                    raise UnsupportedSemanticSurface(
                        f"{source}: cannot establish unique {table} identities: {exc}"
                    ) from exc

            if "variable_state" in tables:
                _state_identities(
                    conn,
                    original,
                    tuple(column[0] for column in schema.tables["variable_state"]),
                )
            for table in sorted(tables):
                columns = tuple(column[0] for column in schema.tables[table])
                foreign = _foreign_columns(original, table)
                expressions = []
                kept = []
                for column in columns:
                    if (table, column) in {
                        ("variable_state", "state_id"),
                        ("timeseries_event", "timeseries_event_id"),
                    }:
                        continue
                    kept.append(column)
                    expression = _column_expression(table, column, foreign)
                    expressions.append(expression + " AS " + _quoted(column))
                if table in _INHERITED_SCOPE:
                    state_column = _quoted(_INHERITED_SCOPE[table])
                    for bound in ("valid_from", "valid_to"):
                        if bound in kept:
                            raise UnsupportedSemanticSurface(
                                f"{table} now defines its own {bound}; scope projection needs review"
                            )
                        kept.append(bound)
                        expressions.append(
                            f"(SELECT {bound} FROM _id_variable_state "
                            f"WHERE original_id = s.{state_column}) AS {bound}"
                        )
                interval_table = table in _INTERVAL_TABLES
                destination = "_interval_rows" if interval_table else table
                conn.execute(
                    f"CREATE TABLE {_quoted(destination)} ("
                    + ", ".join(_quoted(column) for column in kept)
                    + ")"
                )
                conn.execute(
                    f"INSERT INTO {_quoted(destination)} SELECT "
                    + ", ".join(expressions)
                    + f" FROM input.{_quoted(table)} s"
                )
                if interval_table:
                    payload_columns = tuple(
                        column
                        for column in kept
                        if column not in {"valid_from", "valid_to"}
                    )
                    canonical_columns = (
                        *payload_columns,
                        "valid_from",
                        "valid_to",
                        "multiplicity",
                        "interval_representation",
                    )
                    conn.execute(
                        f"CREATE TABLE {_quoted(table)} ("
                        + ", ".join(_quoted(column) for column in canonical_columns)
                        + ")"
                    )
                    conn.executemany(
                        f"INSERT INTO {_quoted(table)} VALUES ("
                        + ", ".join("?" for _ in canonical_columns)
                        + ")",
                        _intervals(conn, payload_columns),
                    )
                    conn.execute("DROP TABLE _interval_rows")
            conn.commit()


def diff_catalog_semantics(
    db_a: str | Path,
    db_b: str | Path,
    *,
    sample_rows: int = 10,
    temporary_directory: str | Path | None = None,
) -> SemanticDiffReport:
    """Compare every supported surface, failing loudly on unsupported content.

    Inputs are always opened read-only. Projection data and identity indexes live
    in disposable SQLite files; Python does not retain corpus-sized maps/row lists.
    Unlike dbdiff's default, even import timestamps remain explicitly compared.
    """
    first, second = Path(db_a), Path(db_b)
    schema_report = DiffReport(db_a=first, db_b=second)
    with (
        closing(_connect_readonly(first)) as left,
        closing(_connect_readonly(second)) as right,
    ):
        _compare_schema(_read_schema(left), _read_schema(right), schema_report)
    with TemporaryDirectory(
        prefix="regmeta-semantic-diff-", dir=temporary_directory
    ) as temporary:
        first_projection = Path(temporary) / "a.sqlite"
        second_projection = Path(temporary) / "b.sqlite"
        try:
            _project(first, first_projection)
            _project(second, second_projection)
        except sqlite3.Error as exc:
            raise UnsupportedSemanticSurface(
                f"cannot project catalog semantics: {exc}"
            ) from exc
        content = diff_db_content(
            first_projection, second_projection, ignore={}, sample_rows=sample_rows
        )
        content.db_a, content.db_b = first, second
    return SemanticDiffReport(schema=schema_report, content=content)
