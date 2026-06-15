"""Test fixtures: reg_meta fixture DBs pointed at via REG_META_DB.

CI has no real reg_meta asset (5.1.0 is unpublished), so the backend tests build
fixture DBs and point the app at them via the highest-precedence ``REG_META_DB``
override (``reg_meta.db.default_db_dir``).

- ``/api/context`` reads ONLY ``import_manifest`` → the manifest-only fixture
  (``compatible_db`` / ``mismatched_db``) needs nothing but that table.
- ``/api/catalog`` resolves/lists against the full reg_meta schema → the
  ``catalog_db`` fixture builds a slugged DB via ``reg_meta_build``'s
  ``_slugged_db`` helper (one provider/register/variant/variable/classification,
  plus extra registers/bindings) and stamps an ``import_manifest`` so ``open_db``
  passes its boot compat check. We mirror ``reg_meta/tests/conftest.py``'s
  sys.path injection to import that bare-name helper.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest
import reg_meta.db
import reg_meta.doc_db

# `_slugged_db` is a bare-name helper in reg_meta_build/tests/. Add that dir to
# sys.path so this backend conftest can import the catalog-fixture-DB builder
# (mirrors reg_meta/tests/conftest.py).
sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[3] / "reg_meta_build" / "tests"),
)

FIXTURE_IMPORT_DATE = "2026-06-01T00:00:00Z"

# A schema_version that PASSES open_db's gate (same major.minor) but differs from
# the code constant in the PATCH (_check_schema_compat ignores patch) — so
# test_context proves /api/context surfaces the manifest's value, not an echo of
# reg_meta.SCHEMA_VERSION.
_MAJOR, _MINOR, _ = reg_meta.db.SCHEMA_VERSION.split(".")
FIXTURE_SCHEMA_VERSION = f"{_MAJOR}.{_MINOR}.999"


def _write_manifest_db(db_path: Path, schema_version: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE import_manifest(key TEXT PRIMARY KEY, value TEXT)")
        conn.executemany(
            "INSERT INTO import_manifest(key, value) VALUES (?, ?)",
            [
                ("schema_version", schema_version),
                ("import_date", FIXTURE_IMPORT_DATE),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def _point_app_at(monkeypatch: pytest.MonkeyPatch, db_dir: Path) -> None:
    # REG_META_DB is the highest-precedence dir in reg_meta.db.default_db_dir.
    monkeypatch.setenv("REG_META_DB", str(db_dir))


@pytest.fixture
def fixture_import_date() -> str:
    return FIXTURE_IMPORT_DATE


@pytest.fixture
def fixture_schema_version() -> str:
    return FIXTURE_SCHEMA_VERSION


@pytest.fixture
def compatible_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A fixture DB whose manifest is gate-compatible with the installed code."""
    db_path = tmp_path / reg_meta.db.DB_FILENAME
    _write_manifest_db(db_path, FIXTURE_SCHEMA_VERSION)
    _point_app_at(monkeypatch, tmp_path)
    return db_path


@pytest.fixture
def mismatched_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A fixture DB one MAJOR ahead — startup must reject it."""
    db_path = tmp_path / reg_meta.db.DB_FILENAME
    major = int(reg_meta.db.SCHEMA_VERSION.split(".")[0])
    _write_manifest_db(db_path, f"{major + 1}.0.0")
    _point_app_at(monkeypatch, tmp_path)
    return db_path


def _stamp_manifest(conn: sqlite3.Connection) -> None:
    """Add the boot-required ``import_manifest`` to a freshly-built slugged DB so
    ``open_db``'s schema-compat gate (run in the lifespan) passes. The slugged-DB
    DDL has the manifest table; we just fill the two keys the lifespan needs."""
    conn.executemany(
        "INSERT INTO import_manifest(key, value) VALUES (?, ?)",
        [
            ("schema_version", FIXTURE_SCHEMA_VERSION),
            ("import_date", FIXTURE_IMPORT_DATE),
        ],
    )
    conn.commit()


def _build_catalog_fixture_db(db_path: Path) -> None:
    """Build a slugged catalog DB on disk for the ``/api/catalog`` tests.

    Uses ``reg_meta_build``'s ``_slugged_db`` builder: the default
    ``scb/lisa/kon`` binding (with one state) plus a value-set on it, a second
    register ``scb/rams`` with its own binding, and a ``variable_same_as`` edge
    so the embedded leaf carries a non-empty ``same_as``. Then copies the
    in-memory DB to ``db_path`` and stamps the manifest."""
    from _slugged_db import (  # noqa: PLC0415 — sys.path-injected test helper
        add_register,
        add_state,
        add_value_set,
        add_variable,
        add_variant,
        add_version,
        build_slugged_db,
    )

    src = build_slugged_db()
    # A value set on the default kon binding so the embedded leaf exercises
    # value_set hydration.
    add_value_set(src, value_set_id=1, codes=[("1", "Man"), ("2", "Kvinna")])
    src.execute(
        "UPDATE variable_state SET value_set_id = 1 "
        "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')"
    )
    # A second register + binding so a provider node has >1 child register and a
    # register node has a non-default binding to list.
    add_register(src, register_id=2, slug="rams", name="RAMS")
    add_variant(src, register_variant_id=20, register_id=2, slug="standard", name="Std")
    # A4.4c: panel-shape columns on the `standard` variant so the variant endpoint
    # exercises non-NULL panel serialization (composite entity key → JSON array).
    src.execute(
        "UPDATE register_variant SET panel_entity_key = ?, panel_time_key = ?, "
        "panel_time_grain = ? WHERE register_variant_id = 20",
        (json.dumps(["foretag", "arbetsstalle"]), "period", "delivery"),
    )
    add_version(src, regver_id=200, register_variant_id=20, name="2019")
    add_variable(src, register_id=2, var_id=77, name="Sysselsättning", slug="syss")
    add_state(
        src,
        register_id=2,
        variable_slug="syss",
        register_variant_id=20,
        delivery_column_name="Syss",
    )
    # A curated same_as edge kon→syss so the kon leaf embeds a same_as ref.
    src.execute(
        "INSERT INTO variable_same_as "
        "(a_provider, a_register, a_variable, b_provider, b_register, b_variable) "
        "VALUES ('scb','lisa','kon','scb','rams','syss')"
    )
    _seed_kon_edges(src)
    _seed_succession_chain(src)
    _seed_concept_groups(src, add_variable)
    _seed_code_variable_map(src)
    _seed_merged_family(src, add_variable, add_state)
    _rebuild_fts(src)
    _stamp_manifest(src)

    dst = sqlite3.connect(db_path)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()


def _rebuild_fts(src: sqlite3.Connection) -> None:
    """Populate the external-content FTS5 indexes from their content tables, so
    the slugged fixture exercises ``/api/search`` (#350/#352). Base-table INSERTs
    don't sync external-content FTS5; the 'rebuild' command repopulates each
    index from its `content=` table — mirrors what the real build does.

    `value_code_fts` (#352) gets 'rebuild' too. The build-time stoplist exclusion
    is NOT reproduced here (rebuild indexes every content row); the fixture's
    value labels ("Man"/"Kvinna") aren't stoplisted anyway, so this is faithful
    for the codes-group test."""
    for index in (
        "register_fts",
        "variable_fts",
        "classification_fts",
        "value_code_fts",
    ):
        src.execute(f"INSERT INTO {index}({index}) VALUES('rebuild')")


def _seed_code_variable_map(src: sqlite3.Connection) -> None:
    """Map the kon binding's value codes to the kon variable (#352) so a code/value
    search resolves each (code, label) to its owning variable and computes
    mapping_count. Mirrors the real build's `code_variable_map` + mapping_count
    pass over the value_set on the kon state.

    Also links the "Man" code to the existing `sun2020` classification (a
    `classification_code` row) so a code hit carries a non-empty
    `classification_count` (the catalog-scoped owner side of #352). Runs AFTER
    `_seed_concept_groups` (which inserts sun2020)."""
    kon_vid = src.execute(
        "SELECT variable_id FROM variable WHERE slug = 'kon'"
    ).fetchone()[0]
    src.execute(
        "INSERT INTO code_variable_map (code_id, variable_id) "
        "SELECT DISTINCT vsm.code_id, ? FROM value_set_member vsm "
        "JOIN variable_state vs ON vs.value_set_id = vsm.value_set_id "
        "WHERE vs.variable_id = ?",
        (kon_vid, kon_vid),
    )
    src.execute(
        "UPDATE value_code SET mapping_count = ("
        "SELECT COUNT(*) FROM code_variable_map WHERE code_id = value_code.code_id)"
    )
    man_code_id = src.execute(
        "SELECT code_id FROM value_code WHERE label = 'Man'"
    ).fetchone()[0]
    sun2020_id = src.execute(
        "SELECT id FROM classification WHERE slug = 'sun2020'"
    ).fetchone()[0]
    src.execute(
        "INSERT INTO classification_code (classification_id, code_id, level, is_valid) "
        "VALUES (?, ?, NULL, 1)",
        (sun2020_id, man_code_id),
    )


def _seed_merged_family(src: sqlite3.Connection, add_variable, add_state) -> None:
    """Seed a MERGED monthly-family variable (#319) on scb/lisa: one variable
    `lonfink` with ONE annual 2018 state + three month columns in `variable_alias`
    and three `variable_alias_window` rows (jan/feb/mars 2018). Exercises the
    resolver's read-time per-month expansion through `/api/catalog/{fqid}?period=`
    and the backend's compound-key (state_id, column, valid_from) dedup."""
    add_variable(src, register_id=1, var_id=950, name="Lön per månad", slug="lonfink")
    add_state(
        src,
        register_id=1,
        variable_slug="lonfink",
        register_variant_id=10,  # lisa's default variant
        valid_from="2018-01-01",
        valid_to="2018-12-31",
        delivery_column_name="LonFinkJan",
    )
    vid = src.execute(
        "SELECT variable_id FROM variable WHERE slug = 'lonfink'"
    ).fetchone()[0]
    for col, lo, hi in (
        ("LonFinkJan", "2018-01-01", "2018-01-31"),
        ("LonFinkFeb", "2018-02-01", "2018-02-28"),
        ("LonFinkMars", "2018-03-01", "2018-03-31"),
    ):
        # All three columns live in variable_alias (get_datacolumns) + the window
        # table (resolver). The annual state's own column (Jan) is already in
        # variable_alias via add_state; add Feb/Mars.
        src.execute(
            "INSERT OR IGNORE INTO variable_alias "
            "(variable_id, register_variant_id, delivery_column_name) "
            "VALUES (?, 10, ?)",
            (vid, col),
        )
        src.execute(
            "INSERT INTO variable_alias_window (variable_id, register_variant_id, "
            "delivery_column_name, valid_from, valid_to) VALUES (?, 10, ?, ?, ?)",
            (vid, col, lo, hi),
        )


def _seed_kon_edges(src: sqlite3.Connection) -> None:
    """Seed the variable-grain edges + state-grain lineage the A5.2a-ii suffixed
    sub-endpoints read off the ``scb/lisa/kon`` binding, so their tests assert
    non-empty results (the leaf-embed tests only assert these fields are PRESENT,
    so adding rows is compatible):

    - ``variable_replaced_by``: kon → rams/syss (a succession edge, so
      ``/successors`` on kon and ``/predecessors`` on syss are non-empty).
    - ``variable_related_to``: kon ↔ rams/syss split-sibling (``/related``).
    - ``variable_state_lineage``: kon's state consumes rams/syss's state
      (``/lineage`` non-empty, with a real ``source_fqid``).
    - ``variable_state_lineage_warning``: a ``no_source_state`` warning on kon's
      state (``/lineage_warnings`` non-empty)."""
    kon_state = src.execute(
        "SELECT state_id FROM variable_state WHERE variable_id = "
        "(SELECT variable_id FROM variable WHERE slug = 'kon')"
    ).fetchone()[0]
    syss_state = src.execute(
        "SELECT state_id FROM variable_state WHERE variable_id = "
        "(SELECT variable_id FROM variable WHERE slug = 'syss')"
    ).fetchone()[0]
    src.execute(
        "INSERT INTO variable_replaced_by "
        "(predecessor_provider, predecessor_register, predecessor_variable, "
        "successor_provider, successor_register, successor_variable, "
        "effective_year, note, beskrivning) "
        "VALUES ('scb','lisa','kon','scb','rams','syss',2019,'auto:test','kon→syss')"
    )
    src.execute(
        "INSERT INTO variable_related_to "
        "(a_provider, a_register, a_variable, b_provider, b_register, b_variable, "
        "relation_kind) "
        "VALUES ('scb','lisa','kon','scb','rams','syss','same_definition_different_column')"
    )
    src.execute(
        "INSERT INTO variable_state_lineage "
        "(consumer_state_id, source_state_id, valid_from, valid_to) VALUES (?, ?, ?, ?)",
        (kon_state, syss_state, "2018-01-01", "9999-12-31"),
    )
    src.execute(
        "INSERT INTO variable_state_lineage_warning "
        "(consumer_state_id, warning_kind, message) VALUES (?, ?, ?)",
        (kon_state, "no_source_state", "no source state for 2017"),
    )


def _seed_succession_chain(src: sqlite3.Connection) -> None:
    """Seed a SELF-CONTAINED succession chain of DEAD (renamed) slugs for the
    catalog 301-redirect tests, at BOTH grains the redirect walk supports:

    Binding grain (#355 PART 2):
        scb/lisa/renamed-head → scb/lisa/renamed-mid → scb/rams/syss

    Register grain (#412):
        scb/oldreg → scb/lisa

    The dead predecessors carry NO live row (no ``variable`` / ``register`` — exactly
    the renamed-slug case: citing them 404s). Each chain terminates at a LIVE,
    edge-free leaf so the redirect target itself resolves 200 when followed:
    ``scb/rams/syss`` (a succession *successor* of kon, added above, so it has no
    OUTBOUND binding edge) and the live ``scb/lisa`` register. A GET on a dead head
    must 301 to its terminal. Kept as its own helper (not folded into
    ``_seed_kon_edges``) so the existing predecessor/successor count assertions on
    kon/syss are untouched."""
    src.executemany(
        "INSERT INTO variable_replaced_by "
        "(predecessor_provider, predecessor_register, predecessor_variable, "
        "successor_provider, successor_register, successor_variable, note) "
        "VALUES (?,?,?,?,?,?,'auto:test')",
        [
            ("scb", "lisa", "renamed-head", "scb", "lisa", "renamed-mid"),
            ("scb", "lisa", "renamed-mid", "scb", "rams", "syss"),
        ],
    )
    src.execute(
        "INSERT INTO register_replaced_by "
        "(predecessor_provider, predecessor_register, "
        "successor_provider, successor_register, note) "
        "VALUES ('scb','oldreg','scb','lisa','auto:test')"
    )


def _seed_concept_groups(src: sqlite3.Connection, add_variable) -> None:
    """Seed #303 concept groups so the register / classification-root responses
    exercise the `groups` surface (grouped members ALSO stay in `children`):

    - a token month group `ink` on scb/rams over two added variables; and
    - a classification vintage group `sun` over sun2000 (added) + sun2020."""
    src.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (10, 'variable', 2, 'ink', 'Inkomst', 'token')"
    )
    for i, (slug, month, month_label) in enumerate(
        [("inkjan", "01", "januari"), ("inkfeb", "02", "februari")]
    ):
        add_variable(src, register_id=2, var_id=900 + i, name="Inkomst", slug=slug)
        vid = src.execute(
            "SELECT variable_id FROM variable WHERE register_id = 2 AND slug = ?",
            (slug,),
        ).fetchone()[0]
        src.execute(
            "INSERT INTO concept_group_variable (variable_id, group_id) VALUES (?, 10)",
            (vid,),
        )
        src.execute(
            "INSERT INTO concept_group_variable_facet (variable_id, axis, value, "
            "label) VALUES (?, 'month', ?, ?)",
            (vid, month, month_label),
        )
    src.execute(
        "INSERT INTO classification (id, short_name, name, slug) "
        "VALUES (50, 'SUN2000', 'Svensk utbildningsnomenklatur', 'sun2000')"
    )
    src.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (11, 'classification', NULL, 'sun', "
        "'Svensk utbildningsnomenklatur', 'token')"
    )
    src.executemany(
        "INSERT INTO concept_group_classification (classification_id, group_id, "
        "facet_value, facet_label) VALUES (?, 11, ?, ?)",
        [
            (50, "2000", "2000"),
            (
                src.execute(
                    "SELECT id FROM classification WHERE slug = 'sun2020'"
                ).fetchone()[0],
                "2020",
                "2020",
            ),
        ],
    )


@pytest.fixture
def catalog_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A slugged reg_meta DB for the catalog browse tests, pointed at via
    REG_META_DB. Resolves: ``scb`` (provider, 2 registers), ``scb/lisa``
    (register, binding ``kon`` + variants-ref), ``scb/lisa/kon`` (binding leaf,
    1 state w/ value set + a same_as edge), ``class`` (classification-root, 1
    classification), ``class/sun2020`` (classification leaf)."""
    db_path = tmp_path / reg_meta.db.DB_FILENAME
    _build_catalog_fixture_db(db_path)
    _point_app_at(monkeypatch, tmp_path)
    return db_path


def _build_docs_fixture_db(db_path: Path) -> None:
    """Build a minimal `reg_meta_docs.db` for the #354 docs-endpoint tests:
    two LISA docs (so register-scoping + register-coverage have content) with the
    FTS index rebuilt and the `schema_version` meta `open_doc_db` gates on."""
    from reg_meta_build.doc_db import DOC_DDL

    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(DOC_DDL)
        conn.executemany(
            "INSERT INTO doc (register, filename, variable, display_name, tags, "
            "source, body, body_clean) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "lisa",
                    "Kon.md",
                    "Kon",
                    "Kön",
                    json.dumps(["type/variable", "topic/demography"]),
                    "lisa-bakgrundsfakta-1990-2017",
                    "**Kön Kon**\n\nKönstillhörighet för individen.",
                    "Kön Kon Könstillhörighet för individen.",
                ),
                (
                    "lisa",
                    "Sysselsattning.md",
                    "SyssStat",
                    "Sysselsättningsstatus",
                    json.dumps(["type/variable", "topic/employment"]),
                    "lisa-bakgrundsfakta-1990-2017",
                    "**Sysselsättningsstatus SyssStat**\n\nIndividens ställning.",
                    "Sysselsättningsstatus SyssStat Individens ställning.",
                ),
            ],
        )
        conn.execute("INSERT INTO doc_fts(doc_fts) VALUES('rebuild')")
        conn.executemany(
            "INSERT INTO doc_meta(key, value) VALUES (?, ?)",
            [
                ("schema_version", reg_meta.doc_db.DOC_SCHEMA_VERSION),
                ("doc_count", "2"),
            ],
        )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def docs_db(catalog_db: Path) -> Path:
    """The catalog DB PLUS a `reg_meta_docs.db` in the same REG_META_DB dir, so
    the app boots the main catalog AND opens the optional docs index (#354).
    Returns the docs DB path. Tests that want the docs-ABSENT degradation use the
    plain ``catalog_db`` fixture (no docs DB written)."""
    docs_path = catalog_db.parent / reg_meta.doc_db.DOC_DB_FILENAME
    _build_docs_fixture_db(docs_path)
    return docs_path
