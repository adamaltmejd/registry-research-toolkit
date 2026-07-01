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

import hashlib
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
    # #567: a sibling variant carrying a COMPOSITE panel_time_key (UHT's
    # (year, quarter) coordinate → JSON array), so the variant endpoint also
    # exercises composite time-key serialization.
    add_variant(
        src, register_variant_id=21, register_id=2, slug="quarterly", name="Qtr"
    )
    src.execute(
        "UPDATE register_variant SET panel_entity_key = ?, panel_time_key = ?, "
        "panel_time_grain = ? WHERE register_variant_id = 21",
        ("peorgnr", json.dumps(["ar", "kvartal"]), "row"),
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
    _seed_classification_split_root(src)
    _seed_same_as_alias_to_grouped(src)
    _seed_code_variable_map(src)
    _seed_merged_family(src, add_variable, add_state)
    _seed_representation_group(src)
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
    icd10_id = src.execute(
        "INSERT INTO classification (short_name, name, slug) VALUES (?, ?, ?)",
        (
            "ICD-10-SE",
            "Internationell statistisk klassifikation av sjukdomar och "
            "relaterade hälsoproblem, svensk version (ICD-10-SE)",
            "icd-10-se",
        ),
    ).lastrowid
    # A CODE-SHAPED code (digit + len>=3) owned by ICD-10-SE so a code-shaped
    # query ('C12') surfaces its real motivating classification via
    # code-containment (#393 item 5). Its label is unique so existing code-search
    # assertions (which pin Man/Kvinna) are untouched.
    c12_code_id = src.execute(
        "INSERT INTO value_code (code, label, mapping_count) "
        "VALUES ('C12', 'Malign tumör i tungbas', 0)"
    ).lastrowid
    src.execute(
        "INSERT INTO classification_code (classification_id, code_id, level, is_valid) "
        "VALUES (?, ?, NULL, 1)",
        (icd10_id, c12_code_id),
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


def _seed_representation_group(src: sqlite3.Connection) -> None:
    """Seed a #819 REPRESENTATION-member concept group over the merged-family
    `scb/lisa/lonfink` variable: ONE variable, TWO members distinguished by
    `delivery_column_name` (LonFinkJan / LonFinkFeb) — i.e. two members sharing one
    FQID. Backs the search column-grain narrowing test (Fix 2): a steward holding only
    the LonFinkJan column still admits the `scb/lisa/lonfink` FQID, so the FQID-grain
    narrow keeps BOTH representation members; only the webapp's column-grain refinement
    drops the unheld LonFinkFeb representation.

    The group carries a distinctive label (`Lönefink månadsfamilj`) so a search on the
    LABEL folds it: two representation members share one variable, so they are NOT ≥2
    DISTINCT member variables (the member-hit fold trigger) — the label match is the
    reliable fold path for a single-variable representation family. Runs AFTER
    `_seed_merged_family` (which mints `lonfink` + its Jan/Feb/Mars alias columns)."""
    vid = src.execute(
        "SELECT variable_id FROM variable WHERE register_id = 1 AND slug = 'lonfink'"
    ).fetchone()[0]
    src.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (12, 'variable', 1, 'lonefink-rep', "
        "'Lönefink månadsfamilj', 'curated')"
    )
    src.execute(
        "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
        "VALUES (12, 'month', 0, 'månad')"
    )
    for col, value, label in (
        ("LonFinkJan", "01", "januari"),
        ("LonFinkFeb", "02", "februari"),
    ):
        cur = src.execute(
            "INSERT INTO concept_group_variable "
            "(group_id, variable_id, delivery_column_name) VALUES (12, ?, ?)",
            (vid, col),
        )
        src.execute(
            "INSERT INTO concept_group_variable_facet "
            "(member_id, axis, value, label) VALUES (?, 'month', ?, ?)",
            (cur.lastrowid, value, label),
        )


def _seed_kon_edges(src: sqlite3.Connection) -> None:
    """Seed the variable-grain edges + state-grain lineage the A5.2a-ii suffixed
    sub-endpoints read off the ``scb/lisa/kon`` binding, so their tests assert
    non-empty results (the leaf-embed tests only assert these fields are PRESENT,
    so adding rows is compatible):

    - ``variable_replaced_by``: kon → rams/syss (a succession edge, so
      ``/successors`` on kon and ``/predecessors`` on syss are non-empty).
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
        scb/oldreg → scb/lisa   (dead predecessor — the renamed-register 301 case)
        scb/rams   → scb/lisa   (LIVE predecessor — the #859 CHANGE-1 404-not-301 lock)

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
    src.executemany(
        "INSERT INTO register_replaced_by "
        "(predecessor_provider, predecessor_register, "
        "successor_provider, successor_register, note) "
        "VALUES (?,?,?,?,'auto:test')",
        [
            # Dead register → live `scb/lisa` (the #412 dead-register 301 case).
            ("scb", "oldreg", "scb", "lisa"),
            # LIVE register `scb/rams` → live `scb/lisa`: a succession edge between
            # two LIVE registers, mirroring the live-binding `kon → syss` edge. Lets
            # the steward test pin the CHANGE-1 fix — a LIVE unheld register with a
            # `register_replaced_by` edge to a HELD successor must 404, NOT 301.
            # `scb/lisa` has no outbound edge, so `scb/oldreg`'s terminal walk is
            # unaffected (still ends at `scb/lisa`).
            ("scb", "rams", "scb", "lisa"),
        ],
    )


def _seed_concept_groups(src: sqlite3.Connection, add_variable) -> None:
    """Seed #303 concept groups so the register / classification-root responses
    exercise the `groups` surface (grouped members ALSO stay in `children`):

    - a token month group `ink` on scb/rams over two added variables; and
    - a #516 classification umbrella group `sun` (AXIS-LESS — zero concept_group_axis rows,
      mirroring the real group:sun shape) over the terminal `sun2020` edition plus
      a standalone non-succession `niva-test` aggregate — both TERMINAL members so
      the classification-root's superseded-by drop keeps them. The members keep
      their own short facet `value`/`label` (the picker label) even though the
      umbrella carries no axis.

    Also seeds the sun1996 → sun2000 → sun2020 succession chain and projects
    `classification.supersedes_id` from it exactly as the build does (see
    `_project_supersedes_id`), so `list_classifications.superseded_by` is truthy
    on the two superseded editions — the read surface's terminal-only filter is
    therefore actually exercised (a NULL `supersedes_id` would make it a no-op)."""
    src.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (10, 'variable', 2, 'ink', 'Inkomst', 'token')"
    )
    src.execute(
        "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
        "VALUES (10, 'month', 0, 'månad')"
    )
    for i, (slug, month, month_label) in enumerate(
        [("inkjan", "01", "januari"), ("inkfeb", "02", "februari")]
    ):
        add_variable(src, register_id=2, var_id=900 + i, name="Inkomst", slug=slug)
        vid = src.execute(
            "SELECT variable_id FROM variable WHERE register_id = 2 AND slug = ?",
            (slug,),
        ).fetchone()[0]
        cur = src.execute(
            "INSERT INTO concept_group_variable "
            "(group_id, variable_id, delivery_column_name) VALUES (10, ?, NULL)",
            (vid,),
        )
        src.execute(
            "INSERT INTO concept_group_variable_facet "
            "(member_id, axis, value, label) VALUES (?, 'month', ?, ?)",
            (cur.lastrowid, month, month_label),
        )
    src.execute(
        "INSERT INTO classification (id, short_name, name, slug) "
        "VALUES (50, 'SUN2000', 'Svensk utbildningsnomenklatur', 'sun2000')"
    )
    # A standalone, NON-succession aggregate classification (the fixture analogue of
    # the real niva-oldv1 / grov nivå aggregates): no predecessor edge, so its
    # `supersedes_id` stays NULL and `superseded_by` stays empty → it's terminal and
    # survives the classification-root's superseded-by drop.
    src.execute(
        "INSERT INTO classification (id, short_name, name, slug) "
        "VALUES (51, 'NIVA', 'Utbildningsnivå – aggregat', 'niva-test')"
    )
    # #516 umbrella group `sun` over its distinct classifications (AXIS-LESS —
    # `facet_axis` NULL, mirroring the real group:sun). Members are TERMINAL
    # classifications only — the current `sun2020` edition + the version-independent
    # `niva-test` aggregate — so the classification-root's superseded-by filter keeps
    # them. sun2000 is NOT a member: it's purely a superseded succession edition now
    # (reached via the leaf's edition-chain panel, not the umbrella fold). Each member
    # keeps its own short facet value/label despite the absent group axis.
    src.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (11, 'classification', NULL, 'sun', "
        "'Svensk utbildningsnomenklatur', 'curated')"
    )
    src.executemany(
        "INSERT INTO concept_group_classification (classification_id, group_id, "
        "facet_value, facet_label) VALUES (?, 11, ?, ?)",
        [
            (
                src.execute(
                    "SELECT id FROM classification WHERE slug = 'sun2020'"
                ).fetchone()[0],
                "niva",
                "Utbildningsnivå",
            ),
            (51, "aggregat", "Aggregat"),
        ],
    )
    # #571: a classification SUCCESSION chain sun1996 → sun2000 → sun2020 (distinct
    # from the umbrella concept-group above — that's a presentation fold, this is the
    # edition timeline the leaf node embeds as `edition_chain`). All three are LIVE
    # `classification` rows — the build validator forbids succession edges to dead
    # slugs (validate.py, the classification_replaced_by check), so the fixture
    # mirrors that invariant; sun2020 is the terminal. Exercises the full-chain walk
    # through the real `/api/catalog/class/sun2020` route.
    src.execute(
        "INSERT INTO classification (id, short_name, name, slug) "
        "VALUES (49, 'SUN1996', 'Svensk utbildningsnomenklatur', 'sun1996')"
    )
    src.executemany(
        "INSERT INTO classification_replaced_by "
        "(predecessor_slug, successor_slug, effective_year, note) "
        "VALUES (?, ?, ?, 'derived:test')",
        [
            ("sun1996", "sun2000", 2000),
            ("sun2000", "sun2020", 2020),
        ],
    )
    # Project `classification.supersedes_id` from the edges, mirroring the build's
    # `_project_supersedes_id`: each successor points back at its predecessor. This
    # is what makes `list_classifications.superseded_by` (a GROUP_CONCAT over
    # `supersedes_id`) truthy on sun1996 and sun2000, so the classification-root's
    # terminal-only filter is genuinely exercised rather than a no-op on NULLs.
    for predecessor, successor in (("sun1996", "sun2000"), ("sun2000", "sun2020")):
        src.execute(
            "UPDATE classification SET supersedes_id = "
            "(SELECT id FROM classification WHERE slug = ?) WHERE slug = ?",
            (predecessor, successor),
        )


def _seed_classification_split_root(src: sqlite3.Connection) -> None:
    """#605 / #579: a 1→many classification succession SPLIT — a `sni` root fans out
    into three distinct dimensions, each with its own 2000→2020 edition:

        sni-root1996 → {sni-grp2000, sni-ink2000, sni-niv2000}
        sni-<dim>2000 → sni-<dim>2020   (the three branch tips)

    A DISTINCT root from the linear sun1996→sun2000→sun2020 chain in
    `_seed_concept_groups`, kept separate so the linear `class/sun2020` test stays a
    clean 3-edition chain. Browsing the split root
    (`/api/catalog/class/sni-root1996`) must embed ALL three branches in
    `edition_chain`, with the three 2020 tips all `is_current`. All editions are LIVE
    rows (the build validator forbids dead succession endpoints)."""
    src.execute(
        "INSERT INTO classification (id, short_name, name, slug) "
        "VALUES (100, 'SNI-ROOT1996', 'SNI root 1996', 'sni-root1996')"
    )
    next_id = 101
    for stem in ("grp", "ink", "niv"):
        for vintage in ("2000", "2020"):
            src.execute(
                "INSERT INTO classification (id, short_name, name, slug) "
                "VALUES (?, ?, ?, ?)",
                (
                    next_id,
                    f"SNI-{stem.upper()}{vintage}",
                    f"SNI {stem} {vintage}",
                    f"sni-{stem}{vintage}",
                ),
            )
            next_id += 1
    src.executemany(
        "INSERT INTO classification_replaced_by "
        "(predecessor_slug, successor_slug, effective_year, note) "
        "VALUES (?, ?, ?, 'derived:test')",
        [
            *(
                ("sni-root1996", f"sni-{stem}2000", 2000)
                for stem in ("grp", "ink", "niv")
            ),
            *(
                (f"sni-{stem}2000", f"sni-{stem}2020", 2020)
                for stem in ("grp", "ink", "niv")
            ),
        ],
    )


def _seed_same_as_alias_to_grouped(src: sqlite3.Connection) -> None:
    """#489 P2-A guard: a curated `variable_same_as` edge from a phantom lisa slug
    (`scb/lisa/inkjan-alias`, no live `variable` row) to the grouped target
    `scb/rams/inkjan`. Querying the alias resolves THROUGH same_as to inkjan, so
    `/dimensions` must cite the TARGET register's `ink` group — the regression the
    old register/fqid-from-the-request handler returned `[]` for. Runs AFTER
    `_seed_concept_groups` (which mints inkjan)."""
    for a, b in (
        (("scb", "lisa", "inkjan-alias"), ("scb", "rams", "inkjan")),
        (("scb", "rams", "inkjan"), ("scb", "lisa", "inkjan-alias")),
    ):
        src.execute(
            "INSERT INTO variable_same_as "
            "(a_provider, a_register, a_variable, b_provider, b_register, b_variable) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (*a, *b),
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

    related_pdf = b"%PDF-1.4\n% related document fixture\n%%EOF\n"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(DOC_DDL)
        conn.executemany(
            "INSERT INTO doc (register, filename, variable, display_name, tags, "
            "source, source_url, source_title, body, body_clean) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                # Kon carries a resolved source_url/source_title (#372 curated map
                # applied at doc-DB build); SyssStat leaves them NULL (uncurated)
                # so both the populated and unmapped wire shapes are covered.
                (
                    "lisa",
                    "Kon.md",
                    "Kon",
                    "Kön",
                    json.dumps(["type/variable", "topic/demography"]),
                    "lisa-bakgrundsfakta-1990-2017",
                    "https://www.scb.se/contentassets/"
                    "0521204f13e649299dec73f091e691e0/"
                    "lisa-bakgrundsfakta-1990-2017.pdf",
                    "LISA bakgrundsfakta 1990-2017",
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
                    None,
                    None,
                    "**Sysselsättningsstatus SyssStat**\n\nIndividens ställning.",
                    "Sysselsättningsstatus SyssStat Individens ställning.",
                ),
            ],
        )
        conn.execute(
            "INSERT INTO related_document ("
            "register, title, filename, source_url, license, fetched, sha256, "
            "byte_size, content"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "lisa",
                "LISA register documentation",
                "lisa_related.pdf",
                "https://www.scb.se/lisa-related",
                "CC BY 4.0",
                "2026-06-01",
                hashlib.sha256(related_pdf).hexdigest(),
                len(related_pdf),
                related_pdf,
            ),
        )
        conn.execute("INSERT INTO doc_fts(doc_fts) VALUES('rebuild')")
        conn.executemany(
            "INSERT INTO doc_meta(key, value) VALUES (?, ?)",
            [
                ("schema_version", reg_meta.doc_db.DOC_SCHEMA_VERSION),
                ("doc_count", "2"),
                ("related_document_count", "1"),
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
