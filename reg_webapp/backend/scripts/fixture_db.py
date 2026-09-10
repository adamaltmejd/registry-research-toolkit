"""Build the deterministic synthetic reg_meta DB pair the backend serves without a
released catalog.

Two consumers, one builder:

- ``reg_webapp/backend/tests/conftest.py`` (the ``catalog_db`` / ``docs_db``
  fixtures) — CI has no real reg_meta asset, so the backend tests point the app at
  this pair via ``REG_META_DB``;
- ``dev.sh --fixture-db`` — the same pair, built into a temp dir, so ``smoke`` /
  ``shot`` / plain serve render a populated catalog inside a container where no
  released DB is reachable.

It lives in ``scripts/`` (not ``src/reg_webapp/``) because it needs the repo
checkout: the catalog builder rides on ``reg_meta_build``'s ``_slugged_db`` test
helper, and ``reg_meta_build`` is not a reg_webapp runtime dependency.

The content is fixed (no randomness, no clock): the same interpreter builds a
byte-identical pair on every run, which is what makes a screenshot diff meaningful.

Usage:
    uv run python reg_webapp/backend/scripts/fixture_db.py <dir>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import reg_meta.db
import reg_meta.doc_db

_SLUGGED_DB_DIR = Path(__file__).resolve().parents[3] / "reg_meta_build" / "tests"


def ensure_slugged_db_importable() -> None:
    """Put ``reg_meta_build/tests`` on sys.path so the bare-name ``_slugged_db``
    helper imports (mirrors reg_meta/tests/conftest.py). Idempotent."""
    if str(_SLUGGED_DB_DIR) not in sys.path:
        sys.path.insert(0, str(_SLUGGED_DB_DIR))


FIXTURE_IMPORT_DATE = "2026-06-01T00:00:00Z"

# A schema_version that PASSES open_db's gate (same major.minor) but differs from
# the code constant in the PATCH (_check_schema_compat ignores patch) — so
# test_context proves /api/context surfaces the manifest's value, not an echo of
# reg_meta.SCHEMA_VERSION.
_MAJOR, _MINOR, _ = reg_meta.db.SCHEMA_VERSION.split(".")
FIXTURE_SCHEMA_VERSION = f"{_MAJOR}.{_MINOR}.999"


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


def build_catalog_fixture_db(db_path: Path) -> None:
    """Build a slugged catalog DB on disk for the ``/api/catalog`` tests.

    Uses ``reg_meta_build``'s ``_slugged_db`` builder: the default
    ``scb/lisa/kon`` binding (with one state) plus a value-set on it, a second
    register ``scb/rams`` with its own binding, and a ``variable_same_as`` edge
    so the embedded leaf carries a non-empty ``same_as``. Then copies the
    in-memory DB to ``db_path`` and stamps the manifest."""
    ensure_slugged_db_importable()
    from _slugged_db import (
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
    src.execute(
        "UPDATE register_version SET "
        "registerversionbeskrivning = ?, "
        "registerversionmatinformation = ? "
        "WHERE regver_id = 200",
        ("RAMS 2019 description", "RAMS measurement information"),
    )
    src.execute(
        "INSERT INTO population (regver_id, name, definition, comment, date_range) "
        "VALUES (200, 'Employees', 'People with employment income', "
        "'Fixture population note', '2019')"
    )
    src.execute(
        "INSERT INTO object_type (regver_id, name, definition) "
        "VALUES (200, 'Person', 'Individual worker')"
    )
    add_variable(src, register_id=2, var_id=77, name="Sysselsättning", slug="syss")
    add_state(
        src,
        register_id=2,
        variable_slug="syss",
        register_variant_id=20,
        delivery_column_name="Syss",
    )
    # Y-82: a variable ONLY the `quarterly` variant delivers, so `scb/rams` is a
    # register whose variables arrive from two variants — the shape the register
    # page's variant filter chips exist for. Without it no fixture route renders
    # them (single-variant registers deliberately show none).
    add_variable(
        src, register_id=2, var_id=78, name="Sysselsättning kvartal", slug="syss-kv"
    )
    add_state(
        src,
        register_id=2,
        variable_slug="syss-kv",
        register_variant_id=21,
        valid_from="2005-01-01",
        delivery_column_name="SyssKv",
    )
    # A curated same_as edge kon→syss so the kon leaf embeds a same_as ref.
    src.execute(
        "INSERT INTO variable_same_as "
        "(a_provider, a_register, a_variable, b_provider, b_register, b_variable) "
        "VALUES ('scb','lisa','kon','scb','rams','syss')"
    )
    _seed_first_provider_register(src, add_register, add_variant, add_version)
    _seed_tags(src)
    _seed_kon_edges(src)
    _seed_succession_chain(src)
    _seed_concept_groups(src, add_variable)
    _seed_classification_split_root(src)
    _seed_same_as_alias_to_grouped(src)
    _seed_code_variable_map(src)
    _seed_merged_family(src, add_variable, add_state)
    _seed_representation_group(src)
    _seed_many_state_binding(src, add_variable, add_state, add_value_set)
    _rebuild_fts(src)
    _stamp_manifest(src)

    dst = sqlite3.connect(db_path)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()


def _seed_first_provider_register(
    src: sqlite3.Connection, add_register, add_variant, add_version
) -> None:
    """Give the FIRST slug-ordered provider a register, so drilling the catalog
    from the top lands on content instead of an empty provider.

    ``seed_providers`` inserts all eight agencies, but only ``scb`` carried
    registers — and ``fk`` sorts first, so ``/catalog`` → first provider → …
    (what `dev.sh smoke` walks, and the shortest browse path a reviewer takes)
    dead-ends on an empty node. One curated FK register with one binding fixes
    that at the top of the list; everything is a NEW subtree, so no existing
    fixture row changes meaning.

    The binding's state is open-ended from 2018 (same window as ``scb/lisa/kon``)
    so the leaf's period form narrows on any recent year."""
    from reg_meta_build.db import PROVIDER_ID_FK

    add_register(
        src, register_id=3, slug="midas", name="MiDAS", provider_id=PROVIDER_ID_FK
    )
    add_variant(
        src, register_variant_id=30, register_id=3, slug="standard", name="Standard"
    )
    add_version(src, regver_id=300, register_variant_id=30, name="2019")
    src.execute(
        "INSERT INTO variable (register_id, provider_key, name, slug) "
        "VALUES (3, CAST(88 AS TEXT), 'Sjukpenningdagar', 'sjukpenningdagar')"
    )
    vid = src.execute(
        "SELECT variable_id FROM variable WHERE register_id = 3 AND slug = ?",
        ("sjukpenningdagar",),
    ).fetchone()[0]
    src.execute(
        "INSERT INTO variable_alias "
        "(variable_id, register_variant_id, delivery_column_name) "
        "VALUES (?, 30, 'SjukpenningDagar')",
        (vid,),
    )
    src.execute(
        "INSERT INTO variable_state (variable_id, register_variant_id, valid_from, "
        "valid_to, data_type, delivery_column_name) "
        "VALUES (?, 30, '2018-01-01', '9999-12-31', 'int', 'SjukpenningDagar')",
        (vid,),
    )


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


def _seed_tags(src: sqlite3.Connection) -> None:
    """Seed thematic tags so catalog routes exercise the #311 consumption path."""
    from reg_meta_build.tags import (
        CuratedTag,
        TagMember,
        materialize_tags,
    )

    materialize_tags(
        src,
        (
            CuratedTag(
                slug="income",
                label="Income & earnings",
                description="Income measures and related recommendations.",
                members=(
                    TagMember(
                        "scb",
                        "lisa",
                        "kon",
                        rank=0,
                        starred=True,
                        note="fixture recommendation",
                    ),
                    TagMember("scb", "lisa", None, rank=1, starred=False, note=None),
                ),
            ),
            CuratedTag(
                slug="employment",
                label="Employment",
                description=None,
                members=(
                    TagMember("scb", "rams", None, rank=0, starred=False, note=None),
                ),
            ),
        ),
        providers=frozenset({"scb"}),
    )


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


# The topic the Y-18 ranking fixture puts in a register purpose, a variable
# name/definition AND the head of six incidental value-code labels. INVENTED text
# for that fixture, not a claim about any real register's terms.
_TOPIC = "covid test testing provtagning"


def seed_topical_rows(src: sqlite3.Connection) -> None:
    """Layer the Y-18 topical ranking scenario onto a built catalog fixture.

    Deliberately NOT called by `build_catalog_fixture_db`: only the backend's
    `topical_catalog_db` fixture seeds it, so the pair `dev.sh --fixture-db`
    builds (and the UI gates screenshot) stays byte-identical. Reproduces a
    researcher's topical search where the intended register/variable competes
    with value codes whose LABELS merely begin with the same term.
    """
    ensure_slugged_db_importable()
    from _slugged_db import add_state, add_value_set, add_variable

    variant_id = 20  # scb/rams `standard`
    value_set_id = 2
    delivery_column = "CovidAnalys04"

    # The register carries the topic in its PURPOSE and the variable in its
    # NAME + DEFINITION — the "present definition/name match" the codes displace.
    src.execute(
        "UPDATE register SET purpose = ? WHERE slug = 'rams'",
        (f"Sysselsättning samt {_TOPIC} på arbetsmarknaden.",),
    )
    add_variable(
        src, register_id=2, var_id=78, name="Antal covid analyser", slug="covidanalys"
    )
    src.execute(
        "UPDATE variable SET definition = ? WHERE slug = 'covidanalys'",
        (f"Antal analyser per månad: {_TOPIC}.",),
    )
    add_state(
        src,
        register_id=2,
        variable_slug="covidanalys",
        register_variant_id=variant_id,
        delivery_column_name=delivery_column,
        value_set_id=value_set_id,
    )
    variable_id = src.execute(
        "SELECT variable_id FROM variable WHERE slug = 'covidanalys'"
    ).fetchone()[0]
    # `variable_fts`'s delivery_column_names (and the search result's own chips)
    # read `variable_alias`, so the exact-identifier control needs the alias row.
    src.execute(
        "INSERT INTO variable_alias "
        "(variable_id, register_variant_id, delivery_column_name) VALUES (?, ?, ?)",
        (variable_id, variant_id, delivery_column),
    )

    # Two incidental code sets whose LABELS begin with the topic, one per value
    # group: a `classification_code` row makes a code classification-owned, a bare
    # `mapping_count > 0` makes it register-local (same split as
    # `_seed_code_variable_map`).
    icd10_id = src.execute(
        "SELECT id FROM classification WHERE slug = 'icd-10-se'"
    ).fetchone()[0]
    for ordinal, code in enumerate(("C900", "C901", "C902")):
        code_id = src.execute(
            "INSERT INTO value_code (code, label, mapping_count) VALUES (?, ?, 0)",
            (code, f"{_TOPIC} incidental klassifikationsetikett {ordinal}"),
        ).lastrowid
        src.execute(
            "INSERT INTO classification_code "
            "(classification_id, code_id, level, is_valid) VALUES (?, ?, NULL, 1)",
            (icd10_id, code_id),
        )
    add_value_set(
        src,
        value_set_id=value_set_id,
        codes=[
            (code, f"{_TOPIC} incidental värdemängdsetikett {ordinal}")
            for ordinal, code in enumerate(("L900", "L901", "L902"))
        ],
    )
    src.execute(
        "INSERT INTO code_variable_map (code_id, variable_id) "
        "SELECT code_id, ? FROM value_set_member WHERE value_set_id = ?",
        (variable_id, value_set_id),
    )
    src.execute(
        "UPDATE value_code SET mapping_count = ("
        "SELECT COUNT(*) FROM code_variable_map WHERE code_id = value_code.code_id)"
    )
    _rebuild_fts(src)


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


# The synthetic many-state binding (Y-46). Its point is CARDINALITY: many states
# over a handful of large SHARED codings, which is what makes an initial binding
# payload that embeds every state's members explode (the real
# `scb/rtb/forsamling` shipped 24 MB of mostly-repeated codes). Kept as ids so the
# seeder and its tests name the same sets.
_PARISH_VALUE_SETS = {
    # (value_set_id, code count, label prefix) — 900/901 share a VERSION LABEL and
    # differ only by content, so the leaf has to disambiguate them by span.
    900: (400, "Församling"),
    901: (400, "Socken"),
    902: (600, "Distrikt"),
}
# A coding with no members at all, and a dense integer one — the two shapes that
# render as something OTHER than a code table.
_EMPTY_VALUE_SET = 903
_AGE_VALUE_SET = 904


def _parish_codes(value_set_id: int) -> list[tuple[str, str]]:
    count, prefix = _PARISH_VALUE_SETS[value_set_id]
    # `value_code` is UNIQUE on (code, label), so each set gets its own namespace.
    return [
        (f"{value_set_id}{n:04d}", f"{prefix} {n:04d}") for n in range(1, count + 1)
    ]


def _seed_many_state_binding(
    src: sqlite3.Connection, add_variable, add_state, add_value_set
) -> None:
    """Seed `scb/lisa/forsamling`: 72 yearly states sharing five codings (Y-46).

    The shape the ticket is about — a long history whose states repeat a few
    large code sets — plus the edge shapes a code panel has to render: a coding
    with NO members, a state with NO coding at all, a dense integer coding, two
    distinct codings under ONE version label, and stored classification
    conformance mismatches (one severed, and two kept over the two codings ONE
    classification era spans) whose code lists are read per state rather than
    embedded."""
    add_variable(
        src,
        register_id=1,
        var_id=960,
        name="Församling",
        slug="forsamling",
        operational_definition="Parish of registration at year end.",
    )
    for value_set_id in _PARISH_VALUE_SETS:
        add_value_set(src, value_set_id=value_set_id, codes=_parish_codes(value_set_id))
    add_value_set(src, value_set_id=_EMPTY_VALUE_SET, codes=[])
    add_value_set(
        src,
        value_set_id=_AGE_VALUE_SET,
        codes=[(str(age), f"{age} år") for age in range(111)],
    )

    sun2020 = src.execute(
        "SELECT id FROM classification WHERE slug = 'sun2020'"
    ).fetchone()[0]
    # (first year, last year, value_set_id, version label, classification_id)
    eras = [
        (1952, 1990, 900, "Församling historisk", None),
        (1991, 2005, 901, "Församling historisk", None),
        (2006, 2019, 902, "Församling 2006", sun2020),
        (2020, 2021, _EMPTY_VALUE_SET, "Församling tom", None),
        (2022, 2023, None, "Fritext", None),
    ]
    states: dict[int, int] = {}
    for first, last, value_set_id, label, classification_id in eras:
        for year in range(first, last + 1):
            states[year] = add_state(
                src,
                register_id=1,
                variable_slug="forsamling",
                register_variant_id=10,
                valid_from=f"{year}-01-01",
                valid_to=f"{year}-12-31",
                data_type="char",
                delivery_column_name="Forsamling",
                value_set_id=value_set_id,
                value_set_version_label=label,
                classification_id=classification_id,
            )
    # A dense integer coding on its own state, so the leaf has a coding that
    # renders as a RANGE rather than a 111-row table.
    add_state(
        src,
        register_id=1,
        variable_slug="forsamling",
        register_variant_id=10,
        valid_from="1930-01-01",
        valid_to="1951-12-31",
        data_type="int",
        delivery_column_name="ForsamlingKod",
        value_set_id=_AGE_VALUE_SET,
        value_set_version_label="Kodnummer",
    )

    # A co-delivered column over an OLDER coding inside the SAME classification
    # era: what makes an edition's collapsed row carry TWO stored mismatch lists
    # (one per coding). Each has to stay separately reachable — collapsing the row
    # must not collapse the evidence.
    parallel_state = add_state(
        src,
        register_id=1,
        variable_slug="forsamling",
        register_variant_id=10,
        valid_from="2006-01-01",
        valid_to="2019-12-31",
        data_type="char",
        delivery_column_name="ForsamlingHist",
        value_set_id=900,
        value_set_version_label="Församling historisk",
        classification_id=sun2020,
    )

    # Stored conformance: one SEVERED verdict on a plain coding and two KEPT
    # verdicts on the classification-tagged era's two codings. Their mismatch code
    # lists live in `classification_conformance_code` and are read per state on
    # demand.
    _seed_conformance(
        src,
        state_id=states[1995],
        classification_id=sun2020,
        status="severed",
        checked=400,
        matched=16,
        codes=[c for c, _ in _parish_codes(901)[:5]],
    )
    _seed_conformance(
        src,
        state_id=states[2010],
        classification_id=sun2020,
        status="kept",
        checked=600,
        matched=597,
        codes=[c for c, _ in _parish_codes(902)[:3]],
    )
    _seed_conformance(
        src,
        state_id=parallel_state,
        classification_id=sun2020,
        status="kept",
        checked=400,
        matched=396,
        codes=[c for c, _ in _parish_codes(900)[:4]],
    )


def _seed_conformance(
    src: sqlite3.Connection,
    *,
    state_id: int,
    classification_id: int,
    status: str,
    checked: int,
    matched: int,
    codes: list[str],
) -> None:
    src.execute(
        "INSERT INTO classification_conformance (state_id, "
        "declared_classification_id, status, checked_code_count, "
        "matched_code_count, nonconforming_code_count, overlap) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            state_id,
            classification_id,
            status,
            checked,
            matched,
            len(codes),
            matched / checked,
        ),
    )
    src.executemany(
        "INSERT INTO classification_conformance_code (state_id, code_id) "
        "SELECT ?, code_id FROM value_code WHERE code = ?",
        [(state_id, code) for code in codes],
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


def build_docs_fixture_db(db_path: Path) -> None:
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


def build_fixture_db_dir(db_dir: Path) -> Path:
    """Build BOTH fixture DBs into ``db_dir`` — the shape ``REG_META_DB`` points at.

    The catalog DB is what the app boots on; the docs DB is optional at boot but
    `/doc/<identifier>` renders an empty state without it, so `--fixture-db` always
    writes the pair."""
    db_dir.mkdir(parents=True, exist_ok=True)
    build_catalog_fixture_db(db_dir / reg_meta.db.DB_FILENAME)
    build_docs_fixture_db(db_dir / reg_meta.doc_db.DOC_DB_FILENAME)
    return db_dir


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("db_dir", type=Path, help="directory to write the DB pair into")
    args = parser.parse_args()
    print(build_fixture_db_dir(args.db_dir))


if __name__ == "__main__":
    main()
