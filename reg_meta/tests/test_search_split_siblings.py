"""Regression: value→variable search must attribute a value code only to the
split sibling(s) whose value set actually contains it.

A2.2 triage splits one source `var_id` into sibling `variable` rows that SHARE a
`provider_key` (#139). Before `code_variable_map` was re-grained to
`(code_id, variable_id)` (SCHEMA 5.1.0), the search joined on
`(register_id, provider_key)`, so a code fanned across EVERY sibling — including
ones whose value set excluded it (false positives). The fix carries each cvid's
owning `variable_id` via the coalescer's ground-truth stamp (#150).

Builds through the real build pipeline (the build helpers are on `sys.path` via
the reg_meta test conftest) so it exercises both the build-time population and
the query-time join end to end.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import (
    REGISTERINFORMATION_ROWS,
    VARDEMANGDER_ROWS,
    _var_row,
    write_scb_input,
)
from reg_meta.db import open_db
from reg_meta.queries import search
from reg_meta_build.db import build_db

if TYPE_CHECKING:
    from pathlib import Path

PIPE = "|"

# Hemkommun + Skolkommun under one var_id (920), same edition/year (the #139
# canonical split geometry) → two siblings sharing provider_key '920'.
_SPLIT_ROWS = [
    _var_row(colname="Hemkommun", cvid=9300, var_id=920, year="2019"),
    _var_row(colname="Skolkommun", cvid=9301, var_id=920, year="2019"),
]

# Disjoint value sets: '0180'/Stockholm only in the Hemkommun sibling,
# '1480'/Göteborg only in Skolkommun. Distinct version label keeps kod != version
# so the importer's `vardemangder_drift` guard (kod==version) stays quiet.
_SPLIT_VARDEMANGDER = [
    PIPE.join(["Kommun", "1", "0180", "Stockholms kommun", "9300", "7001"]),
    PIPE.join(["Kommun", "1", "1480", "Göteborgs kommun", "9301", "7002"]),
]


@pytest.fixture(scope="module")
def split_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    input_dir = tmp_path_factory.mktemp("split_input")
    db_dir = tmp_path_factory.mktemp("split_db")
    slug_dir = tmp_path_factory.mktemp("split_slugs")
    write_scb_input(
        input_dir,
        registerinformation_rows=REGISTERINFORMATION_ROWS + _SPLIT_ROWS,
        vardemangder_rows=list(VARDEMANGDER_ROWS) + _SPLIT_VARDEMANGDER,
    )
    (slug_dir / "scb.toml").write_text(
        '[register."1"]\nslug = "testreg"\n'
        '[register."2"]\nslug = "otherreg"\n'
        '[register_variant."1.10"]\nslug = "individer"\n'
        '[register_variant."2.20"]\nslug = "foretag"\n',
        encoding="utf-8",
    )
    (slug_dir / "classifications.toml").write_text("", encoding="utf-8")
    build_db(
        input_dir=input_dir,
        db_dir=db_dir,
        skip_classifications=True,
        slug_dir=slug_dir,
    )
    return db_dir / "reg_meta.db"


def _slug_by_column(conn) -> dict[str, str]:
    """Map each var_id-920 split sibling's delivery column to its stored slug."""
    rows = conn.execute(
        "SELECT DISTINCT vs.delivery_column_name, v.slug "
        "FROM variable v JOIN variable_state vs ON vs.variable_id = v.variable_id "
        "WHERE v.register_id = 1 AND v.provider_key = '920' "
        "AND vs.delivery_column_name IN ('Hemkommun', 'Skolkommun')"
    ).fetchall()
    return {r["delivery_column_name"]: r["slug"] for r in rows}


def _owner_slugs(hits: list) -> set[str]:
    """Variable slugs across every code hit's owning-variable annotations (#352).

    Each `type: "code"` hit (search field="value") carries owning variables under
    `variables` as `{fqid, name, register}`; the variable slug is the binding
    FQID's last segment (`provider/register/variable`)."""
    slugs: set[str] = set()
    for hit in hits:
        for var in hit["variables"]:
            assert var["fqid"], "owning variable should be FQID-addressable"
            slugs.add(var["fqid"].split("/")[-1])
    return slugs


def test_value_search_attributes_code_to_owning_sibling_only(split_db: Path) -> None:
    conn = open_db(split_db)
    try:
        # Precondition: the split fired into two siblings with distinct slugs.
        slug_by_col = _slug_by_column(conn)
        assert set(slug_by_col) == {"Hemkommun", "Skolkommun"}, slug_by_col
        hem_slug, sko_slug = slug_by_col["Hemkommun"], slug_by_col["Skolkommun"]
        assert hem_slug != sko_slug

        # A code in ONLY the Hemkommun value set must resolve to ONLY that
        # sibling — not Skolkommun, which shares var_id 920 but not the code.
        hits = search(conn, "Stockholms kommun", field="value")["results"]
        assert hits, "code should resolve to its owning variable"
        leaked = _owner_slugs(hits) - {hem_slug}
        assert not leaked, f"code leaked to non-owning sibling(s): {leaked}"

        # Symmetric check for the Skolkommun-only code.
        hits = search(conn, "Göteborgs kommun", field="value")["results"]
        assert hits
        assert _owner_slugs(hits) == {sko_slug}
    finally:
        conn.close()
