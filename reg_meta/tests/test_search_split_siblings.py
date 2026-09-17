"""Regression: value→variable search must attribute a value code only to the
split sibling(s) whose value set actually contains it.

The two explicitly resolved siblings share a native provider key, but have
separate code sets. Exercise the actual writer and the query join together: a
code must never fan out through the shared provider key to the other sibling.
Source identity formation has its own tests in reg_meta_build.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.db import open_db
from reg_meta.queries import search
from reg_meta_build.resolved_catalog import (
    ResolvedCodeSet,
    ResolvedRegister,
    ResolvedState,
    ResolvedVariable,
    ResolvedVariant,
    write_resolved_catalog,
)

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(scope="module")
def split_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    output = tmp_path_factory.mktemp("split_db") / "reg_meta.db"
    register = ResolvedRegister(provider="scb", slug="testreg", name="TESTREG")
    variant = ResolvedVariant(slug="individer", name="Individer")
    variables = tuple(
        ResolvedVariable(
            register=register,
            slug=column.lower(),
            provider_key="920",
            name=column,
            definition=None,
            description=None,
            operational_definition=None,
            measurement_unit=None,
            is_identifier=False,
            is_sensitive=False,
            states=(
                ResolvedState(
                    variant=variant,
                    valid_from="2019-01-01",
                    valid_to="2019-12-31",
                    delivery_column_name=column,
                    data_type="text",
                    data_length="4",
                    operational_definition=None,
                    provenance=None,
                    value_set=ResolvedCodeSet(members=((code, label),)),
                ),
            ),
        )
        for column, code, label in (
            ("Hemkommun", "0180", "Stockholms kommun"),
            ("Skolkommun", "1480", "Göteborgs kommun"),
        )
    )
    write_resolved_catalog(variables, output, manifest={})
    return output


def _slug_by_column(conn) -> dict[str, str]:
    """Map each var_id-920 split sibling's delivery column to its stored slug."""
    rows = conn.execute(
        "SELECT DISTINCT vs.delivery_column_name, v.slug "
        "FROM variable v JOIN variable_state vs ON vs.variable_id = v.variable_id "
        "JOIN register r ON r.register_id = v.register_id "
        "WHERE r.slug = 'testreg' AND v.provider_key = '920' "
        "AND vs.delivery_column_name IN ('Hemkommun', 'Skolkommun')"
    ).fetchall()
    return {r["delivery_column_name"]: r["slug"] for r in rows}


def _owner_slugs(hits: list) -> set[str]:
    """Variable slugs across every code hit's owning-variable annotations (#352).

    Each `type: "code"` hit (search field="value") carries owning variables under
    `variables` as `CodeOwnerVariable` models (#701); the variable slug is the
    binding FQID's last segment (`provider/register/variable`)."""
    slugs: set[str] = set()
    for hit in hits:
        for var in hit.variables:
            assert var.fqid, "owning variable should be FQID-addressable"
            slugs.add(str(var.fqid).split("/")[-1])
    return slugs


def test_value_search_attributes_code_to_owning_sibling_only(split_db: Path) -> None:
    conn = open_db(split_db)
    try:
        # Precondition: the writer retained two siblings with distinct slugs.
        slug_by_col = _slug_by_column(conn)
        assert set(slug_by_col) == {"Hemkommun", "Skolkommun"}, slug_by_col
        hem_slug, sko_slug = slug_by_col["Hemkommun"], slug_by_col["Skolkommun"]
        assert hem_slug != sko_slug

        # A code in ONLY the Hemkommun value set must resolve to ONLY that
        # sibling — not Skolkommun, which shares var_id 920 but not the code.
        hits = search(conn, "Stockholms kommun", field="value").results
        assert hits, "code should resolve to its owning variable"
        leaked = _owner_slugs(hits) - {hem_slug}
        assert not leaked, f"code leaked to non-owning sibling(s): {leaked}"

        # Symmetric check for the Skolkommun-only code.
        hits = search(conn, "Göteborgs kommun", field="value").results
        assert hits
        assert _owner_slugs(hits) == {sko_slug}
    finally:
        conn.close()
