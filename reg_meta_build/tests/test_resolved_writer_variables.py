"""The resolved writer stores a variable's source-register attribution as given.

The rest of this file moved to the build cases (`cases/build/README.md`). This leg
stays until the build case `lineage-edges-need-accepted-identity-and-overlapping-states`
(the catalog-lineage port) projects the attributed `variables.source_register`,
`source_label` and `source_register_text`; delete it when that case lands.
"""

from __future__ import annotations

from contextlib import closing
from typing import TYPE_CHECKING

from _resolved_catalog_support import resolved_variable as _variable
from catalog_manifest import synthetic_manifest
from reg_meta_build.db import open_built_db
from reg_meta_build.resolved_catalog import ResolvedRegister, write_resolved_catalog

if TYPE_CHECKING:
    from pathlib import Path


def test_source_register_attribution_is_written_as_resolved(tmp_path: Path) -> None:
    # Fails if the writer drops or re-derives a variable's attributed source register,
    # its label or its source texts instead of storing the resolved values.
    variable = _variable()
    source = ResolvedRegister(provider="sos", slug="source", name="Source registry")
    variable = variable.model_copy(
        update={
            "source_register": source,
            "source_register_text": "Source as supplied",
            "source_label": "Resolved source label",
            "states": tuple(
                state.model_copy(update={"source_register_text": "State source"})
                for state in variable.states
            ),
        }
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog((variable,), output, manifest=synthetic_manifest())
    with closing(open_built_db(output)) as conn:
        row = conn.execute(
            "SELECT r.slug, v.source_register_text, v.source_label FROM variable v "
            "JOIN register r ON r.register_id = v.source_register_id"
        ).fetchone()
        assert tuple(row) == ("source", "Source as supplied", "Resolved source label")
        assert [
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT source_register_text FROM variable_state"
            )
        ] == ["State source"]
