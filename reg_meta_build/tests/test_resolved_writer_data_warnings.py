"""Resolved-writer data-warning behavior that no build reaches.

Every warning a build publishes goes through `write_resolved_catalog`; its owner,
state, text and refs are pinned at the artifact by the build case
`warning-attaches-only-to-witnessed-owners-per-register`. A build names only the
variables it formed and mints each warning's identity itself, so the writer's
demotion of a warning for an unwritten variable, its refusal of one for an
unwritten register or variant, and its identity check are reached only by a direct
writer call.
"""

from __future__ import annotations

from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from _resolved_catalog_support import resolved_variable as _variable
from catalog_manifest import synthetic_manifest
from pydantic import ValidationError
from reg_meta_build.db import open_built_db
from reg_meta_build.resolved_catalog import (
    write_resolved_catalog,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_warning_with_a_changed_payload_under_its_old_identity_is_refused(
    tmp_path: Path,
):
    # Fails if the resolved writer stops re-validating each warning's identity
    # against its complete content, so a payload changed after its warning_id was
    # minted is published. A JSON-contract guard at the write boundary: the
    # warning model refuses it, so it stays a validation error, not a located code.
    import json
    from pathlib import Path

    from reg_meta_build.data_warnings import DataWarning
    from reg_meta_build.source_evidence import canonical_sha256

    fixture = DataWarning.model_validate_json(
        (Path(__file__).parent / "cases/holdings/warning/warning.json").read_text()
    )
    payload = fixture.model_dump(mode="json", exclude={"warning_id"})
    payload.update(register_fqid="scb/example")
    written = DataWarning.model_validate_json(
        json.dumps({"warning_id": canonical_sha256(payload), **payload})
    )
    output = tmp_path / "reg_meta.db"
    with pytest.raises(ValidationError, match="identity"):
        write_resolved_catalog(
            (_variable(),),
            output,
            manifest=synthetic_manifest(),
            data_warnings=(written.model_copy(update={"detail": "Changed"}),),
        )
    assert not output.exists()


def test_warning_naming_an_unwritten_variable_is_demoted_to_its_register(
    tmp_path: Path,
):
    # Fails if the resolved writer refuses a warning whose variable it did not
    # write, or stores it still scoped to that variable or under its old identity,
    # instead of demoting it to its register.
    import json
    from pathlib import Path

    from reg_meta_build.data_warnings import DataWarning
    from reg_meta_build.source_evidence import canonical_sha256

    fixture = DataWarning.model_validate_json(
        (Path(__file__).parent / "cases/holdings/warning/warning.json").read_text()
    )
    payload = fixture.model_dump(mode="json", exclude={"warning_id"})
    payload.update(
        register_fqid="scb/example",
        variable_fqid="scb/example/unwritten",
        variant="_default",
        delivery_column_name="X",
    )
    missing = DataWarning.model_validate_json(
        json.dumps({"warning_id": canonical_sha256(payload), **payload})
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(
        (_variable(),), output, manifest=synthetic_manifest(), data_warnings=(missing,)
    )
    with closing(open_built_db(output)) as conn:
        (raw,) = conn.execute("SELECT warning_json FROM data_warning").fetchone()
    demoted = DataWarning.model_validate_json(raw)
    assert demoted.register_fqid == missing.register_fqid
    assert (
        demoted.variable_fqid,
        demoted.variant,
        demoted.delivery_column_name,
    ) == (None, None, None)
    assert demoted.warning_id != missing.warning_id


@pytest.mark.parametrize(
    ("scope", "code"),
    [
        ({"register_fqid": "scb/unwritten"}, "data_warning_register_unwritten"),
        (
            {
                "register_fqid": "scb/example",
                "variable_fqid": "scb/example/ampoltyp",
                "variant": "unwritten",
            },
            "data_warning_variant_unwritten",
        ),
    ],
    ids=["register", "variant"],
)
def test_warning_naming_an_unwritten_register_or_variant_is_refused_located(
    tmp_path: Path, scope: dict[str, str], code: str
):
    # Fails if the resolved writer publishes a warning whose register or variant
    # it did not write, demotes it instead of refusing, or refuses it without the
    # located EXIT_CONFIG code naming the warning and its curation case, or leaves
    # an output behind.
    import json
    from pathlib import Path

    from reg_meta_build.data_warnings import DataWarning
    from reg_meta_build.errors import EXIT_CONFIG, RegMetaError
    from reg_meta_build.source_evidence import canonical_sha256

    fixture = DataWarning.model_validate_json(
        (Path(__file__).parent / "cases/holdings/warning/warning.json").read_text()
    )
    payload = fixture.model_dump(mode="json", exclude={"warning_id"})
    payload.update(scope)
    unwritten = DataWarning.model_validate_json(
        json.dumps({"warning_id": canonical_sha256(payload), **payload})
    )
    output = tmp_path / "reg_meta.db"
    with pytest.raises(RegMetaError) as refused:
        write_resolved_catalog(
            (_variable(),),
            output,
            manifest=synthetic_manifest(),
            data_warnings=(unwritten,),
        )
    assert (refused.value.exit_code, refused.value.code) == (EXIT_CONFIG, code)
    assert unwritten.warning_id in refused.value.message
    assert f"case {fixture.case_id!r}" in refused.value.message
    assert not output.exists()
