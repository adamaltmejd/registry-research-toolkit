"""Resolved-writer data-warning behavior that no build reaches.

Every warning a build publishes goes through `write_resolved_catalog`; its owner,
state, text and refs are pinned at the artifact by the build case
`warning-attaches-only-to-witnessed-owners-per-register`. A build mints each
warning's identity itself, so the writer's identity check is reached only by a
direct writer call.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _resolved_catalog_support import resolved_variable as _variable
from catalog_manifest import synthetic_manifest
from pydantic import ValidationError
from reg_meta_build.materialize import write_resolved_catalog

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
