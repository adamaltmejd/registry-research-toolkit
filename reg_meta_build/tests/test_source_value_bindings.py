"""Prepared value sources refuse an item join whose validity source is absent."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta_build.prepared_values import prepare_source_values
from reg_meta_build.source_evidence import SourceRevision
from reg_meta_build.source_values import (
    SourceValue,
    SourceValueAssociation,
    SourceValueDescriptor,
    SourceValueJoin,
)

if TYPE_CHECKING:
    from pathlib import Path


def _prepare(path: Path):
    revision = SourceRevision.create(
        dataset="values",
        publisher="Fixture",
        purpose="test",
        upstream_revision="1",
        artifact_path="values",
        artifact_size=0,
        artifact_sha256="0" * 64,
    )
    return prepare_source_values(
        path,
        revision=revision,
        descriptors=(SourceValueDescriptor("list", version="v1"),),
        values=(SourceValue("a", "01", "One"),),
        associations=(
            SourceValueAssociation(
                2, "list", "a", "values", member_id="1001", item_id="1"
            ),
        ),
        validity_revision=None,
        join=SourceValueJoin(
            record_sources=("records",),
            member_target="native_member",
            member_format="integer",
            validity_target="item",
            rule="Fixture explicit structural relation",
            provenance=("fixture format",),
        ),
    )


def test_absent_item_validity_is_refused(tmp_path: Path) -> None:
    """Input: an item-validity join whose validity source is absent (no revision).

    Expected: preparing it raises and publishes nothing. An item join reads an
    unlisted item as unrestricted, so an absent table would make every member
    valid in every year.

    No build reaches this: the SCB snapshot refuses Vardemangder.csv without
    VardemangderValidDates.csv (test_scb_snapshot_selection.py), and
    `prepared_catalog` refuses a value source without its validity revision. This
    validator is the prepared manifest's own contract behind both.

    Fails if `PreparedValueManifest`'s join check (prepared_values.py) stops
    refusing an item-validity join without a validity source revision.
    """
    with pytest.raises(ValueError, match="absent item-validity"):
        _prepare(tmp_path / "values")
    assert not (tmp_path / "values").exists()
