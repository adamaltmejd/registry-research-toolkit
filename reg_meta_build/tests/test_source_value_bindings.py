"""Prepared value sources refuse an item join that reads an absent validity file as unrestricted."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import pytest
from reg_meta.source_evidence import SourceRevision
from reg_meta_build.prepared_values import prepare_source_values
from reg_meta_build.source_values import (
    SourceValue,
    SourceValueAssociation,
    SourceValueDescriptor,
    SourceValueJoin,
)

if TYPE_CHECKING:
    from pathlib import Path


def _prepare(path: Path, missing: Literal["unknown", "unrestricted"]):
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
            missing_validity=missing,
            rule="Fixture explicit structural relation",
            provenance=("fixture format",),
        ),
    )


def test_absent_item_validity_is_refused_unless_read_as_unknown(
    tmp_path: Path,
) -> None:
    """Input: an item-validity join whose validity source is absent (no revision).

    Expected: preparing it with `missing_validity="unrestricted"` raises and
    publishes nothing; the same source with `"unknown"` prepares, so its members
    later bind as unknown validity.

    No build reaches this: the SCB snapshot refuses Vardemangder.csv without
    VardemangderValidDates.csv (test_scb_snapshot_selection.py), and
    `prepared_catalog` declares an absent table `"unknown"` itself. This validator
    is the prepared manifest's own contract behind both.

    Fails if `PreparedValueManifest`'s join check (prepared_values.py) stops
    refusing an absent item-validity source declared unrestricted, which would
    let every member of such a list read as valid in every year.
    """
    with pytest.raises(ValueError, match="absent item-validity"):
        _prepare(tmp_path / "unrestricted", "unrestricted")
    assert not (tmp_path / "unrestricted").exists()
    assert _prepare(tmp_path / "unknown", "unknown").association_count == 1
