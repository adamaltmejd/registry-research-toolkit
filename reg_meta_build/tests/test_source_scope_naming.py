"""An ambiguous-name guard of complete-scope composition that no build reaches.

The reachable naming behavior (deprecation, ambiguous-name withholding, columnless
remainders, provider keys beside a period family) is pinned by `cases/build/`.
"""

from __future__ import annotations

import pytest
from _source_scope_support import guard, record, resolve
from reg_meta.source_evidence import canonical_sha256
from reg_meta_build.source_coordinates import (
    native_variable_key,
    source_register_key,
)
from reg_meta_build.source_curation import (
    capture_expectations,
)
from reg_meta_build.source_naming import (
    AcceptedNamingEntry,
    NamingAmbiguity,
    NativeNamingTarget,
)
from reg_meta_build.source_records import (
    value_field,
)

from reg_meta_build.fqid_slugs import SlugEntry


def ambiguity(records):
    first = records[0]
    expectations = capture_expectations(records, fields=("column_name",))
    return NamingAmbiguity(
        family=NativeNamingTarget(
            kind="variable",
            provider="scb",
            source_key=native_variable_key(first),
            register_key=source_register_key(first),
            expectations=expectations,
            peer_guards=(
                guard(first).model_copy(
                    update={"expected_members": tuple(e.ref for e in expectations)}
                ),
            ),
        ),
        entries=(
            AcceptedNamingEntry(
                revision="curation/registers/scb/example.toml",
                origin="authored",
                entry=SlugEntry("variable", "1.5.code", "code", provider="scb"),
                supplied_fields=("slug",),
                content_sha256=canonical_sha256({"slug": "code"}),
            ),
        ),
        candidate_columns=tuple(
            ("1.5.code", column)
            for column in sorted({r.fields.column_name.value for r in records})
        ),
        reason="Two literal spellings share the accepted discriminator; ownership is unresolved.",
    )


@pytest.mark.parametrize("change", ["column", "new_peer", "wrong_family"])
def test_ambiguous_naming_bridge_requires_its_whole_original_family(change):
    """Kept as a unit test by maintainer decision (#1267): no build reaches it,
    because the compiler builds each ambiguity from the same build's records, so
    its bridge always matches its family. Input: the family's column renamed, a
    new peer spelling, or the bridge pointing at another native variable.
    Refusal: "ambiguous naming bridge is stale or belongs to another family".
    Fails if resolve_source_scope applies a bridge without comparing it to the
    family's current records.
    """
    original = record(column="CODE")
    pending = ambiguity((original,))
    records = (original,)
    if change == "column":
        records = (
            original.model_copy(
                update={
                    "fields": original.fields.model_copy(
                        update={"column_name": value_field("NEW")}
                    )
                }
            ),
        )
    elif change == "new_peer":
        records += (record(2, column="Code"),)
    else:
        pending = pending.model_copy(
            update={
                "family": pending.family.model_copy(
                    update={"source_key": (*pending.family.source_key[:-1], 6)}
                )
            }
        )
    with pytest.raises(
        ValueError,
        match="ambiguous naming bridge is stale or belongs to another family",
    ):
        resolve(
            records,
            naming_ambiguities=(pending,),
            provider_keys={native_variable_key(original): None},
        )
