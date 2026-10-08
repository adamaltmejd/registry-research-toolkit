"""Occurrence-period authority compiles with complete source guards.

Kept until the build-case runner can select one SOS Deldatamängder (variant-level
parent) record as an `authority` placeholder.
"""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from typing import Any, cast

import pytest
from _curation_compile_support import (
    checked_correction_fixture as _checked_correction_fixture,
)
from reg_meta_build.curation_compile import (
    compile_occurrence_corrections,
)
from reg_meta_build.source_coordinates import (
    native_variable_key,
)
from reg_meta_build.source_curation import (
    capture_expectations,
    evaluate_cases,
)
from reg_meta_build.source_effects import (
    record_ref,
)
from reg_meta_build.source_records import (
    SourceCoordinate,
    SourceFields,
    value_field,
)


@pytest.mark.parametrize(
    "change",
    [None, "scope", "parent-prose", "missing", "duplicate", "unrelated", "new-parent"],
)
def test_occurrence_period_parent_authority_guards_fresh_and_stored_cases(
    tmp_path, change
):
    tree, scope, original, negative, entry = _checked_correction_fixture(
        tmp_path, period=True
    )
    coordinate = original.subject.variant.model_copy(update={"name": entry.variant})
    authority = original.model_copy(
        update={
            "locators": (
                original.locators[0].model_copy(
                    update={"semantic_record_key": ("parent-authority",)}
                ),
            ),
            "subject": original.subject.model_copy(
                update={
                    "variable": SourceCoordinate(status="not_applicable"),
                    "variant": coordinate,
                }
            ),
            "fields": SourceFields(),
            "edition_scope": entry.edition_scope,
            "parent_facts": (
                original.parent_facts[0].model_copy(
                    update={
                        "kind": "variant",
                        "coordinate": coordinate,
                        "register_name": original.subject.register_name,
                        "variant": coordinate,
                        "fields": SourceFields(
                            coverage_from=value_field("2016"),
                            description=value_field("Annual delivery"),
                        ),
                    }
                ),
            ),
        }
    )
    entry = entry.model_copy(
        update={
            "authority": list(
                capture_expectations(
                    (authority,),
                    fields=tuple(SourceFields.model_fields),
                    parents=True,
                    coding=True,
                )
            )
        }
    )
    reg = tree.registers[0]
    tree = replace(
        tree,
        registers=(
            reg.model_copy(
                update={
                    "errata": reg.errata.model_copy(
                        update={"occurrence_period": [entry]}
                    )
                }
            ),
        ),
    )

    def compile_with(parents):
        reader = SimpleNamespace(
            iter_without_native_family=lambda source: iter(parents),
            iter_native_families=lambda *args, **kwargs: iter(
                ((native_variable_key(original), (original, negative)),)
            ),
            lookup=lambda source, key: iter(
                r
                for r in parents
                if record_ref(r).source == source
                and record_ref(r).semantic_record_key == key
            ),
        )
        return compile_occurrence_corrections(
            tree, cast("Any", SimpleNamespace(records=reader)), (scope,), subset=False
        )

    cases, issues, _ = compile_with((authority,))
    assert not issues
    (case,) = cases[scope.source, None]
    assert entry.authority[0] in case.support
    parents = (authority,)
    if change == "scope":
        parents = (
            authority.model_copy(update={"edition_scope": original.edition_scope}),
        )
    elif change == "parent-prose":
        parent = authority.parent_facts[0]
        parents = (
            authority.model_copy(
                update={
                    "parent_facts": (
                        parent.model_copy(
                            update={
                                "fields": parent.fields.model_copy(
                                    update={
                                        "description": value_field("Changed authority")
                                    }
                                )
                            }
                        ),
                    )
                }
            ),
        )
    elif change == "missing":
        parents = ()
    elif change == "duplicate":
        parents = (authority, authority)
    elif change == "unrelated":
        parents = (
            authority.model_copy(
                update={
                    "subject": authority.subject.model_copy(
                        update={
                            "variant": coordinate.model_copy(update={"name": "OTHER"})
                        }
                    )
                }
            ),
        )
    if change == "new-parent":
        parents = (
            authority,
            authority.model_copy(
                update={
                    "locators": (
                        authority.locators[0].model_copy(
                            update={"semantic_record_key": ("new-parent",)}
                        ),
                    )
                }
            ),
        )
    fresh, diagnostics, _ = compile_with(parents)
    stored = evaluate_cases((case,), (original, negative, *parents))
    if change is None:
        assert fresh and not diagnostics and stored[0].status == "applicable"
    else:
        assert not fresh and diagnostics
        # Identical physical duplicates retain the existing semantic dedup policy.
        assert stored[0].status == ("applicable" if change == "duplicate" else "stale")
