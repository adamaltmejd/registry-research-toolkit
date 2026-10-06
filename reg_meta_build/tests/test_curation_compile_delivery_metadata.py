"""Delivery metadata, disjoint edition splits and occurrence-period authority compile with complete source guards."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest
from _curation_compile_support import (
    checked_correction_fixture as _checked_correction_fixture,
    pooled_parallel_fixture as _pooled_parallel_fixture,
    run_checked_correction as _run_checked_correction,
)
from reg_meta.errors import RegMetaError
from reg_meta_build.curation_compile import (
    compile_occurrence_corrections,
)
from reg_meta_build.curation_tree import (
    load_curation_tree,
    load_register_files,
)
from reg_meta_build.source_coordinates import (
    native_variable_key,
    native_variant_key,
)
from reg_meta_build.source_curation import (
    CheckedIdentityChange,
    capture_expectations,
    evaluate_cases,
)
from reg_meta_build.source_effects import (
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    NativeCoordinates,
    SourceCoordinate,
    SourceFields,
    value_field,
)

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize("drift", [None, "unit", "missing", "new", "partial", "window"])
def test_delivery_metadata_compiler_keeps_complete_literal_source_guards(
    tmp_path, drift
):
    from reg_meta_build.curation_compile import compile_delivery_metadata
    from reg_meta_build.curation_tree import DeliveryMetadataEntry
    from reg_meta_build.source_curation import (
        DeliveryMetadataColumn,
        capture_expectations,
    )
    from reg_meta_build.source_records import SourceFields, value_field

    path, raw, naming = _pooled_parallel_fixture(tmp_path)
    records = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={
                        "measurement_unit": value_field(unit),
                        "definition": value_field("Source income definition"),
                    }
                )
            }
        )
        for record, unit in zip(raw, ("100-tal kronor", "Kronor (SEK)"), strict=True)
    )
    entry = DeliveryMetadataEntry(
        fields=["name"],
        variable="1.1.income",
        records=list(
            capture_expectations(
                records,
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            )
        ),
        columns=[
            DeliveryMetadataColumn(
                variant_key=native_variant_key(record),
                column=record.fields.column_name.value,
                valid_from=start,
                valid_to=end,
                expected_codings=(),
            )
            for record, start, end in zip(
                records,
                ("2020-01-01", "2022-01-01"),
                ("2022-12-31", "2024-12-31"),
                strict=True,
            )
        ],
        evidence="Retain source encoding without value conversion",
        noted="2026-09-30",
    )
    (register,) = load_register_files(path.parents[2])
    register = register.model_copy(
        update={
            "representation": register.representation.model_copy(
                update={"parallel": [], "delivery_metadata": [entry]}
            )
        }
    )
    if drift == "unit":
        records = (
            records[0],
            records[1].model_copy(
                update={
                    "fields": records[1].fields.model_copy(
                        update={"measurement_unit": value_field("changed")}
                    )
                }
            ),
        )
    elif drift == "missing":
        records = records[:-1]
    elif drift == "new":
        new = records[0].model_copy(
            update={
                "locators": (
                    records[0]
                    .locators[0]
                    .model_copy(
                        update={
                            "semantic_record_key": (
                                *records[0].locators[0].semantic_record_key,
                                "new-peer",
                            )
                        }
                    ),
                )
            }
        )
        records = (*records, new)
    elif drift == "partial":
        register = register.model_copy(
            update={
                "representation": register.representation.model_copy(
                    update={
                        "delivery_metadata": [
                            entry.model_copy(update={"records": entry.records[:-1]})
                        ]
                    }
                )
            }
        )
    elif drift == "window":
        register = register.model_copy(
            update={
                "representation": register.representation.model_copy(
                    update={
                        "delivery_metadata": [
                            entry.model_copy(
                                update={
                                    "columns": [
                                        entry.columns[0].model_copy(
                                            update={"valid_to": "2021-12-31"}
                                        ),
                                        entry.columns[1],
                                    ]
                                }
                            )
                        ]
                    }
                )
            }
        )
    from reg_meta_build.source_curation import (
        CurationCase,
        OccurrenceCorrectionDecision,
        PeerGuard,
    )

    original_targets = tuple(entry.records)
    identity = CurationCase(
        case_id="unit-owner",
        targets=original_targets,
        peer_guards=(
            PeerGuard(
                guard_id="unit-identity-peers",
                source=original_targets[0].ref.source,
                native=NativeCoordinates(
                    register_id=1, register_variant_id=10, variable_id=1
                ),
                expected_members=tuple(target.ref for target in original_targets),
            ),
        ),
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            effects=tuple(
                CheckedIdentityChange(
                    ref=target.ref, variable_key=naming[-1].target.source_key
                )
                for target in original_targets
            ),
            reason="Checked owner",
            provenance="Exact family",
        ),
    )
    cases, diagnostics = compile_delivery_metadata(
        register, records, naming, ownership_cases=(identity,)
    )
    assert bool(cases) == (drift is None)
    assert bool(diagnostics) == (drift is not None)


def test_disjoint_edition_splits_share_native_variant_but_not_target_edition(
    tmp_path: Path,
) -> None:
    root = tmp_path / "curation"
    path = root / "registers/scb/sample.toml"
    path.parent.mkdir(parents=True)
    (root / "classifications").mkdir()
    prefix = (
        '[register]\nprovider="scb"\nslug="sample"\nnative_id="1"\n'
        '[[variant]]\nnative_id="1.2"\nslug="flow"\n'
        '[[variant]]\nnative_id="1.2.marriage"\nslug="marriage"\n'
        '[[variant]]\nnative_id="1.2.divorce"\nslug="divorce"\n'
    )
    entries = (
        '[[identity.edition_split]]\nvariant="1.2"\nsplit="1.2.marriage"\n'
        'editions=["Married"]\nsource_editions=["Divorced"]\n'
        'evidence="Explicit source event edition"\nnoted="2026-09-30"\n'
        '[[identity.edition_split]]\nvariant="1.2"\nsplit="1.2.divorce"\n'
        'editions=["Divorced"]\nsource_editions=["Married"]\n'
        'evidence="Explicit source event edition"\nnoted="2026-09-30"\n'
    )
    path.write_text(prefix + entries)
    assert len(load_curation_tree(root).registers[0].identity.edition_split) == 2
    path.write_text(
        prefix
        + entries.replace(
            'editions=["Divorced"]\nsource_editions=["Married"]',
            'editions=["Married"]\nsource_editions=["Divorced"]',
        )
    )
    with pytest.raises(RegMetaError) as exc:
        load_curation_tree(root)
    assert "editions assigned twice" in exc.value.message


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


def test_period_correction_uses_exact_prose_to_distinguish_same_column_originals(
    tmp_path,
):
    tree, scope, original, negative, entry = _checked_correction_fixture(
        tmp_path, period=True
    )
    negative = negative.model_copy(
        update={
            "edition_scope": original.edition_scope,
            "edition_period_scope": original.edition_period_scope,
            "original_period_text": original.original_period_text,
            "fields": original.fields.model_copy(
                update={"name": value_field("Distinct ninth event")}
            ),
        }
    )
    cases, issues, _ = _run_checked_correction(tree, scope, (original, negative))
    assert not issues
    (case,) = cases[scope.source, None]
    assert {target.ref for target in case.targets} == {record_ref(original)}
    assert {support.ref for support in case.support} == {record_ref(negative)}
    result = apply_occurrence_cases((original, negative), (case,))
    assert not result.diagnostics
    assert result.occurrences[0].edition_scope == entry.edition_scope
    assert result.occurrences[1] == source_occurrence(negative)
    drift = original.model_copy(
        update={
            "fields": original.fields.model_copy(
                update={"name": value_field("Changed event")}
            )
        }
    )
    assert _run_checked_correction(tree, scope, (drift, negative))[1]
    assert evaluate_cases((case,), (drift, negative))[0].status == "stale"
    extra = original.model_copy(
        update={
            "locators": (
                original.locators[0].model_copy(
                    update={"semantic_record_key": ("new-original",)}
                ),
            )
        }
    )
    assert _run_checked_correction(tree, scope, (original, negative, extra))[1]
    assert evaluate_cases((case,), (original, negative, extra))[0].status == "stale"
