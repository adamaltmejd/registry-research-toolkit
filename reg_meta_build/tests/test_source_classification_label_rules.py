"""Checked codebook bindings: how a matched label rule binds, merges and yields to overrides."""

from dataclasses import replace

from _source_classification_bindings_support import (
    binding_setup as _setup,
    sole_classification as _sole_classification,
    sole_conformance as _sole_conformance,
)
from reg_meta_build.resolved_catalog import (
    ResolvedClassificationCode,
)
from reg_meta_build.source_classification_bindings import (
    apply_classification_cases,
)
from reg_meta_build.source_coding import (
    resolve_code_membership,
)
from reg_meta_build.source_occurrences import source_occurrence


def test_label_rule_normalizes_claims_and_preserves_occurrence_evidence():
    setup = _setup()
    key, original = next(iter(setup[2].items()))
    claim = replace(original.claims[0], version_label="  LKF\u00a0 1998  ")
    coding = {key: resolve_code_membership((claim,))}
    matched: set[str] = set()
    result = apply_classification_cases(
        (setup[0],),
        (),
        coding=coding,
        classifications=setup[3],
        occurrences=(source_occurrence(setup[0]),),
        label_rules={"LKF 1998": "fixture"},
        matched_labels=matched,
    )
    segment = result.coding[key].segments[0]
    assert _sole_classification(segment) == "fixture"
    assert (
        _sole_conformance(segment) is not None
        and _sole_conformance(segment).status == "conforming"
    )
    assert (
        "label rule: 'LKF 1998' -> fixture (classifications/FIX.toml)"
        in segment.provenance
    )
    assert matched == {"LKF 1998"}
    changed_book = setup[3]["fixture"].model_copy(
        update={
            "codes": (
                *setup[3]["fixture"].codes,
                ResolvedClassificationCode(code="03", label="New code"),
            )
        }
    )
    changed = apply_classification_cases(
        (setup[0],),
        (),
        coding=coding,
        classifications={"fixture": changed_book},
        occurrences=(source_occurrence(setup[0]),),
        label_rules={"LKF 1998": "fixture"},
    )
    assert changed.diagnostics == ()
    assert _sole_classification(changed.coding[key].segments[0]) == "fixture"


def test_label_rule_skips_segment_without_catalog_occurrence():
    setup = _setup()
    key, original = next(iter(setup[2].items()))
    claim = replace(original.claims[0], version_label="Listed")
    coding = {key: resolve_code_membership((claim,))}
    result = apply_classification_cases(
        (setup[0],),
        (),
        coding=coding,
        classifications=setup[3],
        occurrences=(),
        label_rules={"Listed": "fixture"},
    )
    assert result.coding == coding
    assert result.diagnostics == ()


def test_two_labels_for_one_book_make_one_rule_binding():
    setup = _setup()
    key, original = next(iter(setup[2].items()))
    first = replace(original.claims[0], version_label="First")
    second = replace(first, claim_id="other", version_label="Second")
    result = apply_classification_cases(
        (setup[0],),
        (),
        coding={key: resolve_code_membership((first, second))},
        classifications=setup[3],
        occurrences=(source_occurrence(setup[0]),),
        label_rules={"First": "fixture", "Second": "fixture"},
    )
    segment = result.coding[key].segments[0]
    assert _sole_classification(segment) == "fixture"
    assert segment.provenance == (
        "label rule: 'First' -> fixture (classifications/FIX.toml)",
    )
    assert result.diagnostics == ()


def test_label_rule_preserves_multiple_books_and_respects_omitted_state():
    setup = _setup()
    key, original = next(iter(setup[2].items()))
    first = replace(original.claims[0], version_label="First")
    second = replace(first, claim_id="other", version_label="Second")
    coding = {key: resolve_code_membership((first, second))}
    alternate = setup[3]["fixture"].model_copy(update={"slug": "alternate"})
    kwargs = {
        "coding": coding,
        "classifications": {**setup[3], "alternate": alternate},
        "occurrences": (source_occurrence(setup[0]),),
        "label_rules": {"First": "fixture", "Second": "alternate"},
    }
    result = apply_classification_cases((setup[0],), (), **kwargs)
    assert tuple(
        link.classification
        for link in result.coding[key].segments[0].classification_links
    ) == ("alternate", "fixture")
    assert [issue.code for issue in result.diagnostics] == [
        "multiple_classifications_declared"
    ]
    omitted = replace(
        coding[key],
        segments=tuple(
            replace(segment, state_disposition="omit")
            for segment in coding[key].segments
        ),
    )
    result = apply_classification_cases(
        (setup[0],), (), **{**kwargs, "coding": {key: omitted}}
    )
    assert result.coding == {key: omitted}
    assert result.diagnostics == ()


def test_override_wins_over_label_rule_on_its_window():
    setup = _setup()
    key, original = next(iter(setup[2].items()))
    claim = replace(original.claims[0], version_label="Listed")
    alternate = setup[3]["fixture"].model_copy(update={"slug": "alternate"})
    result = apply_classification_cases(
        (setup[0],),
        (),
        coding={key: resolve_code_membership((claim,))},
        classifications={**setup[3], "alternate": alternate},
        occurrences=(source_occurrence(setup[0]),),
        label_rules={"Listed": "fixture"},
        override=("alternate", "classifications/ALT.toml#/binding/variable/1"),
    )
    assert result.diagnostics == ()
    assert _sole_classification(result.coding[key].segments[0]) == "alternate"
    duplicate_overrides: set[str] = set()
    apply_classification_cases(
        (setup[0],),
        (),
        coding={key: resolve_code_membership((claim,))},
        classifications=setup[3],
        occurrences=(source_occurrence(setup[0]),),
        label_rules={"Listed": "fixture"},
        override=("fixture", "classifications/FIX.toml#/binding/variable/1"),
        duplicate_overrides=duplicate_overrides,
    )
    assert duplicate_overrides == {"classifications/FIX.toml#/binding/variable/1"}
