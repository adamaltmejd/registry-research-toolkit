"""Checked codebook bindings: label rules normalize claims and the committed SNI label rules bind only detailed books."""

from dataclasses import replace
from functools import cache
from pathlib import Path

import pytest
from _source_classification_bindings_support import (
    binding_setup as _setup,
    sole_classification as _sole_classification,
    sole_conformance as _sole_conformance,
)
from reg_meta_build.classifications import load_valid_codes
from reg_meta_build.curation_tree import load_curation_tree
from reg_meta_build.resolved_catalog import (
    ResolvedClassification,
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


_REPO_ROOT = Path(__file__).resolve().parent.parent


_CURATION = _REPO_ROOT / "curation"


_CLASSIFICATION_CODES = _REPO_ROOT / "input_data" / "classifications"


@cache
def _repo_label_rules() -> dict[str, str]:
    tree = load_curation_tree(_CURATION)
    return {
        label: entry.classification.slug
        for entry in tree.classifications
        for label in entry.binding.value_set_labels
    }


@cache
def _repo_book(short_name: str, codes_file: str) -> ResolvedClassification:
    return ResolvedClassification(
        slug=short_name.lower(),
        short_name=short_name,
        name=short_name,
        codes=tuple(
            ResolvedClassificationCode(code=code, label=label)
            for code, label in load_valid_codes(
                _CLASSIFICATION_CODES / codes_file
            ).items()
        ),
    )


def _synthetic_label_claim(setup, *, label: str, code: str):
    key, original = next(iter(setup[2].items()))
    claim = replace(
        original.claims[0],
        version_label=label,
        members=(replace(original.claims[0].members[0], code=code),),
    )
    return key, {key: resolve_code_membership((claim,))}


@pytest.mark.parametrize(
    ("label", "code"),
    [
        ("SNI 2002, begränsad nivå", "01"),
        ("SNI 2002, begränsad nivå", "42"),
        # SNI 92 reporting groups: limited (42 groups) and coarse (10 groups).
        ("SNI 92, begränsad nivå", "42"),
        ("SNI 92, begränsad nivå", "70"),  # RAMS 'Okänd värde' (missing)
        ("SNI 92, grov nivå", "03"),
        ("SNI 92, grov nivå", "00000"),  # coarse 'Uppgift saknas' (missing)
        # SNI 2002 coarse reporting groups (10 groups).
        ("SNI 2002, grov nivå", "02"),
        ('SNI 2007, grov nivå - "populärversion"', "G01"),
        ('SNI 2007, grov nivå - "populärversion"', "G99"),
        ('SNI 2007, utökad nivå - "populärversion"', "U01"),
        ('SNI 2007, utökad nivå - "populärversion"', "U99"),
    ],
)
def test_grouped_sni_reporting_labels_are_not_bound_to_detailed_books(label, code):
    setup = _setup()
    key, coding = _synthetic_label_claim(setup, label=label, code=code)
    result = apply_classification_cases(
        (setup[0],),
        (),
        coding=coding,
        classifications={
            "sni1992": _repo_book("SNI92", "sni92.csv").model_copy(
                update={"slug": "sni1992"}
            ),
            "sni2002": _repo_book("SNI2002", "sni2002.csv"),
            "sni2007": _repo_book("SNI2007", "sni2007.csv"),
        },
        occurrences=(source_occurrence(setup[0]),),
        label_rules=_repo_label_rules(),
    )
    assert result.diagnostics == ()
    segment = result.coding[key].segments[0]
    assert _sole_classification(segment) is None
    assert _sole_conformance(segment) is None
    assert segment.code_set is not None
    assert segment.code_set.members == ((code, "Source label"),)
    assert result.coding[key].claims[0].version_label == label
    assert (segment.valid_from, segment.valid_to) == ("2020-01-01", "2020-12-31")
    assert not any(p.startswith("label rule:") for p in segment.provenance)


def test_retained_detailed_sni_label_still_binds():
    setup = _setup()
    key, coding = _synthetic_label_claim(
        setup,
        label="Standard för svensk näringsgrensindelning, 2002 Branscher",
        code="01",
    )
    result = apply_classification_cases(
        (setup[0],),
        (),
        coding=coding,
        classifications={"sni2002": _repo_book("SNI2002", "sni2002.csv")},
        occurrences=(source_occurrence(setup[0]),),
        label_rules=_repo_label_rules(),
    )
    assert result.diagnostics == ()
    segment = result.coding[key].segments[0]
    assert _sole_classification(segment) == "sni2002"
    assert (
        _sole_conformance(segment) is not None
        and _sole_conformance(segment).status == "conforming"
    )
    assert any(p.startswith("label rule:") for p in segment.provenance)
