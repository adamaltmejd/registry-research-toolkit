"""A month family's varying definitions are authorized only when every contributor is checked.

``DESIGN.md`` ("A calendar-month period family may supply an exact
``expected_definitions`` map"): only a fully checked period family may leave its
monthly definitions on the delivery columns. Every effective contributor must be
covered by an applicable guarded case, and an ordinary variable's definition
conflict stays an error. ``source_formation._checked_month_definitions`` enforces
both rules.

No build reaches either refusal, so they stay here as slim tests (maintainer
decision #1267 for unreachable guards; a dead-code PR deletes the guard and its test
together):

- The identity case and the representation case of a family are minted over the
  same ``members`` (``curation_compile.compile_period_families``), so a family
  occurrence is always covered.
- The only other per-column representation, ``[[representation.parallel]]``,
  refuses differing definitions: the nested-edition conflict check and
  ``common_quantity`` in ``compile_parallel_representations``, and the loader's
  "every source column must cover the shared window" rule.

Probe: with the coverage loop and the family-key check both removed, every other
non-integration ``reg_meta_build/tests`` test still passes. The allowed side, a
complete checked family, is ``cases/build/period-family-months-merge-or-go-stale-per-register``.
"""

from __future__ import annotations

import json
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, var_row
from reg_meta_build.curation_compile import compile_period_families
from reg_meta_build.curation_tree import load_register_files
from reg_meta_build.resolved_catalog import ResolvedRegister, ResolvedVariant
from reg_meta_build.source_coding import resolve_code_membership
from reg_meta_build.source_coordinates import column_identity
from reg_meta_build.source_curation import RepresentationDecision
from reg_meta_build.source_effects import apply_occurrence_cases
from reg_meta_build.source_evidence import SourceRevision
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_records import SourceFields, value_field
from reg_meta_build.source_representations import resolve_representation_cases
from reg_meta_build.sources.scb_records import clean_scb_row

if TYPE_CHECKING:
    from pathlib import Path

MONTHS = (
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "Maj",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Okt",
    "Nov",
    "Dec",
)
DEFINITIONS = {
    f"{month:02}": f"Salary paid in month {month:02}; annual business income / 12."
    for month in range(1, 13)
}


def _checked_family(tmp_path: Path):
    """LISA's twelve 2020 month columns, each with its own definition, and a
    period-family entry whose reviewed map matches every one of them."""
    root = tmp_path / "curation"
    path = root / "registers" / "scb" / "lisa.toml"
    path.parent.mkdir(parents=True)
    path.write_text(
        '[register]\nprovider = "scb"\nslug = "lisa"\nnative_id = "34"\n\n'
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "lonfink"\nlabel = "Lön per månad"\nslug = "lonfink"\n'
        "expected_definitions = { "
        + ", ".join(
            f"{json.dumps(k)} = {json.dumps(v)}" for k, v in DEFINITIONS.items()
        )
        + " }\n",
        encoding="utf-8",
    )
    (register,) = load_register_files(root)
    revision = SourceRevision.create(
        dataset="scb-registerinformation",
        publisher="SCB",
        purpose="test",
        upstream_revision="1",
        artifact_path="rows.csv",
        artifact_size=1,
        artifact_sha256="a" * 64,
    )
    header = REGISTERINFORMATION_HEADER.split("|")
    records = []
    for index, month in enumerate(MONTHS, 1):
        cells = var_row(
            colname=f"LonFink{month}",
            cvid=100 + index,
            var_id=index,
            vardef=DEFINITIONS[f"{index:02}"],
            register=("LISA", 34, 10),
        ).split("|")
        raw = {
            name: (True, value, value)
            for name, value in zip(header, cells, strict=True)
        }
        records.append(clean_scb_row(header, index, raw, revision).record)
    return register, tuple(records)


def _form(register, records, *, uncovered: bool, ordinary: bool):
    """Form the family the way the build does, then break one input: drop the
    representation case's last target, or move the family to an ordinary key."""
    cases, _, _, issues = compile_period_families(register, records)
    assert issues == ()
    identity, case = cases
    corrected = apply_occurrence_cases(records, (identity,))
    assert corrected.diagnostics == ()
    decision = case.decision
    assert isinstance(decision, RepresentationDecision)
    if uncovered:
        case = case.model_copy(update={"targets": case.targets[:-1]})
    if ordinary:
        key = ("accepted", "ordinary", "quantity")
        decision = decision.model_copy(update={"variable_key": key})
        case = case.model_copy(update={"decision": decision})
        corrected = replace(
            corrected,
            occurrences=tuple(
                replace(occurrence, variable_key=key)
                for occurrence in corrected.occurrences
            ),
        )
    coding = {
        column_identity(
            decision.variable_key, decision.variant_key, column.column
        ): resolve_code_membership(())
        for column in decision.columns
    }
    proof = resolve_representation_cases(records, (case,), coding=coding)
    assert proof.diagnostics == ()
    return form_native_variable(
        corrected.occurrences,
        register=ResolvedRegister(provider="scb", slug="lisa", name="LISA"),
        variants={decision.variant_key: ResolvedVariant(slug="people", name="People")},
        slug="lonfink",
        provider_key="monthly",
        flags=SourceFields(
            identifier=value_field(False), sensitivity=value_field(False)
        ),
        coding=coding,
        representations=proof.cases,
    )


@pytest.mark.parametrize(
    ("uncovered", "ordinary"),
    [(False, False), (True, False), (False, True)],
    ids=["checked", "uncovered", "ordinary"],
)
def test_varying_month_definitions_need_a_completely_checked_family(
    tmp_path: Path, uncovered: bool, ordinary: bool
) -> None:
    """Input: the checked family as compiled, then with one contributor left out of
    the representation case (uncovered), or moved to an ordinary variable key
    (ordinary). Expected: the checked family projects its definitions; the other
    two report ``conflicting_variable_fact`` and no projection. Fails if
    ``_checked_month_definitions`` drops its per-contributor coverage loop
    (uncovered) or its period-family key check (ordinary).
    """
    register, records = _checked_family(tmp_path)
    formed = _form(register, records, uncovered=uncovered, ordinary=ordinary)
    codes = {diagnostic.code for diagnostic in formed.diagnostics}
    if uncovered or ordinary:
        assert "conflicting_variable_fact" in codes
        assert "period_family_definition_projected" not in codes
    else:
        assert codes == {"period_family_definition_projected"}
