"""Source classification declarations and the builder's catalog open, at public results.

The declaration cases read one synthetic SCB Registerinformation row (``var_row``)
through the real SCB reader and assert on ``apply_classification_cases`` →
``ClassificationBindingResolution`` diagnostics and coding. The open case asserts on the
located ``RegMetaError`` of ``open_built_db``. Literals come from the tests these
cases replace.
"""

from __future__ import annotations

import gc
import sqlite3
import warnings
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, var_row
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.source_evidence import SourceRevision
from reg_meta_build.db import open_built_db
from reg_meta_build.resolved_catalog import (
    ResolvedClassification,
    ResolvedClassificationCode,
)
from reg_meta_build.source_classification_bindings import apply_classification_cases
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    resolve_code_membership,
)
from reg_meta_build.source_curation import SourceRecordRef
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import ScopeInterval, TemporalScope, value_field
from reg_meta_build.sources.scb_records import clean_scb_row

if TYPE_CHECKING:
    from pathlib import Path

YEAR_2020 = TemporalScope(
    kind="intervals", intervals=(ScopeInterval(start="2020", end="2020"),)
)


def scb_record():
    """One synthetic SCB variable row (column ``Column``, year 2020) as read by SCB."""
    revision = SourceRevision.create(
        dataset="fixture",
        publisher="SCB",
        purpose="test",
        upstream_revision="1",
        artifact_path="rows.csv",
        artifact_size=1,
        artifact_sha256="a" * 64,
    )
    header = REGISTERINFORMATION_HEADER.split("|")
    values = var_row(
        colname="Column", var_id=1, cvid=100, varname="Variable", year="2020"
    ).split("|")
    cells = {k: (True, v, v) for k, v in zip(header, values, strict=True)}
    return clean_scb_row(header, 1, cells, revision).record


def declared(record, declaration: str):
    occurrence = source_occurrence(record)
    return replace(
        occurrence,
        fields=occurrence.fields.model_copy(
            update={"classification_declared": value_field(declaration)}
        ),
    )


def apply_declarations(record, occurrences):
    """Apply source declarations only; no reference names any of them."""
    key = source_occurrence(record).column_key
    inline = CodeListClaim(
        "list",
        YEAR_2020,
        (
            CodeMembershipClaim(
                "01", "Source label", TemporalScope(kind="year_independent")
            ),
        ),
    )
    book = ResolvedClassification(
        slug="fixture",
        short_name="FIX",
        name="Fixture",
        codes=(ResolvedClassificationCode(code="01", label="Canonical label"),),
    )
    return apply_classification_cases(
        tuple({r.record_id: r for o in occurrences for r in o.source_records}.values()),
        (),
        coding={key: resolve_code_membership((inline,))},
        classifications={"fixture": book},
        references={},
        occurrences=occurrences,
    )


def test_duplicate_originals_of_one_unresolved_declaration_report_it_once() -> None:
    """Two physical originals stating the same unresolved declaration at the same
    column, period and record reference share one diagnostic and bind nothing."""
    record = scb_record()
    declaration = declared(record, "Missing historical dictionary")
    duplicate = record.model_copy(update={"record_id": record.record_id + ":duplicate"})
    repeated = replace(declaration, source_records=(duplicate,))
    result = apply_declarations(record, (declaration, repeated))
    assert [(d.code, d.refs) for d in result.diagnostics] == [
        (
            "unresolved_classification_reference",
            (
                SourceRecordRef(
                    source=record.source,
                    semantic_record_key=record.locators[0].semantic_record_key,
                ),
            ),
        )
    ]
    assert all(
        not segment.classification_links
        for resolution in result.coding.values()
        for segment in resolution.segments
    )


def changed_detail(record, declaration):
    return declared(record, "Other missing dictionary")


def changed_period(record, declaration):
    return replace(
        declaration,
        edition_period_scope=TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2019", end="2019"),)
        ),
    )


def changed_ref(record, declaration):
    locator = record.locators[0]
    other = record.model_copy(
        update={
            "locators": (
                locator.model_copy(
                    update={
                        "semantic_record_key": (
                            *locator.semantic_record_key,
                            "another-row",
                        )
                    }
                ),
            )
        }
    )
    return replace(declaration, source_records=(other,))


@pytest.mark.parametrize(
    "change",
    [changed_detail, changed_period, changed_ref],
    ids=["declaration", "period", "record-ref"],
)
def test_unresolved_declarations_differing_in_any_identity_part_report_separately(
    change,
) -> None:
    """Declaration text, period and original record reference each keep a
    diagnostic of their own next to the duplicated pair's single one."""
    record = scb_record()
    declaration = declared(record, "Missing historical dictionary")
    duplicate = record.model_copy(update={"record_id": record.record_id + ":duplicate"})
    repeated = replace(declaration, source_records=(duplicate,))
    result = apply_declarations(
        record, (declaration, repeated, change(record, declaration))
    )
    assert [d.code for d in result.diagnostics] == [
        "unresolved_classification_reference"
    ] * 2


def _refusal(path: Path) -> tuple[str, int, str]:
    """Open ``path`` and return the refusal's code, exit code and message only, so
    no traceback keeps the builder's frames (and their connection) alive."""
    try:
        open_built_db(path)
    except RegMetaError as error:
        return error.code, error.exit_code, error.message
    raise AssertionError("open_built_db accepted a catalog without a manifest")


def test_builder_refuses_a_catalog_without_a_manifest(tmp_path: Path) -> None:
    """A SQLite file without ``import_manifest`` is a located configuration error,
    and the refused connection is closed (Python's sqlite3 emits a
    ``ResourceWarning`` when an unclosed connection is garbage-collected)."""
    path = tmp_path / "missing-manifest.db"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE dummy (x TEXT)")
    connection.close()
    # Collect first so connections leaked by earlier tests in this worker are not
    # attributed to this refusal.
    gc.collect()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ResourceWarning)
        code, exit_code, message = _refusal(path)
        gc.collect()
    assert code == "schema_incompatible"
    assert exit_code == EXIT_CONFIG
    assert "manifest is missing or unreadable" in message
    assert not [w for w in caught if issubclass(w.category, ResourceWarning)]
