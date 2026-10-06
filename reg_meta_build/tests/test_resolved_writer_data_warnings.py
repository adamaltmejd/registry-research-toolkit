"""Data-warning ownership and persistence through the resolved writer."""

from __future__ import annotations

from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from _resolved_catalog_support import (
    resolved_state as _state,
    resolved_variable as _variable,
)
from catalog_manifest import synthetic_manifest
from pydantic import ValidationError
from reg_meta_build.db import open_built_db
from reg_meta_build.resolved_catalog import (
    write_resolved_catalog,
)

if TYPE_CHECKING:
    from pathlib import Path

    from reg_meta_build.source_scope import ScopeResolution


def test_data_warnings_persist_exact_ownership_without_inventing_scope(tmp_path: Path):
    from hashlib import sha256

    from reg_meta.catalog import DataWarning
    from reg_meta.source_evidence import canonical_sha256

    payload = {
        "register_fqid": "scb/example",
        "variable_fqid": "scb/example/ampoltyp",
        "variant": "individuals",
        "delivery_column_name": "AmPolTyp",
        "valid_from": "2000-01-01",
        "valid_to": "2000-06-15",
        "code": "missing_coding_period",
        "severity": "warning",
        "summary": "Historical codes unavailable",
        "detail": "No source list for this window.",
        "diagnostic_detail_sha256": sha256(
            b"No source list for this window."
        ).hexdigest(),
        "source_subject": "native:44",
        "fields": [],
        "refs": [],
        "withheld_output": ["coding"],
        "acknowledged_by": "review-44",
        "case_id": None,
    }
    warning = DataWarning.model_validate(
        {"warning_id": canonical_sha256(payload), **payload}
    )
    unscoped_payload = {
        **payload,
        "variable_fqid": None,
        "variant": None,
        "delivery_column_name": None,
        "valid_from": None,
        "valid_to": None,
    }
    unscoped = DataWarning.model_validate(
        {"warning_id": canonical_sha256(unscoped_payload), **unscoped_payload}
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(
        (_variable(),),
        output,
        manifest=synthetic_manifest(),
        data_warnings=(warning, unscoped),
    )
    with closing(open_built_db(output)) as conn:
        rows = conn.execute("SELECT * FROM data_warning ORDER BY warning_id").fetchall()
        assert {
            DataWarning.model_validate_json(row["warning_json"]) for row in rows
        } == {warning, unscoped}
        scoped = next(row for row in rows if row["warning_id"] == warning.warning_id)
        assert (
            scoped["valid_from"],
            scoped["valid_to"],
            scoped["delivery_column_name"],
        ) == (warning.valid_from, warning.valid_to, warning.delivery_column_name)
        assert (
            scoped["variable_id"]
            == conn.execute("SELECT variable_id FROM variable").fetchone()[0]
        )
        assert (
            scoped["register_variant_id"]
            == conn.execute(
                "SELECT register_variant_id FROM register_variant"
            ).fetchone()[0]
        )
        global_row = next(
            row for row in rows if row["warning_id"] == unscoped.warning_id
        )
        assert all(
            global_row[field] is None
            for field in (
                "variable_id",
                "register_variant_id",
                "delivery_column_name",
                "valid_from",
                "valid_to",
            )
        )
        assert global_row["register_id"] == scoped["register_id"]
    for label, state in (
        ("open", _state(2000).model_copy(update={"valid_to": "9999-12-31"})),
        (
            "independent",
            _state(2000).model_copy(
                update={
                    "period_scope": "year_independent",
                    "valid_from": None,
                    "valid_to": None,
                }
            ),
        ),
    ):
        candidate = tmp_path / f"{label}.db"
        write_resolved_catalog(
            (_variable().model_copy(update={"states": (state,)}),),
            candidate,
            manifest=synthetic_manifest(),
            data_warnings=(warning, unscoped),
        )
        with closing(open_built_db(candidate)) as conn:
            assert {
                DataWarning.model_validate_json(row[0])
                for row in conn.execute("SELECT warning_json FROM data_warning")
            } == {warning, unscoped}
            assert tuple(
                conn.execute(
                    "SELECT valid_from, valid_to, period_scope FROM variable_state"
                ).fetchone()
            ) == (state.valid_from, state.valid_to, state.period_scope)
    with pytest.raises(ValidationError, match="identity"):
        write_resolved_catalog(
            (_variable(),),
            tmp_path / "tampered.db",
            manifest=synthetic_manifest(),
            data_warnings=(warning.model_copy(update={"detail": "Changed"}),),
        )


def test_warning_ownership_requires_complete_source_and_delivery_witnesses():
    from hashlib import sha256
    from types import SimpleNamespace
    from typing import cast

    from reg_meta.source_evidence import SourceRecordRef
    from reg_meta_build.data_warnings import scope_data_warnings
    from reg_meta_build.source_curation import ResolutionDiagnostic
    from reg_meta_build.source_records import ScopeInterval, TemporalScope

    variable = _variable()
    ref = SourceRecordRef(source="fixture", semantic_record_key=("44",))
    occurrence = SimpleNamespace(
        variable_key=("44",),
        variant_key=("v",),
        corrections=(),
        source_records=(),
        column_key=None,
        edition_scope=TemporalScope(kind="not_applicable"),
        edition_period_scope=TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2000", end="2000"),)
        ),
        fields=SimpleNamespace(
            column_name=SimpleNamespace(status="value", value="AmPolTyp")
        ),
        evidence=(
            SimpleNamespace(
                source="fixture",
                locators=(SimpleNamespace(semantic_record_key=("44",)),),
            ),
        ),
    )
    issue = ResolutionDiagnostic(
        code="missing_coding_period",
        severity="warning",
        subject="44",
        detail="Domain unavailable",
        refs=(ref,),
        valid_from="2000-01-01",
        valid_to="2000-12-31",
        acknowledged_by="accepted",
    )
    scope = SimpleNamespace(
        variables={("44",): variable},
        corrections=SimpleNamespace(occurrences=(occurrence,)),
        parents=SimpleNamespace(
            registers={("r",): variable.register_ref},
            variants={("v",): variable.states[0].variant},
        ),
        diagnostics=(issue,),
        evaluations=(),
    )
    (warning,) = scope_data_warnings(cast("ScopeResolution", scope))
    assert warning.detail != issue.detail
    assert warning.diagnostic_detail_sha256 == sha256(issue.detail.encode()).hexdigest()
    assert str(warning.variable_fqid) == "scb/example/ampoltyp"
    assert warning.variant == "individuals"
    assert warning.delivery_column_name == "AmPolTyp"
    large_detail = "Original source label Å " * 100_000
    scope.diagnostics = (
        issue.model_copy(
            update={"code": "item_validity_set_aside", "detail": large_detail}
        ),
    )
    (compact,) = scope_data_warnings(cast("ScopeResolution", scope))
    assert len(compact.detail) < 300
    assert (
        compact.diagnostic_detail_sha256
        == sha256(large_detail.encode("utf-8")).hexdigest()
    )
    scope.diagnostics = (
        issue.model_copy(
            update={
                "refs": (
                    SourceRecordRef(source="fixture", semantic_record_key=("missing",)),
                )
            }
        ),
    )
    (warning,) = scope_data_warnings(cast("ScopeResolution", scope))
    assert warning.variable_fqid is None and warning.variant is None
    scope.diagnostics = (
        issue.model_copy(update={"valid_from": None, "valid_to": None}),
    )
    (warning,) = scope_data_warnings(cast("ScopeResolution", scope))
    assert warning.variable_fqid is not None and warning.variant is None
    scope.diagnostics = (
        issue.model_copy(
            update={"code": "metadata_projected", "acknowledged_by": None}
        ),
    )
    assert scope_data_warnings(cast("ScopeResolution", scope)) == ()


@pytest.mark.parametrize(
    ("fields", "code"),
    [
        (("data_type",), "assumed_storage_type"),
        (("identity",), "source_identity_assumption"),
        (("coding",), "response_domain_assumption"),
        (("availability",), "source_availability_limitation"),
    ],
)
def test_assumption_warns_on_added_physical_column_not_anchor_identity(fields, code):
    from types import SimpleNamespace
    from typing import cast

    from reg_meta_build.data_warnings import scope_data_warnings
    from reg_meta_build.source_records import ScopeInterval, TemporalScope

    variable = _variable()
    scope = SimpleNamespace(
        variables={("graft",): variable},
        parents=SimpleNamespace(
            registers={("r",): variable.register_ref},
            variants={("v",): variable.states[0].variant},
        ),
        diagnostics=(),
        evaluations=(
            SimpleNamespace(
                case_id="graft-case",
                status="applicable",
                decision=SimpleNamespace(
                    kind="correct_occurrences",
                    data_warning="Historical storage type is assumed",
                    data_warning_refs=(),
                    data_warning_fields=fields,
                    reason="Historical text assumption",
                    provenance="Exact reviewed errata-column authority",
                ),
            ),
        ),
        corrections=SimpleNamespace(
            occurrences=(
                SimpleNamespace(
                    variable_key=("graft",),
                    variant_key=("v",),
                    occurrence_key="added-column",
                    column_key=None,
                    edition_scope=TemporalScope(kind="not_applicable"),
                    edition_period_scope=TemporalScope(
                        kind="intervals",
                        intervals=(ScopeInterval(start="2000", end="2000"),),
                    ),
                    fields=SimpleNamespace(
                        column_name=SimpleNamespace(status="value", value="AmPolTyp")
                    ),
                    evidence=(),
                    source_records=(),
                    corrections=(
                        SimpleNamespace(
                            case_id="graft-case",
                            provenance="maintainer-authorized storage-type assumption: historical text",
                        ),
                    ),
                ),
            )
        ),
    )
    (warning,) = scope_data_warnings(cast("ScopeResolution", scope))
    assert str(warning.variable_fqid) == "scb/example/ampoltyp"
    assert warning.code == code and warning.refs == ()
    assert warning.source_subject == "graft-case" and warning.case_id == "graft-case"
    assert (
        warning.valid_from == "2000-01-01"
        and warning.delivery_column_name == "AmPolTyp"
    )
