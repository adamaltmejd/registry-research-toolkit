"""Resolved writer classification books, succession, conformance and sentinel certificates."""

from __future__ import annotations

import json
from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from _resolved_catalog_support import (
    classified_alias_variable as _classified_alias_variable,
    resolved_classification as _classification,
    resolved_state as _state,
    resolved_variable as _variable,
    scoped_sentinel_variable as _scoped_sentinel_variable,
)
from catalog_manifest import synthetic_manifest
from pydantic import ValidationError
from reg_meta.db import CLASSIFICATION_SUCCESSION_AS_OF_YEAR
from reg_meta_build.curation_tree import SentinelCode
from reg_meta_build.db import open_built_db
from reg_meta_build.resolved_catalog import (
    ResolvedAliasWindow,
    ResolvedClassification,
    ResolvedClassificationCode,
    ResolvedClassificationLink,
    ResolvedClassificationSuccession,
    ResolvedCodeSet,
    ResolvedConformance,
    ResolvedState,
    write_resolved_catalog,
)
from reg_meta_build.validate import validate_built_db

if TYPE_CHECKING:
    from pathlib import Path


def test_sentinel_overlapping_canonical_code_is_refused() -> None:
    with pytest.raises(ValidationError, match="example-codes") as exc_info:
        ResolvedClassification(
            slug="example-codes",
            short_name="example-codes",
            name="Canonical example",
            codes=(ResolvedClassificationCode(code="001", label="Canonical one"),),
            sentinel_codes=(SentinelCode(code="001", meaning="stale entry"),),
        )
    assert "'001'" in str(exc_info.value)


@pytest.mark.parametrize("status", ["extended"])
def test_classifications_and_explicit_conformance_are_written_exactly(
    tmp_path: Path,
    status: str,
) -> None:
    classification = _classification()
    predecessor = _classification("older-codes")
    succession = ResolvedClassificationSuccession(
        predecessor=predecessor.slug,
        successor=classification.slug,
        effective_year=2000,
        note="curated:fixture",
    )
    conformance = ResolvedConformance.model_validate(
        {
            "declared_classification": classification.slug,
            "status": status,
            "checked_codes": ("", "001", "missing"),
            "nonconforming_members": (
                ("", "Unspecified"),
                ("missing", "Unlisted label"),
            ),
        }
    )
    variable = _variable()
    state = variable.states[0].model_copy(
        update={
            "value_set": ResolvedCodeSet(
                members=(
                    ("001", "Observed label"),
                    ("missing", "Unlisted label"),
                    ("", "Unspecified"),
                )
            ),
            "classification_links": (
                ResolvedClassificationLink(
                    classification=classification.slug, conformance=conformance
                ),
            ),
        }
    )
    variable = variable.model_copy(update={"states": (state,)})
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(
        (variable,),
        output,
        manifest=synthetic_manifest(),
        classifications=(classification, predecessor),
        classification_successions=(succession,),
    )
    original = output.read_bytes()
    with closing(open_built_db(output)) as conn:
        assert tuple(
            conn.execute(
                "SELECT c.short_name, c.name, c.name_en, c.publisher, c.valid_from, c.valid_to, "
                "c.description, c.url, p.slug, c.code_count, c.valid_code_count FROM classification c "
                "LEFT JOIN classification p ON p.id=c.supersedes_id WHERE c.slug=?",
                (classification.slug,),
            ).fetchone()
        ) == (
            classification.short_name,
            classification.name,
            classification.name_en,
            classification.publisher,
            2000,
            2020,
            classification.description,
            classification.url,
            predecessor.slug,
            2,
            2,
        )
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT code, label, level, is_valid FROM classification_code cc "
                "JOIN classification c ON c.id=cc.classification_id JOIN value_code vc USING(code_id) "
                "WHERE c.slug=? ORDER BY code",
                (classification.slug,),
            )
        ] == [("001", "Canonical one", 1, 1), ("002", "Canonical two", 2, 1)]
        assert tuple(
            conn.execute(
                "SELECT c.slug, status, checked_code_count, matched_code_count, nonconforming_code_count, overlap "
                "FROM classification_conformance cc JOIN classification c ON c.id=cc.declared_classification_id"
            ).fetchone()
        ) == (classification.slug, status, 3, 1, 2, 1 / 3)
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT code, label FROM classification_conformance_code JOIN value_code USING(code_id) ORDER BY code, label"
            )
        ] == [("", "Unspecified"), ("missing", "Unlisted label")]
        assert conn.execute(
            "SELECT c.slug FROM variable_state s JOIN state_classification sc USING(state_id) JOIN classification c ON c.id=sc.classification_id"
        ).fetchone()[0] == (classification.slug)
        assert conn.execute("SELECT count(*) FROM code_variable_map").fetchone()[0] == 3
        # Source and official labels can differ; membership is checked by code.
        canonical = conn.execute(
            "SELECT vc.code, vc.label FROM value_set_member m "
            "JOIN value_code vc USING(code_id) "
            "WHERE m.value_set_id=(SELECT value_set_id FROM variable_state) "
            "AND vc.code IN (SELECT ccvc.code FROM classification_code cc "
            "JOIN value_code ccvc ON ccvc.code_id=cc.code_id "
            "JOIN classification c ON c.id=cc.classification_id WHERE c.slug=?)",
            (classification.slug,),
        ).fetchall()
        extensions = conn.execute(
            "SELECT vc.code, vc.label FROM classification_conformance_code cc "
            "JOIN value_code vc USING(code_id) JOIN classification c ON c.id=cc.declared_classification_id WHERE c.slug=?",
            (classification.slug,),
        ).fetchall()
        assert {row[0] for row in canonical}.isdisjoint(row[0] for row in extensions)
        assert state.value_set is not None
        assert sorted(tuple(row) for row in (*canonical, *extensions)) == sorted(
            state.value_set.members
        )

    write_resolved_catalog(
        (variable,),
        output,
        manifest=synthetic_manifest(),
        classifications=(predecessor, classification),
        classification_successions=(succession,),
    )
    assert output.read_bytes() == original


@pytest.mark.parametrize("defect", ["missing", "duplicate", "conformance"])
def test_bad_classification_references_or_membership_preserve_previous_catalog(
    tmp_path: Path,
    defect: str,
) -> None:
    classification = _classification()
    classifications = (classification,)
    variable = _variable()
    if defect == "missing":
        variable = variable.model_copy(
            update={
                "states": (
                    variable.states[0].model_copy(
                        update={
                            "classification_links": (
                                ResolvedClassificationLink(classification="missing"),
                            )
                        }
                    ),
                )
            }
        )
    elif defect == "duplicate":
        classifications = (classification, classification)
    else:
        variable = variable.model_copy(
            update={
                "states": (
                    variable.states[0].model_copy(
                        update={
                            "value_set": ResolvedCodeSet(members=(("999", "Missing"),)),
                            "classification_links": (
                                ResolvedClassificationLink(
                                    classification=classification.slug,
                                    conformance=ResolvedConformance(
                                        declared_classification=classification.slug,
                                        status="conforming",
                                        checked_codes=("999",),
                                    ),
                                ),
                            ),
                        }
                    ),
                )
            }
        )
    output = tmp_path / "existing.db"
    output.write_bytes(b"previous")
    with pytest.raises(ValueError, match="classification|conformance"):
        write_resolved_catalog(
            (variable,),
            output,
            manifest=synthetic_manifest(),
            classifications=classifications,
        )
    assert output.read_bytes() == b"previous"


@pytest.mark.parametrize("effective_year", [None, 2050])
def test_cyclic_classification_succession_preserves_previous_catalog(
    tmp_path: Path, effective_year: int | None
) -> None:
    first, second = _classification("first-codes"), _classification("second-codes")
    output = tmp_path / "existing.db"
    output.write_bytes(b"previous")
    edges = tuple(
        ResolvedClassificationSuccession(
            predecessor=predecessor.slug,
            successor=successor.slug,
            effective_year=effective_year,
        )
        for predecessor, successor in ((first, second), (second, first))
    )
    with pytest.raises(ValueError, match="cyclic classification succession"):
        write_resolved_catalog(
            (_variable(),),
            output,
            manifest=synthetic_manifest(),
            classifications=(first, second),
            classification_successions=edges,
        )
    assert output.read_bytes() == b"previous"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["existing.db"]


def test_classification_predecessor_is_a_deterministic_projection_of_active_edges(
    tmp_path: Path,
) -> None:
    books = tuple(_classification(slug) for slug in ("a", "b", "current", "future"))
    edges = (
        ResolvedClassificationSuccession(
            predecessor="a", successor="current", note="undated declaration"
        ),
        ResolvedClassificationSuccession(
            predecessor="b",
            successor="current",
            effective_year=CLASSIFICATION_SUCCESSION_AS_OF_YEAR,
        ),
        ResolvedClassificationSuccession(
            predecessor="current",
            successor="future",
            effective_year=CLASSIFICATION_SUCCESSION_AS_OF_YEAR + 1,
        ),
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(
        (_variable(),),
        output,
        manifest=synthetic_manifest(),
        classifications=books,
        classification_successions=edges,
    )
    original = output.read_bytes()
    with closing(open_built_db(output)) as conn:
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT c.slug, p.slug FROM classification c "
                "LEFT JOIN classification p ON p.id=c.supersedes_id ORDER BY c.slug"
            )
        ] == [("a", None), ("b", None), ("current", "a"), ("future", None)]
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT predecessor_slug, successor_slug, effective_year, note "
                "FROM classification_replaced_by ORDER BY predecessor_slug, successor_slug"
            )
        ] == [
            (edge.predecessor, edge.successor, edge.effective_year, edge.note)
            for edge in edges
        ]
    write_resolved_catalog(
        (_variable(),),
        output,
        manifest=synthetic_manifest(),
        classifications=tuple(reversed(books)),
        classification_successions=tuple(reversed(edges)),
    )
    assert output.read_bytes() == original


@pytest.mark.parametrize("defect", ["predecessor", "successor", "duplicate"])
def test_invalid_classification_edges_fail_before_creating_output(
    tmp_path: Path, defect: str
) -> None:
    edges = (
        ResolvedClassificationSuccession(
            predecessor="missing" if defect == "predecessor" else "before",
            successor="missing" if defect == "successor" else "after",
        ),
    )
    if defect == "duplicate":
        edges += (edges[0].model_copy(update={"effective_year": 2050}),)
    with pytest.raises(ValueError, match="classification succession"):
        write_resolved_catalog(
            (_variable(),),
            tmp_path / "diagnostic.db",
            manifest=synthetic_manifest(),
            diagnostic=True,
            classifications=(_classification("before"), _classification("after")),
            classification_successions=edges,
        )
    assert list(tmp_path.iterdir()) == []


def test_scoped_sentinel_certificate_keeps_local_member_without_changing_book(
    tmp_path: Path,
) -> None:
    variable, book = _scoped_sentinel_variable()
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(
        (variable,), output, manifest=synthetic_manifest(), classifications=(book,)
    )
    with closing(open_built_db(output)) as conn:
        assert tuple(
            conn.execute(
                "SELECT status, checked_code_count, matched_code_count, nonconforming_code_count FROM classification_conformance"
            ).fetchone()
        ) == ("extended", 2, 1, 1)
        assert (
            conn.execute(
                "SELECT count(*) FROM classification_code WHERE code_id IN (SELECT code_id FROM value_code WHERE code='09350')"
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                "SELECT count(*) FROM value_code WHERE code='09350' AND label='Okänt'"
            ).fetchone()[0]
            == 1
        )
        kind, meaning, payload = conn.execute(
            "SELECT member_kind, sentinel_meaning, scoped_sentinels FROM classification_conformance_code"
        ).fetchone()
        assert (kind, meaning) == ("sentinel", None)
        assert json.loads(payload) == [
            variable.states[0]
            .classification_links[0]
            .conformance.scoped_sentinels[0]
            .model_dump(mode="json")
        ]


def test_state_conformance_preserves_global_sentinel_meaning_and_substantive_extension(
    tmp_path: Path,
) -> None:
    book = _classification().model_copy(
        update={"sentinel_codes": (SentinelCode(code="99", meaning="Not applicable"),)}
    )
    conformance = ResolvedConformance(
        declared_classification=book.slug,
        status="extended",
        checked_codes=("001", "98", "99"),
        nonconforming_members=(("98", "Substantive extra"),),
        sentinel_members=(("99", "Source wording"),),
    )
    state = _state(2000).model_copy(
        update={
            "value_set": ResolvedCodeSet(
                members=(
                    ("001", "Source label"),
                    ("98", "Substantive extra"),
                    ("99", "Source wording"),
                )
            ),
            "classification_links": (
                ResolvedClassificationLink(
                    classification=book.slug, conformance=conformance
                ),
            ),
        }
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(
        (_variable().model_copy(update={"states": (state,)}),),
        output,
        manifest=synthetic_manifest(),
        classifications=(book,),
    )
    with closing(open_built_db(output)) as conn:
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT vc.code, vc.label, cc.member_kind, cc.sentinel_meaning, cc.scoped_sentinels "
                "FROM classification_conformance_code cc JOIN value_code vc USING(code_id) ORDER BY vc.code"
            )
        ] == [
            ("98", "Substantive extra", "nonstandard", None, "[]"),
            ("99", "Source wording", "sentinel", "Not applicable", "[]"),
        ]


@pytest.mark.parametrize(
    "defect", ["missing", "book", "column", "window", "label", "canonical"]
)
def test_scoped_sentinel_certificate_tampering_is_refused_before_publish(
    tmp_path: Path, defect: str
) -> None:
    variable, book = _scoped_sentinel_variable()
    state = variable.states[0]
    conformance = state.classification_links[0].conformance
    assert conformance is not None
    certificate = conformance.scoped_sentinels[0]
    if defect == "missing":
        conformance = conformance.model_copy(update={"scoped_sentinels": ()})
    else:
        update = {
            "book": {"classification_sha256": "b" * 64},
            "column": {"delivery_column_name": "Other"},
            "window": {"valid_to": "2000-06-30"},
            "label": {"members": (("09350", "Different meaning"),)},
            "canonical": {"members": (("001", "Source label"),)},
        }[defect]
        conformance = conformance.model_copy(
            update={"scoped_sentinels": (certificate.model_copy(update=update),)}
        )
    variable = variable.model_copy(
        update={
            "states": (
                state.model_copy(
                    update={
                        "classification_links": (
                            state.classification_links[0].model_copy(
                                update={"conformance": conformance}
                            ),
                        )
                    }
                ),
            )
        }
    )
    output = tmp_path / "existing.db"
    output.write_bytes(b"previous")
    with pytest.raises(
        ValueError, match="scoped sentinel certificate|conformance disagrees"
    ):
        write_resolved_catalog(
            (variable,), output, manifest=synthetic_manifest(), classifications=(book,)
        )
    assert output.read_bytes() == b"previous"


def test_scoped_sentinel_certificate_requires_positive_source_and_case_evidence() -> (
    None
):
    variable, _ = _scoped_sentinel_variable()
    certificate = (
        variable.states[0].classification_links[0].conformance.scoped_sentinels[0]
    )
    for field, invalid in (("source_fingerprints", ()), ("provenance", "")):
        with pytest.raises(ValidationError):
            type(certificate).model_validate(
                {**certificate.model_dump(), field: invalid}
            )


def test_incomplete_classification_partition_preserves_previous_catalog(
    tmp_path: Path,
) -> None:
    book = _classification()
    variable = _variable()
    conformance = ResolvedConformance(
        declared_classification=book.slug,
        status="conforming",
        checked_codes=("001",),
    )
    state = variable.states[0].model_copy(
        update={
            "value_set": ResolvedCodeSet(
                members=(("001", "Source label"), ("", "Source missing"))
            ),
            "classification_links": (
                ResolvedClassificationLink(
                    classification=book.slug, conformance=conformance
                ),
            ),
        }
    )
    output = tmp_path / "reg_meta.db"
    output.write_bytes(b"previous")
    with pytest.raises(ValueError, match="conformance must check every distinct code"):
        write_resolved_catalog(
            (variable.model_copy(update={"states": (state,)}),),
            output,
            manifest=synthetic_manifest(),
            classifications=(book,),
        )
    assert output.read_bytes() == b"previous"


@pytest.mark.parametrize("defect", ["duplicate", "order"])
def test_state_classification_links_refuse_duplicate_or_unsorted_books(defect):
    first = ResolvedClassificationLink(classification="first")
    second = ResolvedClassificationLink(classification="second")
    links = (first, first) if defect == "duplicate" else (second, first)
    with pytest.raises(ValueError, match="unique sorted books"):
        ResolvedState.model_validate(
            _state(2000).model_copy(update={"classification_links": links})
        )


def test_classified_alias_keeps_own_domain_and_book_without_backing_inheritance(
    tmp_path,
):
    variable, book = _classified_alias_variable()
    output = tmp_path / "alias.db"
    write_resolved_catalog(
        (variable,), output, manifest=synthetic_manifest(), classifications=(book,)
    )
    with closing(open_built_db(output)) as conn:
        row = conn.execute(
            "SELECT a.delivery_column_name, c.slug, a.conformance FROM alias_window_classification a JOIN classification c ON c.id=a.classification_id"
        ).fetchone()
        assert tuple(row[:2]) == ("AmPolTyp", book.slug)
        assert (
            ResolvedConformance.model_validate_json(row[2])
            == variable.aliases[0].windows[0].classification_links[0].conformance
        )
        assert (
            conn.execute("SELECT COUNT(*) FROM state_classification").fetchone()[0] == 0
        )
        assert (
            conn.execute("SELECT value_set_id FROM variable_state").fetchone()[0]
            is None
        )
        assert validate_built_db(output, corpus=False).passed


def test_alias_classification_contract_refuses_shared_and_unchecked_domain():
    variable, _ = _classified_alias_variable()
    window = variable.aliases[0].windows[0]
    with pytest.raises(ValidationError, match="shared representation coding"):
        ResolvedAliasWindow.model_validate(
            window.model_dump() | {"coding_metadata": "shared"}
        )
    link = window.classification_links[0]
    assert link.conformance is not None
    with pytest.raises(ValidationError, match="every distinct code"):
        ResolvedAliasWindow.model_validate(
            window.model_dump()
            | {
                "classification_links": (
                    link.model_copy(
                        update={
                            "conformance": link.conformance.model_copy(
                                update={"checked_codes": ("001",)}
                            )
                        }
                    ),
                )
            }
        )


def test_alias_scoped_certificate_book_fingerprint_is_checked_before_writing(tmp_path):
    variable, book = _classified_alias_variable()
    alias = variable.aliases[0]
    window = alias.windows[0]
    link = window.classification_links[0]
    assert link.conformance is not None
    certificate = link.conformance.scoped_sentinels[0].model_copy(
        update={"classification_sha256": "b" * 64}
    )
    conformance = link.conformance.model_copy(
        update={"scoped_sentinels": (certificate,)}
    )
    window = window.model_copy(
        update={
            "classification_links": (
                link.model_copy(update={"conformance": conformance}),
            )
        }
    )
    variable = variable.model_copy(
        update={"aliases": (alias.model_copy(update={"windows": (window,)}),)}
    )
    output = tmp_path / "bad-certificate.db"
    with pytest.raises(ValueError, match="scoped sentinel certificate"):
        write_resolved_catalog(
            (variable,), output, manifest=synthetic_manifest(), classifications=(book,)
        )
    assert not output.exists()
