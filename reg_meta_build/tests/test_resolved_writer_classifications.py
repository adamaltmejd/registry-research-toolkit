"""Resolved writer preflight refusals of classification links that no build reaches.

The pipeline computes every classification link, its conformance and its sentinel
certificates from the build it writes, so these refusals are defense in depth behind
the compile checks. The written outcomes are the build cases
`classification-book-written-with-conformance-sentinels-and-successions` and
`representation-column-coding-and-books-stay-per-column`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _resolved_catalog_support import (
    classified_alias_variable as _classified_alias_variable,
    resolved_classification as _classification,
    resolved_variable as _variable,
    scoped_sentinel_variable as _scoped_sentinel_variable,
)
from catalog_manifest import synthetic_manifest
from reg_meta_build.resolved_catalog import (
    ResolvedClassificationLink,
    ResolvedCodeSet,
    ResolvedConformance,
    write_resolved_catalog,
)

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize("defect", ["missing", "conformance", "unchecked"])
def test_bad_classification_references_or_membership_preserve_previous_catalog(
    tmp_path: Path,
    defect: str,
) -> None:
    """A state link the catalog cannot back is refused and the old file survives.

    Input: a state linked to a book that is not written (`missing`), a link whose
    conformance claims `conforming` while its checked code 999 is not in the book
    (`conformance`), or a conformance that leaves the value set's blank code ''
    unchecked (`unchecked`). Expected: ValueError naming classification or conformance
    (for `unchecked`, the every-distinct-code partition), and the previous output bytes
    unchanged. No build reaches it: compile binds only compiled books and
    `resolve_classification_conformance` checks every distinct code itself.
    Fails if `write_resolved_catalog` stops cross-checking links against the written
    books and the state's value set before it replaces the output, or its partition
    check skips a falsy (blank) code.
    """
    book = _classification()
    state = _variable().states[0]
    message = "classification|conformance"
    if defect == "missing":
        update = {
            "classification_links": (
                ResolvedClassificationLink(classification="missing"),
            )
        }
    elif defect == "conformance":
        update = {
            "value_set": ResolvedCodeSet(members=(("999", "Missing"),)),
            "classification_links": (
                ResolvedClassificationLink(
                    classification=book.slug,
                    conformance=ResolvedConformance(
                        declared_classification=book.slug,
                        status="conforming",
                        checked_codes=("999",),
                    ),
                ),
            ),
        }
    else:
        update = {
            "value_set": ResolvedCodeSet(
                members=(("", "Source missing"), ("001", "Source label"))
            ),
            "classification_links": (
                ResolvedClassificationLink(
                    classification=book.slug,
                    conformance=ResolvedConformance(
                        declared_classification=book.slug,
                        status="conforming",
                        checked_codes=("001",),
                    ),
                ),
            ),
        }
        message = "conformance must check every distinct code in the value set"
    variable = _variable().model_copy(
        update={"states": (state.model_copy(update=update),)}
    )
    output = tmp_path / "existing.db"
    output.write_bytes(b"previous")
    with pytest.raises(ValueError, match=message):
        write_resolved_catalog(
            (variable,), output, manifest=synthetic_manifest(), classifications=(book,)
        )
    assert output.read_bytes() == b"previous"


@pytest.mark.parametrize(
    "defect", ["missing", "book", "column", "window", "label", "canonical", "alias"]
)
def test_scoped_sentinel_certificate_tampering_is_refused_before_publish(
    tmp_path: Path, defect: str
) -> None:
    """A scoped sentinel certificate that disagrees with its state is refused.

    Input: a state (or, for `alias`, a per-column alias window) whose recorded
    sentinel 09350 lost its certificate (`missing`), or whose certificate names
    another book fingerprint (`book`, `alias`), column, window, label, or a canonical
    member. Expected: ValueError naming the scoped sentinel certificate or the
    conformance disagreement, and the previous output bytes unchanged (no file for
    `alias`). No build reaches it: compile recomputes every certificate from the build
    it applies to (8a kept the application-time guards in
    `test_source_classification_bindings.py`). Fails if `write_resolved_catalog`
    stops re-checking certificates against the state, the column and the written book.
    """
    if defect == "alias":
        variable, book = _classified_alias_variable()
        alias = variable.aliases[0]
        window = alias.windows[0]
        link = window.classification_links[0]
        assert link.conformance is not None
        certificate = link.conformance.scoped_sentinels[0].model_copy(
            update={"classification_sha256": "b" * 64}
        )
        window = window.model_copy(
            update={
                "classification_links": (
                    link.model_copy(
                        update={
                            "conformance": link.conformance.model_copy(
                                update={"scoped_sentinels": (certificate,)}
                            )
                        }
                    ),
                )
            }
        )
        variable = variable.model_copy(
            update={"aliases": (alias.model_copy(update={"windows": (window,)}),)}
        )
        output = tmp_path / "bad-certificate.db"
        with pytest.raises(ValueError, match="scoped sentinel certificate"):
            write_resolved_catalog(
                (variable,),
                output,
                manifest=synthetic_manifest(),
                classifications=(book,),
            )
        assert not output.exists()
        return
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
