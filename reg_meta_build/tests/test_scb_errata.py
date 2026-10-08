"""The scoped-attributions provenance format for SCB errata.

Loading and structural validation of the errata tables are the `scb-errata-*`
cases in `cases/curation_toml/`.
"""

from __future__ import annotations

from reg_meta_build.scb_errata import scoped_state_provenance


def test_scoped_formatter_keeps_multiline_evidence_with_its_edition() -> None:
    # Fails if the formatter drops the evidence's paragraph break or its source
    # edition. Input is the loader's provenance for a default-class
    # [[errata.delivered]] entry with multiline evidence
    # (`scb-errata-delivered-default-class-multiline-evidence-loads`).
    provenance = (
        "errata:omitted-column-in-version\nFirst paragraph.\n\nSecond paragraph."
    )
    assert scoped_state_provenance([(provenance, "2010")]) == (
        "errata:scoped-attributions\n"
        '[{"class":"omitted-column-in-version",'
        '"evidence":"First paragraph.\\n\\nSecond paragraph.",'
        '"source_editions":["2010"]}]'
    )
