"""Register and subset parent resolution from Socialstyrelsen workbooks.

The prepared parent records are claimed by `cases/prepare/sos-*` cases. This keeps
the one build-side claim no build case reaches yet: two subsets that repeat one
Deldatamängdsnamn resolve as two register variants.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from _sos_fixtures import (
    source_revision as _revision,
    write_source_workbook as _write_source_workbook,
)
from reg_meta_build.catalog_resolution import resolve_parents
from reg_meta_build.source_coordinates import native_parent_key, source_register_key
from reg_meta_build.source_naming import NamingDeclaration, NativeNamingTarget
from reg_meta_build.sources.sos import parse_register_file
from reg_meta_build.sources.sos_records import (
    clean_sos_source,
)

from reg_meta_build.fqid_slugs import SlugEntry

if TYPE_CHECKING:
    from pathlib import Path


def test_lova_repeated_subset_token_resolves_two_variants(tmp_path: Path) -> None:
    # The prepare half (two keys, labelled names) is
    # cases/prepare/sos-repeated-subset-token-keys-each-subset-by-its-label.
    # Fails if parent resolution merges the two LOVA subsets or reports
    # unknown_parent_name for one of them.
    import openpyxl

    path = tmp_path / "Metadata LOVA (LOVA).xlsx"
    _write_source_workbook(path)
    workbook = openpyxl.load_workbook(path)
    subsets = workbook.create_sheet("Deldatamängder och datavyer")
    subsets.append(
        [
            "Deldatamängdsetikett",
            "Deldatamängdsnamn",
            "Deldatamängdsbeskrivning",
            "Data från",
            "Data till",
            "Uppdateringsfrekvens",
            "Aggregeringsnivå",
            "Kommentar",
        ]
    )
    labels = (
        "Legitimerade omsorgs- och vårdyrkesgruppers ekonomi och arbetsmarknadssituation",
        "Legitimerade omsorgs- och vårdyrkesgruppers arbetsmarknadsstatus",
    )
    subsets.append(
        [
            labels[0],
            "LOVA",
            "Arbetsmarknadsstatus och vissa ekomoniska uppgifter",
            1995,
            None,
            None,
            "Individ",
            None,
        ]
    )
    for _ in range(12):
        subsets.append([None] * 8)
    subsets.append(
        [
            labels[1],
            "LOVA",
            "Uppgifter om innehavare",
            1995,
            None,
            None,
            "Individ",
            "Huvudtabell som samanställer uppgifter från flera källar",
        ]
    )
    workbook.save(path)
    workbook.close()

    records = clean_sos_source(parse_register_file(path), _revision(path)).records
    names = []
    seen = set()
    for record in records:
        for parent in record.parent_facts:
            if parent.kind not in {"register", "variant"}:
                continue
            key = native_parent_key(record.source, "sos", parent)
            assert key is not None
            if key in seen:
                continue
            seen.add(key)
            kind = "register" if parent.kind == "register" else "register_variant"
            names.append(
                NamingDeclaration(
                    target=NativeNamingTarget(
                        kind=kind,
                        provider="sos",
                        source_key=key,
                        register_key=source_register_key(record)
                        if parent.kind == "variant"
                        else None,
                    ),
                    naming=SlugEntry(
                        kind=kind,
                        provider="sos",
                        source_id="1" if kind == "register" else f"1.{len(names)}",
                        slug="lova" if kind == "register" else f"view-{len(names)}",
                    ),
                    contributors=(),
                )
            )
    resolved = resolve_parents(records, tuple(names))
    assert len(resolved.variants) == 2
    assert not any(
        issue.code == "unknown_parent_name" for issue in resolved.diagnostics
    )
